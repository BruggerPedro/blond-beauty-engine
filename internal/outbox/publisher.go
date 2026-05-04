// Package outbox implements the transactional outbox publisher.
//
// Schema contract: the API owns `outbox_messages`. The Engine reads pending
// rows, publishes them to RabbitMQ with publisher confirms, and marks rows
// `published_at = now()` only after broker ack. Expected schema (owned by
// API; documented here for reference):
//
//	create table outbox_messages (
//	    id            uuid primary key,
//	    queue         text        not null,
//	    payload       jsonb       not null,   -- canonical envelope JSON
//	    headers       jsonb,                  -- optional amqp headers
//	    created_at    timestamptz not null default now(),
//	    published_at  timestamptz,
//	    attempts      int         not null default 0,
//	    last_error    text
//	);
//	create index on outbox_messages (published_at) where published_at is null;
//
// The publisher is safe to run with multiple replicas: rows are claimed via
// SELECT ... FOR UPDATE SKIP LOCKED and updated within the same transaction.
package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/blondbeauty/blond-beauty-engine/internal/observability"
	"github.com/blondbeauty/blond-beauty-engine/internal/queue"
)

type Publisher struct {
	logger    *slog.Logger
	pool      *pgxpool.Pool
	pub       *queue.Publisher
	metrics   *observability.Metrics
	batchSize int
	pollEvery time.Duration
}

type Options struct {
	BatchSize int
	PollEvery time.Duration
}

func New(logger *slog.Logger, pool *pgxpool.Pool, pub *queue.Publisher, m *observability.Metrics, opts Options) *Publisher {
	if opts.BatchSize <= 0 {
		opts.BatchSize = 64
	}
	if opts.PollEvery <= 0 {
		opts.PollEvery = 1 * time.Second
	}
	return &Publisher{
		logger:    logger.With("component", "outbox"),
		pool:      pool,
		pub:       pub,
		metrics:   m,
		batchSize: opts.BatchSize,
		pollEvery: opts.PollEvery,
	}
}

// Run loops until ctx is cancelled. It is safe to invoke from a goroutine.
// If the `outbox_messages` table does not exist (API has not deployed its
// schema), the publisher logs once and idles.
func (p *Publisher) Run(ctx context.Context) error {
	t := time.NewTicker(p.pollEvery)
	defer t.Stop()

	missingTableLogged := false
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			n, err := p.tick(ctx)
			if err != nil {
				if isMissingTable(err) {
					if !missingTableLogged {
						p.logger.Warn("outbox_messages table missing; idling", "hint", "API owns this table; deploy API migrations")
						missingTableLogged = true
					}
					continue
				}
				p.logger.Error("outbox tick failed", "err", err)
				continue
			}
			if n > 0 {
				missingTableLogged = false
			}
		}
	}
}

func (p *Publisher) tick(ctx context.Context) (int, error) {
	pending, err := p.pendingCount(ctx)
	if err != nil {
		return 0, err
	}
	p.metrics.OutboxPending.Set(float64(pending))

	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, `
SELECT id, queue, payload, headers
FROM outbox_messages
WHERE published_at IS NULL
ORDER BY created_at
FOR UPDATE SKIP LOCKED
LIMIT $1`, p.batchSize)
	if err != nil {
		return 0, err
	}

	type row struct {
		id      string
		queue   string
		payload []byte
		headers []byte
	}
	var batch []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.queue, &r.payload, &r.headers); err != nil {
			rows.Close()
			return 0, err
		}
		batch = append(batch, r)
	}
	rows.Close()
	if rows.Err() != nil {
		return 0, rows.Err()
	}

	for _, r := range batch {
		var headers amqp.Table
		if len(r.headers) > 0 {
			h := map[string]any{}
			if err := json.Unmarshal(r.headers, &h); err == nil {
				headers = amqp.Table(h)
			}
		}
		if err := p.pub.Publish(ctx, r.queue, r.payload, headers); err != nil {
			p.metrics.OutboxPublished.WithLabelValues("fail").Inc()
			if _, uerr := tx.Exec(ctx,
				`UPDATE outbox_messages SET attempts = attempts + 1, last_error = $2 WHERE id = $1`,
				r.id, truncate(err.Error(), 500)); uerr != nil {
				return 0, uerr
			}
			continue
		}
		if _, err := tx.Exec(ctx,
			`UPDATE outbox_messages SET published_at = now(), last_error = NULL WHERE id = $1`, r.id); err != nil {
			return 0, err
		}
		p.metrics.OutboxPublished.WithLabelValues("ok").Inc()
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return len(batch), nil
}

func (p *Publisher) pendingCount(ctx context.Context) (int64, error) {
	var n int64
	err := p.pool.QueryRow(ctx, `SELECT count(*) FROM outbox_messages WHERE published_at IS NULL`).Scan(&n)
	return n, err
}

func isMissingTable(err error) bool {
	if err == nil {
		return false
	}
	// pgx returns *pgconn.PgError with SQLState "42P01" for undefined_table.
	type sqlState interface{ SQLState() string }
	var s sqlState
	if errors.As(err, &s) && s.SQLState() == "42P01" {
		return true
	}
	return false
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
