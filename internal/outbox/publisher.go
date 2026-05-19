// Package outbox implements the transactional outbox publisher.
//
// Schema contract: the API owns `outbox_messages`. The Engine reads pending
// rows, publishes them to RabbitMQ with publisher confirms, and marks rows
// published only after broker ack. Actual schema (owned by API migrations):
//
//	create table outbox_messages (
//	    id              uuid        primary key,
//	    aggregate_type  text        not null,
//	    aggregate_id    text        not null,
//	    event_type      text        not null,
//	    schema_version  int         not null default 1,
//	    payload         jsonb       not null,
//	    status          text        not null default 'pending',  -- pending|publishing|published|failed
//	    attempts        int         not null default 0,
//	    next_attempt_at timestamptz not null default now(),
//	    published_at    timestamptz,
//	    last_error      text,
//	    locked_at       timestamptz,
//	    created_at      timestamptz not null default now()
//	);
//
// The queue name is derived from event_type via the static routing table below.
// The publisher is safe to run with multiple replicas: rows are claimed via
// SELECT ... FOR UPDATE SKIP LOCKED and updated within the same transaction.
package outbox

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/blondbeauty/blond-beauty-engine/internal/observability"
	"github.com/blondbeauty/blond-beauty-engine/internal/queue"
)

// eventTypeToQueue maps API event_type values to RabbitMQ queue names.
// Must stay in sync with the API's internal/outbox/routing.go.
var eventTypeToQueue = map[string]string{
	"payment.create.requested":    "payments.create",
	"payment.confirm.requested":   "payments.confirm",
	"payment.webhook.received":    "payments.webhook",
	"email.send.requested":        "emails.transactional",
	"fiscal.issue.requested":      "fiscal.issue",
	"fiscal.retry.requested":      "fiscal.retry",
	"fiscal.cancel.requested":     "fiscal.cancel",
	"order.fulfillment.requested": "orders.fulfillment",
	"report.export.requested":     "reports.export",
	"privacy.export.requested":    "privacy.export",
	"privacy.delete.requested":    "privacy.delete",
}

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

	// Claim a batch: atomically transition pending → publishing.
	// FOR UPDATE SKIP LOCKED ensures concurrent publisher instances (API's own
	// publisher + this one) do not claim the same rows.
	rows, err := tx.Query(ctx, `
WITH claimed AS (
    SELECT id
    FROM   outbox_messages
    WHERE  status = 'pending'
      AND  next_attempt_at <= now()
    ORDER  BY next_attempt_at ASC
    LIMIT  $1
    FOR UPDATE SKIP LOCKED
)
UPDATE outbox_messages om
SET    status = 'publishing', locked_at = now()
FROM   claimed
WHERE  om.id = claimed.id
RETURNING om.id, om.event_type, om.payload, om.attempts`, p.batchSize)
	if err != nil {
		return 0, err
	}

	type row struct {
		id        string
		eventType string
		payload   []byte
		attempts  int
	}
	var batch []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.eventType, &r.payload, &r.attempts); err != nil {
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
		queueName, ok := eventTypeToQueue[r.eventType]
		if !ok {
			errMsg := fmt.Sprintf("no routing entry for event_type %q", r.eventType)
			p.logger.Warn("outbox: unknown event type, marking failed", "event_type", r.eventType, "id", r.id)
			if _, uerr := tx.Exec(ctx,
				`UPDATE outbox_messages SET status = 'failed', last_error = $2, locked_at = NULL WHERE id = $1`,
				r.id, errMsg); uerr != nil {
				return 0, uerr
			}
			p.metrics.OutboxPublished.WithLabelValues("fail").Inc()
			continue
		}

		if err := p.pub.Publish(ctx, queueName, r.payload, amqp.Table{}); err != nil {
			p.metrics.OutboxPublished.WithLabelValues("fail").Inc()
			// Exponential back-off: 30s, 60s, 120s, … capped at 10 min.
			backoff := time.Duration(1<<uint(r.attempts)) * 30 * time.Second
			if backoff > 10*time.Minute {
				backoff = 10 * time.Minute
			}
			if _, uerr := tx.Exec(ctx,
				`UPDATE outbox_messages
				 SET status = 'pending', attempts = attempts + 1,
				     next_attempt_at = now() + $2, last_error = $3, locked_at = NULL
				 WHERE id = $1`,
				r.id, backoff, truncate(err.Error(), 500)); uerr != nil {
				return 0, uerr
			}
			continue
		}

		if _, err := tx.Exec(ctx,
			`UPDATE outbox_messages
			 SET status = 'published', published_at = now(), last_error = NULL, locked_at = NULL
			 WHERE id = $1`, r.id); err != nil {
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
	err := p.pool.QueryRow(ctx,
		`SELECT count(*) FROM outbox_messages WHERE status IN ('pending', 'publishing')`).Scan(&n)
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
