package workers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/blondbeauty/blond-beauty-engine/internal/message"
	"github.com/blondbeauty/blond-beauty-engine/internal/observability"
	"github.com/blondbeauty/blond-beauty-engine/internal/queue"
)

// Handler processes one message envelope inside a Postgres transaction.
//
// Implementations MUST be idempotent: the framework provides dedup via
// IdempotencyStore.Claim, but handlers should also key business writes by
// stable identifiers (order_id, payment_id, message_id).
//
// Returning a Permanent(err) routes the message straight to DLQ. Returning any
// other error triggers retry with exponential backoff up to MaxAttempts; after
// that the message is also sent to DLQ.
type Handler func(ctx context.Context, tx pgx.Tx, env *message.Envelope) error

type Config struct {
	Queue       string
	Concurrency int
	MaxAttempts int
	BackoffBase time.Duration
	BackoffMax  time.Duration
}

func (c *Config) defaults() {
	if c.Concurrency <= 0 {
		c.Concurrency = 1
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 5
	}
	if c.BackoffBase <= 0 {
		c.BackoffBase = 500 * time.Millisecond
	}
	if c.BackoffMax <= 0 {
		c.BackoffMax = 30 * time.Second
	}
}

const (
	headerAttempt = "x-engine-attempt"
)

type Worker struct {
	cfg     Config
	logger  *slog.Logger
	pool    *pgxpool.Pool
	conn    *queue.Conn
	idem    *IdempotencyStore
	metrics *observability.Metrics
	handler Handler
}

func New(cfg Config, logger *slog.Logger, pool *pgxpool.Pool, conn *queue.Conn, idem *IdempotencyStore, m *observability.Metrics, h Handler) *Worker {
	cfg.defaults()
	return &Worker{
		cfg:     cfg,
		logger:  logger.With("worker", cfg.Queue),
		pool:    pool,
		conn:    conn,
		idem:    idem,
		metrics: m,
		handler: h,
	}
}

// Run consumes until ctx is cancelled. It declares its queue + DLX/DLQ on
// startup. Returns once all in-flight messages drain after ctx cancellation.
func (w *Worker) Run(ctx context.Context) error {
	spec := queue.DefaultSpec(w.cfg.Queue)

	ch, err := w.conn.Channel()
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}
	defer ch.Close()

	if err := queue.Declare(ch, spec); err != nil {
		return err
	}
	retryPub, err := queue.NewPublisher(w.conn)
	if err != nil {
		return fmt.Errorf("retry publisher: %w", err)
	}
	defer retryPub.Close()
	if err := ch.Qos(w.cfg.Concurrency, 0, false); err != nil {
		return fmt.Errorf("qos: %w", err)
	}
	deliveries, err := ch.Consume(spec.Name, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	w.logger.Info("worker started", "concurrency", w.cfg.Concurrency)

	var wg sync.WaitGroup
	sem := make(chan struct{}, w.cfg.Concurrency)

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case d, ok := <-deliveries:
			if !ok {
				break loop
			}
			sem <- struct{}{}
			wg.Add(1)
			go func(d amqp.Delivery) {
				defer wg.Done()
				defer func() { <-sem }()
				w.process(ctx, retryPub, &d)
			}(d)
		}
	}

	wg.Wait()
	w.logger.Info("worker stopped")
	return nil
}

func (w *Worker) process(ctx context.Context, retryPub *queue.Publisher, d *amqp.Delivery) {
	w.metrics.WorkerInflight.WithLabelValues(w.cfg.Queue).Inc()
	defer w.metrics.WorkerInflight.WithLabelValues(w.cfg.Queue).Dec()
	start := time.Now()

	env, err := message.Decode(d.Body)
	if err != nil {
		w.logger.Error("invalid envelope; routing to DLQ", "err", err)
		_ = d.Nack(false, false) // requeue=false → DLX
		w.metrics.MessagesProcessed.WithLabelValues(w.cfg.Queue, "dlq").Inc()
		return
	}

	attempt := readAttempt(d.Headers) + 1
	finalAttempt := attempt >= w.cfg.MaxAttempts
	logger := w.logger.With(
		"message_id", env.MessageID,
		"correlation_id", env.CorrelationID,
		"event_type", env.EventType,
		"aggregate_id", env.AggregateID,
		"attempt", attempt,
	)

	err = w.runOnce(ctx, env, finalAttempt)
	w.metrics.MessageLatency.WithLabelValues(w.cfg.Queue).Observe(time.Since(start).Seconds())

	switch {
	case err == nil:
		_ = d.Ack(false)
		w.metrics.MessagesProcessed.WithLabelValues(w.cfg.Queue, "ok").Inc()
		logger.Info("message processed", "latency_ms", time.Since(start).Milliseconds())

	case errors.Is(err, errAlreadyProcessed):
		_ = d.Ack(false)
		w.metrics.MessagesProcessed.WithLabelValues(w.cfg.Queue, "skip").Inc()
		logger.Info("duplicate; acked")

	case Classify(err) == RetryPermanent || finalAttempt:
		// route to DLQ
		_ = d.Nack(false, false)
		w.metrics.MessagesProcessed.WithLabelValues(w.cfg.Queue, "dlq").Inc()
		logger.Error("routing to DLQ", "err", err, "permanent", Classify(err) == RetryPermanent)

	default:
		// Transient: sleep with backoff, then publish a fresh copy with an
		// incremented attempt header. Ack the original only after the retry copy
		// is durably confirmed by RabbitMQ.
		delay := Backoff(attempt, w.cfg.BackoffBase, w.cfg.BackoffMax)
		logger.Warn("transient failure; will retry", "err", err, "delay", delay.String())
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			_ = d.Nack(false, true)
			return
		}
		headers := retryHeaders(d.Headers, attempt)
		if pubErr := retryPub.Publish(ctx, w.cfg.Queue, d.Body, headers); pubErr != nil {
			logger.Error("retry publish failed; requeueing original", "err", pubErr)
			_ = d.Nack(false, true)
			return
		}
		_ = d.Ack(false)
		w.metrics.MessagesProcessed.WithLabelValues(w.cfg.Queue, "retry").Inc()
	}
}

var errAlreadyProcessed = errors.New("already processed")

func (w *Worker) runOnce(ctx context.Context, env *message.Envelope, finalAttempt bool) error {
	tx, err := w.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	claimed, err := w.idem.Claim(ctx, tx, w.cfg.Queue, env.MessageID)
	if err != nil {
		return err
	}
	if !claimed {
		return errAlreadyProcessed
	}

	// Savepoint: if the handler runs a query that fails and leaves the
	// transaction in the PostgreSQL "aborted" state (SQLSTATE 25P02), we
	// must roll back to here before attempting Release or MarkDone on the
	// same connection — otherwise every subsequent statement in this tx
	// will also fail with 25P02.
	if _, err := tx.Exec(ctx, "SAVEPOINT handler_start"); err != nil {
		return fmt.Errorf("savepoint: %w", err)
	}

	handlerErr := w.handler(ctx, tx, env)
	if handlerErr != nil {
		// Roll back the handler's partial writes and clear any aborted-tx
		// state, keeping the idempotency Claim INSERT intact.
		if _, spErr := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT handler_start"); spErr != nil {
			return fmt.Errorf("rollback to savepoint: %w", spErr)
		}
		if Classify(handlerErr) == RetryPermanent || finalAttempt {
			if markErr := w.idem.MarkDone(ctx, tx, w.cfg.Queue, env.MessageID); markErr != nil {
				return markErr
			}
		} else {
			if releaseErr := w.idem.Release(ctx, tx, w.cfg.Queue, env.MessageID); releaseErr != nil {
				return releaseErr
			}
		}
		if commitErr := tx.Commit(ctx); commitErr != nil {
			return commitErr
		}
		return handlerErr
	}
	if err := w.idem.MarkDone(ctx, tx, w.cfg.Queue, env.MessageID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func readAttempt(h amqp.Table) int {
	if h == nil {
		return 0
	}
	v, ok := h[headerAttempt]
	if !ok {
		return 0
	}
	switch n := v.(type) {
	case int32:
		return int(n)
	case int64:
		return int(n)
	case int:
		return n
	}
	return 0
}

func retryHeaders(in amqp.Table, completedAttempt int) amqp.Table {
	out := amqp.Table{}
	for k, v := range in {
		out[k] = v
	}
	out[headerAttempt] = int32(completedAttempt)
	return out
}
