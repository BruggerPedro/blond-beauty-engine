package workers

import (
	"context"
	"log/slog"

	"github.com/jackc/pgx/v5"

	"github.com/blondbeauty/blond-beauty-engine/internal/message"
)

// FakeQueue is the no-op queue name used to validate the worker lifecycle in
// Slice A. Remove once real workers are wired in later slices.
const FakeQueue = "engine.fake.noop"

// NewFakeHandler returns a Handler that does nothing besides logging. It
// participates in the dedup/idempotency flow because the framework wraps it
// in a transaction and Claim/MarkDone calls.
func NewFakeHandler(logger *slog.Logger) Handler {
	return func(_ context.Context, _ pgx.Tx, env *message.Envelope) error {
		logger.Info("fake worker handled message",
			"event_type", env.EventType,
			"message_id", env.MessageID,
			"aggregate_id", env.AggregateID,
		)
		return nil
	}
}
