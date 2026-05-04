package fiscal

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/blondbeauty/blond-beauty-engine/internal/message"
	"github.com/blondbeauty/blond-beauty-engine/internal/outbox"
)

// EventSink emits a follow-up event onto the caller's transaction.
// Production wires this to outbox.Enqueue; tests use a capturing sink.
type EventSink interface {
	Emit(ctx context.Context, tx pgx.Tx, queue string, env *message.Envelope) error
}

// OutboxSink is the production EventSink: writes follow-up events to
// outbox_messages so the outbox publisher delivers them with publisher confirms.
type OutboxSink struct{}

func (OutboxSink) Emit(ctx context.Context, tx pgx.Tx, queue string, env *message.Envelope) error {
	return outbox.Enqueue(ctx, tx, queue, env)
}
