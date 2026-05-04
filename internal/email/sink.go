package email

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/blondbeauty/blond-beauty-engine/internal/message"
	"github.com/blondbeauty/blond-beauty-engine/internal/outbox"
)

// OutboxSink is the production EventSink: it writes follow-up events to
// outbox_messages so the engine's outbox publisher delivers them with
// publisher confirms.
type OutboxSink struct{}

func (OutboxSink) Emit(ctx context.Context, tx pgx.Tx, queue string, env *message.Envelope) error {
	return outbox.Enqueue(ctx, tx, queue, env)
}
