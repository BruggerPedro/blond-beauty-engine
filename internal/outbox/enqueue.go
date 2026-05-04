package outbox

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/blondbeauty/blond-beauty-engine/internal/message"
)

// Enqueue inserts a follow-up event into outbox_messages within the caller's
// transaction. The Engine's outbox publisher will deliver it to RabbitMQ with
// publisher confirms.
//
// queue is the destination queue name (matches what API/engine consumers
// subscribe to). The envelope must already be valid; Encode is called once
// here so the stored payload is canonical.
func Enqueue(ctx context.Context, tx pgx.Tx, queue string, env *message.Envelope) error {
	body, err := env.Encode()
	if err != nil {
		return fmt.Errorf("encode envelope: %w", err)
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO outbox_messages (id, queue, payload, headers)
VALUES ($1, $2, $3::jsonb, NULL)`, uuid.NewString(), queue, body); err != nil {
		return fmt.Errorf("insert outbox row: %w", err)
	}
	return nil
}
