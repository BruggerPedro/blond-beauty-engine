// Package workers provides the worker lifecycle framework: queue consumption,
// idempotency, exponential backoff retry, DLQ routing, and graceful shutdown.
package workers

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// IdempotencyStore deduplicates processed messages by (queue, message_id).
// It uses the engine-operational table `engine_processed_messages`, which is
// owned by the Engine and not part of the API domain schema.
type IdempotencyStore struct {
	pool *pgxpool.Pool
}

func NewIdempotencyStore(pool *pgxpool.Pool) *IdempotencyStore {
	return &IdempotencyStore{pool: pool}
}

// Claim atomically reserves a message for processing. It returns:
//   - (true, nil)  if this is the first time we see this message;
//   - (false, nil) if the message was already processed (duplicate);
//   - (false, err) on db error.
//
// On success the caller must call Commit (with the same tx if it wants
// transactional consistency between business state and the dedup record).
func (s *IdempotencyStore) Claim(ctx context.Context, tx pgx.Tx, queue, messageID string) (bool, error) {
	const q = `
INSERT INTO engine_processed_messages (queue, message_id, status)
VALUES ($1, $2, 'in_progress')
ON CONFLICT (queue, message_id) DO NOTHING
RETURNING message_id
`
	var got string
	err := tx.QueryRow(ctx, q, queue, messageID).Scan(&got)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("claim message: %w", err)
	}
	return true, nil
}

// MarkDone updates the dedup row to processed. Call inside the same tx as the
// business write so both commit atomically.
func (s *IdempotencyStore) MarkDone(ctx context.Context, tx pgx.Tx, queue, messageID string) error {
	_, err := tx.Exec(ctx,
		`UPDATE engine_processed_messages SET status='done', updated_at=now() WHERE queue=$1 AND message_id=$2`,
		queue, messageID)
	if err != nil {
		return fmt.Errorf("mark done: %w", err)
	}
	return nil
}
