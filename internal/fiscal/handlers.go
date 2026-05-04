package fiscal

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5"

	"github.com/blondbeauty/blond-beauty-engine/internal/message"
	"github.com/blondbeauty/blond-beauty-engine/internal/workers"
)

// Handlers returns one workers.Handler per fiscal queue.
// Decode failures are classified as Permanent (DLQ).
func (s *Service) Handlers() map[string]workers.Handler {
	return map[string]workers.Handler{
		QueueIssue:  decodeAnd(s.HandleIssue),
		QueueRetry:  decodeAnd(s.HandleRetry),
		QueueCancel: decodeAnd(s.HandleCancel),
	}
}

func decodeAnd[P any](fn func(context.Context, pgx.Tx, *message.Envelope, P) error) workers.Handler {
	return func(ctx context.Context, tx pgx.Tx, env *message.Envelope) error {
		var p P
		if len(env.Payload) > 0 {
			if err := json.Unmarshal(env.Payload, &p); err != nil {
				return workers.Permanent(err)
			}
		}
		return fn(ctx, tx, env, p)
	}
}
