package fulfillment

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5"

	"github.com/blondbeauty/blond-beauty-engine/internal/message"
	"github.com/blondbeauty/blond-beauty-engine/internal/workers"
)

// Handler returns the workers.Handler for QueueFulfillment.
func (s *Service) Handler() workers.Handler {
	return func(ctx context.Context, tx pgx.Tx, env *message.Envelope) error {
		var p FulfillmentPayload
		if len(env.Payload) > 0 {
			if err := json.Unmarshal(env.Payload, &p); err != nil {
				return workers.Permanent(err)
			}
		}
		return s.HandleFulfillment(ctx, tx, env, p)
	}
}
