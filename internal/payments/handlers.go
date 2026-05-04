package payments

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5"

	"github.com/blondbeauty/blond-beauty-engine/internal/message"
	"github.com/blondbeauty/blond-beauty-engine/internal/workers"
)

// Queue names consumed by the engine. Keep aligned with the spec (§5).
const (
	QueueCreate    = "payments.create"
	QueueConfirm   = "payments.confirm"
	QueueCapture   = "payments.capture"
	QueueCancel    = "payments.cancel"
	QueueRefund    = "payments.refund"
	QueueReconcile = "payments.reconcile"
	QueueWebhook   = "payments.webhook" // Slice C
)

// Handlers returns one workers.Handler per payment queue. Each handler
// decodes the envelope payload into the operation-specific struct and
// delegates to the service. Decode failures are permanent (DLQ).
func (s *Service) Handlers() map[string]workers.Handler {
	return map[string]workers.Handler{
		QueueCreate:    decodeAnd(s.HandleCreate),
		QueueConfirm:   decodeAnd(s.HandleConfirm),
		QueueCapture:   decodeAnd(s.HandleCapture),
		QueueCancel:    decodeAnd(s.HandleCancel),
		QueueRefund:    decodeAnd(s.HandleRefund),
		QueueReconcile: decodeAnd(s.HandleReconcile),
	}
}

// decodeAnd returns a workers.Handler that decodes the payload as P and
// delegates to fn. Decode failures classify as Permanent.
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

// decodePayload is a shared helper used by webhook.go.
func decodePayload(raw []byte, dst any) error {
	if len(raw) == 0 {
		return nil
	}
	return json.Unmarshal(raw, dst)
}

// WebhookHandler returns a workers.Handler for the QueueWebhook queue.
func (ws *WebhookService) WebhookHandler() workers.Handler {
	return ws.Handle
}
