package fulfillment

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"

	"github.com/blondbeauty/blond-beauty-engine/internal/idempotency"
	"github.com/blondbeauty/blond-beauty-engine/internal/message"
	"github.com/blondbeauty/blond-beauty-engine/internal/workers"
)

// Service is the fulfillment orchestrator. Constructed once at startup; shared
// across workers. No carrier/3PL-specific imports belong here.
type Service struct {
	logger   *slog.Logger
	provider Provider
	store    Store
	sink     EventSink
}

func NewService(logger *slog.Logger, prov Provider, store Store, sink EventSink) *Service {
	return &Service{
		logger:   logger.With("component", "fulfillment"),
		provider: prov,
		store:    store,
		sink:     sink,
	}
}

// FulfillmentPayload is the shape of order.fulfillment.requested.
type FulfillmentPayload struct {
	OrderID         string `json:"order_id"`
	FiscalInvoiceID string `json:"fiscal_invoice_id,omitempty"` // informational; state is re-read
}

// HandleFulfillment processes order.fulfillment.requested.
//
// Pipeline (within the worker's transaction):
//  1. Validate payload — order_id required.
//  2. Load fulfillment state (order + payment + fiscal) from DB.
//     Events are triggers, not authority — all preconditions are re-checked here.
//  3. Precondition gate — permanent errors for all disallowed states.
//  4. FindOrCreateShipment — idempotent upsert keyed on order_id (single-shipment model).
//  5. Idempotency guard — already dispatched → ack; already failed → permanent.
//  6. Call provider.DispatchShipment — TEMPORARY MODEL caveat below.
//  7. On success: UpdateShipment (dispatched) + InsertShipmentEvent + emit events.
//  8. On permanent failure: UpdateShipment (failed) + InsertShipmentEvent +
//     emit order.fulfillment.failed.
//  9. On transient failure: persist attempt_count — worker retries with backoff.
//
// Security invariants enforced here:
//   - TrackingURL is NEVER emitted in events or logged — stored in DB only.
//   - TrackingNumber is logged only at debug level; omitted from events.
//   - No customer name, address, phone, CPF/CNPJ, or email appears in this layer.
//   - No fiscal XML/DANFE content appears here.
//
// TEMPORARY MODEL — acceptable only while using the fake provider for local
// development. The provider.DispatchShipment call happens inside the DB
// transaction that holds the FOR UPDATE lock on the shipment row. For the fake
// provider this is instant and harmless. Before wiring any real carrier/3PL
// adapter, refactor to a two-phase model:
//  1. Short tx: set shipment.status = 'pending', increment attempt_count.
//  2. External dispatch call outside any DB transaction.
//  3. Short tx: set status = 'dispatched'/'failed' + persist result + emit events.
//
// The unique index on shipments(idempotency_key) + provider-level idempotency key
// together guarantee no double-dispatch across retries without a long-held lock.
func (s *Service) HandleFulfillment(ctx context.Context, tx pgx.Tx, env *message.Envelope, p FulfillmentPayload) error {
	if p.OrderID == "" {
		return workers.Permanent(fmt.Errorf("fulfillment: order_id missing in payload"))
	}

	// Always re-read state from DB — the event is only a trigger.
	state, err := s.store.LoadFulfillmentState(ctx, tx, p.OrderID)
	if err != nil {
		if errors.Is(err, ErrOrderNotFound) {
			return workers.Permanent(err)
		}
		return err
	}

	if err := checkPreconditions(state); err != nil {
		return err // already wrapped as Permanent where appropriate
	}

	idemKey := "fulfill|" + p.OrderID
	shipment, err := s.store.FindOrCreateShipment(ctx, tx, &ShipmentRow{
		OrderID:               p.OrderID,
		Provider:              s.provider.Name(),
		IdempotencyKey:        idemKey,
		ShippedEmailMessageID: "", // populated from shipments row after FindOrCreate
		CorrelationID:         env.CorrelationID,
		CausationID:           env.MessageID,
	})
	if err != nil {
		return err
	}

	// Idempotency: already dispatched by a previous delivery.
	if shipment.Status == ShipmentDispatched {
		s.logger.Info("shipment already dispatched; skipping",
			"shipment_id", shipment.ID,
			"order_id", p.OrderID,
		)
		return nil
	}

	// Terminal: previous attempt was permanently rejected — do not retry.
	if shipment.Status == ShipmentFailed {
		return workers.Permanent(fmt.Errorf("%w: shipment %s", ErrShipmentAlreadyFailed, shipment.ID))
	}

	requestKey := buildRequestKey(env.MessageID, shipment.ID)
	result, dispErr := s.provider.DispatchShipment(ctx, DispatchRequest{
		ShipmentID:     shipment.ID,
		OrderID:        p.OrderID,
		IdempotencyKey: requestKey,
	})

	shipment.AttemptCount++

	if dispErr != nil {
		shipment.LastError = truncate(dispErr.Error(), 500)

		if errors.Is(dispErr, ErrProviderPermanent) {
			shipment.Status = ShipmentFailed
			if err := s.store.UpdateShipment(ctx, tx, shipment); err != nil {
				return err
			}
			if err := s.insertEvent(ctx, tx, shipment, EventFulfillmentFailed, map[string]any{
				"error": truncate(dispErr.Error(), 200),
			}); err != nil {
				return err
			}
			if err := s.emitFulfillmentFailed(ctx, tx, env, shipment); err != nil {
				return err
			}
			return workers.Permanent(dispErr)
		}

		// Transient: persist attempt_count, let framework retry.
		if err := s.store.UpdateShipment(ctx, tx, shipment); err != nil {
			return err
		}
		return dispErr
	}

	// Dispatch succeeded.
	shipment.Status = ShipmentDispatched
	shipment.Carrier = result.Carrier
	shipment.TrackingNumber = result.TrackingNumber
	shipment.TrackingURL = result.TrackingURL // stored in DB; never emitted or logged
	shipment.ProviderShipmentID = result.ProviderShipmentID
	shipment.LastError = ""

	if err := s.store.UpdateShipment(ctx, tx, shipment); err != nil {
		return err
	}
	if err := s.insertEvent(ctx, tx, shipment, EventFulfilled, map[string]any{
		"carrier": shipment.Carrier,
		// tracking_number intentionally omitted from audit events per privacy policy
	}); err != nil {
		return err
	}

	// If a pre-created email_messages row exists, populate it and request delivery.
	if shipment.ShippedEmailMessageID != "" {
		emailData := ShipmentEmailData{
			OrderID:        p.OrderID,
			Carrier:        shipment.Carrier,
			TrackingNumber: shipment.TrackingNumber,
			TrackingURL:    shipment.TrackingURL, // SENSITIVE — goes to email_messages only
		}
		if err := s.store.UpdateEmailTemplateData(ctx, tx, shipment.ShippedEmailMessageID, emailData); err != nil {
			return err
		}
		if err := s.emitEmailSendRequested(ctx, tx, env, shipment); err != nil {
			return err
		}
	}

	if err := s.emitFulfilled(ctx, tx, env, shipment, p.OrderID); err != nil {
		return err
	}

	// Log only non-sensitive identifiers. TrackingURL is intentionally absent.
	s.logger.Info("shipment dispatched",
		"shipment_id", shipment.ID,
		"order_id", p.OrderID,
		"carrier", shipment.Carrier,
		"provider", shipment.Provider,
	)
	return nil
}

// ---- precondition gate ---------------------------------------------------

// checkPreconditions validates the loaded DB state before attempting dispatch.
// Returns a workers.Permanent error for any state that permanently blocks
// fulfillment. Returns a plain error for transient states (not currently used —
// all blocking states are treated as permanent to avoid retry storms on broken data).
func checkPreconditions(state *FulfillmentState) error {
	// Order status.
	switch state.OrderStatus {
	case "cancelled", "refunded":
		return workers.Permanent(fmt.Errorf("%w: order status is %q", ErrPreconditionFailed, state.OrderStatus))
	default:
		if !allowedOrderStatuses[state.OrderStatus] {
			return workers.Permanent(fmt.Errorf("%w: unexpected order status %q", ErrPreconditionFailed, state.OrderStatus))
		}
	}

	// Payment status — must be in a successful canonical state.
	if !successfulPaymentStatuses[state.PaymentStatus] {
		if state.PaymentStatus == "" {
			return workers.Permanent(fmt.Errorf("%w: no successful payment found for order", ErrPreconditionFailed))
		}
		return workers.Permanent(fmt.Errorf("%w: payment status %q does not allow fulfillment", ErrPreconditionFailed, state.PaymentStatus))
	}

	// Fiscal gate — enforced only when the order requires fiscal authorization.
	if state.RequiresFiscal {
		switch state.FiscalStatus {
		case "authorized":
			// Fulfillment may proceed.
		case "":
			return workers.Permanent(fmt.Errorf("%w: fiscal required but no invoice found", ErrPreconditionFailed))
		case "pending", "issuing":
			// In correct operation this should not happen (fiscal.authorized triggers
			// fulfillment.requested), but we block unconditionally to uphold the gating
			// invariant. A race condition here is a data integrity issue.
			return workers.Permanent(fmt.Errorf("%w: fiscal authorization is still pending", ErrPreconditionFailed))
		case "rejected":
			return workers.Permanent(fmt.Errorf("%w: fiscal invoice was rejected", ErrPreconditionFailed))
		case "cancelled":
			return workers.Permanent(fmt.Errorf("%w: fiscal invoice was cancelled", ErrPreconditionFailed))
		default:
			return workers.Permanent(fmt.Errorf("%w: unexpected fiscal status %q", ErrPreconditionFailed, state.FiscalStatus))
		}
	}

	return nil
}

// ---- event emitters ------------------------------------------------------

func (s *Service) emitFulfilled(ctx context.Context, tx pgx.Tx, parent *message.Envelope, ship *ShipmentRow, orderID string) error {
	env, err := message.New(EventFulfilled, "order", orderID, map[string]any{
		"order_id":    orderID,
		"shipment_id": ship.ID,
		"carrier":     ship.Carrier,
		"provider":    ship.Provider,
		// tracking_number: deliberately omitted — not PII but sensitive in aggregate
		// tracking_url: NEVER emitted — SENSITIVE, accessible only via API
	})
	if err != nil {
		return err
	}
	chain(env, parent)
	return s.sink.Emit(ctx, tx, QueueEvents, env)
}

func (s *Service) emitFulfillmentFailed(ctx context.Context, tx pgx.Tx, parent *message.Envelope, ship *ShipmentRow) error {
	env, err := message.New(EventFulfillmentFailed, "order", ship.OrderID, map[string]any{
		"order_id":    ship.OrderID,
		"shipment_id": ship.ID,
		"provider":    ship.Provider,
		// error details are truncated and in last_error; raw provider response never emitted
	})
	if err != nil {
		return err
	}
	chain(env, parent)
	return s.sink.Emit(ctx, tx, QueueEvents, env)
}

func (s *Service) emitEmailSendRequested(ctx context.Context, tx pgx.Tx, parent *message.Envelope, ship *ShipmentRow) error {
	env, err := message.New(EventEmailSendRequested, "email_message", ship.ShippedEmailMessageID, map[string]any{
		"email_message_id": ship.ShippedEmailMessageID,
	})
	if err != nil {
		return err
	}
	chain(env, parent)
	return s.sink.Emit(ctx, tx, QueueEmailTransactional, env)
}

// ---- helpers -------------------------------------------------------------

func (s *Service) insertEvent(ctx context.Context, tx pgx.Tx, ship *ShipmentRow, eventType string, payload map[string]any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal shipment event payload: %w", err)
	}
	return s.store.InsertShipmentEvent(ctx, tx, &ShipmentEventRow{
		ShipmentID: ship.ID,
		EventType:  eventType,
		Payload:    raw,
	})
}

func chain(child *message.Envelope, parent *message.Envelope) {
	child.CorrelationID = parent.CorrelationID
	cause := parent.MessageID
	child.CausationID = &cause
}

// buildRequestKey derives a stable idempotency key for (message, shipment).
// Stable across worker retries of the same message.
func buildRequestKey(messageID, shipmentID string) string {
	return idempotency.Key("fulfillment.provider_request", messageID, shipmentID)
}
