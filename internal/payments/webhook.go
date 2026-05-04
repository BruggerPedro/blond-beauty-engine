package payments

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/blondbeauty/blond-beauty-engine/internal/message"
	"github.com/blondbeauty/blond-beauty-engine/internal/workers"
)

// statusRank assigns an ordering so the webhook handler can enforce
// forward-only transitions. Equal rank means different terminal states
// (cancelled vs refunded vs failed) which is ambiguous and triggers a
// provider lookup before deciding.
var statusRank = map[Status]int{
	StatusUnknown:        0,
	StatusRequiresAction: 1,
	StatusAuthorized:     2,
	StatusCaptured:       3,
	StatusPaid:           3,
	StatusFailed:         4,
	StatusCancelled:      4,
	StatusRefunded:       4,
	StatusChargeback:     5,
}

// WebhookPayload is the shape of the `payment.webhook.received` envelope
// payload published by the API after storing the raw ingress.
type WebhookPayload struct {
	IngressID string `json:"ingress_id"`
}

// WebhookService processes `payment.webhook.received` messages. It is
// separate from Service so the concern boundary is clear: Service drives
// engine-initiated operations (create, capture, …); WebhookService drives
// provider-initiated notifications.
type WebhookService struct {
	logger   *slog.Logger
	pool     *pgxpool.Pool
	registry *Registry
	store    Store
	sink     EventSink
}

func NewWebhookService(
	logger *slog.Logger,
	pool *pgxpool.Pool,
	reg *Registry,
	store Store,
	sink EventSink,
) *WebhookService {
	return &WebhookService{
		logger:   logger.With("component", "payments.webhook"),
		pool:     pool,
		registry: reg,
		store:    store,
		sink:     sink,
	}
}

// Handle processes a single `payment.webhook.received` envelope.
//
// Pipeline (all within one tx):
//  1. Load and lock webhook_ingress_events row.
//  2. Guard: if already verified or rejected, ack as duplicate.
//  3. Resolve provider adapter by ingress.provider — unknown → reject + ack.
//  4. VerifyWebhook — invalid signature → reject + ack (Permanent, not transient).
//     NO business state is mutated before this point.
//  5. ParseWebhook → WebhookEvent.
//  6. InsertPaymentEvent ON CONFLICT DO NOTHING — if already exists, ack.
//  7. Lock payment row by (provider, provider_payment_id).
//     If not found → return transient so the worker retries (handles
//     the race where the webhook arrives before the create worker commits).
//  8. Determine status transition with forward-only enforcement.
//     If ambiguous (same rank, different terminal) → provider lookup.
//  9. Update payment row; mark ingress verified.
//  10. Enqueue follow-up event (same as service.go, preserving
//     correlation_id and causation_id chain).
//
// NOTE — provider lookup inside tx: for the fake provider this is instant.
// Before wiring real providers, the same TEMPORARY MODEL caveat from
// service.go applies here: an external call inside a DB transaction holds
// the row lock for the duration of the network round-trip. Refactor to a
// two-phase model (short claim tx → external call → short result tx)
// before going to production with any live payment provider.
func (s *WebhookService) Handle(ctx context.Context, tx pgx.Tx, env *message.Envelope) error {
	var p WebhookPayload
	if err := decodePayload(env.Payload, &p); err != nil {
		return workers.Permanent(fmt.Errorf("webhook: decode payload: %w", err))
	}
	if p.IngressID == "" {
		return workers.Permanent(fmt.Errorf("webhook: ingress_id missing"))
	}

	ingress, err := s.store.LockIngress(ctx, tx, p.IngressID)
	if err != nil {
		// Ingress row not found could mean the API transaction hasn't
		// committed yet. Treat as transient so the worker retries.
		return fmt.Errorf("webhook: lock ingress: %w", err)
	}

	// Guard: already processed by a previous delivery of this or another message.
	if ingress.VerificationStatus == "verified" || ingress.VerificationStatus == "rejected" {
		s.logger.Info("webhook ingress already processed; skipping",
			"ingress_id", ingress.ID,
			"status", ingress.VerificationStatus,
		)
		return nil // ack
	}

	prov, err := s.registry.Get(ingress.Provider)
	if err != nil {
		// Unknown provider — reject the ingress and ack. This is permanent:
		// retrying won't bring the adapter into existence.
		if err := s.store.MarkIngressRejected(ctx, tx, ingress.ID, "unknown provider: "+ingress.Provider); err != nil {
			return err
		}
		return workers.Permanent(fmt.Errorf("%w: %s", ErrUnknownProvider, ingress.Provider))
	}

	// --- Verification gate ---
	// NO state mutation to payments, orders, or inventory is permitted
	// before VerifyWebhook returns nil.
	if verifyErr := prov.VerifyWebhook(ctx, ingress.RawHeaders, ingress.RawBody); verifyErr != nil {
		reason := truncateReason(verifyErr.Error())
		if err := s.store.MarkIngressRejected(ctx, tx, ingress.ID, reason); err != nil {
			return err
		}
		s.logger.Warn("webhook signature invalid; ingress rejected",
			"ingress_id", ingress.ID,
			"provider", ingress.Provider,
		)
		// Permanent: a bad signature cannot be fixed by retrying.
		return workers.Permanent(fmt.Errorf("%w: ingress=%s", ErrWebhookSignatureInvalid, ingress.ID))
	}

	event, parseErr := prov.ParseWebhook(ctx, ingress.RawHeaders, ingress.RawBody)
	if parseErr != nil {
		reason := truncateReason(parseErr.Error())
		if err := s.store.MarkIngressRejected(ctx, tx, ingress.ID, reason); err != nil {
			return err
		}
		return workers.Permanent(fmt.Errorf("%w: ingress=%s", ErrWebhookParseFailed, ingress.ID))
	}

	// Dedup: insert payment_events now (before loading the payment row).
	// If this event was already processed the insert returns inserted=false
	// and we ack without any further mutation.
	eventRow := &PaymentEventRow{
		ID:              uuid.NewString(),
		Provider:        ingress.Provider,
		ProviderEventID: event.ProviderEventID,
		EventType:       event.EventType,
		CanonicalStatus: event.CanonicalStatus,
		OccurredAt:      event.OccurredAt,
		IngressID:       ingress.ID,
		RedactedPayload: event.RedactedPayload,
	}
	inserted, err := s.store.InsertPaymentEvent(ctx, tx, eventRow)
	if err != nil {
		return fmt.Errorf("webhook: insert event: %w", err)
	}
	if !inserted {
		s.logger.Info("duplicate webhook event; skipping",
			"provider", ingress.Provider,
			"provider_event_id", event.ProviderEventID,
		)
		// Still mark the ingress verified (it was legitimate).
		return s.store.MarkIngressVerified(ctx, tx, ingress.ID, event.ProviderEventID)
	}

	// Load the payment row.
	if event.ProviderPaymentID == "" {
		// Cannot link to a payment without a provider payment id.
		// Mark ingress rejected; this is a parse-level issue.
		if err := s.store.MarkIngressRejected(ctx, tx, ingress.ID, "missing provider_payment_id in event"); err != nil {
			return err
		}
		return workers.Permanent(fmt.Errorf("webhook: provider_payment_id empty in parsed event"))
	}

	payment, err := s.store.LockPaymentByProviderID(ctx, tx, ingress.Provider, event.ProviderPaymentID)
	if err != nil {
		if errors.Is(err, ErrPaymentNotFound) {
			// Transient: the create worker may not have committed yet.
			// The worker framework will retry with backoff.
			return fmt.Errorf("webhook: payment not found for provider_payment_id=%s (transient): %w",
				event.ProviderPaymentID, ErrProviderTransient)
		}
		return fmt.Errorf("webhook: lock payment: %w", err)
	}

	// Update event row with resolved payment_id.
	eventRow.PaymentID = payment.ID

	// Forward-only transition.
	newStatus, skip, err := s.resolveTransition(ctx, prov, payment, event)
	if err != nil {
		return fmt.Errorf("webhook: resolve transition: %w", err)
	}

	if !skip {
		payment.Status = newStatus
		if err := s.store.UpdatePayment(ctx, tx, payment, ""); err != nil {
			return fmt.Errorf("webhook: update payment: %w", err)
		}
	}

	if err := s.store.MarkIngressVerified(ctx, tx, ingress.ID, event.ProviderEventID); err != nil {
		return fmt.Errorf("webhook: mark ingress verified: %w", err)
	}

	if !skip {
		if et := successEventFor(OpCreate, newStatus); et != "" {
			if err := s.enqueueEvent(ctx, tx, env, et, payment); err != nil {
				return fmt.Errorf("webhook: enqueue event: %w", err)
			}
		}
	}

	s.logger.Info("webhook processed",
		"ingress_id", ingress.ID,
		"provider", ingress.Provider,
		"provider_event_id", event.ProviderEventID,
		"payment_id", payment.ID,
		"new_status", newStatus,
		"skipped_transition", skip,
	)
	return nil
}

// resolveTransition applies forward-only logic. Returns (newStatus, skip, err).
// skip=true means the transition should not be applied (already at or past
// the event's status).
//
// If the incoming status has the same rank as the current status but different
// value (ambiguous terminal state), the function calls GetPaymentStatus for
// confirmation. For the fake provider this is instant; for real providers this
// hits the network — see the TEMPORARY MODEL caveat in Handle.
func (s *WebhookService) resolveTransition(
	ctx context.Context,
	prov Provider,
	payment *PaymentRow,
	event WebhookEvent,
) (Status, bool, error) {
	incomingRank := statusRank[event.CanonicalStatus]
	currentRank := statusRank[payment.Status]

	// Fast path: incoming rank is strictly higher → accept.
	if incomingRank > currentRank {
		return event.CanonicalStatus, false, nil
	}

	// Already at or past this status — no regression.
	if incomingRank < currentRank {
		return payment.Status, true, nil
	}

	// Same rank and same status: idempotent re-delivery of an event already
	// applied. Skip without a provider lookup.
	if event.CanonicalStatus == payment.Status {
		return payment.Status, true, nil
	}

	// Same rank, different status — ambiguous (e.g., payment is "cancelled"
	// but webhook says "failed"). Consult the provider to determine truth.
	lookupResult, err := prov.GetPaymentStatus(ctx, event.ProviderPaymentID)
	if err != nil {
		// Provider lookup failed transiently — don't commit ambiguous state.
		return "", false, fmt.Errorf("ambiguous status, provider lookup failed (transient): %w",
			ErrProviderTransient)
	}
	authoritative := lookupResult.Status
	if authoritative == payment.Status || statusRank[authoritative] <= currentRank {
		return payment.Status, true, nil // provider agrees we're already there
	}
	return authoritative, false, nil
}

func (s *WebhookService) enqueueEvent(ctx context.Context, tx pgx.Tx, parent *message.Envelope, eventType string, row *PaymentRow) error {
	payload := map[string]any{
		"payment_id":          row.ID,
		"order_id":            row.OrderID,
		"provider":            row.Provider,
		"provider_payment_id": row.ProviderPaymentID,
		"status":              row.Status,
		"amount_cents":        row.AmountCents,
		"currency":            row.Currency,
	}
	env, err := message.New(eventType, "payment", row.ID, payload)
	if err != nil {
		return err
	}
	env.CorrelationID = parent.CorrelationID
	cause := parent.MessageID
	env.CausationID = &cause
	return s.sink.Emit(ctx, tx, QueueEvents, env)
}

func truncateReason(s string) string {
	if len(s) > 500 {
		return s[:500]
	}
	return s
}
