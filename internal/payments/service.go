package payments

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/blondbeauty/blond-beauty-engine/internal/message"
	"github.com/blondbeauty/blond-beauty-engine/internal/workers"
)

// Follow-up event types and the queues they're enqueued onto.
const (
	EventAuthorized       = "payment.authorized"
	EventCaptured         = "payment.captured"
	EventFailed           = "payment.failed"
	EventRefunded         = "payment.refunded"
	EventCancelled        = "payment.cancelled"
	EventChargebackOpened = "payment.chargeback_opened"
	EventRequiresAction   = "payment.requires_action"

	// Default queue for follow-up events that downstream workers (email,
	// fiscal, fulfillment, ...) subscribe to. Keep in sync with the API and
	// later slices.
	QueueEvents = "payments.events"
)

// EventSink emits a follow-up event onto the caller's transaction. The
// production sink writes to outbox_messages so the outbox publisher delivers
// to RabbitMQ with publisher confirms; tests use a capturing sink.
type EventSink interface {
	Emit(ctx context.Context, tx pgx.Tx, queue string, env *message.Envelope) error
}

// Service is the provider-agnostic orchestrator for the six payment
// operations. It is constructed once at startup and shared across workers.
type Service struct {
	logger   *slog.Logger
	registry *Registry
	store    Store
	sink     EventSink
}

func NewService(logger *slog.Logger, reg *Registry, store Store, sink EventSink) *Service {
	return &Service{
		logger:   logger.With("component", "payments"),
		registry: reg,
		store:    store,
		sink:     sink,
	}
}

// HandleCreate processes a `payment.create.requested` envelope. The payload
// must contain at least {"payment_id": "<uuid>"} and an optional method.
type CreatePayload struct {
	PaymentID string         `json:"payment_id"`
	Method    string         `json:"method,omitempty"`
	ReturnURL string         `json:"return_url,omitempty"`
	Metadata  map[string]any `json:"metadata,omitempty"`
}

func (s *Service) HandleCreate(ctx context.Context, tx pgx.Tx, env *message.Envelope, payload CreatePayload) error {
	return s.run(ctx, tx, env, OpCreate, payload.PaymentID, func(prov Provider, p *PaymentRow, requestID string) (Result, error) {
		return prov.CreatePayment(ctx, CreateRequest{
			Ref:       refFromRow(p, requestID),
			Method:    payload.Method,
			ReturnURL: payload.ReturnURL,
			Metadata:  payload.Metadata,
		})
	})
}

type RefByPayment struct {
	PaymentID string `json:"payment_id"`
}

func (s *Service) HandleConfirm(ctx context.Context, tx pgx.Tx, env *message.Envelope, p RefByPayment) error {
	return s.run(ctx, tx, env, OpConfirm, p.PaymentID, func(prov Provider, row *PaymentRow, requestID string) (Result, error) {
		return prov.ConfirmPayment(ctx, ConfirmRequest{Ref: refFromRow(row, requestID)})
	})
}

type CapturePayload struct {
	PaymentID   string `json:"payment_id"`
	AmountCents int64  `json:"amount_cents,omitempty"` // 0 → full
}

func (s *Service) HandleCapture(ctx context.Context, tx pgx.Tx, env *message.Envelope, payload CapturePayload) error {
	return s.run(ctx, tx, env, OpCapture, payload.PaymentID, func(prov Provider, row *PaymentRow, requestID string) (Result, error) {
		amount := Money{AmountCents: payload.AmountCents, Currency: row.Currency}
		if amount.AmountCents <= 0 {
			amount.AmountCents = row.AmountCents
		}
		if amount.AmountCents > row.AmountCents {
			return Result{}, fmt.Errorf("%w: capture amount exceeds authorized", ErrIllegalTransition)
		}
		return prov.CapturePayment(ctx, CaptureRequest{Ref: refFromRow(row, requestID), Amount: amount})
	})
}

type CancelPayload struct {
	PaymentID string `json:"payment_id"`
	Reason    string `json:"reason,omitempty"`
}

func (s *Service) HandleCancel(ctx context.Context, tx pgx.Tx, env *message.Envelope, p CancelPayload) error {
	return s.run(ctx, tx, env, OpCancel, p.PaymentID, func(prov Provider, row *PaymentRow, requestID string) (Result, error) {
		return prov.CancelPayment(ctx, CancelRequest{Ref: refFromRow(row, requestID), Reason: p.Reason})
	})
}

type RefundPayload struct {
	PaymentID   string `json:"payment_id"`
	AmountCents int64  `json:"amount_cents,omitempty"` // 0 → full
	Reason      string `json:"reason,omitempty"`
}

func (s *Service) HandleRefund(ctx context.Context, tx pgx.Tx, env *message.Envelope, p RefundPayload) error {
	return s.run(ctx, tx, env, OpRefund, p.PaymentID, func(prov Provider, row *PaymentRow, requestID string) (Result, error) {
		amount := Money{AmountCents: p.AmountCents, Currency: row.Currency}
		if amount.AmountCents <= 0 {
			amount.AmountCents = row.AmountCents
		}
		if amount.AmountCents > row.AmountCents {
			return Result{}, fmt.Errorf("%w: refund amount exceeds payment", ErrIllegalTransition)
		}
		if amount.AmountCents < row.AmountCents && !providerCaps(prov).PartialRefund {
			return Result{}, fmt.Errorf("%w: provider %q does not support partial refund",
				ErrIllegalTransition, prov.Name())
		}
		return prov.RefundPayment(ctx, RefundRequest{Ref: refFromRow(row, requestID), Amount: amount, Reason: p.Reason})
	})
}

func (s *Service) HandleReconcile(ctx context.Context, tx pgx.Tx, env *message.Envelope, p RefByPayment) error {
	return s.run(ctx, tx, env, OpReconcile, p.PaymentID, func(prov Provider, row *PaymentRow, _ string) (Result, error) {
		if row.ProviderPaymentID == "" {
			return Result{}, fmt.Errorf("%w: cannot reconcile payment without provider_payment_id", ErrIllegalTransition)
		}
		return prov.GetPaymentStatus(ctx, row.ProviderPaymentID)
	})
}

// run is the common pipeline for every operation. It locks the payment row,
// invokes the provider call, persists an attempt row, updates the payment
// status, and enqueues a follow-up event onto outbox_messages — all on the
// caller-supplied tx so dedup + business state commit atomically.
//
// Error handling:
//   - ErrPaymentNotFound, ErrIllegalTransition, ErrUnknownProvider, and
//     ErrProviderRejected are wrapped in workers.Permanent → DLQ.
//   - ErrProviderTransient (and any unclassified error) is returned plain
//     so the worker framework retries with exponential backoff.
//
// We persist a `failed` AttemptRow even on transient errors so the audit
// trail records every provider call. The payment row itself is only moved to
// `failed` for permanent rejections; transient errors leave status untouched.
func (s *Service) run(
	ctx context.Context,
	tx pgx.Tx,
	env *message.Envelope,
	op Operation,
	paymentID string,
	call func(prov Provider, row *PaymentRow, requestID string) (Result, error),
) error {
	if paymentID == "" {
		return workers.Permanent(fmt.Errorf("%s: payment_id missing", op))
	}

	row, err := s.store.LockPayment(ctx, tx, paymentID)
	if err != nil {
		if errors.Is(err, ErrPaymentNotFound) {
			return workers.Permanent(err)
		}
		return err
	}
	if !canTransition(row.Status, op) {
		return workers.Permanent(fmt.Errorf("%w: %s from %s", ErrIllegalTransition, op, row.Status))
	}

	prov, err := s.registry.Get(row.Provider)
	if err != nil {
		return workers.Permanent(err)
	}

	requestID := buildRequestID(env.MessageID, paymentID, op)

	// TEMPORARY MODEL — acceptable only while using the fake provider for
	// local development. The provider call happens here, inside the DB
	// transaction that holds the FOR UPDATE row lock on `payments`. For the
	// fake provider this is instant and harmless, but real external providers
	// (Getnet, Stripe, …) introduce network latency that would hold the lock
	// for seconds, reducing throughput and risking lock-wait timeouts.
	//
	// BEFORE implementing the real Getnet/Santander adapter (or any live
	// provider), refactor this to a two-phase model:
	//   1. Short tx: claim/create an attempt row with status='pending'.
	//   2. External call outside any DB transaction.
	//   3. Short tx: update attempt + payment row + enqueue follow-up event.
	// The unique index on payment_attempts(payment_id, operation, request_id)
	// and the provider's own idempotency key together guarantee safety without
	// a long-held row lock.
	res, callErr := call(prov, row, requestID)

	attempt := &AttemptRow{
		ID:               uuid.NewString(),
		PaymentID:        row.ID,
		Operation:        op,
		RequestID:        requestID,
		Status:           "ok",
		CanonicalStatus:  res.Status,
		RedactedRequest:  res.RedactedRequest,
		RedactedResponse: res.RedactedResponse,
	}

	if callErr != nil {
		attempt.Status = "failed"
		attempt.ErrorMessage = callErr.Error()
		switch {
		case errors.Is(callErr, ErrProviderRejected):
			attempt.ErrorCode = "provider_rejected"
			attempt.CanonicalStatus = StatusFailed
			row.Status = StatusFailed
		case errors.Is(callErr, ErrProviderTransient):
			attempt.ErrorCode = "provider_transient"
		case errors.Is(callErr, ErrIllegalTransition):
			attempt.ErrorCode = "illegal_transition"
		default:
			attempt.ErrorCode = "unknown"
		}
	} else {
		if res.ProviderPaymentID != "" {
			row.ProviderPaymentID = res.ProviderPaymentID
		}
		if res.Status != "" && res.Status != StatusUnknown {
			row.Status = res.Status
		}
	}

	if err := s.store.InsertAttempt(ctx, tx, attempt); err != nil {
		return err
	}
	if err := s.store.UpdatePayment(ctx, tx, row, attempt.ErrorMessage); err != nil {
		return err
	}

	// On permanent failures we still emit a follow-up event so downstream
	// workers (email, fulfillment block) can react. Transient failures are
	// silent: nothing is committed-as-final until the next retry.
	if callErr == nil {
		if eventType := successEventFor(op, row.Status); eventType != "" {
			if err := s.enqueueEvent(ctx, tx, env, eventType, row); err != nil {
				return err
			}
		}
	} else if errors.Is(callErr, ErrProviderRejected) {
		if err := s.enqueueEvent(ctx, tx, env, EventFailed, row); err != nil {
			return err
		}
	}

	switch {
	case callErr == nil:
		return nil
	case errors.Is(callErr, ErrProviderRejected),
		errors.Is(callErr, ErrIllegalTransition),
		errors.Is(callErr, ErrUnknownProvider):
		return workers.Permanent(callErr)
	default:
		return callErr // transient → retry
	}
}

func (s *Service) enqueueEvent(ctx context.Context, tx pgx.Tx, parent *message.Envelope, eventType string, row *PaymentRow) error {
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

func refFromRow(p *PaymentRow, requestID string) PaymentRef {
	return PaymentRef{
		PaymentID:         p.ID,
		OrderID:           p.OrderID,
		ProviderPaymentID: p.ProviderPaymentID,
		IdempotencyKey:    requestID,
		Amount:            Money{AmountCents: p.AmountCents, Currency: p.Currency},
	}
}

// buildRequestID derives a deterministic, opaque idempotency key for a single
// (message, payment, operation) tuple. The same retry of the same message
// produces the same key, so a unique index on payment_attempts and the
// provider's own idempotency mechanism both guard against double-charge.
func buildRequestID(messageID, paymentID string, op Operation) string {
	h := sha256.Sum256([]byte(messageID + "|" + paymentID + "|" + string(op)))
	return hex.EncodeToString(h[:16])
}

func providerCaps(p Provider) Capabilities { return p.Capabilities() }

func canTransition(cur Status, op Operation) bool {
	switch op {
	case OpCreate:
		// Allowed from "pending-like" or unknown statuses set by the API.
		return cur == StatusUnknown || cur == "" || cur == StatusRequiresAction
	case OpConfirm:
		return cur == StatusRequiresAction || cur == StatusAuthorized
	case OpCapture:
		return cur == StatusAuthorized
	case OpCancel:
		return cur == StatusAuthorized || cur == StatusRequiresAction
	case OpRefund:
		return cur == StatusPaid || cur == StatusCaptured
	case OpReconcile:
		return true
	}
	return false
}

func successEventFor(op Operation, st Status) string {
	switch op {
	case OpCreate, OpConfirm:
		switch st {
		case StatusAuthorized:
			return EventAuthorized
		case StatusRequiresAction:
			return EventRequiresAction
		case StatusFailed:
			return EventFailed
		case StatusPaid:
			return EventCaptured
		}
	case OpCapture:
		if st == StatusPaid || st == StatusCaptured {
			return EventCaptured
		}
	case OpRefund:
		if st == StatusRefunded {
			return EventRefunded
		}
	case OpCancel:
		if st == StatusCancelled {
			return EventCancelled
		}
	case OpReconcile:
		// Reconcile updates state silently; event emission is left to
		// webhook handlers (Slice C). Avoid double-publishing here.
	}
	return ""
}
