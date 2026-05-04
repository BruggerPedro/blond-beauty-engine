package fiscal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"

	"github.com/blondbeauty/blond-beauty-engine/internal/message"
	"github.com/blondbeauty/blond-beauty-engine/internal/workers"
)

// Service is the fiscal orchestrator. Constructed once at startup; shared
// across workers. No provider-specific imports belong here.
type Service struct {
	logger   *slog.Logger
	provider Provider
	store    Store
	sink     EventSink
}

func NewService(logger *slog.Logger, prov Provider, store Store, sink EventSink) *Service {
	return &Service{
		logger:   logger.With("component", "fiscal"),
		provider: prov,
		store:    store,
		sink:     sink,
	}
}

// ---- payload types -------------------------------------------------------

// IssuePayload is the shape of fiscal.issue.requested / fiscal.retry.requested.
type IssuePayload struct {
	FiscalInvoiceID string `json:"fiscal_invoice_id"`
}

// CancelPayload is the shape of fiscal.cancel.requested.
type CancelPayload struct {
	FiscalInvoiceID string `json:"fiscal_invoice_id"`
	Reason          string `json:"reason,omitempty"`
}

// ---- handlers ------------------------------------------------------------

// HandleIssue processes fiscal.issue.requested for a NEW (pending) invoice.
func (s *Service) HandleIssue(ctx context.Context, tx pgx.Tx, env *message.Envelope, p IssuePayload) error {
	return s.performIssue(ctx, tx, env, p.FiscalInvoiceID, false)
}

// HandleRetry processes fiscal.retry.requested for a previously REJECTED invoice.
func (s *Service) HandleRetry(ctx context.Context, tx pgx.Tx, env *message.Envelope, p IssuePayload) error {
	return s.performIssue(ctx, tx, env, p.FiscalInvoiceID, true)
}

// HandleCancel processes fiscal.cancel.requested.
func (s *Service) HandleCancel(ctx context.Context, tx pgx.Tx, env *message.Envelope, p CancelPayload) error {
	if p.FiscalInvoiceID == "" {
		return workers.Permanent(fmt.Errorf("fiscal: fiscal_invoice_id missing in cancel payload"))
	}

	inv, err := s.store.LockInvoice(ctx, tx, p.FiscalInvoiceID)
	if err != nil {
		if errors.Is(err, ErrInvoiceNotFound) {
			return workers.Permanent(err)
		}
		return err
	}

	// Idempotency: already cancelled.
	if inv.Status == StatusCancelled {
		s.logger.Info("fiscal invoice already cancelled; skipping",
			"fiscal_invoice_id", inv.ID,
			"order_id", inv.OrderID,
		)
		return nil
	}

	// Cannot cancel a pending/rejected invoice via provider — just void it locally.
	if inv.Status == StatusPending || inv.Status == StatusRejected || inv.Status == StatusIssuing {
		inv.Status = StatusCancelled
		inv.AttemptCount++
		if err := s.store.UpdateInvoice(ctx, tx, inv); err != nil {
			return err
		}
		if err := s.insertEvent(ctx, tx, inv, EventCancelled, map[string]any{
			"reason":         p.Reason,
			"voided_locally": true,
		}); err != nil {
			return err
		}
		return s.emitCancelled(ctx, tx, env, inv)
	}

	// Status is authorized: must call provider to cancel at SEFAZ.
	if inv.Status != StatusAuthorized {
		return workers.Permanent(fmt.Errorf("%w: cannot cancel invoice with status %q",
			ErrIllegalTransition, inv.Status))
	}

	idemKey := buildIdemKey(env.MessageID, inv.ID, "cancel")

	// TEMPORARY MODEL — see service.go comment in fiscal package.
	_, cancelErr := s.provider.CancelInvoice(ctx, CancelRequest{
		InvoiceID:      inv.ID,
		AccessKey:      inv.AccessKey,
		IdempotencyKey: idemKey,
		Reason:         p.Reason,
	})

	inv.AttemptCount++

	if cancelErr != nil {
		inv.LastError = truncate(cancelErr.Error(), 500)
		if errors.Is(cancelErr, ErrProviderTransient) {
			_ = s.store.UpdateInvoice(ctx, tx, inv)
			return cancelErr
		}
		// Permanent cancellation failure: log and DLQ — the invoice remains authorized.
		_ = s.store.UpdateInvoice(ctx, tx, inv)
		return workers.Permanent(cancelErr)
	}

	inv.Status = StatusCancelled
	inv.LastError = ""
	if err := s.store.UpdateInvoice(ctx, tx, inv); err != nil {
		return err
	}
	if err := s.insertEvent(ctx, tx, inv, EventCancelled, map[string]any{
		"reason": p.Reason,
	}); err != nil {
		return err
	}
	s.logger.Info("fiscal invoice cancelled",
		"fiscal_invoice_id", inv.ID,
		"order_id", inv.OrderID,
		"provider", inv.Provider,
	)
	return s.emitCancelled(ctx, tx, env, inv)
}

// ---- shared issue pipeline -----------------------------------------------

// performIssue is shared by HandleIssue (isRetry=false) and HandleRetry (isRetry=true).
//
// Pipeline (within the worker's transaction):
//  1. Validate payload.
//  2. Lock fiscal_invoices row FOR UPDATE.
//  3. Guard: already authorized → ack (idempotent).
//  4. Guard: cancelled → permanent error.
//  5. Guard: for non-retry path, reject if status != pending/issuing.
//  6. Guard: for retry path, reject if status != rejected.
//  7. Call provider.IssueInvoice — TEMPORARY MODEL caveat below.
//  8. Persist result (UpdateInvoice).
//  9. Insert fiscal_invoice_events row.
//  10. On authorization:
//     a. Update email_messages.template_data if NFeEmailMessageID is set.
//     b. Emit fiscal.authorized.
//     c. Emit email.send.requested (if NFeEmailMessageID set).
//     d. Emit order.fulfillment.requested — fulfillment gating enforced here.
//  11. On rejection: emit fiscal.rejected. NO fulfillment event.
//
// Security invariants:
//   - AccessKey and Protocol are public; safe to log.
//   - XMLStorageKey and DANFEStorageKey are opaque refs; never logged.
//   - RejectionMessage is truncated to 500 chars before storage/logging.
//   - No CPF, CNPJ, customer data, certificate, or raw XML is handled here.
//
// TEMPORARY MODEL — acceptable only while using the fake provider for local
// development. The provider.IssueInvoice call happens inside the DB transaction
// that holds the FOR UPDATE row lock on fiscal_invoices. For the fake provider
// this is instant and harmless. Before wiring any real SEFAZ adapter, refactor
// to a two-phase model:
//  1. Short tx: set status = 'issuing', increment attempt_count.
//  2. External SEFAZ call outside any DB transaction.
//  3. Short tx: set status = 'authorized'/'rejected' + persist result + emit events.
//
// The idempotency_key unique constraint + provider dedup key together guarantee
// no double-issuance across retries without a long-held row lock.
func (s *Service) performIssue(
	ctx context.Context,
	tx pgx.Tx,
	env *message.Envelope,
	invoiceID string,
	isRetry bool,
) error {
	if invoiceID == "" {
		return workers.Permanent(fmt.Errorf("fiscal: fiscal_invoice_id missing"))
	}

	inv, err := s.store.LockInvoice(ctx, tx, invoiceID)
	if err != nil {
		if errors.Is(err, ErrInvoiceNotFound) {
			return workers.Permanent(err)
		}
		return err
	}

	// Idempotency: already authorized by a previous delivery.
	if inv.Status == StatusAuthorized {
		s.logger.Info("fiscal invoice already authorized; skipping",
			"fiscal_invoice_id", inv.ID,
			"order_id", inv.OrderID,
			"access_key", inv.AccessKey, // public — safe to log
		)
		return nil
	}

	// Cannot issue/retry a cancelled invoice.
	if inv.Status == StatusCancelled {
		return workers.Permanent(fmt.Errorf("%w: invoice %s is cancelled",
			ErrIllegalTransition, inv.ID))
	}

	// Status gating per operation.
	if isRetry {
		if inv.Status != StatusRejected {
			return workers.Permanent(fmt.Errorf("%w: retry requires rejected status, got %q",
				ErrIllegalTransition, inv.Status))
		}
	} else {
		if inv.Status != StatusPending && inv.Status != StatusIssuing {
			return workers.Permanent(fmt.Errorf("%w: issue requires pending/issuing status, got %q",
				ErrIllegalTransition, inv.Status))
		}
	}

	idemKey := buildIdemKey(env.MessageID, inv.ID, "issue")

	result, rejection, provErr := s.provider.IssueInvoice(ctx, IssueRequest{
		InvoiceID:      inv.ID,
		OrderID:        inv.OrderID,
		PaymentID:      inv.PaymentID,
		Model:          inv.Model,
		Series:         inv.Series,
		IdempotencyKey: idemKey,
		TotalCents:     inv.TotalCents,
		Currency:       inv.Currency,
	})

	inv.AttemptCount++

	if provErr != nil {
		inv.LastError = truncate(provErr.Error(), 500)

		if errors.Is(provErr, ErrProviderRejected) {
			inv.Status = StatusRejected
			inv.RejectionCode = rejection.Code
			inv.RejectionMessage = truncate(rejection.Message, 500)
			_ = s.store.UpdateInvoice(ctx, tx, inv)
			_ = s.insertEvent(ctx, tx, inv, EventRejected, map[string]any{
				"rejection_code":    rejection.Code,
				"rejection_message": rejection.Message,
			})
			_ = s.emitRejected(ctx, tx, env, inv)
			return workers.Permanent(provErr)
		}

		// Transient: persist attempt_count + last_error, retry later.
		_ = s.store.UpdateInvoice(ctx, tx, inv)
		return provErr
	}

	// Authorization succeeded.
	inv.Status = StatusAuthorized
	inv.AccessKey = result.AccessKey
	inv.Protocol = result.Protocol
	inv.Number = result.Number
	inv.XMLStorageKey = result.XMLStorageKey
	inv.DANFEStorageKey = result.DANFEStorageKey
	inv.RejectionCode = ""
	inv.RejectionMessage = ""
	inv.LastError = ""

	if err := s.store.UpdateInvoice(ctx, tx, inv); err != nil {
		return err
	}
	if err := s.insertEvent(ctx, tx, inv, EventAuthorized, map[string]any{
		"access_key": inv.AccessKey, // public — safe
		"protocol":   inv.Protocol,
		"number":     inv.Number,
	}); err != nil {
		return err
	}

	// Populate the pre-created email_messages row and request email delivery.
	if inv.NFeEmailMessageID != "" {
		emailData := NFeEmailTemplateData{
			OrderID:   inv.OrderID,
			AccessKey: inv.AccessKey,
			DANFELink: inv.DANFEStorageKey, // fake: key IS the link; production must sign
			XMLLink:   inv.XMLStorageKey,   // fake: key IS the link; production must sign
		}
		if err := s.store.UpdateEmailTemplateData(ctx, tx, inv.NFeEmailMessageID, emailData); err != nil {
			return err
		}
		if err := s.emitEmailSendRequested(ctx, tx, env, inv); err != nil {
			return err
		}
	}

	// Emit fiscal.authorized before fulfillment so downstream can order events correctly.
	if err := s.emitAuthorized(ctx, tx, env, inv); err != nil {
		return err
	}

	// FULFILLMENT GATE: emit order.fulfillment.requested ONLY after authorization.
	if err := s.emitFulfillmentRequested(ctx, tx, env, inv); err != nil {
		return err
	}

	s.logger.Info("fiscal invoice authorized",
		"fiscal_invoice_id", inv.ID,
		"order_id", inv.OrderID,
		"access_key", inv.AccessKey, // public
		"provider", inv.Provider,
		"number", inv.Number,
	)
	return nil
}

// ---- event emitters ------------------------------------------------------

func (s *Service) emitAuthorized(ctx context.Context, tx pgx.Tx, parent *message.Envelope, inv *InvoiceRow) error {
	env, err := message.New(EventAuthorized, "fiscal_invoice", inv.ID, map[string]any{
		"fiscal_invoice_id": inv.ID,
		"order_id":          inv.OrderID,
		"payment_id":        inv.PaymentID,
		"model":             inv.Model,
		"access_key":        inv.AccessKey, // public
		"protocol":          inv.Protocol,
		"number":            inv.Number,
		"provider":          inv.Provider,
		// xml_storage_key and danfe_storage_key are INTENTIONALLY omitted from
		// events — they are opaque internal refs that must be converted to
		// signed URLs by the storage layer before sharing externally.
	})
	if err != nil {
		return err
	}
	chain(env, parent)
	return s.sink.Emit(ctx, tx, QueueEvents, env)
}

func (s *Service) emitRejected(ctx context.Context, tx pgx.Tx, parent *message.Envelope, inv *InvoiceRow) error {
	env, err := message.New(EventRejected, "fiscal_invoice", inv.ID, map[string]any{
		"fiscal_invoice_id": inv.ID,
		"order_id":          inv.OrderID,
		"payment_id":        inv.PaymentID,
		"rejection_code":    inv.RejectionCode,
		"rejection_message": inv.RejectionMessage, // truncated ≤500; no raw XML
		"provider":          inv.Provider,
	})
	if err != nil {
		return err
	}
	chain(env, parent)
	return s.sink.Emit(ctx, tx, QueueEvents, env)
}

func (s *Service) emitCancelled(ctx context.Context, tx pgx.Tx, parent *message.Envelope, inv *InvoiceRow) error {
	env, err := message.New(EventCancelled, "fiscal_invoice", inv.ID, map[string]any{
		"fiscal_invoice_id": inv.ID,
		"order_id":          inv.OrderID,
		"payment_id":        inv.PaymentID,
		"provider":          inv.Provider,
	})
	if err != nil {
		return err
	}
	chain(env, parent)
	return s.sink.Emit(ctx, tx, QueueEvents, env)
}

func (s *Service) emitEmailSendRequested(ctx context.Context, tx pgx.Tx, parent *message.Envelope, inv *InvoiceRow) error {
	env, err := message.New(EventEmailSendRequested, "email_message", inv.NFeEmailMessageID, map[string]any{
		"email_message_id": inv.NFeEmailMessageID,
	})
	if err != nil {
		return err
	}
	chain(env, parent)
	return s.sink.Emit(ctx, tx, QueueEmailTransactional, env)
}

func (s *Service) emitFulfillmentRequested(ctx context.Context, tx pgx.Tx, parent *message.Envelope, inv *InvoiceRow) error {
	env, err := message.New(EventOrderFulfillRequested, "order", inv.OrderID, map[string]any{
		"order_id":          inv.OrderID,
		"fiscal_invoice_id": inv.ID,
		"access_key":        inv.AccessKey, // public
	})
	if err != nil {
		return err
	}
	chain(env, parent)
	return s.sink.Emit(ctx, tx, QueueOrderFulfillment, env)
}

// ---- helpers -------------------------------------------------------------

func (s *Service) insertEvent(ctx context.Context, tx pgx.Tx, inv *InvoiceRow, eventType string, payload map[string]any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal fiscal event payload: %w", err)
	}
	return s.store.InsertInvoiceEvent(ctx, tx, &InvoiceEventRow{
		FiscalInvoiceID: inv.ID,
		EventType:       eventType,
		Payload:         raw,
	})
}

// chain wires correlation_id and causation_id from a parent envelope.
func chain(child *message.Envelope, parent *message.Envelope) {
	child.CorrelationID = parent.CorrelationID
	cause := parent.MessageID
	child.CausationID = &cause
}

// buildIdemKey derives a stable, opaque key for (message, invoice, op).
func buildIdemKey(messageID, invoiceID, op string) string {
	return messageID + "|" + invoiceID + "|" + op
}
