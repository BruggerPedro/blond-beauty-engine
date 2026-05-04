package email

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

// EventSink emits a follow-up event onto the caller's transaction.
// Production wires this to outbox.Enqueue; tests use a capturing sink.
type EventSink interface {
	Emit(ctx context.Context, tx pgx.Tx, queue string, env *message.Envelope) error
}

// SendPayload is the shape of the email.send.requested envelope payload.
// Only the message ID is on the wire; all sensitive data lives in email_messages.
type SendPayload struct {
	EmailMessageID string `json:"email_message_id"`
}

// Service is the email orchestrator. It is constructed once at startup and
// shared across workers. No provider-specific imports belong here.
type Service struct {
	logger   *slog.Logger
	provider Provider
	store    Store
	registry *Registry
	sink     EventSink
}

func NewService(logger *slog.Logger, prov Provider, store Store, reg *Registry, sink EventSink) *Service {
	return &Service{
		logger:   logger.With("component", "email"),
		provider: prov,
		store:    store,
		registry: reg,
		sink:     sink,
	}
}

// Handler returns a workers.Handler that decodes the payload and calls Handle.
func (s *Service) Handler() func(context.Context, pgx.Tx, *message.Envelope) error {
	return func(ctx context.Context, tx pgx.Tx, env *message.Envelope) error {
		return s.Handle(ctx, tx, env)
	}
}

// Handle processes a single email.send.requested envelope.
//
// Pipeline (within the worker's transaction):
//  1. Decode payload → email_message_id.
//  2. Lock email_messages row (FOR UPDATE).
//  3. Guard: status already 'sent' → ack (idempotent re-delivery).
//  4. Render template from template_data.
//  5. Call provider.Send — see TEMPORARY MODEL caveat below.
//  6. Update status ('sent' or 'failed') + provider_message_id.
//  7. Emit email.sent or email.failed follow-up event.
//
// Security invariants enforced here:
//   - m.Recipient is NEVER passed to slog — only RecipientHash appears in logs.
//   - m.TemplateData is NEVER logged — may contain PII, reset links, NF-e URLs.
//   - The rendered TextBody is NEVER logged for the same reason.
func (s *Service) Handle(ctx context.Context, tx pgx.Tx, env *message.Envelope) error {
	var p SendPayload
	if len(env.Payload) > 0 {
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return workers.Permanent(fmt.Errorf("email: decode payload: %w", err))
		}
	}
	if p.EmailMessageID == "" {
		return workers.Permanent(fmt.Errorf("email: email_message_id missing"))
	}

	m, err := s.store.LockMessage(ctx, tx, p.EmailMessageID)
	if err != nil {
		if errors.Is(err, ErrMessageNotFound) || errors.Is(err, ErrInvalidRecipient) {
			return workers.Permanent(err)
		}
		return err // transient DB error → retry
	}

	// Idempotency guard: already sent by a previous delivery of this message.
	if m.Status == StatusSent {
		s.logger.Info("email already sent; skipping",
			"email_message_id", m.ID,
			"email_type", m.EmailType,
			"recipient_hash", m.RecipientHash,
		)
		return nil
	}

	rendered, renderErr := s.registry.Render(m.EmailType, m.TemplateData)
	if renderErr != nil {
		// Template/data errors are permanent; persist failure before DLQ.
		m.Status = StatusFailed
		m.LastError = truncate(renderErr.Error(), 500)
		m.AttemptCount++
		_ = s.store.UpdateMessage(ctx, tx, m)
		_ = s.emitFailed(ctx, tx, env, m, renderErr.Error())
		return workers.Permanent(renderErr)
	}
	if rendered.Subject != "" {
		m.Subject = rendered.Subject
	}

	// TEMPORARY MODEL — acceptable only while using the fake provider for
	// local development. The provider.Send call happens inside the DB
	// transaction that holds the FOR UPDATE row lock on email_messages.
	// For the fake provider this is instant and harmless. Before wiring any
	// real SMTP/SES/Resend adapter, refactor to a two-phase model:
	//   1. Short tx: set status = 'sending', increment attempt_count.
	//   2. External send call outside any DB transaction.
	//   3. Short tx: set status = 'sent'/'failed' + provider_message_id.
	// The idempotency_key unique constraint + provider dedup key together
	// guarantee no double-send across retries without a long-held row lock.
	result, sendErr := s.provider.Send(ctx, Message{
		To:             m.Recipient, // used for send only; NEVER logged
		Subject:        m.Subject,
		TextBody:       rendered.TextBody,
		IdempotencyKey: m.IdempotencyKey,
	})

	m.AttemptCount++
	m.Provider = s.provider.Name()

	if sendErr != nil {
		m.LastError = truncate(sendErr.Error(), 500)
		if errors.Is(sendErr, ErrProviderPermanent) {
			m.Status = StatusFailed
			_ = s.store.UpdateMessage(ctx, tx, m)
			_ = s.emitFailed(ctx, tx, env, m, sendErr.Error())
			return workers.Permanent(sendErr)
		}
		_ = s.store.UpdateMessage(ctx, tx, m)
		return sendErr // transient → worker retries with backoff
	}

	m.Status = StatusSent
	m.ProviderMessageID = result.ProviderMessageID
	m.LastError = ""
	if err := s.store.UpdateMessage(ctx, tx, m); err != nil {
		return err
	}
	if err := s.emitSent(ctx, tx, env, m); err != nil {
		return err
	}

	// Only recipient_hash and non-sensitive fields are logged here.
	s.logger.Info("email sent",
		"email_message_id", m.ID,
		"email_type", m.EmailType,
		"recipient_hash", m.RecipientHash,
		"provider", m.Provider,
	)
	return nil
}

func (s *Service) emitSent(ctx context.Context, tx pgx.Tx, parent *message.Envelope, m *MessageRow) error {
	env, err := message.New(EventEmailSent, "email_message", m.ID, map[string]any{
		"email_message_id":    m.ID,
		"email_type":          string(m.EmailType),
		"recipient_hash":      m.RecipientHash,
		"provider":            m.Provider,
		"provider_message_id": m.ProviderMessageID,
	})
	if err != nil {
		return err
	}
	env.CorrelationID = parent.CorrelationID
	cause := parent.MessageID
	env.CausationID = &cause
	return s.sink.Emit(ctx, tx, QueueEvents, env)
}

func (s *Service) emitFailed(ctx context.Context, tx pgx.Tx, parent *message.Envelope, m *MessageRow, reason string) error {
	env, err := message.New(EventEmailFailed, "email_message", m.ID, map[string]any{
		"email_message_id": m.ID,
		"email_type":       string(m.EmailType),
		"recipient_hash":   m.RecipientHash,
		"error":            truncate(reason, 200), // never includes raw template data
	})
	if err != nil {
		return err
	}
	env.CorrelationID = parent.CorrelationID
	cause := parent.MessageID
	env.CausationID = &cause
	return s.sink.Emit(ctx, tx, QueueEvents, env)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
