package email

// Tests live in package email (white-box) to access unexported helpers and
// sentinel constants. The fake email provider is NOT imported here to avoid
// a potential import cycle (fake imports email). Instead, stubProvider and
// memStore implement the Provider and Store interfaces inline.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/blondbeauty/blond-beauty-engine/internal/message"
	"github.com/blondbeauty/blond-beauty-engine/internal/workers"
)

// ---- stub provider -------------------------------------------------------

type stubProvider struct {
	mu      sync.Mutex
	sent    []Message
	failErr error // if non-nil, Send returns this error
}

func (s *stubProvider) Name() string { return "stub-email" }

func (s *stubProvider) Send(_ context.Context, msg Message) (ProviderResult, error) {
	if s.failErr != nil {
		return ProviderResult{}, s.failErr
	}
	s.mu.Lock()
	s.sent = append(s.sent, msg)
	s.mu.Unlock()
	return ProviderResult{ProviderMessageID: "stub_msg_" + fmt.Sprint(len(s.sent))}, nil
}

func (s *stubProvider) getSent() []Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Message, len(s.sent))
	copy(out, s.sent)
	return out
}

// ---- in-memory store -----------------------------------------------------

type memStore struct {
	mu       sync.Mutex
	messages map[string]*MessageRow
	updateFn func(m *MessageRow) // optional hook for assertions
}

func newMemStore() *memStore {
	return &memStore{messages: map[string]*MessageRow{}}
}

func (ms *memStore) put(m *MessageRow) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	cp := *m
	ms.messages[m.ID] = &cp
}

func (ms *memStore) LockMessage(_ context.Context, _ pgx.Tx, id string) (*MessageRow, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	m, ok := ms.messages[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrMessageNotFound, id)
	}
	cp := *m
	return &cp, nil
}

func (ms *memStore) UpdateMessage(_ context.Context, _ pgx.Tx, m *MessageRow) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	cp := *m
	ms.messages[m.ID] = &cp
	if ms.updateFn != nil {
		ms.updateFn(&cp)
	}
	return nil
}

// ---- capturing EventSink -------------------------------------------------

type captureSink struct {
	mu     sync.Mutex
	events []*message.Envelope
}

func (c *captureSink) Emit(_ context.Context, _ pgx.Tx, _ string, env *message.Envelope) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, env)
	return nil
}

func (c *captureSink) drainByType(eventType string) []*message.Envelope {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []*message.Envelope
	for _, e := range c.events {
		if e.EventType == eventType {
			out = append(out, e)
		}
	}
	return out
}

// ---- helpers -------------------------------------------------------------

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// newEnvelope creates a minimal test envelope for email.send.requested.
func newEnvelope(emailMessageID string) *message.Envelope {
	payload, _ := json.Marshal(SendPayload{EmailMessageID: emailMessageID})
	env, _ := message.New("email.send.requested", "email_message", emailMessageID, nil)
	env.Payload = payload
	return env
}

// baseRow returns a valid MessageRow for a given email type and recipient.
func baseRow(id, emailType, recipient string, templateData any) *MessageRow {
	raw, _ := json.Marshal(templateData)
	return &MessageRow{
		ID:             id,
		EmailType:      EmailType(emailType),
		Recipient:      recipient,
		RecipientHash:  HashRecipient(recipient),
		Subject:        "",
		TemplateData:   json.RawMessage(raw),
		Status:         StatusQueued,
		IdempotencyKey: "idem-" + id,
	}
}

func buildService(prov Provider, store Store, sink EventSink) *Service {
	return NewService(discardLogger(), prov, store, NewRegistry(), sink)
}

// ---- test scenarios ------------------------------------------------------

// Scenario 1: successful send of a password_reset email.
func TestHandle_SuccessfulSend(t *testing.T) {
	prov := &stubProvider{}
	store := newMemStore()
	sink := &captureSink{}

	row := baseRow("msg-1", string(TypePasswordReset), "user@example.com",
		PasswordResetData{ResetURL: "https://app.example.com/reset?token=abc"})
	store.put(row)

	svc := buildService(prov, store, sink)
	err := svc.Handle(context.Background(), nil, newEnvelope("msg-1"))
	if err != nil {
		t.Fatalf("Handle returned error: %v", err)
	}

	// Status persisted as 'sent'.
	stored, _ := store.LockMessage(context.Background(), nil, "msg-1")
	if stored.Status != StatusSent {
		t.Errorf("status = %q, want %q", stored.Status, StatusSent)
	}
	if stored.ProviderMessageID == "" {
		t.Error("expected non-empty ProviderMessageID")
	}

	// email.sent event emitted.
	sent := sink.drainByType(EventEmailSent)
	if len(sent) != 1 {
		t.Fatalf("expected 1 email.sent event, got %d", len(sent))
	}
}

// Scenario 2: idempotent re-delivery — message already 'sent'.
func TestHandle_AlreadySent_Idempotent(t *testing.T) {
	prov := &stubProvider{}
	store := newMemStore()
	sink := &captureSink{}

	row := baseRow("msg-2", string(TypePasswordReset), "user@example.com",
		PasswordResetData{ResetURL: "https://example.com/r"})
	row.Status = StatusSent
	store.put(row)

	svc := buildService(prov, store, sink)
	err := svc.Handle(context.Background(), nil, newEnvelope("msg-2"))
	if err != nil {
		t.Fatalf("Handle returned error: %v", err)
	}

	// Provider must NOT have been called.
	if len(prov.getSent()) != 0 {
		t.Error("provider.Send must not be called for already-sent messages")
	}
	// No follow-up event emitted.
	if len(sink.events) != 0 {
		t.Error("no events should be emitted for idempotent ack")
	}
}

// Scenario 3: transient provider error → non-permanent error (worker will retry).
func TestHandle_TransientProviderError(t *testing.T) {
	prov := &stubProvider{failErr: fmt.Errorf("%w: upstream timeout", ErrProviderTransient)}
	store := newMemStore()
	sink := &captureSink{}

	row := baseRow("msg-3", string(TypePasswordReset), "user@example.com",
		PasswordResetData{ResetURL: "https://example.com/r"})
	store.put(row)

	svc := buildService(prov, store, sink)
	err := svc.Handle(context.Background(), nil, newEnvelope("msg-3"))
	if err == nil {
		t.Fatal("expected non-nil error for transient failure")
	}
	if workers.Classify(err) == workers.RetryPermanent {
		t.Error("transient error must not be classified as permanent")
	}

	// Status persisted (still queued/in-progress — attempt_count incremented).
	stored, _ := store.LockMessage(context.Background(), nil, "msg-3")
	if stored.Status == StatusSent {
		t.Error("status must not be 'sent' after transient failure")
	}
	// No email.failed event for transient errors.
	if len(sink.drainByType(EventEmailFailed)) != 0 {
		t.Error("email.failed must not be emitted for transient errors")
	}
}

// Scenario 4: permanent provider error → workers.Permanent wrapped, email.failed emitted.
func TestHandle_PermanentProviderError(t *testing.T) {
	prov := &stubProvider{failErr: fmt.Errorf("%w: address blocked", ErrProviderPermanent)}
	store := newMemStore()
	sink := &captureSink{}

	row := baseRow("msg-4", string(TypePasswordReset), "blocked+fail@example.com",
		PasswordResetData{ResetURL: "https://example.com/r"})
	store.put(row)

	svc := buildService(prov, store, sink)
	err := svc.Handle(context.Background(), nil, newEnvelope("msg-4"))
	if err == nil {
		t.Fatal("expected error for permanent failure")
	}

	var pe *workers.PermanentError
	if !errors.As(err, &pe) {
		t.Errorf("expected workers.PermanentError, got %T: %v", err, err)
	}

	stored, _ := store.LockMessage(context.Background(), nil, "msg-4")
	if stored.Status != StatusFailed {
		t.Errorf("status = %q, want %q", stored.Status, StatusFailed)
	}

	failed := sink.drainByType(EventEmailFailed)
	if len(failed) != 1 {
		t.Fatalf("expected 1 email.failed event, got %d", len(failed))
	}
}

// Scenario 5: missing template type → permanent error, email.failed emitted.
func TestHandle_MissingTemplate(t *testing.T) {
	prov := &stubProvider{}
	store := newMemStore()
	sink := &captureSink{}

	row := baseRow("msg-5", "unknown_email_type", "user@example.com", map[string]any{})
	store.put(row)

	svc := buildService(prov, store, sink)
	err := svc.Handle(context.Background(), nil, newEnvelope("msg-5"))
	if err == nil {
		t.Fatal("expected error for unknown template type")
	}

	var pe *workers.PermanentError
	if !errors.As(err, &pe) {
		t.Errorf("expected workers.PermanentError, got %T: %v", err, err)
	}

	stored, _ := store.LockMessage(context.Background(), nil, "msg-5")
	if stored.Status != StatusFailed {
		t.Errorf("status = %q, want %q", stored.Status, StatusFailed)
	}

	failed := sink.drainByType(EventEmailFailed)
	if len(failed) != 1 {
		t.Fatalf("expected 1 email.failed event, got %d", len(failed))
	}
}

// Scenario 6: message not found → permanent error.
func TestHandle_MessageNotFound(t *testing.T) {
	prov := &stubProvider{}
	store := newMemStore()
	sink := &captureSink{}

	svc := buildService(prov, store, sink)
	err := svc.Handle(context.Background(), nil, newEnvelope("nonexistent"))
	if err == nil {
		t.Fatal("expected error for missing message")
	}
	var pe *workers.PermanentError
	if !errors.As(err, &pe) {
		t.Errorf("expected workers.PermanentError, got %T: %v", err, err)
	}
}

// Scenario 7: missing email_message_id in payload → permanent error.
func TestHandle_MissingEmailMessageID(t *testing.T) {
	prov := &stubProvider{}
	store := newMemStore()
	sink := &captureSink{}

	env, _ := message.New("email.send.requested", "email_message", "x", nil)
	env.Payload = json.RawMessage(`{}`) // no email_message_id

	svc := buildService(prov, store, sink)
	err := svc.Handle(context.Background(), nil, env)
	if err == nil {
		t.Fatal("expected error for missing email_message_id")
	}
	var pe *workers.PermanentError
	if !errors.As(err, &pe) {
		t.Errorf("expected workers.PermanentError, got %T", err)
	}
}

// Scenario 8: privacy — recipient address must NOT appear in the message
// passed to the provider's Send (it does, but we verify the log path never
// sees it by ensuring the stub records To without leaking it into events).
// This test also checks that the email.sent event payload uses recipient_hash,
// not the raw recipient.
func TestHandle_EventPayload_UsesRecipientHash(t *testing.T) {
	prov := &stubProvider{}
	store := newMemStore()
	sink := &captureSink{}

	recipient := "private@example.com"
	row := baseRow("msg-8", string(TypePasswordReset), recipient,
		PasswordResetData{ResetURL: "https://example.com/r"})
	store.put(row)

	svc := buildService(prov, store, sink)
	_ = svc.Handle(context.Background(), nil, newEnvelope("msg-8"))

	events := sink.drainByType(EventEmailSent)
	if len(events) == 0 {
		t.Fatal("expected email.sent event")
	}

	var payload map[string]any
	if err := json.Unmarshal(events[0].Payload, &payload); err != nil {
		t.Fatalf("unmarshal event payload: %v", err)
	}

	// recipient_hash must be present.
	if _, ok := payload["recipient_hash"]; !ok {
		t.Error("event payload must contain recipient_hash")
	}
	// raw recipient must NOT appear in event payload.
	raw, _ := json.Marshal(payload)
	if strings.Contains(string(raw), recipient) {
		t.Errorf("raw recipient %q must not appear in email.sent event payload", recipient)
	}
}

// Scenario 9: email.sent event has correct causation / correlation chain.
func TestHandle_EventCorrelation(t *testing.T) {
	prov := &stubProvider{}
	store := newMemStore()
	sink := &captureSink{}

	row := baseRow("msg-9", string(TypePasswordReset), "user@example.com",
		PasswordResetData{ResetURL: "https://example.com/r"})
	store.put(row)

	svc := buildService(prov, store, sink)
	env := newEnvelope("msg-9")
	env.CorrelationID = "corr-abc"
	_ = svc.Handle(context.Background(), nil, env)

	events := sink.drainByType(EventEmailSent)
	if len(events) == 0 {
		t.Fatal("expected email.sent event")
	}
	if events[0].CorrelationID != "corr-abc" {
		t.Errorf("CorrelationID = %q, want %q", events[0].CorrelationID, "corr-abc")
	}
	if events[0].CausationID == nil || *events[0].CausationID != env.MessageID {
		t.Errorf("CausationID = %v, want %q", events[0].CausationID, env.MessageID)
	}
}

// Scenario 10: attempt_count is incremented on each Handle call regardless of outcome.
func TestHandle_AttemptCountIncremented(t *testing.T) {
	prov := &stubProvider{failErr: fmt.Errorf("%w: blip", ErrProviderTransient)}
	store := newMemStore()
	sink := &captureSink{}

	row := baseRow("msg-10", string(TypePasswordReset), "user@example.com",
		PasswordResetData{ResetURL: "https://example.com/r"})
	store.put(row)

	svc := buildService(prov, store, sink)
	// First attempt — transient failure.
	_ = svc.Handle(context.Background(), nil, newEnvelope("msg-10"))

	stored, _ := store.LockMessage(context.Background(), nil, "msg-10")
	if stored.AttemptCount != 1 {
		t.Errorf("AttemptCount = %d after first attempt, want 1", stored.AttemptCount)
	}
}
