package fiscal

// White-box tests: package fiscal, no fake import (avoids import cycle).
// The fake provider is NOT imported here; instead, a minimal stubProvider
// implements the Provider interface inline.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/blondbeauty/blond-beauty-engine/internal/message"
	"github.com/blondbeauty/blond-beauty-engine/internal/workers"
)

// ---- stub provider -------------------------------------------------------

type stubProvider struct {
	mu       sync.Mutex
	issueFn  func(IssueRequest) (IssueResult, RejectionDetail, error)
	cancelFn func(CancelRequest) (CancelResult, error)
	issued   []IssueRequest
}

func newStub() *stubProvider {
	p := &stubProvider{}
	// Default: authorize everything.
	p.issueFn = func(req IssueRequest) (IssueResult, RejectionDetail, error) {
		return IssueResult{
			AccessKey:       "FAKEKEY0000000000000000000000000000000000000",
			Protocol:        "proto_" + req.InvoiceID,
			Number:          "num_" + req.InvoiceID,
			XMLStorageKey:   "fake://xml/" + req.InvoiceID,
			DANFEStorageKey: "fake://danfe/" + req.InvoiceID,
		}, RejectionDetail{}, nil
	}
	p.cancelFn = func(_ CancelRequest) (CancelResult, error) {
		return CancelResult{Protocol: "cancel_proto"}, nil
	}
	return p
}

func (s *stubProvider) Name() string { return "stub-fiscal" }

func (s *stubProvider) IssueInvoice(_ context.Context, req IssueRequest) (IssueResult, RejectionDetail, error) {
	s.mu.Lock()
	s.issued = append(s.issued, req)
	s.mu.Unlock()
	return s.issueFn(req)
}

func (s *stubProvider) CancelInvoice(_ context.Context, req CancelRequest) (CancelResult, error) {
	return s.cancelFn(req)
}

// ---- in-memory store -----------------------------------------------------

type memStore struct {
	mu           sync.Mutex
	invoices     map[string]*InvoiceRow
	events       []*InvoiceEventRow
	emailUpdates map[string]NFeEmailTemplateData
}

func newMemStore() *memStore {
	return &memStore{
		invoices:     map[string]*InvoiceRow{},
		emailUpdates: map[string]NFeEmailTemplateData{},
	}
}

func (ms *memStore) put(inv *InvoiceRow) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	cp := *inv
	ms.invoices[inv.ID] = &cp
}

func (ms *memStore) get(id string) *InvoiceRow {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	inv, ok := ms.invoices[id]
	if !ok {
		return nil
	}
	cp := *inv
	return &cp
}

func (ms *memStore) LockInvoice(_ context.Context, _ pgx.Tx, id string) (*InvoiceRow, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	inv, ok := ms.invoices[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrInvoiceNotFound, id)
	}
	cp := *inv
	return &cp, nil
}

func (ms *memStore) UpdateInvoice(_ context.Context, _ pgx.Tx, inv *InvoiceRow) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	cp := *inv
	ms.invoices[inv.ID] = &cp
	return nil
}

func (ms *memStore) InsertInvoiceEvent(_ context.Context, _ pgx.Tx, ev *InvoiceEventRow) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	cp := *ev
	ms.events = append(ms.events, &cp)
	return nil
}

func (ms *memStore) UpdateEmailTemplateData(_ context.Context, _ pgx.Tx, emailMsgID string, data NFeEmailTemplateData) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.emailUpdates[emailMsgID] = data
	return nil
}

func (ms *memStore) eventsByType(t string) []*InvoiceEventRow {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	var out []*InvoiceEventRow
	for _, ev := range ms.events {
		if ev.EventType == t {
			out = append(out, ev)
		}
	}
	return out
}

// ---- capturing EventSink -------------------------------------------------

type captureSink struct {
	mu     sync.Mutex
	events []*message.Envelope
}

func (c *captureSink) Emit(_ context.Context, _ pgx.Tx, _ string, env *message.Envelope) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := *env
	c.events = append(c.events, &cp)
	return nil
}

func (c *captureSink) byType(eventType string) []*message.Envelope {
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

func buildSvc(prov Provider, store Store, sink EventSink) *Service {
	return NewService(discardLogger(), prov, store, sink)
}

func newIssueEnv(invoiceID string) *message.Envelope {
	payload, _ := json.Marshal(IssuePayload{FiscalInvoiceID: invoiceID})
	env, _ := message.New("fiscal.issue.requested", "fiscal_invoice", invoiceID, nil)
	env.Payload = payload
	env.CorrelationID = "corr-" + invoiceID
	return env
}

func newCancelEnv(invoiceID, reason string) *message.Envelope {
	payload, _ := json.Marshal(CancelPayload{FiscalInvoiceID: invoiceID, Reason: reason})
	env, _ := message.New("fiscal.cancel.requested", "fiscal_invoice", invoiceID, nil)
	env.Payload = payload
	env.CorrelationID = "corr-cancel-" + invoiceID
	return env
}

func baseInvoice(id string, totalCents int64) *InvoiceRow {
	return &InvoiceRow{
		ID:             id,
		OrderID:        "order-" + id,
		PaymentID:      "pay-" + id,
		Model:          ModelNFe,
		Series:         "1",
		Status:         StatusPending,
		IdempotencyKey: "idem-" + id,
		Provider:       "stub-fiscal",
		TotalCents:     totalCents,
		Currency:       "BRL",
	}
}

// ---- test scenarios ------------------------------------------------------

// Scenario 1: fiscal issue authorized — success path.
func TestHandleIssue_Authorized(t *testing.T) {
	prov := newStub()
	store := newMemStore()
	sink := &captureSink{}

	inv := baseInvoice("inv-1", 5000)
	store.put(inv)

	svc := buildSvc(prov, store, sink)
	env := newIssueEnv("inv-1")
	if err := svc.HandleIssue(context.Background(), nil, env, IssuePayload{FiscalInvoiceID: "inv-1"}); err != nil {
		t.Fatalf("HandleIssue returned error: %v", err)
	}

	stored := store.get("inv-1")
	if stored.Status != StatusAuthorized {
		t.Errorf("status = %q, want %q", stored.Status, StatusAuthorized)
	}
	if stored.AccessKey == "" {
		t.Error("expected non-empty AccessKey")
	}
	if stored.AttemptCount != 1 {
		t.Errorf("AttemptCount = %d, want 1", stored.AttemptCount)
	}
}

// Scenario 2: fiscal issue rejected — permanent error, fiscal.rejected emitted, NO fulfillment.
func TestHandleIssue_Rejected(t *testing.T) {
	prov := newStub()
	prov.issueFn = func(_ IssueRequest) (IssueResult, RejectionDetail, error) {
		return IssueResult{}, RejectionDetail{Code: "225", Message: "invalid data"},
			fmt.Errorf("%w: 225", ErrProviderRejected)
	}
	store := newMemStore()
	sink := &captureSink{}

	store.put(baseInvoice("inv-2", 5000))

	svc := buildSvc(prov, store, sink)
	err := svc.HandleIssue(context.Background(), nil, newIssueEnv("inv-2"), IssuePayload{FiscalInvoiceID: "inv-2"})
	if err == nil {
		t.Fatal("expected error for rejection")
	}
	var pe *workers.PermanentError
	if !errors.As(err, &pe) {
		t.Errorf("expected PermanentError, got %T: %v", err, err)
	}

	stored := store.get("inv-2")
	if stored.Status != StatusRejected {
		t.Errorf("status = %q, want %q", stored.Status, StatusRejected)
	}
	if stored.RejectionCode != "225" {
		t.Errorf("RejectionCode = %q, want %q", stored.RejectionCode, "225")
	}

	// fiscal.rejected emitted.
	if len(sink.byType(EventRejected)) != 1 {
		t.Errorf("expected 1 fiscal.rejected event, got %d", len(sink.byType(EventRejected)))
	}
	// NO fulfillment event.
	if len(sink.byType(EventOrderFulfillRequested)) != 0 {
		t.Error("fulfillment must NOT be emitted on rejected fiscal")
	}
}

// Scenario 3: retry after rejection — rejected → authorized.
func TestHandleRetry_AfterRejection(t *testing.T) {
	prov := newStub()
	store := newMemStore()
	sink := &captureSink{}

	inv := baseInvoice("inv-3", 5000)
	inv.Status = StatusRejected
	inv.RejectionCode = "225"
	store.put(inv)

	svc := buildSvc(prov, store, sink)
	err := svc.HandleRetry(context.Background(), nil, newIssueEnv("inv-3"), IssuePayload{FiscalInvoiceID: "inv-3"})
	if err != nil {
		t.Fatalf("HandleRetry returned error: %v", err)
	}

	stored := store.get("inv-3")
	if stored.Status != StatusAuthorized {
		t.Errorf("status = %q, want %q", stored.Status, StatusAuthorized)
	}
}

// Scenario 4: cancel authorized invoice — provider called, fiscal.cancelled emitted.
func TestHandleCancel_AuthorizedInvoice(t *testing.T) {
	prov := newStub()
	store := newMemStore()
	sink := &captureSink{}

	inv := baseInvoice("inv-4", 5000)
	inv.Status = StatusAuthorized
	inv.AccessKey = "FAKEKEY0000000000000000000000000000000000000"
	store.put(inv)

	svc := buildSvc(prov, store, sink)
	err := svc.HandleCancel(context.Background(), nil, newCancelEnv("inv-4", "customer request"), CancelPayload{
		FiscalInvoiceID: "inv-4",
		Reason:          "customer request",
	})
	if err != nil {
		t.Fatalf("HandleCancel returned error: %v", err)
	}

	stored := store.get("inv-4")
	if stored.Status != StatusCancelled {
		t.Errorf("status = %q, want %q", stored.Status, StatusCancelled)
	}
	if len(sink.byType(EventCancelled)) != 1 {
		t.Errorf("expected 1 fiscal.cancelled event, got %d", len(sink.byType(EventCancelled)))
	}
}

// Scenario 5: duplicate issue — already authorized → idempotent ack, no provider call.
func TestHandleIssue_AlreadyAuthorized_Idempotent(t *testing.T) {
	prov := newStub()
	store := newMemStore()
	sink := &captureSink{}

	inv := baseInvoice("inv-5", 5000)
	inv.Status = StatusAuthorized
	inv.AccessKey = "FAKEKEY0000000000000000000000000000000000000"
	store.put(inv)

	svc := buildSvc(prov, store, sink)
	err := svc.HandleIssue(context.Background(), nil, newIssueEnv("inv-5"), IssuePayload{FiscalInvoiceID: "inv-5"})
	if err != nil {
		t.Fatalf("HandleIssue returned error: %v", err)
	}

	// Provider must not have been called.
	prov.mu.Lock()
	calls := len(prov.issued)
	prov.mu.Unlock()
	if calls != 0 {
		t.Error("provider must not be called for already-authorized invoice")
	}
	// No events emitted.
	if len(sink.events) != 0 {
		t.Error("no events should be emitted for idempotent ack")
	}
}

// Scenario 6: repeated retry — already authorized → idempotent ack.
func TestHandleRetry_AlreadyAuthorized_Idempotent(t *testing.T) {
	prov := newStub()
	store := newMemStore()
	sink := &captureSink{}

	inv := baseInvoice("inv-6", 5000)
	inv.Status = StatusAuthorized
	store.put(inv)

	svc := buildSvc(prov, store, sink)
	err := svc.HandleRetry(context.Background(), nil, newIssueEnv("inv-6"), IssuePayload{FiscalInvoiceID: "inv-6"})
	if err != nil {
		t.Fatalf("HandleRetry returned error: %v", err)
	}
	if len(sink.events) != 0 {
		t.Error("no events should be emitted for idempotent retry ack")
	}
}

// Scenario 7: no fulfillment emitted on rejection — verified in Scenario 2.
// This extra test verifies the rejected state is also safe on cancel (no fulfillment gate bypass).
func TestHandleCancel_RejectedInvoice_NoFulfillment(t *testing.T) {
	prov := newStub()
	store := newMemStore()
	sink := &captureSink{}

	inv := baseInvoice("inv-7", 5000)
	inv.Status = StatusRejected
	store.put(inv)

	svc := buildSvc(prov, store, sink)
	err := svc.HandleCancel(context.Background(), nil, newCancelEnv("inv-7", "void"), CancelPayload{
		FiscalInvoiceID: "inv-7",
		Reason:          "void",
	})
	if err != nil {
		t.Fatalf("HandleCancel returned error: %v", err)
	}

	if len(sink.byType(EventOrderFulfillRequested)) != 0 {
		t.Error("fulfillment must NOT be emitted when cancelling a rejected invoice")
	}
	if len(sink.byType(EventCancelled)) != 1 {
		t.Error("expected fiscal.cancelled event")
	}
}

// Scenario 8: fulfillment emitted after authorized fiscal.
func TestHandleIssue_FulfillmentEmittedAfterAuth(t *testing.T) {
	prov := newStub()
	store := newMemStore()
	sink := &captureSink{}

	store.put(baseInvoice("inv-8", 5000))

	svc := buildSvc(prov, store, sink)
	if err := svc.HandleIssue(context.Background(), nil, newIssueEnv("inv-8"), IssuePayload{FiscalInvoiceID: "inv-8"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ff := sink.byType(EventOrderFulfillRequested)
	if len(ff) != 1 {
		t.Fatalf("expected 1 order.fulfillment.requested, got %d", len(ff))
	}
	var payload map[string]any
	_ = json.Unmarshal(ff[0].Payload, &payload)
	if payload["order_id"] != "order-inv-8" {
		t.Errorf("fulfillment payload order_id = %v, want %q", payload["order_id"], "order-inv-8")
	}
}

// Scenario 9: email.send.requested emitted after authorized fiscal when NFeEmailMessageID is set.
func TestHandleIssue_EmailSendRequestedAfterAuth(t *testing.T) {
	prov := newStub()
	store := newMemStore()
	sink := &captureSink{}

	inv := baseInvoice("inv-9", 5000)
	inv.NFeEmailMessageID = "email-msg-001"
	store.put(inv)

	svc := buildSvc(prov, store, sink)
	if err := svc.HandleIssue(context.Background(), nil, newIssueEnv("inv-9"), IssuePayload{FiscalInvoiceID: "inv-9"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	emails := sink.byType(EventEmailSendRequested)
	if len(emails) != 1 {
		t.Fatalf("expected 1 email.send.requested, got %d", len(emails))
	}
	var payload map[string]any
	_ = json.Unmarshal(emails[0].Payload, &payload)
	if payload["email_message_id"] != "email-msg-001" {
		t.Errorf("email_message_id = %v, want %q", payload["email_message_id"], "email-msg-001")
	}

	// email_messages.template_data should have been updated.
	data, ok := store.emailUpdates["email-msg-001"]
	if !ok {
		t.Fatal("UpdateEmailTemplateData was not called")
	}
	if data.AccessKey == "" {
		t.Error("template data AccessKey must be set")
	}
	if data.OrderID != "order-inv-9" {
		t.Errorf("template data OrderID = %q, want %q", data.OrderID, "order-inv-9")
	}
}

// Scenario 10: invoice not found → permanent error.
func TestHandleIssue_InvoiceNotFound(t *testing.T) {
	prov := newStub()
	store := newMemStore()
	sink := &captureSink{}

	svc := buildSvc(prov, store, sink)
	err := svc.HandleIssue(context.Background(), nil, newIssueEnv("nonexistent"), IssuePayload{FiscalInvoiceID: "nonexistent"})
	if err == nil {
		t.Fatal("expected error")
	}
	var pe *workers.PermanentError
	if !errors.As(err, &pe) {
		t.Errorf("expected PermanentError, got %T", err)
	}
}

// Scenario 11: correlation/causation chain preserved in emitted events.
func TestHandleIssue_EventCorrelationChain(t *testing.T) {
	prov := newStub()
	store := newMemStore()
	sink := &captureSink{}

	store.put(baseInvoice("inv-11", 5000))

	svc := buildSvc(prov, store, sink)
	env := newIssueEnv("inv-11")
	env.CorrelationID = "corr-xyz"
	_ = svc.HandleIssue(context.Background(), nil, env, IssuePayload{FiscalInvoiceID: "inv-11"})

	authorized := sink.byType(EventAuthorized)
	if len(authorized) == 0 {
		t.Fatal("expected fiscal.authorized event")
	}
	if authorized[0].CorrelationID != "corr-xyz" {
		t.Errorf("CorrelationID = %q, want %q", authorized[0].CorrelationID, "corr-xyz")
	}
	if authorized[0].CausationID == nil || *authorized[0].CausationID != env.MessageID {
		t.Errorf("CausationID = %v, want %q", authorized[0].CausationID, env.MessageID)
	}
}

// Scenario 12: transient provider error → non-permanent, attempt_count incremented.
func TestHandleIssue_TransientError(t *testing.T) {
	prov := newStub()
	prov.issueFn = func(_ IssueRequest) (IssueResult, RejectionDetail, error) {
		return IssueResult{}, RejectionDetail{},
			fmt.Errorf("%w: SEFAZ timeout", ErrProviderTransient)
	}
	store := newMemStore()
	sink := &captureSink{}

	store.put(baseInvoice("inv-12", 5000))

	svc := buildSvc(prov, store, sink)
	err := svc.HandleIssue(context.Background(), nil, newIssueEnv("inv-12"), IssuePayload{FiscalInvoiceID: "inv-12"})
	if err == nil {
		t.Fatal("expected non-nil error for transient failure")
	}
	if workers.Classify(err) == workers.RetryPermanent {
		t.Error("transient error must not be classified as permanent")
	}

	stored := store.get("inv-12")
	if stored.AttemptCount != 1 {
		t.Errorf("AttemptCount = %d after transient, want 1", stored.AttemptCount)
	}
	if stored.Status == StatusAuthorized {
		t.Error("status must not be authorized after transient failure")
	}
	// No fiscal.authorized or fulfillment emitted.
	if len(sink.byType(EventAuthorized)) != 0 {
		t.Error("fiscal.authorized must not be emitted on transient error")
	}
	if len(sink.byType(EventOrderFulfillRequested)) != 0 {
		t.Error("fulfillment must not be emitted on transient error")
	}
}
