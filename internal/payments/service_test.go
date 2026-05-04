package payments

import (
	"context"
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

// stubProvider is a minimal in-test provider so service_test stays free of
// the fake provider package (avoiding an import cycle: fake imports payments).
type stubProvider struct {
	mu        sync.Mutex
	statuses  map[string]Status
	idCounter int
}

func newStub() *stubProvider { return &stubProvider{statuses: map[string]Status{}} }

func (s *stubProvider) Name() string { return "stub" }
func (s *stubProvider) Capabilities() Capabilities {
	return Capabilities{Confirm: true, SeparateCapture: true, PartialRefund: true, Cancel: true}
}

func (s *stubProvider) decide(amount int64) (Status, error) {
	switch amount % 1000 {
	case 13:
		return StatusFailed, fmt.Errorf("%w: declined", ErrProviderRejected)
	case 17:
		return "", fmt.Errorf("%w: blip", ErrProviderTransient)
	}
	return StatusAuthorized, nil
}

func (s *stubProvider) CreatePayment(_ context.Context, req CreateRequest) (Result, error) {
	st, err := s.decide(req.Ref.Amount.AmountCents)
	if err != nil {
		return Result{}, err
	}
	s.mu.Lock()
	s.idCounter++
	id := fmt.Sprintf("stub_%d", s.idCounter)
	s.statuses[id] = st
	s.mu.Unlock()
	return Result{Status: st, ProviderPaymentID: id}, nil
}

func (s *stubProvider) ConfirmPayment(_ context.Context, req ConfirmRequest) (Result, error) {
	return Result{Status: StatusAuthorized, ProviderPaymentID: req.Ref.ProviderPaymentID}, nil
}

func (s *stubProvider) CapturePayment(_ context.Context, req CaptureRequest) (Result, error) {
	s.mu.Lock()
	s.statuses[req.Ref.ProviderPaymentID] = StatusPaid
	s.mu.Unlock()
	return Result{Status: StatusPaid, ProviderPaymentID: req.Ref.ProviderPaymentID}, nil
}

func (s *stubProvider) CancelPayment(_ context.Context, req CancelRequest) (Result, error) {
	s.mu.Lock()
	s.statuses[req.Ref.ProviderPaymentID] = StatusCancelled
	s.mu.Unlock()
	return Result{Status: StatusCancelled, ProviderPaymentID: req.Ref.ProviderPaymentID}, nil
}

func (s *stubProvider) RefundPayment(_ context.Context, req RefundRequest) (Result, error) {
	s.mu.Lock()
	s.statuses[req.Ref.ProviderPaymentID] = StatusRefunded
	s.mu.Unlock()
	return Result{Status: StatusRefunded, ProviderPaymentID: req.Ref.ProviderPaymentID}, nil
}

func (s *stubProvider) GetPaymentStatus(_ context.Context, providerPaymentID string) (Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Result{Status: s.statuses[providerPaymentID], ProviderPaymentID: providerPaymentID}, nil
}

func (s *stubProvider) VerifyWebhook(_ context.Context, _ map[string]string, _ []byte) error {
	return nil
}

func (s *stubProvider) ParseWebhook(_ context.Context, _ map[string]string, _ []byte) (WebhookEvent, error) {
	return WebhookEvent{}, nil
}

// memStore is an in-memory Store. It ignores tx since service tests don't
// exercise pgx semantics.
type memStore struct {
	rows     map[string]*PaymentRow
	attempts []*AttemptRow
}

func newMemStore() *memStore { return &memStore{rows: map[string]*PaymentRow{}} }

func (m *memStore) put(row PaymentRow) { r := row; m.rows[row.ID] = &r }

func (m *memStore) LockPayment(_ context.Context, _ pgx.Tx, id string) (*PaymentRow, error) {
	r, ok := m.rows[id]
	if !ok {
		return nil, ErrPaymentNotFound
	}
	cp := *r
	return &cp, nil
}

func (m *memStore) UpdatePayment(_ context.Context, _ pgx.Tx, p *PaymentRow, _ string) error {
	cp := *p
	m.rows[p.ID] = &cp
	return nil
}

func (m *memStore) InsertAttempt(_ context.Context, _ pgx.Tx, a *AttemptRow) error {
	for _, existing := range m.attempts {
		if existing.PaymentID == a.PaymentID && existing.Operation == a.Operation && existing.RequestID == a.RequestID {
			return nil
		}
	}
	cp := *a
	m.attempts = append(m.attempts, &cp)
	return nil
}

// Slice C stubs — not exercised by service tests.
func (m *memStore) LockIngress(_ context.Context, _ pgx.Tx, _ string) (*IngressRow, error) {
	return nil, ErrPaymentNotFound
}
func (m *memStore) MarkIngressVerified(_ context.Context, _ pgx.Tx, _, _ string) error { return nil }
func (m *memStore) MarkIngressRejected(_ context.Context, _ pgx.Tx, _, _ string) error { return nil }
func (m *memStore) LockPaymentByProviderID(_ context.Context, _ pgx.Tx, _, _ string) (*PaymentRow, error) {
	return nil, ErrPaymentNotFound
}
func (m *memStore) InsertPaymentEvent(_ context.Context, _ pgx.Tx, _ *PaymentEventRow) (bool, error) {
	return true, nil
}

type capturingSink struct {
	events []*message.Envelope
	queues []string
}

func (c *capturingSink) Emit(_ context.Context, _ pgx.Tx, queue string, env *message.Envelope) error {
	c.events = append(c.events, env)
	c.queues = append(c.queues, queue)
	return nil
}

func newSvc(t *testing.T) (*Service, *memStore, *capturingSink) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg := NewRegistry()
	reg.Register(newStub())
	store := newMemStore()
	sink := &capturingSink{}
	return NewService(logger, reg, store, sink), store, sink
}

func mkEnv(t *testing.T, eventType string) *message.Envelope {
	t.Helper()
	env, err := message.New(eventType, "order", "00000000-0000-0000-0000-000000000001", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	return env
}

func TestHandleCreate_AuthorizedEmitsEvent(t *testing.T) {
	svc, store, sink := newSvc(t)
	store.put(PaymentRow{ID: "pay-1", OrderID: "ord-1", Provider: "stub", AmountCents: 2500, Currency: "BRL", Status: StatusUnknown})

	env := mkEnv(t, "payment.create.requested")
	if err := svc.HandleCreate(context.Background(), nil, env, CreatePayload{PaymentID: "pay-1"}); err != nil {
		t.Fatalf("HandleCreate: %v", err)
	}
	if got := store.rows["pay-1"].Status; got != StatusAuthorized {
		t.Fatalf("status: %v", got)
	}
	if store.rows["pay-1"].ProviderPaymentID == "" {
		t.Fatal("provider id not persisted")
	}
	if len(store.attempts) != 1 || store.attempts[0].Status != "ok" {
		t.Fatalf("attempts: %+v", store.attempts)
	}
	if len(sink.events) != 1 || sink.events[0].EventType != EventAuthorized {
		t.Fatalf("events: %+v", sink.events)
	}
	if sink.queues[0] != QueueEvents {
		t.Fatalf("queue: %s", sink.queues[0])
	}
	if sink.events[0].CorrelationID != env.CorrelationID {
		t.Fatal("correlation id not propagated")
	}
	if sink.events[0].CausationID == nil || *sink.events[0].CausationID != env.MessageID {
		t.Fatal("causation id not set")
	}
}

func TestHandleCreate_RejectedIsPermanentAndEmitsFailed(t *testing.T) {
	svc, store, sink := newSvc(t)
	store.put(PaymentRow{ID: "pay-2", Provider: "stub", AmountCents: 1013, Currency: "BRL", Status: StatusUnknown})

	err := svc.HandleCreate(context.Background(), nil, mkEnv(t, "payment.create.requested"), CreatePayload{PaymentID: "pay-2"})
	if err == nil {
		t.Fatal("expected error")
	}
	if workers.Classify(err) != workers.RetryPermanent {
		t.Fatalf("expected permanent: %v", err)
	}
	if store.rows["pay-2"].Status != StatusFailed {
		t.Fatalf("status: %v", store.rows["pay-2"].Status)
	}
	if len(sink.events) != 1 || sink.events[0].EventType != EventFailed {
		t.Fatalf("events: %+v", sink.events)
	}
}

func TestHandleCreate_TransientRetries(t *testing.T) {
	svc, store, sink := newSvc(t)
	store.put(PaymentRow{ID: "pay-3", Provider: "stub", AmountCents: 1017, Currency: "BRL", Status: StatusUnknown})

	err := svc.HandleCreate(context.Background(), nil, mkEnv(t, "payment.create.requested"), CreatePayload{PaymentID: "pay-3"})
	if err == nil {
		t.Fatal("expected error")
	}
	if workers.Classify(err) != workers.RetryTransient {
		t.Fatal("expected transient")
	}
	if !errors.Is(err, ErrProviderTransient) {
		t.Fatalf("not transient: %v", err)
	}
	if store.rows["pay-3"].Status == StatusFailed {
		t.Fatal("transient errors must NOT mark payment failed")
	}
	if len(sink.events) != 0 {
		t.Fatal("transient must not emit events")
	}
	if len(store.attempts) != 1 || store.attempts[0].Status != "failed" {
		t.Fatal("attempt audit row should be persisted on transient failure")
	}
}

func TestPaymentNotFoundIsPermanent(t *testing.T) {
	svc, _, _ := newSvc(t)
	err := svc.HandleCreate(context.Background(), nil, mkEnv(t, "payment.create.requested"), CreatePayload{PaymentID: "missing"})
	if workers.Classify(err) != workers.RetryPermanent {
		t.Fatalf("want permanent, got %v", err)
	}
}

func TestUnknownProviderIsPermanent(t *testing.T) {
	svc, store, _ := newSvc(t)
	store.put(PaymentRow{ID: "pay-4", Provider: "nonexistent", AmountCents: 100, Currency: "BRL", Status: StatusUnknown})
	err := svc.HandleCreate(context.Background(), nil, mkEnv(t, "payment.create.requested"), CreatePayload{PaymentID: "pay-4"})
	if workers.Classify(err) != workers.RetryPermanent {
		t.Fatalf("want permanent, got %v", err)
	}
}

func TestIllegalTransitionIsPermanent(t *testing.T) {
	svc, store, _ := newSvc(t)
	store.put(PaymentRow{ID: "pay-5", Provider: "stub", AmountCents: 100, Currency: "BRL", Status: StatusUnknown})
	err := svc.HandleCapture(context.Background(), nil, mkEnv(t, "payment.capture.requested"), CapturePayload{PaymentID: "pay-5"})
	if workers.Classify(err) != workers.RetryPermanent {
		t.Fatalf("want permanent, got %v", err)
	}
}

func TestFullCaptureRefundFlow(t *testing.T) {
	svc, store, sink := newSvc(t)
	store.put(PaymentRow{ID: "pay-6", Provider: "stub", AmountCents: 5000, Currency: "BRL", Status: StatusUnknown})

	if err := svc.HandleCreate(context.Background(), nil, mkEnv(t, "payment.create.requested"), CreatePayload{PaymentID: "pay-6"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if store.rows["pay-6"].Status != StatusAuthorized {
		t.Fatalf("after create: %v", store.rows["pay-6"].Status)
	}
	if err := svc.HandleCapture(context.Background(), nil, mkEnv(t, "payment.capture.requested"), CapturePayload{PaymentID: "pay-6"}); err != nil {
		t.Fatalf("capture: %v", err)
	}
	if store.rows["pay-6"].Status != StatusPaid {
		t.Fatalf("after capture: %v", store.rows["pay-6"].Status)
	}
	if err := svc.HandleRefund(context.Background(), nil, mkEnv(t, "payment.refund.requested"), RefundPayload{PaymentID: "pay-6"}); err != nil {
		t.Fatalf("refund: %v", err)
	}
	if store.rows["pay-6"].Status != StatusRefunded {
		t.Fatalf("after refund: %v", store.rows["pay-6"].Status)
	}

	wantEvents := []string{EventAuthorized, EventCaptured, EventRefunded}
	if len(sink.events) != len(wantEvents) {
		t.Fatalf("events: %d", len(sink.events))
	}
	for i, e := range sink.events {
		if e.EventType != wantEvents[i] {
			t.Fatalf("event[%d]: %s", i, e.EventType)
		}
	}
}

func TestBuildRequestIDStableAcrossRetries(t *testing.T) {
	a := buildRequestID("msg-1", "pay-1", OpCreate)
	b := buildRequestID("msg-1", "pay-1", OpCreate)
	if a != b {
		t.Fatal("must be stable for same inputs")
	}
	if a == buildRequestID("msg-2", "pay-1", OpCreate) {
		t.Fatal("must differ for different message ids")
	}
}
