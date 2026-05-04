package payments

// Webhook tests use entirely in-package fakes — no import of the fake
// provider package, which would cause an import cycle. testProvider below
// mirrors just the webhook-relevant behaviour of the fake adapter.
//
// Scenarios:
//  1. Valid webhook → payment updated, event inserted, ingress verified, follow-up emitted.
//  2. Invalid signature → ingress rejected, no payment mutation.
//  3. Duplicate webhook (same provider_event_id) → ack, ingress re-verified, no double event.
//  4. Out-of-order webhook (lower rank) → skip, payment unchanged.
//  5. Ambiguous same-rank transition → provider lookup used to resolve.
//  6. Unknown provider → ingress rejected, permanent error.
//  7. Payment not found → transient (out-of-order before create worker commits).
//  8. No state mutation before verification — explicit assertion.
//  9. Idempotent repeated message delivery (ingress already verified on 2nd call).

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/blondbeauty/blond-beauty-engine/internal/message"
	"github.com/blondbeauty/blond-beauty-engine/internal/workers"
)

// ---- test provider -------------------------------------------------------

const testProviderName = "testprov"
const testSigHeader = "x-test-signature"

// testProvider is a self-contained provider for webhook tests.
// It implements the full Provider interface; outbound payment methods
// are stubs since webhook tests never invoke them.
type testProvider struct {
	verifyErr    error // override to simulate bad signatures
	parseResult  WebhookEvent
	parseErr     error
	lookupResult Status
	lookupErr    error
	verifyCalls  int
}

func newTestProvider() *testProvider { return &testProvider{} }

func (p *testProvider) Name() string { return testProviderName }
func (p *testProvider) Capabilities() Capabilities {
	return Capabilities{Confirm: true, SeparateCapture: true, PartialRefund: true, Cancel: true, Webhooks: true}
}
func (p *testProvider) CreatePayment(_ context.Context, _ CreateRequest) (Result, error) {
	return Result{}, nil
}
func (p *testProvider) ConfirmPayment(_ context.Context, _ ConfirmRequest) (Result, error) {
	return Result{}, nil
}
func (p *testProvider) CapturePayment(_ context.Context, _ CaptureRequest) (Result, error) {
	return Result{}, nil
}
func (p *testProvider) CancelPayment(_ context.Context, _ CancelRequest) (Result, error) {
	return Result{}, nil
}
func (p *testProvider) RefundPayment(_ context.Context, _ RefundRequest) (Result, error) {
	return Result{}, nil
}
func (p *testProvider) GetPaymentStatus(_ context.Context, id string) (Result, error) {
	return Result{Status: p.lookupResult, ProviderPaymentID: id}, p.lookupErr
}

// VerifyWebhook checks x-test-signature == sha256hex(body).
func (p *testProvider) VerifyWebhook(_ context.Context, headers map[string]string, body []byte) error {
	p.verifyCalls++
	if p.verifyErr != nil {
		return p.verifyErr
	}
	got := headers[testSigHeader]
	if got != signBody(body) {
		return fmt.Errorf("%w: mismatch", ErrWebhookSignatureInvalid)
	}
	return nil
}

func (p *testProvider) ParseWebhook(_ context.Context, _ map[string]string, body []byte) (WebhookEvent, error) {
	if p.parseErr != nil {
		return WebhookEvent{}, p.parseErr
	}
	if p.parseResult.ProviderEventID != "" {
		return p.parseResult, nil
	}
	// Default: parse as fakeWebhookBody JSON.
	var b struct {
		EventID   string `json:"event_id"`
		ProvPayID string `json:"provider_payment_id"`
		EventType string `json:"event_type"`
		Status    string `json:"status"`
		OccAt     string `json:"occurred_at"`
	}
	if err := json.Unmarshal(body, &b); err != nil {
		return WebhookEvent{}, fmt.Errorf("%w: %v", ErrWebhookParseFailed, err)
	}
	st := Status(b.Status)
	if !st.Valid() {
		return WebhookEvent{}, fmt.Errorf("%w: unknown status %q", ErrWebhookParseFailed, b.Status)
	}
	redacted, _ := json.Marshal(map[string]any{"event_id": b.EventID, "status": b.Status})
	return WebhookEvent{
		ProviderEventID:   b.EventID,
		ProviderPaymentID: b.ProvPayID,
		EventType:         b.EventType,
		CanonicalStatus:   st,
		OccurredAt:        time.Now().UTC(),
		RedactedPayload:   redacted,
	}, nil
}

func signBody(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }

// ---- in-memory stores ---------------------------------------------------

// webhookStore implements the full Store interface with in-memory maps.
type webhookStore struct {
	payments      map[string]*PaymentRow
	attempts      []*AttemptRow
	ingresses     map[string]*IngressRow
	paymentEvents map[string]*PaymentEventRow // key: provider+"|"+provider_event_id
	payByProvider map[string]*PaymentRow      // key: provider+"|"+providerPaymentID
}

func newWebhookStore() *webhookStore {
	return &webhookStore{
		payments:      map[string]*PaymentRow{},
		ingresses:     map[string]*IngressRow{},
		paymentEvents: map[string]*PaymentEventRow{},
		payByProvider: map[string]*PaymentRow{},
	}
}

func (s *webhookStore) putIngress(r IngressRow) { cp := r; s.ingresses[r.ID] = &cp }

func (s *webhookStore) putPaymentByProvider(r PaymentRow) {
	cp := r
	s.payments[r.ID] = &cp
	s.payByProvider[r.Provider+"|"+r.ProviderPaymentID] = &cp
}

// Slice B methods (not exercised by webhook tests, but required by interface).
func (s *webhookStore) LockPayment(_ context.Context, _ pgx.Tx, id string) (*PaymentRow, error) {
	r, ok := s.payments[id]
	if !ok {
		return nil, ErrPaymentNotFound
	}
	cp := *r
	return &cp, nil
}

func (s *webhookStore) UpdatePayment(_ context.Context, _ pgx.Tx, p *PaymentRow, _ string) error {
	cp := *p
	s.payments[p.ID] = &cp
	if prev, ok := s.payByProvider[p.Provider+"|"+p.ProviderPaymentID]; ok {
		prev.Status = p.Status
	}
	return nil
}

func (s *webhookStore) InsertAttempt(_ context.Context, _ pgx.Tx, a *AttemptRow) error {
	s.attempts = append(s.attempts, a)
	return nil
}

// Slice C methods.
func (s *webhookStore) LockIngress(_ context.Context, _ pgx.Tx, id string) (*IngressRow, error) {
	r, ok := s.ingresses[id]
	if !ok {
		return nil, fmt.Errorf("ingress not found: %s", id)
	}
	cp := *r
	return &cp, nil
}

func (s *webhookStore) MarkIngressVerified(_ context.Context, _ pgx.Tx, id, evtID string) error {
	if r, ok := s.ingresses[id]; ok {
		r.VerificationStatus = "verified"
		r.ProviderEventID = evtID
	}
	return nil
}

func (s *webhookStore) MarkIngressRejected(_ context.Context, _ pgx.Tx, id, _ string) error {
	if r, ok := s.ingresses[id]; ok {
		r.VerificationStatus = "rejected"
	}
	return nil
}

func (s *webhookStore) LockPaymentByProviderID(_ context.Context, _ pgx.Tx, provider, provID string) (*PaymentRow, error) {
	key := provider + "|" + provID
	r, ok := s.payByProvider[key]
	if !ok {
		return nil, ErrPaymentNotFound
	}
	cp := *r
	return &cp, nil
}

func (s *webhookStore) InsertPaymentEvent(_ context.Context, _ pgx.Tx, e *PaymentEventRow) (bool, error) {
	key := e.Provider + "|" + e.ProviderEventID
	if _, exists := s.paymentEvents[key]; exists {
		return false, nil
	}
	cp := *e
	s.paymentEvents[key] = &cp
	return true, nil
}

// ---- helpers ------------------------------------------------------------

func newWebhookSvcFull(t *testing.T, store Store, prov Provider) (*WebhookService, *capturingSink) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg := NewRegistry()
	reg.Register(prov)
	sink := &capturingSink{}
	return NewWebhookService(logger, &pgxpool.Pool{}, reg, store, sink), sink
}

func buildBody(evtID, provPayID, status string) []byte {
	ts := time.Now().UTC().Format(time.RFC3339)
	return []byte(`{"event_id":"` + evtID + `","provider_payment_id":"` + provPayID +
		`","event_type":"payment.` + status + `","status":"` + status + `","occurred_at":"` + ts + `"}`)
}

func validHeaders(body []byte) map[string]string {
	return map[string]string{testSigHeader: signBody(body)}
}

func webhookMsg(t *testing.T, ingressID string) *message.Envelope {
	t.Helper()
	env, err := message.New("payment.webhook.received", "webhook_ingress", ingressID,
		WebhookPayload{IngressID: ingressID})
	if err != nil {
		t.Fatal(err)
	}
	return env
}

// ---- tests --------------------------------------------------------------

// 1. Valid webhook.
func TestWebhook_ValidUpdatesPaymentAndEmitsEvent(t *testing.T) {
	store := newWebhookStore()
	body := buildBody("evt-1", "prov_pay-1", "authorized")
	store.putIngress(IngressRow{ID: "ing-1", Provider: testProviderName,
		RawHeaders: validHeaders(body), RawBody: body, VerificationStatus: "received"})
	store.putPaymentByProvider(PaymentRow{ID: "pay-1", OrderID: "ord-1",
		Provider: testProviderName, ProviderPaymentID: "prov_pay-1",
		AmountCents: 1000, Currency: "BRL", Status: StatusUnknown})

	svc, sink := newWebhookSvcFull(t, store, newTestProvider())
	if err := svc.Handle(context.Background(), nil, webhookMsg(t, "ing-1")); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if store.ingresses["ing-1"].VerificationStatus != "verified" {
		t.Fatal("ingress not verified")
	}
	if store.payments["pay-1"].Status != StatusAuthorized {
		t.Fatalf("status: %v", store.payments["pay-1"].Status)
	}
	if len(sink.events) != 1 || sink.events[0].EventType != EventAuthorized {
		t.Fatalf("events: %+v", sink.events)
	}
	if sink.events[0].CorrelationID == "" {
		t.Fatal("correlation_id missing")
	}
}

// 2. Invalid signature → reject ingress, no payment mutation.
func TestWebhook_InvalidSignatureRejectsIngressNoMutation(t *testing.T) {
	store := newWebhookStore()
	body := buildBody("evt-2", "prov_pay-2", "authorized")
	store.putIngress(IngressRow{ID: "ing-2", Provider: testProviderName,
		RawHeaders: map[string]string{testSigHeader: "badsig"}, RawBody: body,
		VerificationStatus: "received"})
	store.putPaymentByProvider(PaymentRow{ID: "pay-2", Provider: testProviderName,
		ProviderPaymentID: "prov_pay-2", Status: StatusUnknown})

	updateCalled := false
	spy := &updateSpy{webhookStore: store, cb: func() { updateCalled = true }}
	svc, _ := newWebhookSvcFull(t, spy, newTestProvider())

	err := svc.Handle(context.Background(), nil, webhookMsg(t, "ing-2"))
	if !errors.Is(err, ErrWebhookSignatureInvalid) {
		t.Fatalf("want ErrWebhookSignatureInvalid, got %v", err)
	}
	if workers.Classify(err) != workers.RetryPermanent {
		t.Fatalf("want permanent, got transient")
	}
	if store.ingresses["ing-2"].VerificationStatus != "rejected" {
		t.Fatal("ingress not rejected")
	}
	if updateCalled {
		t.Fatal("UpdatePayment called before verification")
	}
}

// updateSpy wraps webhookStore to detect UpdatePayment calls.
type updateSpy struct {
	*webhookStore
	cb func()
}

func (s *updateSpy) UpdatePayment(ctx context.Context, tx pgx.Tx, p *PaymentRow, e string) error {
	if s.cb != nil {
		s.cb()
	}
	return s.webhookStore.UpdatePayment(ctx, tx, p, e)
}

// 3. Duplicate webhook (same provider_event_id from two ingress rows).
func TestWebhook_DuplicateEventIdempotent(t *testing.T) {
	store := newWebhookStore()
	body := buildBody("evt-3", "prov_pay-3", "authorized")
	hdr := validHeaders(body)
	store.putIngress(IngressRow{ID: "ing-3a", Provider: testProviderName, RawHeaders: hdr, RawBody: body, VerificationStatus: "received"})
	store.putIngress(IngressRow{ID: "ing-3b", Provider: testProviderName, RawHeaders: hdr, RawBody: body, VerificationStatus: "received"})
	store.putPaymentByProvider(PaymentRow{ID: "pay-3", Provider: testProviderName,
		ProviderPaymentID: "prov_pay-3", Status: StatusUnknown})

	svc, sink := newWebhookSvcFull(t, store, newTestProvider())

	if err := svc.Handle(context.Background(), nil, webhookMsg(t, "ing-3a")); err != nil {
		t.Fatalf("first: %v", err)
	}
	if err := svc.Handle(context.Background(), nil, webhookMsg(t, "ing-3b")); err != nil {
		t.Fatalf("second: %v", err)
	}

	if len(sink.events) != 1 {
		t.Fatalf("expected 1 follow-up event, got %d", len(sink.events))
	}
	if store.ingresses["ing-3a"].VerificationStatus != "verified" ||
		store.ingresses["ing-3b"].VerificationStatus != "verified" {
		t.Fatal("both ingresses should be verified")
	}
}

// 4. Out-of-order: webhook rank lower than current payment status.
func TestWebhook_OutOfOrderLowerRankSkipped(t *testing.T) {
	store := newWebhookStore()
	body := buildBody("evt-4", "prov_pay-4", "authorized") // rank 2
	store.putIngress(IngressRow{ID: "ing-4", Provider: testProviderName,
		RawHeaders: validHeaders(body), RawBody: body, VerificationStatus: "received"})
	store.putPaymentByProvider(PaymentRow{ID: "pay-4", Provider: testProviderName,
		ProviderPaymentID: "prov_pay-4", Status: StatusPaid}) // rank 3 — already ahead

	svc, sink := newWebhookSvcFull(t, store, newTestProvider())
	if err := svc.Handle(context.Background(), nil, webhookMsg(t, "ing-4")); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	if store.payments["pay-4"].Status != StatusPaid {
		t.Fatalf("status should stay paid, got %v", store.payments["pay-4"].Status)
	}
	if len(sink.events) != 0 {
		t.Fatalf("no follow-up events expected, got %d", len(sink.events))
	}
	if store.ingresses["ing-4"].VerificationStatus != "verified" {
		t.Fatal("ingress should be verified even on skip")
	}
}

// 5. Ambiguous same-rank transition → provider lookup determines outcome.
func TestWebhook_AmbiguousSameRankUsesProviderLookup(t *testing.T) {
	store := newWebhookStore()

	// Webhook says "failed" (rank 4); payment is "cancelled" (rank 4).
	prov := newTestProvider()
	// Override parse to return controlled event.
	body := buildBody("evt-5", "prov_pay-5", "failed")
	prov.parseResult = WebhookEvent{
		ProviderEventID:   "evt-5",
		ProviderPaymentID: "prov_pay-5",
		EventType:         "payment.failed",
		CanonicalStatus:   StatusFailed,
		OccurredAt:        time.Now().UTC(),
	}
	// Provider lookup returns "cancelled" (already there) → skip.
	prov.lookupResult = StatusCancelled

	store.putIngress(IngressRow{ID: "ing-5", Provider: testProviderName,
		RawHeaders: validHeaders(body), RawBody: body, VerificationStatus: "received"})
	store.putPaymentByProvider(PaymentRow{ID: "pay-5", Provider: testProviderName,
		ProviderPaymentID: "prov_pay-5", Status: StatusCancelled})

	svc, sink := newWebhookSvcFull(t, store, prov)
	if err := svc.Handle(context.Background(), nil, webhookMsg(t, "ing-5")); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	// Provider said cancelled → we stay at cancelled.
	if store.payments["pay-5"].Status != StatusCancelled {
		t.Fatalf("status: %v", store.payments["pay-5"].Status)
	}
	if len(sink.events) != 0 {
		t.Fatalf("no follow-up events expected, got %d", len(sink.events))
	}
}

// 6. Unknown provider.
func TestWebhook_UnknownProviderRejectsIngress(t *testing.T) {
	store := newWebhookStore()
	store.putIngress(IngressRow{ID: "ing-6", Provider: "ghost",
		RawHeaders: nil, RawBody: []byte(`{}`), VerificationStatus: "received"})

	svc, _ := newWebhookSvcFull(t, store, newTestProvider())
	err := svc.Handle(context.Background(), nil, webhookMsg(t, "ing-6"))

	if workers.Classify(err) != workers.RetryPermanent {
		t.Fatalf("want permanent, got %v", err)
	}
	if !errors.Is(err, ErrUnknownProvider) {
		t.Fatalf("want ErrUnknownProvider, got %v", err)
	}
	if store.ingresses["ing-6"].VerificationStatus != "rejected" {
		t.Fatal("ingress should be rejected")
	}
}

// 7. Payment not found → transient.
func TestWebhook_PaymentNotFoundIsTransient(t *testing.T) {
	store := newWebhookStore()
	body := buildBody("evt-7", "prov_pay-missing", "authorized")
	store.putIngress(IngressRow{ID: "ing-7", Provider: testProviderName,
		RawHeaders: validHeaders(body), RawBody: body, VerificationStatus: "received"})
	// No payment row registered.

	svc, _ := newWebhookSvcFull(t, store, newTestProvider())
	err := svc.Handle(context.Background(), nil, webhookMsg(t, "ing-7"))

	if err == nil {
		t.Fatal("expected error")
	}
	if workers.Classify(err) == workers.RetryPermanent {
		t.Fatalf("must be transient for missing payment, got permanent: %v", err)
	}
	// Ingress must NOT be rejected so we can retry.
	if store.ingresses["ing-7"].VerificationStatus == "rejected" {
		t.Fatal("ingress must not be rejected on transient payment-not-found")
	}
}

// 8. No mutation before verification — explicit assertion.
func TestWebhook_NoMutationBeforeVerification(t *testing.T) {
	store := newWebhookStore()
	body := buildBody("evt-8", "prov_pay-8", "paid")
	// Bad signature.
	store.putIngress(IngressRow{ID: "ing-8", Provider: testProviderName,
		RawHeaders: map[string]string{testSigHeader: "wrong"}, RawBody: body,
		VerificationStatus: "received"})
	store.putPaymentByProvider(PaymentRow{ID: "pay-8", Provider: testProviderName,
		ProviderPaymentID: "prov_pay-8", Status: StatusAuthorized})

	updateCalled := false
	spy := &updateSpy{webhookStore: store, cb: func() { updateCalled = true }}
	svc, _ := newWebhookSvcFull(t, spy, newTestProvider())
	_ = svc.Handle(context.Background(), nil, webhookMsg(t, "ing-8"))

	if updateCalled {
		t.Fatal("UpdatePayment must not be called before verification succeeds")
	}
	if store.payments["pay-8"].Status != StatusAuthorized {
		t.Fatalf("payment status mutated without verification: %v", store.payments["pay-8"].Status)
	}
}

// 9. Idempotent repeated delivery (ingress already verified on 2nd call).
func TestWebhook_IdempotentRepeatedDelivery(t *testing.T) {
	store := newWebhookStore()
	body := buildBody("evt-9", "prov_pay-9", "authorized")
	store.putIngress(IngressRow{ID: "ing-9", Provider: testProviderName,
		RawHeaders: validHeaders(body), RawBody: body, VerificationStatus: "received"})
	store.putPaymentByProvider(PaymentRow{ID: "pay-9", Provider: testProviderName,
		ProviderPaymentID: "prov_pay-9", Status: StatusUnknown})

	svc, sink := newWebhookSvcFull(t, store, newTestProvider())
	env := webhookMsg(t, "ing-9")

	if err := svc.Handle(context.Background(), nil, env); err != nil {
		t.Fatalf("first: %v", err)
	}
	// Second delivery: ingress is now "verified" → early exit.
	if err := svc.Handle(context.Background(), nil, env); err != nil {
		t.Fatalf("second: %v", err)
	}

	if len(sink.events) != 1 {
		t.Fatalf("expected exactly 1 follow-up event, got %d", len(sink.events))
	}
	if store.payments["pay-9"].Status != StatusAuthorized {
		t.Fatalf("status: %v", store.payments["pay-9"].Status)
	}
}
