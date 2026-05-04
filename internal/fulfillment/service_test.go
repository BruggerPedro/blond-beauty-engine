package fulfillment

// White-box tests in package fulfillment. The fake provider is NOT imported
// here (avoids import cycle: fake imports fulfillment). All provider/store
// behaviour is controlled through inline stubs.

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
	mu         sync.Mutex
	dispatched []DispatchRequest
	dispatchFn func(DispatchRequest) (DispatchResult, error)
}

func newStub() *stubProvider {
	p := &stubProvider{}
	p.dispatchFn = func(req DispatchRequest) (DispatchResult, error) {
		return DispatchResult{
			ProviderShipmentID: "stub_ship_" + req.ShipmentID,
			Carrier:            "Stub Carrier",
			TrackingNumber:     "STUB-TRACK-001",
			TrackingURL:        "https://stub-carrier.local/track/STUB-TRACK-001",
		}, nil
	}
	return p
}

func (s *stubProvider) Name() string { return "stub-fulfillment" }

func (s *stubProvider) DispatchShipment(_ context.Context, req DispatchRequest) (DispatchResult, error) {
	s.mu.Lock()
	s.dispatched = append(s.dispatched, req)
	s.mu.Unlock()
	return s.dispatchFn(req)
}

func (s *stubProvider) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.dispatched)
}

// ---- in-memory store -----------------------------------------------------

type memStore struct {
	mu           sync.Mutex
	states       map[string]*FulfillmentState // keyed by orderID
	shipments    map[string]*ShipmentRow      // keyed by idempotency_key
	events       []*ShipmentEventRow
	emailUpdates map[string]ShipmentEmailData
	nextShipID   int
}

func newMemStore() *memStore {
	return &memStore{
		states:       map[string]*FulfillmentState{},
		shipments:    map[string]*ShipmentRow{},
		emailUpdates: map[string]ShipmentEmailData{},
	}
}

func (ms *memStore) putState(orderID string, s *FulfillmentState) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	cp := *s
	ms.states[orderID] = &cp
}

func (ms *memStore) getShipment(idemKey string) *ShipmentRow {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	s := ms.shipments[idemKey]
	if s == nil {
		return nil
	}
	cp := *s
	return &cp
}

func (ms *memStore) LoadFulfillmentState(_ context.Context, _ pgx.Tx, orderID string) (*FulfillmentState, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	s, ok := ms.states[orderID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrOrderNotFound, orderID)
	}
	cp := *s
	return &cp, nil
}

func (ms *memStore) FindOrCreateShipment(_ context.Context, _ pgx.Tx, tmpl *ShipmentRow) (*ShipmentRow, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if existing, ok := ms.shipments[tmpl.IdempotencyKey]; ok {
		cp := *existing
		return &cp, nil
	}
	ms.nextShipID++
	row := &ShipmentRow{
		ID:                    fmt.Sprintf("ship-%03d", ms.nextShipID),
		OrderID:               tmpl.OrderID,
		Status:                ShipmentPending,
		Provider:              tmpl.Provider,
		IdempotencyKey:        tmpl.IdempotencyKey,
		ShippedEmailMessageID: tmpl.ShippedEmailMessageID,
		CorrelationID:         tmpl.CorrelationID,
		CausationID:           tmpl.CausationID,
	}
	ms.shipments[tmpl.IdempotencyKey] = row
	cp := *row
	return &cp, nil
}

func (ms *memStore) UpdateShipment(_ context.Context, _ pgx.Tx, s *ShipmentRow) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	cp := *s
	ms.shipments[s.IdempotencyKey] = &cp
	return nil
}

func (ms *memStore) InsertShipmentEvent(_ context.Context, _ pgx.Tx, ev *ShipmentEventRow) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	cp := *ev
	ms.events = append(ms.events, &cp)
	return nil
}

func (ms *memStore) UpdateEmailTemplateData(_ context.Context, _ pgx.Tx, emailMsgID string, data ShipmentEmailData) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.emailUpdates[emailMsgID] = data
	return nil
}

func (ms *memStore) shipmentByOrder(orderID string) *ShipmentRow {
	return ms.getShipment("fulfill|" + orderID)
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

func (c *captureSink) byType(t string) []*message.Envelope {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []*message.Envelope
	for _, e := range c.events {
		if e.EventType == t {
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

func paidState(requiresFiscal bool) *FulfillmentState {
	s := &FulfillmentState{
		OrderStatus:    "paid",
		RequiresFiscal: requiresFiscal,
		PaymentStatus:  "paid",
	}
	if requiresFiscal {
		s.FiscalStatus = "authorized"
	}
	return s
}

func newFulfillEnv(orderID string) *message.Envelope {
	payload, _ := json.Marshal(FulfillmentPayload{OrderID: orderID})
	env, _ := message.New("order.fulfillment.requested", "order", orderID, nil)
	env.Payload = payload
	env.CorrelationID = "corr-" + orderID
	return env
}

func handle(svc *Service, orderID string) error {
	env := newFulfillEnv(orderID)
	p := FulfillmentPayload{OrderID: orderID}
	return svc.HandleFulfillment(context.Background(), nil, env, p)
}

// ---- test scenarios ------------------------------------------------------

// Scenario 1: successful fulfillment after paid + fiscal authorized.
func TestHandleFulfillment_Success(t *testing.T) {
	prov := newStub()
	store := newMemStore()
	sink := &captureSink{}
	store.putState("order-1", paidState(true))

	svc := buildSvc(prov, store, sink)
	if err := handle(svc, "order-1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ship := store.shipmentByOrder("order-1")
	if ship == nil || ship.Status != ShipmentDispatched {
		t.Fatalf("expected dispatched shipment, got %v", ship)
	}
	if ship.Carrier == "" || ship.TrackingNumber == "" {
		t.Error("expected non-empty carrier and tracking number")
	}
}

// Scenario 2: blocked when fiscal pending.
func TestHandleFulfillment_BlockedFiscalPending(t *testing.T) {
	prov := newStub()
	store := newMemStore()
	sink := &captureSink{}
	store.putState("order-2", &FulfillmentState{
		OrderStatus: "paid", RequiresFiscal: true,
		PaymentStatus: "paid", FiscalStatus: "pending",
	})

	svc := buildSvc(prov, store, sink)
	err := handle(svc, "order-2")
	assertPermanent(t, err, "fiscal pending")
	if prov.callCount() != 0 {
		t.Error("provider must not be called when fiscal is pending")
	}
}

// Scenario 3: blocked when fiscal rejected.
func TestHandleFulfillment_BlockedFiscalRejected(t *testing.T) {
	prov := newStub()
	store := newMemStore()
	sink := &captureSink{}
	store.putState("order-3", &FulfillmentState{
		OrderStatus: "paid", RequiresFiscal: true,
		PaymentStatus: "paid", FiscalStatus: "rejected",
	})

	svc := buildSvc(prov, store, sink)
	assertPermanent(t, handle(svc, "order-3"), "fiscal rejected")
}

// Scenario 4: blocked when fiscal cancelled.
func TestHandleFulfillment_BlockedFiscalCancelled(t *testing.T) {
	prov := newStub()
	store := newMemStore()
	sink := &captureSink{}
	store.putState("order-4", &FulfillmentState{
		OrderStatus: "paid", RequiresFiscal: true,
		PaymentStatus: "paid", FiscalStatus: "cancelled",
	})

	svc := buildSvc(prov, store, sink)
	assertPermanent(t, handle(svc, "order-4"), "fiscal cancelled")
}

// Scenario 5: blocked when payment not successful.
func TestHandleFulfillment_BlockedPaymentFailed(t *testing.T) {
	prov := newStub()
	store := newMemStore()
	sink := &captureSink{}
	store.putState("order-5", &FulfillmentState{
		OrderStatus: "paid", RequiresFiscal: false,
		PaymentStatus: "failed",
	})

	svc := buildSvc(prov, store, sink)
	assertPermanent(t, handle(svc, "order-5"), "payment failed")
}

// Scenario 6: blocked when order cancelled.
func TestHandleFulfillment_BlockedOrderCancelled(t *testing.T) {
	prov := newStub()
	store := newMemStore()
	sink := &captureSink{}
	store.putState("order-6", &FulfillmentState{
		OrderStatus: "cancelled", RequiresFiscal: false,
		PaymentStatus: "paid",
	})

	svc := buildSvc(prov, store, sink)
	assertPermanent(t, handle(svc, "order-6"), "order cancelled")
}

// Scenario 7: duplicate message is idempotent — shipment already dispatched.
func TestHandleFulfillment_DuplicateMessage_Idempotent(t *testing.T) {
	prov := newStub()
	store := newMemStore()
	sink := &captureSink{}
	store.putState("order-7", paidState(false))

	svc := buildSvc(prov, store, sink)

	// First delivery.
	if err := handle(svc, "order-7"); err != nil {
		t.Fatalf("first delivery failed: %v", err)
	}
	callsAfterFirst := prov.callCount()

	// Second delivery of the same message.
	if err := handle(svc, "order-7"); err != nil {
		t.Fatalf("second delivery returned error: %v", err)
	}
	// Provider must not have been called again.
	if prov.callCount() != callsAfterFirst {
		t.Error("provider must not be called again for already-dispatched shipment")
	}
}

// Scenario 8: existing shipment in failed state → permanent error, no re-dispatch.
func TestHandleFulfillment_ExistingFailedShipment(t *testing.T) {
	prov := newStub()
	store := newMemStore()
	sink := &captureSink{}
	store.putState("order-8", paidState(false))

	// Pre-insert a failed shipment.
	idemKey := "fulfill|order-8"
	store.mu.Lock()
	store.nextShipID++
	store.shipments[idemKey] = &ShipmentRow{
		ID:             fmt.Sprintf("ship-%03d", store.nextShipID),
		OrderID:        "order-8",
		Status:         ShipmentFailed,
		IdempotencyKey: idemKey,
		Provider:       "stub-fulfillment",
	}
	store.mu.Unlock()

	svc := buildSvc(prov, store, sink)
	err := handle(svc, "order-8")
	assertPermanent(t, err, "already failed shipment")
	if prov.callCount() != 0 {
		t.Error("provider must not be called when shipment is already failed")
	}
}

// Scenario 9: transient provider error is retryable.
func TestHandleFulfillment_TransientProviderError(t *testing.T) {
	prov := newStub()
	prov.dispatchFn = func(_ DispatchRequest) (DispatchResult, error) {
		return DispatchResult{}, fmt.Errorf("%w: carrier timeout", ErrProviderTransient)
	}
	store := newMemStore()
	sink := &captureSink{}
	store.putState("order-9", paidState(false))

	svc := buildSvc(prov, store, sink)
	err := handle(svc, "order-9")
	if err == nil {
		t.Fatal("expected non-nil error for transient failure")
	}
	if workers.Classify(err) == workers.RetryPermanent {
		t.Error("transient error must not be classified as permanent")
	}

	ship := store.shipmentByOrder("order-9")
	if ship.AttemptCount != 1 {
		t.Errorf("AttemptCount = %d after transient, want 1", ship.AttemptCount)
	}
	if ship.Status == ShipmentDispatched || ship.Status == ShipmentFailed {
		t.Errorf("status must remain pending after transient, got %q", ship.Status)
	}
}

// Scenario 10: permanent provider error marks shipment failed.
func TestHandleFulfillment_PermanentProviderError(t *testing.T) {
	prov := newStub()
	prov.dispatchFn = func(_ DispatchRequest) (DispatchResult, error) {
		return DispatchResult{}, fmt.Errorf("%w: invalid address", ErrProviderPermanent)
	}
	store := newMemStore()
	sink := &captureSink{}
	store.putState("order-10", paidState(false))

	svc := buildSvc(prov, store, sink)
	err := handle(svc, "order-10")
	assertPermanent(t, err, "permanent dispatch failure")

	ship := store.shipmentByOrder("order-10")
	if ship.Status != ShipmentFailed {
		t.Errorf("status = %q, want %q", ship.Status, ShipmentFailed)
	}
}

// Scenario 11: order.fulfilled emitted on success.
func TestHandleFulfillment_FulfilledEventEmitted(t *testing.T) {
	prov := newStub()
	store := newMemStore()
	sink := &captureSink{}
	store.putState("order-11", paidState(false))

	svc := buildSvc(prov, store, sink)
	_ = handle(svc, "order-11")

	fulfilled := sink.byType(EventFulfilled)
	if len(fulfilled) != 1 {
		t.Fatalf("expected 1 order.fulfilled event, got %d", len(fulfilled))
	}
	var payload map[string]any
	_ = json.Unmarshal(fulfilled[0].Payload, &payload)
	if payload["order_id"] != "order-11" {
		t.Errorf("event order_id = %v, want %q", payload["order_id"], "order-11")
	}
}

// Scenario 12: order.fulfillment.failed emitted on permanent failure.
func TestHandleFulfillment_FulfillmentFailedEventEmitted(t *testing.T) {
	prov := newStub()
	prov.dispatchFn = func(_ DispatchRequest) (DispatchResult, error) {
		return DispatchResult{}, fmt.Errorf("%w: rejected", ErrProviderPermanent)
	}
	store := newMemStore()
	sink := &captureSink{}
	store.putState("order-12", paidState(false))

	svc := buildSvc(prov, store, sink)
	_ = handle(svc, "order-12")

	failed := sink.byType(EventFulfillmentFailed)
	if len(failed) != 1 {
		t.Fatalf("expected 1 order.fulfillment.failed event, got %d", len(failed))
	}
}

// Scenario 13: shipped email event emitted when ShippedEmailMessageID is set.
func TestHandleFulfillment_EmailSendRequestedEmitted(t *testing.T) {
	prov := newStub()
	store := newMemStore()
	sink := &captureSink{}
	store.putState("order-13", paidState(false))

	// Pre-insert shipment with a shipped_email_message_id.
	idemKey := "fulfill|order-13"
	store.mu.Lock()
	store.nextShipID++
	store.shipments[idemKey] = &ShipmentRow{
		ID:                    fmt.Sprintf("ship-%03d", store.nextShipID),
		OrderID:               "order-13",
		Status:                ShipmentPending,
		IdempotencyKey:        idemKey,
		Provider:              "stub-fulfillment",
		ShippedEmailMessageID: "email-msg-ship-001",
	}
	store.mu.Unlock()

	svc := buildSvc(prov, store, sink)
	if err := handle(svc, "order-13"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	emails := sink.byType(EventEmailSendRequested)
	if len(emails) != 1 {
		t.Fatalf("expected 1 email.send.requested, got %d", len(emails))
	}
	var payload map[string]any
	_ = json.Unmarshal(emails[0].Payload, &payload)
	if payload["email_message_id"] != "email-msg-ship-001" {
		t.Errorf("email_message_id = %v", payload["email_message_id"])
	}

	// email template_data must be populated.
	data, ok := store.emailUpdates["email-msg-ship-001"]
	if !ok {
		t.Fatal("UpdateEmailTemplateData was not called")
	}
	if data.Carrier != "Stub Carrier" {
		t.Errorf("template data Carrier = %q, want %q", data.Carrier, "Stub Carrier")
	}
	if data.TrackingURL == "" {
		t.Error("template data TrackingURL must be set (stored in email row, not in events)")
	}
}

// Scenario 14: correlation/causation chain preserved in emitted events.
func TestHandleFulfillment_EventCorrelationChain(t *testing.T) {
	prov := newStub()
	store := newMemStore()
	sink := &captureSink{}
	store.putState("order-14", paidState(false))

	svc := buildSvc(prov, store, sink)
	env := newFulfillEnv("order-14")
	env.CorrelationID = "corr-xyz"
	p := FulfillmentPayload{OrderID: "order-14"}
	_ = svc.HandleFulfillment(context.Background(), nil, env, p)

	fulfilled := sink.byType(EventFulfilled)
	if len(fulfilled) == 0 {
		t.Fatal("expected order.fulfilled event")
	}
	if fulfilled[0].CorrelationID != "corr-xyz" {
		t.Errorf("CorrelationID = %q, want %q", fulfilled[0].CorrelationID, "corr-xyz")
	}
	if fulfilled[0].CausationID == nil || *fulfilled[0].CausationID != env.MessageID {
		t.Errorf("CausationID = %v, want %q", fulfilled[0].CausationID, env.MessageID)
	}
}

// Scenario 15: no sensitive address/customer/fiscal data in emitted events.
func TestHandleFulfillment_NoSensitiveDataInEvents(t *testing.T) {
	prov := newStub()
	store := newMemStore()
	sink := &captureSink{}
	store.putState("order-15", paidState(true))

	svc := buildSvc(prov, store, sink)
	_ = handle(svc, "order-15")

	sensitiveTerms := []string{
		"tracking_url",       // must not appear in any event payload
		"fake-carrier.local", // TrackingURL domain
		"customer",           // no customer fields
		"address",            // no address fields
		"cpf", "cnpj",        // no fiscal identifiers
		"stub-carrier.local", // TrackingURL from stub
	}

	for _, ev := range sink.events {
		raw := strings.ToLower(string(ev.Payload))
		for _, term := range sensitiveTerms {
			if strings.Contains(raw, term) {
				t.Errorf("event %q payload contains sensitive term %q: %s",
					ev.EventType, term, ev.Payload)
			}
		}
	}
}

// ---- helpers -------------------------------------------------------------

func assertPermanent(t *testing.T, err error, label string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: expected error, got nil", label)
	}
	var pe *workers.PermanentError
	if !errors.As(err, &pe) {
		t.Errorf("%s: expected PermanentError, got %T: %v", label, err, err)
	}
}
