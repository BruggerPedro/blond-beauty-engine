// Package contract validates that the Engine's consumed and emitted message
// payload shapes are consistent with the API outbox contract.
//
// These tests do NOT hit the database or RabbitMQ; they verify JSON round-trips
// and structural invariants of every message type crossing the API↔Engine boundary.
//
// When the API changes a payload field, update these examples first — a failing
// test here means the Engine and API are out of sync.
package contract_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/blondbeauty/blond-beauty-engine/internal/email"
	"github.com/blondbeauty/blond-beauty-engine/internal/fiscal"
	"github.com/blondbeauty/blond-beauty-engine/internal/fulfillment"
	"github.com/blondbeauty/blond-beauty-engine/internal/message"
	"github.com/blondbeauty/blond-beauty-engine/internal/payments"
)

// canonicalEnvelope wraps a payload JSON string in a minimal valid envelope
// matching what the API writes to outbox_messages.
func canonicalEnvelope(t *testing.T, eventType, aggregateType, aggregateID, payloadJSON string) []byte {
	t.Helper()
	e := message.Envelope{
		MessageID:     "00000000-0000-0000-0000-000000000001",
		CorrelationID: "00000000-0000-0000-0000-000000000002",
		EventType:     eventType,
		AggregateType: aggregateType,
		AggregateID:   "00000000-0000-0000-0000-000000000003",
		SchemaVersion: 1,
		OccurredAt:    time.Date(2026, 5, 3, 12, 0, 0, 0, time.UTC),
		Payload:       json.RawMessage(payloadJSON),
	}
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	return b
}

func decodeEnvelope(t *testing.T, raw []byte) *message.Envelope {
	t.Helper()
	env, err := message.Decode(raw)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	return env
}

// ---- payments ---------------------------------------------------------------

func TestConsumed_PaymentCreate(t *testing.T) {
	raw := canonicalEnvelope(t,
		"payment.create.requested", "payment", "pay-001",
		`{"payment_id":"pay-001","method":"credit_card","return_url":"https://example.com/return"}`,
	)
	env := decodeEnvelope(t, raw)

	var p payments.CreatePayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		t.Fatalf("unmarshal CreatePayload: %v", err)
	}
	if p.PaymentID != "pay-001" {
		t.Errorf("payment_id = %q, want %q", p.PaymentID, "pay-001")
	}
	if p.Method != "credit_card" {
		t.Errorf("method = %q, want %q", p.Method, "credit_card")
	}
}

func TestConsumed_PaymentCreate_MinimalPayload(t *testing.T) {
	// The API may send only payment_id; all other fields are optional.
	raw := canonicalEnvelope(t,
		"payment.create.requested", "payment", "pay-002",
		`{"payment_id":"pay-002"}`,
	)
	env := decodeEnvelope(t, raw)

	var p payments.CreatePayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		t.Fatalf("unmarshal CreatePayload minimal: %v", err)
	}
	if p.PaymentID != "pay-002" {
		t.Errorf("payment_id = %q, want %q", p.PaymentID, "pay-002")
	}
	if p.Method != "" || p.ReturnURL != "" {
		t.Errorf("unexpected non-empty optional fields: method=%q return_url=%q", p.Method, p.ReturnURL)
	}
}

func TestConsumed_PaymentConfirmReconcile(t *testing.T) {
	// confirm and reconcile both use RefByPayment.
	for _, eventType := range []string{"payment.confirm.requested", "payment.reconcile.requested"} {
		raw := canonicalEnvelope(t, eventType, "payment", "pay-003",
			`{"payment_id":"pay-003"}`,
		)
		env := decodeEnvelope(t, raw)

		var p payments.RefByPayment
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			t.Fatalf("%s: unmarshal RefByPayment: %v", eventType, err)
		}
		if p.PaymentID != "pay-003" {
			t.Errorf("%s: payment_id = %q, want %q", eventType, p.PaymentID, "pay-003")
		}
	}
}

func TestConsumed_PaymentCapture(t *testing.T) {
	raw := canonicalEnvelope(t,
		"payment.capture.requested", "payment", "pay-004",
		`{"payment_id":"pay-004","amount_cents":5000}`,
	)
	env := decodeEnvelope(t, raw)

	var p payments.CapturePayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		t.Fatalf("unmarshal CapturePayload: %v", err)
	}
	if p.PaymentID != "pay-004" {
		t.Errorf("payment_id = %q, want %q", p.PaymentID, "pay-004")
	}
	if p.AmountCents != 5000 {
		t.Errorf("amount_cents = %d, want 5000", p.AmountCents)
	}
}

func TestConsumed_PaymentCancel(t *testing.T) {
	raw := canonicalEnvelope(t,
		"payment.cancel.requested", "payment", "pay-005",
		`{"payment_id":"pay-005","reason":"customer request"}`,
	)
	env := decodeEnvelope(t, raw)

	var p payments.CancelPayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		t.Fatalf("unmarshal CancelPayload: %v", err)
	}
	if p.PaymentID != "pay-005" || p.Reason != "customer request" {
		t.Errorf("unexpected fields: payment_id=%q reason=%q", p.PaymentID, p.Reason)
	}
}

func TestConsumed_PaymentRefund(t *testing.T) {
	raw := canonicalEnvelope(t,
		"payment.refund.requested", "payment", "pay-006",
		`{"payment_id":"pay-006","amount_cents":0,"reason":"damaged item"}`,
	)
	env := decodeEnvelope(t, raw)

	var p payments.RefundPayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		t.Fatalf("unmarshal RefundPayload: %v", err)
	}
	if p.PaymentID != "pay-006" {
		t.Errorf("payment_id = %q, want %q", p.PaymentID, "pay-006")
	}
	if p.AmountCents != 0 {
		t.Errorf("amount_cents = %d, want 0 (full refund)", p.AmountCents)
	}
}

func TestConsumed_WebhookReceived(t *testing.T) {
	raw := canonicalEnvelope(t,
		"payment.webhook.received", "payment", "pay-007",
		`{"ingress_id":"igr-001"}`,
	)
	env := decodeEnvelope(t, raw)

	var p payments.WebhookPayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		t.Fatalf("unmarshal WebhookPayload: %v", err)
	}
	if p.IngressID != "igr-001" {
		t.Errorf("ingress_id = %q, want %q", p.IngressID, "igr-001")
	}
}

// ---- email ------------------------------------------------------------------

func TestConsumed_EmailSendRequested(t *testing.T) {
	raw := canonicalEnvelope(t,
		"email.send.requested", "email_message", "msg-001",
		`{"email_message_id":"msg-001"}`,
	)
	env := decodeEnvelope(t, raw)

	var p email.SendPayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		t.Fatalf("unmarshal SendPayload: %v", err)
	}
	if p.EmailMessageID != "msg-001" {
		t.Errorf("email_message_id = %q, want %q", p.EmailMessageID, "msg-001")
	}
}

// ---- fiscal -----------------------------------------------------------------

func TestConsumed_FiscalIssue(t *testing.T) {
	for _, eventType := range []string{"fiscal.issue.requested", "fiscal.retry.requested"} {
		raw := canonicalEnvelope(t, eventType, "fiscal_invoice", "inv-001",
			`{"fiscal_invoice_id":"inv-001"}`,
		)
		env := decodeEnvelope(t, raw)

		var p fiscal.IssuePayload
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			t.Fatalf("%s: unmarshal IssuePayload: %v", eventType, err)
		}
		if p.FiscalInvoiceID != "inv-001" {
			t.Errorf("%s: fiscal_invoice_id = %q, want %q", eventType, p.FiscalInvoiceID, "inv-001")
		}
	}
}

func TestConsumed_FiscalCancel(t *testing.T) {
	raw := canonicalEnvelope(t,
		"fiscal.cancel.requested", "fiscal_invoice", "inv-002",
		`{"fiscal_invoice_id":"inv-002","reason":"order cancelled"}`,
	)
	env := decodeEnvelope(t, raw)

	var p fiscal.CancelPayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		t.Fatalf("unmarshal CancelPayload: %v", err)
	}
	if p.FiscalInvoiceID != "inv-002" {
		t.Errorf("fiscal_invoice_id = %q, want %q", p.FiscalInvoiceID, "inv-002")
	}
	if p.Reason != "order cancelled" {
		t.Errorf("reason = %q, want %q", p.Reason, "order cancelled")
	}
}

// ---- fulfillment ------------------------------------------------------------

func TestConsumed_OrderFulfillmentRequested(t *testing.T) {
	// Emitted by the fiscal worker after authorization.
	raw := canonicalEnvelope(t,
		"order.fulfillment.requested", "order", "ord-001",
		`{"order_id":"ord-001","fiscal_invoice_id":"inv-001","access_key":"44444444444444444444444444444444444444444444"}`,
	)
	env := decodeEnvelope(t, raw)

	var p fulfillment.FulfillmentPayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		t.Fatalf("unmarshal FulfillmentPayload: %v", err)
	}
	if p.OrderID != "ord-001" {
		t.Errorf("order_id = %q, want %q", p.OrderID, "ord-001")
	}
	// fiscal_invoice_id is informational; engine re-reads DB state.
	if p.FiscalInvoiceID != "inv-001" {
		t.Errorf("fiscal_invoice_id = %q, want %q", p.FiscalInvoiceID, "inv-001")
	}
}

func TestConsumed_OrderFulfillmentRequested_NoFiscal(t *testing.T) {
	// Orders that do not require fiscal (future: digital goods) omit fiscal_invoice_id.
	raw := canonicalEnvelope(t,
		"order.fulfillment.requested", "order", "ord-002",
		`{"order_id":"ord-002"}`,
	)
	env := decodeEnvelope(t, raw)

	var p fulfillment.FulfillmentPayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		t.Fatalf("unmarshal FulfillmentPayload (no fiscal): %v", err)
	}
	if p.FiscalInvoiceID != "" {
		t.Errorf("expected empty fiscal_invoice_id, got %q", p.FiscalInvoiceID)
	}
}

// ---- envelope invariants ----------------------------------------------------

func TestEnvelopeValidation_MissingMessageID(t *testing.T) {
	b := []byte(`{
		"message_id":"",
		"correlation_id":"00000000-0000-0000-0000-000000000002",
		"event_type":"payment.create.requested",
		"aggregate_type":"payment",
		"aggregate_id":"pay-001",
		"schema_version":1,
		"occurred_at":"2026-05-03T12:00:00Z",
		"payload":{"payment_id":"pay-001"}
	}`)
	_, err := message.Decode(b)
	if err == nil {
		t.Error("expected error for empty message_id, got nil")
	}
}

func TestEnvelopeValidation_ZeroSchemaVersion(t *testing.T) {
	b := []byte(`{
		"message_id":"00000000-0000-0000-0000-000000000001",
		"correlation_id":"00000000-0000-0000-0000-000000000002",
		"event_type":"payment.create.requested",
		"aggregate_type":"payment",
		"aggregate_id":"pay-001",
		"schema_version":0,
		"occurred_at":"2026-05-03T12:00:00Z",
		"payload":{"payment_id":"pay-001"}
	}`)
	_, err := message.Decode(b)
	if err == nil {
		t.Error("expected error for schema_version=0, got nil")
	}
}

func TestEnvelopeValidation_UnknownFieldsIgnored(t *testing.T) {
	// Additive API changes (new optional fields) must not break the Engine.
	b := []byte(`{
		"message_id":"00000000-0000-0000-0000-000000000001",
		"correlation_id":"00000000-0000-0000-0000-000000000002",
		"event_type":"payment.create.requested",
		"aggregate_type":"payment",
		"aggregate_id":"pay-001",
		"schema_version":1,
		"occurred_at":"2026-05-03T12:00:00Z",
		"payload":{"payment_id":"pay-001"},
		"future_field":"ignored"
	}`)
	env, err := message.Decode(b)
	if err != nil {
		t.Fatalf("unexpected error on forward-compatible envelope: %v", err)
	}
	if env.EventType != "payment.create.requested" {
		t.Errorf("event_type = %q", env.EventType)
	}
}
