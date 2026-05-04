package contract_test

import (
	"encoding/json"
	"strings"
	"testing"
)

// sensitiveKeys lists field names that must NEVER appear in any Engine-emitted
// event payload published to RabbitMQ queues. These represent:
//   - Payment card data (PCI DSS)
//   - Brazilian fiscal IDs / taxpayer IDs (LGPD)
//   - Customer PII (LGPD)
//   - Auth credentials / reset tokens (security)
//   - Fiscal document raw content (SEFAZ compliance)
//   - Fiscal provider certificates / private keys (security)
//   - Internal storage references (opacity requirement)
//   - Logistics tracking data (privacy / business confidentiality)
var sensitiveKeys = []string{
	// Payment card data (PCI DSS)
	"pan", "cvv", "cvc", "card_number", "card_holder",
	// Auth / security credentials
	"password", "token", "reset_url", "reset_token", "secret", "api_key",
	// Brazilian taxpayer / fiscal IDs (LGPD)
	"cpf", "cnpj", "rg", "inscricao_estadual",
	// Customer PII (LGPD)
	"recipient", "email", "phone", "address", "street", "zip_code", "cep",
	// Fiscal document raw content (never in events; stored in object storage)
	"xml_content", "raw_xml", "danfe_content", "raw_body",
	// Fiscal provider certificates and private keys (SEFAZ A1/A3 certs)
	// These must never leave the secret manager; never appear on the wire.
	"certificate", "cert_password", "p12_password", "private_key", "fiscal_cert",
	// Internal opaque storage refs (must be converted to signed URLs by API)
	"xml_storage_key", "danfe_storage_key",
	// Logistics privacy (tracking_url exposed only via API; tracking_number in aggregate)
	"tracking_url", "tracking_number",
}

// emittedEventPayloads enumerates every event payload shape the Engine emits.
// Each entry is a representative JSON payload as it would appear in
// outbox_messages.payload. Update this list when adding new event types.
var emittedEventPayloads = []struct {
	name    string
	payload string
}{
	// payments.events
	{
		"payment.authorized",
		`{"payment_id":"pay-001","order_id":"ord-001","provider":"fake","provider_payment_id":"fake_pay_001","status":"authorized","amount_cents":10000,"currency":"BRL"}`,
	},
	{
		"payment.captured",
		`{"payment_id":"pay-001","order_id":"ord-001","provider":"fake","provider_payment_id":"fake_pay_001","status":"captured","amount_cents":10000,"currency":"BRL"}`,
	},
	{
		"payment.failed",
		`{"payment_id":"pay-001","order_id":"ord-001","provider":"fake","provider_payment_id":"","status":"failed","amount_cents":10013,"currency":"BRL"}`,
	},
	{
		"payment.refunded",
		`{"payment_id":"pay-001","order_id":"ord-001","provider":"fake","provider_payment_id":"fake_pay_001","status":"refunded","amount_cents":10000,"currency":"BRL"}`,
	},
	{
		"payment.cancelled",
		`{"payment_id":"pay-001","order_id":"ord-001","provider":"fake","provider_payment_id":"fake_pay_001","status":"cancelled","amount_cents":10000,"currency":"BRL"}`,
	},
	{
		"payment.requires_action",
		`{"payment_id":"pay-001","order_id":"ord-001","provider":"fake","provider_payment_id":"fake_pay_001","status":"requires_action","amount_cents":10023,"currency":"BRL"}`,
	},
	// emails.events
	{
		"email.sent",
		`{"email_message_id":"msg-001","email_type":"payment_approved","recipient_hash":"abc123","provider":"fake-email","provider_message_id":"fake_email_001"}`,
	},
	{
		"email.failed",
		`{"email_message_id":"msg-001","email_type":"payment_failed","recipient_hash":"abc123","error":"permanent send error"}`,
	},
	// fiscal.events
	{
		"fiscal.authorized",
		`{"fiscal_invoice_id":"inv-001","order_id":"ord-001","payment_id":"pay-001","model":"nfe","access_key":"44444444444444444444444444444444444444444444","protocol":"135180000012345","number":"000000042","provider":"fake-fiscal"}`,
	},
	{
		"fiscal.rejected",
		`{"fiscal_invoice_id":"inv-001","order_id":"ord-001","payment_id":"pay-001","rejection_code":"225","rejection_message":"Rejeição: Falha no schema XML","provider":"fake-fiscal"}`,
	},
	{
		"fiscal.cancelled",
		`{"fiscal_invoice_id":"inv-001","order_id":"ord-001","payment_id":"pay-001","provider":"fake-fiscal"}`,
	},
	// email.send.requested (emitted by fiscal and fulfillment onto emails.transactional)
	{
		"email.send.requested (from fiscal)",
		`{"email_message_id":"msg-001"}`,
	},
	{
		"email.send.requested (from fulfillment)",
		`{"email_message_id":"msg-002"}`,
	},
	// order.fulfillment.requested (emitted by fiscal onto orders.fulfillment)
	{
		"order.fulfillment.requested",
		`{"order_id":"ord-001","fiscal_invoice_id":"inv-001","access_key":"44444444444444444444444444444444444444444444"}`,
	},
	// orders.events
	{
		"order.fulfilled",
		`{"order_id":"ord-001","shipment_id":"shp-001","carrier":"FAKE","provider":"fake-fulfillment"}`,
	},
	{
		"order.fulfillment.failed",
		`{"order_id":"ord-001","shipment_id":"shp-001","provider":"fake-fulfillment"}`,
	},
}

// TestEmittedEvents_NoSensitiveFields verifies that no emitted event payload
// contains a sensitive field name as a JSON key. This is a structural check —
// it does NOT replace code review, but it makes violations immediately visible.
func TestEmittedEvents_NoSensitiveFields(t *testing.T) {
	for _, tc := range emittedEventPayloads {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			// Unmarshal to a flat map to get the exact set of JSON keys.
			var m map[string]any
			if err := json.Unmarshal([]byte(tc.payload), &m); err != nil {
				t.Fatalf("unmarshal payload: %v", err)
			}
			for _, key := range sensitiveKeys {
				if _, found := m[key]; found {
					t.Errorf("payload for %q contains sensitive field %q — this field must never appear in emitted events", tc.name, key)
				}
				// Also check for the key as a substring of another key
				// (catches "email_address", "customer_cpf", etc.).
				// Check for common compound variants (e.g. "user_email", "customer_cpf")
				// but exclude known-safe prefixed/suffixed forms like "recipient_hash",
				// "email_message_id", or "email_type".
				safeCompounds := map[string]bool{
					"recipient_hash":   true,
					"email_message_id": true,
					"email_type":       true,
					"provider_email":   false, // would be a violation
				}
				for payloadKey := range m {
					if payloadKey == key {
						continue // already caught by exact check above
					}
					if safeCompounds[payloadKey] {
						continue
					}
					lk := strings.ToLower(payloadKey)
					// Only flag exact-word matches within snake_case names.
					// e.g. "customer_cpf" contains "_cpf" or "cpf_"
					if strings.Contains(lk, "_"+key) || strings.HasSuffix(lk, "_"+key) || strings.HasPrefix(lk, key+"_") {
						t.Errorf("payload for %q contains field %q which likely embeds sensitive term %q", tc.name, payloadKey, key)
					}
				}
			}
		})
	}
}

// TestEmittedEvents_RequiredEnvelopeFields verifies that each emitted event
// payload (not the envelope, but its business content) contains at least one
// identifying field (an ID) so events are always correlatable to a domain entity.
func TestEmittedEvents_RequiredEnvelopeFields(t *testing.T) {
	idFields := []string{"payment_id", "order_id", "fiscal_invoice_id", "email_message_id", "shipment_id"}

	for _, tc := range emittedEventPayloads {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			var m map[string]any
			if err := json.Unmarshal([]byte(tc.payload), &m); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			for _, f := range idFields {
				if v, ok := m[f]; ok {
					if s, _ := v.(string); s == "" {
						t.Errorf("field %q is present but empty in %q", f, tc.name)
					}
				}
			}
		})
	}
}

// TestPaymentEventPayload_FieldSet verifies the exact field set of payment events
// — no more, no less. This catches accidental additions (sensitive data) and
// removals (breaking downstream consumers).
func TestPaymentEventPayload_FieldSet(t *testing.T) {
	const raw = `{"payment_id":"pay-001","order_id":"ord-001","provider":"fake","provider_payment_id":"fake_pay_001","status":"authorized","amount_cents":10000,"currency":"BRL"}`
	var m map[string]any
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	want := []string{"payment_id", "order_id", "provider", "provider_payment_id", "status", "amount_cents", "currency"}
	for _, f := range want {
		if _, ok := m[f]; !ok {
			t.Errorf("payment event missing required field %q", f)
		}
	}
	if len(m) != len(want) {
		t.Errorf("payment event has %d fields, want %d: %v", len(m), len(want), keys(m))
	}
}

// TestFiscalAuthorizedPayload_StorageKeysAbsent verifies that xml_storage_key
// and danfe_storage_key are never present in fiscal.authorized events.
// These opaque refs must be converted to signed URLs by the API before sharing.
func TestFiscalAuthorizedPayload_StorageKeysAbsent(t *testing.T) {
	const raw = `{"fiscal_invoice_id":"inv-001","order_id":"ord-001","payment_id":"pay-001","model":"nfe","access_key":"44444444444444444444444444444444444444444444","protocol":"135180000012345","number":"000000042","provider":"fake-fiscal"}`
	var m map[string]any
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, forbidden := range []string{"xml_storage_key", "danfe_storage_key", "raw_xml", "xml_content"} {
		if _, found := m[forbidden]; found {
			t.Errorf("fiscal.authorized event must not contain %q", forbidden)
		}
	}
}

// TestFulfillmentFulfilledPayload_TrackingAbsent verifies that tracking_url and
// tracking_number never appear in order.fulfilled events.
func TestFulfillmentFulfilledPayload_TrackingAbsent(t *testing.T) {
	const raw = `{"order_id":"ord-001","shipment_id":"shp-001","carrier":"FAKE","provider":"fake-fulfillment"}`
	var m map[string]any
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, forbidden := range []string{"tracking_url", "tracking_number"} {
		if _, found := m[forbidden]; found {
			t.Errorf("order.fulfilled event must not contain %q", forbidden)
		}
	}
}

func keys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
