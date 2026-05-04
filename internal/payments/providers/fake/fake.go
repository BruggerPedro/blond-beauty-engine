// Package fake is a deterministic in-memory PaymentProvider used for local
// development and tests. It is registered by name "fake" via the registry.
//
// # Outbound call behaviour (amount-driven)
//
//	amount_cents % 1000 == 13  → ErrProviderRejected (permanent)
//	amount_cents % 1000 == 17  → ErrProviderTransient (retryable)
//	amount_cents % 1000 == 23  → Status = requires_action with redirect URL
//	otherwise                  → Status = authorized
//
// # Webhook verification
//
// The fake adapter signs by writing the hex-encoded SHA-256 of the raw body
// into the header X-Fake-Signature. The API ingress must store this header.
// Tests can generate a valid signature with SignBody.
//
//	X-Fake-Signature: <sha256(rawBody) in hex>
//
// # Webhook parse
//
// The raw body must be a JSON object with the following fields:
//
//	{
//	  "event_id":            "evt_xxx",          // provider event identifier
//	  "provider_payment_id": "fake_xxx",
//	  "event_type":          "payment.paid",      // provider-specific name
//	  "status":              "paid",              // canonical status string
//	  "occurred_at":         "2026-05-02T…Z"
//	}
//
// SeparateCapture is true; full lifecycle: Create (authorized) →
// Capture (paid) → Refund (refunded) or Cancel.
package fake

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/blondbeauty/blond-beauty-engine/internal/payments"
)

const (
	Name      = "fake"
	sigHeader = "x-fake-signature" // lower-cased canonical form
)

type Provider struct {
	mu       sync.Mutex
	statuses map[string]payments.Status
}

func New() *Provider {
	return &Provider{statuses: map[string]payments.Status{}}
}

func (p *Provider) Name() string { return Name }

func (p *Provider) Capabilities() payments.Capabilities {
	return payments.Capabilities{
		Confirm:         true,
		SeparateCapture: true,
		PartialRefund:   true,
		Cancel:          true,
		Webhooks:        true,
	}
}

// ---- outbound payment operations ----

func (p *Provider) CreatePayment(_ context.Context, req payments.CreateRequest) (payments.Result, error) {
	switch req.Ref.Amount.AmountCents % 1000 {
	case 13:
		return p.rejected(payments.OpCreate, "card_declined")
	case 17:
		return p.transient(payments.OpCreate, "upstream_5xx")
	}
	id := "fake_" + uuid.NewString()
	st := payments.StatusAuthorized
	var next *payments.NextAction
	if req.Ref.Amount.AmountCents%1000 == 23 {
		st = payments.StatusRequiresAction
		next = &payments.NextAction{Type: "redirect", URL: "https://fake.local/3ds/" + id}
	}
	p.set(id, st)
	return p.ok(payments.OpCreate, id, st, next, req.Ref.Amount), nil
}

func (p *Provider) ConfirmPayment(_ context.Context, req payments.ConfirmRequest) (payments.Result, error) {
	cur := p.get(req.Ref.ProviderPaymentID)
	if cur != payments.StatusRequiresAction && cur != payments.StatusAuthorized {
		return payments.Result{}, fmt.Errorf("%w: confirm requires authorized/requires_action, got %q",
			payments.ErrIllegalTransition, cur)
	}
	p.set(req.Ref.ProviderPaymentID, payments.StatusAuthorized)
	return p.ok(payments.OpConfirm, req.Ref.ProviderPaymentID, payments.StatusAuthorized, nil, req.Ref.Amount), nil
}

func (p *Provider) CapturePayment(_ context.Context, req payments.CaptureRequest) (payments.Result, error) {
	cur := p.get(req.Ref.ProviderPaymentID)
	if cur != payments.StatusAuthorized {
		return payments.Result{}, fmt.Errorf("%w: capture requires authorized, got %q",
			payments.ErrIllegalTransition, cur)
	}
	p.set(req.Ref.ProviderPaymentID, payments.StatusPaid)
	return p.ok(payments.OpCapture, req.Ref.ProviderPaymentID, payments.StatusPaid, nil, req.Amount), nil
}

func (p *Provider) CancelPayment(_ context.Context, req payments.CancelRequest) (payments.Result, error) {
	cur := p.get(req.Ref.ProviderPaymentID)
	if cur == payments.StatusPaid || cur == payments.StatusRefunded || cur == payments.StatusCancelled {
		return payments.Result{}, fmt.Errorf("%w: cancel from %q not allowed",
			payments.ErrIllegalTransition, cur)
	}
	p.set(req.Ref.ProviderPaymentID, payments.StatusCancelled)
	return p.ok(payments.OpCancel, req.Ref.ProviderPaymentID, payments.StatusCancelled, nil, req.Ref.Amount), nil
}

func (p *Provider) RefundPayment(_ context.Context, req payments.RefundRequest) (payments.Result, error) {
	cur := p.get(req.Ref.ProviderPaymentID)
	if cur != payments.StatusPaid && cur != payments.StatusCaptured {
		return payments.Result{}, fmt.Errorf("%w: refund requires paid/captured, got %q",
			payments.ErrIllegalTransition, cur)
	}
	p.set(req.Ref.ProviderPaymentID, payments.StatusRefunded)
	return p.ok(payments.OpRefund, req.Ref.ProviderPaymentID, payments.StatusRefunded, nil, req.Amount), nil
}

func (p *Provider) GetPaymentStatus(_ context.Context, id string) (payments.Result, error) {
	st := p.get(id)
	if st == "" {
		st = payments.StatusUnknown
	}
	body, _ := json.Marshal(map[string]any{"provider_payment_id": id, "status": st})
	return payments.Result{
		Status:            st,
		ProviderPaymentID: id,
		RedactedRequest:   json.RawMessage(`{"op":"reconcile"}`),
		RedactedResponse:  body,
	}, nil
}

// ---- webhook ----

// SignBody returns the header value that VerifyWebhook expects.
// Tests use this to produce a valid X-Fake-Signature.
func SignBody(body []byte) string {
	h := sha256.Sum256(body)
	return hex.EncodeToString(h[:])
}

// VerifyWebhook checks X-Fake-Signature == sha256(rawBody).
func (p *Provider) VerifyWebhook(_ context.Context, headers map[string]string, rawBody []byte) error {
	got := headers[sigHeader]
	if got == "" {
		return fmt.Errorf("%w: missing %s header", payments.ErrWebhookSignatureInvalid, sigHeader)
	}
	gotDigest, err := hex.DecodeString(got)
	if err != nil || len(gotDigest) != sha256.Size {
		return fmt.Errorf("%w: malformed signature", payments.ErrWebhookSignatureInvalid)
	}
	want := sha256.Sum256(rawBody)
	if !hmac.Equal(gotDigest, want[:]) {
		return fmt.Errorf("%w: signature mismatch", payments.ErrWebhookSignatureInvalid)
	}
	return nil
}

// fakeWebhookBody is the expected shape of the fake provider's webhook body.
type fakeWebhookBody struct {
	EventID           string `json:"event_id"`
	ProviderPaymentID string `json:"provider_payment_id"`
	EventType         string `json:"event_type"`
	Status            string `json:"status"`
	OccurredAt        string `json:"occurred_at"`
}

// ParseWebhook decodes a verified fake webhook body.
func (p *Provider) ParseWebhook(_ context.Context, _ map[string]string, rawBody []byte) (payments.WebhookEvent, error) {
	var b fakeWebhookBody
	if err := json.Unmarshal(rawBody, &b); err != nil {
		return payments.WebhookEvent{}, fmt.Errorf("%w: %v", payments.ErrWebhookParseFailed, err)
	}
	if b.EventID == "" || b.ProviderPaymentID == "" || b.Status == "" {
		return payments.WebhookEvent{}, fmt.Errorf("%w: missing required fields", payments.ErrWebhookParseFailed)
	}
	st := payments.Status(b.Status)
	if !st.Valid() {
		return payments.WebhookEvent{}, fmt.Errorf("%w: unknown status %q", payments.ErrWebhookParseFailed, b.Status)
	}
	var occurredAt time.Time
	if b.OccurredAt != "" {
		t, err := time.Parse(time.RFC3339, b.OccurredAt)
		if err != nil {
			return payments.WebhookEvent{}, fmt.Errorf("%w: bad occurred_at: %v", payments.ErrWebhookParseFailed, err)
		}
		occurredAt = t
	} else {
		occurredAt = time.Now().UTC()
	}
	redacted, _ := json.Marshal(map[string]any{
		"event_id":            b.EventID,
		"provider_payment_id": b.ProviderPaymentID,
		"event_type":          b.EventType,
		"status":              b.Status,
	})
	return payments.WebhookEvent{
		ProviderEventID:   b.EventID,
		ProviderPaymentID: b.ProviderPaymentID,
		EventType:         b.EventType,
		CanonicalStatus:   st,
		OccurredAt:        occurredAt,
		RedactedPayload:   redacted,
	}, nil
}

// SetStatus lets tests force a provider-side state (e.g., simulating a webhook).
func (p *Provider) SetStatus(id string, st payments.Status) { p.set(id, st) }

// ---- helpers ----

func (p *Provider) get(id string) payments.Status {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.statuses[id]
}

func (p *Provider) set(id string, st payments.Status) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.statuses[id] = st
}

func (p *Provider) ok(op payments.Operation, id string, st payments.Status, next *payments.NextAction, amount payments.Money) payments.Result {
	req, _ := json.Marshal(map[string]any{"op": string(op), "amount_cents": amount.AmountCents, "currency": amount.Currency})
	resp, _ := json.Marshal(map[string]any{"provider_payment_id": id, "status": st})
	return payments.Result{Status: st, ProviderPaymentID: id, NextAction: next, RedactedRequest: req, RedactedResponse: resp}
}

func (p *Provider) rejected(op payments.Operation, code string) (payments.Result, error) {
	resp, _ := json.Marshal(map[string]any{"error_code": code, "status": payments.StatusFailed})
	return payments.Result{
		Status:           payments.StatusFailed,
		RedactedRequest:  json.RawMessage(`{"op":"` + string(op) + `"}`),
		RedactedResponse: resp,
	}, fmt.Errorf("%w: %s", payments.ErrProviderRejected, code)
}

func (p *Provider) transient(op payments.Operation, code string) (payments.Result, error) {
	return payments.Result{
		RedactedRequest:  json.RawMessage(`{"op":"` + string(op) + `"}`),
		RedactedResponse: json.RawMessage(`{"error":"` + code + `"}`),
	}, fmt.Errorf("%w: %s", payments.ErrProviderTransient, code)
}
