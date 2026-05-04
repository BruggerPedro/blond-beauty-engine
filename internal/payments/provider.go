package payments

import (
	"context"
	"encoding/json"
	"time"
)

// Capabilities describes what an adapter supports. The service consults this
// before issuing operations a provider cannot perform (e.g., partial refunds).
type Capabilities struct {
	Confirm         bool
	SeparateCapture bool
	PartialRefund   bool
	Cancel          bool
	Webhooks        bool
}

// Money is integer cents in a single currency. Avoids float rounding issues.
type Money struct {
	AmountCents int64  `json:"amount_cents"`
	Currency    string `json:"currency"` // ISO 4217
}

// PaymentRef is the common identifying snapshot passed to every provider call.
type PaymentRef struct {
	PaymentID         string // engine/API canonical UUID
	OrderID           string
	ProviderPaymentID string // empty on Create; required on subsequent ops
	IdempotencyKey    string // stable per (payment_id, operation, message_id)
	Amount            Money
}

// Result is the canonical response from any provider operation. Raw provider
// data is captured in RedactedResponse for audit, but business code MUST only
// branch on Status.
type Result struct {
	Status            Status
	ProviderPaymentID string
	NextAction        *NextAction
	RedactedRequest   json.RawMessage
	RedactedResponse  json.RawMessage
}

// NextAction carries data needed to complete a requires_action step.
type NextAction struct {
	Type string `json:"type"` // "redirect" | "3ds_challenge" | "qr_code"
	URL  string `json:"url,omitempty"`
}

// CreateRequest is the input for an initial provider charge attempt. Customer
// PII is intentionally minimal; PAN/CVV must never appear in this struct.
type CreateRequest struct {
	Ref       PaymentRef
	Method    string // "credit_card" | "pix" | "boleto" | ...
	ReturnURL string
	Metadata  map[string]any // small, non-PII metadata
}

type ConfirmRequest struct {
	Ref PaymentRef
}

type CaptureRequest struct {
	Ref    PaymentRef
	Amount Money // capture amount; must be ≤ authorized amount
}

type CancelRequest struct {
	Ref    PaymentRef
	Reason string
}

type RefundRequest struct {
	Ref    PaymentRef
	Amount Money
	Reason string
}

// WebhookEvent is the canonical, provider-neutral representation of an
// inbound webhook notification. Adapters MUST populate all fields; if the
// provider does not supply a stable ProviderEventID the adapter MUST
// synthesise one from a deterministic hash of the payload so the engine's
// dedup constraint can work correctly.
type WebhookEvent struct {
	// ProviderEventID is the stable identifier issued by the provider for
	// this notification. Combined with Provider name it forms the dedup key.
	ProviderEventID string

	// ProviderPaymentID links the notification back to a payment row.
	// Required for status-change notifications. May be empty for account-level
	// notifications (not handled in Slice C).
	ProviderPaymentID string

	// EventType is the provider-specific event name for audit ("PAYMENT.PAID",
	// "charge.succeeded", etc.). Never branch business logic on this — use
	// CanonicalStatus instead.
	EventType string

	// CanonicalStatus is the engine-domain status this event maps to.
	CanonicalStatus Status

	OccurredAt      time.Time
	RedactedPayload json.RawMessage // safe-to-log, no PAN/CVV
}

// Provider is the canonical, provider-agnostic interface for payment adapters.
// Each external integration (fake, Getnet, Stripe, …) implements this in its
// own package. No business code outside the adapter imports provider-specific
// types.
//
// Implementations MUST:
//   - honour ctx deadlines and respect cancellation;
//   - return ErrProviderRejected for permanent business failures and
//     ErrProviderTransient for retryable infrastructure failures;
//   - never log secrets or sensitive payloads;
//   - populate RedactedRequest / RedactedResponse with safe JSON for audit.
type Provider interface {
	Name() string
	Capabilities() Capabilities

	CreatePayment(ctx context.Context, req CreateRequest) (Result, error)
	ConfirmPayment(ctx context.Context, req ConfirmRequest) (Result, error)
	CapturePayment(ctx context.Context, req CaptureRequest) (Result, error)
	CancelPayment(ctx context.Context, req CancelRequest) (Result, error)
	RefundPayment(ctx context.Context, req RefundRequest) (Result, error)

	// GetPaymentStatus performs a read-only provider-side lookup. Used by the
	// reconcile worker and by the webhook handler for out-of-order resolution.
	GetPaymentStatus(ctx context.Context, providerPaymentID string) (Result, error)

	// VerifyWebhook authenticates the raw inbound notification before any
	// state mutation is allowed. headers is the canonicalised (single-value)
	// HTTP header map as stored by the API ingress. Returns nil on success or
	// ErrWebhookSignatureInvalid (permanent) on failure.
	VerifyWebhook(ctx context.Context, headers map[string]string, rawBody []byte) error

	// ParseWebhook decodes an authenticated notification into a canonical
	// WebhookEvent. MUST NOT be called before VerifyWebhook succeeds.
	// Returns ErrWebhookParseFailed (permanent) when the body cannot be
	// decoded into a known event shape.
	ParseWebhook(ctx context.Context, headers map[string]string, rawBody []byte) (WebhookEvent, error)
}
