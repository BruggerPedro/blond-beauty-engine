package email

import (
	"context"
	"errors"
)

// Message is the canonical, provider-agnostic send request. It contains
// only the data needed to dispatch the message. Implementations MUST NOT
// store or log Body: it may contain PII, reset links, or fiscal data.
type Message struct {
	// To is the validated recipient address. NEVER log this field — log
	// RecipientHash from the MessageRow instead.
	To      string
	Subject string
	// TextBody is the plain-text rendering of the template.
	TextBody string
	// HTMLBody is optional enriched rendering; empty for Slice D templates.
	HTMLBody string
	// IdempotencyKey is a stable per-(email_message_id, attempt) key used
	// to deduplicate sends at the provider level when supported.
	IdempotencyKey string
}

// ProviderResult is the successful send outcome.
type ProviderResult struct {
	// ProviderMessageID is the ID assigned by the provider (e.g. SES MessageId,
	// SendGrid x-message-id). Empty for providers that don't expose one.
	ProviderMessageID string
}

// Provider is the canonical, provider-agnostic interface for email adapters.
// Implementations MUST:
//   - honour ctx deadlines;
//   - return ErrProviderPermanent for permanent failures (invalid address,
//     blocked domain, quota exceeded) and ErrProviderTransient for retryable
//     failures (5xx, timeout, rate-limit);
//   - NEVER log msg.To, msg.TextBody, or msg.HTMLBody.
type Provider interface {
	Name() string
	Send(ctx context.Context, msg Message) (ProviderResult, error)
}

// ErrProviderPermanent signals a non-retryable send failure. Wrap with %w.
var ErrProviderPermanent = errors.New("email provider permanent failure")

// ErrProviderTransient signals a retryable infrastructure failure. Wrap with %w.
var ErrProviderTransient = errors.New("email provider transient failure")
