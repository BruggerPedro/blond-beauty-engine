package payments

import "errors"

// Provider-facing errors. Adapters return these (optionally wrapped) so the
// service layer can decide retry vs DLQ without parsing strings.

// ErrProviderRejected is a permanent business rejection (e.g., insufficient
// funds, invalid card, fraud). Service layer must NOT retry; the message goes
// to DLQ via workers.Permanent.
var ErrProviderRejected = errors.New("payment provider rejected")

// ErrProviderTransient is a transient failure (network blip, 5xx, timeout).
// Service layer SHOULD retry per the worker framework's backoff policy.
var ErrProviderTransient = errors.New("payment provider transient failure")

// ErrIllegalTransition is a domain error: the requested operation is not
// valid for the payment's current status (e.g., refund a failed payment).
// Permanent — DLQ.
var ErrIllegalTransition = errors.New("illegal payment transition")

// ErrPaymentNotFound is returned when the referenced payment row does not
// exist. Permanent — DLQ. The API is the source of truth for payment row
// creation; if it's missing, retrying will not fix it.
var ErrPaymentNotFound = errors.New("payment not found")

// ErrUnknownProvider is returned when a payment row references a provider
// that is not registered. Permanent — DLQ until the adapter is shipped.
var ErrUnknownProvider = errors.New("unknown payment provider")

// ErrWebhookSignatureInvalid is returned by VerifyWebhook when the provider
// signature does not match. Permanent — the ingress must be marked rejected;
// no business state is mutated.
var ErrWebhookSignatureInvalid = errors.New("webhook signature invalid")

// ErrWebhookParseFailed is returned by ParseWebhook when the payload cannot
// be decoded into a known event shape. Permanent.
var ErrWebhookParseFailed = errors.New("webhook parse failed")
