// Package email owns the transactional email worker: provider interface,
// template registry, service, and fake adapter. It never exposes public HTTP.
// Real SMTP/SES/Resend adapters are out of scope for this slice.
package email

// EmailType is the canonical name for a transactional email template.
// The API writes this into email_messages.email_type; the Engine resolves
// a template by this value. Unknown types are permanent errors → DLQ.
type EmailType string

const (
	TypeOrderConfirmation EmailType = "order_confirmation"
	TypePaymentApproved   EmailType = "payment_approved"
	TypePaymentFailed     EmailType = "payment_failed"
	TypeNFeAuthorized     EmailType = "nfe_authorized"
	TypePasswordReset     EmailType = "password_reset"
	TypeSecurityAlert     EmailType = "security_alert"
	TypeOrderShipped      EmailType = "order_shipped" // Slice F: emitted by fulfillment worker
)

// Status values for email_messages.status (API-owned column).
const (
	StatusQueued  = "queued"
	StatusSending = "sending"
	StatusSent    = "sent"
	StatusFailed  = "failed"
)

// Follow-up event types emitted by the engine after send/fail.
const (
	EventEmailSent   = "email.sent"
	EventEmailFailed = "email.failed"
)

// QueueTransactional is the queue the engine consumes.
// QueueEvents is where follow-up events are published.
const (
	QueueTransactional = "emails.transactional"
	QueueEvents        = "emails.events"
)
