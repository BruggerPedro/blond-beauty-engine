package fiscal

import "errors"

// ErrInvoiceNotFound is returned when the fiscal_invoices row does not exist.
// Permanent — DLQ.
var ErrInvoiceNotFound = errors.New("fiscal invoice not found")

// ErrIllegalTransition is returned when the current invoice status does not
// allow the requested operation. Permanent — DLQ.
var ErrIllegalTransition = errors.New("fiscal illegal status transition")

// ErrProviderRejected signals a definitive SEFAZ rejection (invalid data,
// duplicate access key, etc.). Permanent — log rejection_code/message.
var ErrProviderRejected = errors.New("fiscal provider rejected")

// ErrProviderTransient signals a retryable failure (network timeout, SEFAZ
// unavailable, rate limit). The worker retries with exponential backoff.
var ErrProviderTransient = errors.New("fiscal provider transient")
