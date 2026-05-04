// Package payments owns the canonical payment domain types, the
// provider-agnostic PaymentProvider interface, and the workers that drive
// the six payment operations: create, confirm, capture, cancel, refund,
// reconcile.
//
// The Engine never depends on a specific provider's DTOs outside its own
// adapter package. Business code only sees canonical types defined here.
package payments

// Status is the canonical payment status used across the engine, the API DB,
// and the outbox. Provider adapters MUST map their own statuses to one of
// these values; never leak provider-specific strings into the domain.
type Status string

const (
	StatusUnknown        Status = "unknown"
	StatusRequiresAction Status = "requires_action"
	StatusAuthorized     Status = "authorized"
	StatusCaptured       Status = "captured"
	StatusPaid           Status = "paid"
	StatusFailed         Status = "failed"
	StatusCancelled      Status = "cancelled"
	StatusRefunded       Status = "refunded"
	StatusChargeback     Status = "chargeback"
)

// IsTerminal reports whether the status is a final state for the payment
// lifecycle — no further provider transitions are expected.
func (s Status) IsTerminal() bool {
	switch s {
	case StatusPaid, StatusFailed, StatusCancelled, StatusRefunded, StatusChargeback:
		return true
	}
	return false
}

func (s Status) Valid() bool {
	switch s {
	case StatusUnknown, StatusRequiresAction, StatusAuthorized, StatusCaptured,
		StatusPaid, StatusFailed, StatusCancelled, StatusRefunded, StatusChargeback:
		return true
	}
	return false
}

// Operation enumerates the canonical payment operations the engine performs.
type Operation string

const (
	OpCreate    Operation = "create"
	OpConfirm   Operation = "confirm"
	OpCapture   Operation = "capture"
	OpCancel    Operation = "cancel"
	OpRefund    Operation = "refund"
	OpReconcile Operation = "reconcile"
)
