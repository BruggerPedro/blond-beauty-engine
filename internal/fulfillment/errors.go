package fulfillment

import "errors"

// ErrOrderNotFound is returned when the orders row does not exist. Permanent.
var ErrOrderNotFound = errors.New("fulfillment: order not found")

// ErrPreconditionFailed is returned when persisted state prevents fulfillment
// (bad payment status, fiscal not authorized, order cancelled, etc.). Permanent.
var ErrPreconditionFailed = errors.New("fulfillment: precondition not met")

// ErrShipmentAlreadyFailed is returned when the existing shipment row is in a
// terminal failed state. Permanent — DLQ. Manual intervention required.
var ErrShipmentAlreadyFailed = errors.New("fulfillment: shipment already in failed state")

// ErrProviderPermanent signals a non-retryable dispatch failure (address invalid,
// carrier rejected, etc.). Wrap with %w.
var ErrProviderPermanent = errors.New("fulfillment provider permanent failure")

// ErrProviderTransient signals a retryable infrastructure failure. Wrap with %w.
var ErrProviderTransient = errors.New("fulfillment provider transient failure")
