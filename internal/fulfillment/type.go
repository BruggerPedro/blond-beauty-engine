// Package fulfillment owns the order fulfillment worker: it consumes
// order.fulfillment.requested events, verifies all preconditions from
// persisted state (events are triggers, not authority), dispatches shipments
// through a provider-agnostic interface, and emits follow-up events.
//
// # Single-shipment full fulfillment (Slice F)
//
// This slice implements exactly one shipment per order. Partial fulfillment
// (multiple shipments per order, line-item tracking) is documented as future
// work. A permanently failed shipment requires manual database intervention
// to create a replacement (different idempotency key scheme).
//
// # Manual exception support
//
// Manual fulfillment exceptions (e.g., "approve fulfillment despite rejected
// fiscal") are explicitly out of scope. The gating logic enforces hard rules;
// any exception path must be designed and implemented separately with full
// audit trail support.
//
// # Real carriers out of scope
//
// No Correios, Melhor Envio, Frenet, or any other real carrier/3PL is
// implemented here. The fake provider is the only adapter in this slice.
package fulfillment

// ShipmentStatus is the canonical lifecycle of a shipment row.
type ShipmentStatus string

const (
	ShipmentPending    ShipmentStatus = "pending"
	ShipmentDispatched ShipmentStatus = "dispatched" // provider confirmed dispatch
	ShipmentFailed     ShipmentStatus = "failed"     // permanent provider failure
	ShipmentCancelled  ShipmentStatus = "cancelled"
)

// Queue names.
const (
	QueueFulfillment = "orders.fulfillment" // consumed by this worker
	QueueEvents      = "orders.events"      // follow-up events emitted here
)

// Event types emitted by this package.
const (
	EventFulfilled         = "order.fulfilled"
	EventFulfillmentFailed = "order.fulfillment.failed"
)

// Cross-domain event and queue targets.
const (
	EventEmailSendRequested = "email.send.requested"
	QueueEmailTransactional = "emails.transactional"
)

// Precondition: allowed order statuses for fulfillment.
// "fulfilled" is allowed to support idempotent re-delivery.
var allowedOrderStatuses = map[string]bool{
	"paid":       true,
	"processing": true,
	"fulfilled":  true,
}

// Precondition: payment statuses that represent successful payment.
var successfulPaymentStatuses = map[string]bool{
	"paid":     true,
	"captured": true,
}
