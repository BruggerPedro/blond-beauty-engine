package fulfillment

import "context"

// DispatchRequest is the provider-agnostic shipment dispatch request.
//
// Security: this struct intentionally contains NO customer PII (name, address,
// phone, CPF/CNPJ, email). The fulfillment provider adapter is the only layer
// permitted to fetch and use address data — it must retrieve it directly from
// the order service or its own secure channel. The Engine passes only opaque
// identifiers. Implementations MUST NOT log ShipmentID or OrderID in ways that
// can be correlated with individual customer addresses.
type DispatchRequest struct {
	ShipmentID     string
	OrderID        string
	IdempotencyKey string // derived from (message_id, shipment_id) — stable across retries
}

// DispatchResult holds the outcome of a successful dispatch.
//
// Security: TrackingURL is a carrier-generated link that may indirectly expose
// the delivery address (e.g., through carrier lookup pages). It MUST NOT be
// logged, included in emitted message events, or stored outside the
// email_messages.template_data row. It is written to shipments.tracking_url
// for internal use only. The API controls access to this field.
type DispatchResult struct {
	ProviderShipmentID string
	Carrier            string
	TrackingNumber     string // not PII, but sensitive in aggregate — do not mass-log
	TrackingURL        string // SENSITIVE — never log, never include in events
}

// Provider is the canonical, provider-agnostic interface for fulfillment adapters.
//
// Implementations MUST:
//   - honour ctx deadlines;
//   - return ErrProviderPermanent (wrapped with %w) for non-retryable failures;
//   - return ErrProviderTransient (wrapped with %w) for retryable failures;
//   - NEVER fetch or log customer address, phone, CPF/CNPJ, or fiscal payloads;
//   - NEVER log DispatchResult.TrackingURL.
type Provider interface {
	Name() string
	DispatchShipment(ctx context.Context, req DispatchRequest) (DispatchResult, error)
}
