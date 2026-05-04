// Package fake is a deterministic in-memory FulfillmentProvider used for local
// development and tests. It never contacts any real carrier, 3PL, Correios,
// Melhor Envio, Frenet, or external shipping API.
//
// # Dispatch behaviour (OrderID-driven)
//
//	OrderID contains "+fail"      → ErrProviderPermanent (carrier rejected)
//	OrderID contains "+transient" → ErrProviderTransient (carrier unavailable)
//	otherwise                     → success with fake carrier/tracking values
//
// # Generated values
//
// All generated values are clearly prefixed with "FAKE-" so they can never be
// mistaken for real carrier data. They are not valid tracking numbers.
//
// # Privacy note
//
// The fake provider generates tracking URLs of the form
// "https://fake-carrier.local/track/FAKE-<id>". These are clearly placeholder
// values, contain no customer data, and are safe for logs in development.
// Real provider implementations MUST NOT log tracking URLs.
package fake

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/google/uuid"

	"github.com/blondbeauty/blond-beauty-engine/internal/fulfillment"
)

const Name = "fake-fulfillment"

// DispatchedRecord is a redacted record of a successful DispatchShipment call.
// TrackingURL is intentionally omitted — even in tests it should not be
// compared against emitted events (it must not appear there).
type DispatchedRecord struct {
	ShipmentID     string
	OrderID        string
	Carrier        string
	TrackingNumber string
}

// Provider is the fake FulfillmentProvider implementation.
type Provider struct {
	mu         sync.Mutex
	dispatched []DispatchedRecord
}

func New() *Provider { return &Provider{} }

func (p *Provider) Name() string { return Name }

// DispatchShipment returns a deterministic result driven by OrderID content.
func (p *Provider) DispatchShipment(_ context.Context, req fulfillment.DispatchRequest) (fulfillment.DispatchResult, error) {
	if strings.Contains(req.OrderID, "+fail") {
		return fulfillment.DispatchResult{},
			fmt.Errorf("%w: carrier rejected shipment (fake)", fulfillment.ErrProviderPermanent)
	}
	if strings.Contains(req.OrderID, "+transient") {
		return fulfillment.DispatchResult{},
			fmt.Errorf("%w: carrier unavailable (fake)", fulfillment.ErrProviderTransient)
	}

	trackingNum := "FAKE-" + strings.ToUpper(uuid.NewString()[:12])
	result := fulfillment.DispatchResult{
		ProviderShipmentID: "fake_ship_" + uuid.NewString()[:8],
		Carrier:            "Fake Carrier",
		TrackingNumber:     trackingNum,
		TrackingURL:        "https://fake-carrier.local/track/" + trackingNum,
	}

	p.mu.Lock()
	p.dispatched = append(p.dispatched, DispatchedRecord{
		ShipmentID:     req.ShipmentID,
		OrderID:        req.OrderID,
		Carrier:        result.Carrier,
		TrackingNumber: result.TrackingNumber,
	})
	p.mu.Unlock()

	return result, nil
}

// Dispatched returns a snapshot of all successfully dispatched shipments.
func (p *Provider) Dispatched() []DispatchedRecord {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]DispatchedRecord, len(p.dispatched))
	copy(out, p.dispatched)
	return out
}

// Reset clears the dispatched log. Useful between test cases.
func (p *Provider) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.dispatched = p.dispatched[:0]
}
