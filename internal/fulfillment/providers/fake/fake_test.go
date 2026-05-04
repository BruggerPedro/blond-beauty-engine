package fake_test

import (
	"context"
	"errors"
	"testing"

	"github.com/blondbeauty/blond-beauty-engine/internal/fulfillment"
	"github.com/blondbeauty/blond-beauty-engine/internal/fulfillment/providers/fake"
)

func req(orderID string) fulfillment.DispatchRequest {
	return fulfillment.DispatchRequest{
		ShipmentID:     "ship-001",
		OrderID:        orderID,
		IdempotencyKey: "idem-001",
	}
}

func TestFakeProvider_Name(t *testing.T) {
	p := fake.New()
	if p.Name() != fake.Name {
		t.Fatalf("Name() = %q, want %q", p.Name(), fake.Name)
	}
}

func TestFakeProvider_Dispatch_Success(t *testing.T) {
	p := fake.New()
	res, err := p.DispatchShipment(context.Background(), req("order-001"))
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if res.Carrier == "" || res.TrackingNumber == "" || res.ProviderShipmentID == "" {
		t.Error("expected non-empty carrier, tracking number, and provider shipment ID")
	}
	if res.TrackingURL == "" {
		t.Error("expected non-empty TrackingURL")
	}
	d := p.Dispatched()
	if len(d) != 1 {
		t.Fatalf("Dispatched() len = %d, want 1", len(d))
	}
	if d[0].TrackingNumber != res.TrackingNumber {
		t.Errorf("recorded TrackingNumber mismatch")
	}
}

func TestFakeProvider_Dispatch_PermanentFailure(t *testing.T) {
	p := fake.New()
	_, err := p.DispatchShipment(context.Background(), req("order+fail"))
	if !errors.Is(err, fulfillment.ErrProviderPermanent) {
		t.Fatalf("expected ErrProviderPermanent, got %v", err)
	}
	if len(p.Dispatched()) != 0 {
		t.Error("permanent failure must not record a dispatched entry")
	}
}

func TestFakeProvider_Dispatch_TransientFailure(t *testing.T) {
	p := fake.New()
	_, err := p.DispatchShipment(context.Background(), req("order+transient"))
	if !errors.Is(err, fulfillment.ErrProviderTransient) {
		t.Fatalf("expected ErrProviderTransient, got %v", err)
	}
}

func TestFakeProvider_UniqueTrackingNumbers(t *testing.T) {
	p := fake.New()
	r1, _ := p.DispatchShipment(context.Background(), req("order-a"))
	r2, _ := p.DispatchShipment(context.Background(), req("order-b"))
	if r1.TrackingNumber == r2.TrackingNumber {
		t.Error("TrackingNumbers should be unique")
	}
}

func TestFakeProvider_Reset(t *testing.T) {
	p := fake.New()
	_, _ = p.DispatchShipment(context.Background(), req("order-a"))
	p.Reset()
	if len(p.Dispatched()) != 0 {
		t.Fatal("expected 0 dispatched after Reset")
	}
}
