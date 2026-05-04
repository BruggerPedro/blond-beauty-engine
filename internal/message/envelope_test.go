package message

import (
	"encoding/json"
	"testing"
)

func TestNewAndValidate(t *testing.T) {
	e, err := New("payment.create.requested", "order", "00000000-0000-0000-0000-000000000001", map[string]any{"amount": 1000})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := e.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if e.SchemaVersion != SchemaVersion {
		t.Fatalf("schema_version mismatch")
	}
}

func TestEncodeDecodeRoundtrip(t *testing.T) {
	e, err := New("noop.demo", "demo", "00000000-0000-0000-0000-000000000002", map[string]string{"hello": "world"})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := e.Encode()
	if err != nil {
		t.Fatal(err)
	}
	got, err := Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got.MessageID != e.MessageID || got.EventType != e.EventType {
		t.Fatalf("roundtrip mismatch")
	}
	var payload map[string]string
	if err := json.Unmarshal(got.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["hello"] != "world" {
		t.Fatalf("payload mismatch")
	}
}

func TestValidateRejectsMissingFields(t *testing.T) {
	e := &Envelope{}
	if err := e.Validate(); err == nil {
		t.Fatal("expected error")
	}
}
