package idempotency

import "testing"

func TestKeyIsStable(t *testing.T) {
	a := Key("payments.provider_request", "msg-1", "pay-1", "create")
	b := Key("payments.provider_request", "msg-1", "pay-1", "create")
	if a != b {
		t.Fatalf("Key() not stable: %q != %q", a, b)
	}
	if len(a) != 64 {
		t.Fatalf("Key() length = %d, want 64 hex chars", len(a))
	}
}

func TestKeySeparatesDelimiterCollisions(t *testing.T) {
	a := Key("fiscal.provider_request", "msg|invoice", "issue", "op")
	b := Key("fiscal.provider_request", "msg", "invoice|issue", "op")
	if a == b {
		t.Fatal("Key() collided for inputs that collide under pipe-delimited concatenation")
	}
}

func TestKeySeparatesScopes(t *testing.T) {
	a := Key("payments.provider_request", "msg-1", "aggregate-1", "create")
	b := Key("fiscal.provider_request", "msg-1", "aggregate-1", "create")
	if a == b {
		t.Fatal("Key() collided across scopes")
	}
}
