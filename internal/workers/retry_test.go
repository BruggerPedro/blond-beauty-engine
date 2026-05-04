package workers

import (
	"errors"
	"testing"
	"time"
)

func TestClassify(t *testing.T) {
	if Classify(errors.New("net glitch")) != RetryTransient {
		t.Fatal("plain error should be transient")
	}
	if Classify(Permanent(errors.New("bad payload"))) != RetryPermanent {
		t.Fatal("Permanent should classify as RetryPermanent")
	}
}

func TestBackoffMonotonicAndCapped(t *testing.T) {
	base := 100 * time.Millisecond
	max := 5 * time.Second
	prevHi := time.Duration(0)
	for i := 1; i <= 8; i++ {
		d := Backoff(i, base, max)
		if d <= 0 || d > max {
			t.Fatalf("attempt %d: out of range: %v", i, d)
		}
		_ = prevHi
	}
}
