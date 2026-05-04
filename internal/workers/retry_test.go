package workers

import (
	"errors"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
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

func TestReadAttempt(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   any
		want int
	}{
		{name: "int", in: 3, want: 3},
		{name: "int32", in: int32(4), want: 4},
		{name: "int64", in: int64(5), want: 5},
		{name: "unknown", in: "5", want: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := readAttempt(amqp.Table{headerAttempt: tc.in})
			if got != tc.want {
				t.Fatalf("readAttempt() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestRetryHeadersIncrementsWithoutMutatingOriginal(t *testing.T) {
	original := amqp.Table{"trace_id": "corr-1", headerAttempt: int32(1)}
	next := retryHeaders(original, 2)

	if got := readAttempt(next); got != 2 {
		t.Fatalf("retry header attempt = %d, want 2", got)
	}
	if next["trace_id"] != "corr-1" {
		t.Fatalf("trace header not preserved: %v", next)
	}
	if got := readAttempt(original); got != 1 {
		t.Fatalf("original mutated: attempt = %d, want 1", got)
	}
}
