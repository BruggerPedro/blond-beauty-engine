package workers

import (
	"errors"
	"math"
	"math/rand/v2"
	"time"
)

// Retry classifies an error returned by a Handler.
type Retry int

const (
	// RetryTransient: retry with backoff, then DLQ when attempts are exhausted.
	RetryTransient Retry = iota
	// RetryPermanent: do not retry; route to DLQ immediately.
	RetryPermanent
)

// PermanentError marks an error as non-retryable.
type PermanentError struct{ Err error }

func (e *PermanentError) Error() string { return e.Err.Error() }
func (e *PermanentError) Unwrap() error { return e.Err }

func Permanent(err error) error { return &PermanentError{Err: err} }

func Classify(err error) Retry {
	var p *PermanentError
	if errors.As(err, &p) {
		return RetryPermanent
	}
	return RetryTransient
}

// Backoff returns an exponential delay with jitter, capped at max.
func Backoff(attempt int, base, max time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := time.Duration(float64(base) * math.Pow(2, float64(attempt-1)))
	if d <= 0 || d > max {
		d = max
	}
	jitter := time.Duration(rand.Int64N(int64(d / 4)))
	return d/2 + jitter
}
