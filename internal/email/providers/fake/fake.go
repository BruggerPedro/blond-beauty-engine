// Package fake is a deterministic in-memory EmailProvider used for local
// development and tests.
//
// # Send behaviour (address-driven)
//
//	To contains "+fail@"      → ErrProviderPermanent (invalid address / blocked)
//	To contains "+transient@" → ErrProviderTransient (retryable infrastructure error)
//	otherwise                 → success with a generated ProviderMessageID
//
// Sent messages are recorded in an in-memory log accessible via Sent() for
// test assertions. The log never stores sensitive fields (TextBody, HTMLBody).
package fake

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/google/uuid"

	"github.com/blondbeauty/blond-beauty-engine/internal/email"
)

const Name = "fake-email"

// SentRecord is a redacted record of a successful send. TextBody and HTMLBody
// are intentionally omitted — they may contain PII or reset links.
type SentRecord struct {
	To             string // recorded for test assertions only; never log in production
	Subject        string
	IdempotencyKey string
	MessageID      string // generated ProviderMessageID
}

// Provider is the fake EmailProvider implementation.
type Provider struct {
	mu   sync.Mutex
	sent []SentRecord
}

func New() *Provider { return &Provider{} }

func (p *Provider) Name() string { return Name }

func (p *Provider) Send(_ context.Context, msg email.Message) (email.ProviderResult, error) {
	if strings.Contains(msg.To, "+fail@") {
		return email.ProviderResult{}, fmt.Errorf("%w: address blocked by fake provider", email.ErrProviderPermanent)
	}
	if strings.Contains(msg.To, "+transient@") {
		return email.ProviderResult{}, fmt.Errorf("%w: upstream unavailable", email.ErrProviderTransient)
	}

	id := "fake_email_" + uuid.NewString()
	p.mu.Lock()
	p.sent = append(p.sent, SentRecord{
		To:             msg.To,
		Subject:        msg.Subject,
		IdempotencyKey: msg.IdempotencyKey,
		MessageID:      id,
	})
	p.mu.Unlock()

	return email.ProviderResult{ProviderMessageID: id}, nil
}

// Sent returns a snapshot of all successfully sent messages. Safe for concurrent use.
func (p *Provider) Sent() []SentRecord {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]SentRecord, len(p.sent))
	copy(out, p.sent)
	return out
}

// Reset clears the sent log. Useful between test cases.
func (p *Provider) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sent = p.sent[:0]
}
