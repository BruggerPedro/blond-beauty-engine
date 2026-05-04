// Package fake is a deterministic in-memory FiscalProvider used for local
// development and tests. It never contacts SEFAZ or any real fiscal authority.
//
// # Issue behaviour (TotalCents-driven)
//
//	TotalCents % 1000 == 13  → ErrProviderRejected (SEFAZ rejection code "225")
//	TotalCents % 1000 == 17  → ErrProviderTransient (SEFAZ unavailable)
//	otherwise                → authorized; generates fake access_key, protocol, number
//
// # Cancel behaviour
//
//	Always succeeds for known InvoiceIDs; returns ErrProviderTransient if
//	InvoiceID contains "+transient".
//
// # Generated values
//
// All generated values (AccessKey, Protocol, Number, XMLStorageKey,
// DANFEStorageKey) are clearly prefixed with "fake_" so they can never be
// mistaken for real SEFAZ documents. They are NOT valid NF-e keys.
//
// # Compliance notice
//
// This provider MUST NOT be used in production. It produces no valid fiscal
// documents, performs no SEFAZ communication, and applies no Brazilian tax
// rules. See the package-level compliance notice in internal/fiscal/type.go.
package fake

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/google/uuid"

	"github.com/blondbeauty/blond-beauty-engine/internal/fiscal"
)

const Name = "fake-fiscal"

// IssuedRecord is a redacted record of a successful IssueInvoice call.
// It is accessible via Issued() for test assertions.
type IssuedRecord struct {
	InvoiceID string
	OrderID   string
	AccessKey string
	Protocol  string
	Number    string
}

// Provider is the fake FiscalProvider implementation.
type Provider struct {
	mu     sync.Mutex
	issued []IssuedRecord
}

func New() *Provider { return &Provider{} }

func (p *Provider) Name() string { return Name }

// IssueInvoice returns a deterministic result driven by TotalCents % 1000.
func (p *Provider) IssueInvoice(_ context.Context, req fiscal.IssueRequest) (fiscal.IssueResult, fiscal.RejectionDetail, error) {
	switch req.TotalCents % 1000 {
	case 13:
		return fiscal.IssueResult{}, fiscal.RejectionDetail{
			Code:    "225",
			Message: "fake: NF-e rejected by deterministic rule (total_cents % 1000 == 13)",
		}, fmt.Errorf("%w: rejection code 225 (fake)", fiscal.ErrProviderRejected)

	case 17:
		return fiscal.IssueResult{}, fiscal.RejectionDetail{},
			fmt.Errorf("%w: SEFAZ unavailable (fake transient)", fiscal.ErrProviderTransient)
	}

	accessKey := fakeAccessKey(req.InvoiceID)
	protocol := "fake_proto_" + uuid.NewString()[:8]
	number := "fake_num_" + uuid.NewString()[:8]
	xmlKey := "fake://xml/" + req.InvoiceID
	danfeKey := "fake://danfe/" + req.InvoiceID

	p.mu.Lock()
	p.issued = append(p.issued, IssuedRecord{
		InvoiceID: req.InvoiceID,
		OrderID:   req.OrderID,
		AccessKey: accessKey,
		Protocol:  protocol,
		Number:    number,
	})
	p.mu.Unlock()

	return fiscal.IssueResult{
		AccessKey:       accessKey,
		Protocol:        protocol,
		Number:          number,
		XMLStorageKey:   xmlKey,
		DANFEStorageKey: danfeKey,
	}, fiscal.RejectionDetail{}, nil
}

// CancelInvoice always succeeds unless InvoiceID contains "+transient".
func (p *Provider) CancelInvoice(_ context.Context, req fiscal.CancelRequest) (fiscal.CancelResult, error) {
	if strings.Contains(req.InvoiceID, "+transient") {
		return fiscal.CancelResult{}, fmt.Errorf("%w: cancel transient (fake)", fiscal.ErrProviderTransient)
	}
	return fiscal.CancelResult{
		Protocol: "fake_cancel_proto_" + uuid.NewString()[:8],
	}, nil
}

// Issued returns a snapshot of all successfully issued invoices. Safe for concurrent use.
func (p *Provider) Issued() []IssuedRecord {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]IssuedRecord, len(p.issued))
	copy(out, p.issued)
	return out
}

// Reset clears the issued log. Useful between test cases.
func (p *Provider) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.issued = p.issued[:0]
}

// fakeAccessKey generates a fake 44-char access key deterministically from the
// invoice ID. Real access keys are computed by SEFAZ; this is a safe placeholder.
func fakeAccessKey(invoiceID string) string {
	base := strings.ReplaceAll(invoiceID, "-", "")
	for len(base) < 44 {
		base += "0"
	}
	return "FAKE" + base[:40]
}
