package fiscal

import "context"

// IssueRequest is the provider-agnostic invoice issuance request.
//
// Security: this struct intentionally contains NO customer PII (CPF, CNPJ,
// name, address), no tax line-item detail (CFOP, NCM, ICMS rates), and no
// certificate material. The Engine delegates tax-sensitive construction to
// the fiscal provider adapter, which is the only layer permitted to compose
// the NF-e XML. Implementations MUST NOT log TotalCents as a standalone value
// in production logs where it could be correlated with individual customers.
type IssueRequest struct {
	InvoiceID      string // fiscal_invoices.id
	OrderID        string // for idempotency correlation
	PaymentID      string // for idempotency correlation
	Model          string // ModelNFe | ModelNFCe
	Series         string
	IdempotencyKey string // derived from (message_id, invoice_id)
	TotalCents     int64  // used by the fake provider for deterministic outcomes
	Currency       string
}

// IssueResult holds the fields returned after SEFAZ authorization.
//
// Security: AccessKey (chave de acesso, 44 chars) and Protocol are public
// identifiers — safe to log. XMLStorageKey and DANFEStorageKey are opaque
// storage references, NOT raw document content. Implementations MUST NOT
// store or log raw NF-e XML, DANFE PDF, or certificate material here.
type IssueResult struct {
	AccessKey       string // 44-char chave de acesso — public; safe to log
	Protocol        string // SEFAZ authorization protocol
	Number          string // NF-e number assigned by SEFAZ
	XMLStorageKey   string // opaque internal reference to stored XML (not raw XML)
	DANFEStorageKey string // opaque internal reference to stored DANFE PDF
}

// RejectionDetail carries structured rejection information from SEFAZ.
// Both Code and Message are operational metadata — not raw XML content.
type RejectionDetail struct {
	Code    string // SEFAZ rejection code (e.g., "225")
	Message string // human-readable rejection reason (truncated to ≤500 chars)
}

// CancelRequest is the provider-agnostic invoice cancellation request.
type CancelRequest struct {
	InvoiceID      string
	AccessKey      string // required to identify the invoice at SEFAZ
	IdempotencyKey string
	Reason         string // mandatory justification (≥15 chars per SEFAZ rules)
}

// CancelResult holds the protocol returned after SEFAZ accepts the cancellation.
type CancelResult struct {
	Protocol string // SEFAZ cancellation protocol
}

// Provider is the canonical, provider-agnostic interface for fiscal adapters.
//
// Implementations MUST:
//   - honour ctx deadlines;
//   - return ErrProviderRejected (wrapped with %w) for permanent SEFAZ rejections,
//     and populate rejection in the returned RejectionDetail;
//   - return ErrProviderTransient (wrapped with %w) for retryable failures;
//   - NEVER log raw NF-e XML, DANFE content, CPF, CNPJ, or certificate material.
type Provider interface {
	Name() string
	IssueInvoice(ctx context.Context, req IssueRequest) (IssueResult, RejectionDetail, error)
	CancelInvoice(ctx context.Context, req CancelRequest) (CancelResult, error)
}
