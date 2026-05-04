// Package fiscal owns the NF-e / NFC-e worker: provider interface, store,
// service, and fake adapter. It never exposes public HTTP.
//
// # Compliance notice
//
// Production fiscal integration must be reviewed and validated by a qualified
// Brazilian accountant (contador) and tax specialist before any real SEFAZ
// adapter is implemented. The Engine enforces only structural correctness;
// it does not validate Brazilian tax rules, CNPJ/CPF validity, CFOP codes,
// NCM codes, ICMS/PIS/COFINS calculations, or any other tax obligation.
//
// The fake provider is safe for local development and tests only.
package fiscal

// InvoiceStatus is the canonical lifecycle of a fiscal invoice.
type InvoiceStatus string

const (
	StatusPending    InvoiceStatus = "pending"
	StatusIssuing    InvoiceStatus = "issuing"    // intermediate: provider call in-flight
	StatusAuthorized InvoiceStatus = "authorized" // SEFAZ authorized — fulfillment may proceed
	StatusRejected   InvoiceStatus = "rejected"   // SEFAZ rejected — retry allowed
	StatusCancelled  InvoiceStatus = "cancelled"  // voided
)

// InvoiceModel identifies the fiscal document type.
const (
	ModelNFe  = "nfe"  // Nota Fiscal Eletrônica (products)
	ModelNFCe = "nfce" // Nota Fiscal de Consumidor Eletrônica (retail)
)

// Queue names consumed by the engine.
const (
	QueueIssue  = "fiscal.issue"
	QueueRetry  = "fiscal.retry"
	QueueCancel = "fiscal.cancel"
)

// QueueEvents is where the engine publishes fiscal follow-up events.
const QueueEvents = "fiscal.events"

// Follow-up event types emitted by this package.
const (
	EventAuthorized = "fiscal.authorized"
	EventRejected   = "fiscal.rejected"
	EventCancelled  = "fiscal.cancelled"
)

// Cross-domain event types emitted by this package onto other queues.
const (
	// EventEmailSendRequested targets QueueEmailTransactional.
	// Only emitted when InvoiceRow.NFeEmailMessageID is set and authorization succeeds.
	EventEmailSendRequested = "email.send.requested"

	// EventOrderFulfillRequested targets QueueOrderFulfillment.
	// Fulfillment gating: ONLY emitted after fiscal.authorized. NEVER emitted
	// while status is pending, issuing, or rejected.
	EventOrderFulfillRequested = "order.fulfillment.requested"
)

// Cross-domain queue targets.
const (
	QueueEmailTransactional = "emails.transactional"
	QueueOrderFulfillment   = "orders.fulfillment"
)
