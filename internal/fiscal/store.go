package fiscal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// InvoiceRow is the engine's read/write projection of the API-owned
// `fiscal_invoices` table.
//
// Expected schema (API-owned; documented here as a contract):
//
//	create table fiscal_invoices (
//	    id                  uuid primary key,
//	    order_id            uuid not null,
//	    payment_id          uuid not null,
//	    model               text not null check (model in ('nfe','nfce')),
//	    series              text not null,
//	    number              text,
//	    access_key          text,           -- 44-char NF-e access key; public
//	    protocol            text,           -- SEFAZ authorization protocol
//	    xml_storage_key     text,           -- opaque ref to stored XML — NOT raw XML
//	    danfe_storage_key   text,           -- opaque ref to stored DANFE — NOT raw PDF
//	    status              text not null default 'pending'
//	        check (status in ('pending','issuing','authorized','rejected','cancelled')),
//	    rejection_code      text,           -- SEFAZ rejection code, e.g. "225"
//	    rejection_message   text,           -- human-readable; truncated ≤500 chars
//	    idempotency_key     text not null unique,
//	    provider            text not null,
//	    attempt_count       int  not null default 0,
//	    last_error          text,
//	    total_cents         bigint not null,
//	    currency            text   not null,
//	    -- Optional: pre-created email_messages row for the NF-e email.
//	    -- The API creates this row (with email_type='nfe_authorized') before
//	    -- issuing the fiscal invoice. The Engine will populate template_data
//	    -- and emit email.send.requested after authorization.
//	    nfe_email_message_id uuid,
//	    correlation_id      text,
//	    causation_id        text,
//	    created_at          timestamptz not null default now(),
//	    updated_at          timestamptz not null default now()
//	);
type InvoiceRow struct {
	ID               string
	OrderID          string
	PaymentID        string
	Model            string
	Series           string
	Number           string
	AccessKey        string
	Protocol         string
	XMLStorageKey    string
	DANFEStorageKey  string
	Status           InvoiceStatus
	RejectionCode    string
	RejectionMessage string
	IdempotencyKey   string
	Provider         string
	AttemptCount     int
	LastError        string
	TotalCents       int64
	Currency         string
	// NFeEmailMessageID is the ID of a pre-created email_messages row the API
	// inserted with email_type='nfe_authorized'. If non-empty, the Engine will
	// update its template_data and emit email.send.requested after authorization.
	NFeEmailMessageID string
	CorrelationID     string
	CausationID       string
}

// InvoiceEventRow records one lifecycle event for audit.
//
// Expected schema (API-owned):
//
//	create table fiscal_invoice_events (
//	    id                uuid primary key default gen_random_uuid(),
//	    fiscal_invoice_id uuid not null references fiscal_invoices(id),
//	    event_type        text not null,
//	    payload           jsonb not null default '{}',
//	    created_at        timestamptz not null default now()
//	);
type InvoiceEventRow struct {
	FiscalInvoiceID string
	EventType       string
	Payload         json.RawMessage
}

// NFeEmailTemplateData is the data the engine writes into email_messages.template_data
// after NF-e authorization. It maps to email.NFeAuthorizedData.
//
// Security: DANFEStorageKey and XMLStorageKey are opaque internal references.
// The Engine writes them as-is; the API or storage layer must convert them to
// short-lived, pre-signed URLs before they appear in the rendered email.
// For the fake provider these keys ARE treated as the links directly.
type NFeEmailTemplateData struct {
	OrderID   string `json:"order_id"`
	AccessKey string `json:"access_key"`
	DANFELink string `json:"danfe_link"`
	XMLLink   string `json:"xml_link"`
}

// Store abstracts fiscal_invoices persistence for testability.
type Store interface {
	LockInvoice(ctx context.Context, tx pgx.Tx, id string) (*InvoiceRow, error)
	UpdateInvoice(ctx context.Context, tx pgx.Tx, inv *InvoiceRow) error
	InsertInvoiceEvent(ctx context.Context, tx pgx.Tx, ev *InvoiceEventRow) error
	// UpdateEmailTemplateData fills template_data on a pre-created email_messages
	// row. Called after NF-e authorization so the email worker can render the
	// NFeAuthorized template with the correct access key and storage links.
	// No-op if emailMessageID is empty.
	UpdateEmailTemplateData(ctx context.Context, tx pgx.Tx, emailMessageID string, data NFeEmailTemplateData) error
}

// PgxStore is the production Store backed by pgx/v5.
type PgxStore struct{}

func NewPgxStore() *PgxStore { return &PgxStore{} }

func (PgxStore) LockInvoice(ctx context.Context, tx pgx.Tx, id string) (*InvoiceRow, error) {
	var inv InvoiceRow
	var status string
	err := tx.QueryRow(ctx, `
SELECT id, order_id, payment_id, model, series,
       COALESCE(number,''), COALESCE(access_key,''), COALESCE(protocol,''),
       COALESCE(xml_storage_key,''), COALESCE(danfe_storage_key,''),
       status,
       COALESCE(rejection_code,''), COALESCE(rejection_message,''),
       idempotency_key, provider, attempt_count,
       COALESCE(last_error,''),
       total_cents, currency,
       COALESCE(nfe_email_message_id::text,''),
       COALESCE(correlation_id,''), COALESCE(causation_id,'')
FROM fiscal_invoices
WHERE id = $1
FOR UPDATE`, id).Scan(
		&inv.ID, &inv.OrderID, &inv.PaymentID, &inv.Model, &inv.Series,
		&inv.Number, &inv.AccessKey, &inv.Protocol,
		&inv.XMLStorageKey, &inv.DANFEStorageKey,
		&status,
		&inv.RejectionCode, &inv.RejectionMessage,
		&inv.IdempotencyKey, &inv.Provider, &inv.AttemptCount,
		&inv.LastError,
		&inv.TotalCents, &inv.Currency,
		&inv.NFeEmailMessageID,
		&inv.CorrelationID, &inv.CausationID,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: %s", ErrInvoiceNotFound, id)
	}
	if err != nil {
		return nil, fmt.Errorf("lock fiscal invoice: %w", err)
	}
	inv.Status = InvoiceStatus(status)
	return &inv, nil
}

func (PgxStore) UpdateInvoice(ctx context.Context, tx pgx.Tx, inv *InvoiceRow) error {
	_, err := tx.Exec(ctx, `
UPDATE fiscal_invoices
SET status            = $2,
    number            = NULLIF($3,''),
    access_key        = NULLIF($4,''),
    protocol          = NULLIF($5,''),
    xml_storage_key   = NULLIF($6,''),
    danfe_storage_key = NULLIF($7,''),
    rejection_code    = NULLIF($8,''),
    rejection_message = NULLIF($9,''),
    attempt_count     = $10,
    last_error        = NULLIF($11,''),
    updated_at        = now()
WHERE id = $1`,
		inv.ID, string(inv.Status),
		inv.Number, inv.AccessKey, inv.Protocol,
		inv.XMLStorageKey, inv.DANFEStorageKey,
		inv.RejectionCode, inv.RejectionMessage,
		inv.AttemptCount, inv.LastError,
	)
	if err != nil {
		return fmt.Errorf("update fiscal invoice: %w", err)
	}
	return nil
}

func (PgxStore) InsertInvoiceEvent(ctx context.Context, tx pgx.Tx, ev *InvoiceEventRow) error {
	_, err := tx.Exec(ctx, `
INSERT INTO fiscal_invoice_events (fiscal_invoice_id, event_type, payload)
VALUES ($1, $2, $3)`,
		ev.FiscalInvoiceID, ev.EventType, ev.Payload,
	)
	if err != nil {
		return fmt.Errorf("insert fiscal invoice event: %w", err)
	}
	return nil
}

func (PgxStore) UpdateEmailTemplateData(ctx context.Context, tx pgx.Tx, emailMessageID string, data NFeEmailTemplateData) error {
	if emailMessageID == "" {
		return nil
	}
	raw, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("marshal nfe email template data: %w", err)
	}
	_, err = tx.Exec(ctx, `
UPDATE email_messages
SET template_data = $2,
    updated_at    = now()
WHERE id = $1`,
		emailMessageID, raw,
	)
	if err != nil {
		return fmt.Errorf("update email template data: %w", err)
	}
	return nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
