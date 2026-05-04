package fulfillment

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// FulfillmentState is the read-only snapshot of persisted order/payment/fiscal
// state used for precondition checking. Loaded fresh on every Handle call —
// the incoming event is a trigger, not an authority.
type FulfillmentState struct {
	// OrderStatus is orders.status. Allowed values for fulfillment:
	// "paid", "processing", "fulfilled" (idempotent). Blocked: "cancelled", "refunded".
	OrderStatus string
	// RequiresFiscal is orders.requires_fiscal. When true, FiscalStatus must be
	// "authorized" before fulfillment proceeds.
	RequiresFiscal bool
	// PaymentStatus is the canonical status of the order's successful payment.
	// Empty string means no successful payment exists.
	PaymentStatus string
	// FiscalStatus is the status of the order's fiscal_invoices row.
	// Empty string means no fiscal invoice exists (only valid when RequiresFiscal=false).
	FiscalStatus string
}

// ShipmentRow is the engine's read/write projection of the API-owned `shipments` table.
//
// Expected schema (API-owned):
//
//	create table shipments (
//	    id                    uuid primary key default gen_random_uuid(),
//	    order_id              uuid not null references orders(id),
//	    status                text not null default 'pending'
//	        check (status in ('pending','dispatched','failed','cancelled')),
//	    carrier               text,
//	    tracking_number       text,      -- do not mass-log; carrier-specific, not PII
//	    tracking_url          text,      -- SENSITIVE: may reveal delivery address context
//	    provider              text,
//	    provider_shipment_id  text,
//	    idempotency_key       text not null unique,
//	    last_error            text,
//	    attempt_count         int  not null default 0,
//	    -- Optional: pre-created email_messages row for the shipped notification.
//	    -- The API creates this row (email_type='order_shipped') before issuing
//	    -- the fulfillment request. The Engine updates template_data and emits
//	    -- email.send.requested after dispatch. Same pattern as NF-e email.
//	    shipped_email_message_id uuid,
//	    correlation_id        text,
//	    causation_id          text,
//	    created_at            timestamptz not null default now(),
//	    updated_at            timestamptz not null default now()
//	);
type ShipmentRow struct {
	ID                    string
	OrderID               string
	Status                ShipmentStatus
	Carrier               string
	TrackingNumber        string
	TrackingURL           string // SENSITIVE — stored in DB, never emitted in events
	Provider              string
	ProviderShipmentID    string
	IdempotencyKey        string
	LastError             string
	AttemptCount          int
	ShippedEmailMessageID string
	CorrelationID         string
	CausationID           string
}

// ShipmentEventRow records one lifecycle event for audit.
//
// Expected schema (API-owned):
//
//	create table shipment_events (
//	    id          uuid primary key default gen_random_uuid(),
//	    shipment_id uuid not null references shipments(id),
//	    event_type  text not null,
//	    payload     jsonb not null default '{}',
//	    created_at  timestamptz not null default now()
//	);
type ShipmentEventRow struct {
	ShipmentID string
	EventType  string
	Payload    json.RawMessage
}

// ShipmentEmailData is written into email_messages.template_data for the
// order_shipped email after dispatch. Maps to email.OrderShippedData.
//
// Security: TrackingURL is SENSITIVE. Same pattern as NFeEmailTemplateData —
// the Engine writes it only to the pre-created email_messages row; it is never
// included in emitted message events or logs.
type ShipmentEmailData struct {
	OrderID        string `json:"order_id"`
	Carrier        string `json:"carrier"`
	TrackingNumber string `json:"tracking_number"`
	TrackingURL    string `json:"tracking_url"` // SENSITIVE — never log
}

// Store abstracts fulfillment persistence for testability.
type Store interface {
	// LoadFulfillmentState loads the order/payment/fiscal state for precondition
	// checking. Returns ErrOrderNotFound if the order row does not exist.
	// Uses a non-locking read — only the shipment row is locked FOR UPDATE.
	LoadFulfillmentState(ctx context.Context, tx pgx.Tx, orderID string) (*FulfillmentState, error)

	// FindOrCreateShipment atomically inserts a new shipment using the row's
	// IdempotencyKey (ON CONFLICT DO NOTHING), then locks and returns the
	// actual row. Returns the existing row if it was already present.
	FindOrCreateShipment(ctx context.Context, tx pgx.Tx, template *ShipmentRow) (*ShipmentRow, error)

	UpdateShipment(ctx context.Context, tx pgx.Tx, s *ShipmentRow) error
	InsertShipmentEvent(ctx context.Context, tx pgx.Tx, ev *ShipmentEventRow) error

	// UpdateEmailTemplateData fills template_data on a pre-created email_messages
	// row. No-op if emailMessageID is empty. Same contract as fiscal.Store.
	UpdateEmailTemplateData(ctx context.Context, tx pgx.Tx, emailMessageID string, data ShipmentEmailData) error
}

// PgxStore is the production Store backed by pgx/v5.
type PgxStore struct{}

func NewPgxStore() *PgxStore { return &PgxStore{} }

func (PgxStore) LoadFulfillmentState(ctx context.Context, tx pgx.Tx, orderID string) (*FulfillmentState, error) {
	var s FulfillmentState
	err := tx.QueryRow(ctx, `
SELECT
    o.status,
    o.requires_fiscal,
    COALESCE((
        SELECT p.status FROM payments p
        WHERE p.order_id = o.id
        ORDER BY CASE p.status
            WHEN 'paid'     THEN 1
            WHEN 'captured' THEN 2
            ELSE 9
        END
        LIMIT 1
    ), '') AS payment_status,
    COALESCE((
        SELECT fi.status FROM fiscal_invoices fi
        WHERE fi.order_id = o.id
        ORDER BY fi.created_at DESC
        LIMIT 1
    ), '') AS fiscal_status
FROM orders o
WHERE o.id = $1`, orderID).Scan(
		&s.OrderStatus, &s.RequiresFiscal,
		&s.PaymentStatus, &s.FiscalStatus,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: %s", ErrOrderNotFound, orderID)
	}
	if err != nil {
		return nil, fmt.Errorf("load fulfillment state: %w", err)
	}
	return &s, nil
}

func (PgxStore) FindOrCreateShipment(ctx context.Context, tx pgx.Tx, tmpl *ShipmentRow) (*ShipmentRow, error) {
	// Try to insert; silently ignore if idempotency_key already exists.
	_, err := tx.Exec(ctx, `
INSERT INTO shipments
    (order_id, status, provider, idempotency_key,
     shipped_email_message_id,
     correlation_id, causation_id)
VALUES ($1, 'pending', $2, $3,
        NULLIF($4, ''),
        NULLIF($5, ''), NULLIF($6, ''))
ON CONFLICT (idempotency_key) DO NOTHING`,
		tmpl.OrderID, tmpl.Provider, tmpl.IdempotencyKey,
		tmpl.ShippedEmailMessageID,
		tmpl.CorrelationID, tmpl.CausationID,
	)
	if err != nil {
		return nil, fmt.Errorf("upsert shipment: %w", err)
	}

	// Lock and return the actual row (new or pre-existing).
	var s ShipmentRow
	var status string
	err = tx.QueryRow(ctx, `
SELECT id, order_id, status,
       COALESCE(carrier,''), COALESCE(tracking_number,''), COALESCE(tracking_url,''),
       COALESCE(provider,''), COALESCE(provider_shipment_id,''),
       idempotency_key, COALESCE(last_error,''), attempt_count,
       COALESCE(shipped_email_message_id::text,''),
       COALESCE(correlation_id,''), COALESCE(causation_id,'')
FROM shipments
WHERE idempotency_key = $1
FOR UPDATE`, tmpl.IdempotencyKey).Scan(
		&s.ID, &s.OrderID, &status,
		&s.Carrier, &s.TrackingNumber, &s.TrackingURL,
		&s.Provider, &s.ProviderShipmentID,
		&s.IdempotencyKey, &s.LastError, &s.AttemptCount,
		&s.ShippedEmailMessageID,
		&s.CorrelationID, &s.CausationID,
	)
	if err != nil {
		return nil, fmt.Errorf("lock shipment: %w", err)
	}
	s.Status = ShipmentStatus(status)
	return &s, nil
}

func (PgxStore) UpdateShipment(ctx context.Context, tx pgx.Tx, s *ShipmentRow) error {
	_, err := tx.Exec(ctx, `
UPDATE shipments
SET status               = $2,
    carrier              = NULLIF($3,''),
    tracking_number      = NULLIF($4,''),
    tracking_url         = NULLIF($5,''),
    provider_shipment_id = NULLIF($6,''),
    last_error           = NULLIF($7,''),
    attempt_count        = $8,
    updated_at           = now()
WHERE id = $1`,
		s.ID, string(s.Status),
		s.Carrier, s.TrackingNumber, s.TrackingURL,
		s.ProviderShipmentID, s.LastError, s.AttemptCount,
	)
	if err != nil {
		return fmt.Errorf("update shipment: %w", err)
	}
	return nil
}

func (PgxStore) InsertShipmentEvent(ctx context.Context, tx pgx.Tx, ev *ShipmentEventRow) error {
	_, err := tx.Exec(ctx, `
INSERT INTO shipment_events (shipment_id, event_type, payload)
VALUES ($1, $2, $3)`,
		ev.ShipmentID, ev.EventType, ev.Payload,
	)
	if err != nil {
		return fmt.Errorf("insert shipment event: %w", err)
	}
	return nil
}

func (PgxStore) UpdateEmailTemplateData(ctx context.Context, tx pgx.Tx, emailMessageID string, data ShipmentEmailData) error {
	if emailMessageID == "" {
		return nil
	}
	raw, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("marshal shipment email data: %w", err)
	}
	_, err = tx.Exec(ctx, `
UPDATE email_messages
SET template_data = $2,
    updated_at    = now()
WHERE id = $1`,
		emailMessageID, raw,
	)
	if err != nil {
		return fmt.Errorf("update shipment email template data: %w", err)
	}
	return nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
