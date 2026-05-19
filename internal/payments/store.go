package payments

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// PaymentRow is the canonical projection of the API-owned `payments` table
// that the engine reads/writes. The API owns the schema; this struct mirrors
// only the columns the engine needs.
//
// Expected schema (API-owned; documented in README):
//
//	create table payments (
//	    id                  uuid primary key,
//	    order_id            uuid not null,
//	    provider            text not null,
//	    provider_payment_id text,
//	    amount_cents        bigint not null,
//	    currency            text   not null,
//	    status              text   not null,
//	    last_error          text,
//	    created_at          timestamptz not null default now(),
//	    updated_at          timestamptz not null default now()
//	);
type PaymentRow struct {
	ID                string
	OrderID           string
	Provider          string
	ProviderPaymentID string
	AmountCents       int64
	Currency          string
	Status            Status
}

// AttemptRow records one provider call. Expected schema (API-owned):
//
//	create table payment_attempts (
//	    id                       uuid primary key default gen_random_uuid(),
//	    payment_id               uuid not null references payments(id) on delete cascade,
//	    provider                 text not null,
//	    provider_operation_id    text,
//	    operation_type           text not null,
//	    status                   text not null,
//	    request_hash             bytea,
//	    response_snapshot        jsonb,
//	    error_code               text,
//	    created_at               timestamptz not null default now()
//	);
//
// Fields not present as dedicated columns (CanonicalStatus, ErrorMessage,
// RedactedRequest, RedactedResponse) are encoded into response_snapshot JSONB.
type AttemptRow struct {
	ID               string
	PaymentID        string
	Provider         string // maps to provider column (NOT NULL)
	Operation        Operation
	RequestID        string // maps to provider_operation_id
	Status           string // "ok" | "failed"
	CanonicalStatus  Status // stored in response_snapshot
	ErrorCode        string
	ErrorMessage     string          // stored in response_snapshot
	RedactedRequest  json.RawMessage // stored in response_snapshot
	RedactedResponse json.RawMessage // stored in response_snapshot
}

// IngressRow is the engine's read/write projection of the API-owned
// `webhook_ingress_events` table. The Engine is the only component that
// transitions verification_status from "received" → "verified" or "rejected".
//
// Expected schema (API-owned; documented in README):
//
//	create table webhook_ingress_events (
//	    id                   uuid primary key,
//	    provider             text not null,
//	    raw_headers          jsonb not null,   -- canonicalised (single-value) map
//	    raw_body             bytea not null,
//	    received_at          timestamptz not null default now(),
//	    verification_status  text not null default 'received'
//	        check (verification_status in ('received','verified','rejected')),
//	    provider_event_id    text,             -- set after successful parse
//	    rejection_reason     text,
//	    updated_at           timestamptz not null default now()
//	);
//	create unique index on webhook_ingress_events (provider, provider_event_id)
//	    where provider_event_id is not null;
type IngressRow struct {
	ID                 string
	Provider           string
	RawHeaders         map[string]string
	RawBody            []byte
	VerificationStatus string // "received" | "verified" | "rejected"
	ProviderEventID    string // empty until parsed
}

// PaymentEventRow records one canonical webhook event for audit and dedup.
//
// Expected schema (API-owned):
//
//	create table payment_events (
//	    id                 uuid primary key,
//	    payment_id         uuid references payments(id),
//	    provider           text not null,
//	    provider_event_id  text not null,
//	    event_type         text not null,
//	    canonical_status   text not null,
//	    occurred_at        timestamptz not null,
//	    ingress_id         uuid references webhook_ingress_events(id),
//	    redacted_payload   jsonb,
//	    created_at         timestamptz not null default now(),
//	    unique (provider, provider_event_id)
//	);
type PaymentEventRow struct {
	ID              string
	PaymentID       string // may be empty if payment not found yet
	Provider        string
	ProviderEventID string
	EventType       string
	CanonicalStatus Status
	OccurredAt      time.Time
	IngressID       string
	RedactedPayload json.RawMessage
}

// Store abstracts payment persistence so the service can be unit-tested
// without a real Postgres connection. The pgx implementation operates on
// the caller's transaction throughout.
type Store interface {
	// Slice B
	LockPayment(ctx context.Context, tx pgx.Tx, paymentID string) (*PaymentRow, error)
	UpdatePayment(ctx context.Context, tx pgx.Tx, p *PaymentRow, lastError string) error
	InsertAttempt(ctx context.Context, tx pgx.Tx, a *AttemptRow) error

	// Slice C — webhook path
	LockIngress(ctx context.Context, tx pgx.Tx, ingressID string) (*IngressRow, error)
	MarkIngressVerified(ctx context.Context, tx pgx.Tx, ingressID, providerEventID string) error
	MarkIngressRejected(ctx context.Context, tx pgx.Tx, ingressID, reason string) error
	LockPaymentByProviderID(ctx context.Context, tx pgx.Tx, provider, providerPaymentID string) (*PaymentRow, error)
	// InsertPaymentEvent inserts the event row. Returns (true, nil) when the
	// row was inserted, (false, nil) when it already exists (duplicate by
	// provider + provider_event_id).
	InsertPaymentEvent(ctx context.Context, tx pgx.Tx, e *PaymentEventRow) (inserted bool, err error)
}

// PgxStore is the production Store backed by pgx. All methods operate on the
// transaction passed in by the worker framework.
type PgxStore struct{}

func NewPgxStore() *PgxStore { return &PgxStore{} }

func (PgxStore) LockPayment(ctx context.Context, tx pgx.Tx, id string) (*PaymentRow, error) {
	var p PaymentRow
	err := tx.QueryRow(ctx, `
SELECT id, order_id, provider, COALESCE(provider_payment_id, ''),
       amount_cents, currency, status
FROM payments
WHERE id = $1
FOR UPDATE`, id).Scan(
		&p.ID, &p.OrderID, &p.Provider, &p.ProviderPaymentID,
		&p.AmountCents, &p.Currency, &p.Status,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: %s", ErrPaymentNotFound, id)
	}
	if err != nil {
		return nil, fmt.Errorf("lock payment: %w", err)
	}
	if !p.Status.Valid() {
		return nil, fmt.Errorf("invalid status %q for payment %s", p.Status, p.ID)
	}
	return &p, nil
}

func (PgxStore) UpdatePayment(ctx context.Context, tx pgx.Tx, p *PaymentRow, lastError string) error {
	var le any
	if lastError != "" {
		le = lastError
	}
	_, err := tx.Exec(ctx, `
UPDATE payments
SET provider_payment_id = NULLIF($2, ''),
    status = $3,
    last_error = $4,
    updated_at = now()
WHERE id = $1`, p.ID, p.ProviderPaymentID, string(p.Status), le)
	if err != nil {
		return fmt.Errorf("update payment: %w", err)
	}
	return nil
}

func (PgxStore) InsertAttempt(ctx context.Context, tx pgx.Tx, a *AttemptRow) error {
	// Pack fields the API schema doesn't expose as dedicated columns into a
	// single response_snapshot JSONB so they're still observable for debugging.
	type snapFields struct {
		CanonicalStatus string          `json:"canonical_status,omitempty"`
		ErrorMessage    string          `json:"error_message,omitempty"`
		Request         json.RawMessage `json:"request,omitempty"`
		Response        json.RawMessage `json:"response,omitempty"`
	}
	snap := snapFields{
		CanonicalStatus: string(a.CanonicalStatus),
		ErrorMessage:    a.ErrorMessage,
		Request:         a.RedactedRequest,
		Response:        a.RedactedResponse,
	}
	var snapJSON json.RawMessage
	if snap.CanonicalStatus != "" || snap.ErrorMessage != "" ||
		len(snap.Request) > 0 || len(snap.Response) > 0 {
		snapJSON, _ = json.Marshal(snap)
	}

	_, err := tx.Exec(ctx, `
INSERT INTO payment_attempts
    (id, payment_id, provider, provider_operation_id, operation_type,
     status, response_snapshot, error_code)
VALUES ($1, $2, $3, NULLIF($4,''), $5, $6, $7::jsonb, $8)`,
		a.ID, a.PaymentID, a.Provider, a.RequestID, string(a.Operation),
		a.Status, snapJSON, nullable(a.ErrorCode),
	)
	if err != nil {
		return fmt.Errorf("insert payment attempt: %w", err)
	}
	return nil
}

func nullable(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// ---- Slice C: webhook store methods ----

func (PgxStore) LockIngress(ctx context.Context, tx pgx.Tx, id string) (*IngressRow, error) {
	var r IngressRow
	var rawHeaders []byte
	err := tx.QueryRow(ctx, `
SELECT id, provider, raw_headers, raw_body, verification_status,
       COALESCE(provider_event_id, '')
FROM webhook_ingress_events
WHERE id = $1
FOR UPDATE`, id).Scan(
		&r.ID, &r.Provider, &rawHeaders, &r.RawBody, &r.VerificationStatus, &r.ProviderEventID,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("webhook ingress not found: %s", id)
	}
	if err != nil {
		return nil, fmt.Errorf("lock ingress: %w", err)
	}
	if len(rawHeaders) > 0 {
		_ = json.Unmarshal(rawHeaders, &r.RawHeaders)
	}
	return &r, nil
}

func (PgxStore) MarkIngressVerified(ctx context.Context, tx pgx.Tx, id, providerEventID string) error {
	_, err := tx.Exec(ctx, `
UPDATE webhook_ingress_events
SET verification_status = 'verified',
    provider_event_id   = NULLIF($2, ''),
    updated_at          = now()
WHERE id = $1`, id, providerEventID)
	if err != nil {
		return fmt.Errorf("mark ingress verified: %w", err)
	}
	return nil
}

func (PgxStore) MarkIngressRejected(ctx context.Context, tx pgx.Tx, id, reason string) error {
	_, err := tx.Exec(ctx, `
UPDATE webhook_ingress_events
SET verification_status = 'rejected',
    rejection_reason    = $2,
    updated_at          = now()
WHERE id = $1`, id, reason)
	if err != nil {
		return fmt.Errorf("mark ingress rejected: %w", err)
	}
	return nil
}

func (PgxStore) LockPaymentByProviderID(ctx context.Context, tx pgx.Tx, provider, providerPaymentID string) (*PaymentRow, error) {
	var p PaymentRow
	err := tx.QueryRow(ctx, `
SELECT id, order_id, provider, COALESCE(provider_payment_id, ''),
       amount_cents, currency, status
FROM payments
WHERE provider = $1 AND provider_payment_id = $2
FOR UPDATE`, provider, providerPaymentID).Scan(
		&p.ID, &p.OrderID, &p.Provider, &p.ProviderPaymentID,
		&p.AmountCents, &p.Currency, &p.Status,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: provider=%s provider_payment_id=%s",
			ErrPaymentNotFound, provider, providerPaymentID)
	}
	if err != nil {
		return nil, fmt.Errorf("lock payment by provider id: %w", err)
	}
	return &p, nil
}

func (PgxStore) InsertPaymentEvent(ctx context.Context, tx pgx.Tx, e *PaymentEventRow) (bool, error) {
	err := tx.QueryRow(ctx, `
INSERT INTO payment_events
    (id, payment_id, provider, provider_event_id, event_type,
     canonical_status, occurred_at, ingress_id, redacted_payload)
VALUES ($1, NULLIF($2,'')::uuid, $3, $4, $5, $6, $7, NULLIF($8,'')::uuid, $9::jsonb)
ON CONFLICT (provider, provider_event_id) DO NOTHING
RETURNING id`,
		e.ID, e.PaymentID, e.Provider, e.ProviderEventID, e.EventType,
		string(e.CanonicalStatus), e.OccurredAt, e.IngressID, e.RedactedPayload,
	).Scan(new(string))
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil // conflict → duplicate
	}
	if err != nil {
		return false, fmt.Errorf("insert payment event: %w", err)
	}
	return true, nil
}
