package email

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// MessageRow is the Engine's read/write projection of the API-owned
// `email_messages` table.
//
// Expected schema (API-owned; documented in README):
//
//	create table email_messages (
//	    id                  uuid primary key,
//	    email_type          text not null,
//	    recipient           text not null,         -- actual address; access-controlled
//	    recipient_hash      text not null,         -- sha256(lower(recipient)); safe to log
//	    subject             text not null,         -- pre-filled by API or left to engine
//	    template_data       jsonb not null,        -- SENSITIVE: PII, tokens, fiscal links
//	    status              text not null default 'queued'
//	        check (status in ('queued','sending','sent','failed')),
//	    idempotency_key     text not null,
//	    provider            text,
//	    provider_message_id text,
//	    last_error          text,
//	    attempt_count       int not null default 0,
//	    correlation_id      text,
//	    causation_id        text,
//	    created_at          timestamptz not null default now(),
//	    updated_at          timestamptz not null default now(),
//	    unique (idempotency_key)
//	);
type MessageRow struct {
	ID                string
	EmailType         EmailType
	Recipient         string // NEVER log — use RecipientHash
	RecipientHash     string // sha256(lower(recipient)) — safe to log
	Subject           string
	TemplateData      json.RawMessage // SENSITIVE — never log, never put in DLQ
	Status            string
	IdempotencyKey    string
	Provider          string
	ProviderMessageID string
	LastError         string
	AttemptCount      int
	CorrelationID     string
	CausationID       string
}

// ErrMessageNotFound is returned when the email_messages row does not exist.
// Permanent — DLQ.
var ErrMessageNotFound = errors.New("email message not found")

// ErrInvalidRecipient is returned when recipient is empty or malformed.
// Permanent — DLQ.
var ErrInvalidRecipient = errors.New("invalid email recipient")

// Store abstracts email_messages persistence for testability.
type Store interface {
	LockMessage(ctx context.Context, tx pgx.Tx, id string) (*MessageRow, error)
	UpdateMessage(ctx context.Context, tx pgx.Tx, m *MessageRow) error
}

// PgxStore is the production Store backed by pgx/v5.
type PgxStore struct{}

func NewPgxStore() *PgxStore { return &PgxStore{} }

func (PgxStore) LockMessage(ctx context.Context, tx pgx.Tx, id string) (*MessageRow, error) {
	var m MessageRow
	var emailType string
	err := tx.QueryRow(ctx, `
SELECT id, email_type, recipient, recipient_hash, subject,
       template_data, status, idempotency_key,
       COALESCE(provider, ''), COALESCE(provider_message_id, ''),
       COALESCE(last_error, ''), attempt_count,
       COALESCE(correlation_id, ''), COALESCE(causation_id, '')
FROM email_messages
WHERE id = $1
FOR UPDATE`, id).Scan(
		&m.ID, &emailType, &m.Recipient, &m.RecipientHash, &m.Subject,
		&m.TemplateData, &m.Status, &m.IdempotencyKey,
		&m.Provider, &m.ProviderMessageID,
		&m.LastError, &m.AttemptCount,
		&m.CorrelationID, &m.CausationID,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: %s", ErrMessageNotFound, id)
	}
	if err != nil {
		return nil, fmt.Errorf("lock email message: %w", err)
	}
	m.EmailType = EmailType(emailType)
	if m.Recipient == "" {
		return nil, fmt.Errorf("%w: id=%s", ErrInvalidRecipient, id)
	}
	return &m, nil
}

func (PgxStore) UpdateMessage(ctx context.Context, tx pgx.Tx, m *MessageRow) error {
	_, err := tx.Exec(ctx, `
UPDATE email_messages
SET status              = $2,
    provider            = NULLIF($3, ''),
    provider_message_id = NULLIF($4, ''),
    last_error          = NULLIF($5, ''),
    attempt_count       = $6,
    updated_at          = now()
WHERE id = $1`,
		m.ID, m.Status, m.Provider, m.ProviderMessageID, m.LastError, m.AttemptCount,
	)
	if err != nil {
		return fmt.Errorf("update email message: %w", err)
	}
	return nil
}
