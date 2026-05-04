// Package message defines the canonical envelope for all RabbitMQ messages
// exchanged between the API and the Engine.
//
// The envelope is a versioned contract. Additive changes (new optional fields,
// new event_type values) are safe. Breaking changes require migration plus a
// dual-read/dual-write period.
package message

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// SchemaVersion is the current envelope schema version.
const SchemaVersion = 1

// Envelope is the canonical wire format. payload contents are owned per
// event_type and validated by handlers.
type Envelope struct {
	MessageID     string          `json:"message_id"`
	CorrelationID string          `json:"correlation_id"`
	CausationID   *string         `json:"causation_id"`
	EventType     string          `json:"event_type"`
	AggregateType string          `json:"aggregate_type"`
	AggregateID   string          `json:"aggregate_id"`
	SchemaVersion int             `json:"schema_version"`
	OccurredAt    time.Time       `json:"occurred_at"`
	Payload       json.RawMessage `json:"payload"`
}

// New returns an envelope with required fields populated.
func New(eventType, aggregateType, aggregateID string, payload any) (*Envelope, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}
	return &Envelope{
		MessageID:     uuid.NewString(),
		CorrelationID: uuid.NewString(),
		EventType:     eventType,
		AggregateType: aggregateType,
		AggregateID:   aggregateID,
		SchemaVersion: SchemaVersion,
		OccurredAt:    time.Now().UTC(),
		Payload:       raw,
	}, nil
}

// Validate enforces invariants required to safely route and dedupe a message.
func (e *Envelope) Validate() error {
	switch {
	case e == nil:
		return errors.New("envelope is nil")
	case e.MessageID == "":
		return errors.New("message_id required")
	case e.CorrelationID == "":
		return errors.New("correlation_id required")
	case e.EventType == "":
		return errors.New("event_type required")
	case e.AggregateType == "":
		return errors.New("aggregate_type required")
	case e.AggregateID == "":
		return errors.New("aggregate_id required")
	case e.SchemaVersion <= 0:
		return errors.New("schema_version must be > 0")
	case e.OccurredAt.IsZero():
		return errors.New("occurred_at required")
	}
	return nil
}

func Decode(raw []byte) (*Envelope, error) {
	var e Envelope
	if err := json.Unmarshal(raw, &e); err != nil {
		return nil, fmt.Errorf("decode envelope: %w", err)
	}
	if err := e.Validate(); err != nil {
		return nil, err
	}
	return &e, nil
}

func (e *Envelope) Encode() ([]byte, error) {
	if err := e.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(e)
}
