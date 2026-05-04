// Package idempotency builds deterministic keys for provider calls and other
// retry-safe operations.
package idempotency

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

type keyMaterial struct {
	Version int      `json:"v"`
	Scope   string   `json:"scope"`
	Parts   []string `json:"parts"`
}

// Key returns an opaque SHA-256 key for ordered, typed key material.
//
// The material is encoded as structured JSON before hashing, so callers do not
// depend on delimiters that may also appear in user-controlled identifiers.
func Key(scope string, parts ...string) string {
	raw, err := json.Marshal(keyMaterial{
		Version: 1,
		Scope:   scope,
		Parts:   append([]string(nil), parts...),
	})
	if err != nil {
		panic("idempotency: marshal key material: " + err.Error())
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
