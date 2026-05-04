package email

import (
	"crypto/sha256"
	"fmt"
	"strings"
)

// HashRecipient returns the hex-encoded SHA-256 of the email address.
// Store and log this value; never the raw address.
func HashRecipient(address string) string {
	h := sha256.Sum256([]byte(strings.ToLower(strings.TrimSpace(address))))
	return fmt.Sprintf("%x", h)
}

// MaskRecipient returns a display-safe masked form for human-readable logs:
// first two chars + "***@" + domain.  e.g. "jo***@example.com".
// Use only in development/debug output; production logs should use HashRecipient.
func MaskRecipient(address string) string {
	at := strings.LastIndex(address, "@")
	if at < 0 {
		return "***"
	}
	local := address[:at]
	domain := address[at+1:]
	if len(local) <= 2 {
		return "**@" + domain
	}
	return local[:2] + "***@" + domain
}
