// Package observability wires structured logging, Prometheus metrics, and
// HTTP health/readiness endpoints. The engine never exposes business HTTP
// endpoints; this package is the only HTTP surface.
package observability

import (
	"log/slog"
	"os"
	"strings"
)

func NewLogger(level, service, env string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})
	return slog.New(h).With("service", service, "env", env)
}
