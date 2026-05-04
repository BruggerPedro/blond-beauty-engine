package observability

import (
	"context"
	"encoding/json"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ReadinessCheck reports whether a dependency is healthy. Implementations must
// be cheap, non-blocking, and respect ctx (typically <1s).
type ReadinessCheck func(ctx context.Context) error

type HTTPServer struct {
	srv    *http.Server
	ready  atomic.Bool
	checks map[string]ReadinessCheck
}

func NewHTTPServer(addr string, m *Metrics, checks map[string]ReadinessCheck) *HTTPServer {
	h := &HTTPServer{checks: checks}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", h.healthz)
	mux.HandleFunc("/readyz", h.readyz)
	mux.Handle("/metrics", promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{Registry: m.Registry}))
	h.srv = &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return h
}

// SetReady toggles readiness. Set true after dependencies are connected and
// workers started; set false at the start of graceful shutdown.
func (h *HTTPServer) SetReady(v bool) { h.ready.Store(v) }

func (h *HTTPServer) ListenAndServe() error {
	err := h.srv.ListenAndServe()
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}

func (h *HTTPServer) Shutdown(ctx context.Context) error { return h.srv.Shutdown(ctx) }

func (h *HTTPServer) healthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

func (h *HTTPServer) readyz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if !h.ready.Load() {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "starting"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	results := make(map[string]string, len(h.checks))
	allOK := true
	for name, check := range h.checks {
		if err := check(ctx); err != nil {
			results[name] = err.Error()
			allOK = false
		} else {
			results[name] = "ok"
		}
	}
	status := http.StatusOK
	if !allOK {
		status = http.StatusServiceUnavailable
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"status": statusString(allOK), "checks": results})
}

func statusString(ok bool) string {
	if ok {
		return "ok"
	}
	return "degraded"
}
