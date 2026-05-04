// Package config loads engine configuration from environment variables.
//
// All sensitive values (DB DSN, RabbitMQ URL, Redis URL) come from env;
// defaults are only used for non-sensitive operational knobs.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Env         string // "dev" | "staging" | "prod"
	ServiceName string
	LogLevel    string // "debug" | "info" | "warn" | "error"

	HTTPAddr string // bind address for /healthz, /readyz, /metrics

	PostgresURL      string
	PostgresMaxConns int32

	RabbitURL      string
	RabbitPrefetch int

	RedisURL string

	ShutdownTimeout time.Duration

	// Slice A demo flag; remove once real workers exist.
	EnableFakeWorker bool

	// Slice B: payments workers. Default on.
	EnablePaymentsWorkers bool
	// Per-queue concurrency for payment workers. Single knob in v1; can split
	// per-operation later if a single provider becomes a bottleneck.
	PaymentsConcurrency int

	// Slice D: email worker. Default on.
	EnableEmailWorkers bool
	EmailsConcurrency  int

	// Slice E: fiscal worker. Default on.
	EnableFiscalWorkers bool
	FiscalConcurrency   int

	// Slice F: fulfillment worker. Default on.
	EnableFulfillmentWorkers bool
	FulfillmentConcurrency   int
}

func Load() (*Config, error) {
	cfg := &Config{
		Env:                      getenv("ENGINE_ENV", "dev"),
		ServiceName:              getenv("ENGINE_SERVICE_NAME", "blond-beauty-engine"),
		LogLevel:                 getenv("ENGINE_LOG_LEVEL", "info"),
		HTTPAddr:                 getenv("ENGINE_HTTP_ADDR", ":8081"),
		PostgresURL:              os.Getenv("ENGINE_POSTGRES_URL"),
		RabbitURL:                os.Getenv("ENGINE_RABBIT_URL"),
		RedisURL:                 os.Getenv("ENGINE_REDIS_URL"),
		EnableFakeWorker:         getenvBool("ENGINE_ENABLE_FAKE_WORKER", true),
		EnablePaymentsWorkers:    getenvBool("ENGINE_ENABLE_PAYMENTS_WORKERS", true),
		EnableEmailWorkers:       getenvBool("ENGINE_ENABLE_EMAIL_WORKERS", true),
		EnableFiscalWorkers:      getenvBool("ENGINE_ENABLE_FISCAL_WORKERS", true),
		EnableFulfillmentWorkers: getenvBool("ENGINE_ENABLE_FULFILLMENT_WORKERS", true),
	}

	pc, err := getenvInt("ENGINE_PAYMENTS_CONCURRENCY", 4)
	if err != nil {
		return nil, err
	}
	cfg.PaymentsConcurrency = pc

	ec, err := getenvInt("ENGINE_EMAILS_CONCURRENCY", 4)
	if err != nil {
		return nil, err
	}
	cfg.EmailsConcurrency = ec

	fc, err := getenvInt("ENGINE_FISCAL_CONCURRENCY", 2)
	if err != nil {
		return nil, err
	}
	cfg.FiscalConcurrency = fc

	ffc, err := getenvInt("ENGINE_FULFILLMENT_CONCURRENCY", 4)
	if err != nil {
		return nil, err
	}
	cfg.FulfillmentConcurrency = ffc

	maxConns, err := getenvInt32("ENGINE_POSTGRES_MAX_CONNS", 10)
	if err != nil {
		return nil, err
	}
	cfg.PostgresMaxConns = maxConns

	prefetch, err := getenvInt("ENGINE_RABBIT_PREFETCH", 32)
	if err != nil {
		return nil, err
	}
	cfg.RabbitPrefetch = prefetch

	shutdown, err := getenvDuration("ENGINE_SHUTDOWN_TIMEOUT", 30*time.Second)
	if err != nil {
		return nil, err
	}
	cfg.ShutdownTimeout = shutdown

	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func (c *Config) Validate() error {
	var errs []string
	if c.PostgresURL == "" {
		errs = append(errs, "ENGINE_POSTGRES_URL is required")
	}
	if c.RabbitURL == "" {
		errs = append(errs, "ENGINE_RABBIT_URL is required")
	}
	// Redis is optional in Slice A; treat empty as disabled.
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

func getenv(k, def string) string {
	if v, ok := os.LookupEnv(k); ok && v != "" {
		return v
	}
	return def
}

func getenvBool(k string, def bool) bool {
	v, ok := os.LookupEnv(k)
	if !ok || v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return b
}

func getenvInt(k string, def int) (int, error) {
	v, ok := os.LookupEnv(k)
	if !ok || v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", k, err)
	}
	return n, nil
}

func getenvInt32(k string, def int32) (int32, error) {
	n, err := getenvInt(k, int(def))
	if err != nil {
		return 0, err
	}
	return int32(n), nil
}

func getenvDuration(k string, def time.Duration) (time.Duration, error) {
	v, ok := os.LookupEnv(k)
	if !ok || v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", k, err)
	}
	return d, nil
}
