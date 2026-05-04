// Command engine is the entrypoint for the Blond Beauty async execution
// engine. It connects to Postgres, RabbitMQ, and (optionally) Redis, runs
// outbox publishing and registered workers, and exposes /healthz, /readyz,
// and /metrics on a single internal HTTP port.
//
// The engine never exposes business HTTP endpoints. Public/customer traffic
// is owned by the API. See README.md for the architectural contract.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/blondbeauty/blond-beauty-engine/internal/cache"
	"github.com/blondbeauty/blond-beauty-engine/internal/config"
	"github.com/blondbeauty/blond-beauty-engine/internal/db"
	"github.com/blondbeauty/blond-beauty-engine/internal/email"
	emailfake "github.com/blondbeauty/blond-beauty-engine/internal/email/providers/fake"
	"github.com/blondbeauty/blond-beauty-engine/internal/fiscal"
	fiscalfake "github.com/blondbeauty/blond-beauty-engine/internal/fiscal/providers/fake"
	"github.com/blondbeauty/blond-beauty-engine/internal/fulfillment"
	fulfillmentfake "github.com/blondbeauty/blond-beauty-engine/internal/fulfillment/providers/fake"
	"github.com/blondbeauty/blond-beauty-engine/internal/observability"
	"github.com/blondbeauty/blond-beauty-engine/internal/outbox"
	"github.com/blondbeauty/blond-beauty-engine/internal/payments"
	"github.com/blondbeauty/blond-beauty-engine/internal/payments/providers/fake"
	"github.com/blondbeauty/blond-beauty-engine/internal/queue"
	"github.com/blondbeauty/blond-beauty-engine/internal/workers"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	logger := observability.NewLogger(cfg.LogLevel, cfg.ServiceName, cfg.Env)
	slog.SetDefault(logger)
	logger.Info("starting", "version", "slice-a")

	// Root context terminated by SIGINT/SIGTERM. After cancellation we still
	// need a separate timeout context to drive shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	startCtx, cancelStart := context.WithTimeout(ctx, 30*time.Second)
	defer cancelStart()

	pool, err := db.NewPool(startCtx, cfg.PostgresURL, cfg.PostgresMaxConns)
	if err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	defer pool.Close()

	rabbit, err := queue.Dial(cfg.RabbitURL)
	if err != nil {
		return fmt.Errorf("rabbitmq: %w", err)
	}
	defer rabbit.Close()

	redis, err := cache.NewClient(startCtx, cfg.RedisURL)
	if err != nil {
		return fmt.Errorf("redis: %w", err)
	}
	if redis != nil {
		defer redis.Close()
	}

	metrics := observability.NewMetrics()
	httpSrv := observability.NewHTTPServer(cfg.HTTPAddr, metrics, map[string]observability.ReadinessCheck{
		"postgres": db.Ping(pool),
		"rabbitmq": rabbit.HealthCheck(),
		"redis":    cache.Ping(redis),
	})
	httpServerErr := make(chan error, 1)
	go func() {
		logger.Info("http server listening", "addr", cfg.HTTPAddr)
		httpServerErr <- httpSrv.ListenAndServe()
	}()

	// Outbox publisher.
	pub, err := queue.NewPublisher(rabbit)
	if err != nil {
		return fmt.Errorf("publisher: %w", err)
	}
	defer pub.Close()
	outboxPub := outbox.New(logger, pool, pub, metrics, outbox.Options{})

	// Workers.
	idem := workers.NewIdempotencyStore(pool)
	var registered []*workers.Worker
	if cfg.EnableFakeWorker {
		registered = append(registered, workers.New(
			workers.Config{Queue: workers.FakeQueue, Concurrency: 4},
			logger, pool, rabbit, idem, metrics,
			workers.NewFakeHandler(logger),
		))
	}

	if cfg.EnablePaymentsWorkers {
		registry := payments.NewRegistry()
		// Slice B ships only the fake provider; real adapters land in Slice G.
		registry.Register(fake.New())
		store := payments.NewPgxStore()
		sink := payments.OutboxSink{}

		paySvc := payments.NewService(logger, registry, store, sink)
		for queueName, handler := range paySvc.Handlers() {
			registered = append(registered, workers.New(
				workers.Config{Queue: queueName, Concurrency: cfg.PaymentsConcurrency},
				logger, pool, rabbit, idem, metrics, handler,
			))
		}

		// Slice C: webhook consumer. Ingress is owned by the API; the Engine
		// consumes verified events from the payments.webhook queue.
		webhookSvc := payments.NewWebhookService(logger, pool, registry, store, sink)
		registered = append(registered, workers.New(
			workers.Config{Queue: payments.QueueWebhook, Concurrency: cfg.PaymentsConcurrency},
			logger, pool, rabbit, idem, metrics, webhookSvc.WebhookHandler(),
		))

		logger.Info("payments workers registered", "providers", registry.Names())
	}

	if cfg.EnableEmailWorkers {
		emailProv := emailfake.New()
		emailStore := email.NewPgxStore()
		emailSink := email.OutboxSink{}
		emailSvc := email.NewService(logger, emailProv, emailStore, email.NewRegistry(), emailSink)
		registered = append(registered, workers.New(
			workers.Config{Queue: email.QueueTransactional, Concurrency: cfg.EmailsConcurrency},
			logger, pool, rabbit, idem, metrics, emailSvc.Handler(),
		))
		logger.Info("email worker registered", "provider", emailProv.Name())
	}

	if cfg.EnableFiscalWorkers {
		fiscalProv := fiscalfake.New()
		fiscalStore := fiscal.NewPgxStore()
		fiscalSink := fiscal.OutboxSink{}
		fiscalSvc := fiscal.NewService(logger, fiscalProv, fiscalStore, fiscalSink)
		for queueName, handler := range fiscalSvc.Handlers() {
			registered = append(registered, workers.New(
				workers.Config{Queue: queueName, Concurrency: cfg.FiscalConcurrency},
				logger, pool, rabbit, idem, metrics, handler,
			))
		}
		logger.Info("fiscal workers registered", "provider", fiscalProv.Name())
	}

	if cfg.EnableFulfillmentWorkers {
		fulfillProv := fulfillmentfake.New()
		fulfillStore := fulfillment.NewPgxStore()
		fulfillSink := fulfillment.OutboxSink{}
		fulfillSvc := fulfillment.NewService(logger, fulfillProv, fulfillStore, fulfillSink)
		registered = append(registered, workers.New(
			workers.Config{Queue: fulfillment.QueueFulfillment, Concurrency: cfg.FulfillmentConcurrency},
			logger, pool, rabbit, idem, metrics, fulfillSvc.Handler(),
		))
		logger.Info("fulfillment worker registered", "provider", fulfillProv.Name())
	}

	httpSrv.SetReady(true)

	// Run outbox + workers under the root context.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := outboxPub.Run(ctx); err != nil {
			logger.Error("outbox publisher exited", "err", err)
		}
	}()
	for _, w := range registered {
		wg.Add(1)
		go func(w *workers.Worker) {
			defer wg.Done()
			if err := w.Run(ctx); err != nil {
				logger.Error("worker exited", "err", err)
			}
		}(w)
	}

	// Wait for shutdown signal or HTTP server failure.
	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	case err := <-httpServerErr:
		if err != nil {
			logger.Error("http server failed", "err", err)
		}
		stop()
	}

	httpSrv.SetReady(false)

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancelShutdown()

	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		logger.Error("http shutdown", "err", err)
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
		logger.Info("workers drained")
	case <-shutdownCtx.Done():
		logger.Warn("shutdown timeout; some workers did not drain")
		return errors.New("shutdown timeout")
	}

	logger.Info("bye")
	return nil
}
