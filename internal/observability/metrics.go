package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Metrics struct {
	Registry *prometheus.Registry

	MessagesProcessed *prometheus.CounterVec   // labels: queue, result (ok|retry|dlq|skip)
	MessageLatency    *prometheus.HistogramVec // labels: queue
	WorkerInflight    *prometheus.GaugeVec     // labels: queue
	OutboxPublished   *prometheus.CounterVec   // labels: result (ok|fail)
	OutboxPending     prometheus.Gauge
}

func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	factory := promauto.With(reg)
	return &Metrics{
		Registry: reg,
		MessagesProcessed: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "engine_messages_processed_total",
			Help: "Messages processed by workers, by queue and outcome.",
		}, []string{"queue", "result"}),
		MessageLatency: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "engine_message_latency_seconds",
			Help:    "End-to-end worker handler latency.",
			Buckets: prometheus.DefBuckets,
		}, []string{"queue"}),
		WorkerInflight: factory.NewGaugeVec(prometheus.GaugeOpts{
			Name: "engine_worker_inflight",
			Help: "In-flight messages per queue.",
		}, []string{"queue"}),
		OutboxPublished: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "engine_outbox_published_total",
			Help: "Outbox messages published to RabbitMQ.",
		}, []string{"result"}),
		OutboxPending: factory.NewGauge(prometheus.GaugeOpts{
			Name: "engine_outbox_pending",
			Help: "Outbox rows pending publish.",
		}),
	}
}
