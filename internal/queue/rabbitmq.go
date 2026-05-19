// Package queue wraps the RabbitMQ connection and channel lifecycle. It
// exposes a thin Conn type and helpers to declare queues, publish with
// publisher confirms, and consume with manual acks.
package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// DeliveryMode persistent ensures messages survive broker restart.
const persistentDelivery uint8 = 2

type Conn struct {
	url  string
	mu   sync.Mutex
	conn *amqp.Connection
}

func Dial(url string) (*Conn, error) {
	c := &Conn{url: url}
	if err := c.reconnect(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Conn) reconnect() error {
	conn, err := amqp.DialConfig(c.url, amqp.Config{
		Heartbeat: 10 * time.Second,
		Locale:    "en_US",
	})
	if err != nil {
		return fmt.Errorf("rabbit dial: %w", err)
	}
	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()
	return nil
}

func (c *Conn) raw() (*amqp.Connection, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil || c.conn.IsClosed() {
		return nil, errors.New("rabbit connection closed")
	}
	return c.conn, nil
}

func (c *Conn) Channel() (*amqp.Channel, error) {
	conn, err := c.raw()
	if err != nil {
		return nil, err
	}
	return conn.Channel()
}

// HealthCheck returns a ReadinessCheck-compatible function.
func (c *Conn) HealthCheck() func(ctx context.Context) error {
	return func(_ context.Context) error {
		_, err := c.raw()
		return err
	}
}

func (c *Conn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// QueueSpec describes a queue and its DLX wiring. Keep DLX naming consistent:
// `<queue>.dlq` paired with a default exchange routing key.
type QueueSpec struct {
	Name       string
	DLXName    string // dead-letter exchange (direct); typically "<queue>.dlx"
	DLQName    string // "<queue>.dlq"
	Durable    bool
	MaxRetries int // informational; enforced by handler envelope, not queue TTL
}

func DefaultSpec(queue string) QueueSpec {
	return QueueSpec{
		Name:       queue,
		DLXName:    queue + ".dlx",
		DLQName:    queue + ".dlq",
		Durable:    true,
		MaxRetries: 5,
	}
}

// Declare ensures the queue, DLX, and DLQ exist with consistent settings.
func Declare(ch *amqp.Channel, spec QueueSpec) error {
	if err := ch.ExchangeDeclare(spec.DLXName, "direct", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare dlx: %w", err)
	}
	if _, err := ch.QueueDeclare(spec.DLQName, spec.Durable, false, false, false, nil); err != nil {
		return fmt.Errorf("declare dlq: %w", err)
	}
	if err := ch.QueueBind(spec.DLQName, spec.Name, spec.DLXName, false, nil); err != nil {
		return fmt.Errorf("bind dlq: %w", err)
	}
	args := amqp.Table{
		"x-dead-letter-exchange":    spec.DLXName,
		"x-dead-letter-routing-key": spec.Name,
	}
	if _, err := ch.QueueDeclare(spec.Name, spec.Durable, false, false, false, args); err != nil {
		return fmt.Errorf("declare queue: %w", err)
	}
	return nil
}

// Publisher publishes JSON envelopes to the default exchange (direct to queue
// name) with publisher confirms.
//
// confirms is registered once at construction time. Calling NotifyPublish on
// every Publish would accumulate stale listener channels inside the amqp
// library and eventually starve the current call of its confirmation.
type Publisher struct {
	ch       *amqp.Channel
	mu       sync.Mutex
	confirms chan amqp.Confirmation
}

func NewPublisher(c *Conn) (*Publisher, error) {
	ch, err := c.Channel()
	if err != nil {
		return nil, err
	}
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("enable confirms: %w", err)
	}
	// Buffer large enough that a burst of concurrent publishes never blocks
	// the broker from sending confirms back to us.
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 128))
	return &Publisher{ch: ch, confirms: confirms}, nil
}

// Publish sends a payload to the named queue and waits for broker confirmation.
// Caller is responsible for ensuring the queue exists (call Declare).
func (p *Publisher) Publish(ctx context.Context, queue string, body []byte, headers amqp.Table) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	err := p.ch.PublishWithContext(ctx,
		"",    // default exchange (direct to queue)
		queue, // routing key = queue name
		true,  // mandatory: fail if unroutable
		false, // immediate (deprecated)
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: persistentDelivery,
			Timestamp:    time.Now().UTC(),
			Headers:      headers,
			Body:         body,
		},
	)
	if err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	select {
	case c, ok := <-p.confirms:
		if !ok {
			return errors.New("confirms channel closed")
		}
		if !c.Ack {
			return errors.New("publisher nack from broker")
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(10 * time.Second):
		return errors.New("publisher confirm timeout")
	}
}

func (p *Publisher) Close() error { return p.ch.Close() }
