# Blond Beauty Engine

Asynchronous execution layer for the Blond Beauty platform. The Engine consumes
durable commands/events from RabbitMQ, performs retryable/external/long-running
work, and writes canonical results back to Postgres for the API to expose.

The Engine is **not** a public HTTP API. The only HTTP it exposes is operational:
`/healthz`, `/readyz`, `/metrics`.

## ⚠ Engine MVP Freeze

**The Engine fake-provider async layer is frozen for API + frontend integration.**

Allowed work until final sign-off:
- Bug fixes found during integration
- README / env / docs corrections
- Contract mismatch fixes with the API (schema or payload mismatches)
- Test fixes if integration reveals a schema or payload discrepancy

Not allowed until the post-MVP roadmap begins:
- Getnet or any real payment provider
- Real SEFAZ / NF-e provider (requires contador review first)
- Real email provider (requires reset-token log/DLQ audit first)
- Real fulfillment / carrier integration
- New architecture expansion or new worker slices

**Post-MVP roadmap (approved order):**

1. Two-phase provider-call refactor across payments / email / fiscal / fulfillment
2. Getnet sandbox adapter (payments)
3. Real email provider with secure reset-token handling
4. Fiscal provider / SEFAZ strategy with qualified contador review
5. Production observability and deployment hardening

**Integration validation checklist (pending):**
- [ ] API emits the exact 13 consumed queue payloads documented here
- [ ] Engine writes only the documented API-owned tables
- [ ] Frontend can observe updated status through the API
- [ ] Fake checkout flow: order → payment fake → fiscal fake → email fake → fulfillment fake
- [ ] DLQ / retry / idempotency behaviour under duplicate messages
- [ ] No sensitive data appears in events, logs, DLQ, or test fixtures

---

## Status

**Slice G — MVP Hardening (current).** Contract tests for all consumed and emitted
message payloads; sensitive-data checklist tests; comprehensive documentation of
all queues, tables, event shapes, and fake provider behavior; graceful shutdown
with all workers enabled simultaneously.

**Slice F — Order Fulfillment.** Adds the `orders.fulfillment` consumer with full
precondition re-verification from DB (events are triggers, not authority),
`FindOrCreateShipment` idempotent upsert, carrier dispatch via `FulfillmentProvider`,
`TrackingURL` stored only in DB and email template data (never emitted), and
follow-up events (`order.fulfilled`, `order.fulfillment.failed`).

**Slice E — Fiscal Worker.** Adds the `fiscal.issue`, `fiscal.retry`, `fiscal.cancel`
consumers, `FiscalProvider` interface, deterministic fake provider. Fulfillment gating:
`order.fulfillment.requested` is emitted **only** after `fiscal.authorized`. NF-e email
template data is populated on authorization before requesting email delivery.

**Slice D — Email Worker.** Adds the `emails.transactional` consumer with
`EmailProvider` interface, six template types (`order_confirmation`, `payment_approved`,
`payment_failed`, `nfe_authorized`, `password_reset`, `security_alert`, `order_shipped`).
`recipient` is never logged — only `recipient_hash` (sha256-hex) appears in logs and events.

**Slice C — Webhooks.** Adds the `payments.webhook` consumer with provider signature
verification, `(provider, provider_event_id)` dedup, forward-only status transitions
with ambiguity resolution via provider lookup, rejection of forged/malformed ingress,
and follow-up event emission.

**Slice B — Payments Core.** Adds the canonical `payments.Provider` interface, a
deterministic fake provider, the six operation workers (`payments.create / confirm /
capture / cancel / refund / reconcile`), an `outbox.Enqueue` helper for follow-up
events, and unit tests. No real Getnet adapter yet — that lands in Slice G.

**Slice A — Runnable skeleton.** Delivers the engine entrypoint, config loader,
structured JSON logging, Postgres pool, RabbitMQ connection, Redis client, canonical
message envelope, worker framework, transactional outbox publisher, Prometheus metrics,
and one no-op fake worker.

## Architecture contract with the API

| Concern | Owner |
| --- | --- |
| Public HTTP / customer & admin endpoints | API |
| Domain schema (`orders`, `payments`, `fiscal_invoices`, `outbox_messages`, …) | API |
| Domain migrations | API |
| Outbox row writes (commands/events for the engine) | API |
| RabbitMQ message envelope schema | shared (versioned contract) |
| Outbox → RabbitMQ publishing | Engine (this project) |
| Worker consumption + provider calls + result persistence | Engine |
| Engine-operational tables (`engine_processed_messages`, …) | Engine |

The Engine treats the API DB schema as a **versioned contract**. The Engine does not
own or migrate domain tables; it only adds tables prefixed `engine_*` that are strictly
internal runtime concerns (idempotency dedup, etc.).

### Communication model

1. API validates a request and writes domain rows + an `outbox_messages` row in the
   same Postgres transaction.
2. Engine's outbox publisher reads pending rows and publishes to RabbitMQ with publisher
   confirms; the row is marked `published_at = now()` only after broker ack.
3. Engine workers consume the queue, perform work, and write results back to the
   API-owned tables in their own transaction.
4. API exposes updated state via HTTP read endpoints (frontend polls or subscribes).

The Engine does not call the API to "return" results.

## Queues

### Consumed by the Engine

| Queue | Event type consumed | Handler package |
| --- | --- | --- |
| `engine.fake.noop` | `noop.demo` | `internal/workers` |
| `payments.create` | `payment.create.requested` | `internal/payments` |
| `payments.confirm` | `payment.confirm.requested` | `internal/payments` |
| `payments.capture` | `payment.capture.requested` | `internal/payments` |
| `payments.cancel` | `payment.cancel.requested` | `internal/payments` |
| `payments.refund` | `payment.refund.requested` | `internal/payments` |
| `payments.reconcile` | `payment.reconcile.requested` | `internal/payments` |
| `payments.webhook` | `payment.webhook.received` | `internal/payments` |
| `emails.transactional` | `email.send.requested` | `internal/email` |
| `fiscal.issue` | `fiscal.issue.requested` | `internal/fiscal` |
| `fiscal.retry` | `fiscal.retry.requested` | `internal/fiscal` |
| `fiscal.cancel` | `fiscal.cancel.requested` | `internal/fiscal` |
| `orders.fulfillment` | `order.fulfillment.requested` | `internal/fulfillment` |

Each consumed queue has a corresponding `.dlq` dead-letter queue for permanently
rejected messages (signature failures, missing IDs, illegal transitions, unknown
providers).

### Emitted by the Engine

| Queue | Event type | Emitted by |
| --- | --- | --- |
| `payments.events` | `payment.authorized` | payments service |
| `payments.events` | `payment.captured` | payments service |
| `payments.events` | `payment.failed` | payments service |
| `payments.events` | `payment.refunded` | payments service |
| `payments.events` | `payment.cancelled` | payments service |
| `payments.events` | `payment.requires_action` | payments service |
| `emails.events` | `email.sent` | email service |
| `emails.events` | `email.failed` | email service |
| `fiscal.events` | `fiscal.authorized` | fiscal service |
| `fiscal.events` | `fiscal.rejected` | fiscal service |
| `fiscal.events` | `fiscal.cancelled` | fiscal service |
| `emails.transactional` | `email.send.requested` | fiscal service (NF-e email) |
| `emails.transactional` | `email.send.requested` | fulfillment service (shipped email) |
| `orders.fulfillment` | `order.fulfillment.requested` | fiscal service (fulfillment gate) |
| `orders.events` | `order.fulfilled` | fulfillment service |
| `orders.events` | `order.fulfillment.failed` | fulfillment service |

## API-owned schemas (contract)

These tables are created and migrated by the API. The Engine reads/writes them as a
versioned contract. **The Engine never creates or alters these tables.**

### `outbox_messages`

```sql
create table outbox_messages (
    id            uuid primary key,
    queue         text        not null,
    payload       jsonb       not null,   -- canonical envelope JSON
    headers       jsonb,                  -- optional amqp headers
    created_at    timestamptz not null default now(),
    published_at  timestamptz,
    attempts      int         not null default 0,
    last_error    text
);
create index on outbox_messages (published_at) where published_at is null;
```

If the table is missing, the publisher logs a warning once and idles.

### `payments` / `payment_attempts`

```sql
create table payments (
    id                  uuid primary key,
    order_id            uuid not null,
    provider            text not null,           -- "fake", "getnet", ...
    provider_payment_id text,                    -- set after first provider call
    amount_cents        bigint not null,
    currency            text   not null,         -- ISO 4217
    status              text   not null,         -- canonical (see below)
    last_error          text,
    created_at          timestamptz not null default now(),
    updated_at          timestamptz not null default now()
);

create table payment_attempts (
    id                uuid primary key,
    payment_id        uuid not null references payments(id),
    operation         text not null,             -- create|confirm|capture|cancel|refund|reconcile
    request_id        text not null,             -- engine-derived idempotency key
    status            text not null,             -- ok|failed
    canonical_status  text,                      -- result status
    error_code        text,
    error_message     text,
    redacted_request  jsonb,
    redacted_response jsonb,
    created_at        timestamptz not null default now(),
    unique (payment_id, operation, request_id)
);
```

### `webhook_ingress_events`

```sql
create table webhook_ingress_events (
    id                   uuid primary key,
    provider             text not null,
    raw_headers          jsonb not null,   -- canonicalised single-value map
    raw_body             bytea not null,
    received_at          timestamptz not null default now(),
    verification_status  text not null default 'received'
        check (verification_status in ('received','verified','rejected')),
    provider_event_id    text,
    rejection_reason     text,
    updated_at           timestamptz not null default now()
);
create unique index on webhook_ingress_events (provider, provider_event_id)
    where provider_event_id is not null;
```

### `payment_events`

```sql
create table payment_events (
    id                uuid primary key,
    payment_id        uuid references payments(id),
    provider          text not null,
    provider_event_id text not null,
    event_type        text not null,
    canonical_status  text not null,
    occurred_at       timestamptz not null,
    ingress_id        uuid references webhook_ingress_events(id),
    redacted_payload  jsonb,
    created_at        timestamptz not null default now(),
    unique (provider, provider_event_id)
);
```

### `email_messages`

```sql
create table email_messages (
    id                   uuid primary key,
    email_type           text not null,           -- order_confirmation|payment_approved|...
    recipient            text not null,           -- SENSITIVE — never logged by engine
    subject              text not null default '',
    status               text not null default 'queued',  -- queued|sending|sent|failed
    idempotency_key      text not null unique,
    template_data        jsonb,                   -- rendered by engine; may contain PII
    provider             text,                    -- set by engine after send
    provider_message_id  text,                    -- set by engine after send
    attempt_count        int  not null default 0,
    last_error           text,
    created_at           timestamptz not null default now(),
    updated_at           timestamptz not null default now()
);
```

The API pre-creates `email_messages` rows. For NF-e and shipment notifications, the
Engine writes `template_data` (via `UPDATE email_messages SET template_data = $2 WHERE
id = $1`) after the authorizing event, then emits `email.send.requested`.

### `fiscal_invoices`

```sql
create table fiscal_invoices (
    id                   uuid primary key,
    order_id             uuid not null,
    payment_id           uuid not null,
    provider             text not null,           -- "fake-fiscal", "sefaz-sp", ...
    model                text not null,           -- "nfe" | "nfce"
    series               text not null,
    status               text not null,           -- pending|issuing|authorized|rejected|cancelled
    access_key           text,                    -- 44-digit SEFAZ key (public)
    protocol             text,
    number               text,
    xml_storage_key      text,                    -- opaque internal ref; sign before sharing
    danfe_storage_key    text,                    -- opaque internal ref; sign before sharing
    rejection_code       text,
    rejection_message    text,
    total_cents          bigint not null,
    currency             text not null,
    attempt_count        int not null default 0,
    last_error           text,
    nfe_email_message_id uuid,                    -- pre-created email_messages row for NF-e email
    created_at           timestamptz not null default now(),
    updated_at           timestamptz not null default now()
);

create table fiscal_invoice_events (
    id                uuid primary key,
    fiscal_invoice_id uuid not null references fiscal_invoices(id),
    event_type        text not null,
    payload           jsonb,
    created_at        timestamptz not null default now()
);
```

### `shipments`

```sql
create table shipments (
    id                       uuid primary key,
    order_id                 uuid not null,
    status                   text not null,        -- pending|dispatched|failed|cancelled
    carrier                  text,
    tracking_number          text,
    tracking_url             text,                 -- SENSITIVE — never emitted in events
    provider                 text not null,
    provider_shipment_id     text,
    idempotency_key          text not null unique, -- "fulfill|<order_id>" (single-shipment)
    last_error               text,
    attempt_count            int not null default 0,
    shipped_email_message_id uuid,                 -- pre-created email_messages row
    correlation_id           text,
    causation_id             text,
    created_at               timestamptz not null default now(),
    updated_at               timestamptz not null default now()
);

create table shipment_events (
    id          uuid primary key,
    shipment_id uuid not null references shipments(id),
    event_type  text not null,
    payload     jsonb,
    created_at  timestamptz not null default now()
);
```

## Engine-owned schema

### `engine_processed_messages` (migration `001`)

```sql
create table engine_processed_messages (
    id         bigserial primary key,
    queue      text        not null,
    message_id text        not null,
    status     text        not null,   -- 'done' | 'skip'
    processed_at timestamptz not null default now(),
    unique (queue, message_id)
);
```

The Engine creates and manages this table via its own migration in `migrations/engine/`.

## Message envelope

```json
{
  "message_id":     "uuid",
  "correlation_id": "uuid",
  "causation_id":   "uuid-or-null",
  "event_type":     "payment.create.requested",
  "aggregate_type": "order",
  "aggregate_id":   "uuid",
  "schema_version": 1,
  "occurred_at":    "2026-05-03T12:00:00Z",
  "payload":        {}
}
```

Additive changes (new optional fields, new event types) are safe. Breaking changes
require migration plus a dual-read/dual-write period. See
[`internal/message/envelope.go`](internal/message/envelope.go).

## Event payload examples

### Consumed payloads (API → Engine)

**`payment.create.requested`** — queue `payments.create`
```json
{
  "payment_id": "uuid",
  "method":     "credit_card",
  "return_url": "https://example.com/return"
}
```

**`payment.capture.requested`** — queue `payments.capture`
```json
{
  "payment_id":   "uuid",
  "amount_cents": 10000
}
```

**`payment.webhook.received`** — queue `payments.webhook`
```json
{
  "ingress_id": "uuid"
}
```

**`email.send.requested`** — queue `emails.transactional`
```json
{
  "email_message_id": "uuid"
}
```

**`fiscal.issue.requested`** / **`fiscal.retry.requested`** — queues `fiscal.issue`, `fiscal.retry`
```json
{
  "fiscal_invoice_id": "uuid"
}
```

**`fiscal.cancel.requested`** — queue `fiscal.cancel`
```json
{
  "fiscal_invoice_id": "uuid",
  "reason":            "order cancelled"
}
```

**`order.fulfillment.requested`** — queue `orders.fulfillment`
```json
{
  "order_id":          "uuid",
  "fiscal_invoice_id": "uuid",
  "access_key":        "44444444444444444444444444444444444444444444"
}
```

> `fiscal_invoice_id` and `access_key` are informational. The fulfillment worker
> always re-reads full precondition state from DB (events are triggers, not authority).

### Emitted payloads (Engine → downstream)

**`payment.authorized`** / **`payment.captured`** / **`payment.failed`** etc. — queue `payments.events`
```json
{
  "payment_id":          "uuid",
  "order_id":            "uuid",
  "provider":            "fake",
  "provider_payment_id": "fake_pay_001",
  "status":              "authorized",
  "amount_cents":        10000,
  "currency":            "BRL"
}
```

**`email.sent`** — queue `emails.events`
```json
{
  "email_message_id":    "uuid",
  "email_type":          "payment_approved",
  "recipient_hash":      "sha256hex...",
  "provider":            "fake-email",
  "provider_message_id": "fake_email_001"
}
```

> `recipient` (raw address) is **never** in any event payload or log.

**`fiscal.authorized`** — queue `fiscal.events`
```json
{
  "fiscal_invoice_id": "uuid",
  "order_id":          "uuid",
  "payment_id":        "uuid",
  "model":             "nfe",
  "access_key":        "44444444444444444444444444444444444444444444",
  "protocol":          "135180000012345",
  "number":            "000000042",
  "provider":          "fake-fiscal"
}
```

> `xml_storage_key` and `danfe_storage_key` are **never** emitted. They are
> opaque internal refs and must be converted to signed URLs by the API before sharing.

**`order.fulfilled`** — queue `orders.events`
```json
{
  "order_id":    "uuid",
  "shipment_id": "uuid",
  "carrier":     "FAKE",
  "provider":    "fake-fulfillment"
}
```

> `tracking_url` and `tracking_number` are **never** emitted in events. The API
> reads `tracking_url` from the `shipments` table and exposes it via its own endpoints.

## Canonical payment statuses

`unknown · requires_action · authorized · captured · paid · failed ·
cancelled · refunded · chargeback`

Provider adapters MUST map their own strings to these. Business code never branches
on provider strings.

## Fake provider behavior

### Payments (`fake`)

Behavior is `amount_cents % 1000`-driven so tests need no configuration:

| `amount_cents % 1000` | Result |
| --- | --- |
| `13` | `ErrProviderRejected` (permanent) |
| `17` | `ErrProviderTransient` (retry) |
| `23` | `status = requires_action` with redirect URL |
| anything else | `status = authorized` |

Subsequent operations (`confirm`, `capture`, `cancel`, `refund`) follow a real
authorize-then-capture lifecycle in memory. The fake adapter verifies webhook
signatures via `sha256-hex(rawBody)` in the `X-Fake-Signature` header.

### Email (`fake-email`)

Behavior is address-driven:

| Recipient address contains | Result |
| --- | --- |
| `+fail@` | `ErrProviderPermanent` (→ DLQ) |
| `+transient@` | `ErrProviderTransient` (→ retry) |
| anything else | success, returns `fake_email_<uuid>` |

### Fiscal (`fake-fiscal`)

Behavior is `total_cents % 1000`-driven:

| `total_cents % 1000` | Result |
| --- | --- |
| `13` | `ErrProviderRejected`, rejection code `"225"` |
| `17` | `ErrProviderTransient` (→ retry) |
| anything else | authorized; returns deterministic 44-char access key |

### Fulfillment (`fake-fulfillment`)

Behavior is `order_id[-1]`-driven (last hex digit of the UUID):

| Last hex digit | Result |
| --- | --- |
| `f` | `ErrProviderPermanent` |
| `e` | `ErrProviderTransient` (→ retry) |
| anything else | dispatched; returns `FAKE` carrier, deterministic tracking number |

## Idempotency, retry, and DLQ

### Worker-level idempotency

Each worker records `(queue, message_id)` in `engine_processed_messages` inside the
same transaction as its business work. A duplicate message is acked without re-processing
(`result="skip"` in metrics).

### Domain-level idempotency

Each domain has additional dedup:

- **Payments**: `payment_attempts(payment_id, operation, request_id)` unique index.
  `request_id` is `sha256(message_id | payment_id | operation)`.
- **Webhook**: `payment_events(provider, provider_event_id)` unique index.
- **Email**: `email_messages.idempotency_key` unique constraint; already-sent guard in `Handle`.
- **Fiscal**: invoice status guard (already-authorized → ack); `idempotency_key` per
  call is `message_id|invoice_id|operation`.
- **Fulfillment**: `shipments.idempotency_key` = `"fulfill|" + order_id` unique; already-
  dispatched guard in `Handle`.

### Retry classification

| Error | Classification |
| --- | --- |
| `ErrProviderRejected`, `ErrIllegalTransition`, `ErrUnknownProvider`, not-found, decode failure | `workers.Permanent` → DLQ immediately |
| `ErrProviderTransient`, unclassified errors | plain error → exponential backoff retry |

The worker framework (see [`internal/workers/worker.go`](internal/workers/worker.go))
handles nack-with-requeue for transient errors and publishes to `.dlq` for permanent ones.

### Permanent errors and DLQ

Permanent errors are routed to `<queue>.dlq` (e.g., `payments.create.dlq`). Investigate
DLQ messages before discarding — they represent data integrity issues, not infrastructure
failures.

## Sensitive data checklist

The following data **must never** appear in RabbitMQ events, logs, metrics, or DLQ payloads.

| Category | Forbidden fields | Where it lives instead |
| --- | --- | --- |
| Payment card | `pan`, `cvv`, `card_number`, `card_holder` | Provider only (never persisted) |
| Auth credentials | `password`, `reset_url`, `reset_token`, `api_key` | `email_messages.template_data` only |
| Brazilian fiscal IDs | `cpf`, `cnpj`, `inscricao_estadual` | API-owned tables only |
| Customer contact | `recipient`, `email`, `phone`, `address` | `email_messages.recipient` only; hash in events |
| Fiscal raw content | `raw_xml`, `xml_content`, `danfe_content` | Storage layer; opaque refs in DB |
| Storage keys | `xml_storage_key`, `danfe_storage_key` | `fiscal_invoices` table only |
| Tracking link | `tracking_url` | `shipments.tracking_url`; API serves to frontend |

The `internal/contract/sensitive_test.go` test suite enforces these invariants for all
emitted event payload shapes.

### Logging rules

- `email_messages.recipient` — never logged; only `recipient_hash` in logs.
- `email_messages.template_data` — never logged; may contain reset links, NF-e URLs, PII.
- `shipments.tracking_url` — never logged; informational only via API.
- `raw_body` from `webhook_ingress_events` — read from DB, never echoed to logs.

## Running locally

Prerequisites: Postgres and RabbitMQ. Redis is optional.

```bash
cp .env.example .env
# fill in ENGINE_POSTGRES_URL and ENGINE_RABBIT_URL

# apply the engine-operational migration (requires psql or golang-migrate)
ENGINE_POSTGRES_URL='postgres://engine:engine@localhost:5432/blondbeauty?sslmode=disable' \
  make migrate-up

make run
```

Verify the server is up:

```bash
curl localhost:8081/healthz   # {"status":"ok"}
curl localhost:8081/readyz    # {"postgres":"ok","rabbitmq":"ok","redis":"ok"}
curl localhost:8081/metrics   # Prometheus text
```

### Local run sequence (full fake flow)

To exercise the complete pipeline locally using only fake providers:

1. Start the Engine (`make run`).
2. Publish a `payment.create.requested` envelope to the `payments.create` queue.
3. Engine creates the payment via the fake provider (`amount_cents != *13/*17/*23`
   for a clean `authorized` result).
4. Confirm the `payments` row is updated and a `payment.authorized` event appears in
   `outbox_messages`.
5. Simulate a webhook: insert a row into `webhook_ingress_events` (signature =
   `sha256-hex(rawBody)` in `raw_headers.X-Fake-Signature`), then publish
   `payment.webhook.received`.
6. Engine verifies, dedupes, transitions status, and emits another `payments.events` event.
7. Publish `fiscal.issue.requested` after the API creates a `fiscal_invoices` row.
8. Engine authorizes the invoice and emits both `fiscal.authorized` and
   `order.fulfillment.requested` in the same transaction.
9. Engine picks up `order.fulfillment.requested`, verifies preconditions, dispatches,
   and emits `order.fulfilled`.

### Verifying the fake worker end-to-end

The fake worker consumes queue `engine.fake.noop` and only logs. Publishing a valid
envelope to that queue exercises declare → consume → dedup → ack → metrics → DLQ-on-bad-payload.

```bash
rabbitmqadmin publish exchange="" routing_key="engine.fake.noop" \
  payload='{"message_id":"00000000-0000-0000-0000-000000000001","correlation_id":"00000000-0000-0000-0000-000000000002","event_type":"noop.demo","aggregate_type":"demo","aggregate_id":"00000000-0000-0000-0000-000000000003","schema_version":1,"occurred_at":"2026-05-03T12:00:00Z","payload":{}}'
```

Expected:
- Engine logs `fake worker handled message`.
- A row appears in `engine_processed_messages` with `status = 'done'`.
- Re-publishing the same `message_id` is acked as `result="skip"`.
- A malformed envelope is routed to `engine.fake.noop.dlq`.

## Environment variables

| Var | Required | Default | Notes |
| --- | --- | --- | --- |
| `ENGINE_POSTGRES_URL` | yes | — | shared DB with API |
| `ENGINE_RABBIT_URL` | yes | — | |
| `ENGINE_REDIS_URL` | no | `""` | leave empty to disable |
| `ENGINE_HTTP_ADDR` | no | `:8081` | bind for /healthz /readyz /metrics |
| `ENGINE_LOG_LEVEL` | no | `info` | `debug` / `info` / `warn` / `error` |
| `ENGINE_ENV` | no | `dev` | `dev` / `staging` / `prod`; fake providers are refused in `prod`/`production` |
| `ENGINE_SERVICE_NAME` | no | `blond-beauty-engine` | added to every log line |
| `ENGINE_POSTGRES_MAX_CONNS` | no | `10` | |
| `ENGINE_RABBIT_PREFETCH` | no | `32` | per-consumer prefetch |
| `ENGINE_SHUTDOWN_TIMEOUT` | no | `30s` | |
| `ENGINE_ENABLE_FAKE_WORKER` | no | `true` | Slice A demo; set `false` in prod |
| `ENGINE_ENABLE_PAYMENTS_WORKERS` | no | `true` | |
| `ENGINE_PAYMENTS_CONCURRENCY` | no | `4` | goroutines per payment queue |
| `ENGINE_ENABLE_EMAIL_WORKERS` | no | `true` | |
| `ENGINE_EMAILS_CONCURRENCY` | no | `4` | goroutines for emails.transactional |
| `ENGINE_ENABLE_FISCAL_WORKERS` | no | `true` | |
| `ENGINE_FISCAL_CONCURRENCY` | no | `2` | goroutines per fiscal queue |
| `ENGINE_ENABLE_FULFILLMENT_WORKERS` | no | `true` | |
| `ENGINE_FULFILLMENT_CONCURRENCY` | no | `4` | goroutines for orders.fulfillment |

Never put production credentials in `.env.example` or commit `.env`.
With `ENGINE_ENV=prod` or `ENGINE_ENV=production`, the Engine fails fast if any
currently fake-backed provider worker is enabled.

## Graceful shutdown

On `SIGINT` or `SIGTERM`:

1. HTTP server marks `/readyz` as not-ready (load balancers drain).
2. Root context is cancelled — all workers stop accepting new messages.
3. In-flight handlers are allowed to finish (up to `ENGINE_SHUTDOWN_TIMEOUT`).
4. HTTP server shuts down cleanly.
5. Postgres pool and RabbitMQ connections are closed.

If the shutdown timeout expires before all workers drain, the Engine logs a warning and
exits non-zero. In-flight messages will be redelivered by RabbitMQ (at-least-once
delivery). Worker idempotency guards handle the resulting duplicate.

## Development

```bash
make tidy        # go mod tidy
make build       # binary at ./bin/engine
make test        # unit tests
make test-race   # tests with -race
make vet         # go vet
make vuln        # govulncheck
```

All enabled workers can run simultaneously (tested in CI). There are no shared mutable
state assumptions between worker goroutines; each handler call gets its own DB
transaction from the pgx pool.

## Known temporary caveats

### TEMPORARY MODEL — provider calls inside DB transactions

All provider calls in Slices B–F happen **inside** the DB transaction that holds a
`SELECT … FOR UPDATE` lock on the domain row. For fake providers (instant, local)
this is harmless. For any real provider (Getnet, SEFAZ, Correios, SMTP), network
latency would hold the lock for seconds, risking lock-wait timeouts and degraded
throughput.

**Before wiring any live provider**, refactor the service to a two-phase model:

1. Short tx: claim/set status to an intermediate value (`issuing`, `sending`, `pending`).
2. External call **outside** any DB transaction.
3. Short tx: update status + persist result + enqueue follow-up event.

The unique indexes on `payment_attempts`, `email_messages.idempotency_key`,
`fiscal_invoices`, and `shipments.idempotency_key` together with the provider's own
idempotency key guarantee safety without a long-held lock.

### No real providers yet

| Domain | Current | Next |
| --- | --- | --- |
| Payments | `fake` | Getnet (Slice G) |
| Email | `fake-email` | Resend / SES (future) |
| Fiscal | `fake-fiscal` | SEFAZ SP (requires contador review) |
| Fulfillment | `fake-fulfillment` | Correios / Melhor Envio (future) |

The Engine's real Getnet adapter is explicitly deferred. The fake provider is the only
adapter that ships in this slice. The process refuses to boot fake-backed provider
workers when `ENGINE_ENV=prod` or `ENGINE_ENV=production`. Do not implement Getnet
until the two-phase model refactor is in place.

### Fiscal compliance notice

Production fiscal integration **must be reviewed and validated by a qualified Brazilian
accountant (contador) and tax specialist** before any real SEFAZ adapter is implemented.
The Engine enforces only structural correctness; it does not validate Brazilian tax rules,
CNPJ/CPF validity, CFOP codes, NCM codes, ICMS/PIS/COFINS calculations, or any other tax
obligation.

## Layout

```
cmd/engine                              entrypoint, graceful shutdown
internal/config                         env loader
internal/observability                  slog + prometheus + http (health/ready/metrics)
internal/db                             pgx/v5 pool
internal/cache                          optional redis client
internal/queue                          rabbitmq connection, declare, publisher with confirms
internal/message                        canonical envelope (versioned contract)
internal/workers                        worker lifecycle, idempotency, retry, DLQ, fake handler
internal/outbox                         transactional outbox: publisher + Enqueue helper
internal/payments                       provider interface, registry, store, service, webhook
internal/payments/providers/fake        deterministic fake adapter
internal/email                          email provider interface, template registry, service
internal/email/providers/fake           deterministic fake email adapter
internal/fiscal                         fiscal provider interface, store, service (NF-e/NFC-e)
internal/fiscal/providers/fake          deterministic fake fiscal adapter
internal/fulfillment                    fulfillment provider interface, store, service
internal/fulfillment/providers/fake     deterministic fake fulfillment adapter
internal/contract                       API contract tests: payload shapes + sensitive-data checks
migrations/engine                       engine-operational SQL (idempotency dedup only)
```
