# Blond Beauty Engine - Senior Implementation Prompt

This prompt is intentionally written in English for coding-agent quality. Keep Brazilian fiscal/legal terms as official domain terms: `NF-e`, `NFC-e`, `SEFAZ`, `DANFE`, `LGPD`, `CPF`, `CNPJ`, `CFOP`, `NCM`, `CEST`, `CST`, `CSOSN`, `ICMS`, `PIS`, `COFINS`, `IPI`.

## 1. Project Role

You are a senior backend engineer implementing the separate `blond-beauty-engine` project. This engine is not the public HTTP API. It is the asynchronous execution layer for the Blond Beauty platform.

The API project is described in `docs/backend-implementation-prompt.md`. The API owns customer/admin HTTP endpoints, validates requests, creates domain records, writes outbox messages in Postgres, and exposes current state to the frontend. The Engine consumes durable commands/events from RabbitMQ, performs retryable/external/long-running work, writes results back to Postgres, and publishes follow-up events.

The Engine must be production-grade, secure, idempotent, observable, horizontally scalable, and safe to run with multiple replicas.

## 2. Communication Model With the API

Primary communication is RabbitMQ plus the outbox pattern.

Flow:
1. API receives a customer/admin request.
2. API validates authorization, idempotency, stock/prices/totals as needed.
3. API writes domain rows plus `outbox_messages` in the same Postgres transaction.
4. Outbox publisher publishes commands/events to RabbitMQ with publisher confirms.
5. Engine consumes RabbitMQ messages.
6. Engine calls external providers or performs async work.
7. Engine writes results back to Postgres and may publish follow-up events.
8. API exposes updated state through HTTP read endpoints.

Result return strategy:
- Engine does not normally call the API to "return" results.
- Engine writes canonical results to shared Postgres tables/read models: `payments`, `payment_attempts`, `payment_events`, `webhook_ingress_events`, `orders`, `shipments`, `fiscal_invoices`, `fiscal_invoice_events`, `email_messages`, `report_exports`, `privacy_requests`, `audit_logs`, and related operational tables.
- API reads those results and returns them to frontend/admin.
- Frontend initially uses polling against API endpoints. SSE/WebSocket can be added later from the API if necessary.

RabbitMQ vs gRPC:
- RabbitMQ/outbox is mandatory for critical business flows: payments, NF-e/NFC-e, transactional emails, reports, reconciliation, webhook handling, fulfillment, abuse events, retries, and DLQ.
- Do not use gRPC in v1 for payment creation, fiscal issuance, email sending, report generation, or any provider-dependent workflow. These flows require durability, idempotency, backoff, retry, and DLQ.
- gRPC is optional later only for an internal control plane: worker health details, queue drain, worker pause/resume, admin diagnostics, or capability introspection. If added, it must be internal-only, mTLS-protected, authenticated, authorized, audited, and never on the customer checkout critical path.
- If future UX requires synchronous payment-session creation, first prefer provider-hosted/tokenized flows or API-created pending order plus polling. Add gRPC only after an ADR explains why durable queue-based flow is insufficient.

## 3. Required Engine Responsibilities

Implement workers and schedulers for:
- Outbox publishing to RabbitMQ if the engine owns the outbox publisher deployment.
- Payments: create/confirm payment, capture, cancel, refund, reconcile, query provider status.
- Payment webhooks: consume validated webhook events, deduplicate, map provider status to canonical status, update payments/orders.
- Getnet/Santander payment adapter as the first real provider.
- Future payment provider adapters: Mercado Pago, Sicoob, Stripe, Pagar.me, etc.
- Transactional email sending.
- NF-e/NFC-e issuance after payment confirmation and before fulfillment.
- DANFE/PDF generation and secure XML/PDF storage.
- Fiscal retry/cancel/event handling.
- Order fulfillment commands and status transitions.
- Admin report/export generation.
- LGPD privacy export/deletion/anonymization support when the API publishes authorized privacy requests.
- Scheduled jobs: payment reconciliation, stale pending payments, abandoned cart events, token/idempotency cleanup, fiscal close, low-stock alerts.
- DLQ inspection/retry/replay support with audit logs.
- Abuse/security event processing if API publishes security events.

## 4. Suggested Go Project Structure

```text
cmd/engine
internal/config
internal/db
internal/queue
internal/outbox
internal/workers
internal/scheduler
internal/payments
internal/payments/providers/getnet
internal/payments/providers/fake
internal/fiscal
internal/fiscal/providers/sefaz
internal/fiscal/providers/fake
internal/email
internal/reports
internal/privacy
internal/fulfillment
internal/abuse
internal/storage
internal/observability
internal/security
```

The Engine may have its own repository/project, but it must treat the API database schema and RabbitMQ message schemas as versioned contracts.

## 5. Message Contract

Every RabbitMQ message must contain:

```json
{
  "message_id": "uuid",
  "correlation_id": "uuid",
  "causation_id": "uuid-or-null",
  "event_type": "payment.create.requested",
  "aggregate_type": "order",
  "aggregate_id": "uuid",
  "schema_version": 1,
  "occurred_at": "2026-05-02T12:00:00Z",
  "payload": {}
}
```

Rules:
- Messages contain ids and minimal payloads.
- Do not include secrets, card data, raw provider credentials, reset tokens, or unnecessary PII.
- Include immutable snapshots only when required for consistency, for example order item/payment amount snapshot.
- Consumers are idempotent by `message_id` plus business keys.
- Message schemas are versioned. Additive changes are preferred.
- Breaking changes require migration and dual-read/dual-write period.

Core command/event types:
- `payment.create.requested`
- `payment.confirm.requested`
- `payment.capture.requested`
- `payment.refund.requested`
- `payment.cancel.requested`
- `payment.reconcile.requested`
- `payment.webhook.received`
- `payment.authorized`
- `payment.captured`
- `payment.failed`
- `payment.refunded`
- `payment.chargeback_opened`
- `fiscal.issue.requested`
- `fiscal.retry.requested`
- `fiscal.cancel.requested`
- `fiscal.authorized`
- `fiscal.rejected`
- `email.send.requested`
- `email.sent`
- `email.failed`
- `report.export.requested`
- `report.export.completed`
- `privacy.export.requested`
- `privacy.export.completed`
- `privacy.delete.requested`
- `privacy.delete.completed`
- `order.fulfillment.requested`
- `security.abuse.detected`

Suggested queues:
- `payments.create`
- `payments.confirm`
- `payments.capture`
- `payments.refund`
- `payments.reconcile`
- `payments.webhook`
- `fiscal.issue`
- `fiscal.retry`
- `fiscal.cancel`
- `emails.transactional`
- `reports.export`
- `privacy.requests`
- `orders.fulfillment`
- `abuse.security-events`
- DLQ for every queue.

## 6. Worker Guarantees

- Assume at-least-once delivery.
- Never assume exactly-once processing.
- Every worker must be idempotent and safe to retry.
- Use Postgres transactions for state transitions.
- Use row locks or advisory locks where necessary.
- Use provider idempotency keys when supported, in addition to internal idempotency.
- Use exponential backoff with jitter.
- Use finite retry attempts, then DLQ.
- Use circuit breakers/timeouts for external providers.
- Acknowledge RabbitMQ messages only after durable result persistence.
- Graceful shutdown must stop accepting new work, finish/extend in-flight work safely, then close connections.

## 7. Payments

Payment provider architecture:
- Define a canonical `PaymentProvider` interface in the engine.
- No order/payment worker should depend on Getnet-specific DTOs outside the Getnet adapter.
- Getnet/Santander is the first real adapter.
- Fake provider is required for local/dev/integration tests.
- Future providers must be added as isolated adapters without changing business services.

Expected interface:

```go
type PaymentProvider interface {
    Name() string
    Capabilities() PaymentCapabilities
    CreatePayment(ctx context.Context, req CreatePaymentRequest) (CreatePaymentResult, error)
    ConfirmPayment(ctx context.Context, req ConfirmPaymentRequest) (ConfirmPaymentResult, error)
    CapturePayment(ctx context.Context, req CapturePaymentRequest) (CapturePaymentResult, error)
    CancelPayment(ctx context.Context, req CancelPaymentRequest) (CancelPaymentResult, error)
    RefundPayment(ctx context.Context, req RefundPaymentRequest) (RefundPaymentResult, error)
    GetPaymentStatus(ctx context.Context, providerPaymentID string) (ProviderPaymentStatus, error)
    VerifyWebhook(ctx context.Context, headers map[string]string, rawBody []byte) error
    ParseWebhook(ctx context.Context, headers map[string]string, rawBody []byte) (ProviderWebhookEvent, error)
}
```

Getnet/Santander adapter:
- Use current official Getnet documentation at implementation time.
- Support sandbox/production credentials through env vars or secret manager.
- Never hardcode secrets.
- Isolate OAuth/access-token handling with secure cache, expiration, renewal, and failure handling.
- Use Getnet idempotency when available.
- Map Getnet statuses/errors into canonical internal statuses/errors.
- Never expose raw Getnet errors or sensitive payloads to customers.
- Store provider ids and redacted snapshots only.

Canonical statuses:
- `requires_action`
- `authorized`
- `captured`
- `paid`
- `failed`
- `cancelled`
- `refunded`
- `chargeback`
- `unknown`

Payment worker behavior:
- Load payment/order snapshot from Postgres for `payment.create.requested`, `payment.confirm.requested`, capture, cancel, refund, and reconciliation commands.
- Validate amount/currency against stored order/payment rows, not the message payload.
- Call provider.
- Persist `payment_attempts`.
- Update `payments` and `orders` in a transaction.
- Publish follow-up events: email, fiscal issuance, abuse alert if needed. Publish fulfillment only when fiscal/legal preconditions are satisfied.
- Never confirm/capture/refund/cancel a payment twice. Use internal idempotency keys plus provider idempotency keys/correlation ids when supported.

## 8. Webhooks

Preferred flow:
- Public webhook ingress remains in API or a dedicated ingress component.
- API stores raw webhook metadata safely in `webhook_ingress_events`, validates basic request constraints, and publishes `payment.webhook.received`.
- Engine performs provider-specific verification/parsing if raw body and trusted headers are available, or consumes an already-verified event if API owns verification.

Security rules:
- Verify provider signature/authenticity before state mutation.
- Unverified webhook ingress can only be marked `received` or `rejected`; it must never update `payments`, `orders`, inventory, fiscal, loyalty, or fulfillment state.
- Deduplicate by `(provider, provider_event_id)`.
- Reject oversized payloads.
- Redact or hash payloads before persistence/logging.
- Handle duplicate and out-of-order events.
- Query provider status when event order is ambiguous.
- Persist verification status and rejection reasons without exposing raw provider secrets or sensitive payloads.

## 9. Fiscal / NF-e / NFC-e

The engine owns fiscal issuance.

Rules:
- Validate final tax rules with accountant/tax specialist before production.
- Prepare NF-e model 55 for physical product e-commerce.
- Evaluate NFC-e model 65 only when operation/UF/business rules justify it.
- Use a `FiscalProvider` interface for SEFAZ direct integration or an approved fiscal provider.
- Store A1 certificate/fiscal credentials only in secret manager/KMS.
- Generate XML according to current official layout.
- Digitally sign XML.
- Submit for authorization.
- Store access key, protocol, authorized XML, DANFE/PDF, rejection info, and events.
- If NF-e is rejected, block fulfillment until corrected/retried or manually handled with audit.
- Fiscal operations must be idempotent by order_id/model/series/number.

Fiscal events:
- authorization;
- rejection;
- cancellation;
- carta de correção when applicable;
- inutilização de numeração;
- contingency when required.

## 10. Email

Transactional email responsibilities:
- Order confirmation.
- Payment approved/failed.
- Order shipped/delivered.
- Password reset.
- Email changed.
- NF-e authorized with DANFE/XML link.
- Admin export ready.
- Security alert when relevant.

Rules:
- Never put secrets in email logs.
- Store recipient as hash or redacted where possible.
- Respect newsletter/marketing consent separately from transactional emails.
- Track delivery status and retry with backoff.

## 11. Reports and Admin Exports

- Admin exports must be async.
- Scope exports by admin permission and filters.
- Update `report_exports` with `queued`, `running`, `completed`, `failed`, and expiration metadata.
- Generate signed/expiring download links.
- Audit who requested/exported what.
- Redact sensitive data by default.
- Large exports must stream to storage, not memory.

## 12. Fulfillment

- Fulfillment workers consume `order.fulfillment.requested` only after payment and fiscal preconditions are satisfied.
- Create/update `shipments` and `shipment_events` with carrier/tracking data when available.
- Fulfillment must be idempotent by order/shipment/business key.
- If NF-e is pending or rejected, fulfillment must remain blocked unless an audited manual exception is explicitly allowed.
- Shipping/customer-facing tracking state is exposed by the API from persisted shipment rows; the engine does not call the frontend.

## 13. LGPD Privacy Requests

- Consume `privacy.export.requested` and `privacy.delete.requested` only after the API has authenticated/authorized the requester and created a `privacy_requests` row.
- Exports must be scoped to the requester, encrypted at rest, audited, and expire.
- Deletion/anonymization must preserve fiscal/accounting/legal records that cannot be erased, while preventing non-required usage.
- Never include secrets, password hashes, card data, provider credentials, raw tokens, or unrelated users' data in exports.
- Update `privacy_requests` with status, completion time, and safe result reference.

## 14. Observability

Required:
- Structured JSON logs with `correlation_id`, `message_id`, `worker`, `event_type`, `aggregate_id`, `attempt`, `latency_ms`, and redacted metadata.
- Prometheus metrics: queue lag, processed count, failure count, DLQ count, retry count, provider latency/errors, fiscal authorization/rejection, email sent/failed, outbox pending.
- Health endpoint `/healthz`.
- Readiness endpoint `/readyz` validating Postgres, RabbitMQ, Redis when used, and provider dependencies in degraded mode.
- Optional OpenTelemetry tracing.

## 15. Security Requirements

- No PAN/CVV ever.
- No secrets in logs, metrics, traces, DB snapshots, RabbitMQ messages, or DLQ payloads.
- Provider credentials through env/secret manager only.
- mTLS if gRPC is ever added.
- Strict outbound allowlist for provider domains to reduce SSRF risk.
- Timeouts and body size limits for all provider calls.
- Encrypt fiscal XML/DANFE and sensitive exports at rest.
- Audit all manual replay, DLQ retry, refund, fiscal cancellation, report generation, and admin-triggered operations.
- Run `govulncheck` and dependency scanning.

## 16. Tests

Required tests:
- Unit tests for payment/fiscal/email services.
- Fake payment provider success/failure/requires-action/confirm/refund/cancel/chargeback.
- Fake fiscal provider authorized/rejected/retry/cancel.
- Webhook duplicate/invalid/out-of-order handling, including "invalid signature causes no payment/order mutation".
- Worker idempotency under repeated message delivery.
- RabbitMQ integration tests when available.
- Postgres transaction tests for payment/order/fiscal/fulfillment transitions.
- Report export and privacy request status-transition tests.
- DLQ and retry tests.
- Graceful shutdown tests.

## 17. Acceptance Criteria

- Engine runs locally with Postgres, RabbitMQ, and Redis when needed.
- Engine can consume all documented queues.
- Engine can publish follow-up events.
- Engine updates Postgres result tables consumed by the API.
- Fake payment provider enables full local checkout flow without real Getnet credentials.
- Engine handles `payment.create.requested` and `payment.confirm.requested` without duplicating provider operations.
- Getnet adapter is isolated and ready for sandbox/production credentials.
- Fake fiscal provider enables local NF-e authorized/rejected flows.
- Fulfillment remains blocked when fiscal issuance is pending/rejected and unblocks only after valid authorization or audited exception.
- Invalid/forged webhook ingress is rejected and does not mutate business state.
- Report exports and LGPD privacy exports are async, encrypted, audited, and expiring.
- Email worker sends through fake provider locally.
- Retries, DLQ, idempotency, and audit logs work.
- No secrets or sensitive PII leak in logs/messages.
- README documents how API and Engine communicate.
- Message schema/versioning is documented.

## 18. Official References

- API prompt: `docs/backend-implementation-prompt.md`
- OWASP API Security Top 10 2023: https://owasp.org/API-Security/editions/2023/en/0x11-t10/
- OWASP REST Security Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/REST_Security_Cheat_Sheet.html
- Getnet official docs: https://docs.globalgetnet.com/en
- Portal Nacional NF-e: https://www.nfe.fazenda.gov.br/portal/principal.aspx
- NF-e/NFC-e manuals in Portal Nacional: https://www.nfe.fazenda.gov.br/portal/listaConteudo.aspx?tipoConteudo=ndIjl+iEFdE%3D
