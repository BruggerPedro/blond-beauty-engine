-- Engine-operational schema. Owned by the Engine, NOT by the API.
--
-- Why this lives here: the Engine needs a place to record which RabbitMQ
-- messages it has already processed so workers can be at-least-once safe and
-- duplicate-suppress. This concern is internal to the Engine's runtime and
-- has no analogue in the API domain. Keep all engine-operational tables under
-- a clear `engine_*` prefix to keep the API/Engine schema boundary visible.
--
-- The API domain schema (orders, payments, fiscal_invoices, outbox_messages,
-- ...) is owned and migrated by the API project and is treated by the
-- Engine as a versioned contract.

CREATE TABLE IF NOT EXISTS engine_processed_messages (
    queue       text        NOT NULL,
    message_id  text        NOT NULL,
    status      text        NOT NULL CHECK (status IN ('in_progress', 'done')),
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (queue, message_id)
);

CREATE INDEX IF NOT EXISTS engine_processed_messages_created_at_idx
    ON engine_processed_messages (created_at);
