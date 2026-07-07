-- +goose Up
CREATE TABLE IF NOT EXISTS orders (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id   UUID        NOT NULL,
    status        VARCHAR(50) NOT NULL,
    total_amount  BIGINT      NOT NULL CHECK (total_amount >= 0),
    currency      VARCHAR(3)  NOT NULL,
    items         JSONB       NOT NULL DEFAULT '[]'::jsonb,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at    TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS order_events (
    id          UUID   PRIMARY KEY,
    order_id    UUID        NOT NULL REFERENCES orders (id),
    event_type  VARCHAR(20) NOT NULL CHECK (event_type IN ('created', 'updated', 'viewed', 'deleted')),
    source      VARCHAR(20) NOT NULL CHECK (source IN ('http_sync', 'kafka')),
    payload     JSONB       NOT NULL DEFAULT '{}'::jsonb,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_orders_status ON orders (status) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_order_events_order_id ON order_events (order_id);

-- +goose Down
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS order_events;
DROP INDEX IF EXISTS idx_orders_status;
DROP INDEX IF EXISTS idx_orders_created_at;
DROP INDEX IF EXISTS idx_order_events_order_id;