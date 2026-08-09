-- +goose Up
CREATE TABLE IF NOT EXISTS outbox (
     id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- для маршрутизации / идемпотентности
    aggregate_type  VARCHAR(50)  NOT NULL,  -- 'order'
    aggregate_id    UUID         NOT NULL,  -- order.id
    event_type      VARCHAR(50)  NOT NULL,  -- 'order.created' | 'order.updated' | 'order.deleted'
    -- тело события для Kafka (JSON)
    payload         JSONB        NOT NULL,
    -- опционально: явный топик, если не выводите из event_type
    topic           VARCHAR(100) NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    published_at    TIMESTAMPTZ,            -- NULL = ещё не опубликовано
    -- счётчик неудачных попыток publish в Kafka.
    attempts        INT          NOT NULL DEFAULT 0,
    last_error      TEXT
);

-- relay: брать неопубликованные по порядку
CREATE INDEX IF NOT EXISTS idx_outbox_unpublished
    ON outbox (created_at)
    WHERE published_at IS NULL;

-- +goose Down
DROP INDEX IF EXISTS idx_outbox_unpublished;
DROP TABLE IF EXISTS outbox;
