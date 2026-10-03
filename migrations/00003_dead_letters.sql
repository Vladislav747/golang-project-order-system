-- +goose Up
CREATE TABLE IF NOT EXISTS dead_letters (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source VARCHAR(32) NOT NULL CHECK (source IN ('outbox_relay', 'kafka_consumer')),
    -- outbox.payload(outbox_relay) or kafka_consumer.value
    payload JSONB NOT NULL,
    -- Это топик, откуда событие пришло или куда оно должно было уйти. Для outbox_relay сюда копируется outbox.topic (сейчас orders.events). Для kafka_consumer — топик, из которого consumer прочитал сообщение.
    original_topic VARCHAR(100) NOT NULL,
    -- Это id события в outbox. Для outbox_relay сюда копируется outbox.id. Для kafka_consumer — NULL.
    original_outbox_id UUID,
    -- Сюда копируется outbox.aggregate_type. Сейчас это всегда order: тип сущности, к которой относится событие. Для kafka_consumer колонка остаётся NULL
    aggregate_type VARCHAR(50),
    -- Сюда копируется outbox.aggregate_id: id конкретной сущности, сейчас это order.id.
    aggregate_id UUID,
    -- Сюда копируется outbox.event_type: order.created, order.updated или order.deleted. Для kafka_consumer колонка остаётся NULL.
    event_type VARCHAR(50),
    original_partition INT,
    -- Это позиция сообщения внутри партиции. Тип BIGINT, потому что offset в Kafka — 64-битное число, INT для него мал. Для outbox_relay колонка остаётся NULL.
    original_offset BIGINT,
    -- Сюда пишется текст ошибки, из-за которой строка попала в карантин. Для обеих сторон поле обязательное: в админке должно быть видно, почему событие умерло. Тип TEXT, потому что длина сообщения об ошибке заранее неизвестна.
    error_text TEXT NOT NULL,
    -- retryable — сеть, Postgres, таймаут: сообщение ретраили, попытки кончились, тогда ушло в карантин. non_retryable — битый JSON, неизвестное действие, повторный create с тем же id: в DLQ сразу, без ретраев.
    error_class VARCHAR(32) NOT NULL CHECK (error_class IN ('retryable', 'non_retryable')),

    attempts INT NOT NULL, -- сколько раз событие пытались обработать до карантина: publish у outbox_relay, in-process ретраи у kafka_consumer
    first_failed_at TIMESTAMPTZ NOT NULL, -- Это момент первой ошибки, не момент записи строки в карантин. 
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), -- Это момент записи строки в карантин. 
    retried_at TIMESTAMPTZ, -- ставит админка в момент POST /admin/dlq/{id}/retry: строку вернули из карантина в работу. Пока NULL, событие ещё лежит в DLQ и попадает в список «ещё не возвращённые».
    retry_error TEXT -- ошибка именно этого возврата, если он не удался. Исходная причина смерти по-прежнему в error_text. Если redrive прошёл, retry_error остаётся NULL.


);

-- Карантин, который ещё не возвращали в работу. Свежие сверху.
CREATE INDEX IF NOT EXISTS idx_dead_letters_pending
    ON dead_letters (created_at DESC)
    WHERE retried_at IS NULL;

-- Одна строка outbox не попадает в DLQ дважды. Только source = outbox_relay.
CREATE UNIQUE INDEX IF NOT EXISTS idx_dead_letters_outbox_origin
    ON dead_letters (original_outbox_id)
    WHERE source = 'outbox_relay' AND original_outbox_id IS NOT NULL;


-- Один и тот же (topic, partition, offset) не пишется в карантин повторно. Только source = kafka_consumer.
CREATE UNIQUE INDEX IF NOT EXISTS idx_dead_letters_kafka_origin
    ON dead_letters (original_topic, original_partition, original_offset)
    WHERE source = 'kafka_consumer'
      AND original_partition IS NOT NULL
      AND original_offset IS NOT NULL;


-- +goose Down
DROP INDEX IF EXISTS idx_dead_letters_pending;
DROP INDEX IF EXISTS idx_dead_letters_outbox_origin;
DROP INDEX IF EXISTS idx_dead_letters_kafka_origin;
DROP TABLE IF EXISTS dead_letters;
