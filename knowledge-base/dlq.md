# Dead Letter Queue (DLQ)

Источник правды по дизайну: корневой `PLAN.md`. Здесь — сжатый статус для агентов.

## Зачем

Два пути карантина:

| Источник | Когда | Куда |
|----------|--------|------|
| `outbox_relay` | publish из outbox исчерпал attempts | строка в `dead_letters` |
| `kafka_consumer` | handler/unmarshal не смог обработать сообщение | топик `orders.dlq` **и** строка в `dead_letters` |

Consumer: **MarkMessage только после успеха handler или успешного Send в DLQ**. Никаких `continue` на ошибке — иначе offset проезжает мимо.

Порядок consumer sink: **Kafka DLQ → INSERT dead_letters**. Ошибка любого шага → без Mark.

## Сделано

- Phase A/B: миграция, модели, outbox backoff, relay → DLQ, cleaner только retention, тесты relay
- Phase C (почти):
  - producer Key/Headers
  - `internal/transport/kafka/dlq.go` — `DeadLetterRecord`, `FailedMessageHandler`
  - `internal/dlq/sink.go` — Send (Kafka + DB, truncate error 2048 байт)
  - `consumer.go` — `processMessage`, retries, classify, sendToDLQ
  - integration `order_async_test` + топик `orders.dlq` заведены под новую сигнатуру `NewConsumer`

## Не сделано / дыры

1. **Блокер:** `mustStartConsumer` — `dlq.NewSink` + `cfg.DLQ.*` + `pool`/`producer` (order-service не собирается).
2. Phase C хвост: юнит `processMessage`, integration «битый JSON → DLQ».
3. Phase D опционально: батч `POST /admin/dlq/retry`, тесты handler; перенос `transport/dlq` → `handler/dlq`.
4. Integration Postgres тянет только `00001_init.sql` — нет `dead_letters` на DLQ-пути.

## Ключевые файлы

- `internal/dlq/sink.go`
- `internal/transport/kafka/{consumer,dlq,classify,message,producer}.go`
- `internal/repository/deadletter/`
- `internal/worker/outbox.go` (+ backoff)
- `config/*.yaml` → `dlq.topic`, `consumer_max_retries`, `consumer_retry_backoff`
- `cmd/order-service/main.go` — wiring consumer (TODO)
- `cmd/outbox-worker/main.go` — relay уже с deadletter repo

## Конвенции кода

- Импорты gci: stdlib → third-party → `github.com/Vladislav747/golang-project-order-system/...`
- Пользователь пишет основной код сам; агент направляет и правит точечно по просьбе.
- Ответы агента — на русском.
- Коммиты — только по явной просьбе пользователя.
