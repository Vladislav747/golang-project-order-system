# Order System

Сервис управления заказами, написанный на Go.

ТЗ: Сервис заказов (Go + PostgreSQL + Kafka)
Суть. Backend для заказов с двумя режимами создания — синхронно (HTTP→БД) и асинхронно (через Kafka). Плюс аудит-лог всех событий, где изменение заказа и запись лога идут в одной транзакции.
Слои:

domain — сущности (Order, OrderEvent) и интерфейсы, без зависимостей на БД/Kafka/HTTP.
repository/postgres — только SQL, методы принимают tx снаружи(открывает и закрывает транзакцию снаружи).
service — бизнес-логика и управление транзакцией (BEGIN/COMMIT/ROLLBACK здесь, не в repo).
transport/http + transport/kafka — хендлеры, DTO, продьюсер, консьюмер. DTO ≠ domain.
Плюс config, logger, cmd/order-service/main.go, migrations/, docker-compose.yml.

Сущности:

orders: id(uuid), customer_id, status, total_amount, currency, items(jsonb), created/updated/deleted_at (мягкое удаление).
order_events: id, order_id, event_type(created/updated/viewed/deleted), source(http_sync/kafka), payload(jsonb), created_at.

Фичи:

Синхронное создание: POST /orders → в одной tx вставка заказа + лог created.
Асинхронное: POST /orders/async → только publish в Kafka, ответ 202; консьюмер создаёт заказ + лог в одной tx.
CRUD: GET /orders/{id} (лог viewed), GET /orders (пагинация + фильтр по статусу), PATCH (updated), DELETE (мягкое, deleted).

Особенности (на чём ловить понимание):

Транзакция в сервисе, не в репозитории.
Идемпотентность консьюмера: order_id с продьюсера, INSERT ... ON CONFLICT DO NOTHING — повтор сообщения не плодит дубль.
Offset коммитится после успешной обработки.
context сквозь всю цепочку, graceful shutdown, pgxpool, миграции (goose/migrate).

Инфра: docker-compose up поднимает Postgres + Kafka + сервис. Конфиг через env/yaml. Минимум тестов: unit на сервис + 1 integration через testcontainers.
Бонус: transactional outbox, DLQ, метрики.



```go
go run ./cmd/order-service/main.go
go build ./cmd/order-service/main.go
```

Что по следующим шагам

### Уже есть

### Приоритет 1 — ядро по ТЗ (без этого «не закрыто»)

1. **order_events + одна транзакция**
   - `repository.InsertOrderEvent(ctx, tx, event)`
   - В каждой write-операции service в **одной tx**: заказ + событие
   - `created` + `source=http_sync` при `CreateOrder`
   - `created` + `source=kafka` при `CreateOrderFromKafka`
   - `viewed` при `GetOrder`, `updated` при `UpdateOrder`, `deleted` при `DeleteOrder`

2. **Мягкое удаление**
   - `DELETE` → `UPDATE orders SET deleted_at = NOW()` (не hard delete)
   - `GetOrders` / `GetOrder` — не показывать удалённые (`WHERE deleted_at IS NULL`)

3. **Идемпотентность Kafka consumer**
   - `order_id` генерируется в `CreateOrderKafka` **до** publish (уже частично есть)
   - `INSERT INTO orders ... ON CONFLICT (id) DO NOTHING` в repository
   - commit offset только после успеха (уже есть)

4. **Graceful shutdown полностью**
   - `cancel()` для consumer context
   - `consumer.Close()`, `producer.Close()` в shutdown
   - `pool.Close()` — один раз, без дублей

### Приоритет 2 — API по ТЗ

5. **GET /orders: пагинация + фильтр**
   - query: `?status=pending&limit=20&offset=0`
   - repository + handler

6. **DTO (transport ≠ domain)**
   - `internal/transport/http/dto` — request/response для HTTP (`CreateOrderRequest`, `OrderResponse`)
   - handler парсит DTO → маппит в `model.Order`
   - `kafka.CreateOrderMessage` уже DTO для Kafka — оставить отдельно от domain
   - Не обязательно переименовывать `model/` в `domain/` сразу, но разделить transport и domain модели

7. **Ответы API**
   - `POST /order` → JSON `{"id":"..."}`, не голая строка UUID
   - Единый формат ошибок

### Приоритет 3 — инфра и качество

8. **service_test** — доработать `service_test.go` (убрать `t.Skip`, mock repo + mock producer)

9. **Миграции** — goose или golang-migrate вместо только `init.sql`

10. **Integration test** — 1 тест через testcontainers (Postgres + Kafka)

11. **Чистка**
    - Убрать `fmt.Println` из repository
    - `.gitignore`: `.DS_Store`
    - `broker`: `KAFKA_LISTENERS` + `KAFKA_INTER_BROKER_LISTENER_NAME`

### Бонус (после основного)
- transactional outbox
- DLQ для poison messages в Kafka
- Kafka-метрики
- Grafana dashboard as code

### Рекомендуемый порядок
```
order_events → soft delete → идемпотентность → graceful shutdown
→ пагинация/фильтр → DTO → service_test → миграции → integration test
```
