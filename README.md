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