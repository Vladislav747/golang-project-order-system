# PLAN: Dead Letter Queue (DLQ)

Цель: сообщения, которые не удалось обработать, не исчезают и не крутятся вечно. После N попыток они уезжают в карантин вместе с причиной. Человек смотрит `GET /admin/dlq` и возвращает их в обработку через redrive.

Две независимые точки отказа — два независимых DLQ. Их нельзя смешивать в одной «очереди», но админка смотрит в одну таблицу.

| Сторона | Где ломается сейчас | Куда кладём DLQ | Как redrive |
|---|---|---|---|
| Producer / outbox relay | Kafka не приняла событие из `outbox` | таблица `dead_letters` (source=`outbox_relay`) | INSERT обратно в `outbox` |
| Consumer | прочитали из Kafka, handler упал | топик `orders.dlq` **и** строка в `dead_letters` (source=`kafka_consumer`) | publish обратно в исходный топик |

---

## 0. Что сломано прямо сейчас

### 0.1. Cleaner тихо убивает бизнес-события

Файл: `internal/repository/outbox/repository.go` → `CleanOutboxMessages`

```sql
DELETE FROM outbox
WHERE (published_at IS NOT NULL AND published_at < NOW() - $1::interval)
   OR (published_at IS NULL AND attempts >= $2)
```

Второе условие — дырка. Relay инкрементит `attempts` / `last_error`, а cleaner при `attempts >= max_attempts` удаляет строку. Событие не уехало в Kafka и никто об этом не узнает.

Вызов: `internal/worker/outbox_cleaner.go` → `clean()` передаёт `maxAttempts` из конфига.

Комментарий в `config/local.yaml` тоже врёт: «очистка очереди от не опубликованных сообщений». На деле retention должен чистить **только успешно опубликованные**.

### 0.2. Consumer коммитит offset мимо упавшего сообщения

Файл: `internal/transport/kafka/consumer.go` → `ConsumeClaim`

Комментарии говорят: «offset не коммитим — сообщение будет прочитано снова». Это неправда.

Почему:

1. Цикл `for msg := range claim.Messages()` **не перечитывает** текущее сообщение. `continue` берёт **следующее**.
2. Kafka хранит один committed offset на пару (group, partition) — это watermark, не набор «обработанных id».
3. `session.MarkMessage(msg, "")` помечает offset = `msg.Offset+1`. Следующее успешное сообщение двигает watermark вперёд.
4. Sarama по умолчанию `Consumer.Offsets.AutoCommit.Enable = true` и периодически коммитит максимальный marked offset партиции.

Итог: упавшее сообщение N, успешное N+1 → committed offset оказывается за N → N потеряно.

Плюс ядовитое сообщение (битый JSON, unknown action) даже при честном ретрае будет падать вечно и блокировать партицию. Значит после классификации ошибок: non-retryable сразу в DLQ, retryable — M раз, потом DLQ, потом MarkMessage.

---

## 1. Решение по хранению (обязательно прочитать до кода)

### 1.1. Не статус в `outbox`, а отдельная таблица `dead_letters`

Пользователь разрешил выбрать. Выбор: **новая таблица `dead_letters`**, не колонка `status` в `outbox`.

Почему не статус в `outbox`:

- У живой очереди и кладбища разный lifecycle. Outbox — горячий путь relay (`SELECT … LIMIT`, индекс по unpublished). DLQ — холодный путь админки. Смешивать значит: каждый SELECT relay обязан фильтровать `status`, cleaner обязан не трогать dead, индекс усложняется.
- Consumer-side DLQ — это Kafka-сообщение, у него нет строки outbox. Статус в outbox покрывает только producer-side. Для админки всё равно нужна вторая сущность.
- Redrive из отдельной таблицы = INSERT в `outbox` / publish в Kafka. Redrive через статус = UPDATE. Первое явно: запись покинула карантин и заново встала в очередь. Второе легко сломать (забыли сбросить `attempts` / `next_retry_at`).
- Extra-поля DLQ (`source`, `original_partition`, `original_offset`, `first_failed_at`, `retried_at`, `error_class`) не должны торчать в outbox.

Почему одна таблица на обе стороны, а не `outbox_dead_letters` + отдельный consumer-store:

- `GET /admin/dlq` и `POST /admin/dlq/{id}/retry` требуют стабильный `id`. Kafka-топик один это плохо умеет (нет пагинации, нет id, нет фильтра по ошибке).
- Колонка `source` разделяет redrive-логику. Админка одна.

Kafka-топик `orders.dlq` при этом **обязателен** на consumer-side: это классический стриминговый карантин (алерты, отдельный consumer, replay без хождения в БД). Таблица — source of truth для HTTP-админки.

### 1.2. Dual-write на consumer-side (Kafka DLQ + Postgres)

Порядок в одной обработке сообщения:

1. Решаем, что сообщение мертвое (non-retryable или retryable исчерпаны).
2. Публикуем в `orders.dlq` **с headers**. Если Kafka DLQ не приняла — **не** MarkMessage, **не** `continue`. Ре траим публикацию в DLQ или возвращаем ошибку из `ConsumeClaim` (см. §4.4).
3. INSERT в `dead_letters`. Идемпотентность: unique `(original_topic, original_partition, original_offset)` для `source='kafka_consumer'`. Повторная обработка того же offset после рестарта не плодит дубли в админке (`ON CONFLICT DO NOTHING`).
4. Только после 2+3 — `session.MarkMessage`.

Дубль в Kafka-топике DLQ при падении между шагами 2 и 3 допустим (at-least-once). Админка на unique не раздуется.

### 1.3. Что не делаем в этой задаче

- Auth на `/admin/*` — не блокирует реализацию, помечаем TODO в хендлере.
- Отдельный DLQ-cleaner по retention для `dead_letters`. Карантин не чистим автоматически: это и есть смысл DLQ. Если очень надо — отдельная задача.
- Idempotent handlers на 100%. Redrive create с тем же UUID упрётся в PK — это non-retryable, и это ок.
- Менять claim-модель outbox на `SELECT FOR UPDATE SKIP LOCKED` внутри той же транзакции, что publish. Сейчас select → kafka → update в другой tx. Не раздуваем скоуп. Можно упомянуть как follow-up.

---

## 2. Конфиг

### 2.1. YAML (`config/local.yaml`, `config/prod.yaml`)

Текущий `outbox`:

```yaml
outbox:
  relay_interval: 2s
  limit: 200
  cleanup_interval: 1h
  published_retention: 24h
  max_attempts: 3
```

Добавить:

```yaml
outbox:
  relay_interval: 2s
  limit: 200
  cleanup_interval: 1h
  published_retention: 24h
  max_attempts: 3
  backoff_initial: 1s    # 1s → 2s → 4s → …
  backoff_max: 8s        # потолок экспоненты

dlq:
  topic: orders.dlq
  consumer_max_retries: 3
  consumer_retry_backoff: 200ms
```

Смысл `backoff_*` у outbox: если Kafka легла на 30 секунд, relay с интервалом 2s не должен сжечь 3 попытки и уехать в DLQ. Попытка засчитывается, но следующая берётся только когда `next_retry_at <= NOW()`.

`consumer_max_retries` — in-process ретраи **одного** Kafka-сообщения до DLQ. Это не outbox `max_attempts`. Разные числа, разные оси.

Исправить комментарий у `cleanup_interval`: чистим только опубликованные по `published_retention`. Неопубликованные cleaner больше не трогает.

### 2.2. Struct'ы

`internal/config/config.go`:

```go
type Outbox struct {
    RelayInterval      time.Duration `yaml:"relay_interval"`
    Limit              int           `yaml:"limit"`
    CleanupInterval    time.Duration `yaml:"cleanup_interval"`
    PublishedRetention time.Duration `yaml:"published_retention"`
    MaxAttempts        int           `yaml:"max_attempts"`
    BackoffInitial     time.Duration `yaml:"backoff_initial"`
    BackoffMax         time.Duration `yaml:"backoff_max"`
}

type DLQ struct {
    Topic                string        `yaml:"topic"`
    ConsumerMaxRetries   int           `yaml:"consumer_max_retries"`
    ConsumerRetryBackoff time.Duration `yaml:"consumer_retry_backoff"`
}
```

Поле `DLQ` в корневой `Config`.

Hot-reload в `internal/config/watch.go` → `configChanges` и `onReload` в `cmd/outbox-worker/main.go`:

- `relay.SetBackoffInitial` / `SetBackoffMax`
- cleaner **больше не получает** `maxAttempts` (удалить `SetMaxAttempts` у cleaner и поле `maxAttempts`)

`cmd/order-service/main.go` onReload: consumer retries лучше читать через `provider.Get()` на каждое сообщение, чтобы не плодить сеттеры. Тогда hot-reload dlq-параметров работает без callback.

### 2.3. Env

`internal/config/env.go` — `KafkaConfig`:

```go
type KafkaConfig struct {
    Brokers       []string
    TopicOrders   string
    TopicDLQ      string // fallback, если yaml пустой: KAFKA_TOPIC_DLQ
    ConsumerGroup string
}
```

`.env.example` / `.env.prod.example`:

```
KAFKA_TOPIC_DLQ=orders.dlq
```

`validateEnv`: `KAFKA_TOPIC_DLQ` обязателен **или** берём `cfg.DLQ.Topic` из yaml. Выбрать одно место истины:

**Рекомендация:** топик DLQ как у `KAFKA_TOPIC_ORDERS` — из env (`KAFKA_TOPIC_DLQ`), retries/backoff — из yaml. Тогда `cfg.DLQ.Topic` можно не дублировать, а в yaml оставить только retries. Но пользователь явно сказал топик `orders.dlq`. Практично: yaml `dlq.topic` + env перекрывает, как удобно. Минимум сюрпризов: **yaml `dlq.topic` + validate непустой**. Env `KAFKA_TOPIC_DLQ` опционально перекрывает (по аналогии с портами).

### 2.4. Docker / интеграционные топики

Kafka в `docker-compose.yml` топики сама не создаёт (auto-create обычно включён). Для e2e/integration: `tests/integration/kafka.go` — создать ещё `orders.dlq` рядом с `orders.create`.

---

## 3. Миграция и модели

Файл: `migrations/00003_dead_letters.sql`

### 3.1. `outbox`: колонка backoff

```sql
-- +goose Up
ALTER TABLE outbox
    ADD COLUMN IF NOT EXISTS next_retry_at TIMESTAMPTZ;

-- старый индекс только по created_at WHERE published_at IS NULL
DROP INDEX IF EXISTS idx_outbox_unpublished;

-- relay берёт «пора ретраить», сортировка по created_at как сейчас
CREATE INDEX IF NOT EXISTS idx_outbox_unpublished
    ON outbox (created_at)
    WHERE published_at IS NULL;
```

`NOW()` в partial index нельзя, поэтому фильтр `next_retry_at IS NULL OR next_retry_at <= NOW()` живёт только в SQL SELECT.

Опционально вторым индексом (если таблица вырастет):

```sql
CREATE INDEX IF NOT EXISTS idx_outbox_due
    ON outbox (next_retry_at, created_at)
    WHERE published_at IS NULL;
```

На старте хватит одного.

### 3.2. Таблица `dead_letters`

```sql
CREATE TABLE IF NOT EXISTS dead_letters (
    id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source               VARCHAR(32)  NOT NULL
                         CHECK (source IN ('outbox_relay', 'kafka_consumer')),

    payload              JSONB        NOT NULL,
    original_topic       VARCHAR(100) NOT NULL,

    -- заполняется для source = outbox_relay
    original_outbox_id   UUID,
    aggregate_type       VARCHAR(50),
    aggregate_id         UUID,
    event_type           VARCHAR(50),

    -- заполняется для source = kafka_consumer
    original_partition   INT,
    original_offset      BIGINT,

    error_text           TEXT         NOT NULL,
    error_class          VARCHAR(32)  NOT NULL
                         CHECK (error_class IN ('retryable', 'non_retryable')),
    attempts             INT          NOT NULL,
    first_failed_at      TIMESTAMPTZ  NOT NULL,
    created_at           TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    retried_at           TIMESTAMPTZ,
    retry_error          TEXT
);

CREATE INDEX IF NOT EXISTS idx_dead_letters_pending
    ON dead_letters (created_at DESC)
    WHERE retried_at IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_dead_letters_outbox_origin
    ON dead_letters (original_outbox_id)
    WHERE source = 'outbox_relay' AND original_outbox_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_dead_letters_kafka_origin
    ON dead_letters (original_topic, original_partition, original_offset)
    WHERE source = 'kafka_consumer'
      AND original_partition IS NOT NULL
      AND original_offset IS NOT NULL;

-- +goose Down
DROP INDEX IF EXISTS idx_dead_letters_kafka_origin;
DROP INDEX IF EXISTS idx_dead_letters_outbox_origin;
DROP INDEX IF EXISTS idx_dead_letters_pending;
DROP TABLE IF EXISTS dead_letters;
ALTER TABLE outbox DROP COLUMN IF EXISTS next_retry_at;
CREATE INDEX IF NOT EXISTS idx_outbox_unpublished
    ON outbox (created_at)
    WHERE published_at IS NULL;
```

Redrive **не удаляет** строку. Ставит `retried_at`. История остаётся. Повторный retry уже retried записи — либо 409, либо разрешить повторно (лучше разрешить: база снова упала после первого redrive). Тогда просто обновляем `retried_at` / `retry_error`.

### 3.3. Модели

`internal/model/outbox_message.go` — поле:

```go
NextRetryAt *time.Time `json:"next_retry_at,omitempty"`
```

Новый файл `internal/model/dead_letter.go`:

```go
type DeadLetterSource string
const (
    DeadLetterSourceOutbox   DeadLetterSource = "outbox_relay"
    DeadLetterSourceConsumer DeadLetterSource = "kafka_consumer"
)

type DeadLetterErrorClass string
const (
    ErrorClassRetryable    DeadLetterErrorClass = "retryable"
    ErrorClassNonRetryable DeadLetterErrorClass = "non_retryable"
)

type DeadLetter struct {
    ID                 uuid.UUID
    Source             DeadLetterSource
    Payload            json.RawMessage
    OriginalTopic      string
    OriginalOutboxID   *uuid.UUID
    AggregateType      *AggregateType
    AggregateID        *uuid.UUID
    EventType          *EventTypeOutbox
    OriginalPartition  *int32
    OriginalOffset     *int64
    ErrorText          string
    ErrorClass         DeadLetterErrorClass
    Attempts           int
    FirstFailedAt      time.Time
    CreatedAt          time.Time
    RetriedAt          *time.Time
    RetryError         *string
}
```

Sentinel-ошибки (новый файл `internal/model/errors.go` или рядом):

```go
var (
    ErrNonRetryable = errors.New("non-retryable")
    // retryable — всё, что не обёрнуто в ErrNonRetryable
)

func NonRetryable(err error) error {
    if err == nil {
        return nil
    }
    return fmt.Errorf("%w: %w", ErrNonRetryable, err)
}

func IsNonRetryable(err error) bool {
    return errors.Is(err, ErrNonRetryable)
}
```

Хендлеры/consumer делают `errors.Is(err, model.ErrNonRetryable)` или `model.IsNonRetryable(err)`.

---

## 4. Producer-side DLQ (outbox relay + cleaner)

Затрагиваемые файлы:

- `internal/repository/outbox/repository.go`
- `internal/worker/outbox.go`
- `internal/worker/outbox_cleaner.go`
- `internal/worker/outbox_cleaner_test.go`
- `internal/worker/mocks/*` (перегенерировать mockery)
- `internal/service/mocks/mock_RepositoryOutbox.go` (интерфейс Service тоже поменяется, если трогать сигнатуры)
- `cmd/outbox-worker/main.go`
- новый `internal/repository/deadletter/repository.go`

Service-интерфейс `RepositoryOutbox` в `internal/service/service.go` содержит `Get/MarkPublished/MarkFailed`. Consumer-хендлеры их не вызывают, только `CreateOutboxMessage`. Можно сузить интерфейс сервиса до Create — **не обязательно в этой задаче**. Relay живёт на своём интерфейсе `worker.OutboxRepository`.

### 4.1. SELECT: уважать backoff

`GetOutboxMessagesUnpublished`:

```sql
SELECT *
FROM outbox
WHERE published_at IS NULL
  AND attempts < $2
  AND (next_retry_at IS NULL OR next_retry_at <= NOW())
ORDER BY created_at ASC
LIMIT $1
```

`attempts < max_attempts` остаётся: запись, которая уже выжгла попытки, не должна попадать в relay даже на один тик до переноса в DLQ. Перенос делает сам relay в той же транзакции, что и mark failed (см. ниже), так что «зависших» с `attempts >= max` в норме нет. Если процесс умер между инкрементом и переносом — нужен safety-net (см. §4.3).

### 4.2. Fail path: backoff или DLQ, в одной транзакции

Сигнатуры репозитория outbox:

```go
MarkOutboxMessageFailed(ctx, tx, id, lastError string, nextRetryAt time.Time) error

MoveToDeadLetter(ctx, tx, msg model.OutboxMessage, lastError string, errorClass model.DeadLetterErrorClass) error
```

`MoveToDeadLetter` — **один SQL** (CTE), чтобы не было окна «удалили из outbox / не вставили в DLQ» даже внутри tx:

```sql
WITH moved AS (
    DELETE FROM outbox
    WHERE id = $1
    RETURNING *
)
INSERT INTO dead_letters (
    source, payload, original_topic,
    original_outbox_id, aggregate_type, aggregate_id, event_type,
    error_text, error_class, attempts, first_failed_at
)
SELECT
    'outbox_relay',
    payload,
    topic,
    id,
    aggregate_type,
    aggregate_id,
    event_type,
    $2,             -- last_error
    $3,             -- error_class
    attempts + 1,   -- эта попытка тоже считается
    NOW()
FROM moved;
```

Если `DELETE` ничего не удалил (уже уехало) — INSERT не произойдёт, это ок.

Альтернатива: отдельный `deadletter.Repository.Insert` + `outbox.Delete` в том же `pgx.BeginFunc`. Тоже корректно при одной tx. CTE проще не размазывать по двум репозиториям. Если хочется чистых границ — два вызова в одной tx, это тоже ок. Главное: **не коммитить удаление без insert**.

`MarkOutboxMessageFailed`:

```sql
UPDATE outbox
SET attempts = attempts + 1,
    last_error = $2,
    next_retry_at = $3
WHERE id = $1;
```

Экспонента (считать в worker, не в SQL):

```
attemptsAfter = msg.Attempts + 1
delay = backoffInitial * 2^(attemptsAfter-1)
if delay > backoffMax { delay = backoffMax }
nextRetryAt = now + delay
```

Пример при `initial=1s`, `max=8s`, `max_attempts=3`:

| текущий attempts | после fail | действие |
|---|---|---|
| 0 | 1 | UPDATE, next_retry = now+1s |
| 1 | 2 | UPDATE, next_retry = now+2s |
| 2 | 3 | `3 >= max_attempts` → MoveToDeadLetter |

Kafka легла на 30s: за это время сгорят не 15 попыток (30/2s), а три, разнесённые на 1+2 (+ дальше не будет, уже DLQ). С `backoff_max=8s` и большим `max_attempts` переживают простой без DLQ. Имеет смысл в проде поднять `max_attempts` до 8–10 **или** `backoff_max` до 30s. В плане оставить 3 и 8s как в ТЗ, в комментарии yaml предупредить.

Producer-ошибки Kafka почти всегда retryable (`ErrNotEnoughReplicas`, сеть, брокер down). `error_class` на этой стороне ставить `retryable`. Non-retryable на relay можно не классифицировать в v1.

### 4.3. Изменение `OutboxRelay.relayMessages`

Текущий поток (не ломать):

1. Get unpublished
2. `producer.PublishEvents`
3. Один `BeginFunc`: success → `MarkPublished`, fail → `MarkFailed`

Новый шаг 3:

```
for _, msg := range msgs {
    if pubErr, ok := failed[msg.ID]; ok {
        if msg.Attempts+1 >= maxAttempts {
            MoveToDeadLetter(...)
            log warn: "outbox message moved to DLQ"
            continue
        }
        next := backoff(msg.Attempts+1)
        MarkOutboxMessageFailed(..., next)
        continue
    }
    MarkPublished(...)
}
```

`backoff` и `maxAttempts` читать из atomic, как сейчас interval/limit. Добавить `backoffInitial` / `backoffMax` atomics + сеттеры для watch.

Safety-net на случай краша между UPDATE attempts=max и DLQ: отдельный метод `MoveExhaustedToDeadLetter(ctx, tx, maxAttempts)` — `DELETE … RETURNING` всех `published_at IS NULL AND attempts >= max`. Вызывать из relay в начале тика **или** из cleaner. Лучше из relay (cleaner больше не знает про attempts). Один раз за тик, в той же или отдельной короткой tx.

### 4.4. Cleaner: только retention опубликованных

`CleanOutboxMessages(ctx, tx, retention time.Duration) error` — **убрать `maxAttempts`**.

```sql
DELETE FROM outbox
WHERE published_at IS NOT NULL
  AND published_at < NOW() - $1::interval
```

`OutboxCleaner`: удалить поле `maxAttempts`, `SetMaxAttempts`, аргумент конструктора.

`cmd/outbox-worker`: не передавать maxAttempts в cleaner, убрать из onReload.

Тесты `outbox_cleaner_test.go`: поправить EXPECT (сейчас ещё и баг: `TestOutboxCleaner_Clean_Error` использует `cleaner` без `NewOutboxCleaner` — починить заодно).

Интерфейс `OutboxCleanerRepository` изменится → `mockery`.

---

## 5. Consumer-side DLQ

Затрагиваемые файлы:

- `internal/transport/kafka/consumer.go` — главная дырка
- `internal/transport/kafka/producer.go` — headers + публикация в произвольный топик с headers
- `internal/transport/kafka/message.go` — константы header keys
- `cmd/order-service/main.go` — прокинуть producer + dlq-конфиг в consumer
- `internal/service/service_async.go` — классификация ошибок из БД
- `internal/repository/order/repository.go` — опционально оборачивать unique violation

### 5.1. Классификация ошибок (ключевая мысль задачи)

| Класс | Примеры | Поведение |
|---|---|---|
| **non-retryable** | битый JSON, unknown action, невалидный payload (пустой id после unmarshal), `model.ErrOrderNotFound`, PK unique violation `23505` (повторный create) | сразу в DLQ, 0 in-process ретраев |
| **retryable** | сеть, Postgres down, timeout, serialization failure, context deadline от БД (не shutdown) | M раз с backoff, потом DLQ |

Правило по умолчанию: **ошибка retryable**, пока её явно не пометили. Иначе новый баг в БД уедет в DLQ с первой попытки.

Где маркировать:

1. **Consumer, до handler** — unmarshal и unknown action: сам consumer вызывает `model.NonRetryable(err)`, handler не видит.
2. **Service/repository** — `ErrOrderNotFound` уже есть, consumer проверяет `errors.Is(err, model.ErrOrderNotFound)` → non-retryable. Unique violation: в `CreateOrder` мапить `pgconn.PgError.Code == "23505"` на `model.NonRetryable(err)` (или отдельный `ErrAlreadyExists`). Не тащить pgx в consumer.
3. Shutdown: `session.Context().Err() != nil` — **не** DLQ, выходим из claim без MarkMessage, сообщение ределиверится.

Вынести классификатор в чистую функцию, чтобы покрыть тестом без Kafka:

```go
// internal/transport/kafka/classify.go
func classify(err error) (retryable bool)
```

Либо в `internal/model` / `internal/pkg/errorsx`. Consumer только спрашивает `IsNonRetryable`.

### 5.2. Честный цикл ConsumeClaim

Вынести тело в `processMessage(session, msg *sarama.ConsumerMessage) error` — иначе юнит-тест не написать.

Псевдокод:

```
for msg := range claim.Messages() {
    err := h.processMessage(session.Context(), msg)
    if err != nil {
        // сюда попадаем только если не смогли ни обработать, ни отправить в DLQ
        return err  // Consume() перезапустится, offset не marked → redelivery
    }
    session.MarkMessage(msg, "")
}
```

Внутри `processMessage`:

```
1. unmarshal
   fail → sendToDLQ(non_retryable, attempts=0) ; return nil  // caller сделает Mark

2. switch action
   default → sendToDLQ(non_retryable, attempts=0) ; return nil

3. retryable loop i = 1..M:
       err = handler(...)
       if err == nil { return nil }           // success, Mark снаружи
       if ctx.Err() != nil { return ctx.Err() } // shutdown, без Mark
       if IsNonRetryable(err) {
           sendToDLQ(non_retryable, attempts=i) ; return sendErr
       }
       log retry
       sleep(backoff)  // time.NewTimer + select ctx.Done()
   sendToDLQ(retryable, attempts=M) ; return sendErr
```

`sendToDLQ` возвращает ошибку публикации/insert. Если она не nil — наружу, **без Mark**. Партиция не едет вперёд.

`continue` из текущего кода **удалить полностью**. Комментарии переписать правдой:

```
// MarkMessage только после успеха handler'а или после успешной отправки в DLQ.
// Kafka offset — watermark партиции: пропуск сообщения + mark следующего
// коммитит упавшее как обработанное.
```

In-process sleep блокирует эту партицию (одну горутину claim). Это сознательно: ядовитое мы уже не ретраим, retryable ждёт 200ms * M. Не делать ретраи через «не коммитить offset» — это и есть текущий баг.

### 5.3. Headers DLQ-сообщения

Константы (префикс `x-dlq-` или без, но один набор):

| Header | Откуда |
|---|---|
| `x-original-topic` | `msg.Topic` |
| `x-original-partition` | `strconv.Itoa(int(msg.Partition))` |
| `x-original-offset` | `strconv.FormatInt(msg.Offset, 10)` |
| `x-error` | `err.Error()` (обрезать, например 2KiB) |
| `x-error-class` | `retryable` / `non_retryable` |
| `x-attempts` | сколько раз пробовали handler |
| `x-first-error-at` | RFC3339Nano момента **первой** ошибки этого сообщения (запомнить до цикла ретраев) |
| `x-original-key` | если `msg.Key != nil` |

Value DLQ-сообщения = **оригинальный `msg.Value` как есть**, не обёртка. Иначе redrive должен уметь разворачивать. Контекст — только headers.

Producer: расширить `PublishMessage`:

```go
type PublishMessage struct {
    Topic    string
    Key      []byte
    Payload  []byte
    Metadata any
    Headers  []RecordHeader // своя тонкая обёртка, чтобы kafka-пакет не торчал в worker
}

type RecordHeader struct {
    Key   string
    Value string
}
```

`PublishEvents` уже собирает `sarama.ProducerMessage` — проставить `Headers`. Для DLQ достаточно одного `PublishEventWithHeaders` / `SendToDLQ`. Не ломать существующие тесты `producer_test.go`: headers опциональны.

Нужен **тот же SyncProducer**, что уже есть в order-service. Consumer получает интерфейс, не конкретный `*Producer`:

```go
type DLQSink interface {
    Publish(topic string, payload []byte, headers []RecordHeader) error
}
```

`*Producer` его реализует. В тестах — мок.

После успешного Kafka publish — INSERT в `dead_letters` (source=`kafka_consumer`). Значит consumer должен знать репозиторий DLQ. Чтобы не тащить pgx в `transport/kafka`, два слоя:

**Вариант A (предпочтительный):** тонкий `processMessage` остаётся в kafka-пакете, но `sendToDLQ` — это интерфейс шире:

```go
type FailedMessageHandler interface {
    // Send сохраняет в Kafka DLQ + БД. Вернул nil — можно коммитить offset.
    Send(ctx context.Context, rec DeadLetterRecord) error
}
```

Реализация живёт в `internal/service` или `internal/worker` / `internal/dlq`: сначала Kafka, потом DB. Consumer не знает про таблицу.

**Вариант B:** consumer пишет только в Kafka; отдельный DLQ-consumer читает `orders.dlq` и пишет в БД. Чище с точки зрения dual-write, больше движущихся частей. Для этой задачи — overkill.

Делаем A.

`DeadLetterRecord` содержит topic/partition/offset/payload/error/attempts/firstFailedAt/errorClass.

### 5.4. Wiring order-service

`kafka.NewConsumer(...)` растёт: `dlqSink FailedMessageHandler`, `maxRetries`, `retryBackoff` (или `*config.Provider`).

`mustStartConsumer` собирает sink: producer + `deadletter.Repository` + topic из конфига.

Не забыть: топик команд сейчас `KAFKA_TOPIC_ORDERS=orders.create`. DLQ — `orders.dlq`. Outbox публикует в `orders.events`. Consumer DLQ — про **входной** топик команд, не про outbox events. Outbox events отдельного consumer в этом репо нет — producer-side DLQ их покрывает таблицей.

### 5.5. first_failed_at

Зафиксировать `time.Now()` в момент первой ошибки (unmarshal fail или первая ошибка handler). В headers и в строку `dead_letters`. Не обновлять на каждом ретрае.

---

## 6. Админка и redrive

Это главная ценность DLQ. Без ручек таблица — кладбище.

Сервис: `order-service` (там HTTP). Worker HTTP не поднимает.

### 6.1. API

Следовать стилю `internal/handler/order` + `RegisterRoutes` + `InstrumentMetricsHandler`.

```
GET  /admin/dlq?limit=50&offset=0&source=&retried=false
GET  /admin/dlq/{id}
POST /admin/dlq/{id}/retry
POST /admin/dlq/retry
```

`GET /admin/dlq`:

- query: `limit` (default 50, cap 200), `offset`, `source` (`outbox_relay`|`kafka_consumer`, опционально), `retried` (`true`/`false`/`all`, default `false`)
- 200 JSON:

```json
{
  "items": [
    {
      "id": "...",
      "source": "outbox_relay",
      "original_topic": "orders.events",
      "error_text": "...",
      "error_class": "retryable",
      "attempts": 3,
      "first_failed_at": "...",
      "created_at": "...",
      "retried_at": null,
      "payload": {}
    }
  ],
  "limit": 50,
  "offset": 0
}
```

`POST /admin/dlq/{id}/retry`:

- 404 если нет строки
- 200 `{ "id": "...", "source": "...", "retried": true }`
- 500 если redrive не удался: строка остаётся с `retried_at IS NULL`, пишем `retry_error`

`POST /admin/dlq/retry` батч:

```json
{
  "ids": ["uuid", "..."],
  "source": "outbox_relay",
  "limit": 100
}
```

Правила фильтра:

- если `ids` не пустой — только они
- иначе все pending (`retried_at IS NULL`) с опциональным `source`, `ORDER BY created_at`, `LIMIT`

Ответ: `{ "retried": 12, "failed": [{"id":"...","error":"..."}] }` — частичный успех, HTTP 200. Не валить весь батч из-за одной строки.

### 6.2. Redrive-логика (service)

Новый `internal/service/service_dlq.go` или отдельный `internal/service/dlq.go`, чтобы не раздувать `Service`. Можно методы на существующем `Service`, прокинув `repositoryDeadLetter`.

**source = outbox_relay:**

В одной tx:

1. SELECT dead_letter FOR UPDATE (чтобы два админа не вставили два outbox)
2. INSERT в `outbox` с **новым** id (`uuid.New()`), теми же aggregate/event/topic/payload, `attempts=0`, `next_retry_at=NULL`, `published_at=NULL`, `last_error=NULL`
3. UPDATE dead_letters SET retried_at=NOW(), retry_error=NULL

Почему новый id: unique `idx_dead_letters_outbox_origin` держит старый `original_outbox_id`. Старый id из outbox уже удалён, его можно было бы переиспользовать, но новый id проще и не конфликтует с идемпотентностью DLQ-уника. Связь «это redrive той записи» и так есть через `dead_letters.id` / `original_outbox_id` исходного падения.

Worker подхватит на следующем `relay_interval`.

**source = kafka_consumer:**

1. `producer.SendMessage` / `PublishEvent(original_topic, payload)` **без DLQ-headers** — это должно выглядеть как обычная команда. Key если сохранили.
2. UPDATE `retried_at`. Если publish упал — не ставить `retried_at`, записать `retry_error`.

Не в той же tx что Kafka: dual-write. Порядок: сначала Kafka (сообщение в обработке), потом UPDATE retried. Если UPDATE упал — админ нажмёт retry ещё раз, handler должен пережить дубль (PK create → non-retryable → снова DLQ). Задокументировать.

### 6.3. Handler / router / mocks

- `internal/handler/dlq/handler.go`
- интерфейс `Service` с `ListDeadLetters`, `GetDeadLetter`, `RetryDeadLetter`, `RetryDeadLetters`
- тесты по образцу `internal/handler/order/handler_test.go` (httptest + mockery)
- `RegisterRoutes` — четыре новых маршрута
- `cmd/order-service/main.go` — собрать handler, расширить `mustInitService` репозиторием dead letters

TODO-комментарий: ручки админские, без auth.

### 6.4. Репозиторий

`internal/repository/deadletter/repository.go`:

- `Insert(ctx, tx, model.DeadLetter) error` — для consumer sink; `ON CONFLICT DO NOTHING` по kafka origin
- `List(ctx, filter) ([]model.DeadLetter, error)`
- `GetByID(ctx, id) (model.DeadLetter, error)`
- `MarkRetried(ctx, tx, id, retryErr *string) error`

Insert из outbox CTE может обойти этот репозиторий (см. §4.2). Тогда `Insert` нужен только consumer-side. Чтобы не плодить два пути — worker тоже может вызывать `deadletter.Insert` + `outbox.Delete` в одной tx. Так чище по слоям. Выбрать **один** способ и не делать оба.

Рекомендация для реализации: **два репозитория + одна tx в worker**. CTE не тестируется моками. Юнит-тест relay: expect `Insert` + `Delete` / `MoveToDeadLetter` на интерфейсе.

Интерфейс worker:

```go
type OutboxRepository interface {
    GetOutboxMessagesUnpublished(...)
    MarkOutboxMessagePublished(...)
    MarkOutboxMessageFailed(..., nextRetryAt time.Time)
    DeleteOutboxMessage(ctx, tx, id) error
}

type DeadLetterRepository interface {
    Insert(ctx, tx, model.DeadLetter) error
}
```

Relay сам собирает `model.DeadLetter` из `OutboxMessage`.

---

## 7. Тесты — что покрыть обязательно

Без этого DLQ нельзя считать сделанным: регрессия offset-бага вернётся незаметно.

### 7.1. Unit

| Кейс | Где |
|---|---|
| backoff: 1s, 2s, 4s, cap 8s | `internal/worker/backoff_test.go` (чистая функция `nextRetryAt(attempts, initial, max)`) |
| relay: fail, attempts+1 < max → MarkFailed с next_retry | `outbox_relay` тест, сейчас его нет — завести `outbox_test.go` по образцу cleaner |
| relay: fail, attempts+1 >= max → Insert DLQ + Delete outbox, **не** MarkFailed | тот же файл |
| relay: часть батча ok, часть fail — в одной tx оба пути | |
| cleaner: в репо **не** передаётся maxAttempts, SQL-контракт через mock | починить `outbox_cleaner_test.go` |
| `classify`: json/unknown/not found/unique → non-retryable; io timeout → retryable | `classify_test.go` |
| `processMessage`: unmarshal fail → DLQSink.Send, без handler | mock sink + mock handler |
| `processMessage`: handler retryable M раз, потом Send | sink вызван 1 раз, handler M раз |
| `processMessage`: handler non-retryable → Send сразу, handler 1 раз | |
| `processMessage`: Send падает → ошибка наружу (caller не должен Mark) | |
| `processMessage`: success → sink не вызван | |
| producer: headers уходят в sarama mock | расширить `producer_test.go` |
| HTTP GET list / POST retry 404 / retry ok | `internal/handler/dlq/handler_test.go` |

### 7.2. Integration (`tests/integration`, build tag)

- Insert outbox → руками выставить `attempts=max-1` → мок/реальный брокер с ошибкой publish → строка в `dead_letters`, в `outbox` нет.
- Cleaner: unpublished с большим attempts **не** удаляется; published старше retention — удаляется.
- Consumer: отправить битый JSON в топик → сообщение в `orders.dlq` с headers + строка в `dead_letters` + следующий валидный create обрабатывается (партиция не встала). Это главный регресс-тест offset-бага.
- Redrive outbox_relay: после retry строка снова в `outbox` с `attempts=0`.

Kafka helper уже есть: `tests/integration/kafka.go`. Добавить создание топика DLQ.

### 7.3. Mockery

После смены интерфейсов:

```
mockery
```

Конфиг `.mockery.yaml` рекурсивный по `internal`. Новые интерфейсы подхватятся. `CreateOrderHandler` в exclude — если появится `FailedMessageHandler` / `DLQSink` в kafka-пакете, мок сгенерится сам. Handler dlq `Service` — либо exclude не трогать (include-regex `.*`), либо по аналогии с order явно включить.

`worker.OutboxRepository` получит новые методы — перегенерить `internal/worker/mocks`.

`service.RepositoryOutbox` — если сигнатура `MarkFailed` изменится, упадёт компиляция моков сервиса. Сервису методы Mark не нужны: **сузить `service.RepositoryOutbox` до `CreateOutboxMessage`** в этой же задаче (маленький побочный рефакторинг, меньше боли с моками).

---

## 8. Порядок реализации (делать по фазам, можно мержить по отдельности)

Каждая фаза компилируется и зелёные юниты.

### Фаза A — фундамент

1. Миграция `00003_dead_letters.sql`
2. Модели + sentinel errors + `nextRetryAt()` + тесты backoff/classify
3. Конфиг yaml/env/watch, без поведения
4. Репозиторий `deadletter` + правки outbox repo (SELECT backoff, MarkFailed с next_retry, Delete, Clean без maxAttempts)

### Фаза B — producer-side

1. Relay: backoff + MoveTo DLQ в одной tx + safety-net exhausted
2. Cleaner: выпилить maxAttempts
3. Wiring outbox-worker + hot-reload backoff
4. Юнит-тесты relay/cleaner
5. Integration: fail publish → dead_letters, cleaner не ест unpublished

После фазы B дырка «событие удалили» закрыта. Можно пользоваться даже без consumer DLQ.

### Фаза C — consumer-side

1. Producer headers + `Publish` с headers
2. `FailedMessageHandler` (Kafka + Insert DB)
3. Переписать `ConsumeClaim` / `processMessage`
4. Классификация в unmarshal/unknown/service
5. Wiring order-service
6. Юнит processMessage + integration битый JSON → DLQ, следующий ок

После фазы C дырка «offset проезжает мимо» закрыта.

### Фаза D — redrive

1. Service redrive оба source
2. HTTP handler + router
3. Юнит HTTP
4. Integration redrive → outbox / исходный топик

Без D таблица бесполезна операционно. Не откладывать «на потом».

---

## 9. Чеклист файлов

Новые:

- `migrations/00003_dead_letters.sql`
- `internal/model/dead_letter.go`
- `internal/model/errors.go` (или расширить order.go — лучше отдельный файл)
- `internal/repository/deadletter/repository.go`
- `internal/transport/kafka/classify.go` (+ `_test.go`)
- `internal/worker/backoff.go` (+ `_test.go`)
- `internal/worker/outbox_test.go`
- `internal/handler/dlq/handler.go` (+ `_test.go`)
- `internal/service/service_dlq.go`
- `internal/dlq/sink.go` — реализация FailedMessageHandler (имя пакета на вкус; не обязательно отдельный пакет, можно `internal/service`)

Меняются:

- `internal/model/outbox_message.go`
- `internal/repository/outbox/repository.go`
- `internal/worker/outbox.go`
- `internal/worker/outbox_cleaner.go`
- `internal/worker/outbox_cleaner_test.go`
- `internal/transport/kafka/consumer.go`
- `internal/transport/kafka/producer.go`
- `internal/transport/kafka/producer_test.go`
- `internal/transport/kafka/message.go` — header constants
- `internal/config/config.go`, `env.go`, `watch.go`
- `config/local.yaml`, `config/prod.yaml`
- `.env.example`, `.env.prod.example`
- `cmd/outbox-worker/main.go`
- `cmd/order-service/main.go`
- `internal/handler/router.go`
- `internal/service/service.go` — сузить RepositoryOutbox, добавить deadletter repo
- `internal/service/service_async.go` — wrap non-retryable
- `internal/repository/order/repository.go` — unique → NonRetryable (желательно)
- `tests/integration/kafka.go` + новый тест
- моки после mockery

---

## 10. Инварианты (проверить руками перед тем как считать задачу закрытой)

1. Неопубликованная строка outbox **никогда** не удаляется cleaner'ом, только уезжает в `dead_letters` или публикуется.
2. Перенос outbox → DLQ и удаление из outbox — одна транзакция. После краша: либо строка в outbox, либо в dead_letters, никогда «нигде».
3. `GetOutboxMessagesUnpublished` не возвращает строки с `next_retry_at > NOW()`.
4. В `ConsumeClaim` нет `continue` по ошибке обработки. MarkMessage — только success или успешный DLQ.
5. Битый JSON не вызывает handler и не блокирует следующее валидное сообщение партиции.
6. Retryable ошибка handler'а вызывается до `consumer_max_retries` раз с паузой; после — DLQ.
7. В сообщении `orders.dlq` есть headers topic/partition/offset/error/attempts/first-error-at, body = исходный payload.
8. `POST /admin/dlq/{id}/retry` для `outbox_relay` создаёт новую unpublished-строку; relay её публикует.
9. `POST /admin/dlq/{id}/retry` для `kafka_consumer` кладёт payload обратно в `original_topic`.
10. Повторный insert той же (topic, partition, offset) в `dead_letters` не плодит дубли.

---

## 11. Открытые мелочи, решать по месту

- Нужен ли `SELECT FOR UPDATE` на redrive — да, на одну строку, дёшево.
- Обрезать ли `error_text` / header `x-error` (рекомендация: 2048 байт), чтобы ядовитый payload не раздул header (сам payload в value, в error только текст ошибки).
- Метрики (не блокер): `dead_letters_total{source,error_class}`, `outbox_moved_to_dlq_total`. Если трогать `internal/handler/metrics.go` / kafka — по аналогии с HTTP counters.
- `TestOutboxCleaner_Clean_Error` сейчас не компилируется/паникует из-за неинициализированного `cleaner` — починить в фазе B, не игнорировать.
)