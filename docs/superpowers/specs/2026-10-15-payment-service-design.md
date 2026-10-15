# Design: Async Payment Processing Service

**Дата:** 2026-10-15
**Статус:** Approved

## 1. Цель и scope

Микросервис асинхронной обработки платежей: приём платежа по REST, гарантированная
публикация события (Outbox pattern), обработка consumer'ом через эмуляцию шлюза,
уведомление клиента webhook'ом с подписью, retry с экспоненциальной задержкой и DLQ.

**В scope:** API (create/get), Outbox pattern, RabbitMQ-топология с retry и DLQ,
consumer, webhook-доставка с HMAC, Docker-compose, тесты, README.

**Вне scope:** реальный платёжный шлюз, аутентификация пользователей/JWT,
мультиарендность, UI, автоматический репроцессинг DLQ.

## 2. Архитектура

5 сервисов в docker-compose:

| Сервис | Ответственность |
|---|---|
| `api` | FastAPI, REST, пишет `payment` + `outbox` в одной транзакции; runner миграций |
| `outbox-worker` | Поллит `outbox`, публикует события в RabbitMQ, помечает `published_at` |
| `consumer` | Один handler: эмуляция шлюза → update статуса → webhook → ack/retry/DLQ |
| `postgres` | Хранилище платежей и outbox |
| `rabbitmq` | Брокер сообщений (management UI) |

Поток данных:

```
Client ──POST/GET──> api ──(tx)──> postgres[payments, outbox]
                                     ▲
                       outbox-worker │ poll unpublished → publish
                                     ▼
                                   rabbitmq[payments.new]
                                     ▲
                                   consumer ──> webhook ──> Client (HMAC-signed)
```

## 3. Модель данных (Alembic)

### payments

| Поле | Тип | Примечание |
|---|---|---|
| id | UUID PK | gen uuid4 |
| amount | Numeric(18,2) | CHECK amount > 0 |
| currency | Enum(RUB, USD, EUR) | |
| description | Text | |
| metadata | JSONB | default `{}` |
| status | Enum(pending, succeeded, failed) | default pending |
| idempotency_key | Text | UNIQUE NOT NULL |
| webhook_url | Text | |
| created_at | timestamptz | default now() |
| processed_at | timestamptz null | момент установки терминального статуса |
| webhook_delivered_at | timestamptz null | источник правды для доставки webhook |
| webhook_attempts | int default 0 | наблюдаемость доставки |

### outbox

`id bigserial PK`, `event_type text`, `aggregate_id uuid`, `payload JSONB`,
`created_at timestamptz default now()`, `published_at timestamptz null`.

Индекс: partial `(created_at) WHERE published_at IS NULL`.

## 4. RabbitMQ топология

```
exchange payments (direct, durable)
  ├─ queue payments.new     bind "new"  DLX→payments.dlx "dlq"   (consumer)
  ├─ queue payments.retry.1 TTL=2000ms  DLX→payments "new"       (без consumer)
  └─ queue payments.retry.2 TTL=4000ms  DLX→payments "new"       (без consumer)
exchange payments.dlx (direct, durable)
  └─ queue payments.dlq     bind "dlq"
```

- Счётчик попыток в header `x-retry-count` (int, default 0), **прокидывается** при
  публикации в retry-очередь.
- Отдельная очередь на попытку (фиксированный TTL) — точный backoff без
  head-of-line blocking.
- Backoff: попытка 1 → fail → `retry.1` (2s); попытка 2 → fail → `retry.2` (4s);
  попытка 3 → fail → DLQ. Итого 3 попытки обработки.
- Все сообщения `persistent`.

## 5. API контракт

**Аутентификация:** заголовок `X-API-Key` (env `API_KEY`), сравнение constant-time.
Отсутствует/неверный → `401`. Применяется ко всем эндпоинтам.

### POST `/api/v1/payments`

Header `Idempotency-Key` обязателен (отсутствует → `422`).

```json
{
  "amount": "100.50",
  "currency": "RUB",
  "description": "Оплата заказа",
  "metadata": {"order_id": "42"},
  "webhook_url": "https://client.example/webhook"
}
```

Ответ `202 Accepted`:

```json
{ "payment_id": "...", "status": "pending", "created_at": "..." }
```

Идемпотентность: `INSERT payment` + `INSERT outbox` в одной транзакции. При unique
violation по `idempotency_key` → повторный `SELECT` → `202` с существующим платежом.

### GET `/api/v1/payments/{payment_id}`

`200` (детали) / `404`. Возвращает `status`, `amount`, `currency`, `description`,
`metadata`, `created_at`, `processed_at`, `webhook_delivered_at`.

### Ошибки

Единый формат: `{ "detail": "...", "code": "..." }`. Коды: `401`, `404`, `422`.

## 6. Outbox pattern

- Запись `outbox` создаётся **в той же транзакции**, что и `payment` (атомарно).
- Worker: `SELECT ... WHERE published_at IS NULL ORDER BY created_at
  FOR UPDATE SKIP LOCKED LIMIT N` → publish (persistent) → `UPDATE published_at=now()`
  → commit.
- Гарантия **at-least-once**: публикация идёт вне БД-транзакции, возможны дубли →
  consumer дедуплицирует.

## 7. Consumer (один handler)

Управление ack — **явное**: handler не пробрасывает исключения наружу, а сам
классифицирует ошибку и маршрутизирует (publish в retry/DLQ), затем возвращается →
auto-ack. Так не зависим от version-specific nack-поведения FastStream.

```python
async def handle(msg):
    payment_id = msg.payment_id
    payment = load(payment_id)
    if payment is None:                        # poison → сразу DLQ, без retry
        publish_dlq(msg, reason="not_found"); return

    if payment.status == "pending":            # условный переход = дедуп
        won = UPDATE payments
              SET status = ?, processed_at = now()
              WHERE id = ? AND status = 'pending'
              RETURNING id
        if won:                                 # обрабатывает только победитель
            result = gateway.process()          # sleep 2-5s, 90% success
            status = succeeded if result else failed
            commit(status)
        else:
            reload(payment)

    if payment.webhook_delivered_at is None:    # webhook-retry работает даже
        try:                                    # при терминальном статусе
            await webhook.send(payment)         # 3 попытки, backoff 1s/2s
            mark webhook_delivered_at = now()
        except Exception as e:
            route_retry_or_dlq(msg, e); return   # publish retry/DLQ, ack
```

Классификация ошибок:

- **Бизнес-fail (10%)** → `status=failed`, webhook шлётся, ack. Retry нет.
- **Technical (webhook/БД)** → retry-очередь, после 3 попыток → DLQ.
- **Poison (`not_found`, невалидный payload)** → сразу DLQ.

Нюанс: статус коммитится до отправки webhook. Источник правды о доставке —
`webhook_delivered_at`, а не результат шлюза.

## 8. Webhook

- `POST {webhook_url}`, JSON: `{ payment_id, status, amount, currency, processed_at }`.
- Подпись: `X-Signature: sha256=<hex>` — HMAC-SHA256 **сырого тела** с
  env `WEBHOOK_SIGNING_SECRET`. Плюс заголовок `X-Timestamp` (защита от replay).
- `httpx.AsyncClient`, timeout (env), **обязательный** вызов `raise_for_status()`.
- Внутренний retry: 3 попытки, backoff 1s/2s. **Не ретраить `4xx`** (кроме `429`).
  Финальный провал → исключение → broker retry → DLQ.
- At-least-once: клиент может получить дубль → дедуп по `payment_id` (документируем).

## 9. Config (pydantic-settings, env)

`DATABASE_URL`, `RABBITMQ_URL`, `API_KEY`, `WEBHOOK_SIGNING_SECRET`, `WEBHOOK_TIMEOUT`,
`OUTBOX_POLL_INTERVAL`, `OUTBOX_BATCH_SIZE`, `GATEWAY_MIN_DELAY`, `GATEWAY_MAX_DELAY`,
`GATEWAY_SUCCESS_RATE`. Секреты — только через env, не в git.

## 10. Docker

Один `Dockerfile`, разные команды для `api` / `outbox-worker` / `consumer`.
Базовые образы: `postgres:16-alpine`, `rabbitmq:3-management-alpine`. Миграции
выполняются **только** в `api` (или отдельным init-шагом) — устраняет гонку
миграций. Healthchecks для api и consumer, `depends_on: condition: service_healthy`.

## 11. Тесты (TDD)

- **unit:** gateway-распределение, идемпотентность сервиса, HMAC-подпись,
  классификация retry (4xx vs 5xx), сборка outbox-записи.
- **integration** (testcontainers pg+rabbit): `401`/`202`/`200`/`404`, дубль
  `Idempotency-Key`, outbox→брокер, consumer success/fail, webhook-retry после
  терминального статуса, retry→DLQ за 3 попытки, poison→DLQ.
- Инструменты: `pytest`, `pytest-asyncio`, `httpx.AsyncClient`, `testcontainers`.

## 12. Security

- HMAC-подпись webhook.
- `X-API-Key` со сравнением constant-time.
- Валидация `webhook_url` (обязательно `https`).
- Секреты только через env (не в git).

## 13. README — блок «За рамками ТЗ» (с обоснованием)

| Добавление | Почему |
|---|---|
| `webhook_delivered_at` + условный переход статуса | Без этого retry webhook не работает, а дубли сообщений дают двойную обработку |
| `raise_for_status()` + классификация 4xx/5xx | Иначе «провалившийся» webhook считается успехом |
| HMAC-подпись webhook | Требование владельца задачи; защита клиента от подделки |
| `x-retry-count` + poison→DLQ | Предотвращает бесконечный цикл и вечный retry неретраибельного |
| Единый runner миграций | Устраняет гонку миграций между контейнерами |
| Healthchecks + `depends_on: service_healthy` | Детерминированный старт, меньше flaky E2E |
| Валидация `webhook_url` (https/SSRF) | Безопасность: URL задаёт клиент |
| Структурные логи с `payment_id` | Наблюдаемость/отладка |
| Тесты (unit + integration) | «Максимально качественно», TDD по регламенту |

## 14. Порядок реализации (фазы)

1. Scaffold, config, db, модели, Alembic
2. API + auth + идемпотентность
3. Outbox + worker
4. RabbitMQ топология + publisher
5. Consumer + gateway + webhook (HMAC, retry) + retry/DLQ
6. Docker-compose + README (+ блок «за рамками ТЗ»)
7. E2E smoke + review-гейт

## 15. Журнал решений

- Retry = весь handler на брокере + отдельный внутренний retry webhook.
- Outbox relay = отдельный worker-процесс.
- Retry-механизм = retry-queues + TTL + DLX.
- Дубль `Idempotency-Key` → `202` + существующий платёж.
- Эмулируемая 10% ошибка = бизнес-fail без retry.
- HMAC-подпись webhook — да.
