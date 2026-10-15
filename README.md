# Payment System

Асинхронный сервис процессинга платежей: FastAPI + RabbitMQ (Outbox, retry, DLQ) + PostgreSQL.

## Запуск

```bash
cp .env.example .env
docker compose up --build
```

- API: http://localhost:8000 (docs: `/docs`)
- RabbitMQ UI: http://localhost:15672 (guest/guest)

Порт API на хосте настраивается через переменную `API_PORT` (по умолчанию `8000`):

```bash
API_PORT=18000 docker compose up --build
```

## Примеры

Создание платежа:

```bash
curl -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: dev-api-key" \
  -H "Idempotency-Key: demo-1" \
  -H "Content-Type: application/json" \
  -d '{"amount":"100.50","currency":"RUB","description":"order","metadata":{"order_id":"42"},"webhook_url":"https://example.com/hook"}'
```

Ответ: `202 {"payment_id":"...","status":"pending","created_at":"..."}`.

Получение платежа:

```bash
curl http://localhost:8000/api/v1/payments/<payment_id> -H "X-API-Key: dev-api-key"
```

## RabbitMQ топология

```
exchange payments (direct)
  ├─ payments.new     (consumer)  DLX→ payments.dlx "dlq"
  ├─ payments.retry.1 (TTL 2s)    DLX→ payments "new"
  └─ payments.retry.2 (TTL 4s)    DLX→ payments "new"
exchange payments.dlx (direct)
  └─ payments.dlq
```

## Архитектура

Слои (по кодстайлу проекта): **Router → Handler → Unit of Work → Query (БД)**, плюс
доменные сервисы без БД (`PaymentGateway`, `WebhookSender`).

```
main.py / config.py            # точка входа + frozen-настройки (settings)
src/
  database/    db_settings.py (NullPool), models.py (все ORM)
  dependencies/ auth.py (X-API-Key), db.py (get_async_session)
  routers/     *_routers.py    # тонкие: только Depends + вызов handler
  handlers/    *_handlers.py   # оркестрация, маппинг ORM→DTO, бизнес-исключения→HTTP
  schemas/     base.py (CamelSchema), *_schemas.py
  services/    gateway.py; queries/ (чистый SQLAlchemy); uow/ (границы транзакций)
  utils/       exceptions.py (NotFoundError/AlreadyExistsError), http_utils.py (HMAC+webhook)
  messaging/   broker/topology/publisher/consumer (FastStream)
  workers/     outbox_worker.py
telemetry/     setup_telemetry.py  # OpenTelemetry → OTLP
```

**Осознанное отклонение от кодстайла:** внешние JSON-схемы API остаются `snake_case`
(`payment_id`, `created_at`, `webhook_url`), т.к. это контракт ТЗ. `CamelSchema`
применяется только к внутренним схемам; внешние DTO (`src/schemas/payment_schemas.py`)
наследуют обычный `BaseModel`.

## Тесты

```bash
uv run pytest tests/unit
uv run pytest tests/integration -m integration   # требует Docker
```

## За рамками ТЗ (что добавлено и почему)

| Добавление | Почему |
|---|---|
| `webhook_delivered_at` + условный UPDATE статуса (`WHERE status='pending'`) | Без отдельного признака доставки retry webhook ломается (статус уже терминальный), а дубли сообщений дают двойную обработку |
| `raise_for_status()` + классификация 4xx/5xx/429 | Иначе «провалившийся» webhook (500/404) молча считается успехом |
| HMAC-подпись webhook (`X-Signature`, `X-Timestamp`) | Защита клиента от подделки и replay; согласовано с владельцем задачи |
| `x-retry-count` + poison (`not_found`) → DLQ | Предотвращает бесконечный цикл retry-очередей и вечный retry неретраибельного |
| Единый runner миграций (только `api`) | Устраняет гонку миграций при старте нескольких контейнеров |
| Healthchecks + `depends_on: service_healthy` | Детерминированный порядок старта, меньше flaky |
| Валидация `webhook_url` (только https) | Безопасность: URL задаёт клиент (SSRF/утечки) |
| Стандартный `logging` + OpenTelemetry | Наблюдаемость: трассировка/метрики/логи в OTLP, трассировка webhook-доставки |
| Unit + integration тесты (testcontainers) | «Максимально качественно», TDD по регламенту |

## Smoke check

Проверка сквозного пути retry → DLQ на недоступном https-эндпоинте (офлайн-детерминированно):

```bash
# 1. Создать платёж с заведомо недоступным webhook
curl -s -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: dev-api-key" \
  -H "Idempotency-Key: smoke-dlq" \
  -H "Content-Type: application/json" \
  -d '{"amount":"1.00","currency":"EUR","description":"dlq","metadata":{},"webhook_url":"https://127.0.0.1:1/hook"}'
# → 202 {"payment_id":"...","status":"pending",...}

# 2. Дождаться терминального статуса
curl -s http://localhost:8000/api/v1/payments/<payment_id> -H "X-API-Key: dev-api-key"
# → status ∈ {succeeded, failed}, processed_at задан, webhook_delivered_at == null

# 3. Через ~15 с сообщение в DLQ (webhook-ретраи + TTL 2s → 4s → dlq)
curl -s -u guest:guest "http://localhost:15672/api/queues/%2F/payments.dlq"
# → messages == 1; payload: {"payment_id":"...","event_type":"payment.created","retry_count":3}
#   header: x-reason == "all attempts failed"

# 4. Идемпотентность: тот же Idempotency-Key → тот же payment_id
curl -s -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: dev-api-key" -H "Idempotency-Key: smoke-dlq" \
  -H "Content-Type: application/json" \
  -d '{"amount":"999.99","currency":"USD","description":"x","metadata":{},"webhook_url":"https://127.0.0.1:1/hook"}'
# → 202 с тем же payment_id (новый платёж не создаётся)
```

> **Успешный путь** (доставка webhook, `webhook_delivered_at` != null) требует https-эндпоинта,
> доступного из контейнера `consumer`. Так как сервис принимает только `https://` webhook-URL,
> офлайн это не проверить — нужен публичный туннель до локального приёмника (например,
> `ngrok http 9999` + приёмник `python -m http.server 9999`).
