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

## Тесты

```bash
pytest tests/unit
pytest tests/integration -m integration   # требует Docker
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
| Структурные логи (structlog) с `payment_id` | Наблюдаемость и отладка |
| Unit + integration тесты (testcontainers) | «Максимально качественно», TDD по регламенту |
