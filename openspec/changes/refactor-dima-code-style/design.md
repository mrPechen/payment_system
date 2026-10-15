# Design: Рефакторинг payment-system под кодстайл Dima

**Дата:** 2026-10-15
**Ветка:** `refactor/dima-code-style`

Источник истины целевого стиля: `~/.config/opencode/skills/dima-code-style/SKILL.md`.
Рефакторинг поведение не меняет — это перенос кода и пересборка слоёв с сохранением
существующих алгоритмов.

## 1. Целевое дерево проекта

```
payment_system/
├── main.py                      # FastAPI app, lifespan, router registration
├── config.py                    # pydantic-settings (frozen=True), module-level settings
├── gunicorn.conf.py             # gunicorn + uvicorn workers (graceful_timeout)
├── pyproject.toml               # uv-проект, Python 3.13, закреплённые диапазоны
├── uv.lock                      # Lock-файл uv
├── .python-version              # 3.13
├── .env.example                 # UPPER_SNAKE ключи
├── Dockerfile                   # multi-stage python:3.13-slim + uv
├── docker-compose.yml           # 5 сервисов (без изменения)
├── alembic.ini                  # без изменений
├── alembic/
│   ├── env.py                   # импорты: config.settings, src.database.models.Base
│   └── versions/0001_initial.py # без изменений (схема идентична)
│
├── src/
│   ├── __init__.py
│   ├── database/
│   │   ├── __init__.py
│   │   ├── db_settings.py       # engine (NullPool) + SessionLocal + Base (re-export)
│   │   └── models.py            # Base, Currency, PaymentStatus, Payment, OutboxMessage
│   ├── dependencies/
│   │   ├── __init__.py
│   │   ├── auth.py              # require_api_key (Annotated Header, constant-time)
│   │   └── db.py                # get_async_session
│   ├── handlers/
│   │   ├── __init__.py          # re-export PaymentHandler, ProcessingHandler
│   │   ├── payment_handlers.py  # PaymentHandler + get_payment_handler
│   │   └── processing_handlers.py # ProcessingResult, ACTION_*, ProcessingHandler
│   ├── routers/
│   │   ├── __init__.py
│   │   ├── payments_routers.py  # тонкий: Depends + handler
│   │   └── health_routers.py    # тонкий: /health, /health/ready (SELECT 1)
│   ├── schemas/
│   │   ├── __init__.py
│   │   ├── base.py              # CamelSchema
│   │   └── payment_schemas.py   # PaymentCreateRequest/Response, DetailResponse
│   ├── services/
│   │   ├── __init__.py
│   │   ├── gateway.py           # PaymentGateway (доменный сервис, без БД)
│   │   ├── queries/
│   │   │   ├── __init__.py
│   │   │   ├── payment_queries.py  # PaymentQuery
│   │   │   └── outbox_queries.py   # OutboxQuery
│   │   └── uow/
│   │       ├── __init__.py
│   │       ├── base.py          # AbstractUnitOfWork
│   │       └── payments_uow.py  # PaymentsUnitOfWork + get_payments_uow
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── exceptions.py        # NotFoundError, AlreadyExistsError
│   │   └── http_utils.py        # sign_payload, Permanent/TransientWebhookError, WebhookSender
│   ├── messaging/
│   │   ├── __init__.py
│   │   ├── broker.py
│   │   ├── consumer.py          # PaymentEvent, decide_route, handle_new_payment
│   │   ├── publisher.py
│   │   └── topology.py
│   └── workers/
│       ├── __init__.py
│       └── outbox_worker.py
│
├── telemetry/
│   ├── __init__.py
│   └── setup_telemetry.py       # OpenTelemetry → OTLP gRPC insecure
│
└── tests/
    ├── conftest.py              # импорты src.*, get_async_session, settings
    ├── unit/                    # импорты src.* (ассерты без изменений)
    └── integration/             # импорты src.* (ассерты без изменений)
```

## 2. Слои и ответственность

```
HTTP Request → Router (тонкий) → Handler → UoW → Query (БД)
                                    ↓
                               Service (gateway/webhook, без БД)
```

- **Router:** только `Depends` + вызов метода handler-а + `response_model`/`status_code`.
  Не содержит бизнес-логики и SQL.
- **Handler:** оркестрация, `_build_*_response`, маппинг `NotFoundError`→404 /
  `AlreadyExistsError`→409, проверка обязательного `Idempotency-Key` (422).
  Не трогает `session` напрямую.
- **Query:** чистый SQLAlchemy (`select`/`insert`/`update`/`flush`), возвращает
  ORM/`None`, бросает только бизнес-исключения. Никакой бизнес-логики и HTTP.
- **UoW:** границы транзакций (`__aenter__`/`__aexit__`, `commit`/`rollback`).
- **Service:** тяжёлая/внешняя логика без БД (`PaymentGateway`, `WebhookSender`).

## 3. Маппинг текущий → целевой (per module)

| Текущий файл | Целевой файл | Что меняется |
|---|---|---|
| `app/main.py` | `main.py` | `create_app()` + `lifespan` (telemetry) + module-level `app` |
| `app/config.py` | `config.py` | UPPER_SNAKE, `frozen=True`, `case_sensitive=False`, `settings = Settings()` |
| `app/db.py` | `src/database/db_settings.py` | `NullPool`, `SessionLocal(expire_on_commit=False, autoflush=False)` |
| `app/logging.py` | удалён | structlog → std `logging` + OTel |
| — (новое) | `telemetry/setup_telemetry.py` | `setup_telemetry` / `shutdown_telemetry` |
| `app/models/base.py` | `src/database/models.py` | `Base` инлайн |
| `app/models/enums.py` | `src/database/models.py` | `Currency`, `PaymentStatus` (имена/значения без изменений) |
| `app/models/payment.py` | `src/database/models.py` | `Payment` (те же колонки/constraints) |
| `app/models/outbox.py` | `src/database/models.py` | `OutboxMessage` |
| `app/models/__init__.py` | удалён | re-export через `src.database.models` |
| `app/api/deps.py` | `src/dependencies/auth.py` | `require_api_key` |
| `app/db.py::get_session` | `src/dependencies/db.py` | `get_async_session` |
| `app/schemas/payment.py` | `src/schemas/base.py` + `src/schemas/payment_schemas.py` | `CamelSchema` + snake_case DTO |
| `app/api/routes/payments.py` | `src/handlers/payment_handlers.py` + `src/routers/payments_routers.py` | логика → Handler, роут → тонкий Router |
| `app/api/routes/health.py` | `src/routers/health_routers.py` | тонкий роут, `/health/ready` = `SELECT 1` |
| `app/services/payment_service.py` | `src/services/queries/payment_queries.py` + `src/services/queries/outbox_queries.py` + `payment_handlers.py` | `create_payment` → Handler+Query; `get_*` → Query; `DuplicateKeyRace` → `AlreadyExistsError` |
| `app/services/repository.py` | `src/services/queries/payment_queries.py` | `SqlPaymentRepository` → `PaymentQuery` (`mark_processed`, `mark_webhook_delivered`, `increment_webhook_attempts`) |
| `app/services/gateway.py` | `src/services/gateway.py` | `settings.GATEWAY_*` |
| `app/services/processing.py` | `src/handlers/processing_handlers.py` | `ProcessingService` → `ProcessingHandler` (через UoW/Query); `ProcessingResult` + `ACTION_*` остаются |
| `app/services/signature.py` | `src/utils/http_utils.py` | `sign_payload` |
| `app/services/webhook.py` | `src/utils/http_utils.py` | `WebhookSender` + `Permanent/TransientWebhookError` |
| `app/messaging/broker.py` | `src/messaging/broker.py` | `settings.RABBITMQ_URL` |
| `app/messaging/consumer.py` | `src/messaging/consumer.py` | `ProcessingHandler` вместо `ProcessingService`/`SqlPaymentRepository`; `decide_route` остаётся |
| `app/messaging/publisher.py` | `src/messaging/publisher.py` | логика без изменений |
| `app/messaging/topology.py` | `src/messaging/topology.py` | логика без изменений |
| `app/workers/outbox_worker.py` | `src/workers/outbox_worker.py` | `publish_batch` → UoW/Query; `settings.OUTBOX_*` |
| `pyproject.toml` | `pyproject.toml` | uv, 3.13, закреплённые диапазоны, −structlog/ruff, +black/otel/gunicorn |
| — (новое) | `uv.lock`, `.python-version`, `gunicorn.conf.py` | |
| `Dockerfile` | `Dockerfile` | multi-stage `python:3.13-slim` + uv |
| `docker-compose.yml` | `docker-compose.yml` | команды `src.*`, env UPPER_SNAKE, 5 сервисов |
| `.env.example` | `.env.example` | UPPER_SNAKE + `DEBUG`/`SERVICE_NAME`/`OTLP_ENDPOINT` |
| `alembic/env.py` | `alembic/env.py` | `config.settings` + `src.database.models.Base` |
| `tests/*` | `tests/*` | импорты `src.*`, `get_async_session`, `settings`, UPPER_SNAKE-поля |

## 4. Ключевые решения и trade-offs

### 4.1. Внешний API остаётся snake_case (фиксированное решение владельца)

ТЗ требует JSON-ключи `payment_id`, `created_at`, `webhook_url`, `metadata`,
`idempotency_key`. Поэтому `PaymentCreateRequest`, `PaymentCreateResponse`,
`PaymentDetailResponse` **не наследуют** `CamelSchema`, а используют обычный
`pydantic.BaseModel` (snake_case). `CamelSchema` остаётся в `src/schemas/base.py` как
базовый класс для будущих внутренних схем.

**Trade-off:** это осознанное отступление от дефолтного правила стиля «Поля Pydantic
(внешний API) camelCase». Принято владельцем, поскольку контракт ТЗ важнее стиля.
Задокументировано в README и здесь.

### 4.2. Единый `DATABASE_URL` вместо DB_HOST/DB_PORT/DB_USER/DB_PASS/DB_NAME

Стиль показывает сплит на `DB_*`-поля и сборку `sql_link` в `db_settings.py`. Но:
- `docker-compose.yml` и `alembic/env.py` уже используют единый `DATABASE_URL`
  (часть контракта).
- Мета-правило стиля: «если правило конфликтует с тем, что уже есть в проекте —
  следуй проекту».

**Решение:** оставляем `DATABASE_URL` (UPPER_SNAKE), в `db_settings.py` строим
`create_async_engine(settings.DATABASE_URL, poolclass=NullPool)`. Меньше churn, меньше риска.

### 4.3. `DuplicateKeyRace` → `AlreadyExistsError`

В стиле только два бизнес-исключения. Гонка уникального ключа на вставке мапится на
`AlreadyExistsError` с текстом «Concurrent request with the same Idempotency-Key».
Handler различает сценарии так: если при ошибке вставки повторный `SELECT` по ключу
находит платёж — возвращаем `202` (существующий); если не находит — `409`.

### 4.4. `created_at` (server_default func.now()) до коммита

`created_at` задаётся на стороне БД (`server_default=func.now()`), поэтому после
`flush()` ORM-объект ещё не содержит значения. `PaymentQuery.create()` делает
`add → flush → refresh(payment)`, чтобы вернуть платёж с заполненным `created_at`,
а коммит откладывается на `UoW.__aexit__`. Это сохраняет возможность построить ответ
`PaymentCreateResponse` (поле `created_at: datetime`, не optional) до выхода из транзакции.

### 4.5. Транзакционные границы в обработке (критично для поведения)

Текущий `ProcessingService` коммитит статус **до** отправки webhook (`mark_processed`
делает `commit`). Это — зафиксированное в ТЗ поведение: при transient-ошибке webhook
статус уже терминальный, и на retry gateway не перезапускается, а повторяется только
webhook. Одиночный коммит в `UoW.__aexit__` сломал бы это (rollback откатил бы и статус).

**Решение:** `ProcessingHandler.process()` использует **явные точки коммита**
`await self.uow.commit()` после `mark_processed`, после `increment_webhook_attempts`
и после `mark_webhook_delivered`. Условный `UPDATE ... WHERE status='pending'`
сохраняет идемпотентность. `SessionLocal` задаёт `expire_on_commit=False`, чтобы
объект `payment` оставался актуальным между коммитами (текущее поведение `repository.py`).

### 4.6. `WebhookSender` остаётся с инъекцией `httpx.AsyncClient`

Стиль показывает HTTPX-клиенты, владеющие своим пулом. Но существующие тесты
инжектируют `httpx.MockTransport`/`AsyncClient` в `WebhookSender`. Чтобы сохранить
тестируемость без изменения ассертов, `WebhookSender` остаётся тонкой обёрткой над
переданным `httpx.AsyncClient` (клиент создаётся в consumer-е). HMAC-подпись и
классификация 4xx/5xx/429 — без изменений.

### 4.7. Тонкий health-роут как исключение

`/health/ready` выполняет `SELECT 1` напрямую через `Depends(get_async_session)`. Это
инфраструктурный liveness-пробник, а не доменная бизнес-логика — выносить его в
Handler/Query нецелесообразно. Задокументировано как осознанное исключение.

## 5. Неизменяемый контракт ТЗ (регрессионный барьер)

- **Сервисы docker-compose (5):** `postgres`, `rabbitmq`, `api`, `outbox-worker`, `consumer`.
- **API:** `POST /api/v1/payments` (202), `GET /api/v1/payments/{id}` (200/404),
  `GET /health`, `GET /health/ready`. Аутентификация `X-API-Key` (constant-time),
  header `Idempotency-Key` обязателен (422 при отсутствии).
- **Идемпотентность:** дубль ключа → `202` + существующий платёж; гонка → `409`.
- **Outbox:** `payment` + `outbox` в одной транзакции; worker `FOR UPDATE SKIP LOCKED`.
- **Retry/DLQ:** 3 попытки, backoff 2s/4s через TTL+DLX (`payments.retry.1`/`.2`,
  `payments.dlx`→`payments.dlq`), header `x-retry-count`, poison→DLQ.
- **Webhook:** `X-Signature: sha256=<hmac>` по сырому телу, `X-Timestamp`, только
  `https://`, не ретраить `4xx` (кроме 429), внутренний retry 1s/2s.
- **Схема БД:** таблицы/колонки/enum (`currency`, `payment_status`) и их значения
  (`RUB/USD/EUR`, `pending/succeeded/failed`) идентичны.

## 6. Требования (инварианты, WHEN/THEN)

- **R-1.** WHEN `PaymentQuery.get_by_id(payment_id)` не находит запись THEN бросается
  `NotFoundError`; handler мапит его в `404`.
- **R-2.** WHEN `PaymentQuery.create()` ловит `IntegrityError` по `idempotency_key`
  THEN выполняется `rollback` и бросается `AlreadyExistsError`.
- **R-3.** WHEN handler ловит `AlreadyExistsError` и повторный `SELECT` находит платёж
  THEN возвращается `202` с существующим платежом; иначе `409`.
- **R-4.** WHEN `PaymentQuery.mark_processed(payment, status)` выполняет условный
  `UPDATE ... WHERE status='pending' RETURNING id` THEN возвращает `True` только для
  победителя и синхронизирует `payment.status` в `PaymentStatus`-enum.
- **R-5.** WHEN `ProcessingHandler.process()` получает `not_found` THEN возвращается
  `ProcessingResult(ACTION_DLQ, reason="not_found")` без retry.
- **R-6.** WHEN webhook отвечает `4xx` (кроме 429) THEN `WebhookSender.send` бросает
  `PermanentWebhookError`; `5xx`/`429`/timeout → `TransientWebhookError` после 3 попыток.
- **R-7.** WHEN retry-счётчик исчерпан (`retry_count + 1 >= MAX_ATTEMPTS`) THEN
  `decide_route` возвращает `("dlq", None, reason)`.
- **R-8.** WHEN `lifespan` стартует при `settings.DEBUG is False` THEN вызывается
  `setup_telemetry` + `FastAPIInstrumentor.instrument_app`; при остановке — `shutdown_telemetry`.

## 7. Риски

| Риск | Митигация |
|---|---|
| Потеря поведения из-за смены транзакционных границ | Явные точки `commit()` в обработке (4.5), `expire_on_commit=False` |
| `created_at` = None при построении ответа | `flush + refresh` в `PaymentQuery.create` (4.4) |
| Diff схемы БД после слияния моделей | `alembic revision --autogenerate` = пустая миграция (AC-3) |
| Перепутаны snake_case/camelCase в схемах | Только `payment_schemas.py` на `BaseModel`; фиксированный список ключей (4.1) |
| Тесты падают из-за смены импортов/имён | Импорты обновляются по файлам, ассерты не трогаются; прогон после каждой фазы |
| `frozen=True` + module-level `settings` ломает тесты, меняющие env в рантайме | conftest задаёт env **до** импорта `config`; `test_config_validation` строит `Settings(...)` явно |
| OTel требует endpoint в проде | `DEBUG=True` в docker-compose отключает телеметрию (5 сервисов без коллектора) |
