# Tasks: Рефакторинг payment-system под кодстайл Dima

**Порядок обязателен.** Каждая задача — атомарная (2–5 мин). Проверка (Verify) после
каждой задачи. Единица проверки юнитов — `pytest tests/unit` (без Docker), интеграций —
`pytest tests/integration -m integration` (нужен Docker). Финальный гейт — `black --check .`.

---

## Фаза 0 — Scaffold + зависимости + config

### 0.1. Создать скелет пакета `src/` и `telemetry/`
- Создать пустые `src/__init__.py`, `src/database/__init__.py`,
  `src/dependencies/__init__.py`, `src/handlers/__init__.py`, `src/routers/__init__.py`,
  `src/schemas/__init__.py`, `src/services/__init__.py`, `src/services/queries/__init__.py`,
  `src/services/uow/__init__.py`, `src/utils/__init__.py`, `telemetry/__init__.py`.
- Verify: `python -c "import src, telemetry"` проходит.

### 0.2. Переписать `pyproject.toml`
- `requires-python = ">=3.13"`. Добавить `[build-system]` (hatchling) и
  `[tool.hatch.build.targets.wheel.packages] = ["src"]`.
- Зависимости с закреплёнными диапазонами: `fastapi`, `uvicorn[standard]`, `gunicorn`,
  `pydantic`, `pydantic-settings`, `sqlalchemy[asyncio]`, `asyncpg`, `alembic`,
  `faststream[rabbit]`, `httpx`, `opentelemetry-exporter-otlp-proto-grpc`,
  `opentelemetry-instrumentation-fastapi`, `opentelemetry-instrumentation-logging`,
  `opentelemetry-sdk`.
- Удалить `structlog`, `ruff` и `[tool.ruff*]`. Dev: `black`, `pytest`, `pytest-asyncio`,
  `testcontainers[postgres,rabbitmq]`.
- `[tool.pytest.ini_options]`: `asyncio_mode="auto"`, `testpaths=["tests"]`,
  `markers=["integration: requires docker"]`.
- Verify: `uv sync --all-extras` (или `uv lock`) проходит без ошибок.

### 0.3. Сгенерировать `uv.lock` и `.python-version`
- Выполнить `uv lock`; создать `.python-version` с `3.13`.
- Verify: `uv.lock` создан; `uv run python -V` → `3.13.x`.

### 0.4. Обновить `.env.example`
- Ключи: `DATABASE_URL`, `RABBITMQ_URL`, `API_KEY`, `WEBHOOK_SIGNING_SECRET`,
  `WEBHOOK_TIMEOUT`, `OUTBOX_POLL_INTERVAL`, `OUTBOX_BATCH_SIZE`, `GATEWAY_MIN_DELAY`,
  `GATEWAY_MAX_DELAY`, `GATEWAY_SUCCESS_RATE`, `DEBUG=true`, `SERVICE_NAME=payment-system`,
  `OTLP_ENDPOINT=`.
- Verify: визуальная сверка с текущими значениями по умолчанию (без изменений значений).

### 0.5. Создать `config.py`
- `Settings(BaseSettings)` с `SettingsConfigDict(env_file=".env", case_sensitive=False,
  frozen=True, extra="ignore")`.
- UPPER_SNAKE-поля: `DEBUG: bool = True`, `SERVICE_NAME: str = "payment-system"`,
  `OTLP_ENDPOINT: str = ""`, `DATABASE_URL: str = ...`, `RABBITMQ_URL: str = ...`,
  `API_KEY: str = "dev-api-key"`, `WEBHOOK_SIGNING_SECRET: str = "dev-webhook-secret"`,
  `WEBHOOK_TIMEOUT: float = 10.0`, `OUTBOX_POLL_INTERVAL: float = 1.0`,
  `OUTBOX_BATCH_SIZE: int = 100`, `GATEWAY_MIN_DELAY: float = 2.0`,
  `GATEWAY_MAX_DELAY: float = 5.0`, `GATEWAY_SUCCESS_RATE: float = Field(default=0.9, ge=0.0, le=1.0)`.
- Module-level `settings = Settings()`. Удалить `lru_cache`/`get_settings`.
- Verify: `python -c "from config import settings; print(settings.API_KEY)"`.

### 0.6. Адаптировать `tests/unit/test_config_validation.py`
- Импорт `from config import Settings`. Конструктор `Settings(GATEWAY_SUCCESS_RATE=0.9, ...)`.
- Ассерты: `s.GATEWAY_SUCCESS_RATE == 0.9`, `s.OUTBOX_BATCH_SIZE == 100`.
- Verify: `pytest tests/unit/test_config_validation.py`.

---

## Фаза 1 — Database

### 1.1. Создать `src/database/db_settings.py`
- `engine = create_async_engine(settings.DATABASE_URL, echo=False, poolclass=NullPool)`.
- `SessionLocal = sessionmaker(autocommit=False, autoflush=False, class_=AsyncSession,
  bind=engine, expire_on_commit=False)`.
- Re-export `Base` (импорт из `src.database.models`).
- Verify: `python -c "from src.database.db_settings import engine, SessionLocal"`.

### 1.2. Создать `src/database/models.py`
- Слить: `Base(DeclarativeBase)`, `Currency(StrEnum)`, `PaymentStatus(StrEnum)`,
  `Payment`, `OutboxMessage`.
- Сохранить дословно: `__tablename__`, колонки, `CheckConstraint("amount > 0",
  name="ck_payments_amount_positive")`, `SAEnum(..., name="currency"/"payment_status",
  values_callable=...)`, `unique=True, index=True` на `idempotency_key`,
  partial index `ix_outbox_unpublished`, поле `metadata_` → колонка `"metadata"`.
- Verify: `python -c "from src.database.models import Base, Payment, OutboxMessage, Currency, PaymentStatus"`.

### 1.3. Обновить `alembic/env.py`
- Импорты: `from config import settings`, `from src.database.models import Base`.
- `config.set_main_option("sqlalchemy.url", settings.DATABASE_URL)`.
- Verify: `alembic upgrade head` против тестовой БД проходит (или `alembic check`).

### 1.4. Проверить отсутствие diff схемы (регрессионный барьер)
- На пустой БД выполнить `alembic upgrade head`, затем `alembic revision --autogenerate`.
- Verify: создаётся пустая миграция (нет изменений), её удалить (не коммитить).

---

## Фаза 2 — Utils

### 2.1. Создать `src/utils/exceptions.py`
- `NotFoundError(Exception)` и `AlreadyExistsError(Exception)` с `__init__(self, value: str)`
  (как в стиле). Sphinx-докстринги на русском.
- Verify: импорт проходит.

### 2.2. Создать `src/utils/http_utils.py`
- Перенести `sign_payload(secret, body)` (HMAC-SHA256), `PermanentWebhookError`,
  `TransientWebhookError`, `WebhookSender` (как есть: `build_body`, `send` с retry 1s/2s,
  классификация 4xx/5xx/429, заголовки `X-Signature`/`X-Timestamp`).
- Verify: `python -c "from src.utils.http_utils import WebhookSender, sign_payload"`.

---

## Фаза 3 — Schemas

### 3.1. Создать `src/schemas/base.py`
- `CamelSchema(BaseModel)` с `AliasGenerator(validation_alias=to_camel,
  serialization_alias=to_camel)`, `populate_by_name=True`.
- Verify: импорт проходит.

### 3.2. Создать `src/schemas/payment_schemas.py`
- `PaymentCreateRequest(BaseModel)` (snake_case: `amount`, `currency`, `description`,
  `metadata`, `webhook_url`; валидатор https) — **не** CamelSchema (см. design 4.1).
- `PaymentCreateResponse(BaseModel)` (`payment_id`, `status`, `created_at`).
- `PaymentDetailResponse(BaseModel)` (`payment_id`, `amount`, `currency`, `description`,
  `metadata`, `status`, `created_at`, `processed_at`, `webhook_delivered_at`).
- Verify: `python -c "from src.schemas.payment_schemas import PaymentCreateRequest"`.

---

## Фаза 4 — Queries + UoW + gateway

### 4.1. Создать `src/services/queries/payment_queries.py`
- `PaymentQuery(session)`:
  - `get_by_idempotency_key(key) -> Payment | None`.
  - `get_by_id(payment_id) -> Payment` (бросает `NotFoundError`).
  - `create(data) -> Payment`: `_to_decimal` для amount, `Payment(**data)`, `add`, `flush`;
    на `IntegrityError` → `rollback` + `raise AlreadyExistsError(...)`; после flush →
    `refresh(payment)` (заполнить `created_at`).
  - `mark_processed(payment, status) -> bool`: условный `UPDATE ... WHERE status='pending'
    RETURNING id`, синхронизация `payment.status = PaymentStatus(status)` + `processed_at`.
  - `mark_webhook_delivered(payment)`.
  - `increment_webhook_attempts(payment)` (`webhook_attempts + 1`).
- Verify: `python -c "from src.services.queries.payment_queries import PaymentQuery"`.

### 4.2. Создать `src/services/queries/outbox_queries.py`
- `OutboxQuery(session)`:
  - `get_unpublished(batch_size)`: `select(OutboxMessage).where(published_at IS NULL)
    .order_by(created_at).limit(batch_size).with_for_update(skip_locked=True)`.
  - `mark_published(row)`: `row.published_at = datetime.now(UTC)`.
- Verify: импорт проходит.

### 4.3. Создать `src/services/uow/base.py`
- `AbstractUnitOfWork(ABC)` с `__aenter__` (`session.begin()`), `__aexit__`
  (rollback при ошибке / commit), `commit()`, `rollback()`.
- Verify: импорт проходит.

### 4.4. Создать `src/services/uow/payments_uow.py`
- `PaymentsUnitOfWork(AbstractUnitOfWork)`: `self.payment = PaymentQuery(session)`,
  `self.outbox = OutboxQuery(session)`.
- Depends-фабрика `get_payments_uow(session=Depends(get_async_session))` с
  `async with PaymentsUnitOfWork(session) as uow: yield uow`.
- Verify: импорт проходит.

### 4.5. Создать `src/services/gateway.py`
- `PaymentGateway` (как есть), но `from config import settings`, поля `settings.GATEWAY_MIN_DELAY`
  и т.д. Сигнатура `__init__` с опциональными переопределениями — сохранить.
- Verify: `pytest tests/unit/test_gateway.py` (после обновления импорта в 9.5).

### 4.6. Заполнить re-export в `__init__.py`
- `src/services/queries/__init__.py` → `PaymentQuery`, `OutboxQuery`.
- `src/services/uow/__init__.py` → `AbstractUnitOfWork`, `PaymentsUnitOfWork`, `get_payments_uow`.
- Verify: `python -c "from src.services.queries import PaymentQuery"`.

---

## Фаза 5 — Dependencies + Handlers + Routers

### 5.1. Создать `src/dependencies/auth.py`
- `require_api_key(x_api_key: Annotated[str | None, Header()] = None)`: constant-time
  сравнение `settings.API_KEY`; `401` при несовпадении.
- Verify: импорт проходит.

### 5.2. Создать `src/dependencies/db.py`
- `async def get_async_session() -> AsyncGenerator[AsyncSession, None]` с
  `async with SessionLocal() as session: yield session`.
- Verify: импорт проходит.

### 5.3. Создать `src/handlers/payment_handlers.py`
- `PaymentHandler(uow)`:
  - `create_payment(payload: dict, idempotency_key: str | None) -> PaymentCreateResponse`:
    если нет ключа → `HTTPException(422)`; `get_by_idempotency_key` → 202-ответ;
    `try: payment = uow.payment.create(...); uow.outbox.add(payment)`;
    `except AlreadyExistsError`: повторный `get_by_idempotency_key` → 202-ответ, иначе
    `HTTPException(409, "Concurrent request with the same Idempotency-Key")`.
  - `get_payment(payment_id) -> PaymentDetailResponse`: `get_by_id` + `except NotFoundError → 404`.
  - `_build_create_response`, `_build_detail_response` (маппинг ORM → Pydantic, `metadata_` → `metadata`).
  - `get_payment_handler(uow=Depends(get_payments_uow))` Depends-фабрика.
- Verify: `python -c "from src.handlers.payment_handlers import PaymentHandler"`.

### 5.4. Создать `src/handlers/processing_handlers.py`
- `ProcessingResult` (dataclass, `action`/`reason`), константы `ACTION_DONE/RETRY/DLQ`.
- `ProcessingHandler(uow, gateway, webhook)` с `process(payment_id) -> ProcessingResult`:
  явные точки `await self.uow.commit()` (см. design 4.5). Логика эквивалентна
  `ProcessingService.process`.
- Verify: импорт проходит.

### 5.5. Создать `src/routers/payments_routers.py`
- Тонкий роут: `POST ""` (202) и `GET "/{payment_id}"` — только `Depends` +
  вызов `handler`, `payload.model_dump()` для тела.
- Verify: импорт проходит.

### 5.6. Создать `src/routers/health_routers.py`
- `GET /health` → `{"status": "ok"}`; `GET /health/ready` → `SELECT 1` через
  `Depends(get_async_session)` → `{"status": "ready"}`.
- Verify: импорт проходит.

### 5.7. Заполнить `src/handlers/__init__.py`
- Re-export `PaymentHandler`, `get_payment_handler`, `ProcessingHandler`,
  `ProcessingResult`, `ACTION_*`.
- Verify: `python -c "from src.handlers import PaymentHandler, ProcessingHandler"`.

---

## Фаза 6 — Entry point + telemetry + gunicorn

### 6.1. Создать `telemetry/setup_telemetry.py`
- `setup_telemetry(service_name, otlp_endpoint)` (traces/metrics/logs → OTLP gRPC
  insecure, `LoggingInstrumentor().instrument(set_logging_format=True)`) и
  `shutdown_telemetry()` — по шаблону стиля.
- Verify: импорт проходит.

### 6.2. Создать `main.py`
- `lifespan` (asynccontextmanager): при `settings.DEBUG is False` → `setup_telemetry`
  + `FastAPIInstrumentor.instrument_app(app)`; на выходе `shutdown_telemetry`.
- `create_app()`: `FastAPI(title="Payment System", version="0.1.0", lifespan=lifespan)`,
  `include_router(health_router)`, `include_router(payments_router)`.
- Module-level `app = create_app()`.
- Verify: `python -c "from main import app"`.

### 6.3. Создать `gunicorn.conf.py`
- `bind = "0.0.0.0:8000"`, `workers = 10`, `worker_class = "uvicorn.workers.UvicornWorker"`,
  `timeout = 120`, `graceful_timeout = 30`.
- Verify: `gunicorn --check-config main:app` (или синтаксический импорт).

---

## Фаза 7 — Messaging + Workers

### 7.1. Создать `src/messaging/broker.py`
- `broker = RabbitBroker(settings.RABBITMQ_URL)`.
- Verify: импорт проходит.

### 7.2. Создать `src/messaging/topology.py`
- Перенос дословно (обмены/очереди/TTL/DLX/`declare_topology`), только импорт брокера
  не нужен (остаётся как есть, зависит от `RabbitBroker` параметром).
- Verify: импорт проходит.

### 7.3. Создать `src/messaging/publisher.py`
- Перенос дословно (`publish_new`, `publish_retry`, `publish_dlq`, `_message`, `EVENT_TYPE`).
- Verify: импорт проходит.

### 7.4. Создать `src/messaging/consumer.py`
- `PaymentEvent(BaseModel)`, `decide_route` (дословно).
- `handle_new_payment`: вместо `ProcessingService`/`SqlPaymentRepository` строить
  `PaymentsUnitOfWork(session)` + `ProcessingHandler(uow, PaymentGateway(), webhook)`.
- `_declare_topology`/`_run`/`main` — как есть, `logging.getLogger(__name__)` вместо
  `structlog.get_logger`.
- Verify: `python -m src.messaging.consumer --help`-подобный импорт (без запуска брокера).

### 7.5. Создать `src/workers/outbox_worker.py`
- `publish_batch(session, publish, batch_size)` → переписать на `OutboxQuery`
  (`get_unpublished` + `mark_published`) внутри `async with PaymentsUnitOfWork(session)`.
- Сохранить сигнатуру `publish_batch(session, publish, batch_size)` (её тестирует
  `test_outbox_worker`). `main`/`_run` — `logging` вместо structlog, `settings.OUTBOX_*`.
- Verify: импорт проходит.

### 7.6. Обновить `src/messaging/__init__.py`, `src/workers/__init__.py`
- Re-export публичных объектов (по необходимости для тестов).
- Verify: `python -c "from src.messaging import publisher, topology"`.

---

## Фаза 8 — Docker

### 8.1. Переписать `Dockerfile`
- Multi-stage `python:3.13-slim`: builder (`gcc`, `libpq-dev`, `pip install uv`,
  `uv sync --no-dev`), runtime (`libpq5`, `COPY --from=builder .venv`, `COPY . .`,
  `ENV PATH`, `CMD ["gunicorn", "main:app", "-c", "gunicorn.conf.py"]`).
- Verify: `docker build .` проходит.

### 8.2. Обновить `docker-compose.yml`
- Команды: `api` → `sh -c "alembic upgrade head && gunicorn main:app -c gunicorn.conf.py"`;
  `outbox-worker` → `python -m src.workers.outbox_worker`; `consumer` →
  `python -m src.messaging.consumer`.
- env в UPPER_SNAKE (+ `DEBUG=true`). 5 сервисов и healthcheck — без изменений.
- Verify: `docker compose config` валиден.

### 8.3. Обновить `.dockerignore`
- Убедиться: исключены `.venv`, `__pycache__`, `.git`, `tests`, `docs`; **не** исключены
  `pyproject.toml`, `uv.lock`, `src/`, `alembic/`, `config.py`, `main.py`.
- Verify: визуальная сверка.

---

## Фаза 9 — Адаптация тестов (регрессионный гейт, ассерты без изменений)

### 9.1. Обновить `tests/conftest.py`
- `engine`: `from src.database.models import Base`.
- `client`: `from config import settings`; `from src.dependencies.db import get_async_session`;
  `from main import app`; override `get_async_session`. Env `API_KEY=test-key` задаётся
  **до** импорта `config`.
- Verify: `pytest tests/unit` (без Docker) — уже пройденные юниты.

### 9.2. Обновить `tests/integration/test_api_payments.py`
- Импорты не нужны (тест через `client`); ассерты (401/202/200/404/422, snake_case
  `payment_id`) — без изменений.
- Verify: `pytest tests/integration/test_api_payments.py -m integration`.

### 9.3. Адаптировать `tests/integration/test_payment_service.py`
- Заменить вызовы `create_payment(...)` на `PaymentHandler` + `PaymentsUnitOfWork`
  (создание платежа + outbox, дубль ключа, `get_by_id`→None). Ассерты по `status`,
  `aggregate_id`, `payload` — без изменений.
- Verify: `pytest tests/integration/test_payment_service.py -m integration`.

### 9.4. Адаптировать `tests/integration/test_outbox_worker.py`
- `from src.workers.outbox_worker import publish_batch`; `create_payment` → через handler/UoW.
  Ассерты по `processed == 1`, `published`, `published_at is not None` — без изменений.
- Verify: `pytest tests/integration/test_outbox_worker.py -m integration`.

### 9.5. Адаптировать `tests/integration/test_repository.py`
- `SqlPaymentRepository` → `PaymentQuery.mark_processed` (через UoW). Ассерт
  `isinstance(payment.status, PaymentStatus)` и `payment.status.value == "succeeded"` —
  без изменений.
- Verify: `pytest tests/integration/test_repository.py -m integration`.

### 9.6. Адаптировать `tests/integration/test_processing_e2e.py`
- `ProcessingService` → `ProcessingHandler`; `SqlPaymentRepository` → `PaymentQuery`/UoW.
  `WebhookSender` импорт из `src.utils.http_utils`. Ассерты по `ACTION_DONE`, `status`,
  `processed_at`, `webhook_delivered_at` — без изменений.
- Verify: `pytest tests/integration/test_processing_e2e.py -m integration`.

### 9.7. Адаптировать `tests/integration/test_consumer_publish.py`
- `from src.messaging import publisher, topology`. Ассерты (`x-retry-count`, `x-reason`,
  очереди) — без изменений.
- Verify: `pytest tests/integration/test_consumer_publish.py -m integration`.

### 9.8. Адаптировать unit-тесты (импорты)
- `test_gateway.py` → `from src.services.gateway import PaymentGateway`.
- `test_processing.py` → `from src.handlers.processing_handlers import ...`;
  `FakeRepo` → fake `uow`/`query` с методами `get_by_id`, `mark_processed`,
  `mark_webhook_delivered`, `increment_webhook_attempts`, `commit`.
- `test_routing.py` → `from src.messaging.consumer import decide_route`;
  `from src.handlers.processing_handlers import ACTION_*, ProcessingResult`.
- `test_signature.py` → `from src.utils.http_utils import sign_payload`.
- `test_webhook.py` → `from src.utils.http_utils import WebhookSender, ...`.
- Verify: `pytest tests/unit` — все юниты зелёные.

### 9.9. Удалить старый пакет `app/`
- Удалить `app/`, `payment_system.egg-info/`, `.ruff_cache/`.
- Verify: `grep -r "from app" tests/ src/ main.py config.py alembic/` не находит совпадений.

---

## Фаза 10 — Финальный регрессионный гейт

### 10.1. Полный прогон юнитов
- `pytest tests/unit` → все зелёные.

### 10.2. Полный прогон интеграций
- `pytest tests/integration -m integration` → все зелёные (нужен Docker).

### 10.3. Форматирование
- `black .`; затем `black --check .` → без изменений.

### 10.4. Alembic-гейт
- `alembic upgrade head` + `alembic revision --autogenerate` → пустая миграция.

### 10.5. Docker smoke
- `docker compose up --build`; проверить `/health` и создание/чтение платежа;
  retry→DLQ smoke из README (по возможности).
