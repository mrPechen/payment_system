# Async Payment Processing Service — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a FastAPI + RabbitMQ microservice that accepts payments, publishes them via the Outbox pattern, processes them asynchronously with retry/DLQ, and notifies clients via HMAC-signed webhooks.

**Architecture:** API writes `payment` + `outbox` in one DB transaction. A separate outbox-worker polls unpublished rows and publishes to `payments.new`. A single consumer emulates the gateway, updates status with a conditional (idempotent) UPDATE, sends the signed webhook, and routes technical failures to TTL-based retry queues (2s, 4s) or, after 3 attempts, to `payments.dlq`.

**Tech Stack:** Python 3.12, FastAPI, Pydantic v2, SQLAlchemy 2.0 async + asyncpg, Alembic, FastStream (RabbitMQ), httpx, structlog, Docker Compose, pytest + pytest-asyncio + testcontainers.

**Spec:** `docs/superpowers/specs/2026-10-15-payment-service-design.md`

---

## File Structure

```
pyproject.toml            # deps + pytest/ruff config
.env.example              # documented env vars
alembic.ini               # alembic config
alembic/env.py            # async engine wiring
alembic/versions/         # migrations
Dockerfile                # one image, multiple commands
docker-compose.yml        # postgres, rabbitmq, api, outbox-worker, consumer
app/
  __init__.py
  config.py               # pydantic-settings
  logging.py              # structlog setup
  db.py                   # async engine + session factory + get_session
  main.py                 # FastAPI app factory + health
  models/
    __init__.py
    base.py               # DeclarativeBase
    enums.py              # Currency, PaymentStatus
    payment.py            # Payment
    outbox.py             # OutboxMessage
  schemas/
    __init__.py
    payment.py            # request/response models
  services/
    __init__.py
    signature.py          # HMAC signing
    gateway.py            # PaymentGateway emulation
    webhook.py            # WebhookSender (httpx + retry + sign)
    payment_service.py    # create_payment (idempotent) + get_payment
    repository.py         # SqlPaymentRepository (consumer DB access)
    processing.py         # ProcessingService (consumer core, pure routing)
  api/
    __init__.py
    deps.py               # require_api_key
    routes/
      __init__.py
      health.py
      payments.py
  messaging/
    __init__.py
    broker.py             # RabbitBroker instance
    topology.py           # exchanges/queues declarations
    publisher.py          # publish_new / publish_retry / publish_dlq
    consumer.py           # FastStream router + handler + app
  workers/
    __init__.py
    outbox_worker.py      # polling loop entrypoint
tests/
  __init__.py
  conftest.py             # containers + app/db fixtures
  unit/
    test_config_validation.py
    test_signature.py
    test_gateway.py
    test_webhook.py
    test_processing.py
    test_routing.py
  integration/
    test_payment_service.py
    test_api_payments.py
    test_outbox_worker.py
    test_consumer_publish.py
```

---

## Task 1: Project scaffold & dependencies

**Files:**
- Create: `pyproject.toml`, `.gitignore`, `.env.example`
- Create empty `__init__.py` in every package dir listed in File Structure

- [ ] **Step 1: Write `pyproject.toml`**

```toml
[project]
name = "payment-system"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
    "fastapi>=0.115",
    "uvicorn[standard]>=0.30",
    "pydantic>=2.7",
    "pydantic-settings>=2.2",
    "sqlalchemy[asyncio]>=2.0.30",
    "asyncpg>=0.29",
    "alembic>=1.13",
    "faststream[rabbit]>=0.5",
    "httpx>=0.27",
    "structlog>=24.1",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.2",
    "pytest-asyncio>=0.23",
    "testcontainers[postgres,rabbitmq]>=4.5",
    "ruff>=0.5",
]

[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]
markers = ["integration: requires docker"]

[tool.ruff]
line-length = 100
target-version = "py312"

[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B"]
```

- [ ] **Step 2: Write `.gitignore`**

```
__pycache__/
*.pyc
.venv/
.env
.pytest_cache/
.ruff_cache/
*.egg-info/
```

- [ ] **Step 3: Write `.env.example`**

```dotenv
DATABASE_URL=postgresql+asyncpg://payments:payments@localhost:5432/payments
RABBITMQ_URL=amqp://guest:guest@localhost:5672/
API_KEY=dev-api-key
WEBHOOK_SIGNING_SECRET=dev-webhook-secret
WEBHOOK_TIMEOUT=10
OUTBOX_POLL_INTERVAL=1
OUTBOX_BATCH_SIZE=100
GATEWAY_MIN_DELAY=2
GATEWAY_MAX_DELAY=5
GATEWAY_SUCCESS_RATE=0.9
```

- [ ] **Step 4: Create package `__init__.py` files**

Create empty `__init__.py` in: `app/`, `app/models/`, `app/schemas/`, `app/services/`, `app/api/`, `app/api/routes/`, `app/messaging/`, `app/workers/`, `tests/`, `tests/unit/`, `tests/integration/`.

- [ ] **Step 5: Install and verify**

Run: `python3.12 -m venv .venv && . .venv/bin/activate && pip install -e ".[dev]"`
Expected: install succeeds; `python -c "import fastapi, faststream, sqlalchemy"` prints nothing.

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml .gitignore .env.example app tests
git commit -m "chore: project scaffold and dependencies"
```

---

## Task 2: Settings / config

**Files:**
- Create: `app/config.py`
- Test: `tests/unit/test_config_validation.py`

- [ ] **Step 1: Write the failing test**

```python
from app.config import Settings


def test_success_rate_is_validated():
    s = Settings(gateway_success_rate=0.9, api_key="k", webhook_signing_secret="s")
    assert s.gateway_success_rate == 0.9


def test_batch_size_defaults():
    s = Settings(api_key="k", webhook_signing_secret="s")
    assert s.outbox_batch_size == 100
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_config_validation.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'app.config'`

- [ ] **Step 3: Write minimal implementation**

```python
from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5432/payments"
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"

    api_key: str = "dev-api-key"
    webhook_signing_secret: str = "dev-webhook-secret"
    webhook_timeout: float = 10.0

    outbox_poll_interval: float = 1.0
    outbox_batch_size: int = 100

    gateway_min_delay: float = 2.0
    gateway_max_delay: float = 5.0
    gateway_success_rate: float = Field(default=0.9, ge=0.0, le=1.0)


@lru_cache
def get_settings() -> Settings:
    return Settings()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_config_validation.py -v`
Expected: PASS (2 passed)

- [ ] **Step 5: Commit**

```bash
git add app/config.py tests/unit/test_config_validation.py
git commit -m "feat: settings via pydantic-settings"
```

---

## Task 3: Structured logging

**Files:**
- Create: `app/logging.py`

- [ ] **Step 1: Write `app/logging.py`**

```python
import logging

import structlog


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str):
    return structlog.get_logger(name)
```

- [ ] **Step 2: Verify**

Run: `python -c "from app.logging import configure_logging, get_logger; configure_logging(); get_logger('x').info('ok', payment_id='1')"`
Expected: prints a JSON line containing `"payment_id": "1"`.

- [ ] **Step 3: Commit**

```bash
git add app/logging.py
git commit -m "feat: structured logging with structlog"
```

---

## Task 4: Database engine & session

**Files:**
- Create: `app/db.py`

- [ ] **Step 1: Write `app/db.py`**

```python
from collections.abc import AsyncIterator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.config import get_settings

settings = get_settings()

engine = create_async_engine(settings.database_url, pool_pre_ping=True, future=True)

SessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def get_session() -> AsyncIterator[AsyncSession]:
    async with SessionLocal() as session:
        yield session
```

- [ ] **Step 2: Verify**

Run: `python -c "from app.db import engine, SessionLocal, get_session"`
Expected: no error.

- [ ] **Step 3: Commit**

```bash
git add app/db.py
git commit -m "feat: async database engine and session factory"
```

---

## Task 5: Enums & ORM models

**Files:**
- Create: `app/models/base.py`, `app/models/enums.py`, `app/models/payment.py`, `app/models/outbox.py`
- Modify: `app/models/__init__.py`

- [ ] **Step 1: Write `app/models/base.py`**

```python
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass
```

- [ ] **Step 2: Write `app/models/enums.py`**

```python
import enum


class Currency(str, enum.Enum):
    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class PaymentStatus(str, enum.Enum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
```

- [ ] **Step 3: Write `app/models/payment.py`**

```python
import uuid
from datetime import datetime
from decimal import Decimal

from sqlalchemy import CheckConstraint, DateTime, Numeric, Text, func
from sqlalchemy import Enum as SAEnum
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base
from app.models.enums import Currency, PaymentStatus


class Payment(Base):
    __tablename__ = "payments"
    __table_args__ = (CheckConstraint("amount > 0", name="ck_payments_amount_positive"),)

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False)
    currency: Mapped[Currency] = mapped_column(
        SAEnum(Currency, name="currency"), nullable=False
    )
    description: Mapped[str] = mapped_column(Text, nullable=False, default="")
    metadata_: Mapped[dict] = mapped_column(
        "metadata", JSONB, nullable=False, default=dict
    )
    status: Mapped[PaymentStatus] = mapped_column(
        SAEnum(PaymentStatus, name="payment_status"),
        nullable=False,
        default=PaymentStatus.PENDING,
    )
    idempotency_key: Mapped[str] = mapped_column(
        Text, nullable=False, unique=True, index=True
    )
    webhook_url: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    processed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    webhook_delivered_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    webhook_attempts: Mapped[int] = mapped_column(nullable=False, default=0)
```

- [ ] **Step 4: Write `app/models/outbox.py`**

```python
import uuid
from datetime import datetime

from sqlalchemy import BigInteger, DateTime, Index, Text, func, text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


class OutboxMessage(Base):
    __tablename__ = "outbox"
    __table_args__ = (
        Index(
            "ix_outbox_unpublished",
            "created_at",
            postgresql_where=text("published_at IS NULL"),
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    aggregate_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    published_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
```

- [ ] **Step 5: Write `app/models/__init__.py`**

```python
from app.models.base import Base
from app.models.enums import Currency, PaymentStatus
from app.models.outbox import OutboxMessage
from app.models.payment import Payment

__all__ = ["Base", "Currency", "PaymentStatus", "OutboxMessage", "Payment"]
```

- [ ] **Step 6: Verify table registration**

Run: `python -c "import app.models as m; print(sorted(m.Base.metadata.tables))"`
Expected: `['outbox', 'payments']`

- [ ] **Step 7: Commit**

```bash
git add app/models
git commit -m "feat: enums and ORM models (payments, outbox)"
```

---

## Task 6: Alembic initial migration

**Files:**
- Create: `alembic.ini`, `alembic/env.py`, `alembic/script.py.mako`, `alembic/versions/0001_initial.py`

- [ ] **Step 1: Write `alembic.ini`**

```ini
[alembic]
script_location = alembic
prepend_sys_path = .
version_path_separator = os

[loggers]
keys = root,sqlalchemy,alembic

[handlers]
keys = console

[formatters]
keys = generic

[logger_root]
level = WARNING
handlers = console
qualname =

[logger_sqlalchemy]
level = WARNING
handlers =
qualname = sqlalchemy.engine

[logger_alembic]
level = INFO
handlers =
qualname = alembic

[handler_console]
class = StreamHandler
args = (sys.stderr,)
level = NOTSET
formatter = generic

[formatter_generic]
format = %(levelname)-5.5s [%(name)s] %(message)s
datefmt = %H:%M:%S
```

- [ ] **Step 2: Write `alembic/script.py.mako`**

```mako
"""${message}

Revision ID: ${up_revision}
Revises: ${down_revision | comma,n}
Create Date: ${create_date}
"""
from alembic import op
import sqlalchemy as sa
${imports if imports else ""}

revision = ${repr(up_revision)}
down_revision = ${repr(down_revision)}
branch_labels = ${repr(branch_labels)}
depends_on = ${repr(depends_on)}


def upgrade() -> None:
    ${upgrades if upgrades else "pass"}


def downgrade() -> None:
    ${downgrades if downgrades else "pass"}
```

- [ ] **Step 3: Write `alembic/env.py`**

```python
import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy import pool
from sqlalchemy.ext.asyncio import async_engine_from_config

from app.config import get_settings
from app.models import Base

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

config.set_main_option("sqlalchemy.url", get_settings().database_url)
target_metadata = Base.metadata


def run_migrations_offline() -> None:
    context.configure(
        url=config.get_main_option("sqlalchemy.url"),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection) -> None:
    context.configure(
        connection=connection, target_metadata=target_metadata, compare_type=True
    )
    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)
    await connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())
```

- [ ] **Step 4: Write `alembic/versions/0001_initial.py`**

```python
"""initial schema

Revision ID: 0001
Revises:
Create Date: 2026-10-15
"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None

currency = postgresql.ENUM("RUB", "USD", "EUR", name="currency")
payment_status = postgresql.ENUM(
    "pending", "succeeded", "failed", name="payment_status"
)


def upgrade() -> None:
    currency.create(op.get_bind(), checkfirst=True)
    payment_status.create(op.get_bind(), checkfirst=True)

    op.create_table(
        "payments",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("amount", sa.Numeric(18, 2), nullable=False),
        sa.Column("currency", currency, nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("metadata", postgresql.JSONB(), nullable=False),
        sa.Column("status", payment_status, nullable=False, server_default="pending"),
        sa.Column("idempotency_key", sa.Text(), nullable=False),
        sa.Column("webhook_url", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("webhook_delivered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("webhook_attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.CheckConstraint("amount > 0", name="ck_payments_amount_positive"),
        sa.UniqueConstraint("idempotency_key", name="uq_payments_idempotency_key"),
    )
    op.create_index(
        "ix_payments_idempotency_key", "payments", ["idempotency_key"], unique=True
    )

    op.create_table(
        "outbox",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("aggregate_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "ix_outbox_unpublished",
        "outbox",
        ["created_at"],
        postgresql_where=sa.text("published_at IS NULL"),
    )


def downgrade() -> None:
    op.drop_table("outbox")
    op.drop_index("ix_payments_idempotency_key", table_name="payments")
    op.drop_table("payments")
    payment_status.drop(op.get_bind(), checkfirst=True)
    currency.drop(op.get_bind(), checkfirst=True)
```

- [ ] **Step 5: Verify migration against a throwaway postgres**

```bash
docker run -d --rm --name pg-mig -e POSTGRES_USER=payments -e POSTGRES_PASSWORD=payments -e POSTGRES_DB=payments -p 55432:5432 postgres:16-alpine
sleep 6
DATABASE_URL=postgresql+asyncpg://payments:payments@localhost:55432/payments alembic upgrade head
DATABASE_URL=postgresql+asyncpg://payments:payments@localhost:55432/payments alembic downgrade base
docker stop pg-mig
```
Expected: both `upgrade head` and `downgrade base` complete without error.

- [ ] **Step 6: Commit**

```bash
git add alembic.ini alembic
git commit -m "feat: alembic setup and initial migration"
```

---

## Task 7: HMAC signature service

**Files:**
- Create: `app/services/signature.py`
- Test: `tests/unit/test_signature.py`

- [ ] **Step 1: Write the failing test**

```python
import hashlib
import hmac

from app.services.signature import sign_payload


def test_signs_raw_body_with_hmac_sha256():
    body = b'{"a":1}'
    expected = hmac.new(b"secret", body, hashlib.sha256).hexdigest()
    assert sign_payload("secret", body) == expected


def test_signature_is_deterministic():
    assert sign_payload("s", b"x") == sign_payload("s", b"x")


def test_different_secret_changes_signature():
    assert sign_payload("a", b"x") != sign_payload("b", b"x")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_signature.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'app.services.signature'`

- [ ] **Step 3: Write minimal implementation**

```python
import hashlib
import hmac


def sign_payload(secret: str, body: bytes) -> str:
    return hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_signature.py -v`
Expected: PASS (3 passed)

- [ ] **Step 5: Commit**

```bash
git add app/services/signature.py tests/unit/test_signature.py
git commit -m "feat: HMAC-SHA256 webhook signing"
```

---

## Task 8: Gateway emulation

**Files:**
- Create: `app/services/gateway.py`
- Test: `tests/unit/test_gateway.py`

- [ ] **Step 1: Write the failing test**

```python
import time

from app.services.gateway import PaymentGateway


async def test_always_succeeds_when_rate_is_one():
    gw = PaymentGateway(min_delay=0.0, max_delay=0.0, success_rate=1.0)
    assert await gw.process() is True


async def test_always_fails_when_rate_is_zero():
    gw = PaymentGateway(min_delay=0.0, max_delay=0.0, success_rate=0.0)
    assert await gw.process() is False


async def test_delay_within_bounds():
    gw = PaymentGateway(min_delay=0.05, max_delay=0.05, success_rate=1.0)
    start = time.monotonic()
    await gw.process()
    assert time.monotonic() - start >= 0.05
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_gateway.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'app.services.gateway'`

- [ ] **Step 3: Write minimal implementation**

```python
import asyncio
import random

from app.config import get_settings


class PaymentGateway:
    def __init__(
        self,
        min_delay: float | None = None,
        max_delay: float | None = None,
        success_rate: float | None = None,
    ) -> None:
        settings = get_settings()
        self.min_delay = settings.gateway_min_delay if min_delay is None else min_delay
        self.max_delay = settings.gateway_max_delay if max_delay is None else max_delay
        self.success_rate = (
            settings.gateway_success_rate if success_rate is None else success_rate
        )

    async def process(self) -> bool:
        await asyncio.sleep(random.uniform(self.min_delay, self.max_delay))
        return random.random() < self.success_rate
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_gateway.py -v`
Expected: PASS (3 passed)

- [ ] **Step 5: Commit**

```bash
git add app/services/gateway.py tests/unit/test_gateway.py
git commit -m "feat: payment gateway emulation"
```

---

## Task 9: Test fixtures (conftest)

> Must exist before Task 11 (integration tests). Imports of `app.main`/`app.models` are deferred inside fixtures, so this file can be created now.

**Files:**
- Create: `tests/conftest.py`

- [ ] **Step 1: Write `tests/conftest.py`**

```python
import os

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

os.environ.setdefault("API_KEY", "test-key")
os.environ.setdefault("WEBHOOK_SIGNING_SECRET", "test-secret")


@pytest.fixture(scope="session")
def postgres_url():
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine") as pg:
        yield pg.get_connection_url().replace("psycopg2", "asyncpg")


@pytest.fixture(scope="session")
def rabbit_url():
    from testcontainers.rabbitmq import RabbitMQContainer

    with RabbitMQContainer("rabbitmq:3-management-alpine") as rb:
        yield rb.get_amqp_url()


@pytest.fixture
async def engine(postgres_url):
    from app.models import Base

    eng = create_async_engine(postgres_url)
    async with eng.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    yield eng
    await eng.dispose()


@pytest.fixture
async def db_session(engine):
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as session:
        yield session


@pytest.fixture
async def client(engine):
    from app.config import get_settings
    from app.db import get_session
    from app.main import app

    os.environ["API_KEY"] = "test-key"
    get_settings.cache_clear()

    maker = async_sessionmaker(engine, expire_on_commit=False)

    async def override_get_session():
        async with maker() as session:
            yield session

    app.dependency_overrides[get_session] = override_get_session
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        yield ac
    app.dependency_overrides.clear()
```

- [ ] **Step 2: Verify collection (containers start lazily per fixture, so unit tests still work offline)**

Run: `pytest tests/unit -v`
Expected: PASS (all unit tests from Tasks 2, 7, 8).

- [ ] **Step 3: Commit**

```bash
git add tests/conftest.py
git commit -m "test: containers and app/db fixtures"
```

---

## Task 10: Webhook sender (httpx + retry + sign)

**Files:**
- Create: `app/services/webhook.py`
- Test: `tests/unit/test_webhook.py`

- [ ] **Step 1: Write the failing test**

```python
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest

from app.services.signature import sign_payload
from app.services.webhook import (
    PermanentWebhookError,
    TransientWebhookError,
    WebhookSender,
)


def make_payment():
    return SimpleNamespace(
        id=uuid4(),
        status=SimpleNamespace(value="succeeded"),
        amount=Decimal("100.50"),
        currency=SimpleNamespace(value="RUB"),
        processed_at=datetime(2026, 10, 15, tzinfo=timezone.utc),
        webhook_url="https://client.example/hook",
    )


async def test_sends_signed_payload_and_returns():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["body"] = request.content
        seen["sig"] = request.headers["X-Signature"]
        seen["ts"] = request.headers["X-Timestamp"]
        return httpx.Response(200)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    sender = WebhookSender(client=client, secret="sec", retry_delays=(0.0, 0.0))
    await sender.send(make_payment())

    assert seen["sig"] == f"sha256={sign_payload('sec', seen['body'])}"
    assert seen["ts"]


async def test_retries_on_500_then_succeeds():
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(500 if calls["n"] == 1 else 200)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    sender = WebhookSender(client=client, secret="sec", retry_delays=(0.0, 0.0))
    await sender.send(make_payment())
    assert calls["n"] == 2


async def test_permanent_error_on_400():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    sender = WebhookSender(client=client, secret="sec", retry_delays=(0.0, 0.0))
    with pytest.raises(PermanentWebhookError):
        await sender.send(make_payment())


async def test_transient_error_after_all_attempts_exhausted():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    sender = WebhookSender(client=client, secret="sec", retry_delays=(0.0, 0.0))
    with pytest.raises(TransientWebhookError):
        await sender.send(make_payment())
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_webhook.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'app.services.webhook'`

- [ ] **Step 3: Write minimal implementation**

```python
import asyncio
import json
from datetime import datetime, timezone

import httpx

from app.services.signature import sign_payload


class PermanentWebhookError(Exception):
    """Non-retryable delivery failure (e.g. 4xx)."""


class TransientWebhookError(Exception):
    """Retryable delivery failure (e.g. 5xx/timeout) after all attempts."""


class WebhookSender:
    def __init__(
        self,
        client: httpx.AsyncClient,
        secret: str,
        retry_delays: tuple[float, ...] = (1.0, 2.0),
    ) -> None:
        self.client = client
        self.secret = secret
        self.retry_delays = retry_delays

    def build_body(self, payment) -> bytes:
        payload = {
            "payment_id": str(payment.id),
            "status": payment.status.value,
            "amount": str(payment.amount),
            "currency": payment.currency.value,
            "processed_at": payment.processed_at.isoformat()
            if payment.processed_at
            else None,
        }
        return json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()

    async def send(self, payment) -> None:
        body = self.build_body(payment)
        headers = {
            "Content-Type": "application/json",
            "X-Signature": f"sha256={sign_payload(self.secret, body)}",
            "X-Timestamp": datetime.now(timezone.utc).isoformat(),
        }
        last_error: Exception | None = None
        attempts = 1 + len(self.retry_delays)
        for attempt in range(attempts):
            if attempt > 0:
                await asyncio.sleep(self.retry_delays[attempt - 1])
            try:
                response = await self.client.post(
                    payment.webhook_url, content=body, headers=headers
                )
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                last_error = exc
                continue

            if response.status_code == 429 or response.status_code >= 500:
                last_error = TransientWebhookError(f"status {response.status_code}")
                continue
            if response.status_code >= 400:
                raise PermanentWebhookError(f"status {response.status_code}")
            return

        raise TransientWebhookError("all attempts failed") from last_error
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_webhook.py -v`
Expected: PASS (4 passed)

- [ ] **Step 5: Commit**

```bash
git add app/services/webhook.py tests/unit/test_webhook.py
git commit -m "feat: webhook sender with signing and retry classification"
```

---

## Task 11: Payment create service (idempotency + outbox)

**Files:**
- Create: `app/services/payment_service.py`
- Test: `tests/integration/test_payment_service.py`

- [ ] **Step 1: Write the failing test**

```python
from uuid import uuid4

import pytest
from sqlalchemy import func, select

from app.models.outbox import OutboxMessage
from app.services.payment_service import create_payment, get_payment

pytestmark = pytest.mark.integration


async def test_creates_payment_and_outbox_row(db_session):
    payment = await create_payment(
        db_session,
        amount="100.50",
        currency="RUB",
        description="order",
        metadata={"order_id": "1"},
        webhook_url="https://c.example/hook",
        idempotency_key="key-1",
    )
    assert payment.status.value == "pending"

    count = await db_session.scalar(select(func.count()).select_from(OutboxMessage))
    assert count == 1
    row = await db_session.scalar(select(OutboxMessage))
    assert row.aggregate_id == payment.id
    assert row.payload == {"payment_id": str(payment.id)}


async def test_duplicate_key_returns_existing(db_session):
    first = await create_payment(
        db_session,
        amount="10.00",
        currency="USD",
        description="a",
        metadata={},
        webhook_url="https://c.example/hook",
        idempotency_key="dup",
    )
    second = await create_payment(
        db_session,
        amount="999.00",
        currency="USD",
        description="b",
        metadata={},
        webhook_url="https://c.example/hook",
        idempotency_key="dup",
    )
    assert second.id == first.id
    assert second.amount == first.amount


async def test_get_payment_returns_none_for_missing(db_session):
    assert await get_payment(db_session, uuid4()) is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/integration/test_payment_service.py -v -m integration`
Expected: FAIL — `ModuleNotFoundError: No module named 'app.services.payment_service'`

- [ ] **Step 3: Write minimal implementation**

```python
from decimal import Decimal
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.enums import Currency
from app.models.outbox import OutboxMessage
from app.models.payment import Payment

PAYMENT_CREATED_EVENT = "payment.created"


class DuplicateKeyRace(Exception):
    """Raised when a concurrent insert with the same key cannot be resolved."""


def _to_decimal(amount) -> Decimal:
    return Decimal(str(amount))


async def create_payment(
    session: AsyncSession,
    *,
    amount,
    currency: str,
    description: str,
    metadata: dict,
    webhook_url: str,
    idempotency_key: str,
) -> Payment:
    existing = await get_payment_by_key(session, idempotency_key)
    if existing is not None:
        return existing

    payment = Payment(
        amount=_to_decimal(amount),
        currency=Currency(currency),
        description=description,
        metadata_=metadata or {},
        webhook_url=webhook_url,
        idempotency_key=idempotency_key,
    )
    session.add(payment)
    try:
        await session.flush()
        session.add(
            OutboxMessage(
                event_type=PAYMENT_CREATED_EVENT,
                aggregate_id=payment.id,
                payload={"payment_id": str(payment.id)},
            )
        )
        await session.commit()
    except IntegrityError:
        await session.rollback()
        existing = await get_payment_by_key(session, idempotency_key)
        if existing is None:
            raise DuplicateKeyRace from None
        return existing

    await session.refresh(payment)
    return payment


async def get_payment_by_key(session: AsyncSession, key: str) -> Payment | None:
    return await session.scalar(select(Payment).where(Payment.idempotency_key == key))


async def get_payment(session: AsyncSession, payment_id: UUID) -> Payment | None:
    return await session.get(Payment, payment_id)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/integration/test_payment_service.py -v -m integration`
Expected: PASS (3 passed)

- [ ] **Step 5: Commit**

```bash
git add app/services/payment_service.py tests/integration/test_payment_service.py
git commit -m "feat: idempotent payment creation with outbox write"
```

---

## Task 12: FastAPI app, auth dependency, health

**Files:**
- Create: `app/api/deps.py`, `app/api/routes/health.py`, `app/main.py`

- [ ] **Step 1: Write `app/api/deps.py`**

```python
import hmac

from fastapi import Header, HTTPException, status

from app.config import get_settings


async def require_api_key(x_api_key: str | None = Header(default=None)) -> None:
    expected = get_settings().api_key
    provided = x_api_key or ""
    if not hmac.compare_digest(provided.encode(), expected.encode()):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key",
        )
```

- [ ] **Step 2: Write `app/api/routes/health.py`**

```python
from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_session

router = APIRouter(tags=["health"])


@router.get("/health")
async def health() -> dict:
    return {"status": "ok"}


@router.get("/health/ready")
async def ready(session: AsyncSession = Depends(get_session)) -> dict:
    await session.execute(text("SELECT 1"))
    return {"status": "ready"}
```

- [ ] **Step 3: Write `app/main.py`**

```python
from fastapi import FastAPI

from app.api.routes.health import router as health_router
from app.api.routes.payments import router as payments_router
from app.logging import configure_logging


def create_app() -> FastAPI:
    configure_logging()
    app = FastAPI(title="Payment System", version="0.1.0")
    app.include_router(health_router)
    app.include_router(payments_router)
    return app


app = create_app()
```

> Run Task 13 (payments routes) before importing `app.main`, otherwise imports fail.

- [ ] **Step 4: Commit**

```bash
git add app/api/deps.py app/api/routes/health.py app/main.py
git commit -m "feat: FastAPI app, api-key dependency, health endpoints"
```

---

## Task 13: Payments API routes

**Files:**
- Create: `app/schemas/payment.py`, `app/api/routes/payments.py`
- Test: `tests/integration/test_api_payments.py`

- [ ] **Step 1: Write `app/schemas/payment.py`**

```python
from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field, HttpUrl, field_validator

from app.models.enums import Currency, PaymentStatus


class PaymentCreateRequest(BaseModel):
    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=2)
    currency: Currency
    description: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: HttpUrl

    @field_validator("webhook_url")
    @classmethod
    def require_https(cls, value: HttpUrl) -> HttpUrl:
        if value.scheme != "https":
            raise ValueError("webhook_url must use https")
        return value


class PaymentCreateResponse(BaseModel):
    payment_id: UUID
    status: PaymentStatus
    created_at: datetime


class PaymentDetailResponse(BaseModel):
    payment_id: UUID
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    status: PaymentStatus
    created_at: datetime
    processed_at: datetime | None
    webhook_delivered_at: datetime | None
```

- [ ] **Step 2: Write `app/api/routes/payments.py`**

```python
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import require_api_key
from app.db import get_session
from app.schemas.payment import (
    PaymentCreateRequest,
    PaymentCreateResponse,
    PaymentDetailResponse,
)
from app.services.payment_service import (
    DuplicateKeyRace,
    create_payment,
    get_payment,
)

router = APIRouter(
    prefix="/api/v1/payments", tags=["payments"], dependencies=[Depends(require_api_key)]
)


@router.post("", status_code=status.HTTP_202_ACCEPTED)
async def create_payment_endpoint(
    payload: PaymentCreateRequest,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    session: AsyncSession = Depends(get_session),
) -> PaymentCreateResponse:
    if not idempotency_key:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Idempotency-Key header is required",
        )
    try:
        payment = await create_payment(
            session,
            amount=payload.amount,
            currency=payload.currency.value,
            description=payload.description,
            metadata=payload.metadata,
            webhook_url=str(payload.webhook_url),
            idempotency_key=idempotency_key,
        )
    except DuplicateKeyRace:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Concurrent request with the same Idempotency-Key",
        ) from None

    return PaymentCreateResponse(
        payment_id=payment.id,
        status=payment.status,
        created_at=payment.created_at,
    )


@router.get("/{payment_id}")
async def get_payment_endpoint(
    payment_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> PaymentDetailResponse:
    payment = await get_payment(session, payment_id)
    if payment is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not found")
    return PaymentDetailResponse(
        payment_id=payment.id,
        amount=payment.amount,
        currency=payment.currency,
        description=payment.description,
        metadata=payment.metadata_,
        status=payment.status,
        created_at=payment.created_at,
        processed_at=payment.processed_at,
        webhook_delivered_at=payment.webhook_delivered_at,
    )
```

- [ ] **Step 3: Write the failing integration test**

```python
from uuid import uuid4

import pytest

pytestmark = pytest.mark.integration

HEADERS = {"X-API-Key": "test-key"}

BODY = {
    "amount": "100.50",
    "currency": "RUB",
    "description": "order",
    "metadata": {"order_id": "1"},
    "webhook_url": "https://client.example/hook",
}


async def test_requires_api_key(client):
    resp = await client.post(
        "/api/v1/payments", json=BODY, headers={"Idempotency-Key": "k"}
    )
    assert resp.status_code == 401


async def test_create_and_get_payment(client):
    resp = await client.post(
        "/api/v1/payments", json=BODY, headers={**HEADERS, "Idempotency-Key": "k1"}
    )
    assert resp.status_code == 202
    payment_id = resp.json()["payment_id"]

    got = await client.get(f"/api/v1/payments/{payment_id}", headers=HEADERS)
    assert got.status_code == 200
    assert got.json()["status"] == "pending"
    assert got.json()["amount"] == "100.50"


async def test_duplicate_idempotency_key_returns_same_payment(client):
    r1 = await client.post(
        "/api/v1/payments", json=BODY, headers={**HEADERS, "Idempotency-Key": "same"}
    )
    r2 = await client.post(
        "/api/v1/payments",
        json={**BODY, "amount": "1.00"},
        headers={**HEADERS, "Idempotency-Key": "same"},
    )
    assert r1.status_code == 202 and r2.status_code == 202
    assert r1.json()["payment_id"] == r2.json()["payment_id"]


async def test_missing_idempotency_key_is_422(client):
    resp = await client.post("/api/v1/payments", json=BODY, headers=HEADERS)
    assert resp.status_code == 422


async def test_get_missing_payment_is_404(client):
    resp = await client.get(f"/api/v1/payments/{uuid4()}", headers=HEADERS)
    assert resp.status_code == 404


async def test_webhook_url_requires_https(client):
    resp = await client.post(
        "/api/v1/payments",
        json={**BODY, "webhook_url": "http://client.example/hook"},
        headers={**HEADERS, "Idempotency-Key": "k2"},
    )
    assert resp.status_code == 422
```

- [ ] **Step 4: Run tests**

Run: `pytest tests/integration/test_api_payments.py -v -m integration`
Expected: PASS (6 passed)

- [ ] **Step 5: Commit**

```bash
git add app/schemas/payment.py app/api/routes/payments.py tests/integration/test_api_payments.py
git commit -m "feat: payments create/get endpoints"
```

---

## Task 14: RabbitMQ topology & publisher

**Files:**
- Create: `app/messaging/broker.py`, `app/messaging/topology.py`, `app/messaging/publisher.py`

- [ ] **Step 1: Write `app/messaging/broker.py`**

```python
from faststream.rabbit import RabbitBroker

from app.config import get_settings

broker = RabbitBroker(get_settings().rabbitmq_url)
```

- [ ] **Step 2: Write `app/messaging/topology.py`**

```python
from faststream.rabbit import ExchangeType, RabbitExchange, RabbitQueue

MAX_ATTEMPTS = 3

PAYMENTS_EXCHANGE = RabbitExchange("payments", type=ExchangeType.DIRECT, durable=True)
DLX_EXCHANGE = RabbitExchange("payments.dlx", type=ExchangeType.DIRECT, durable=True)

NEW_QUEUE = RabbitQueue(
    "payments.new",
    durable=True,
    routing_key="new",
    arguments={
        "x-dead-letter-exchange": "payments.dlx",
        "x-dead-letter-routing-key": "dlq",
    },
)
RETRY_QUEUES = (
    RabbitQueue(
        "payments.retry.1",
        durable=True,
        routing_key="retry.1",
        arguments={
            "x-message-ttl": 2000,
            "x-dead-letter-exchange": "payments",
            "x-dead-letter-routing-key": "new",
        },
    ),
    RabbitQueue(
        "payments.retry.2",
        durable=True,
        routing_key="retry.2",
        arguments={
            "x-message-ttl": 4000,
            "x-dead-letter-exchange": "payments",
            "x-dead-letter-routing-key": "new",
        },
    ),
)
DLQ = RabbitQueue("payments.dlq", durable=True, routing_key="dlq")


async def declare_topology(broker) -> None:
    await broker.declare_exchange(PAYMENTS_EXCHANGE)
    await broker.declare_exchange(DLX_EXCHANGE)
    await broker.declare_queue(NEW_QUEUE, exchange=PAYMENTS_EXCHANGE)
    for queue in RETRY_QUEUES:
        await broker.declare_queue(queue, exchange=PAYMENTS_EXCHANGE)
    await broker.declare_queue(DLQ, exchange=DLX_EXCHANGE)
```

- [ ] **Step 3: Write `app/messaging/publisher.py`**

```python
from uuid import UUID

from faststream.rabbit import RabbitBroker

from app.messaging.topology import (
    DLQ,
    MAX_ATTEMPTS,
    PAYMENTS_EXCHANGE,
    RETRY_QUEUES,
)

EVENT_TYPE = "payment.created"


def _message(payment_id: UUID, retry_count: int) -> dict:
    return {
        "payment_id": str(payment_id),
        "event_type": EVENT_TYPE,
        "retry_count": retry_count,
    }


async def publish_new(broker: RabbitBroker, payment_id: UUID) -> None:
    await broker.publish(
        _message(payment_id, 0),
        exchange=PAYMENTS_EXCHANGE,
        routing_key="new",
        headers={"x-retry-count": 0},
    )


async def publish_retry(
    broker: RabbitBroker, payment_id: UUID, next_count: int
) -> None:
    queue = RETRY_QUEUES[next_count - 1]
    await broker.publish(
        _message(payment_id, next_count),
        exchange=PAYMENTS_EXCHANGE,
        routing_key=queue.routing_key,
        headers={"x-retry-count": next_count},
    )


async def publish_dlq(broker: RabbitBroker, payment_id: UUID, reason: str) -> None:
    await broker.publish(
        _message(payment_id, MAX_ATTEMPTS),
        queue=DLQ,
        headers={"x-reason": reason},
    )
```

- [ ] **Step 4: Verify import**

Run: `python -c "from app.messaging import topology, publisher, broker"`
Expected: no error.

- [ ] **Step 5: Commit**

```bash
git add app/messaging/broker.py app/messaging/topology.py app/messaging/publisher.py
git commit -m "feat: RabbitMQ topology and publisher"
```

---

## Task 15: Outbox worker

**Files:**
- Create: `app/workers/outbox_worker.py`
- Test: `tests/integration/test_outbox_worker.py`

- [ ] **Step 1: Write the failing test**

```python
import pytest
from sqlalchemy import select

from app.models.outbox import OutboxMessage
from app.services.payment_service import create_payment
from app.workers.outbox_worker import publish_batch

pytestmark = pytest.mark.integration


async def test_publish_batch_marks_rows_and_calls_publisher(db_session):
    published = []

    async def fake_publish(payment_id):
        published.append(payment_id)

    await create_payment(
        db_session,
        amount="5.00",
        currency="EUR",
        description="x",
        metadata={},
        webhook_url="https://c.example/hook",
        idempotency_key="ob-1",
    )

    processed = await publish_batch(db_session, fake_publish, batch_size=10)
    assert processed == 1
    assert len(published) == 1

    rows = (await db_session.scalars(select(OutboxMessage))).all()
    assert all(r.published_at is not None for r in rows)
    assert await publish_batch(db_session, fake_publish, batch_size=10) == 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/integration/test_outbox_worker.py -v -m integration`
Expected: FAIL — `ModuleNotFoundError: No module named 'app.workers.outbox_worker'`

- [ ] **Step 3: Write minimal implementation**

```python
import asyncio
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.db import SessionLocal
from app.logging import configure_logging, get_logger
from app.messaging import publisher
from app.messaging.broker import broker
from app.messaging.topology import declare_topology
from app.models.outbox import OutboxMessage

logger = get_logger("outbox-worker")


async def publish_batch(
    session: AsyncSession,
    publish: Callable[[UUID], Awaitable[None]],
    batch_size: int,
) -> int:
    rows = (
        await session.scalars(
            select(OutboxMessage)
            .where(OutboxMessage.published_at.is_(None))
            .order_by(OutboxMessage.created_at)
            .limit(batch_size)
            .with_for_update(skip_locked=True)
        )
    ).all()

    for row in rows:
        payment_id = UUID(row.payload["payment_id"])
        await publish(payment_id)
        row.published_at = datetime.now(timezone.utc)

    await session.commit()
    return len(rows)


async def _run() -> None:
    settings = get_settings()
    async with broker:
        await declare_topology(broker)
        while True:
            async with SessionLocal() as session:
                try:
                    n = await publish_batch(
                        session,
                        lambda pid: publisher.publish_new(broker, pid),
                        settings.outbox_batch_size,
                    )
                    if n:
                        logger.info("published outbox batch", count=n)
                except Exception:
                    logger.exception("outbox batch failed")
                    await session.rollback()
            await asyncio.sleep(settings.outbox_poll_interval)


def main() -> None:
    configure_logging()
    asyncio.run(_run())


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/integration/test_outbox_worker.py -v -m integration`
Expected: PASS (1 passed)

- [ ] **Step 5: Commit**

```bash
git add app/workers/outbox_worker.py tests/integration/test_outbox_worker.py
git commit -m "feat: outbox relay worker"
```

---

## Task 16: Consumer processing service (pure routing)

**Files:**
- Create: `app/services/processing.py`
- Test: `tests/unit/test_processing.py`

- [ ] **Step 1: Write the failing test**

```python
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

from app.services.processing import (
    ACTION_DONE,
    ACTION_RETRY,
    ProcessingService,
)
from app.services.webhook import PermanentWebhookError, TransientWebhookError


class FakePayment:
    def __init__(self, status="pending", delivered=None):
        self.id = uuid4()
        self.status = SimpleNamespace(value=status)
        self.processed_at = None
        self.webhook_delivered_at = delivered
        self.webhook_attempts = 0
        self.amount = 1
        self.currency = SimpleNamespace(value="RUB")


class FakeGateway:
    def __init__(self, result):
        self.result = result

    async def process(self):
        return self.result


class FakeWebhook:
    def __init__(self, exc=None):
        self.exc = exc
        self.sent = 0

    async def send(self, payment):
        self.sent += 1
        if self.exc:
            raise self.exc


class FakeRepo:
    def __init__(self, payment):
        self.payment = payment

    async def load(self, payment_id):
        return self.payment

    async def mark_processed(self, payment, status):
        payment.status = SimpleNamespace(value=status)
        payment.processed_at = datetime.now(timezone.utc)
        return True

    async def mark_webhook_delivered(self, payment):
        payment.webhook_delivered_at = datetime.now(timezone.utc)

    async def increment_webhook_attempts(self, payment):
        payment.webhook_attempts += 1


async def test_success_sets_status_and_sends_webhook():
    payment = FakePayment()
    webhook = FakeWebhook()
    svc = ProcessingService(FakeRepo(payment), FakeGateway(True), webhook)
    result = await svc.process(payment.id)

    assert result.action == ACTION_DONE
    assert payment.status.value == "succeeded"
    assert webhook.sent == 1


async def test_business_failure_marks_failed_and_sends_webhook():
    payment = FakePayment()
    webhook = FakeWebhook()
    svc = ProcessingService(FakeRepo(payment), FakeGateway(False), webhook)
    result = await svc.process(payment.id)

    assert payment.status.value == "failed"
    assert webhook.sent == 1
    assert result.action == ACTION_DONE


async def test_transient_webhook_error_requests_retry():
    payment = FakePayment()
    webhook = FakeWebhook(exc=TransientWebhookError("boom"))
    svc = ProcessingService(FakeRepo(payment), FakeGateway(True), webhook)
    result = await svc.process(payment.id)

    assert result.action == ACTION_RETRY


async def test_permanent_webhook_error_drops_to_dlq():
    payment = FakePayment()
    webhook = FakeWebhook(exc=PermanentWebhookError("400"))
    svc = ProcessingService(FakeRepo(payment), FakeGateway(True), webhook)
    result = await svc.process(payment.id)

    assert result.action == "dlq"


async def test_terminal_status_still_retries_webhook():
    payment = FakePayment(status="succeeded")
    webhook = FakeWebhook()
    svc = ProcessingService(FakeRepo(payment), FakeGateway(True), webhook)
    result = await svc.process(payment.id)

    assert result.action == ACTION_DONE
    assert webhook.sent == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_processing.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'app.services.processing'`

- [ ] **Step 3: Write minimal implementation**

```python
import uuid
from dataclasses import dataclass
from typing import Literal, Protocol

from app.services.webhook import PermanentWebhookError, TransientWebhookError

ACTION_DONE = "done"
ACTION_RETRY = "retry"
ACTION_DLQ = "dlq"


@dataclass
class ProcessingResult:
    action: Literal["done", "retry", "dlq"]
    reason: str | None = None


class PaymentRepository(Protocol):
    async def load(self, payment_id: uuid.UUID): ...
    async def mark_processed(self, payment, status: str) -> bool: ...
    async def mark_webhook_delivered(self, payment) -> None: ...
    async def increment_webhook_attempts(self, payment) -> None: ...


class Gateway(Protocol):
    async def process(self) -> bool: ...


class Webhook(Protocol):
    async def send(self, payment) -> None: ...


class ProcessingService:
    def __init__(self, repo: PaymentRepository, gateway: Gateway, webhook: Webhook):
        self.repo = repo
        self.gateway = gateway
        self.webhook = webhook

    async def process(self, payment_id: uuid.UUID) -> ProcessingResult:
        payment = await self.repo.load(payment_id)
        if payment is None:
            return ProcessingResult(ACTION_DLQ, reason="not_found")

        if payment.status.value == "pending":
            result = await self.gateway.process()
            status = "succeeded" if result else "failed"
            won = await self.repo.mark_processed(payment, status)
            if not won:
                payment = await self.repo.load(payment_id)

        if payment.webhook_delivered_at is None:
            await self.repo.increment_webhook_attempts(payment)
            try:
                await self.webhook.send(payment)
            except PermanentWebhookError as exc:
                return ProcessingResult(ACTION_DLQ, reason=str(exc))
            except TransientWebhookError as exc:
                return ProcessingResult(ACTION_RETRY, reason=str(exc))
            await self.repo.mark_webhook_delivered(payment)

        return ProcessingResult(ACTION_DONE)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_processing.py -v`
Expected: PASS (5 passed)

- [ ] **Step 5: Commit**

```bash
git add app/services/processing.py tests/unit/test_processing.py
git commit -m "feat: consumer processing service with retry/DLQ classification"
```

---

## Task 17: SQL repository, routing decision & FastStream consumer

**Files:**
- Create: `app/services/repository.py`, `app/messaging/consumer.py`
- Test: `tests/unit/test_routing.py`, `tests/integration/test_consumer_publish.py`

- [ ] **Step 1: Write `app/services/repository.py`**

```python
import uuid
from datetime import datetime, timezone

from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.payment import Payment


class SqlPaymentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def load(self, payment_id: uuid.UUID) -> Payment | None:
        return await self.session.get(Payment, payment_id)

    async def mark_processed(self, payment: Payment, status: str) -> bool:
        result = await self.session.execute(
            update(Payment)
            .where(Payment.id == payment.id, Payment.status == "pending")
            .values(status=status, processed_at=datetime.now(timezone.utc))
            .returning(Payment.id)
        )
        won = result.first() is not None
        await self.session.commit()
        return won

    async def mark_webhook_delivered(self, payment: Payment) -> None:
        await self.session.execute(
            update(Payment)
            .where(Payment.id == payment.id)
            .values(webhook_delivered_at=datetime.now(timezone.utc))
        )
        await self.session.commit()

    async def increment_webhook_attempts(self, payment: Payment) -> None:
        await self.session.execute(
            update(Payment)
            .where(Payment.id == payment.id)
            .values(webhook_attempts=Payment.webhook_attempts + 1)
        )
        await self.session.commit()
```

- [ ] **Step 2: Write the failing routing unit test `tests/unit/test_routing.py`**

```python
from app.messaging.consumer import decide_route
from app.services.processing import ACTION_DONE, ACTION_DLQ, ACTION_RETRY, ProcessingResult


def test_ack_on_done():
    assert decide_route(0, ProcessingResult(ACTION_DONE)) == ("ack", None, None)


def test_retry_advances_count():
    assert decide_route(0, ProcessingResult(ACTION_RETRY)) == ("retry", 1, None)
    assert decide_route(1, ProcessingResult(ACTION_RETRY)) == ("retry", 2, None)


def test_retry_exhausted_goes_to_dlq():
    action, count, reason = decide_route(2, ProcessingResult(ACTION_RETRY, "down"))
    assert action == "dlq"
    assert count is None
    assert reason == "down"


def test_immediate_dlq_on_poison():
    action, _, reason = decide_route(0, ProcessingResult(ACTION_DLQ, "not_found"))
    assert action == "dlq"
    assert reason == "not_found"
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pytest tests/unit/test_routing.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'app.messaging.consumer'`

- [ ] **Step 4: Write `app/messaging/consumer.py`**

```python
import asyncio
import uuid

import httpx
from faststream import FastStream
from faststream.rabbit import RabbitMessage, RabbitRouter
from pydantic import BaseModel

from app.config import get_settings
from app.db import SessionLocal
from app.logging import configure_logging, get_logger
from app.messaging.broker import broker
from app.messaging.publisher import publish_dlq, publish_retry
from app.messaging.topology import (
    MAX_ATTEMPTS,
    NEW_QUEUE,
    PAYMENTS_EXCHANGE,
    declare_topology,
)
from app.services.gateway import PaymentGateway
from app.services.processing import (
    ACTION_DONE,
    ACTION_DLQ,
    ACTION_RETRY,
    ProcessingResult,
    ProcessingService,
)
from app.services.repository import SqlPaymentRepository
from app.services.webhook import WebhookSender

logger = get_logger("consumer")
router = RabbitRouter()


class PaymentEvent(BaseModel):
    payment_id: uuid.UUID
    event_type: str
    retry_count: int = 0


def decide_route(
    retry_count: int, result: ProcessingResult
) -> tuple[str, int | None, str | None]:
    if result.action == ACTION_RETRY:
        if retry_count + 1 >= MAX_ATTEMPTS:
            return ("dlq", None, result.reason or "retries_exhausted")
        return ("retry", retry_count + 1, None)
    if result.action == ACTION_DLQ:
        return ("dlq", None, result.reason or "unknown")
    return ("ack", None, None)


@router.subscriber(NEW_QUEUE, exchange=PAYMENTS_EXCHANGE)
async def handle_new_payment(event: PaymentEvent, message: RabbitMessage) -> None:
    settings = get_settings()
    retry_count = int((message.headers or {}).get("x-retry-count", event.retry_count))

    async with httpx.AsyncClient(timeout=settings.webhook_timeout) as client:
        webhook = WebhookSender(client=client, secret=settings.webhook_signing_secret)
        async with SessionLocal() as session:
            service = ProcessingService(
                repo=SqlPaymentRepository(session),
                gateway=PaymentGateway(),
                webhook=webhook,
            )
            result = await service.process(event.payment_id)

    route, next_count, reason = decide_route(retry_count, result)
    if route == "retry":
        logger.info("scheduling retry", payment_id=str(event.payment_id), next=next_count)
        await publish_retry(broker, event.payment_id, next_count)
    elif route == "dlq":
        logger.error(
            "dropping to dlq", payment_id=str(event.payment_id), reason=reason
        )
        await publish_dlq(broker, event.payment_id, reason)


async def _run() -> None:
    await declare_topology(broker)
    app = FastStream(broker)
    app.include_router(router)
    await app.run()


def main() -> None:
    configure_logging()
    asyncio.run(_run())


if __name__ == "__main__":
    main()
```

- [ ] **Step 5: Write the failing publish integration test `tests/integration/test_consumer_publish.py`**

```python
import uuid

import aio_pika
import pytest
from faststream.rabbit import RabbitBroker

from app.messaging import publisher, topology

pytestmark = pytest.mark.integration


async def test_retry_message_lands_in_retry_queue(rabbit_url):
    broker = RabbitBroker(rabbit_url)
    async with broker:
        await topology.declare_topology(broker)
        await publisher.publish_retry(broker, uuid.uuid4(), 1)

    conn = await aio_pika.connect_robust(rabbit_url)
    async with conn:
        channel = await conn.channel()
        queue = await channel.declare_queue("payments.retry.1", passive=True)
        message = await queue.get(timeout=10)
        assert message is not None
        assert message.headers["x-retry-count"] == 1
        await message.ack()


async def test_dlq_message_lands_in_dlq(rabbit_url):
    broker = RabbitBroker(rabbit_url)
    async with broker:
        await topology.declare_topology(broker)
        await publisher.publish_dlq(broker, uuid.uuid4(), "reason")

    conn = await aio_pika.connect_robust(rabbit_url)
    async with conn:
        channel = await conn.channel()
        queue = await channel.declare_queue("payments.dlq", passive=True)
        message = await queue.get(timeout=10)
        assert message is not None
        assert message.headers["x-reason"] == "reason"
        await message.ack()
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `pytest tests/unit/test_routing.py tests/integration/test_consumer_publish.py -v -m integration`
Expected: routing PASS (4 passed); publish tests PASS (2 passed).

- [ ] **Step 7: Commit**

```bash
git add app/services/repository.py app/messaging/consumer.py tests/unit/test_routing.py tests/integration/test_consumer_publish.py
git commit -m "feat: repository, routing decision, FastStream consumer"
```

---

## Task 18: Docker image & compose

**Files:**
- Create: `Dockerfile`, `docker-compose.yml`, `.dockerignore`

- [ ] **Step 1: Write `Dockerfile`**

```dockerfile
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY pyproject.toml ./
COPY app ./app
RUN pip install --no-cache-dir .

COPY alembic ./alembic
COPY alembic.ini ./

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

- [ ] **Step 2: Write `.dockerignore`**

```
.venv
__pycache__
*.pyc
.pytest_cache
.git
docs
tests
```

- [ ] **Step 3: Write `docker-compose.yml`**

```yaml
services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: payments
      POSTGRES_PASSWORD: payments
      POSTGRES_DB: payments
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U payments"]
      interval: 3s
      timeout: 3s
      retries: 20

  rabbitmq:
    image: rabbitmq:3-management-alpine
    ports:
      - "5672:5672"
      - "15672:15672"
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "-q", "ping"]
      interval: 5s
      timeout: 5s
      retries: 20

  api:
    build: .
    command: sh -c "alembic upgrade head && uvicorn app.main:app --host 0.0.0.0 --port 8000"
    environment:
      DATABASE_URL: postgresql+asyncpg://payments:payments@postgres:5432/payments
      RABBITMQ_URL: amqp://guest:guest@rabbitmq:5672/
      API_KEY: dev-api-key
      WEBHOOK_SIGNING_SECRET: dev-webhook-secret
    ports:
      - "8000:8000"
    depends_on:
      postgres:
        condition: service_healthy
      rabbitmq:
        condition: service_healthy
    healthcheck:
      test:
        [
          "CMD",
          "python",
          "-c",
          "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')",
        ]
      interval: 5s
      timeout: 5s
      retries: 20

  outbox-worker:
    build: .
    command: python -m app.workers.outbox_worker
    environment:
      DATABASE_URL: postgresql+asyncpg://payments:payments@postgres:5432/payments
      RABBITMQ_URL: amqp://guest:guest@rabbitmq:5672/
    depends_on:
      api:
        condition: service_healthy

  consumer:
    build: .
    command: python -m app.messaging.consumer
    environment:
      DATABASE_URL: postgresql+asyncpg://payments:payments@postgres:5432/payments
      RABBITMQ_URL: amqp://guest:guest@rabbitmq:5672/
      WEBHOOK_SIGNING_SECRET: dev-webhook-secret
    depends_on:
      api:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "python", "-c", "print('ok')"]
      interval: 10s
```

- [ ] **Step 4: Build and start**

```bash
docker compose build
docker compose up -d
sleep 12
curl -s localhost:8000/health
```
Expected: `{"status":"ok"}`

- [ ] **Step 5: Commit**

```bash
git add Dockerfile .dockerignore docker-compose.yml
git commit -m "feat: docker image and compose stack"
```

---

## Task 19: README + out-of-scope rationale

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Write `README.md`**

````markdown
# Payment System

Асинхронный сервис процессинга платежей: FastAPI + RabbitMQ (Outbox, retry, DLQ) + PostgreSQL.

## Запуск

```bash
cp .env.example .env
docker compose up --build
```

- API: http://localhost:8000 (docs: `/docs`)
- RabbitMQ UI: http://localhost:15672 (guest/guest)

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
````

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: readme with run instructions and out-of-scope rationale"
```

---

## Task 20: E2E smoke & final verification

**Files:**
- Modify: `README.md` (add a "Smoke check" subsection)

- [ ] **Step 1: Success path — run a local webhook receiver**

In one terminal start a tiny receiver that returns 200:

```bash
python -m http.server 9999
```

In another, create a payment whose `webhook_url` points at a public tunnel to `:9999` (e.g. `ngrok http 9999`), then:

```bash
curl -s -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: dev-api-key" -H "Idempotency-Key: smoke-ok" -H "Content-Type: application/json" \
  -d '{"amount":"42.00","currency":"USD","description":"smoke","metadata":{},"webhook_url":"https://<tunnel-host>/hook"}'
```
Expected: `202`; within ~5–8s `GET` shows `status` in `{succeeded,failed}` and `webhook_delivered_at` set.

- [ ] **Step 2: Retry→DLQ path — unreachable https endpoint (offline)**

```bash
curl -s -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: dev-api-key" -H "Idempotency-Key: smoke-dlq" -H "Content-Type: application/json" \
  -d '{"amount":"1.00","currency":"EUR","description":"dlq","metadata":{},"webhook_url":"https://127.0.0.1:1/hook"}'
```
Expected: webhook connection fails → transient → retry 2s → retry 4s → DLQ. After ~8–12s open http://localhost:15672 → Queues → `payments.dlq` shows 1 message with `x-retry-count=3`.

- [ ] **Step 3: Run full suite + lint**

```bash
ruff check .
pytest tests/unit -v
pytest tests/integration -v -m integration
```
Expected: lint clean; unit + integration all pass.

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "docs: e2e smoke instructions and final verification"
```

---

## Self-Review Notes

- **Spec coverage:** models+outbox (Task 5/6), create/get API + auth + idempotency (Task 12/13), single consumer (Task 16/17), outbox pattern (Task 15), retry exp-backoff 3 attempts (Task 14/16/17), DLQ (Task 14/17), docker compose with 5 services (Task 18), README (Task 19), HMAC (Task 7/10), critical fixes 1–9 (Tasks 10, 13, 15, 16, 17, 18).
- **Type consistency:** `ProcessingResult.action` (`done`/`retry`/`dlq`) matches `decide_route`; `decide_route` returns `("ack"|"retry"|"dlq", int|None, str|None)` and the consumer handles exactly those; `publish_retry`/`publish_dlq` signatures match call sites; `WebhookSender.send` matches the `Webhook` protocol.
- **Open item to verify during Task 14/17:** exact FastStream publish API (`exchange`+`routing_key` vs `queue=`) — the plan uses `exchange=PAYMENTS_EXCHANGE, routing_key=...` for the main/retry paths and `queue=DLQ` for the DLQ path. Confirm against the installed FastStream version; keep `declare_topology` idempotent, and ensure the subscriber declares `PAYMENTS_EXCHANGE`.

## Implementation deviations log

- **Task 5/6 — enum storage (Critical fix).** SQLAlchemy `Enum` persists member **names** by default, but the migration hardcoded lowercase **values**. This mismatch only surfaced outside `create_all` (i.e. the alembic/Docker path would reject inserts). Fixed by adding `values_callable=lambda enum_cls: [m.value for m in enum_cls]` to both `SAEnum` columns in `app/models/payment.py`, and using `enum.StrEnum` for `Currency`/`PaymentStatus`. Verified: ORM insert + `WHERE status='pending'` conditional update both work against an alembic-migrated schema.
- **Task 6 — migration enum creation.** Removed the redundant explicit `currency.create()` / `payment_status.create()` calls in `0001_initial.py`; SQLAlchemy 2.1 creates the PG enum types automatically from `create_table` (avoids "type already exists").
- **Task 12/13 — commit order.** `main.py` imports the payments router, so `deps.py` + `routes/payments.py` were committed with Task 13 to keep every commit importable.
- **Ruff config.** Added `[tool.ruff.lint.flake8-bugbear] extend-immutable-calls` for FastAPI `Depends`/`Header` (B008 is idiomatic FastAPI).
