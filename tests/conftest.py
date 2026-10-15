import os

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

# Обязательные и условные переменные окружения задаются ДО импорта config:
# frozen settings читают их один раз при создании module-level `settings`.
os.environ.setdefault(
    "DATABASE_URL", "postgresql+asyncpg://payments:payments@localhost:5432/payments"
)
os.environ.setdefault("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
os.environ.setdefault("API_KEY", "test-key")
os.environ.setdefault("WEBHOOK_SIGNING_SECRET", "test-secret")
os.environ.setdefault("DEBUG", "true")


@pytest.fixture(scope="session")
def postgres_url():
    from testcontainers.community.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine") as pg:
        yield pg.get_connection_url().replace("psycopg2", "asyncpg")


@pytest.fixture(scope="session")
def rabbit_url():
    from testcontainers.community.rabbitmq import RabbitMqContainer

    with RabbitMqContainer("rabbitmq:3-management-alpine") as rb:
        host = rb.get_container_host_ip()
        port = rb.get_exposed_port(rb.port)
        yield f"amqp://{rb.username}:{rb.password}@{host}:{port}/"


@pytest.fixture
async def engine(postgres_url):
    from src.database.models import Base

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
    from src.dependencies.db import get_async_session
    from main import app

    maker = async_sessionmaker(engine, expire_on_commit=False)

    async def override_get_session():
        async with maker() as session:
            yield session

    app.dependency_overrides[get_async_session] = override_get_session
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        yield ac
    app.dependency_overrides.clear()
