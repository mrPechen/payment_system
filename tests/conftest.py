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
