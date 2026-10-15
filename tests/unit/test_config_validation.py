from config import Settings


def test_success_rate_is_validated():
    s = Settings(
        DATABASE_URL="postgresql+asyncpg://user:pass@localhost/db",
        RABBITMQ_URL="amqp://guest:guest@localhost/",
        GATEWAY_SUCCESS_RATE=0.9,
    )
    assert s.GATEWAY_SUCCESS_RATE == 0.9


def test_batch_size_defaults():
    s = Settings(
        DATABASE_URL="postgresql+asyncpg://user:pass@localhost/db",
        RABBITMQ_URL="amqp://guest:guest@localhost/",
    )
    assert s.OUTBOX_BATCH_SIZE == 100
