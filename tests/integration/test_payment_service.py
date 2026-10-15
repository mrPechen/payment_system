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
