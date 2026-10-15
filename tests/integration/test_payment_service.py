from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import func, select

from src.database.models import OutboxMessage
from src.handlers.payment_handlers import PaymentHandler
from src.services.uow.payments_uow import PaymentsUnitOfWork
from src.utils.exceptions import NotFoundError

pytestmark = pytest.mark.integration


async def _create_payment(
    db_session,
    *,
    amount,
    currency,
    description,
    metadata,
    webhook_url,
    idempotency_key,
):
    async with PaymentsUnitOfWork(db_session) as uow:
        handler = PaymentHandler(uow)
        return await handler.create_payment(
            {
                "amount": amount,
                "currency": currency,
                "description": description,
                "metadata": metadata,
                "webhook_url": webhook_url,
            },
            idempotency_key,
        )


async def test_creates_payment_and_outbox_row(db_session):
    response = await _create_payment(
        db_session,
        amount="100.50",
        currency="RUB",
        description="order",
        metadata={"order_id": "1"},
        webhook_url="https://c.example/hook",
        idempotency_key="key-1",
    )
    assert response.status.value == "pending"

    count = await db_session.scalar(select(func.count()).select_from(OutboxMessage))
    assert count == 1
    row = await db_session.scalar(select(OutboxMessage))
    assert row.aggregate_id == response.payment_id
    assert row.payload == {"payment_id": str(response.payment_id)}


async def test_duplicate_key_returns_existing(db_session):
    first = await _create_payment(
        db_session,
        amount="10.00",
        currency="USD",
        description="a",
        metadata={},
        webhook_url="https://c.example/hook",
        idempotency_key="dup",
    )
    second = await _create_payment(
        db_session,
        amount="999.00",
        currency="USD",
        description="b",
        metadata={},
        webhook_url="https://c.example/hook",
        idempotency_key="dup",
    )
    assert second.payment_id == first.payment_id

    async with PaymentsUnitOfWork(db_session) as uow:
        existing = await uow.payment.get_by_idempotency_key("dup")
    assert existing.amount == Decimal("10.00")


async def test_get_by_id_raises_not_found_for_missing(db_session):
    async with PaymentsUnitOfWork(db_session) as uow:
        with pytest.raises(NotFoundError):
            await uow.payment.get_by_id(uuid4())
