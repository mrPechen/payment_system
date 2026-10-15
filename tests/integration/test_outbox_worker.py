import pytest
from sqlalchemy import select

from src.database.models import OutboxMessage
from src.handlers.payment_handlers import PaymentHandler
from src.services.uow.payments_uow import PaymentsUnitOfWork
from src.workers.outbox_worker import publish_batch

pytestmark = pytest.mark.integration


async def test_publish_batch_marks_rows_and_calls_publisher(db_session):
    published = []

    async def fake_publish(payment_id):
        published.append(payment_id)

    async with PaymentsUnitOfWork(db_session) as uow:
        handler = PaymentHandler(uow)
        await handler.create_payment(
            {
                "amount": "5.00",
                "currency": "EUR",
                "description": "x",
                "metadata": {},
                "webhook_url": "https://c.example/hook",
            },
            "ob-1",
        )

    processed = await publish_batch(db_session, fake_publish, batch_size=10)
    assert processed == 1
    assert len(published) == 1

    rows = (await db_session.scalars(select(OutboxMessage))).all()
    assert all(r.published_at is not None for r in rows)

    # SELECT выше autobegin-ил read-транзакцию, а publish_batch (через UoW) делает
    # явный session.begin() — откатываем чтение, чтобы второй вызов начался чисто.
    await db_session.rollback()
    assert await publish_batch(db_session, fake_publish, batch_size=10) == 0
