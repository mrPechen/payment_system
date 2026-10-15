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
