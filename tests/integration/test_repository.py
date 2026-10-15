import pytest

from app.models.enums import PaymentStatus
from app.services.payment_service import create_payment
from app.services.repository import SqlPaymentRepository

pytestmark = pytest.mark.integration


async def test_mark_processed_leaves_status_as_enum(db_session):
    """После mark_processed объект Payment должен держать enum-статус.

    Вебхук (WebhookSender.build_body) читает payment.status.value; если
    репозиторий синхронизирует строку вместо enum — consumer упадёт.
    """
    payment = await create_payment(
        db_session,
        amount="10.00",
        currency="USD",
        description="a",
        metadata={},
        webhook_url="https://c.example/hook",
        idempotency_key="repo-1",
    )

    repo = SqlPaymentRepository(db_session)
    won = await repo.mark_processed(payment, "succeeded")

    assert won is True
    assert isinstance(payment.status, PaymentStatus)
    assert payment.status.value == "succeeded"
    assert payment.processed_at is not None
