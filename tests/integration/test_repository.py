import pytest

from src.database.models import PaymentStatus
from src.handlers.payment_handlers import PaymentHandler
from src.services.uow.payments_uow import PaymentsUnitOfWork

pytestmark = pytest.mark.integration


async def test_mark_processed_leaves_status_as_enum(db_session):
    """После mark_processed объект Payment должен держать enum-статус.

    Вебхук (WebhookSender.build_body) читает payment.status.value; если
    query синхронизирует строку вместо enum — consumer упадёт.
    """
    async with PaymentsUnitOfWork(db_session) as uow:
        handler = PaymentHandler(uow)
        response = await handler.create_payment(
            {
                "amount": "10.00",
                "currency": "USD",
                "description": "a",
                "metadata": {},
                "webhook_url": "https://c.example/hook",
            },
            "repo-1",
        )
        payment_id = response.payment_id

    async with PaymentsUnitOfWork(db_session) as uow:
        payment = await uow.payment.get_by_id(payment_id)
        won = await uow.payment.mark_processed(payment, "succeeded")

    assert won is True
    assert isinstance(payment.status, PaymentStatus)
    assert payment.status.value == "succeeded"
    assert payment.processed_at is not None
