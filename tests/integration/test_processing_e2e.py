"""Integration test: ProcessingHandler against a real DB + real query/UoW,
with a webhook transport stubbed to return 200 (closes the success-path gap,
since the offline E2E smoke can only exercise the retry->DLQ path)."""

import httpx
import pytest

from src.database.models import Payment
from src.handlers.payment_handlers import PaymentHandler
from src.handlers.processing_handlers import ACTION_DONE, ProcessingHandler
from src.services.uow.payments_uow import PaymentsUnitOfWork
from src.utils.http_utils import WebhookSender

pytestmark = pytest.mark.integration


class StubGateway:
    def __init__(self, result: bool) -> None:
        self.result = result

    async def process(self) -> bool:
        return self.result


def _sender(handler) -> WebhookSender:
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return WebhookSender(client=client, secret="test-secret", retry_delays=(0.0, 0.0))


async def _make_payment(db_session, key: str):
    async with PaymentsUnitOfWork(db_session) as uow:
        handler = PaymentHandler(uow)
        response = await handler.create_payment(
            {
                "amount": "3.00",
                "currency": "RUB",
                "description": "processing-e2e",
                "metadata": {},
                "webhook_url": "https://client.example/hook",
            },
            key,
        )
        return response.payment_id


async def test_success_marks_status_and_webhook_delivered(db_session):
    payment_id = await _make_payment(db_session, "proc-ok")
    sender = _sender(lambda request: httpx.Response(200))
    async with PaymentsUnitOfWork(db_session) as uow:
        handler = ProcessingHandler(uow, StubGateway(True), sender)
        result = await handler.process(payment_id)

    refreshed = await db_session.get(Payment, payment_id)
    assert result.action == ACTION_DONE
    assert refreshed.status.value == "succeeded"
    assert refreshed.processed_at is not None
    assert refreshed.webhook_delivered_at is not None


async def test_business_failure_marks_failed_but_still_delivers(db_session):
    payment_id = await _make_payment(db_session, "proc-fail")
    sender = _sender(lambda request: httpx.Response(200))
    async with PaymentsUnitOfWork(db_session) as uow:
        handler = ProcessingHandler(uow, StubGateway(False), sender)
        result = await handler.process(payment_id)

    refreshed = await db_session.get(Payment, payment_id)
    assert result.action == ACTION_DONE
    assert refreshed.status.value == "failed"
    assert refreshed.webhook_delivered_at is not None
