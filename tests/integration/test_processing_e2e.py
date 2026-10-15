"""Integration test: ProcessingService against a real DB + real repository,
with a webhook transport stubbed to return 200 (closes the success-path gap,
since the offline E2E smoke can only exercise the retry->DLQ path)."""

import httpx
import pytest

from app.services.payment_service import create_payment
from app.services.processing import ACTION_DONE, ProcessingService
from app.services.repository import SqlPaymentRepository
from app.services.webhook import WebhookSender

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
    return await create_payment(
        db_session,
        amount="3.00",
        currency="RUB",
        description="processing-e2e",
        metadata={},
        webhook_url="https://client.example/hook",
        idempotency_key=key,
    )


async def test_success_marks_status_and_webhook_delivered(db_session):
    payment = await _make_payment(db_session, "proc-ok")
    sender = _sender(lambda request: httpx.Response(200))
    svc = ProcessingService(
        SqlPaymentRepository(db_session), StubGateway(True), sender
    )

    result = await svc.process(payment.id)

    refreshed = await db_session.get(type(payment), payment.id)
    assert result.action == ACTION_DONE
    assert refreshed.status.value == "succeeded"
    assert refreshed.processed_at is not None
    assert refreshed.webhook_delivered_at is not None


async def test_business_failure_marks_failed_but_still_delivers(db_session):
    payment = await _make_payment(db_session, "proc-fail")
    sender = _sender(lambda request: httpx.Response(200))
    svc = ProcessingService(
        SqlPaymentRepository(db_session), StubGateway(False), sender
    )

    result = await svc.process(payment.id)

    refreshed = await db_session.get(type(payment), payment.id)
    assert result.action == ACTION_DONE
    assert refreshed.status.value == "failed"
    assert refreshed.webhook_delivered_at is not None
