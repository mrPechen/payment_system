from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

from src.handlers.processing_handlers import (
    ACTION_DONE,
    ACTION_RETRY,
    ProcessingHandler,
)
from src.utils.http_utils import PermanentWebhookError, TransientWebhookError


class FakePayment:
    def __init__(self, status="pending", delivered=None):
        self.id = uuid4()
        self.status = SimpleNamespace(value=status)
        self.processed_at = None
        self.webhook_delivered_at = delivered
        self.webhook_attempts = 0
        self.amount = 1
        self.currency = SimpleNamespace(value="RUB")


class FakeGateway:
    def __init__(self, result):
        self.result = result

    async def process(self):
        return self.result


class FakeWebhook:
    def __init__(self, exc=None):
        self.exc = exc
        self.sent = 0

    async def send(self, payment):
        self.sent += 1
        if self.exc:
            raise self.exc


class FakeQuery:
    def __init__(self, payment):
        self.payment = payment

    async def get_by_id(self, payment_id):
        return self.payment

    async def mark_processed(self, payment, status):
        payment.status = SimpleNamespace(value=status)
        payment.processed_at = datetime.now(UTC)
        return True

    async def mark_webhook_delivered(self, payment):
        payment.webhook_delivered_at = datetime.now(UTC)

    async def increment_webhook_attempts(self, payment):
        payment.webhook_attempts += 1


class FakeUow:
    def __init__(self, payment):
        self.payment = FakeQuery(payment)

    async def commit(self):
        pass


async def test_success_sets_status_and_sends_webhook():
    payment = FakePayment()
    webhook = FakeWebhook()
    svc = ProcessingHandler(FakeUow(payment), FakeGateway(True), webhook)
    result = await svc.process(payment.id)

    assert result.action == ACTION_DONE
    assert payment.status.value == "succeeded"
    assert webhook.sent == 1


async def test_business_failure_marks_failed_and_sends_webhook():
    payment = FakePayment()
    webhook = FakeWebhook()
    svc = ProcessingHandler(FakeUow(payment), FakeGateway(False), webhook)
    result = await svc.process(payment.id)

    assert payment.status.value == "failed"
    assert webhook.sent == 1
    assert result.action == ACTION_DONE


async def test_transient_webhook_error_requests_retry():
    payment = FakePayment()
    webhook = FakeWebhook(exc=TransientWebhookError("boom"))
    svc = ProcessingHandler(FakeUow(payment), FakeGateway(True), webhook)
    result = await svc.process(payment.id)

    assert result.action == ACTION_RETRY


async def test_permanent_webhook_error_drops_to_dlq():
    payment = FakePayment()
    webhook = FakeWebhook(exc=PermanentWebhookError("400"))
    svc = ProcessingHandler(FakeUow(payment), FakeGateway(True), webhook)
    result = await svc.process(payment.id)

    assert result.action == "dlq"


async def test_terminal_status_still_retries_webhook():
    payment = FakePayment(status="succeeded")
    webhook = FakeWebhook()
    svc = ProcessingHandler(FakeUow(payment), FakeGateway(True), webhook)
    result = await svc.process(payment.id)

    assert result.action == ACTION_DONE
    assert webhook.sent == 1
