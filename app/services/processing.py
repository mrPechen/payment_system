import uuid
from dataclasses import dataclass
from typing import Literal, Protocol

from app.services.webhook import PermanentWebhookError, TransientWebhookError

ACTION_DONE = "done"
ACTION_RETRY = "retry"
ACTION_DLQ = "dlq"


@dataclass
class ProcessingResult:
    action: Literal["done", "retry", "dlq"]
    reason: str | None = None


class PaymentRepository(Protocol):
    async def load(self, payment_id: uuid.UUID): ...
    async def mark_processed(self, payment, status: str) -> bool: ...
    async def mark_webhook_delivered(self, payment) -> None: ...
    async def increment_webhook_attempts(self, payment) -> None: ...


class Gateway(Protocol):
    async def process(self) -> bool: ...


class Webhook(Protocol):
    async def send(self, payment) -> None: ...


class ProcessingService:
    def __init__(self, repo: PaymentRepository, gateway: Gateway, webhook: Webhook):
        self.repo = repo
        self.gateway = gateway
        self.webhook = webhook

    async def process(self, payment_id: uuid.UUID) -> ProcessingResult:
        payment = await self.repo.load(payment_id)
        if payment is None:
            return ProcessingResult(ACTION_DLQ, reason="not_found")

        if payment.status.value == "pending":
            result = await self.gateway.process()
            status = "succeeded" if result else "failed"
            won = await self.repo.mark_processed(payment, status)
            if not won:
                payment = await self.repo.load(payment_id)

        if payment.webhook_delivered_at is None:
            await self.repo.increment_webhook_attempts(payment)
            try:
                await self.webhook.send(payment)
            except PermanentWebhookError as exc:
                return ProcessingResult(ACTION_DLQ, reason=str(exc))
            except TransientWebhookError as exc:
                return ProcessingResult(ACTION_RETRY, reason=str(exc))
            await self.repo.mark_webhook_delivered(payment)

        return ProcessingResult(ACTION_DONE)
