import uuid
from dataclasses import dataclass
from typing import Literal, Protocol

from src.services.uow.payments_uow import PaymentsUnitOfWork
from src.utils.exceptions import NotFoundError
from src.utils.http_utils import PermanentWebhookError, TransientWebhookError

ACTION_DONE = "done"
ACTION_RETRY = "retry"
ACTION_DLQ = "dlq"


@dataclass
class ProcessingResult:
    """Итог обработки события: что делать с сообщением в шине."""

    action: Literal["done", "retry", "dlq"]
    reason: str | None = None


class Gateway(Protocol):
    """Контракт платёжного шлюза (доменный сервис без БД)."""

    async def process(self) -> bool: ...


class Webhook(Protocol):
    """Контракт отправителя webhook."""

    async def send(self, payment) -> None: ...


class ProcessingHandler:
    """
    Оркестрация обработки платежа: gateway → статус → webhook.

    Статус коммитится ДО отправки webhook (явные точки commit), чтобы при
    transient-ошибке webhook терминальный статус сохранялся, а retry повторял
    только доставку (design 4.5).
    """

    def __init__(
        self, uow: PaymentsUnitOfWork, gateway: Gateway, webhook: Webhook
    ) -> None:
        self.uow = uow
        self.gateway = gateway
        self.webhook = webhook

    async def process(self, payment_id: uuid.UUID) -> ProcessingResult:
        """
        Обработка одного платежа: шлюз, фиксация статуса, доставка webhook.

        :param payment_id: UUID платежа
        :return: ProcessingResult с решением по маршрутизации сообщения
        """
        # === ШАГ 1: загрузка платежа (not_found → DLQ без retry) ===
        try:
            payment = await self.uow.payment.get_by_id(payment_id)
        except NotFoundError:
            return ProcessingResult(ACTION_DLQ, reason="not_found")

        # === ШАГ 2: проведение в gateway (только из pending) ===
        if payment.status.value == "pending":
            result = await self.gateway.process()
            status = "succeeded" if result else "failed"
            won = await self.uow.payment.mark_processed(payment, status)
            # Статус коммитится до webhook — критично для поведения (design 4.5).
            await self.uow.commit()
            if not won:
                # Проиграли гонку: перечитываем актуальное состояние победителя.
                payment = await self.uow.payment.get_by_id(payment_id)

        # === ШАГ 3: доставка webhook (с фиксацией счётчика попыток) ===
        if payment.webhook_delivered_at is None:
            await self.uow.payment.increment_webhook_attempts(payment)
            await self.uow.commit()
            try:
                await self.webhook.send(payment)
            except PermanentWebhookError as exc:
                return ProcessingResult(ACTION_DLQ, reason=str(exc))
            except TransientWebhookError as exc:
                return ProcessingResult(ACTION_RETRY, reason=str(exc))
            await self.uow.payment.mark_webhook_delivered(payment)
            await self.uow.commit()

        return ProcessingResult(ACTION_DONE)
