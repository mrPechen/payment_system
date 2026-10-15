import asyncio
import logging
import uuid

import httpx
from faststream import FastStream
from faststream.rabbit import RabbitMessage, RabbitRouter
from pydantic import BaseModel

from config import settings
from src.database.db_settings import SessionLocal
from src.handlers.processing_handlers import (
    ACTION_DLQ,
    ACTION_RETRY,
    ProcessingHandler,
    ProcessingResult,
)
from src.messaging.broker import broker
from src.messaging.publisher import publish_dlq, publish_retry
from src.messaging.topology import (
    MAX_ATTEMPTS,
    NEW_QUEUE,
    PAYMENTS_EXCHANGE,
    declare_topology,
)
from src.services.gateway import PaymentGateway
from src.services.uow.payments_uow import PaymentsUnitOfWork
from src.utils.http_utils import WebhookSender

logger = logging.getLogger(__name__)
router = RabbitRouter()


class PaymentEvent(BaseModel):
    """Событие платежа из шины сообщений."""

    payment_id: uuid.UUID
    event_type: str
    retry_count: int = 0


def decide_route(
    retry_count: int, result: ProcessingResult
) -> tuple[str, int | None, str | None]:
    """Решение по маршрутизации сообщения: ack / retry / dlq."""
    if result.action == ACTION_RETRY:
        if retry_count + 1 >= MAX_ATTEMPTS:
            return ("dlq", None, result.reason or "retries_exhausted")
        return ("retry", retry_count + 1, None)
    if result.action == ACTION_DLQ:
        return ("dlq", None, result.reason or "unknown")
    return ("ack", None, None)


@router.subscriber(NEW_QUEUE, exchange=PAYMENTS_EXCHANGE)
async def handle_new_payment(event: PaymentEvent, message: RabbitMessage) -> None:
    """Обработчик события создания платежа (gateway + webhook через handler)."""
    retry_count = int((message.headers or {}).get("x-retry-count", event.retry_count))

    async with httpx.AsyncClient(timeout=settings.WEBHOOK_TIMEOUT) as client:
        webhook = WebhookSender(client=client, secret=settings.WEBHOOK_SIGNING_SECRET)
        async with SessionLocal() as session:
            async with PaymentsUnitOfWork(session) as uow:
                handler = ProcessingHandler(
                    uow=uow,
                    gateway=PaymentGateway(),
                    webhook=webhook,
                )
                result = await handler.process(event.payment_id)

    route, next_count, reason = decide_route(retry_count, result)
    if route == "retry":
        logger.info(f"scheduling retry payment_id={event.payment_id} next={next_count}")
        await publish_retry(broker, event.payment_id, next_count)
    elif route == "dlq":
        logger.error(f"dropping to dlq payment_id={event.payment_id} reason={reason}")
        await publish_dlq(broker, event.payment_id, reason)


async def _declare_topology() -> None:
    """Объявление retry/DLQ-очередей после старта брокера."""
    # FastStream 0.7.7 подключает роутер к брокеру через include_router, а
    # declare_topology объявляет retry/DLQ-очереди уже после старта брокера
    # (after_startup выполняется после broker.start()).
    await declare_topology(broker)


async def _run() -> None:
    """Запуск FastStream-приложения с подпиской на payments.new."""
    # В FastStream 0.7.7 у FastStream нет include_router — роутер подключаем
    # к брокеру напрямую, а топологию объявляем в after_startup.
    broker.include_router(router)
    app = FastStream(broker, after_startup=[_declare_topology])
    await app.run()


def main() -> None:
    """Точка входа консьюмера."""
    asyncio.run(_run())


if __name__ == "__main__":
    main()
