import asyncio
import uuid

import httpx
from faststream import FastStream
from faststream.rabbit import RabbitMessage, RabbitRouter
from pydantic import BaseModel

from app.config import get_settings
from app.db import SessionLocal
from app.logging import configure_logging, get_logger
from app.messaging.broker import broker
from app.messaging.publisher import publish_dlq, publish_retry
from app.messaging.topology import (
    MAX_ATTEMPTS,
    NEW_QUEUE,
    PAYMENTS_EXCHANGE,
    declare_topology,
)
from app.services.gateway import PaymentGateway
from app.services.processing import (
    ACTION_DLQ,
    ACTION_RETRY,
    ProcessingResult,
    ProcessingService,
)
from app.services.repository import SqlPaymentRepository
from app.services.webhook import WebhookSender

logger = get_logger("consumer")
router = RabbitRouter()


class PaymentEvent(BaseModel):
    payment_id: uuid.UUID
    event_type: str
    retry_count: int = 0


def decide_route(
    retry_count: int, result: ProcessingResult
) -> tuple[str, int | None, str | None]:
    if result.action == ACTION_RETRY:
        if retry_count + 1 >= MAX_ATTEMPTS:
            return ("dlq", None, result.reason or "retries_exhausted")
        return ("retry", retry_count + 1, None)
    if result.action == ACTION_DLQ:
        return ("dlq", None, result.reason or "unknown")
    return ("ack", None, None)


@router.subscriber(NEW_QUEUE, exchange=PAYMENTS_EXCHANGE)
async def handle_new_payment(event: PaymentEvent, message: RabbitMessage) -> None:
    settings = get_settings()
    retry_count = int((message.headers or {}).get("x-retry-count", event.retry_count))

    async with httpx.AsyncClient(timeout=settings.webhook_timeout) as client:
        webhook = WebhookSender(client=client, secret=settings.webhook_signing_secret)
        async with SessionLocal() as session:
            service = ProcessingService(
                repo=SqlPaymentRepository(session),
                gateway=PaymentGateway(),
                webhook=webhook,
            )
            result = await service.process(event.payment_id)

    route, next_count, reason = decide_route(retry_count, result)
    if route == "retry":
        logger.info("scheduling retry", payment_id=str(event.payment_id), next=next_count)
        await publish_retry(broker, event.payment_id, next_count)
    elif route == "dlq":
        logger.error("dropping to dlq", payment_id=str(event.payment_id), reason=reason)
        await publish_dlq(broker, event.payment_id, reason)


async def _declare_topology() -> None:
    # FastStream 0.7.7 подключает роутер к брокеру через include_router, а
    # declare_topology объявляет retry/DLQ-очереди уже после старта брокера
    # (after_startup выполняется после broker.start()).
    await declare_topology(broker)


async def _run() -> None:
    # В FastStream 0.7.7 у FastStream нет include_router — роутер подключаем
    # к брокеру напрямую, а топологию объявляем в after_startup.
    broker.include_router(router)
    app = FastStream(broker, after_startup=[_declare_topology])
    await app.run()


def main() -> None:
    configure_logging()
    asyncio.run(_run())


if __name__ == "__main__":
    main()
