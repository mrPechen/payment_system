from uuid import UUID

from faststream.rabbit import RabbitBroker

from src.messaging.topology import (
    DLQ,
    DLX_EXCHANGE,
    MAX_ATTEMPTS,
    PAYMENTS_EXCHANGE,
    RETRY_QUEUES,
)

EVENT_TYPE = "payment.created"


def _message(payment_id: UUID, retry_count: int) -> dict:
    """Тело сообщения о платеже (идентично для new/retry/dlq)."""
    return {
        "payment_id": str(payment_id),
        "event_type": EVENT_TYPE,
        "retry_count": retry_count,
    }


async def publish_new(broker: RabbitBroker, payment_id: UUID) -> None:
    """Публикация события создания платежа в очередь payments.new."""
    await broker.publish(
        _message(payment_id, 0),
        exchange=PAYMENTS_EXCHANGE,
        routing_key="new",
        headers={"x-retry-count": 0},
    )


async def publish_retry(
    broker: RabbitBroker, payment_id: UUID, next_count: int
) -> None:
    """Публикация отложенного повтора в retry-очередь с TTL (backoff)."""
    queue = RETRY_QUEUES[next_count - 1]
    await broker.publish(
        _message(payment_id, next_count),
        exchange=PAYMENTS_EXCHANGE,
        routing_key=queue.routing_key,
        headers={"x-retry-count": next_count},
    )


async def publish_dlq(broker: RabbitBroker, payment_id: UUID, reason: str) -> None:
    """Публикация «ядовитого» сообщения в DLQ с причиной."""
    # DLQ привязана к DLX_EXCHANGE с ключом "dlq", поэтому публикуем через обмен,
    # а не напрямую в очередь (иначе сообщение уйдёт в default exchange и потеряется).
    await broker.publish(
        _message(payment_id, MAX_ATTEMPTS),
        exchange=DLX_EXCHANGE,
        routing_key=DLQ.routing_key,
        headers={"x-reason": reason},
    )
