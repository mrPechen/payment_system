import asyncio
import logging
from collections.abc import Awaitable, Callable
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from config import settings
from src.database.db_settings import SessionLocal
from src.messaging import publisher
from src.messaging.broker import broker
from src.messaging.topology import declare_topology
from src.services.uow.payments_uow import PaymentsUnitOfWork

logger = logging.getLogger(__name__)


async def publish_batch(
    session: AsyncSession,
    publish: Callable[[UUID], Awaitable[None]],
    batch_size: int,
) -> int:
    """
    Публикация одного батча неопубликованных outbox-событий.

    :param session: AsyncSession (внешняя, из SessionLocal)
    :param publish: асинхронная функция публикации одного события
    :param batch_size: максимальный размер батча
    :return: число обработанных событий
    """
    async with PaymentsUnitOfWork(session) as uow:
        rows = await uow.outbox.get_unpublished(batch_size)
        for row in rows:
            payment_id = UUID(row.payload["payment_id"])
            await publish(payment_id)
            await uow.outbox.mark_published(row)
        # Единый коммит: публикация + published_at атомарны в рамках батча.
        await uow.commit()
    return len(rows)


async def _run() -> None:
    """Бесконечный цикл outbox-воркера: пачка → пауза → пачка."""
    async with broker:
        await declare_topology(broker)
        while True:
            async with SessionLocal() as session:
                try:
                    n = await publish_batch(
                        session,
                        lambda pid: publisher.publish_new(broker, pid),
                        settings.OUTBOX_BATCH_SIZE,
                    )
                    if n:
                        logger.info(f"published outbox batch count={n}")
                except Exception:
                    logger.exception("outbox batch failed")
                    await session.rollback()
            await asyncio.sleep(settings.OUTBOX_POLL_INTERVAL)


def main() -> None:
    """Точка входа outbox-воркера."""
    asyncio.run(_run())


if __name__ == "__main__":
    main()
