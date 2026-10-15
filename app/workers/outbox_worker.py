import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.db import SessionLocal
from app.logging import configure_logging, get_logger
from app.messaging import publisher
from app.messaging.broker import broker
from app.messaging.topology import declare_topology
from app.models.outbox import OutboxMessage

logger = get_logger("outbox-worker")


async def publish_batch(
    session: AsyncSession,
    publish: Callable[[UUID], Awaitable[None]],
    batch_size: int,
) -> int:
    rows = (
        await session.scalars(
            select(OutboxMessage)
            .where(OutboxMessage.published_at.is_(None))
            .order_by(OutboxMessage.created_at)
            .limit(batch_size)
            .with_for_update(skip_locked=True)
        )
    ).all()

    for row in rows:
        payment_id = UUID(row.payload["payment_id"])
        await publish(payment_id)
        row.published_at = datetime.now(UTC)

    await session.commit()
    return len(rows)


async def _run() -> None:
    settings = get_settings()
    async with broker:
        await declare_topology(broker)
        while True:
            async with SessionLocal() as session:
                try:
                    n = await publish_batch(
                        session,
                        lambda pid: publisher.publish_new(broker, pid),
                        settings.outbox_batch_size,
                    )
                    if n:
                        logger.info("published outbox batch", count=n)
                except Exception:
                    logger.exception("outbox batch failed")
                    await session.rollback()
            await asyncio.sleep(settings.outbox_poll_interval)


def main() -> None:
    configure_logging()
    asyncio.run(_run())


if __name__ == "__main__":
    main()
