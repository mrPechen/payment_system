from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.models import OutboxMessage


class OutboxQuery:
    """Чистые SQLAlchemy-запросы к таблице outbox (без бизнес-логики)."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get_unpublished(self, batch_size: int) -> list[OutboxMessage]:
        """
        Выборка неопубликованных событий с блокировкой FOR UPDATE SKIP LOCKED.

        :param batch_size: максимальный размер батча
        :return: список ORM-объектов событий outbox
        """
        result = await self.session.scalars(
            select(OutboxMessage)
            .where(OutboxMessage.published_at.is_(None))
            .order_by(OutboxMessage.created_at)
            .limit(batch_size)
            .with_for_update(skip_locked=True)
        )
        return list(result.all())

    async def mark_published(self, row: OutboxMessage) -> None:
        """
        Пометить событие как опубликованное (published_at в UTC).

        :param row: ORM-объект события outbox
        """
        row.published_at = datetime.now(UTC)
