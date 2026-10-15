from abc import ABC
from types import TracebackType

from sqlalchemy.ext.asyncio import AsyncSession


class AbstractUnitOfWork(ABC):
    """Граница транзакции: старт/коммит/откат поверх AsyncSession."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def __aenter__(self) -> "AbstractUnitOfWork":
        # Явный старт транзакции — все операции query входят в один блок.
        await self.session.begin()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if exc_type is not None:
            await self.rollback()
        else:
            await self.commit()

    async def commit(self) -> None:
        """Подтверждение транзакции."""
        await self.session.commit()

    async def rollback(self) -> None:
        """Откат транзакции."""
        await self.session.rollback()
