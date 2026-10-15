from collections.abc import AsyncGenerator

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from src.dependencies.db import get_async_session
from src.services.queries.outbox_queries import OutboxQuery
from src.services.queries.payment_queries import PaymentQuery
from src.services.uow.base import AbstractUnitOfWork


class PaymentsUnitOfWork(AbstractUnitOfWork):
    """Unit of Work контекста платежей: транзакция + привязанные query-классы."""

    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)
        self.payment = PaymentQuery(session)
        self.outbox = OutboxQuery(session)


async def get_payments_uow(
    session: AsyncSession = Depends(get_async_session),
) -> AsyncGenerator[PaymentsUnitOfWork, None]:
    """
    Depends-фабрика: оборачивает сессию в UoW и стартует транзакцию.

    :param session: AsyncSession из get_async_session
    :return: асинхронный генератор PaymentsUnitOfWork
    """
    async with PaymentsUnitOfWork(session) as uow:
        yield uow
