from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession

from src.database.db_settings import SessionLocal


async def get_async_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Генератор AsyncSession: создаёт сессию на время запроса и закрывает после.

    :return: асинхронный генератор сессий
    """
    async with SessionLocal() as session:
        yield session
