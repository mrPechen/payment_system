from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from config import settings
from src.database.models import Base

# NullPool — новое подключение на каждый запрос вместо пула: проще жить под asyncpg.
engine = create_async_engine(settings.DATABASE_URL, echo=False, poolclass=NullPool)

# expire_on_commit=False — ORM-объекты остаются валидными после commit (см. design 4.5).
SessionLocal = async_sessionmaker(
    autocommit=False,
    autoflush=False,
    class_=AsyncSession,
    bind=engine,
    expire_on_commit=False,
)

__all__ = ["Base", "SessionLocal", "engine"]
