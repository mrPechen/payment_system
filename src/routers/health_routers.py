from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from src.dependencies.db import get_async_session

router = APIRouter(tags=["health"])


@router.get("/health")
async def health() -> dict[str, str]:
    """Liveness-пробник: приложение запущено."""
    return {"status": "ok"}


@router.get("/health/ready")
async def ready(
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, str]:
    """Readiness-пробник: SELECT 1 напрямую через сессию (design 4.7)."""
    await session.execute(text("SELECT 1"))
    return {"status": "ready"}
