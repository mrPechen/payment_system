from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

from config import settings
from src.routers.health_routers import router as health_router
from src.routers.payments_routers import router as payments_router
from telemetry.setup_telemetry import setup_telemetry, shutdown_telemetry


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Жизненный цикл: телеметрия включается только в проде (DEBUG=False)."""
    if settings.DEBUG is False:
        setup_telemetry(
            service_name=settings.SERVICE_NAME,
            otlp_endpoint=settings.OTLP_ENDPOINT,
        )
        FastAPIInstrumentor.instrument_app(app)
    yield
    if settings.DEBUG is False:
        shutdown_telemetry()


def create_app() -> FastAPI:
    """Сборка FastAPI-приложения: lifespan + регистрация роутеров."""
    app = FastAPI(title="Payment System", version="0.1.0", lifespan=lifespan)
    app.include_router(health_router)
    app.include_router(payments_router)
    return app


app = create_app()
