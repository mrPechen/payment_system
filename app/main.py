from fastapi import FastAPI

from app.api.routes.health import router as health_router
from app.api.routes.payments import router as payments_router
from app.logging import configure_logging


def create_app() -> FastAPI:
    configure_logging()
    app = FastAPI(title="Payment System", version="0.1.0")
    app.include_router(health_router)
    app.include_router(payments_router)
    return app


app = create_app()
