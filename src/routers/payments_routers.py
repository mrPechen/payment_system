from uuid import UUID

from fastapi import APIRouter, Depends, Header, status

from src.dependencies.auth import require_api_key
from src.handlers.payment_handlers import PaymentHandler, get_payment_handler
from src.schemas.payment_schemas import (
    PaymentCreateRequest,
    PaymentCreateResponse,
    PaymentDetailResponse,
)

router = APIRouter(
    prefix="/api/v1/payments",
    tags=["payments"],
    dependencies=[Depends(require_api_key)],
)


@router.post("", status_code=status.HTTP_202_ACCEPTED)
async def create_payment_endpoint(
    payload: PaymentCreateRequest,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    handler: PaymentHandler = Depends(get_payment_handler),
) -> PaymentCreateResponse:
    """Создание платежа (тонкий роут: Depends + вызов handler-а)."""
    return await handler.create_payment(
        payload=payload.model_dump(),
        idempotency_key=idempotency_key,
    )


@router.get("/{payment_id}")
async def get_payment_endpoint(
    payment_id: UUID,
    handler: PaymentHandler = Depends(get_payment_handler),
) -> PaymentDetailResponse:
    """Получение платежа по идентификатору (тонкий роут)."""
    return await handler.get_payment(payment_id)
