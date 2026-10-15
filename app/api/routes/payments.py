from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import require_api_key
from app.db import get_session
from app.schemas.payment import (
    PaymentCreateRequest,
    PaymentCreateResponse,
    PaymentDetailResponse,
)
from app.services.payment_service import (
    DuplicateKeyRace,
    create_payment,
    get_payment,
)

router = APIRouter(
    prefix="/api/v1/payments", tags=["payments"], dependencies=[Depends(require_api_key)]
)


@router.post("", status_code=status.HTTP_202_ACCEPTED)
async def create_payment_endpoint(
    payload: PaymentCreateRequest,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    session: AsyncSession = Depends(get_session),
) -> PaymentCreateResponse:
    if not idempotency_key:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Idempotency-Key header is required",
        )
    try:
        payment = await create_payment(
            session,
            amount=payload.amount,
            currency=payload.currency.value,
            description=payload.description,
            metadata=payload.metadata,
            webhook_url=str(payload.webhook_url),
            idempotency_key=idempotency_key,
        )
    except DuplicateKeyRace:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Concurrent request with the same Idempotency-Key",
        ) from None

    return PaymentCreateResponse(
        payment_id=payment.id,
        status=payment.status,
        created_at=payment.created_at,
    )


@router.get("/{payment_id}")
async def get_payment_endpoint(
    payment_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> PaymentDetailResponse:
    payment = await get_payment(session, payment_id)
    if payment is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not found")
    return PaymentDetailResponse(
        payment_id=payment.id,
        amount=payment.amount,
        currency=payment.currency,
        description=payment.description,
        metadata=payment.metadata_,
        status=payment.status,
        created_at=payment.created_at,
        processed_at=payment.processed_at,
        webhook_delivered_at=payment.webhook_delivered_at,
    )
