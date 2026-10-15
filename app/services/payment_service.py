from decimal import Decimal
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.enums import Currency
from app.models.outbox import OutboxMessage
from app.models.payment import Payment

PAYMENT_CREATED_EVENT = "payment.created"


class DuplicateKeyRace(Exception):
    """Raised when a concurrent insert with the same key cannot be resolved."""


def _to_decimal(amount) -> Decimal:
    return Decimal(str(amount))


async def create_payment(
    session: AsyncSession,
    *,
    amount,
    currency: str,
    description: str,
    metadata: dict,
    webhook_url: str,
    idempotency_key: str,
) -> Payment:
    existing = await get_payment_by_key(session, idempotency_key)
    if existing is not None:
        return existing

    payment = Payment(
        amount=_to_decimal(amount),
        currency=Currency(currency),
        description=description,
        metadata_=metadata or {},
        webhook_url=webhook_url,
        idempotency_key=idempotency_key,
    )
    session.add(payment)
    try:
        await session.flush()
        session.add(
            OutboxMessage(
                event_type=PAYMENT_CREATED_EVENT,
                aggregate_id=payment.id,
                payload={"payment_id": str(payment.id)},
            )
        )
        await session.commit()
    except IntegrityError:
        await session.rollback()
        existing = await get_payment_by_key(session, idempotency_key)
        if existing is None:
            raise DuplicateKeyRace from None
        return existing

    await session.refresh(payment)
    return payment


async def get_payment_by_key(session: AsyncSession, key: str) -> Payment | None:
    return await session.scalar(select(Payment).where(Payment.idempotency_key == key))


async def get_payment(session: AsyncSession, payment_id: UUID) -> Payment | None:
    return await session.get(Payment, payment_id)
