import uuid
from datetime import UTC, datetime

from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.enums import PaymentStatus
from app.models.payment import Payment


class SqlPaymentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def load(self, payment_id: uuid.UUID) -> Payment | None:
        return await self.session.get(Payment, payment_id)

    async def mark_processed(self, payment: Payment, status: str) -> bool:
        # Условный UPDATE (WHERE status='pending') делает обработку идемпотентной:
        # повторная доставка сообщения не перезапишет уже терминальный статус.
        now = datetime.now(UTC)
        result = await self.session.execute(
            update(Payment)
            .where(Payment.id == payment.id, Payment.status == "pending")
            .values(status=status, processed_at=now)
            .returning(Payment.id)
        )
        won = result.first() is not None
        await self.session.commit()
        if won:
            # Core UPDATE синхронизирует статус как сырую строку, а вебхуку
            # (build_body) нужен enum — явно приводим объект в согласованное состояние.
            payment.status = PaymentStatus(status)
            payment.processed_at = now
        return won

    async def mark_webhook_delivered(self, payment: Payment) -> None:
        await self.session.execute(
            update(Payment)
            .where(Payment.id == payment.id)
            .values(webhook_delivered_at=datetime.now(UTC))
        )
        await self.session.commit()

    async def increment_webhook_attempts(self, payment: Payment) -> None:
        await self.session.execute(
            update(Payment)
            .where(Payment.id == payment.id)
            .values(webhook_attempts=Payment.webhook_attempts + 1)
        )
        await self.session.commit()
