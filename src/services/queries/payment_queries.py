import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.models import Currency, Payment, PaymentStatus
from src.utils.exceptions import AlreadyExistsError, NotFoundError


def _to_decimal(amount: Any) -> Decimal:
    """Нормализация суммы в Decimal через строку (защита от float-артефактов)."""
    return Decimal(str(amount))


class PaymentQuery:
    """Чистые SQLAlchemy-запросы к таблице payments (без бизнес-логики)."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get_by_idempotency_key(self, key: str) -> Payment | None:
        """
        Поиск платежа по ключу идемпотентности.

        :param key: значение Idempotency-Key
        :return: платёж или None, если не найден
        """
        return await self.session.scalar(
            select(Payment).where(Payment.idempotency_key == key)
        )

    async def get_by_id(self, payment_id: uuid.UUID) -> Payment:
        """
        Получение платежа по идентификатору.

        :param payment_id: UUID платежа
        :return: найденный платёж
        :raises NotFoundError: если платёж не найден
        """
        payment = await self.session.get(Payment, payment_id)
        if payment is None:
            raise NotFoundError(f"Платёж {payment_id} не найден")
        return payment

    async def create(self, data: dict[str, Any]) -> Payment:
        """
        Вставка платежа; коммит откладывается на UoW.__aexit__.

        Гонка по уникальному idempotency_key ловится на IntegrityError и мапится
        в AlreadyExistsError (повторный SELECT делает handler, см. design 4.3).

        :param data: поля платежа (amount/currency/description/metadata/webhook_url/idempotency_key)
        :return: созданный платёж с заполненным created_at
        :raises AlreadyExistsError: при конфликте уникального idempotency_key
        """
        payment = Payment(
            amount=_to_decimal(data["amount"]),
            currency=Currency(data["currency"]),
            description=data.get("description", ""),
            metadata_=data.get("metadata") or {},
            webhook_url=str(data["webhook_url"]),
            idempotency_key=data["idempotency_key"],
        )
        self.session.add(payment)
        try:
            await self.session.flush()
        except IntegrityError:
            await self.session.rollback()
            raise AlreadyExistsError(
                "Concurrent request with the same Idempotency-Key"
            ) from None
        # created_at задаёт БД (server_default=func.now()), поэтому после flush
        # ещё пусто — refresh подтягивает значение до коммита (design 4.4).
        await self.session.refresh(payment)
        return payment

    async def mark_processed(self, payment: Payment, status: str) -> bool:
        """
        Условный UPDATE статуса (только из 'pending') — идемпотентная обработка.

        :param payment: ORM-объект платежа
        :param status: новый статус ("succeeded"/"failed")
        :return: True, если UPDATE победил гонку (статус был pending)
        """
        now = datetime.now(UTC)
        result = await self.session.execute(
            update(Payment)
            .where(Payment.id == payment.id, Payment.status == "pending")
            .values(status=status, processed_at=now)
            .returning(Payment.id)
        )
        won = result.first() is not None
        if won:
            # Core UPDATE пишет статус строкой, а webhook.build_body ждёт enum —
            # синхронизируем ORM-объект в согласованное состояние.
            payment.status = PaymentStatus(status)
            payment.processed_at = now
        return won

    async def mark_webhook_delivered(self, payment: Payment) -> None:
        """
        Фиксация времени успешной доставки webhook.

        :param payment: ORM-объект платежа
        """
        await self.session.execute(
            update(Payment)
            .where(Payment.id == payment.id)
            .values(webhook_delivered_at=datetime.now(UTC))
        )

    async def increment_webhook_attempts(self, payment: Payment) -> None:
        """
        Атомарный инкремент счётчика попыток доставки webhook.

        :param payment: ORM-объект платежа
        """
        await self.session.execute(
            update(Payment)
            .where(Payment.id == payment.id)
            .values(webhook_attempts=Payment.webhook_attempts + 1)
        )
