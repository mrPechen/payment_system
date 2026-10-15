import uuid
from collections.abc import AsyncGenerator
from typing import Any

from fastapi import Depends, HTTPException

from src.database.models import Payment
from src.schemas.payment_schemas import PaymentCreateResponse, PaymentDetailResponse
from src.services.uow.payments_uow import PaymentsUnitOfWork, get_payments_uow
from src.utils.exceptions import AlreadyExistsError, NotFoundError


class PaymentHandler:
    """Оркестрация создания и получения платежа (без прямого доступа к session)."""

    def __init__(self, uow: PaymentsUnitOfWork) -> None:
        self.uow = uow

    async def create_payment(
        self, payload: dict[str, Any], idempotency_key: str | None
    ) -> PaymentCreateResponse:
        """
        Создание платежа с идемпотентностью по Idempotency-Key.

        :param payload: поля платежа (model_dump тела запроса)
        :param idempotency_key: значение заголовка Idempotency-Key
        :return: ответ создания платежа (202)
        :raises HTTPException: 422 без ключа, 409 при неразрешимой гонке ключа
        """
        # === ШАГ 1: обязательный Idempotency-Key ===
        if not idempotency_key:
            raise HTTPException(
                status_code=422,
                detail="Idempotency-Key header is required",
            )

        # === ШАГ 2: повторный запрос с тем же ключом → возвращаем существующий ===
        existing = await self.uow.payment.get_by_idempotency_key(idempotency_key)
        if existing is not None:
            return self._build_create_response(existing)

        # === ШАГ 3: вставка платежа + outbox-события в одной транзакции ===
        try:
            payment = await self.uow.payment.create(
                {**payload, "idempotency_key": idempotency_key}
            )
            self.uow.outbox.add(payment)
        except AlreadyExistsError:
            # Гонка: повторный SELECT различает "платёж уже есть" (202) и
            # "конкурент вставил, но ещё не закоммитил" (409) — design 4.3.
            existing = await self.uow.payment.get_by_idempotency_key(idempotency_key)
            if existing is not None:
                return self._build_create_response(existing)
            raise HTTPException(
                status_code=409,
                detail="Concurrent request with the same Idempotency-Key",
            ) from None

        return self._build_create_response(payment)

    async def get_payment(self, payment_id: uuid.UUID) -> PaymentDetailResponse:
        """
        Получение платежа по идентификатору.

        :param payment_id: UUID платежа
        :return: детальная информация о платеже
        :raises HTTPException: 404 если платёж не найден
        """
        try:
            payment = await self.uow.payment.get_by_id(payment_id)
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from None
        return self._build_detail_response(payment)

    def _build_create_response(self, payment: Payment) -> PaymentCreateResponse:
        """Маппинг ORM-объекта в DTO создания платежа."""
        return PaymentCreateResponse(
            payment_id=payment.id,
            status=payment.status,
            created_at=payment.created_at,
        )

    def _build_detail_response(self, payment: Payment) -> PaymentDetailResponse:
        """Маппинг ORM-объекта в DTO деталей (metadata_ → metadata)."""
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


async def get_payment_handler(
    uow: PaymentsUnitOfWork = Depends(get_payments_uow),
) -> AsyncGenerator[PaymentHandler, None]:
    """
    Depends-фабрика: создаёт PaymentHandler с UoW из DI-цепочки.

    :param uow: PaymentsUnitOfWork из get_payments_uow
    :return: асинхронный генератор PaymentHandler
    """
    yield PaymentHandler(uow)
