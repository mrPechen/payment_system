from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field, HttpUrl, field_validator

from src.database.models import Currency, PaymentStatus


class PaymentCreateRequest(BaseModel):
    """Входной DTO создания платежа.

    Внешний API остаётся snake_case (design 4.1), поэтому используется обычный
    BaseModel, а не CamelSchema.
    """

    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=2)
    currency: Currency
    description: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: HttpUrl

    @field_validator("webhook_url")
    @classmethod
    def require_https(cls, value: HttpUrl) -> HttpUrl:
        # Требование ТЗ: webhook принимается только по защищённому протоколу.
        if value.scheme != "https":
            raise ValueError("webhook_url must use https")
        return value


class PaymentCreateResponse(BaseModel):
    """Ответ на создание платежа (202)."""

    payment_id: UUID
    status: PaymentStatus
    created_at: datetime


class PaymentDetailResponse(BaseModel):
    """Детальная информация о платеже."""

    payment_id: UUID
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    status: PaymentStatus
    created_at: datetime
    processed_at: datetime | None
    webhook_delivered_at: datetime | None
