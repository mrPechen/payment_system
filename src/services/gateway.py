import asyncio
import random

from config import settings


class PaymentGateway:
    """Доменный сервис — симуляция платёжного шлюза (без обращения к БД)."""

    def __init__(
        self,
        min_delay: float | None = None,
        max_delay: float | None = None,
        success_rate: float | None = None,
    ) -> None:
        # Опциональные переопределения нужны тестам: нулевые задержки и
        # детерминированная вероятность успеха.
        self.min_delay = settings.GATEWAY_MIN_DELAY if min_delay is None else min_delay
        self.max_delay = settings.GATEWAY_MAX_DELAY if max_delay is None else max_delay
        self.success_rate = (
            settings.GATEWAY_SUCCESS_RATE if success_rate is None else success_rate
        )

    async def process(self) -> bool:
        """
        Симуляция проведения платежа.

        :return: True при успехе, False при отказе
        """
        await asyncio.sleep(random.uniform(self.min_delay, self.max_delay))
        return random.random() < self.success_rate
