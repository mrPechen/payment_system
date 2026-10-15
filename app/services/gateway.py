import asyncio
import random

from app.config import get_settings


class PaymentGateway:
    def __init__(
        self,
        min_delay: float | None = None,
        max_delay: float | None = None,
        success_rate: float | None = None,
    ) -> None:
        settings = get_settings()
        self.min_delay = settings.gateway_min_delay if min_delay is None else min_delay
        self.max_delay = settings.gateway_max_delay if max_delay is None else max_delay
        self.success_rate = (
            settings.gateway_success_rate if success_rate is None else success_rate
        )

    async def process(self) -> bool:
        await asyncio.sleep(random.uniform(self.min_delay, self.max_delay))
        return random.random() < self.success_rate
