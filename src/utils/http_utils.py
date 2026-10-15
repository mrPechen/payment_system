import asyncio
import hashlib
import hmac
import json
from datetime import UTC, datetime

import httpx


class PermanentWebhookError(Exception):
    """Невозвратная ошибка доставки webhook (например, 4xx)."""


class TransientWebhookError(Exception):
    """Возвратная ошибка доставки webhook (5xx/таймаут) после всех попыток."""


def sign_payload(secret: str, body: bytes) -> str:
    """
    HMAC-SHA256 подпись сырого тела для заголовка X-Signature.

    :param secret: секрет подписи webhook
    :param body: сырое тело запроса (до повторной сериализации)
    :return: hex-строка подписи
    """
    return hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


class WebhookSender:
    """Отправка webhook с классификацией ошибок и внутренним retry."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        secret: str,
        retry_delays: tuple[float, ...] = (1.0, 2.0),
    ) -> None:
        self.client = client
        self.secret = secret
        self.retry_delays = retry_delays

    def build_body(self, payment) -> bytes:
        # Детерминированная сериализация (sort_keys): подпись считается от этих байт.
        payload = {
            "payment_id": str(payment.id),
            "status": payment.status.value,
            "amount": str(payment.amount),
            "currency": payment.currency.value,
            "processed_at": payment.processed_at.isoformat()
            if payment.processed_at
            else None,
        }
        return json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()

    async def send(self, payment) -> None:
        body = self.build_body(payment)
        headers = {
            "Content-Type": "application/json",
            "X-Signature": f"sha256={sign_payload(self.secret, body)}",
            "X-Timestamp": datetime.now(UTC).isoformat(),
        }
        last_error: Exception | None = None
        attempts = 1 + len(self.retry_delays)
        for attempt in range(attempts):
            if attempt > 0:
                await asyncio.sleep(self.retry_delays[attempt - 1])
            try:
                response = await self.client.post(
                    payment.webhook_url, content=body, headers=headers
                )
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                last_error = exc
                continue

            # 429 и 5xx — временные ошибки (можно повторить), остальные 4xx — нет.
            if response.status_code == 429 or response.status_code >= 500:
                last_error = TransientWebhookError(f"status {response.status_code}")
                continue
            if response.status_code >= 400:
                raise PermanentWebhookError(f"status {response.status_code}")
            return

        raise TransientWebhookError("all attempts failed") from last_error
