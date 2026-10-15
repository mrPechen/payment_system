from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest

from app.services.signature import sign_payload
from app.services.webhook import (
    PermanentWebhookError,
    TransientWebhookError,
    WebhookSender,
)


def make_payment():
    return SimpleNamespace(
        id=uuid4(),
        status=SimpleNamespace(value="succeeded"),
        amount=Decimal("100.50"),
        currency=SimpleNamespace(value="RUB"),
        processed_at=datetime(2026, 10, 15, tzinfo=UTC),
        webhook_url="https://client.example/hook",
    )


async def test_sends_signed_payload_and_returns():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["body"] = request.content
        seen["sig"] = request.headers["X-Signature"]
        seen["ts"] = request.headers["X-Timestamp"]
        return httpx.Response(200)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    sender = WebhookSender(client=client, secret="sec", retry_delays=(0.0, 0.0))
    await sender.send(make_payment())

    assert seen["sig"] == f"sha256={sign_payload('sec', seen['body'])}"
    assert seen["ts"]


async def test_retries_on_500_then_succeeds():
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(500 if calls["n"] == 1 else 200)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    sender = WebhookSender(client=client, secret="sec", retry_delays=(0.0, 0.0))
    await sender.send(make_payment())
    assert calls["n"] == 2


async def test_permanent_error_on_400():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    sender = WebhookSender(client=client, secret="sec", retry_delays=(0.0, 0.0))
    with pytest.raises(PermanentWebhookError):
        await sender.send(make_payment())


async def test_transient_error_after_all_attempts_exhausted():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    sender = WebhookSender(client=client, secret="sec", retry_delays=(0.0, 0.0))
    with pytest.raises(TransientWebhookError):
        await sender.send(make_payment())
