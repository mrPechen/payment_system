import time

from app.services.gateway import PaymentGateway


async def test_always_succeeds_when_rate_is_one():
    gw = PaymentGateway(min_delay=0.0, max_delay=0.0, success_rate=1.0)
    assert await gw.process() is True


async def test_always_fails_when_rate_is_zero():
    gw = PaymentGateway(min_delay=0.0, max_delay=0.0, success_rate=0.0)
    assert await gw.process() is False


async def test_delay_within_bounds():
    gw = PaymentGateway(min_delay=0.05, max_delay=0.05, success_rate=1.0)
    start = time.monotonic()
    await gw.process()
    assert time.monotonic() - start >= 0.05
