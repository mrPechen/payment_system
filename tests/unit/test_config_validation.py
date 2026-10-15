from app.config import Settings


def test_success_rate_is_validated():
    s = Settings(gateway_success_rate=0.9, api_key="k", webhook_signing_secret="s")
    assert s.gateway_success_rate == 0.9


def test_batch_size_defaults():
    s = Settings(api_key="k", webhook_signing_secret="s")
    assert s.outbox_batch_size == 100
