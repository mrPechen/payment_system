from uuid import uuid4

import pytest

pytestmark = pytest.mark.integration

HEADERS = {"X-API-Key": "test-key"}

BODY = {
    "amount": "100.50",
    "currency": "RUB",
    "description": "order",
    "metadata": {"order_id": "1"},
    "webhook_url": "https://client.example/hook",
}


async def test_requires_api_key(client):
    resp = await client.post(
        "/api/v1/payments", json=BODY, headers={"Idempotency-Key": "k"}
    )
    assert resp.status_code == 401


async def test_create_and_get_payment(client):
    resp = await client.post(
        "/api/v1/payments", json=BODY, headers={**HEADERS, "Idempotency-Key": "k1"}
    )
    assert resp.status_code == 202
    payment_id = resp.json()["payment_id"]

    got = await client.get(f"/api/v1/payments/{payment_id}", headers=HEADERS)
    assert got.status_code == 200
    assert got.json()["status"] == "pending"
    assert got.json()["amount"] == "100.50"


async def test_duplicate_idempotency_key_returns_same_payment(client):
    r1 = await client.post(
        "/api/v1/payments", json=BODY, headers={**HEADERS, "Idempotency-Key": "same"}
    )
    r2 = await client.post(
        "/api/v1/payments",
        json={**BODY, "amount": "1.00"},
        headers={**HEADERS, "Idempotency-Key": "same"},
    )
    assert r1.status_code == 202 and r2.status_code == 202
    assert r1.json()["payment_id"] == r2.json()["payment_id"]


async def test_missing_idempotency_key_is_422(client):
    resp = await client.post("/api/v1/payments", json=BODY, headers=HEADERS)
    assert resp.status_code == 422


async def test_get_missing_payment_is_404(client):
    resp = await client.get(f"/api/v1/payments/{uuid4()}", headers=HEADERS)
    assert resp.status_code == 404


async def test_webhook_url_requires_https(client):
    resp = await client.post(
        "/api/v1/payments",
        json={**BODY, "webhook_url": "http://client.example/hook"},
        headers={**HEADERS, "Idempotency-Key": "k2"},
    )
    assert resp.status_code == 422
