import hashlib
import hmac

from app.services.signature import sign_payload


def test_signs_raw_body_with_hmac_sha256():
    body = b'{"a":1}'
    expected = hmac.new(b"secret", body, hashlib.sha256).hexdigest()
    assert sign_payload("secret", body) == expected


def test_signature_is_deterministic():
    assert sign_payload("s", b"x") == sign_payload("s", b"x")


def test_different_secret_changes_signature():
    assert sign_payload("a", b"x") != sign_payload("b", b"x")
