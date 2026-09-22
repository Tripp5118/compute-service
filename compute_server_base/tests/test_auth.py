"""Coverage for the two verifiers a compute server accepts (S-4).

The security-relevant claims: a token minted for another tool is rejected even
though it was signed by the same dashboard, a token signed with the wrong secret
is rejected, and the static token keeps working through the migration.

Run:
    cd compute_server_base && PYTHONPATH=. uv run --with fastapi --with pyjwt --with pytest pytest tests/test_auth.py -v
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import jwt
import pytest
from compute_server_base import auth

SECRET = "tool-secret-for-tests"
OTHER_SECRET = "a-different-tools-secret"
STATIC = "static-shared-token"


def _token(*, secret: str = SECRET, audience: str = "tool:stub", subject: str = "autobo-worker", age_s: int = 0) -> str:
    now = datetime.now(tz=UTC)
    return jwt.encode(
        {"sub": subject, "aud": audience, "iat": now, "exp": now + timedelta(seconds=3600 - age_s)},
        secret,
        algorithm="HS256",
    )


@pytest.fixture(autouse=True)
def configured(monkeypatch):
    """A server named 'stub' holding both a JWT secret and the static token."""
    monkeypatch.setenv("COMPUTE_JWT_SECRET", SECRET)
    monkeypatch.setenv("COMPUTE_SERVER_TOKEN", STATIC)
    auth.set_audience("stub")


def test_dashboard_token_identifies_its_subject():
    assert auth.authenticate(f"Bearer {_token()}") == "autobo-worker"


def test_static_token_still_works():
    """MatFlow is paused and cannot be repointed, so this path stays valid."""
    assert auth.authenticate(f"Bearer {STATIC}") == "service-token"


def test_token_minted_for_another_tool_is_rejected():
    """Same dashboard, same signature, wrong audience — this is the isolation claim."""
    assert auth.authenticate(f"Bearer {_token(audience='tool:thermocalc')}") is None


def test_token_signed_with_another_tools_secret_is_rejected():
    assert auth.authenticate(f"Bearer {_token(secret=OTHER_SECRET)}") is None


def test_expired_token_is_rejected():
    assert auth.authenticate(f"Bearer {_token(age_s=7200)}") is None


def test_token_without_an_expiry_is_rejected():
    """An unexpiring token could not be retired even by rotation-and-reissue."""
    forever = jwt.encode({"sub": "svc", "aud": "tool:stub"}, SECRET, algorithm="HS256")
    assert auth.authenticate(f"Bearer {forever}") is None


def test_garbage_and_missing_headers_are_rejected():
    assert auth.authenticate("") is None
    assert auth.authenticate("Bearer") is None
    assert auth.authenticate("Basic dXNlcjpwYXNz") is None
    assert auth.authenticate("Bearer not-a-token") is None


def test_ws_check_accepts_both_credentials():
    assert auth.check_ws_token(_token()) is True
    assert auth.check_ws_token(STATIC) is True
    assert auth.check_ws_token("nope") is False


def test_jwt_only_server_rejects_the_static_token(monkeypatch):
    """Once COMPUTE_SERVER_TOKEN is dropped, only signed tokens get in."""
    monkeypatch.delenv("COMPUTE_SERVER_TOKEN")
    assert auth.authenticate(f"Bearer {STATIC}") is None
    assert auth.authenticate(f"Bearer {_token()}") == "autobo-worker"


def test_static_only_server_still_serves(monkeypatch):
    """An instance that has never been restarted with a JWT secret keeps working."""
    monkeypatch.delenv("COMPUTE_JWT_SECRET")
    assert auth.authenticate(f"Bearer {STATIC}") == "service-token"
    assert auth.authenticate(f"Bearer {_token()}") is None
