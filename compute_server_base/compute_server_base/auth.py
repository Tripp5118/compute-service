"""Bearer authentication for a compute server: dashboard-signed JWTs, or the static token.

Two verifiers, in order (docs/infra-cleanup-2026-08.md S-4):

1. **A dashboard-issued JWT**, if `COMPUTE_JWT_SECRET` is set. Verification is an
   HMAC recomputation plus an audience check — no database, no network, nothing
   consulted. A token proves it was signed by something holding this tool's
   secret (the dashboard) *for this tool*, and carries the caller's identity in
   `sub`. Because nothing is looked up, the dashboard can be down and job
   submission still works, which is the whole reason this is a signature rather
   than an API key checked against a synced list.
2. **The static `COMPUTE_SERVER_TOKEN`**, unchanged. It stays valid through the
   migration because MatFlow is paused and cannot be repointed on our schedule.
   Remove it once every caller has a service subject.

Revocation, stated plainly: service tokens are long-lived and checked by
signature alone, so the way to retire one is to rotate this tool's
`COMPUTE_JWT_SECRET` from the dashboard, which invalidates every token for this
tool at once. Granularity is per-tool, and that is the accepted cost of not
having a list to consult. Human tokens expire in an hour instead.

The audience is `tool:<TOOL_NAME>`, so a token minted for materials-framework is
rejected by thermocalc even though both trust the same dashboard.
"""

from __future__ import annotations

import logging
import os
import secrets

import jwt
from fastapi import Header, HTTPException

log = logging.getLogger(__name__)

# Set by create_app() from the instance's tool_name, so the audience check needs
# no configuration of its own and cannot drift from the server's identity.
_audience: str | None = None


def set_audience(tool_name: str) -> None:
    """Record which audience this server accepts tokens for."""
    global _audience  # noqa: PLW0603 — module-level identity, set once at app build
    _audience = f"tool:{tool_name}"


def _static_token_matches(token: str) -> bool:
    expected = os.environ.get("COMPUTE_SERVER_TOKEN", "")
    return bool(expected) and bool(token) and secrets.compare_digest(token, expected)


def _jwt_subject(token: str) -> str | None:
    """Caller identity from a dashboard-signed token, or None if it isn't one.

    Returns None both when no JWT secret is configured and when the token fails
    verification, so the caller falls through to the static token. A malformed or
    expired JWT is not an error here — it might be a static token that happens
    not to be a JWT at all.
    """
    secret = os.environ.get("COMPUTE_JWT_SECRET", "")
    if not secret or not token:
        return None
    try:
        payload = jwt.decode(
            token,
            secret,
            algorithms=["HS256"],
            audience=_audience,
            # A token with no expiry would be unrevocable by any means, including
            # rotation-by-reissue, so require one.
            options={"require": ["exp", "sub", "aud"]},
        )
    except jwt.PyJWTError as exc:
        log.debug("token is not a valid JWT for %s: %s", _audience, exc)
        return None
    subject = payload.get("sub")
    return str(subject) if subject else None


def authenticate(authorization: str) -> str | None:
    """Identify the caller behind an Authorization header, or None if unauthenticated.

    Args:
        authorization: Raw header value, expected to be "Bearer <token>".

    Returns:
        The caller's identity — a JWT `sub` for a dashboard-issued token, or
        `"service-token"` for the static shared credential, which genuinely
        cannot be attributed to anyone more specific.
    """
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token:
        return None
    subject = _jwt_subject(token)
    if subject is not None:
        return subject
    return "service-token" if _static_token_matches(token) else None


def require_token(authorization: str = Header(default="")) -> str:
    """Validate the Authorization header and return the caller's identity.

    Raises:
        HTTPException: 500 if the server has no credential configured at all,
            401 if the header is missing or neither verifier accepts it.
    """
    if not os.environ.get("COMPUTE_SERVER_TOKEN") and not os.environ.get("COMPUTE_JWT_SECRET"):
        raise HTTPException(
            status_code=500,
            detail="neither COMPUTE_SERVER_TOKEN nor COMPUTE_JWT_SECRET is configured on the server",
        )

    caller = authenticate(authorization)
    if caller is None:
        raise HTTPException(status_code=401, detail="invalid or missing bearer token")
    return caller


def check_ws_token(token: str) -> bool:
    """Validate a bare token from a WebSocket connection or a mounted sub-app.

    WebSocket handshakes can't use the Header dependency, and the MCP mount is
    plain ASGI in front of a session manager, so both check the token explicitly.

    Args:
        token: Raw token string, without the "Bearer " prefix.
    """
    return authenticate(f"Bearer {token}") is not None
