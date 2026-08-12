"""Bearer-token authentication for the compute server.

Same pattern as spark-agent's AGENT_API_KEY: a single pre-shared token in an
env var, compared with secrets.compare_digest. No per-user accounts — the
token is held by the trusted callers (MatFlow, autoBO), not end users.

S-4 of docs/infra-cleanup-2026-08.md replaces this with dashboard-issued JWTs
verified against a per-tool COMPUTE_JWT_SECRET. The static token stays working
through that migration, so this stays the only verifier until then.
"""

from __future__ import annotations

import os
import secrets

from fastapi import Header, HTTPException


def require_token(authorization: str = Header(default="")) -> None:
    """Validate the Authorization header against COMPUTE_SERVER_TOKEN.

    Args:
        authorization: Raw "Authorization" header value, expected to be
            "Bearer <token>".

    Raises:
        HTTPException: 500 if the server has no token configured, 401 if the
            header is missing or the token does not match.
    """
    expected = os.environ.get("COMPUTE_SERVER_TOKEN", "")
    if not expected:
        raise HTTPException(status_code=500, detail="COMPUTE_SERVER_TOKEN is not configured on the server")

    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token or not secrets.compare_digest(token, expected):
        raise HTTPException(status_code=401, detail="invalid or missing bearer token")


def check_ws_token(token: str) -> bool:
    """Validate a bearer token supplied on a WebSocket connection.

    WebSocket connections can't use the Header-based FastAPI dependency, so
    the token is checked explicitly by the caller before accepting.

    Args:
        token: Raw token string (without the "Bearer " prefix).

    Returns:
        True if the token matches COMPUTE_SERVER_TOKEN, False otherwise.
    """
    expected = os.environ.get("COMPUTE_SERVER_TOKEN", "")
    return bool(expected) and bool(token) and secrets.compare_digest(token, expected)
