"""
MCP token issuance and verification (Plan B WP5, §11.4).

v1 shim: opaque tokens minted and verified locally in DCL using HMAC-SHA256
over the canonical secret DCL_MCP_TOKEN_SECRET. No Platform round-trip.

Token format: base64url(payload) "." hex(hmac_sha256(payload, secret))
  payload = JSON {"tenant_id": <uuid>, "exp": <unix_ts>, "scope": [...], "token_id": <hex8>}

v2 (deferred): Platform owns issuance. New table mai_mcp_tokens with
revocation, scope, and audit. DCL calls
POST /api/mai/mcp-tokens/verify on every connection (with caching).
Filed in platform_deferred_work.md.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
import time
from dataclasses import dataclass


class TokenError(Exception):
    """Raised when a token cannot be verified."""


@dataclass(frozen=True)
class VerifiedToken:
    tenant_id: str
    expires_at: int
    scope: tuple[str, ...]
    token_id: str


_DEFAULT_TTL_SECONDS = 24 * 60 * 60  # 24h
_DEFAULT_SCOPE = (
    "query_triples",
    "list_domains",
    "concept_lookup",
    "semantic_export",
    "provenance",
)


def _secret() -> bytes:
    """Return the HMAC secret. Fails loudly if unset."""
    secret = os.environ.get("DCL_MCP_TOKEN_SECRET")
    if not secret:
        # Backward-compat: fall back to MCP_API_KEY only when it's a non-default
        # string. Per A1, do not silently degrade with a default secret.
        legacy = os.environ.get("MCP_API_KEY")
        if not legacy or legacy == "dcl-mcp-key-v1":
            raise TokenError(
                "MCP token shim cannot verify or mint tokens — "
                "DCL_MCP_TOKEN_SECRET is not set in env. "
                "Set DCL_MCP_TOKEN_SECRET to a strong random string "
                "(min 32 bytes recommended)."
            )
        secret = legacy
    return secret.encode("utf-8")


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode(s: str) -> bytes:
    padding = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode(s + padding)


def _sign(payload_bytes: bytes) -> str:
    sig = hmac.new(_secret(), payload_bytes, hashlib.sha256).hexdigest()
    return sig


def _compute_token_id(token_str: str) -> str:
    """Stable short ID for audit logging — never exposes the secret."""
    return hashlib.sha256(token_str.encode("utf-8")).hexdigest()[:16]


def mint_token(
    tenant_id: str,
    ttl_seconds: int = _DEFAULT_TTL_SECONDS,
    scope: tuple[str, ...] | list[str] | None = None,
) -> dict:
    """Mint a new MCP token bound to tenant_id.

    Returns {token, expires_at, token_id, tenant_id}.
    """
    if not tenant_id:
        raise TokenError("mint_token requires tenant_id (no anonymous tokens).")
    exp = int(time.time()) + int(ttl_seconds)
    scope_t = tuple(scope) if scope is not None else _DEFAULT_SCOPE
    nonce = secrets.token_hex(4)
    payload = {
        "tenant_id": tenant_id,
        "exp": exp,
        "scope": list(scope_t),
        "nonce": nonce,
    }
    payload_bytes = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    sig = _sign(payload_bytes)
    token = f"{_b64url_encode(payload_bytes)}.{sig}"
    return {
        "token": token,
        "expires_at": exp,
        "token_id": _compute_token_id(token),
        "tenant_id": tenant_id,
        "scope": list(scope_t),
    }


def verify_token(token: str) -> VerifiedToken:
    """Verify a token. Raises TokenError on any failure.

    Never returns a stub / fallback result. No silent degradation.
    """
    if not token or not isinstance(token, str):
        raise TokenError("MCP token is empty or not a string.")
    parts = token.split(".")
    if len(parts) != 2:
        raise TokenError("MCP token is malformed (expected '<payload>.<sig>').")
    payload_b64, sig_given = parts
    try:
        payload_bytes = _b64url_decode(payload_b64)
    except Exception as exc:  # base64 decode failure
        raise TokenError(f"MCP token payload is not valid base64url: {exc}") from exc
    sig_expected = _sign(payload_bytes)
    if not hmac.compare_digest(sig_given, sig_expected):
        raise TokenError("MCP token signature is invalid.")
    try:
        payload = json.loads(payload_bytes.decode("utf-8"))
    except Exception as exc:
        raise TokenError(f"MCP token payload is not valid JSON: {exc}") from exc
    tenant_id = payload.get("tenant_id")
    exp = payload.get("exp")
    scope = payload.get("scope") or []
    if not tenant_id:
        raise TokenError("MCP token payload missing tenant_id.")
    if not isinstance(exp, int):
        raise TokenError("MCP token payload missing or non-integer exp.")
    if exp < int(time.time()):
        raise TokenError("MCP token is expired.")
    return VerifiedToken(
        tenant_id=str(tenant_id),
        expires_at=int(exp),
        scope=tuple(scope),
        token_id=_compute_token_id(token),
    )
