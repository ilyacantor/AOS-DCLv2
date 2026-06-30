"""
Live agent-identity registry reads for the MCP boundary (Gate 3C D2).

migration 026 declared `mcp_agent_identities` and `mcp_mint.py` reads it ONCE,
at MINT time, embedding the resolved 3-axis scope into the HMAC token. That
token is then self-contained: `mcp_auth.verify_token` checks signature + exp
only and never consults the registry, so a minted token keeps its scope until
expiry — the only live revocation was global HMAC-secret rotation.

This module lets `mcp_server_real._call_tool` consult the registry on EVERY
call so an operator can NARROW or REVOKE one identity (migration 030's
`revoked_at` / an updated `domain_scope`) and have the NEXT call enforce it —
no secret rotation, no waiting for expiry.

Cost is bounded by a short in-process TTL cache (~5s) keyed by
(tenant_id, identity_name): a narrow/revoke lands within one TTL window, and
steady-state calls pay a dict lookup, not a DB round trip (B18 — no latency
regression).

Fail-closed (A1): a DB error RAISES `IdentityRegistryError` (never a permissive
default); a token that claims an identity with NO registry row raises
`UnknownIdentityError`. The caller turns both into a loud deny — never
"unrestricted". Errors are NOT cached (a transient DB blip must not pin a deny,
and a freshly-provisioned identity must be visible on its next call).
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass

from backend.core.db import get_connection
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)


class IdentityRegistryError(Exception):
    """The registry could not be read — authorization cannot be evaluated.
    The caller MUST fail closed (deny); it must never proceed as unrestricted."""


class UnknownIdentityError(Exception):
    """The token claims an identity with no row in mcp_agent_identities
    (revoked-by-deletion or never provisioned). Fail-closed: a loud deny,
    never unrestricted."""


@dataclass(frozen=True)
class EffectiveIdentity:
    """The live registry state for one (tenant, identity). Empty scope tuples
    keep the registry's "empty = unrestricted on that axis" convention
    (mirrors the token). revoked_at is truthy when the identity is revoked."""
    tool_scope: tuple[str, ...]
    domain_scope: tuple[str, ...]
    persona_scope: tuple[str, ...]
    revoked_at: object | None  # TIMESTAMPTZ; non-None = revoked


# Short TTL: a narrow/revoke is visible within this window; steady-state calls
# pay a dict lookup, not a DB round trip (B18). Module-level so the process
# shares one cache across MCP sessions.
_CACHE_TTL_SECONDS = 5.0
_cache: dict[tuple[str, str], tuple[float, EffectiveIdentity]] = {}
_cache_lock = threading.Lock()


def _load_from_db(tenant_id: str, identity_name: str) -> EffectiveIdentity:
    """Read one identity row. Raises IdentityRegistryError on any DB failure
    (fail-closed) and UnknownIdentityError when no row exists."""
    sql = (
        "SELECT tool_scope, domain_scope, persona_scope, revoked_at "
        "FROM mcp_agent_identities "
        "WHERE tenant_id = %s AND identity_name = %s"
    )
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (tenant_id, identity_name))
                row = cur.fetchone()
            # Read-only: release the snapshot promptly, don't leave a txn open.
            conn.rollback()
    except Exception as exc:
        raise IdentityRegistryError(
            f"mcp_agent_identities read failed for identity {identity_name!r} "
            f"(tenant {tenant_id}) — MCP authorization cannot be evaluated, "
            f"denying fail-closed. Underlying error: {exc}"
        ) from exc
    if row is None:
        raise UnknownIdentityError(
            f"identity {identity_name!r} has no row in the agent-identity "
            f"registry for tenant {tenant_id} (revoked-by-deletion or never "
            f"provisioned) — denied (fail-closed)."
        )
    return EffectiveIdentity(
        tool_scope=tuple(row[0] or ()),
        domain_scope=tuple(row[1] or ()),
        persona_scope=tuple(row[2] or ()),
        revoked_at=row[3],
    )


def get_effective_identity(tenant_id: str, identity_name: str) -> EffectiveIdentity:
    """Return the live effective scope + revocation state for (tenant, identity),
    served from a ~5s in-process TTL cache.

    Raises UnknownIdentityError (caller → loud deny) when the identity has no
    registry row, IdentityRegistryError (caller → loud deny) when the registry
    cannot be read. Only successful reads are cached — errors are never cached.
    """
    if not tenant_id or not identity_name:
        # Defensive: callers only reach here with token.identity set, but a
        # blank either way is a loud deny, never a pass.
        raise UnknownIdentityError(
            "get_effective_identity requires both tenant_id and identity_name "
            f"(got tenant_id={tenant_id!r}, identity_name={identity_name!r})."
        )
    key = (str(tenant_id), str(identity_name))
    now = time.monotonic()

    with _cache_lock:
        hit = _cache.get(key)
        if hit is not None and hit[0] > now:
            return hit[1]

    # Cache miss / expired — read the DB OUTSIDE the lock (the call may block on
    # the pool; holding the lock would serialize all callers).
    value = _load_from_db(tenant_id, identity_name)

    with _cache_lock:
        _cache[key] = (now + _CACHE_TTL_SECONDS, value)
    return value


def clear_cache() -> None:
    """Drop all cached identities so the next call re-reads the DB. Used by the
    operator revoke surface for an immediate effect and by tests for isolation;
    not on the per-call hot path."""
    with _cache_lock:
        _cache.clear()


def intersect_scope(
    token_axis: tuple[str, ...] | list[str] | None,
    registry_axis: tuple[str, ...] | list[str] | None,
) -> tuple[tuple[str, ...], bool]:
    """Intersect one scope axis under the "empty = unrestricted" convention.

    Returns (effective_tuple, deny_all):
      - both empty            -> ((), False)        unrestricted on this axis
      - token empty, reg set  -> (reg, False)       reg narrows from unrestricted
      - token set, reg empty  -> (token, False)     token's restriction stands
      - both set, overlap      -> (overlap, False)  the intersection
      - both set, DISJOINT     -> ((), True)        DENY-ALL — *not* unrestricted

    The registry can only NARROW: when the token axis is non-empty the result is
    always a subset of it, so the registry can never widen access beyond the
    token. The deny_all flag is the load-bearing edge case: a genuine empty
    intersection (both sides restrict, no overlap) must NOT collapse to () —
    that would read downstream as "unrestricted" and leak everything. The caller
    denies loudly instead.
    """
    t = tuple(token_axis or ())
    r = tuple(registry_axis or ())
    if not t and not r:
        return (), False
    if not t:
        return r, False
    if not r:
        return t, False
    reg_set = set(r)
    overlap = tuple(x for x in t if x in reg_set)
    if not overlap:
        return (), True
    return overlap, False
