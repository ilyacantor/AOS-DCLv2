"""Gate 3C D2: LIVE MCP scope revocation / narrowing at the DCL-MCP boundary.

Operator-visible outcome:
  An agent holds a token minted with identity='live-agent' scoped to
  domains=['cloud_spend','revenue']. WITHOUT re-minting:
    (a) an operator narrows the registry to domain_scope=['cloud_spend'] and the
        NEXT broad read is dispatched with effective_domain_scope=('cloud_spend',)
        — a broad list_domains then returns ONLY cloud_spend, not revenue.
    (b) the operator REVOKES the identity (revoked_at set) and the next call is
        denied loudly: "identity 'live-agent' has been REVOKED ... all MCP access
        denied", dispatch is never reached, an unauthorized audit row is written.
    (c) a LEGACY token (identity=None) never consults the registry and dispatches
        with its token domain scope unchanged — byte-for-byte back-compat.
    (d) the registry trying to WIDEN beyond the token (registry adds 'revenue'
        back to a token scoped to 'cloud_spend') does NOT grant it — the
        intersection holds at ('cloud_spend',).
    (e) the registry read is TTL-cached: two calls inside the window hit the DB
        once; after the TTL elapses the next call re-reads.

These tests MOCK the registry helper and dispatch (no live DB, no shared-DB
mutation). The intersection math and the cache are exercised directly; the
boundary wiring is exercised through the real _call_tool handler.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest

_repo = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_repo))

# Pure-crypto mint/verify need a secret; no DB, no prod. A throwaway test secret
# is sufficient (these tests never touch a real token store).
os.environ.setdefault("DCL_MCP_TOKEN_SECRET", "live-revocation-test-secret-do-not-use")

# This suite mocks ALL registry/dispatch/audit access — it neither reads nor
# writes mcp_agent_identities or mai_mcp_audit, so it does NOT depend on
# migration 030 and never mutates a shared table. We load .env.development only
# so the conftest field_concept_mappings autouse fixtures (aos-dev, read-only
# here) can resolve DATABASE_URL. No prod (.env) is ever loaded.
from dotenv import load_dotenv  # noqa: E402
load_dotenv(_repo / ".env.development")

from mcp import types as _t  # noqa: E402
import backend.api.mcp_server_real as msr  # noqa: E402
from backend.api.mcp_auth import mint_token  # noqa: E402
import backend.api.mcp_identity_registry as reg_mod  # noqa: E402
from backend.api.mcp_identity_registry import (  # noqa: E402
    EffectiveIdentity,
    UnknownIdentityError,
    intersect_scope,
)

pytestmark = pytest.mark.anyio


# ---------------------------------------------------------------------------
# anyio / Playwright loop isolation (mirrors test_mcp_rbac.py)
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _isolate_anyio_from_playwright():
    import anyio.pytest_plugin as _ap

    saved_loop = None
    try:
        saved_loop = asyncio.events._get_running_loop()
        asyncio.events._set_running_loop(None)
    except Exception:
        pass
    _ap._runner_leases = 0
    _ap._runner_stack = None
    _ap._current_runner = None
    try:
        yield
    finally:
        if saved_loop is not None:
            try:
                asyncio.events._set_running_loop(saved_loop)
            except Exception:
                pass


@pytest.fixture
def anyio_backend():
    return "asyncio"


# ---------------------------------------------------------------------------
# Boundary harness: patch registry + dispatch + write_audit so NO DB is hit.
# ---------------------------------------------------------------------------


class _Recorder:
    """Captures the kwargs dispatch() is called with, and the audit rows."""

    def __init__(self) -> None:
        self.dispatch_calls: list[dict] = []
        self.audit_rows: list = []

    def dispatch(self, tenant_id, name, arguments, *, effective_domain_scope=()):
        self.dispatch_calls.append({
            "tenant_id": tenant_id,
            "name": name,
            "arguments": arguments,
            "effective_domain_scope": effective_domain_scope,
        })
        return []  # a tool result shape (empty list) — no DB needed

    def write_audit(self, row) -> None:
        self.audit_rows.append(row)


def _wire(monkeypatch, *, effective=None, raises=None):
    """Patch the boundary's collaborators. `effective` is the EffectiveIdentity
    the (mocked) registry returns; `raises` is an exception the registry raises.
    Returns the _Recorder."""
    rec = _Recorder()
    monkeypatch.setattr(msr, "dispatch", rec.dispatch)
    monkeypatch.setattr(msr, "write_audit", rec.write_audit)

    def _fake_get_effective_identity(tenant_id, identity_name):
        if raises is not None:
            raise raises
        return effective

    monkeypatch.setattr(msr, "get_effective_identity", _fake_get_effective_identity)
    return rec


async def _call(server, name: str, arguments: dict):
    handler = server.request_handlers[_t.CallToolRequest]
    req = _t.CallToolRequest(
        method="tools/call",
        params=_t.CallToolRequestParams(name=name, arguments=arguments),
    )
    return (await handler(req)).root


def _bind(token_str: str):
    from backend.api.mcp_auth import verify_token
    verified = verify_token(token_str)
    rt = msr.bind_token_to_session(verified)
    rtr = msr.bind_transport("test-live-revocation")
    return msr.build_server(), rt, rtr


# ---------------------------------------------------------------------------
# (a) NARROW: registry domain_scope narrowing reaches dispatch as the
#     intersected scope; a broad list_domains then returns only those domains.
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_a_narrow_registry_domain_reaches_dispatch(monkeypatch):
    tenant = str(uuid.uuid4())
    # Token minted broad across two domains; registry has since been narrowed.
    tok = mint_token(
        tenant, scope=["query_triples"], identity="live-agent",
        domain_scope=["cloud_spend", "revenue"],
    )["token"]
    rec = _wire(monkeypatch, effective=EffectiveIdentity(
        tool_scope=("query_triples",),
        domain_scope=("cloud_spend",),   # operator narrowed to cloud_spend only
        persona_scope=(),
        revoked_at=None,
    ))
    server, rt, rtr = _bind(tok)
    try:
        r = await _call(server, "query_triples", {})  # broad read
    finally:
        msr.release_token(rt); msr.release_transport(rtr)

    assert r.isError is False, f"broad in-scope read should succeed: {r}"
    assert len(rec.dispatch_calls) == 1, "dispatch should be reached exactly once"
    eff = rec.dispatch_calls[0]["effective_domain_scope"]
    assert tuple(eff) == ("cloud_spend",), (
        f"live narrowing must reach dispatch as the intersection "
        f"(token∩registry); got effective_domain_scope={eff!r}"
    )
    # Success audit row carries the identity name (governance "who").
    success = [a for a in rec.audit_rows if a.outcome == "success"]
    assert success and success[0].identity == "live-agent", (
        "success audit row must carry identity='live-agent'"
    )


def test_a2_list_domains_filters_to_effective_scope(monkeypatch):
    """The broad-read filter itself: list_domains with the narrowed effective
    scope returns ONLY the in-scope domain (proves narrowing hides revenue)."""
    from backend.engine import mcp_tools

    fake_rows = [
        {"domain": "cloud_spend", "triple_count": 10},
        {"domain": "revenue", "triple_count": 5},
        {"domain": "support", "triple_count": 3},
    ]
    monkeypatch.setattr(
        mcp_tools._store, "mcp_list_domains",
        lambda tenant_id, entity_id: list(fake_rows),
    )
    out = mcp_tools.tool_list_domains(
        "t", None, effective_domain_scope=("cloud_spend",)
    )
    assert [r["domain"] for r in out] == ["cloud_spend"], (
        f"narrowed list_domains must return only cloud_spend; got {out!r}"
    )


# ---------------------------------------------------------------------------
# (b) REVOKE: revoked_at set → next call denied loudly, dispatch never reached.
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_b_revoked_identity_denied_loud(monkeypatch):
    tenant = str(uuid.uuid4())
    tok = mint_token(
        tenant, scope=["query_triples"], identity="live-agent",
        domain_scope=["cloud_spend"],
    )["token"]
    rec = _wire(monkeypatch, effective=EffectiveIdentity(
        tool_scope=("query_triples",),
        domain_scope=("cloud_spend",),
        persona_scope=(),
        revoked_at=datetime(2026, 6, 29, tzinfo=timezone.utc),  # REVOKED
    ))
    server, rt, rtr = _bind(tok)
    try:
        r = await _call(server, "query_triples", {"domain": "cloud_spend"})
    finally:
        msr.release_token(rt); msr.release_transport(rtr)

    assert r.isError is True, "revoked identity must be denied, not served"
    text = r.content[0].text if r.content else ""
    assert "REVOKED" in text and "live-agent" in text, (
        f"denial must be readable and name the identity; got: {text!r}"
    )
    assert rec.dispatch_calls == [], "revoked identity must NEVER reach dispatch"
    denials = [a for a in rec.audit_rows if a.outcome == "unauthorized"]
    assert denials, "a revoked call must write an unauthorized audit row"
    d = denials[0]
    assert d.identity == "live-agent", "audit row must carry the identity name"
    rbac = (d.arguments or {}).get("_rbac_denied")
    assert rbac and rbac["axis"] == "revoked" and rbac["denied"] == "live-agent", (
        f"audit must carry _rbac_denied axis='revoked'; got {rbac!r}"
    )


@pytest.mark.anyio
async def test_b2_unknown_identity_denied_fail_closed(monkeypatch):
    """A token claiming an identity the registry does not have is denied
    (fail-closed), never served as unrestricted."""
    tenant = str(uuid.uuid4())
    tok = mint_token(tenant, scope=["query_triples"], identity="ghost-agent")["token"]
    rec = _wire(monkeypatch, raises=UnknownIdentityError(
        "identity 'ghost-agent' has no row ... (fail-closed)."
    ))
    server, rt, rtr = _bind(tok)
    try:
        r = await _call(server, "query_triples", {"domain": "cloud_spend"})
    finally:
        msr.release_token(rt); msr.release_transport(rtr)

    assert r.isError is True, "unknown identity must be denied (fail-closed)"
    text = r.content[0].text if r.content else ""
    assert "ghost-agent" in text and "fail-closed" in text, (
        f"denial must name the identity and be readable; got: {text!r}"
    )
    assert rec.dispatch_calls == [], "unknown identity must never reach dispatch"


# ---------------------------------------------------------------------------
# (c) LEGACY token (identity=None) — registry never consulted, scope unchanged.
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_c_legacy_token_skips_registry(monkeypatch):
    tenant = str(uuid.uuid4())
    tok = mint_token(tenant)["token"]  # no identity, no domain scope

    calls = {"n": 0}

    def _must_not_call(*a, **k):
        calls["n"] += 1
        raise AssertionError("legacy token must NOT consult the registry")

    rec = _Recorder()
    monkeypatch.setattr(msr, "dispatch", rec.dispatch)
    monkeypatch.setattr(msr, "write_audit", rec.write_audit)
    monkeypatch.setattr(msr, "get_effective_identity", _must_not_call)

    server, rt, rtr = _bind(tok)
    try:
        r = await _call(server, "list_domains", {})
    finally:
        msr.release_token(rt); msr.release_transport(rtr)

    assert r.isError is False, f"legacy token should serve unchanged: {r}"
    assert calls["n"] == 0, "registry must not be consulted for a legacy token"
    assert len(rec.dispatch_calls) == 1
    assert tuple(rec.dispatch_calls[0]["effective_domain_scope"]) == (), (
        "legacy token must dispatch with its (empty/unrestricted) token scope, "
        f"unchanged; got {rec.dispatch_calls[0]['effective_domain_scope']!r}"
    )
    success = [a for a in rec.audit_rows if a.outcome == "success"]
    assert success and success[0].identity is None, (
        "legacy success audit row must have identity=None (historical NULL)"
    )


# ---------------------------------------------------------------------------
# (d) Registry cannot WIDEN beyond the token — the intersection holds.
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_d_registry_widen_does_not_grant(monkeypatch):
    tenant = str(uuid.uuid4())
    # Token is scoped to cloud_spend ONLY.
    tok = mint_token(
        tenant, scope=["query_triples"], identity="live-agent",
        domain_scope=["cloud_spend"],
    )["token"]
    # Registry (mis)configured BROADER — adds revenue. Must NOT widen the token.
    rec = _wire(monkeypatch, effective=EffectiveIdentity(
        tool_scope=("query_triples",),
        domain_scope=("cloud_spend", "revenue"),
        persona_scope=(),
        revoked_at=None,
    ))
    server, rt, rtr = _bind(tok)
    try:
        r = await _call(server, "query_triples", {})
    finally:
        msr.release_token(rt); msr.release_transport(rtr)

    assert r.isError is False
    eff = tuple(rec.dispatch_calls[0]["effective_domain_scope"])
    assert eff == ("cloud_spend",), (
        f"registry must not widen beyond the token; effective must stay "
        f"('cloud_spend',), got {eff!r}"
    )


def test_d2_intersect_scope_semantics():
    """The intersection math, including the empty-set edge cases."""
    # both empty -> unrestricted (no deny)
    assert intersect_scope((), ()) == ((), False)
    # token empty (unrestricted), registry restricts -> registry narrows
    assert intersect_scope((), ("cloud_spend",)) == (("cloud_spend",), False)
    # token restricts, registry empty (unrestricted) -> token stands (NOT widened)
    assert intersect_scope(("cloud_spend",), ()) == (("cloud_spend",), False)
    # both restrict, overlap -> the overlap, order follows the token axis
    assert intersect_scope(("cloud_spend", "revenue"), ("revenue",)) == (
        ("revenue",), False,
    )
    # registry tries to widen -> intersection holds (revenue not granted)
    assert intersect_scope(("cloud_spend",), ("cloud_spend", "revenue")) == (
        ("cloud_spend",), False,
    )
    # both restrict, DISJOINT -> deny-all (NOT unrestricted ())
    assert intersect_scope(("cloud_spend",), ("revenue",)) == ((), True)


@pytest.mark.anyio
async def test_d3_disjoint_intersection_denies_broad_read(monkeypatch):
    """A genuine empty intersection (token∩registry disjoint) denies a broad
    read loudly rather than collapsing to () = unrestricted (the leak guard)."""
    tenant = str(uuid.uuid4())
    tok = mint_token(
        tenant, scope=["query_triples"], identity="live-agent",
        domain_scope=["cloud_spend"],
    )["token"]
    rec = _wire(monkeypatch, effective=EffectiveIdentity(
        tool_scope=("query_triples",),
        domain_scope=("revenue",),  # disjoint from the token's cloud_spend
        persona_scope=(),
        revoked_at=None,
    ))
    server, rt, rtr = _bind(tok)
    try:
        r = await _call(server, "query_triples", {})  # broad read
    finally:
        msr.release_token(rt); msr.release_transport(rtr)

    assert r.isError is True, (
        "disjoint token∩registry must DENY a broad read, never serve it "
        "unrestricted"
    )
    assert rec.dispatch_calls == [], "disjoint intersection must not reach dispatch"
    text = r.content[0].text if r.content else ""
    assert "empty effective domain scope" in text, f"got: {text!r}"


# ---------------------------------------------------------------------------
# (e) TTL cache: one DB read inside the window; re-read after the TTL elapses.
# ---------------------------------------------------------------------------


def test_e_ttl_cache_hits_once_then_refreshes(monkeypatch):
    reg_mod.clear_cache()
    hits = {"n": 0}
    sample = EffectiveIdentity(
        tool_scope=(), domain_scope=("cloud_spend",), persona_scope=(),
        revoked_at=None,
    )

    def _counting_load(tenant_id, identity_name):
        hits["n"] += 1
        return sample

    monkeypatch.setattr(reg_mod, "_load_from_db", _counting_load)

    # Controllable clock so the test never sleeps.
    clock = {"t": 1000.0}
    monkeypatch.setattr(reg_mod.time, "monotonic", lambda: clock["t"])

    a = reg_mod.get_effective_identity("tenant-x", "live-agent")
    b = reg_mod.get_effective_identity("tenant-x", "live-agent")  # within TTL
    assert a == b == sample
    assert hits["n"] == 1, (
        f"two reads inside the TTL window must hit the DB once; got {hits['n']}"
    )

    # Advance the clock just past the TTL → next read refreshes from the DB.
    clock["t"] += reg_mod._CACHE_TTL_SECONDS + 0.1
    reg_mod.get_effective_identity("tenant-x", "live-agent")
    assert hits["n"] == 2, (
        f"after the TTL elapses the next read must re-query the DB; got {hits['n']}"
    )

    reg_mod.clear_cache()


def test_e2_registry_error_not_cached(monkeypatch):
    """A DB error fails loud (fail-closed) and is NOT cached — a transient blip
    must not pin a deny, and a freshly-provisioned identity is visible next call."""
    reg_mod.clear_cache()
    from backend.api.mcp_identity_registry import IdentityRegistryError

    state = {"fail": True, "loads": 0}
    good = EffectiveIdentity(
        tool_scope=(), domain_scope=(), persona_scope=(), revoked_at=None,
    )

    def _flaky_load(tenant_id, identity_name):
        state["loads"] += 1
        if state["fail"]:
            raise IdentityRegistryError("transient DB blip")
        return good

    monkeypatch.setattr(reg_mod, "_load_from_db", _flaky_load)

    with pytest.raises(IdentityRegistryError):
        reg_mod.get_effective_identity("tenant-y", "live-agent")

    # The error was not cached: the recovered DB serves the next call.
    state["fail"] = False
    out = reg_mod.get_effective_identity("tenant-y", "live-agent")
    assert out == good and state["loads"] == 2, (
        "registry errors must not be cached (must re-read on the next call)"
    )
    reg_mod.clear_cache()
