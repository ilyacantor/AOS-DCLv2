"""Gate 3C D1: MCP scoped-identity RBAC tests.

Operator-visible outcome: a token carrying identity='finops-readonly' scoped
to tools=['query_triples'], domains=['cloud_spend'], personas=['CFO'] —
  - query_triples(domain='cloud_spend') SUCCEEDS (in-scope tool + domain)
  - query_triples(domain='revenue') is DENIED with a readable error naming
    'finops-readonly' and writes a decision trace to mai_mcp_audit
  - list_domains (not in tool scope) is DENIED with a readable error
  - query_triples(persona='CRO') is DENIED (out-of-scope persona)
  - query_triples(concept='revenue.total') is DENIED (root out of scope)
A legacy token (no identity / no scope) SUCCEEDS on all the same calls —
  byte-identical response structure, zero denials.
Every denial writes an audit row (outcome='unauthorized') carrying the
identity label and the denied resource, visible via decision_traces.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
import uuid
from pathlib import Path

import pytest

_repo = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_repo))

from dotenv import load_dotenv
load_dotenv(_repo / ".env.development")

# Same single-var secret resolution as wp5/gate2a/gate2b suites.
if not os.environ.get("DCL_MCP_TOKEN_SECRET"):
    from dotenv import dotenv_values
    _live_secret = dotenv_values(_repo / ".env").get("DCL_MCP_TOKEN_SECRET")
    if _live_secret:
        os.environ["DCL_MCP_TOKEN_SECRET"] = _live_secret
os.environ.setdefault("DCL_MCP_TOKEN_SECRET", "rbac-test-secret-do-not-use-in-prod")

from mcp import types as _t  # noqa: E402
from backend.api.mcp_auth import mint_token, verify_token  # noqa: E402
from backend.api.mcp_server_real import (  # noqa: E402
    bind_token_to_session,
    bind_transport,
    build_server,
    release_token,
    release_transport,
)
from backend.core.db import get_connection  # noqa: E402

pytestmark = pytest.mark.anyio

# Per-run isolated tenant — fresh UUID so the dev DB accumulates no cross-run
# contamination and audit rows are uniquely attributable.
_TENANT = str(uuid.uuid4())

# Scoped-identity fixture values.
SCOPED_IDENTITY = "finops-readonly"
ALLOWED_TOOL = "query_triples"
ALLOWED_DOMAIN = "cloud_spend"
ALLOWED_PERSONA = "CFO"
DENIED_TOOL = "list_domains"          # not in TOOL_SCOPE
DENIED_DOMAIN = "revenue"             # not in domain_scope
DENIED_PERSONA = "CRO"               # not in persona_scope
DENIED_CONCEPT = "revenue.total"      # domain root 'revenue' not in scope


# ---------------------------------------------------------------------------
# Anyio / Playwright loop isolation (mirrors test_mcp_wp5.py fixture)
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
# Helpers
# ---------------------------------------------------------------------------


def _audit_rows_since(tenant_id: str, since: float) -> list[dict]:
    sql = (
        "SELECT tenant_id, tool_name, caller_token_id, outcome, "
        "       error_summary, transport, arguments "
        "FROM mai_mcp_audit "
        "WHERE tenant_id = %s AND created_at >= to_timestamp(%s) "
        "ORDER BY created_at DESC"
    )
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (tenant_id, since))
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in rows]


def _build_server_with_token(token_str: str):
    """Bind a verified token to the current context and return a fresh server."""
    from backend.api.mcp_auth import verify_token as _vt
    verified = _vt(token_str)
    rt = bind_token_to_session(verified)
    rtr = bind_transport("test-rbac")
    server = build_server()
    return server, rt, rtr


async def _call(server, name: str, arguments: dict) -> _t.CallToolResult:
    handler = server.request_handlers[_t.CallToolRequest]
    req = _t.CallToolRequest(
        method="tools/call",
        params=_t.CallToolRequestParams(name=name, arguments=arguments),
    )
    srv = await handler(req)
    return srv.root


# ---------------------------------------------------------------------------
# Token fixtures
# ---------------------------------------------------------------------------


def _scoped_token() -> str:
    return mint_token(
        _TENANT,
        scope=[ALLOWED_TOOL],
        identity=SCOPED_IDENTITY,
        domain_scope=[ALLOWED_DOMAIN],
        persona_scope=[ALLOWED_PERSONA],
    )["token"]


def _legacy_token() -> str:
    """Legacy token: no identity, no domain/persona scope — unrestricted."""
    return mint_token(_TENANT)["token"]


# ---------------------------------------------------------------------------
# Test 1: in-scope tool + domain call SUCCEEDS
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_in_scope_call_succeeds():
    tok = _scoped_token()
    server, rt, rtr = _build_server_with_token(tok)
    try:
        r = await _call(server, ALLOWED_TOOL, {"domain": ALLOWED_DOMAIN})
    finally:
        release_token(rt)
        release_transport(rtr)
    assert r.isError is False, (
        f"Expected success for in-scope tool={ALLOWED_TOOL} "
        f"domain={ALLOWED_DOMAIN}, got error: "
        f"{r.content[0].text if r.content else r}"
    )
    payload = json.loads(r.content[0].text)
    assert isinstance(payload, list), f"Expected list result, got {type(payload)}"


# ---------------------------------------------------------------------------
# Test 2: out-of-scope TOOL denied + trace written
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_out_of_scope_tool_denied_with_trace():
    tok = _scoped_token()
    server, rt, rtr = _build_server_with_token(tok)
    since = time.time() - 1
    try:
        r = await _call(server, DENIED_TOOL, {})
    finally:
        release_token(rt)
        release_transport(rtr)

    assert r.isError is True, (
        f"Expected denial for tool={DENIED_TOOL} (not in tool scope), "
        f"got success: {r}"
    )
    error_text = r.content[0].text if r.content else ""
    assert SCOPED_IDENTITY in error_text, (
        f"Denial message must name the identity {SCOPED_IDENTITY!r}; "
        f"got: {error_text!r}"
    )
    assert DENIED_TOOL in error_text, (
        f"Denial message must name the denied tool {DENIED_TOOL!r}; "
        f"got: {error_text!r}"
    )

    # Decision trace must be visible in mai_mcp_audit.
    rows = _audit_rows_since(_TENANT, since)
    denials = [r for r in rows if r["outcome"] == "unauthorized"]
    assert denials, "No unauthorized audit row found after tool-scope denial"
    denial = denials[0]
    assert denial["tool_name"] == DENIED_TOOL, (
        f"Audit row tool_name should be {DENIED_TOOL!r}, got {denial['tool_name']!r}"
    )
    assert SCOPED_IDENTITY in (denial["error_summary"] or ""), (
        f"Audit error_summary must name the identity; got {denial['error_summary']!r}"
    )


# ---------------------------------------------------------------------------
# Test 3: out-of-scope DOMAIN denied + trace written
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_out_of_scope_domain_denied_with_trace():
    tok = _scoped_token()
    server, rt, rtr = _build_server_with_token(tok)
    since = time.time() - 1
    try:
        r = await _call(server, ALLOWED_TOOL, {"domain": DENIED_DOMAIN})
    finally:
        release_token(rt)
        release_transport(rtr)

    assert r.isError is True, (
        f"Expected denial for domain={DENIED_DOMAIN} (not in domain_scope), "
        f"got success: {r}"
    )
    error_text = r.content[0].text if r.content else ""
    assert SCOPED_IDENTITY in error_text, (
        f"Denial message must name the identity; got: {error_text!r}"
    )
    assert DENIED_DOMAIN in error_text, (
        f"Denial message must name the denied domain; got: {error_text!r}"
    )
    # NOT an empty-list 200 — that would be the silent-fallback enemy.
    # The response is an error, not an empty list.
    assert r.isError is True, "Domain denial must be a hard error, not an empty list"

    rows = _audit_rows_since(_TENANT, since)
    denials = [row for row in rows if row["outcome"] == "unauthorized"]
    assert denials, "No unauthorized audit row found after domain-scope denial"
    denial = denials[0]
    assert DENIED_DOMAIN in (denial["error_summary"] or ""), (
        f"Audit error_summary must name the denied domain; got {denial['error_summary']!r}"
    )


# ---------------------------------------------------------------------------
# Test 4: domain-qualified CONCEPT with out-of-scope root denied + trace
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_out_of_scope_concept_root_denied_with_trace():
    tok = _scoped_token()
    server, rt, rtr = _build_server_with_token(tok)
    since = time.time() - 1
    try:
        r = await _call(server, ALLOWED_TOOL, {"concept": DENIED_CONCEPT})
    finally:
        release_token(rt)
        release_transport(rtr)

    assert r.isError is True, (
        f"Expected denial for concept={DENIED_CONCEPT} "
        f"(root 'revenue' not in domain_scope), got success: {r}"
    )
    error_text = r.content[0].text if r.content else ""
    assert SCOPED_IDENTITY in error_text, (
        f"Denial must name identity; got: {error_text!r}"
    )
    assert "revenue" in error_text, (
        f"Denial must name the denied domain root; got: {error_text!r}"
    )

    rows = _audit_rows_since(_TENANT, since)
    assert any(row["outcome"] == "unauthorized" for row in rows), (
        "No unauthorized audit row after concept-root denial"
    )


# ---------------------------------------------------------------------------
# Test 5: out-of-scope PERSONA denied + trace written
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_out_of_scope_persona_denied_with_trace():
    tok = _scoped_token()
    server, rt, rtr = _build_server_with_token(tok)
    since = time.time() - 1
    try:
        r = await _call(server, ALLOWED_TOOL, {"persona": DENIED_PERSONA})
    finally:
        release_token(rt)
        release_transport(rtr)

    assert r.isError is True, (
        f"Expected denial for persona={DENIED_PERSONA} (not in persona_scope), "
        f"got success: {r}"
    )
    error_text = r.content[0].text if r.content else ""
    assert SCOPED_IDENTITY in error_text, (
        f"Denial must name identity; got: {error_text!r}"
    )
    assert DENIED_PERSONA in error_text, (
        f"Denial must name the denied persona; got: {error_text!r}"
    )

    rows = _audit_rows_since(_TENANT, since)
    denials = [row for row in rows if row["outcome"] == "unauthorized"]
    assert denials, "No unauthorized audit row after persona denial"
    denial = denials[0]
    assert DENIED_PERSONA in (denial["error_summary"] or ""), (
        f"Audit error_summary must name the denied persona; got {denial['error_summary']!r}"
    )


# ---------------------------------------------------------------------------
# Test 6: legacy token (no scopes) succeeds on everything — back-compat
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_legacy_token_unrestricted_backcompat():
    """Legacy token must pass tool, domain, and persona checks unchanged.

    Two identical calls with two legacy tokens must return byte-identical
    responses (same DB state, no scope filtering). Back-compat means zero
    behavioral change for callers that don't use identity/scope.
    """
    tenant_legacy = str(uuid.uuid4())  # fresh tenant, no triples

    tok1 = mint_token(tenant_legacy)["token"]
    tok2 = mint_token(tenant_legacy)["token"]

    verified1 = verify_token(tok1)
    verified2 = verify_token(tok2)

    # Legacy tokens carry no identity and no domain/persona scope.
    assert verified1.identity is None, "Legacy token must not carry identity"
    assert verified1.domain_scope == (), "Legacy token must have empty domain_scope"
    assert verified1.persona_scope == (), "Legacy token must have empty persona_scope"

    # Both tokens must succeed on list_domains (would be denied for scoped token).
    rt1 = bind_token_to_session(verified1)
    rtr1 = bind_transport("test-rbac-legacy")
    server1 = build_server()
    try:
        r1 = await _call(server1, "list_domains", {})
    finally:
        release_token(rt1)
        release_transport(rtr1)

    rt2 = bind_token_to_session(verified2)
    rtr2 = bind_transport("test-rbac-legacy2")
    server2 = build_server()
    try:
        r2 = await _call(server2, "list_domains", {})
    finally:
        release_token(rt2)
        release_transport(rtr2)

    assert r1.isError is False, (
        f"Legacy token must succeed on list_domains; got: "
        f"{r1.content[0].text if r1.content else r1}"
    )
    assert r2.isError is False, "Second legacy token must also succeed"

    # Byte-identical: same DB state → same response.
    text1 = r1.content[0].text if r1.content else "[]"
    text2 = r2.content[0].text if r2.content else "[]"
    assert json.loads(text1) == json.loads(text2), (
        f"Legacy token responses must be byte-identical; "
        f"got {text1!r} vs {text2!r}"
    )


# ---------------------------------------------------------------------------
# Test 7: verify_token round-trips identity + 3-axis scope correctly
# ---------------------------------------------------------------------------


def test_token_roundtrip_carries_all_axes():
    tenant = str(uuid.uuid4())
    minted = mint_token(
        tenant,
        scope=["query_triples", "list_runs"],
        identity="test-agent",
        domain_scope=["cloud_spend", "revenue"],
        persona_scope=["CFO", "CRO"],
    )
    tok = verify_token(minted["token"])

    assert tok.identity == "test-agent", f"identity mismatch: {tok.identity!r}"
    assert set(tok.scope) == {"query_triples", "list_runs"}, (
        f"tool scope mismatch: {tok.scope}"
    )
    assert set(tok.domain_scope) == {"cloud_spend", "revenue"}, (
        f"domain_scope mismatch: {tok.domain_scope}"
    )
    assert set(tok.persona_scope) == {"CFO", "CRO"}, (
        f"persona_scope mismatch: {tok.persona_scope}"
    )
    assert tok.tenant_id == tenant
    assert tok.token_id  # non-empty

    # Minted dict also carries the axes.
    assert minted["identity"] == "test-agent"
    assert set(minted["domain_scope"]) == {"cloud_spend", "revenue"}
    assert set(minted["persona_scope"]) == {"CFO", "CRO"}


# ---------------------------------------------------------------------------
# Test 8: in-scope persona call succeeds (CFO persona for CFO-scoped token)
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_in_scope_persona_call_succeeds():
    tok = _scoped_token()
    server, rt, rtr = _build_server_with_token(tok)
    try:
        # CFO persona is in persona_scope — must not be denied at the RBAC boundary.
        # Tool returns [] for a tenant with no triples, but it succeeds (no error).
        r = await _call(server, ALLOWED_TOOL, {"persona": ALLOWED_PERSONA})
    finally:
        release_token(rt)
        release_transport(rtr)

    assert r.isError is False, (
        f"Expected success for in-scope persona={ALLOWED_PERSONA}, "
        f"got error: {r.content[0].text if r.content else r}"
    )


# ---------------------------------------------------------------------------
# Test 9: denial audit rows are queryable via GET /api/dcl/traces
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_denials_visible_in_decision_traces():
    """After a domain denial, the trace is queryable via the traces endpoint."""
    from fastapi.testclient import TestClient
    from backend.api.main import app

    tc = TestClient(app, raise_server_exceptions=False)

    tok = _scoped_token()
    server, rt, rtr = _build_server_with_token(tok)
    since = time.time() - 1
    try:
        await _call(server, ALLOWED_TOOL, {"domain": DENIED_DOMAIN})
    finally:
        release_token(rt)
        release_transport(rtr)

    # Fetch traces for this tenant via the read endpoint.
    resp = tc.get(f"/api/dcl/traces?tenant_id={_TENANT}&trace_type=mcp_call&limit=20")
    assert resp.status_code == 200, f"traces endpoint failed: {resp.text}"
    body = resp.json()
    traces = body.get("traces", [])
    denials = [t for t in traces if t.get("outcome") == "unauthorized"]
    assert denials, (
        f"No unauthorized trace visible in decision_traces for tenant {_TENANT}; "
        f"got {len(traces)} traces total"
    )
    denial = denials[0]
    # The agent field = caller_token_id from mai_mcp_audit — identifies the caller.
    assert denial.get("agent"), "Denial trace must carry caller identity (token_id)"
    # Identity name is in payload._rbac_denied (enriched by _rbac_denial_enrichment).
    # decision_traces.payload = mai_mcp_audit.arguments.
    payload = denial.get("payload") or {}
    rbac = payload.get("_rbac_denied") if isinstance(payload, dict) else None
    assert rbac is not None, (
        f"Denial trace payload must carry '_rbac_denied' block; "
        f"got payload={payload!r}"
    )
    assert rbac.get("identity") == SCOPED_IDENTITY, (
        f"_rbac_denied.identity must be {SCOPED_IDENTITY!r}; got {rbac!r}"
    )
    assert rbac.get("axis") == "domain", (
        f"_rbac_denied.axis must be 'domain'; got {rbac!r}"
    )
    assert rbac.get("denied") == DENIED_DOMAIN, (
        f"_rbac_denied.denied must be {DENIED_DOMAIN!r}; got {rbac!r}"
    )
