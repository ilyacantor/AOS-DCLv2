"""Gate 3C D3: consumer migration + acceptance e2e.

Operator-visible outcome:
  finops-cloud-spend calling query_triples(domain='revenue') is DENIED with a
  readable reason naming 'finops-cloud-spend', and the trace appears in
  mai_mcp_audit; the same identity calling list_domains returns BYTE-IDENTICAL
  bytes to a legacy (unscoped) token against the same DB state.
  demo-panel-b calling list_domains returns BYTE-IDENTICAL bytes to a legacy token.
  A 2-step distinct-approver chain: proposer approve DENIED (traced), A1 step1
  → still pending, A2 step2 → canonical + both approve traces present.
  Demo wire boundary: mint_demo_token() (now identity='demo-panel-b') succeeds
  on every tool in TOKEN_SCOPE at the wire boundary.

Consumer inventory (wire-protocol vs legacy-shim):
  WIRE-PROTOCOL (D1 enforces — migration targets here):
    demo-panel-b       demo/panel_b.py    DCL-local SSE
                       tools: TOKEN_SCOPE (8 tools), domains: unrestricted,
                       personas: unrestricted. Identity added this session.
    finops-cloud-spend finops/server/mcp/ cross-repo SSE
                       tools: [query_triples,list_domains,list_runs,
                               concept_lookup,semantic_export,provenance],
                       domains: [cloud_spend], personas: unrestricted.
                       HANDOFF: finops must pass identity='finops-cloud-spend'.
    console-dcl-client console/backend/   cross-repo SSE
                       tools: [query_triples,list_domains,concept_lookup,
                               semantic_export,provenance],
                       domains: unrestricted, personas: unrestricted.
                       HANDOFF: console must pass identity='console-dcl-client'.
    aam-readback-agent aam/tests/         cross-repo SSE
                       tools: [query_triples],
                       domains: [cloud_spend,revenue,support,customer,service],
                       personas: unrestricted.
                       HANDOFF: aam must pass identity='aam-readback-agent'.
  LEGACY-SHIM (D1 does NOT enforce — #75/#81, NO investment):
    Mai/Platform       POST /api/mcp/tools/call with MCP_API_KEY.
                       Shim does not enforce D1 scopes by design.
                       Shim retirement is a future cross-repo session (#81).
  NOT-MCP:
    NLQ                Uses /query /entities /health REST — not an MCP consumer.
    Farm test          Direct Python import — no wire boundary.

Live tests against aos-dev (B14-safe: per-run-unique tenants where mutable).
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

if not os.environ.get("DCL_MCP_TOKEN_SECRET"):
    from dotenv import dotenv_values
    _live = dotenv_values(_repo / ".env").get("DCL_MCP_TOKEN_SECRET")
    if _live:
        os.environ["DCL_MCP_TOKEN_SECRET"] = _live
os.environ.setdefault("DCL_MCP_TOKEN_SECRET", "gate3c-d3-test-secret-do-not-use")

from mcp import types as _t
from backend.api.mcp_auth import mint_token, verify_token
from backend.api.mcp_server_real import (
    bind_token_to_session,
    bind_transport,
    build_server,
    release_token,
    release_transport,
)
from backend.core.db import get_connection
from fastapi.testclient import TestClient
from backend.api.main import app

pytestmark = pytest.mark.anyio

_TC = TestClient(app, raise_server_exceptions=False)

# Per-run unique tenants — no cross-run contamination (B14-safe).
TAG = uuid.uuid4().hex[:6]
_TENANT_RBAC = str(uuid.uuid4())    # for scope-enforcement tests
_TENANT_CHAIN = str(uuid.uuid4())   # for approval-chain e2e
_CHAIN_PROPOSER  = f"proposer-{TAG}"
_CHAIN_APPROVER1 = f"approver1-{TAG}"
_CHAIN_APPROVER2 = f"approver2-{TAG}"

# finops-cloud-spend scopes (canonical scope for the handoff registry).
_FINOPS_TOOLS = [
    "query_triples", "list_domains", "list_runs",
    "concept_lookup", "semantic_export", "provenance",
]
_FINOPS_DOMAINS = ["cloud_spend"]

# demo-panel-b scopes (matches demo/panel_b.py TOKEN_SCOPE exactly).
_DEMO_TOOLS = [
    "query_triples", "list_domains", "list_runs",
    "concept_lookup", "semantic_export", "provenance",
    "conflict_query", "reconciliation_recommend",
]


# ---------------------------------------------------------------------------
# anyio isolation (mirrors test_mcp_rbac.py / test_mcp_wp5.py fixture)
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _isolate_anyio_from_playwright():
    import anyio.pytest_plugin as _ap
    saved = None
    try:
        saved = asyncio.events._get_running_loop()
        asyncio.events._set_running_loop(None)
    except Exception:
        pass
    _ap._runner_leases = 0
    _ap._runner_stack = None
    _ap._current_runner = None
    try:
        yield
    finally:
        if saved is not None:
            try:
                asyncio.events._set_running_loop(saved)
            except Exception:
                pass


@pytest.fixture
def anyio_backend():
    return "asyncio"


# ---------------------------------------------------------------------------
# Registry helpers — insert identity rows for the per-run test tenant
# ---------------------------------------------------------------------------

def _register_identity(
    tenant_id: str,
    identity_name: str,
    tool_scope: list[str],
    domain_scope: list[str],
    persona_scope: list[str] | None = None,
) -> None:
    """Upsert an identity row in mcp_agent_identities for the test tenant."""
    sql = """
        INSERT INTO mcp_agent_identities
               (tenant_id, identity_name, tool_scope, domain_scope, persona_scope)
        VALUES (%s::uuid, %s, %s, %s, %s)
        ON CONFLICT (tenant_id, identity_name) DO UPDATE
            SET tool_scope = EXCLUDED.tool_scope,
                domain_scope = EXCLUDED.domain_scope,
                persona_scope = EXCLUDED.persona_scope
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (
                tenant_id, identity_name,
                tool_scope, domain_scope, persona_scope or [],
            ))
        conn.commit()


def _resolve_registered_scopes(tenant_id: str, identity_name: str) -> dict:
    sql = (
        "SELECT tool_scope, domain_scope, persona_scope "
        "FROM mcp_agent_identities WHERE tenant_id=%s::uuid AND identity_name=%s"
    )
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (tenant_id, identity_name))
            row = cur.fetchone()
            if row is None:
                raise AssertionError(
                    f"identity {identity_name!r} not found in registry "
                    f"for tenant {tenant_id}"
                )
            return {"tools": list(row[0] or []),
                    "domains": list(row[1] or []),
                    "personas": list(row[2] or [])}


@pytest.fixture(autouse=True, scope="module")
def _register_test_identities():
    """Insert canonical identity rows for the RBAC test tenant once per module run."""
    _register_identity(
        _TENANT_RBAC, "finops-cloud-spend",
        _FINOPS_TOOLS, _FINOPS_DOMAINS,
    )
    _register_identity(
        _TENANT_RBAC, "demo-panel-b",
        _DEMO_TOOLS, [],
    )
    _register_identity(
        _TENANT_RBAC, "console-dcl-client",
        ["query_triples", "list_domains", "concept_lookup", "semantic_export", "provenance"],
        [],
    )
    _register_identity(
        _TENANT_RBAC, "aam-readback-agent",
        ["query_triples"],
        ["cloud_spend", "revenue", "support", "customer", "service"],
    )
    yield


# ---------------------------------------------------------------------------
# MCP server call helpers
# ---------------------------------------------------------------------------

async def _call(server, name: str, arguments: dict) -> _t.CallToolResult:
    handler = server.request_handlers[_t.CallToolRequest]
    req = _t.CallToolRequest(
        method="tools/call",
        params=_t.CallToolRequestParams(name=name, arguments=arguments),
    )
    return (await handler(req)).root


def _build_server(token_str: str):
    verified = verify_token(token_str)
    rt = bind_token_to_session(verified)
    rtr = bind_transport("test-gate3c")
    return build_server(), rt, rtr


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


# ---------------------------------------------------------------------------
# (a) Out-of-scope DENIED + loud trace
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_a1_out_of_scope_domain_denied_with_trace():
    """finops-cloud-spend calling domain='revenue' is denied with a readable
    reason naming 'finops-cloud-spend', and a trace row is written to
    mai_mcp_audit (visible via decision_traces)."""
    tok = mint_token(
        _TENANT_RBAC,
        scope=_FINOPS_TOOLS,
        identity="finops-cloud-spend",
        domain_scope=_FINOPS_DOMAINS,
    )["token"]
    server, rt, rtr = _build_server(tok)
    since = time.time() - 1
    try:
        r = await _call(server, "query_triples", {"domain": "revenue"})
    finally:
        release_token(rt); release_transport(rtr)

    assert r.isError is True, (
        "Expected denial for domain='revenue' (not in cloud_spend scope); "
        f"got success: {r}"
    )
    text = r.content[0].text if r.content else ""
    assert "finops-cloud-spend" in text, (
        f"Denial must name the identity 'finops-cloud-spend'; got: {text!r}"
    )
    assert "revenue" in text, f"Denial must name the denied domain; got: {text!r}"

    # Trace must be in mai_mcp_audit.
    rows = _audit_rows_since(_TENANT_RBAC, since)
    denials = [row for row in rows if row["outcome"] == "unauthorized"]
    assert denials, "No unauthorized audit row found after out-of-scope domain call"
    d = denials[0]
    assert "finops-cloud-spend" in (d["error_summary"] or ""), (
        f"audit error_summary must name identity; got {d['error_summary']!r}"
    )
    assert "revenue" in (d["error_summary"] or ""), (
        f"audit error_summary must name denied domain; got {d['error_summary']!r}"
    )
    # Structured _rbac_denied block in arguments.
    args = d.get("arguments") or {}
    rbac = args.get("_rbac_denied") if isinstance(args, dict) else None
    assert rbac is not None, f"audit arguments must carry _rbac_denied block; got {args!r}"
    assert rbac["identity"] == "finops-cloud-spend"
    assert rbac["axis"] == "domain"
    assert rbac["denied"] == "revenue"


@pytest.mark.anyio
async def test_a2_out_of_scope_tool_denied_traces_identity():
    """finops-cloud-spend calling an out-of-scope tool (trace_query) is denied
    and the trace names the identity."""
    tok = mint_token(
        _TENANT_RBAC,
        scope=_FINOPS_TOOLS,
        identity="finops-cloud-spend",
        domain_scope=_FINOPS_DOMAINS,
    )["token"]
    server, rt, rtr = _build_server(tok)
    since = time.time() - 1
    try:
        r = await _call(server, "trace_query", {})
    finally:
        release_token(rt); release_transport(rtr)

    assert r.isError is True
    text = r.content[0].text if r.content else ""
    assert "finops-cloud-spend" in text, f"Identity missing from denial: {text!r}"
    assert "trace_query" in text, f"Tool name missing from denial: {text!r}"

    # GET /api/dcl/traces must surface the denial.
    resp = _TC.get(
        f"/api/dcl/traces?tenant_id={_TENANT_RBAC}&trace_type=mcp_call&limit=20"
    )
    assert resp.status_code == 200, f"traces endpoint failed: {resp.text}"
    denials = [
        t for t in resp.json().get("traces", [])
        if t.get("outcome") == "unauthorized"
    ]
    assert denials, (
        f"No unauthorized trace visible in GET /api/dcl/traces for tenant {_TENANT_RBAC}"
    )
    rbac = (denials[0].get("payload") or {}).get("_rbac_denied")
    assert rbac is not None, f"_rbac_denied missing from trace payload; trace={denials[0]}"
    assert rbac["identity"] == "finops-cloud-spend"
    assert rbac["axis"] == "tool"


# ---------------------------------------------------------------------------
# (b) In-scope BYTE-IDENTICAL
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_b1_finops_in_scope_byte_identical_to_legacy():
    """finops-cloud-spend calling list_domains returns BYTE-IDENTICAL bytes
    to a legacy (unscoped) token.  list_domains is config-driven; result is
    deterministic regardless of DB state."""
    legacy_tok = mint_token(_TENANT_RBAC)["token"]
    scoped_tok = mint_token(
        _TENANT_RBAC,
        scope=_FINOPS_TOOLS,
        identity="finops-cloud-spend",
        domain_scope=_FINOPS_DOMAINS,
    )["token"]

    server_l, rt_l, rtr_l = _build_server(legacy_tok)
    try:
        r_legacy = await _call(server_l, "list_domains", {})
    finally:
        release_token(rt_l); release_transport(rtr_l)

    server_s, rt_s, rtr_s = _build_server(scoped_tok)
    try:
        r_scoped = await _call(server_s, "list_domains", {})
    finally:
        release_token(rt_s); release_transport(rtr_s)

    assert r_legacy.isError is False, (
        f"Legacy token failed on list_domains: {r_legacy}"
    )
    assert r_scoped.isError is False, (
        f"finops-cloud-spend token failed on in-scope list_domains: "
        f"{r_scoped.content[0].text if r_scoped.content else r_scoped}"
    )
    data_legacy = json.loads(r_legacy.content[0].text)
    data_scoped = json.loads(r_scoped.content[0].text)
    assert data_legacy == data_scoped, (
        "BYTE-IDENTICAL FAIL: finops-cloud-spend in-scope list_domains response "
        f"differs from legacy token.\nlegacy: {data_legacy}\nscoped: {data_scoped}"
    )


@pytest.mark.anyio
async def test_b2_demo_panel_b_in_scope_byte_identical_to_legacy():
    """demo-panel-b calling list_domains returns BYTE-IDENTICAL bytes to a
    legacy token.  Proves the panel_b.py identity='demo-panel-b' migration
    is behavior-neutral for all in-scope calls."""
    legacy_tok = mint_token(_TENANT_RBAC)["token"]
    # Match exactly what mint_demo_token() now produces.
    demo_tok = mint_token(
        _TENANT_RBAC,
        scope=_DEMO_TOOLS,
        identity="demo-panel-b",
    )["token"]

    server_l, rt_l, rtr_l = _build_server(legacy_tok)
    try:
        r_legacy = await _call(server_l, "list_domains", {})
    finally:
        release_token(rt_l); release_transport(rtr_l)

    server_d, rt_d, rtr_d = _build_server(demo_tok)
    try:
        r_demo = await _call(server_d, "list_domains", {})
    finally:
        release_token(rt_d); release_transport(rtr_d)

    assert r_legacy.isError is False
    assert r_demo.isError is False, (
        f"demo-panel-b token failed on in-scope list_domains: "
        f"{r_demo.content[0].text if r_demo.content else r_demo}"
    )
    assert json.loads(r_legacy.content[0].text) == json.loads(r_demo.content[0].text), (
        "BYTE-IDENTICAL FAIL: demo-panel-b list_domains differs from legacy token"
    )


@pytest.mark.anyio
async def test_b3_aam_readback_agent_in_scope_byte_identical_to_legacy():
    """aam-readback-agent calling list_runs (in its tool scope) returns
    BYTE-IDENTICAL bytes to a legacy token for the same tenant.
    list_runs for a fresh per-run tenant returns [] deterministically."""
    fresh_tenant = str(uuid.uuid4())  # no runs, empty list guaranteed
    legacy_tok = mint_token(fresh_tenant)["token"]
    aam_tok = mint_token(
        fresh_tenant,
        scope=["query_triples"],
        identity="aam-readback-agent",
        domain_scope=["cloud_spend", "revenue", "support", "customer", "service"],
    )["token"]

    # list_runs is NOT in aam-readback-agent's tool scope — use query_triples.
    # For a tenant with no triples, query_triples(domain='cloud_spend') = [].
    server_l, rt_l, rtr_l = _build_server(legacy_tok)
    try:
        r_legacy = await _call(server_l, "query_triples", {"domain": "cloud_spend"})
    finally:
        release_token(rt_l); release_transport(rtr_l)

    server_a, rt_a, rtr_a = _build_server(aam_tok)
    try:
        r_aam = await _call(server_a, "query_triples", {"domain": "cloud_spend"})
    finally:
        release_token(rt_a); release_transport(rtr_a)

    assert r_legacy.isError is False
    assert r_aam.isError is False, (
        f"aam-readback-agent failed on in-scope query_triples(cloud_spend): "
        f"{r_aam.content[0].text if r_aam.content else r_aam}"
    )
    assert json.loads(r_legacy.content[0].text) == json.loads(r_aam.content[0].text), (
        "BYTE-IDENTICAL FAIL: aam-readback-agent query_triples(cloud_spend) "
        "differs from legacy token"
    )


# ---------------------------------------------------------------------------
# (c) Two-step approval chain end-to-end
# ---------------------------------------------------------------------------

def _set_chain_policy(tenant_id: str) -> None:
    r = _TC.put("/api/dcl/approval-policy", json={
        "tenant_id": tenant_id,
        "require_distinct_proposer_approver": True,
        "chain_steps": 2,
    })
    assert r.status_code == 200, f"policy set failed {r.status_code}: {r.text}"


def _create_proposal(tenant_id: str, proposer: str) -> str:
    r = _TC.post("/api/dcl/proposals", json={
        "tenant_id": tenant_id,
        "proposals": [{
            "proposal_type": "authority_map",
            "payload": {"concept_prefix": "revenue", "ranked_sources": ["ERP", "CRM"]},
            "confidence": 0.9,
            "provenance": {"basis": "confirmed", "confirmed_by": proposer},
            "proposer": proposer,
        }],
    })
    assert r.status_code == 201, f"intake failed {r.status_code}: {r.text}"
    proposals = r.json()["proposals"]
    assert proposals[0]["status"] == "accepted"
    return proposals[0]["proposal_id"]


def _decide_proposal(tenant_id: str, pid: str, decision: str, decided_by: str):
    return _TC.post(f"/api/dcl/proposals/{pid}/decide", json={
        "tenant_id": tenant_id,
        "decision": decision,
        "decided_by": decided_by,
    })


def _fetch_proposal(tenant_id: str, pid: str) -> dict | None:
    r = _TC.get("/api/dcl/proposals", params={"tenant_id": tenant_id})
    assert r.status_code == 200
    return next((p for p in r.json()["proposals"] if p["proposal_id"] == pid), None)


def _chain_traces(tenant_id: str, pid: str) -> list[dict]:
    r = _TC.get("/api/dcl/traces", params={"tenant_id": tenant_id})
    if r.status_code != 200:
        return []
    return [t for t in r.json().get("traces", [])
            if (t.get("refs") or {}).get("proposal_id") == pid]


def test_c_two_step_approval_chain_e2e():
    """Gate 3C e2e approval chain:
    - Proposer creates a proposal.
    - Proposer self-approve → DENIED 409 + denial trace.
    - A1 approves step 1 → 200, is_final=False, canonical_artifact_id=None, step1 trace.
    - A1 re-approves step 2 → DENIED 409 (already approved step 1).
    - A2 approves step 2 → 200, is_final=True, canonical_artifact_id set, step2 trace.
    Each link traced in decision_traces.
    """
    _set_chain_policy(_TENANT_CHAIN)
    pid = _create_proposal(_TENANT_CHAIN, _CHAIN_PROPOSER)

    # Proposer self-approve → 409 denied.
    r = _decide_proposal(_TENANT_CHAIN, pid, "approve", _CHAIN_PROPOSER)
    assert r.status_code == 409, (
        f"Proposer self-approve must be denied 409; got {r.status_code}: {r.text}"
    )
    assert "cannot approve their own proposal" in r.json().get("detail", ""), r.text

    # Proposal still pending.
    p = _fetch_proposal(_TENANT_CHAIN, pid)
    assert p is not None
    assert p["status"] == "pending", f"Must be pending after denied self-approve; got {p['status']}"
    assert p.get("canonical_artifact_id") is None

    # Denial trace exists.
    traces = _chain_traces(_TENANT_CHAIN, pid)
    assert any(t.get("decision_type") == "denied" for t in traces), (
        f"No denied trace after self-approve attempt; traces={traces}"
    )

    # A1 approves step 1.
    r = _decide_proposal(_TENANT_CHAIN, pid, "approve", _CHAIN_APPROVER1)
    assert r.status_code == 200, f"A1 step1 failed {r.status_code}: {r.text}"
    d = r.json()
    assert d["step_number"] == 1
    assert d["chain_steps"] == 2
    assert d["is_final"] is False, "Step 1 must not be final in a 2-step chain"
    assert d["canonical_artifact_id"] is None, "canonical must NOT be applied at step 1"

    # Proposal still pending after step 1.
    p = _fetch_proposal(_TENANT_CHAIN, pid)
    assert p["status"] == "pending", f"Proposal must stay pending after step1; got {p['status']}"

    # Step-1 trace exists.
    traces = _chain_traces(_TENANT_CHAIN, pid)
    assert any(t.get("decision_type") == "approve" for t in traces), (
        "No approve trace found after step 1"
    )

    # A1 tries step 2 (same approver denied — already approved step 1).
    r = _decide_proposal(_TENANT_CHAIN, pid, "approve", _CHAIN_APPROVER1)
    assert r.status_code == 409, (
        f"A1 re-approving step 2 must be denied 409; got {r.status_code}: {r.text}"
    )
    assert "already approved a prior step" in r.json().get("detail", ""), r.text

    # A2 approves step 2 → final.
    r = _decide_proposal(_TENANT_CHAIN, pid, "approve", _CHAIN_APPROVER2)
    assert r.status_code == 200, f"A2 step2 failed {r.status_code}: {r.text}"
    d = r.json()
    assert d["step_number"] == 2
    assert d["chain_steps"] == 2
    assert d["is_final"] is True, "Step 2 must be final"
    assert d["canonical_artifact_id"] is not None, "canonical must be applied at step 2"

    # Proposal now approved.
    p = _fetch_proposal(_TENANT_CHAIN, pid)
    assert p["status"] == "approved", f"Proposal must be approved after step2; got {p['status']}"
    assert p["canonical_artifact_id"] is not None

    # Both approve traces present.
    traces = _chain_traces(_TENANT_CHAIN, pid)
    approve_traces = [t for t in traces if t.get("decision_type") == "approve"]
    assert len(approve_traces) == 2, (
        f"Expected 2 approve traces (step1 + step2); got {len(approve_traces)}: {approve_traces}"
    )


# ---------------------------------------------------------------------------
# (d) Demo wire-boundary proof
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_d1_demo_panel_b_token_accepted_on_all_demo_tools():
    """mint_demo_token() (now identity='demo-panel-b') produces a token that
    is accepted at the wire boundary for every tool in TOKEN_SCOPE.
    No tool in TOKEN_SCOPE is denied — the migration is behavior-neutral."""
    from demo.panel_b import mint_demo_token, TOKEN_SCOPE

    fresh_tenant = str(uuid.uuid4())
    minted = mint_demo_token(fresh_tenant)

    # Verify the identity was embedded.
    verified = verify_token(minted["token"])
    assert verified.identity == "demo-panel-b", (
        f"mint_demo_token must embed identity='demo-panel-b'; got {verified.identity!r}"
    )
    assert set(verified.scope) == set(TOKEN_SCOPE), (
        f"mint_demo_token scope mismatch: {set(verified.scope)} != {set(TOKEN_SCOPE)}"
    )
    assert verified.domain_scope == (), (
        f"demo-panel-b must have unrestricted domains; got {verified.domain_scope}"
    )
    assert verified.persona_scope == (), (
        f"demo-panel-b must have unrestricted personas; got {verified.persona_scope}"
    )

    # Every tool in TOKEN_SCOPE must NOT be denied at the wire boundary.
    # (list_domains and list_runs return data for any tenant; others return []
    # for a fresh tenant — but none should be denied by scope enforcement.)
    server, rt, rtr = _build_server(minted["token"])
    try:
        for tool_name in TOKEN_SCOPE:
            r = await _call(server, tool_name, {})
            # An isError can mean a real tool error (e.g. missing args) — but
            # scope denial has a specific prefix. Scope denials are the failure;
            # tool-level errors (e.g. "missing required param") are acceptable.
            if r.isError:
                text = r.content[0].text if r.content else ""
                assert "is not scoped for tool" not in text, (
                    f"Tool {tool_name!r} was SCOPE-DENIED for demo-panel-b; "
                    f"migration scope is wrong. Denial: {text!r}"
                )
                assert "is not scoped for domain" not in text, (
                    f"Tool {tool_name!r} was DOMAIN-DENIED for demo-panel-b. "
                    f"Denial: {text!r}"
                )
    finally:
        release_token(rt)
        release_transport(rtr)


@pytest.mark.anyio
async def test_d2_demo_token_identity_visible_in_registry():
    """The canonical demo-panel-b identity is registered and resolves correctly
    from mcp_agent_identities (round-trip: insert in fixture → resolve → check scopes)."""
    scopes = _resolve_registered_scopes(_TENANT_RBAC, "demo-panel-b")
    assert set(scopes["tools"]) == set(_DEMO_TOOLS), (
        f"Registry demo-panel-b tool_scope mismatch: {scopes['tools']} != {_DEMO_TOOLS}"
    )
    assert scopes["domains"] == [], (
        f"demo-panel-b must have empty domain_scope (unrestricted); got {scopes['domains']}"
    )
    assert scopes["personas"] == [], (
        f"demo-panel-b must have empty persona_scope (unrestricted); got {scopes['personas']}"
    )


def test_d3_finops_identity_registered_with_correct_scopes():
    """finops-cloud-spend registered scopes match the canonical handoff spec."""
    scopes = _resolve_registered_scopes(_TENANT_RBAC, "finops-cloud-spend")
    assert set(scopes["tools"]) == set(_FINOPS_TOOLS), (
        f"Registry finops-cloud-spend tool_scope: {scopes['tools']}"
    )
    assert set(scopes["domains"]) == {"cloud_spend"}, (
        f"finops-cloud-spend must be scoped to cloud_spend; got {scopes['domains']}"
    )


def test_d4_console_and_aam_identities_registered():
    """console-dcl-client and aam-readback-agent are registered (DCL-side ready)."""
    scopes_c = _resolve_registered_scopes(_TENANT_RBAC, "console-dcl-client")
    assert "query_triples" in scopes_c["tools"]
    assert "provenance" in scopes_c["tools"]
    assert scopes_c["domains"] == [], "console has unrestricted domains"

    scopes_a = _resolve_registered_scopes(_TENANT_RBAC, "aam-readback-agent")
    assert scopes_a["tools"] == ["query_triples"]
    assert set(scopes_a["domains"]) == {
        "cloud_spend", "revenue", "support", "customer", "service"
    }
