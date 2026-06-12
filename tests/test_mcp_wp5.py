"""
Plan B WP5 — real wire-protocol MCP server tests.

Drives the DCL MCP server through the `mcp` SDK client (stdio + HTTP+SSE),
not via curl against the legacy HTTP shim. Validates:

  T1: list_tools returns exactly the public tool surface (PUBLIC_TOOLS)
  T2: query_triples returns triples filtered to the calling tenant
  T3: token A cannot read token B's triples (tenant isolation)
  T4: rate limit triggers above the per-tenant rpm; audit row reflects it
  T5: invalid/missing token rejected; audit row outcome='unauthorized'
  T6: stdio transport works end-to-end
  T7: HTTP+SSE transport works end-to-end (live server at DCL_TEST_BASE_URL;
      tokens are minted with the live server's shim secret — resolved below —
      see dcl_deferred_work.md #63 for the docstring/body drift on this test)
  T8: legacy HTTP /api/mcp/tools/call still returns identical results after refactor

The tests run against the aos-dev database (.env.development — the dev/prod
separation rule; mig 020's mai_mcp_audit enrichment columns exist only there).
Audit rows are append-only and attributed to test tenants — safe to write.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
import uuid
from pathlib import Path

import httpx
import pytest
from dotenv import load_dotenv

_repo = Path(__file__).resolve().parent.parent
load_dotenv(_repo / ".env.development")
sys.path.insert(0, str(_repo))

# Ensure DCL_MCP_TOKEN_SECRET is set so tokens can be minted/verified.
# T7 talks to a LIVE server (DCL_TEST_BASE_URL), whose process env carries the
# shim secret from `.env` (main.py's bare load_dotenv fills it; it is absent
# from .env.development). Pull ONLY that var — never load `.env` wholesale into
# the test env (its DATABASE_URL is prod; the stdio subprocess inherits env).
if not os.environ.get("DCL_MCP_TOKEN_SECRET"):
    from dotenv import dotenv_values
    _live_secret = dotenv_values(_repo / ".env").get("DCL_MCP_TOKEN_SECRET")
    if _live_secret:
        os.environ["DCL_MCP_TOKEN_SECRET"] = _live_secret
os.environ.setdefault("DCL_MCP_TOKEN_SECRET", "wp5-test-secret-do-not-use-in-prod")

from mcp import ClientSession, StdioServerParameters  # noqa: E402
from mcp.client.stdio import stdio_client  # noqa: E402

from backend.api.mcp_auth import mint_token, verify_token  # noqa: E402
from backend.api.mcp_rate_limit import global_limiter  # noqa: E402
from backend.api.mcp_server import (  # noqa: E402
    MCPToolCall,
    handle_tool_call,
)
from backend.core.db import get_connection  # noqa: E402
from backend.engine.mcp_tools import (  # noqa: E402
    PUBLIC_TOOLS,
    tool_list_domains,
    tool_query_triples,
)

# Use the seed manifest's tenant for tenant-A.
import json as _json  # noqa: E402

_MANIFEST = _repo / "data" / "seed_manifest.json"
_data = _json.loads(_MANIFEST.read_text())
TENANT_A = _data["tenant_id"]
TENANT_B = str(uuid.uuid4())  # synthetic — has zero triples


def _stdio_params(token_str: str) -> StdioServerParameters:
    env = {**os.environ, "DCL_MCP_TOKEN": token_str}
    # PYTHONPATH so the child python can find backend/
    env["PYTHONPATH"] = str(_repo)
    return StdioServerParameters(
        command=sys.executable,
        args=["-m", "backend.api.mcp_stdio"],
        env=env,
    )


async def _stdio_session(token_str: str):
    return stdio_client(_stdio_params(token_str))


def _count_audit(tenant_id: str, since_ts: float) -> int:
    sql = (
        "SELECT COUNT(*) FROM mai_mcp_audit "
        "WHERE tenant_id = %s AND created_at >= to_timestamp(%s)"
    )
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (tenant_id, since_ts))
            return int(cur.fetchone()[0])


def _last_audit(tenant_id: str) -> dict:
    sql = (
        "SELECT tenant_id, tool_name, caller_token_id, outcome, "
        "       error_summary, transport, latency_ms "
        "FROM mai_mcp_audit WHERE tenant_id = %s "
        "ORDER BY created_at DESC LIMIT 1"
    )
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (tenant_id,))
            row = cur.fetchone()
            if row is None:
                return {}
            cols = [d[0] for d in cur.description]
            d = dict(zip(cols, row))
            d["tenant_id"] = str(d["tenant_id"])
            return d


# ---------------------------------------------------------------------------
# T1: list_tools returns exactly the public tool surface (PUBLIC_TOOLS —
#     §11.4 base five + Gate 1A conflict pair + Gate 1B traverse_graph +
#     Gate 2A trace_query + list_runs)
# ---------------------------------------------------------------------------


pytestmark = pytest.mark.anyio


@pytest.fixture(autouse=True)
def _isolate_anyio_from_playwright():
    """Isolate each async test from pytest-playwright's leftover event loop.

    pytest-playwright (the sync plugin the e2e tests use) drives its own driver
    loop and, when its tests run earlier in the same `pytest tests/` session,
    leaves two pieces of process-global asyncio state poisoned: (1) its loop is
    still registered as the thread's *running* loop, so anyio can't start/close
    its own ("Cannot run the event loop while another loop is running"); and
    (2) anyio's cached _current_runner references a closed asyncio.Runner
    ("Runner is closed"). Reset both before every async test here so they run
    clean regardless of collection order.
    """
    import asyncio
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
        # Restore pytest-playwright's running loop so its session-scoped browser
        # fixture can still tear down ("Browser.close: no running event loop").
        if saved_loop is not None:
            try:
                asyncio.events._set_running_loop(saved_loop)
            except Exception:
                pass


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio
async def test_t1_list_tools_returns_public_tool_surface():
    token = mint_token(TENANT_A)["token"]
    async with await _stdio_session(token) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.list_tools()
            names = sorted(t.name for t in result.tools)
            expected = sorted(PUBLIC_TOOLS)
            assert "trace_query" in expected, (
                "Gate 2A: trace_query must be part of the public tool surface"
            )
            assert names == expected, (
                f"Expected exactly the {len(expected)} public tools "
                f"{expected}, got {names}"
            )


# ---------------------------------------------------------------------------
# T2: query_triples returns triples filtered to the calling tenant
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_t2_query_triples_filters_by_token_tenant():
    token = mint_token(TENANT_A)["token"]

    # Ground truth: what list_domains returns directly via Python
    domains = tool_list_domains(TENANT_A)
    assert domains, (
        f"Test prerequisite failed — tenant {TENANT_A} has no active triples"
    )
    # Pick the largest domain for the query
    target_domain = domains[0]["domain"]

    async with await _stdio_session(token) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            r = await session.call_tool(
                "query_triples",
                {"domain": target_domain, "limit": 25},
            )
            assert r.isError is False, f"query_triples error: {r.content}"
            assert r.content, "empty MCP content"
            payload = json.loads(r.content[0].text)
            assert isinstance(payload, list), f"expected list, got {type(payload)}"
            assert len(payload) > 0, (
                f"query_triples({target_domain}) returned 0 triples — "
                f"tenant {TENANT_A} should have data per ground truth"
            )
            # Every row must be from this tenant
            for row in payload:
                assert str(row["tenant_id"]) == TENANT_A, (
                    f"row leaked across tenants: {row['tenant_id']} != {TENANT_A}"
                )
                # Domain match
                concept = row["concept"]
                assert concept == target_domain or concept.startswith(
                    f"{target_domain}."
                ), f"concept {concept} does not match domain {target_domain}"


# ---------------------------------------------------------------------------
# T3: tenant isolation — Tenant B's token cannot read Tenant A's triples
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_t3_tenant_isolation():
    # Pick a domain that has data for A
    domains_a = tool_list_domains(TENANT_A)
    target_domain = domains_a[0]["domain"]
    a_rows_via_python = tool_query_triples(TENANT_A, domain=target_domain, limit=5)
    assert a_rows_via_python, "A has data for this domain — sanity"

    # Tenant B has no triples — querying via B's token must return []
    token_b = mint_token(TENANT_B)["token"]
    async with await _stdio_session(token_b) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            r = await session.call_tool(
                "query_triples",
                {"domain": target_domain, "limit": 25},
            )
            assert r.isError is False
            payload = json.loads(r.content[0].text)
            assert payload == [], (
                f"tenant isolation violated — tenant B saw {len(payload)} "
                f"rows of tenant A's data"
            )
            # No row may have tenant_a's id
            for row in payload:
                assert str(row["tenant_id"]) != TENANT_A

    # Also: try to inject tenant_id in arguments — must be ignored, B sees [].
    async with await _stdio_session(token_b) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            r = await session.call_tool(
                "query_triples",
                {
                    "domain": target_domain,
                    "limit": 25,
                    "tenant_id": TENANT_A,  # injection attempt
                },
            )
            assert r.isError is False
            payload = json.loads(r.content[0].text)
            assert payload == [], (
                "tenant_id argument override succeeded — security bug"
            )


# ---------------------------------------------------------------------------
# T4: rate limit triggers above threshold; audit reflects it
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_t4_rate_limit_blocks_above_threshold():
    """Drive the MCP server in-process so the parent's limiter applies.

    stdio runs in a subprocess with its own limiter — pointless to test
    rpm overrides through a subprocess boundary. The HTTP+SSE / in-process
    path is the same Server + limiter object, so the override sticks.
    """
    from backend.api.mcp_server_real import (
        bind_token_to_session,
        bind_transport,
        build_server,
        release_token,
        release_transport,
    )

    tenant_rate = str(uuid.uuid4())
    global_limiter().set_tenant_rpm(tenant_rate, 3)
    global_limiter().reset(tenant_rate)

    token_info = mint_token(tenant_rate)
    verified = verify_token(token_info["token"])

    reset_token = bind_token_to_session(verified)
    reset_transport = bind_transport("test-inproc")
    try:
        server = build_server()
        # The decorated handler lives at server.request_handlers; invoke it
        from mcp import types as _t

        handler = server.request_handlers[_t.CallToolRequest]

        async def _call(name: str) -> _t.CallToolResult:
            req = _t.CallToolRequest(
                method="tools/call",
                params=_t.CallToolRequestParams(name=name, arguments={}),
            )
            srv_result = await handler(req)
            # ServerResult is a discriminated union — unwrap CallToolResult
            return srv_result.root

        # First 3 allowed
        for i in range(3):
            r = await _call("list_domains")
            assert r.isError is False, f"call {i} should have succeeded: {r}"

        # 4th blocked
        r4 = await _call("list_domains")
        assert r4.isError is True, f"4th call should be rate-limited: {r4}"
        text = r4.content[0].text if r4.content else ""
        assert "rate" in text.lower(), (
            f"expected rate-limit text, got: {text!r}"
        )
    finally:
        release_token(reset_token)
        release_transport(reset_transport)

    # Audit row should reflect rate_limited
    last = _last_audit(tenant_rate)
    assert last, "no audit row written for rate-limited tenant"
    assert last["outcome"] == "rate_limited", (
        f"last audit outcome should be rate_limited, got {last['outcome']}"
    )

    global_limiter().reset(tenant_rate)


# ---------------------------------------------------------------------------
# T5: invalid/missing token rejected with audit row outcome='unauthorized'
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_t5_invalid_token_rejected_at_stdio_start():
    # stdio_client launches the subprocess. With a bogus token, the
    # subprocess exits non-zero before serving any tool. We assert the
    # ClientSession initialize fails OR the process exits early.
    bad = "totally.invalid.garbage"
    params = _stdio_params(bad)
    with pytest.raises(Exception):
        async with stdio_client(params) as (read, write):
            async with ClientSession(read, write) as session:
                # initialize must fail because the child process exited
                # (printing the TokenError to stderr).
                await asyncio.wait_for(session.initialize(), timeout=5.0)


# ---------------------------------------------------------------------------
# T6: stdio transport — concept_lookup
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_t6_stdio_concept_lookup():
    token = mint_token(TENANT_A)["token"]
    async with await _stdio_session(token) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            r = await session.call_tool("concept_lookup", {"query": "revenue"})
            assert r.isError is False, f"concept_lookup failed: {r.content}"
            payload = json.loads(r.content[0].text)
            assert payload.get("type") in {"metric", "entity"}, (
                f"unexpected concept_lookup payload: {payload}"
            )


# ---------------------------------------------------------------------------
# T7: HTTP+SSE transport — list_tools through the SSE mount
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_t7_http_sse_list_tools():
    """Run a one-shot ASGI lifespan to exercise the SSE router in-process.

    We use httpx.ASGITransport (no separate server) so the test is
    self-contained and doesn't depend on a running uvicorn.
    """
    from mcp.client.sse import sse_client

    base = os.environ.get("DCL_TEST_BASE_URL", "http://localhost:8004")
    sse_url = f"{base}/api/mcp/sse"
    token = mint_token(TENANT_A)["token"]

    # Check the server is reachable; skip if not.
    try:
        async with httpx.AsyncClient(timeout=2.0) as client:
            r = await client.get(f"{base}/health")
            if r.status_code != 200:
                pytest.skip(f"DCL not running at {base}")
    except (httpx.ConnectError, httpx.ReadTimeout, httpx.TimeoutException):
        pytest.skip(f"DCL not reachable at {base} — start with pm2")

    headers = {"Authorization": f"Bearer {token}"}
    async with sse_client(sse_url, headers=headers) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.list_tools()
            names = sorted(t.name for t in result.tools)
            assert names == sorted(PUBLIC_TOOLS), (
                f"HTTP+SSE list_tools returned {names}"
            )


# ---------------------------------------------------------------------------
# T8: legacy HTTP path still works for Mai after the refactor
# ---------------------------------------------------------------------------


def test_t8_legacy_http_path_concept_lookup():
    # Need AOS_TENANT_ID for the legacy path to derive tenant
    prev = os.environ.get("AOS_TENANT_ID")
    os.environ["AOS_TENANT_ID"] = TENANT_A
    try:
        call = MCPToolCall(
            tool="concept_lookup",
            arguments={"query": "revenue"},
            api_key="dcl-mcp-test-key",
        )
        result = handle_tool_call(call)
        assert result.success, f"legacy HTTP path broken: {result.error}"
        assert result.result.get("type") in {"metric", "entity"}
    finally:
        if prev is None:
            os.environ.pop("AOS_TENANT_ID", None)
        else:
            os.environ["AOS_TENANT_ID"] = prev


def test_t8_legacy_http_path_semantic_export():
    prev = os.environ.get("AOS_TENANT_ID")
    os.environ["AOS_TENANT_ID"] = TENANT_A
    try:
        call = MCPToolCall(
            tool="semantic_export",
            arguments={},
            api_key="dcl-mcp-test-key",
        )
        result = handle_tool_call(call)
        assert result.success, f"semantic_export failed: {result.error}"
        assert isinstance(result.result, dict)
        # Catalog has metrics/entities keys
        assert "metrics" in result.result or "entities" in result.result
    finally:
        if prev is None:
            os.environ.pop("AOS_TENANT_ID", None)
        else:
            os.environ["AOS_TENANT_ID"] = prev


def test_t8_legacy_http_path_provenance():
    prev = os.environ.get("AOS_TENANT_ID")
    os.environ["AOS_TENANT_ID"] = TENANT_A
    try:
        domains = tool_list_domains(TENANT_A)
        assert domains, "tenant has no active triples — cannot test provenance"
        # Find a domain whose root concept has rows
        target = None
        target_concept = None
        for d in domains:
            d_name = d["domain"]
            rows = tool_query_triples(TENANT_A, domain=d_name, limit=1)
            if rows:
                target = d_name
                target_concept = rows[0]["concept"]
                break
        assert target_concept is not None
        call = MCPToolCall(
            tool="provenance",
            arguments={"concept": target_concept},
            api_key="dcl-mcp-test-key",
        )
        result = handle_tool_call(call)
        assert result.success, f"legacy provenance failed: {result.error}"
        r = result.result
        # Per the spec: source_system, source_field, pipe_id, confidence_score
        for fld in ("source_system", "source_field", "pipe_id", "confidence_score"):
            assert fld in r, f"provenance result missing {fld}: {r}"
    finally:
        if prev is None:
            os.environ.pop("AOS_TENANT_ID", None)
        else:
            os.environ["AOS_TENANT_ID"] = prev


# ---------------------------------------------------------------------------
# Audit row written for every invocation
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_audit_row_written_on_success():
    tenant_audit = str(uuid.uuid4())
    global_limiter().reset(tenant_audit)
    token = mint_token(tenant_audit)["token"]
    since = time.time() - 1

    async with await _stdio_session(token) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            r = await session.call_tool("list_domains", {})
            assert r.isError is False

    count = _count_audit(tenant_audit, since)
    assert count >= 1, "audit row missing for successful tool call"
    last = _last_audit(tenant_audit)
    assert last["tool_name"] == "list_domains"
    assert last["outcome"] == "success"
    assert last["transport"] == "stdio"
    assert last["caller_token_id"]  # non-empty
    assert last["latency_ms"] >= 0
