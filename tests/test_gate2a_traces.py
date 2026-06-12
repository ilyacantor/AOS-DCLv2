"""Gate 2A acceptance (API grain): unified decision traces + standing rules
(ContextOS_Blueprint_v1 §9).

Operator-visible outcome under test: after this run's per-run-unique tenant
dispositions two same-class revenue.total conflicts (sap vs salesforce,
2025-Q1 and 2025-Q2) and an MCP agent calls conflict_query through the real
wire-protocol handler, GET /api/dcl/traces?tenant_id=... returns exactly
those decisions as uniform trace records — the disposition rows carry
agent=decided_by, decision_type=action, conflict_class, concept, period; the
mcp_call row carries agent=caller_token_id, decision_type=tool name, plus the
migration-020 enrichment (entity_id, full arguments, compact result_summary)
— filterable on every axis, as-of-able, and tenant-isolated. From the two
recurring dispositions the operator promotes a standing rule whose provenance
is exactly those two disposition trace_ids, approves it once, and a second
decision is refused with 409.

Live-service integration tests: TestClient drives the real FastAPI app
against the aos-dev database (migrations 020/021 applied there). All
fixture values (tenant, entity, run ids) are per-run-unique.
"""

import asyncio
import datetime
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

# In-proc MCP handler needs a mint/verify secret. Mint+verify happen in THIS
# process (self-consistent with any value), but other suite files (wp5 T7)
# authenticate against the LIVE server, whose env carries the shim secret from
# `.env` — resolve the same way they do so import order never poisons them.
# Pull ONLY that var; never load `.env` wholesale (its DATABASE_URL is prod).
if not os.environ.get("DCL_MCP_TOKEN_SECRET"):
    from dotenv import dotenv_values
    _live_secret = dotenv_values(_repo / ".env").get("DCL_MCP_TOKEN_SECRET")
    if _live_secret:
        os.environ["DCL_MCP_TOKEN_SECRET"] = _live_secret
os.environ.setdefault("DCL_MCP_TOKEN_SECRET", "gate2a-test-secret-do-not-use-in-prod")

from fastapi.testclient import TestClient
from backend.api.main import app
from backend.core.db import get_connection

client = TestClient(app, raise_server_exceptions=False)

# Per-run-unique identity — a fresh tenant every run keeps the durable dev
# store re-runnable (B14) without fixed-tenant cleanup races.
TENANT = str(uuid.uuid4())
TENANT_B = str(uuid.uuid4())  # isolation probe — never receives data
TAG = uuid.uuid4().hex[:6]
ENTITY = f"Gate2A-{TAG}"
PIPE_SAP = str(uuid.uuid4())
PIPE_SF = str(uuid.uuid4())


def _triple(source, pipe, value, *, period):
    return {
        "entity_id": ENTITY, "concept": "revenue.total", "property": "amount",
        "value": value, "period": period, "source_system": source,
        "source_table": "gate2a_probe", "source_field": "amount",
        "pipe_id": pipe, "confidence_score": 0.95, "confidence_tier": "exact",
        "fabric_plane": "ipaas",
    }


def _push(triples):
    run_id = str(uuid.uuid4())
    resp = client.post(
        "/api/dcl/ingest-triples",
        json={"tenant_id": TENANT, "dcl_ingest_id": run_id, "entity_id": ENTITY,
              "snapshot_name": f"{ENTITY}-{run_id.replace('-', '')[:4]}",
              "triples": triples},
    )
    assert resp.status_code == 201, f"ingest failed: {resp.status_code} {resp.text}"
    return run_id, resp.json()


def _run_async(coro):
    """Run a coroutine from sync test code, immune to pytest-playwright's
    leftover running-loop registration (same hazard the wp5 suite isolates)."""
    saved = None
    try:
        saved = asyncio.events._get_running_loop()
        asyncio.events._set_running_loop(None)
    except Exception:
        pass
    try:
        return asyncio.run(coro)
    finally:
        if saved is not None:
            try:
                asyncio.events._set_running_loop(saved)
            except Exception:
                pass


async def _mcp_call_inproc(token_str, tool, arguments):
    """Drive the REAL wire-protocol call handler in-process (wp5 T4 mechanics):
    token-bound session, dispatch, audit write — the same code path the stdio
    and SSE transports execute."""
    from mcp import types as _t
    from backend.api.mcp_auth import verify_token
    from backend.api.mcp_server_real import (
        bind_token_to_session, bind_transport, build_server,
        release_token, release_transport,
    )
    verified = verify_token(token_str)
    reset_token = bind_token_to_session(verified)
    reset_transport = bind_transport("test-inproc")
    try:
        server = build_server()
        handler = server.request_handlers[_t.CallToolRequest]
        req = _t.CallToolRequest(
            method="tools/call",
            params=_t.CallToolRequestParams(name=tool, arguments=arguments),
        )
        result = await handler(req)
        return result.root
    finally:
        release_token(reset_token)
        release_transport(reset_transport)


def _cleanup():
    with get_connection() as conn:
        with conn.cursor() as cur:
            for sql in (
                "DELETE FROM standing_rule_provenance WHERE tenant_id = %s",
                "DELETE FROM standing_rules WHERE tenant_id = %s",
                "DELETE FROM conflict_dispositions WHERE tenant_id = %s",
                "DELETE FROM conflict_register WHERE tenant_id = %s",
                "DELETE FROM semantic_triples WHERE tenant_id = %s",
                "DELETE FROM tenant_runs WHERE tenant_id = %s",
            ):
                cur.execute(sql, (TENANT,))
            conn.commit()


def _traces(**params):
    params.setdefault("tenant_id", TENANT)
    resp = client.get("/api/dcl/traces", params=params)
    assert resp.status_code == 200, resp.text
    return resp.json()


@pytest.fixture(scope="module", autouse=True)
def gate2a_flow():
    """One module-scoped story: two same-class conflicts (2025-Q1/Q2) ingested
    through the real pipeline, both dispositioned to sap with a captured
    mid-point timestamp between them, plus one real MCP conflict_query call.
    mai_mcp_audit is append-only by design and the tenant is per-run-unique,
    so audit rows are left in place (the wp5 posture); the register/rule/run
    surfaces are scrubbed on teardown."""
    from backend.api.mcp_auth import mint_token
    from backend.api.mcp_rate_limit import global_limiter

    state = {}
    _push([_triple("sap", PIPE_SAP, 100.0, period="2025-Q1"),
           _triple("salesforce", PIPE_SF, 110.0, period="2025-Q1"),
           _triple("sap", PIPE_SAP, 200.0, period="2025-Q2"),
           _triple("salesforce", PIPE_SF, 220.0, period="2025-Q2")])

    listed = client.get("/api/dcl/conflicts",
                        params={"tenant_id": TENANT, "status": "open"}).json()
    conflicts = {c["period"]: c for c in listed["conflicts"]
                 if c["conflict_type"] == "value"}
    assert set(conflicts) == {"2025-Q1", "2025-Q2"}, listed
    classes = {c["conflict_class"] for c in conflicts.values()}
    assert len(classes) == 1, f"same concept+property+sources must share a class: {classes}"
    state["conflict_class"] = classes.pop()
    state["conflicts"] = conflicts

    disp_ids = {}
    decided_at = {}
    for i, period in enumerate(("2025-Q1", "2025-Q2")):
        if i == 1:
            time.sleep(1.1)  # distinct decided_at instants with margin
        resp = client.post(
            f"/api/dcl/conflicts/{conflicts[period]['conflict_id']}/disposition",
            json={"action": "accept_b", "decided_by": f"gate2a-operator-{TAG}",
                  "rationale": "sap is the ERP of record", "tenant_id": TENANT},
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["winner_source"] == "sap", body
        disp_ids[period] = body["disposition_id"]
        decided_at[period] = datetime.datetime.fromisoformat(body["decided_at"])
    state["disposition_ids"] = disp_ids
    # as_of pinned strictly between the two decisions, computed from the
    # SERVER's own decided_at clock — immune to host↔DB clock skew.
    state["t_mid"] = (
        decided_at["2025-Q1"]
        + (decided_at["2025-Q2"] - decided_at["2025-Q1"]) / 2
    ).isoformat()

    # Real MCP call through the real handler — the mcp_call trace.
    minted = mint_token(TENANT, scope=("conflict_query", "trace_query"))
    state["token_id"] = minted["token_id"]
    state["mcp_args"] = {"entity_id": ENTITY, "conflict_class": state["conflict_class"]}
    global_limiter().reset(TENANT)
    r = _run_async(_mcp_call_inproc(minted["token"], "conflict_query", state["mcp_args"]))
    assert r.isError is False, f"conflict_query failed: {r.content}"

    yield state
    _cleanup()


class TestTraceSearch:
    def test_01_unified_list_carries_both_trace_types(self, gate2a_flow):
        body = _traces()
        assert body["tenant_id"] == TENANT
        by_type = {}
        for t in body["traces"]:
            by_type.setdefault(t["trace_type"], []).append(t)
        assert set(by_type) == {"conflict_disposition", "mcp_call"}, (
            f"expected exactly the trace types this test created, got {list(by_type)}"
        )
        assert {t["trace_id"] for t in by_type["conflict_disposition"]} == set(
            gate2a_flow["disposition_ids"].values()
        )
        assert len(by_type["mcp_call"]) == 1
        assert body["total_count"] == 3

    def test_02_disposition_axes(self, gate2a_flow):
        cls = gate2a_flow["conflict_class"]
        by_class = _traces(conflict_class=cls)
        assert by_class["total_count"] == 2
        assert {t["trace_id"] for t in by_class["traces"]} == set(
            gate2a_flow["disposition_ids"].values()
        )
        for t in by_class["traces"]:
            assert t["trace_type"] == "conflict_disposition"
            assert t["agent"] == f"gate2a-operator-{TAG}"
            assert t["decision_type"] == "accept_b"
            assert t["concept"] == "revenue.total"
            assert t["outcome"] == "sap"
            assert t["entity_id"] == ENTITY

        q1 = _traces(period="2025-Q1")
        assert q1["total_count"] == 1
        assert q1["traces"][0]["trace_id"] == gate2a_flow["disposition_ids"]["2025-Q1"]

        by_agent = _traces(agent=f"gate2a-operator-{TAG}")
        assert by_agent["total_count"] == 2

        by_action = _traces(decision_type="accept_b")
        assert by_action["total_count"] == 2

    def test_03_mcp_call_trace_carries_mig020_enrichment(self, gate2a_flow):
        body = _traces(trace_type="mcp_call")
        assert body["total_count"] == 1
        t = body["traces"][0]
        assert t["agent"] == gate2a_flow["token_id"]
        assert t["decision_type"] == "conflict_query"
        assert t["entity_id"] == ENTITY
        assert t["payload"] == gate2a_flow["mcp_args"]
        assert t["outcome"] == "success"
        assert t["result_summary"]["total_count"] == 2  # the two register rows
        assert t["result_summary"]["rows"] == 2

        by_tool = _traces(decision_type="conflict_query")
        assert by_tool["total_count"] == 1
        assert by_tool["traces"][0]["trace_id"] == t["trace_id"]

    def test_04_entity_axis_spans_trace_types(self, gate2a_flow):
        body = _traces(entity_id=ENTITY)
        assert body["total_count"] == 3
        assert {t["trace_type"] for t in body["traces"]} == {
            "conflict_disposition", "mcp_call"
        }

    def test_05_as_of_excludes_later_traces(self, gate2a_flow):
        cls = gate2a_flow["conflict_class"]
        before = _traces(conflict_class=cls, as_of=gate2a_flow["t_mid"])
        assert before["total_count"] == 1
        assert before["traces"][0]["trace_id"] == gate2a_flow["disposition_ids"]["2025-Q1"]
        now_all = _traces(conflict_class=cls)
        assert now_all["total_count"] == 2

    def test_06_get_single_trace(self, gate2a_flow):
        tid = gate2a_flow["disposition_ids"]["2025-Q1"]
        resp = client.get(f"/api/dcl/traces/{tid}", params={"tenant_id": TENANT})
        assert resp.status_code == 200, resp.text
        t = resp.json()
        assert t["trace_id"] == tid
        assert t["tenant_id"] == TENANT
        assert t["refs"]["conflict_id"] == gate2a_flow["conflicts"]["2025-Q1"]["conflict_id"]

    def test_07_tenant_isolation(self, gate2a_flow):
        b = _traces(tenant_id=TENANT_B)
        assert b["tenant_id"] == TENANT_B
        assert b["total_count"] == 0 and b["traces"] == []
        # Cross-tenant single-trace read refused loudly.
        tid = gate2a_flow["disposition_ids"]["2025-Q1"]
        resp = client.get(f"/api/dcl/traces/{tid}", params={"tenant_id": TENANT_B})
        assert resp.status_code == 404
        assert "not found" in resp.json()["detail"]


class TestStandingRules:
    def test_08_propose_carries_exact_disposition_provenance(self, gate2a_flow):
        resp = client.post("/api/dcl/rules/propose",
                           json={"tenant_id": TENANT,
                                 "conflict_class": gate2a_flow["conflict_class"],
                                 "proposed_by": f"gate2a-agent-{TAG}"})
        assert resp.status_code == 200, resp.text
        rule = resp.json()
        assert rule["tenant_id"] == TENANT
        assert rule["status"] == "proposed"
        assert rule["rule_scope"] == "conflict_class"
        assert rule["rule_body"] == {"action": "accept_b", "winner_source": "sap"}
        assert sorted(rule["provenance_trace_ids"]) == sorted(
            gate2a_flow["disposition_ids"].values()
        )
        assert all(p["trace_type"] == "conflict_disposition" for p in rule["provenance"])
        assert "2 recurring" in rule["proposal_rationale"]
        assert gate2a_flow["conflict_class"] in rule["proposal_rationale"]
        gate2a_flow["rule_id"] = rule["rule_id"]

    def test_09_provenance_trace_ids_resolve_through_the_view(self, gate2a_flow):
        for tid in (gate2a_flow["disposition_ids"].values()):
            resp = client.get(f"/api/dcl/traces/{tid}", params={"tenant_id": TENANT})
            assert resp.status_code == 200
            assert resp.json()["trace_type"] == "conflict_disposition"

    def test_10_decide_approved_exactly_once(self, gate2a_flow):
        rid = gate2a_flow["rule_id"]
        resp = client.post(
            f"/api/dcl/rules/{rid}/decide",
            json={"tenant_id": TENANT, "decision": "approved",
                  "decided_by": "ilya", "decision_rationale": "matches ERP-of-record policy"},
        )
        assert resp.status_code == 200, resp.text
        rule = resp.json()
        assert rule["status"] == "approved"
        assert rule["decided_by"] == "ilya"
        assert rule["decision_rationale"] == "matches ERP-of-record policy"
        assert sorted(rule["provenance_trace_ids"]) == sorted(
            gate2a_flow["disposition_ids"].values()
        )

        again = client.post(
            f"/api/dcl/rules/{rid}/decide",
            json={"tenant_id": TENANT, "decision": "rejected",
                  "decided_by": "ilya", "decision_rationale": "second thoughts"},
        )
        assert again.status_code == 409
        assert "already approved" in again.json()["detail"]
        assert "exactly once" in again.json()["detail"]

    def test_11_approved_rule_listed_with_provenance(self, gate2a_flow):
        resp = client.get("/api/dcl/rules",
                          params={"tenant_id": TENANT, "status": "approved"})
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["tenant_id"] == TENANT
        assert body["total_count"] == 1
        rule = body["rules"][0]
        assert rule["rule_id"] == gate2a_flow["rule_id"]
        assert sorted(rule["provenance_trace_ids"]) == sorted(
            gate2a_flow["disposition_ids"].values()
        )

    def test_12_propose_with_zero_precedent_is_422(self, gate2a_flow):
        ghost_class = f"entity|ghost.concept.{TAG}|amount|a+b"
        resp = client.post("/api/dcl/rules/propose",
                           json={"tenant_id": TENANT, "conflict_class": ghost_class,
                                 "proposed_by": "gate2a-agent"})
        assert resp.status_code == 422, resp.text
        detail = resp.json()["detail"]
        assert TENANT in detail
        assert ghost_class in detail
        assert "min_recurrence 2" in detail


class TestNegativesAndIdentity:
    def test_13_missing_tenant_id_is_readable_422(self):
        for path in ("/api/dcl/traces", "/api/dcl/rules"):
            resp = client.get(path)
            assert resp.status_code == 422, f"{path}: {resp.status_code}"
            assert "requires tenant_id" in resp.json()["detail"]
        resp = client.post("/api/dcl/rules/propose",
                           json={"conflict_class": "x|y|z|a+b", "proposed_by": "p"})
        assert resp.status_code == 422
        assert "requires tenant_id" in resp.json()["detail"]

    def test_14_invalid_axis_values_readable_422(self):
        resp = client.get("/api/dcl/traces",
                          params={"tenant_id": TENANT, "trace_type": "bogus"})
        assert resp.status_code == 422
        assert "trace_type must be one of" in resp.json()["detail"]
        resp = client.get("/api/dcl/traces",
                          params={"tenant_id": TENANT, "as_of": "not-a-time"})
        assert resp.status_code == 422
        assert "ISO-8601" in resp.json()["detail"]

    def test_15_no_run_id_key_anywhere(self, gate2a_flow):
        """I1: the literal string 'run_id' appears in no response key,
        recursively, on any new endpoint."""
        def keys_of(obj):
            if isinstance(obj, dict):
                for k, v in obj.items():
                    yield str(k)
                    yield from keys_of(v)
            elif isinstance(obj, list):
                for item in obj:
                    yield from keys_of(item)

        tid = gate2a_flow["disposition_ids"]["2025-Q1"]
        payloads = [
            _traces(),
            client.get(f"/api/dcl/traces/{tid}", params={"tenant_id": TENANT}).json(),
            client.get("/api/dcl/rules", params={"tenant_id": TENANT}).json(),
        ]
        for payload in payloads:
            offenders = [k for k in keys_of(payload) if "run_id" in k]
            assert offenders == [], f"I1 violation — run_id-bearing keys: {offenders}"

    def test_16_every_new_endpoint_response_carries_tenant_id(self, gate2a_flow):
        tid = gate2a_flow["disposition_ids"]["2025-Q1"]
        responses = {
            "traces_search": _traces(),
            "traces_get": client.get(f"/api/dcl/traces/{tid}",
                                     params={"tenant_id": TENANT}).json(),
            "rules_list": client.get("/api/dcl/rules",
                                     params={"tenant_id": TENANT}).json(),
        }
        for name, body in responses.items():
            assert body.get("tenant_id") == TENANT, f"{name} dropped tenant_id: {body.keys()}"

    def test_17_audit_read_carries_additive_mig020_fields(self, gate2a_flow):
        """GET /api/dcl/mcp/audit keeps its consumed-as-is fields AND now
        carries entity_id / arguments / result_summary additively."""
        resp = client.get("/api/dcl/mcp/audit", params={"tenant_id": TENANT})
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["tenant_id"] == TENANT
        assert body["total_count"] == 1
        entry = body["entries"][0]
        for legacy in ("tool_name", "caller_token_id", "arguments_hash",
                       "latency_ms", "outcome", "error_summary", "transport",
                       "created_at"):
            assert legacy in entry, f"legacy audit field {legacy} missing"
        assert entry["tool_name"] == "conflict_query"
        assert entry["caller_token_id"] == gate2a_flow["token_id"]
        assert entry["entity_id"] == ENTITY
        assert entry["arguments"] == gate2a_flow["mcp_args"]
        assert entry["result_summary"]["rows"] == 2
        assert entry["result_summary"]["total_count"] == 2

    def test_18_search_reads_twice_identical(self, gate2a_flow):
        a, b = _traces(), _traces()
        assert a == b, "trace reads must be deterministic (B14)"
