"""Gate 2A acceptance (dev stack, :8104): the full §9 decision-trace story.

Operator-visible outcome under test: an agent holding a tenant-scoped MCP
token calls conflict_query and trace_query over the real stdio transport; an
operator ingests this run's two-source revenue disagreement (sap 100 vs
salesforce 110 in 2025-Q1, 200 vs 220 in 2025-Q2) through the real pipeline
and dispositions both to sap; a pending-band identity pair (Northwind<tag>
Traders vs Northwind<tag> Trading Co) lands in the resolver HITL queue via
/api/dcl/ingest-records and is approved. GET /api/dcl/traces?tenant_id=...
on the live dev backend then shows ALL THREE decision kinds as uniform trace
records — the mcp_call rows (agent = the minted token id, decision_type =
the tool name, payload = the exact arguments sent), the conflict_disposition
rows (agent = decided_by, conflict_class/concept/period populated), and the
er_confirmation rows (decision_type = decided_approved, agent = decided_by)
— each axis filter independently returns the right one; an as_of pinned
between the two dispositions shows only the first; the two dispositions
promote to a standing rule whose provenance is exactly their trace_ids,
approval flips it once, and a second decision is refused with a readable 409.

Acceptance grain: live dcl-dev backend (:8104, aos-dev DB where migrations
020/021 are applied) over HTTP; MCP over the real stdio transport (wp5
mechanics — the subprocess inherits this process's .env.development
DATABASE_URL and never loads .env). All fixture values are per-run-unique.
"""

import asyncio
import datetime
import os
import sys
import time
import uuid
from pathlib import Path

import httpx
import pytest

_repo = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_repo))

from dotenv import load_dotenv
load_dotenv(_repo / ".env.development")

# stdio mint (parent) + verify (child) share this env value — self-consistent
# with any value. Resolve it the same way the wp5 suite does (prefer the env,
# else the live server's shim secret — the ONE var from `.env`, never the
# whole file: its DATABASE_URL is prod) so import order in a combined session
# never poisons files that authenticate against the live server.
if not os.environ.get("DCL_MCP_TOKEN_SECRET"):
    from dotenv import dotenv_values
    _live_secret = dotenv_values(_repo / ".env").get("DCL_MCP_TOKEN_SECRET")
    if _live_secret:
        os.environ["DCL_MCP_TOKEN_SECRET"] = _live_secret
os.environ.setdefault("DCL_MCP_TOKEN_SECRET", "gate2a-e2e-secret-do-not-use-in-prod")

DCL_BACKEND = os.environ.get("DCL_BACKEND_URL", "http://localhost:8104")

TENANT = str(uuid.uuid4())
TAG = uuid.uuid4().hex[:6]
ENTITY = f"Gate2AE2E-{TAG}"
PIPE_SAP = str(uuid.uuid4())
PIPE_SF = str(uuid.uuid4())
PIPE_NS = str(uuid.uuid4())
PIPE_SAGE = str(uuid.uuid4())
OPERATOR = f"gate2a-e2e-operator-{TAG}"


def _triple(source, pipe, value, *, period):
    return {
        "entity_id": ENTITY, "concept": "revenue.total", "property": "amount",
        "value": value, "period": period, "source_system": source,
        "source_table": "gate2a_e2e_probe", "source_field": "amount",
        "pipe_id": pipe, "confidence_score": 0.95, "confidence_tier": "exact",
        "fabric_plane": "ipaas",
    }


def _customer_pipe(pipe_id, source_system, records):
    return {
        "pipe_id": pipe_id, "source_system": source_system,
        "fabric_plane": "ipaas", "fabric_product": source_system,
        "domain": "customer", "identity_key": "company_name",
        "record_key_field": "customer_id", "records": records,
    }


def _run_async(coro):
    """asyncio.run, immune to pytest-playwright's leftover running-loop
    registration when the e2e directory runs in one session."""
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


async def _mcp_stdio_calls(token_str, calls):
    """One real stdio MCP session (wp5 transport mechanics), N tool calls.
    Returns the decoded JSON payload per call."""
    import json as _json
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client

    env = {**os.environ, "DCL_MCP_TOKEN": token_str, "PYTHONPATH": str(_repo)}
    params = StdioServerParameters(
        command=sys.executable, args=["-m", "backend.api.mcp_stdio"], env=env
    )
    out = []
    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            for name, args in calls:
                r = await session.call_tool(name, args)
                assert r.isError is False, f"{name} failed: {r.content}"
                out.append(_json.loads(r.content[0].text))
    return out


def _cleanup():
    import psycopg2
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    try:
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM resolver_hitl_audit WHERE hitl_queue_id IN "
            "(SELECT hitl_queue_id FROM resolver_hitl_queue WHERE tenant_id = %s)",
            (TENANT,),
        )
        for sql in (
            "DELETE FROM standing_rule_provenance WHERE tenant_id = %s",
            "DELETE FROM standing_rules WHERE tenant_id = %s",
            "DELETE FROM conflict_dispositions WHERE tenant_id = %s",
            "DELETE FROM conflict_register WHERE tenant_id = %s",
            "DELETE FROM resolver_hitl_queue WHERE tenant_id = %s",
            "DELETE FROM canonical_registry WHERE tenant_id = %s",
            "DELETE FROM semantic_triples WHERE tenant_id = %s",
            "DELETE FROM tenant_runs WHERE tenant_id = %s",
        ):
            cur.execute(sql, (TENANT,))
        conn.commit()
    finally:
        conn.close()


def _get(client, path, **params):
    r = client.get(f"{DCL_BACKEND}{path}", params=params)
    assert r.status_code == 200, f"GET {path}: {r.status_code} {r.text}"
    return r.json()


@pytest.fixture(scope="module", autouse=True)
def story():
    """The full Gate 2A story executed once, in order, through real paths.
    Tests below assert each leg. Teardown scrubs every per-run surface this
    tenant touched except mai_mcp_audit (append-only ledger, house posture)."""
    from backend.api.mcp_auth import mint_token

    state = {}
    with httpx.Client(timeout=120.0) as client:
        health = client.get(f"{DCL_BACKEND}/api/health")
        assert health.status_code == 200, f"DCL dev backend not healthy at {DCL_BACKEND}"

        # (b) per-run-unique conflicts through the real ingest path.
        run_id = str(uuid.uuid4())
        resp = client.post(
            f"{DCL_BACKEND}/api/dcl/ingest-triples",
            json={"tenant_id": TENANT, "dcl_ingest_id": run_id, "entity_id": ENTITY,
                  "snapshot_name": f"{ENTITY}-{run_id.replace('-', '')[:4]}",
                  "triples": [
                      _triple("sap", PIPE_SAP, 100.0, period="2025-Q1"),
                      _triple("salesforce", PIPE_SF, 110.0, period="2025-Q1"),
                      _triple("sap", PIPE_SAP, 200.0, period="2025-Q2"),
                      _triple("salesforce", PIPE_SF, 220.0, period="2025-Q2"),
                  ]},
        )
        assert resp.status_code == 201, f"seed ingest failed: {resp.status_code} {resp.text}"
        assert resp.json()["conflicts_detected"] == 2, resp.json()

        listed = _get(client, "/api/dcl/conflicts", tenant_id=TENANT, status="open")
        conflicts = {c["period"]: c for c in listed["conflicts"]
                     if c["conflict_type"] == "value"}
        assert set(conflicts) == {"2025-Q1", "2025-Q2"}, listed
        state["conflict_class"] = conflicts["2025-Q1"]["conflict_class"]
        assert conflicts["2025-Q2"]["conflict_class"] == state["conflict_class"]

        disp_ids = {}
        decided_at = {}
        for i, period in enumerate(("2025-Q1", "2025-Q2")):
            if i == 1:
                time.sleep(1.1)  # distinct decided_at instants with margin
            r = client.post(
                f"{DCL_BACKEND}/api/dcl/conflicts/{conflicts[period]['conflict_id']}/disposition",
                json={"action": "accept_b", "decided_by": OPERATOR,
                      "rationale": "sap is the ERP of record", "tenant_id": TENANT},
            )
            assert r.status_code == 200, r.text
            assert r.json()["winner_source"] == "sap", r.json()
            disp_ids[period] = r.json()["disposition_id"]
            decided_at[period] = datetime.datetime.fromisoformat(r.json()["decided_at"])
        state["disposition_ids"] = disp_ids
        # as_of pinned strictly between the two decisions, computed from the
        # SERVER's own decided_at clock — immune to host<->DB clock skew.
        state["t_mid"] = (
            decided_at["2025-Q1"]
            + (decided_at["2025-Q2"] - decided_at["2025-Q1"]) / 2
        ).isoformat()

        # (d) ER leg: pending-band identity pair through the real records path,
        # then operator approval — the er_confirmation traces.
        seed_val = f"Northwind{TAG} Traders"
        probe_val = f"Northwind{TAG} Trading Co"
        er_run = str(uuid.uuid4())
        r = client.post(
            f"{DCL_BACKEND}/api/dcl/ingest-records",
            json={"tenant_id": TENANT, "dcl_ingest_id": er_run,
                  "entity_id": ENTITY, "run_mode": "Dev",
                  "pipes": [
                      _customer_pipe(PIPE_NS, "NetSuite",
                                     [{"customer_id": "S1", "company_name": seed_val}]),
                      _customer_pipe(PIPE_SAGE, "Sage Intacct",
                                     [{"customer_id": "P1", "company_name": probe_val}]),
                  ]},
        )
        assert r.status_code == 201, f"records ingest failed: {r.status_code} {r.text}"
        assert r.json()["resolution_summary"].get("hitl_pending") == 1, r.json()

        pending = _get(client, "/api/dcl/resolver/hitl",
                       tenant_id=TENANT, status="pending")
        mine = [it for it in pending["items"]
                if TAG in (it.get("left_value") or "") or TAG in (it.get("right_value") or "")]
        assert len(mine) == 1, pending
        state["hitl_queue_id"] = mine[0]["hitl_queue_id"]
        r = client.post(
            f"{DCL_BACKEND}/api/dcl/resolver/hitl/{state['hitl_queue_id']}/decide",
            json={"decision": "approved", "decided_by": OPERATOR},
        )
        assert r.status_code == 200, r.text

        # (a) real MCP client calls over the stdio transport, dev backing:
        # conflict_query (register read) + trace_query (the new Gate 2A tool).
        minted = mint_token(TENANT, scope=("conflict_query", "trace_query"))
        state["token_id"] = minted["token_id"]
        state["mcp_args"] = {"entity_id": ENTITY,
                             "conflict_class": state["conflict_class"]}
        payloads = _run_async(_mcp_stdio_calls(minted["token"], [
            ("conflict_query", state["mcp_args"]),
            ("trace_query", {"trace_type": "conflict_disposition",
                             "conflict_class": state["conflict_class"]}),
        ]))
        state["mcp_conflict_payload"], state["mcp_trace_payload"] = payloads

    yield state
    _cleanup()


@pytest.fixture()
def client():
    with httpx.Client(timeout=60.0) as c:
        yield c


class TestUnifiedTraceList:
    def test_01_all_three_trace_types_in_one_list(self, story, client):
        body = _get(client, "/api/dcl/traces", tenant_id=TENANT)
        assert body["tenant_id"] == TENANT
        by_type = {}
        for t in body["traces"]:
            by_type.setdefault(t["trace_type"], []).append(t)
        assert set(by_type) == {"mcp_call", "conflict_disposition", "er_confirmation"}, (
            f"expected all three §9 trace types from this run's story, got {list(by_type)}"
        )
        # 2 dispositions + 2 mcp calls + 2 er events (created + decided_approved)
        assert len(by_type["conflict_disposition"]) == 2
        assert len(by_type["mcp_call"]) == 2
        assert len(by_type["er_confirmation"]) == 2
        assert body["total_count"] == 6

    def test_02_mcp_call_axis_agent_is_token_id_decision_type_is_tool(self, story, client):
        body = _get(client, "/api/dcl/traces", tenant_id=TENANT,
                    trace_type="mcp_call")
        assert body["total_count"] == 2
        by_tool = {t["decision_type"]: t for t in body["traces"]}
        assert set(by_tool) == {"conflict_query", "trace_query"}
        for t in body["traces"]:
            assert t["agent"] == story["token_id"]
            assert t["outcome"] == "success"
        cq = by_tool["conflict_query"]
        assert cq["entity_id"] == ENTITY            # mig020 enrichment
        assert cq["payload"] == story["mcp_args"]   # full arguments captured
        assert cq["result_summary"]["total_count"] == 2

        by_agent = _get(client, "/api/dcl/traces", tenant_id=TENANT,
                        agent=story["token_id"])
        assert by_agent["total_count"] == 2
        assert {t["trace_type"] for t in by_agent["traces"]} == {"mcp_call"}

    def test_03_disposition_axis_agent_is_decided_by_with_class_concept_period(self, story, client):
        body = _get(client, "/api/dcl/traces", tenant_id=TENANT,
                    trace_type="conflict_disposition")
        assert body["total_count"] == 2
        assert {t["trace_id"] for t in body["traces"]} == set(
            story["disposition_ids"].values())
        for t in body["traces"]:
            assert t["agent"] == OPERATOR
            assert t["conflict_class"] == story["conflict_class"]
            assert t["concept"] == "revenue.total"
            assert t["period"] in ("2025-Q1", "2025-Q2")
            assert t["outcome"] == "sap"

        q2 = _get(client, "/api/dcl/traces", tenant_id=TENANT, period="2025-Q2")
        assert q2["total_count"] == 1
        assert q2["traces"][0]["trace_id"] == story["disposition_ids"]["2025-Q2"]

        by_class = _get(client, "/api/dcl/traces", tenant_id=TENANT,
                        conflict_class=story["conflict_class"])
        assert by_class["total_count"] == 2

    def test_04_er_axis_decided_approved_by_operator(self, story, client):
        body = _get(client, "/api/dcl/traces", tenant_id=TENANT,
                    trace_type="er_confirmation")
        assert body["total_count"] == 2
        events = {t["decision_type"]: t for t in body["traces"]}
        assert set(events) == {"created", "decided_approved"}
        decided = events["decided_approved"]
        assert decided["agent"] == OPERATOR
        assert decided["concept"] == "customer"      # resolver domain
        assert decided["outcome"] == "approved"
        assert decided["refs"]["hitl_queue_id"] == story["hitl_queue_id"]
        assert f"Northwind{TAG}" in str(decided["payload"])

        by_event = _get(client, "/api/dcl/traces", tenant_id=TENANT,
                        decision_type="decided_approved")
        assert by_event["total_count"] == 1
        assert by_event["traces"][0]["trace_id"] == decided["trace_id"]

    def test_05_trace_query_mcp_tool_serves_the_same_dispositions(self, story):
        payload = story["mcp_trace_payload"]
        assert payload["tenant_id"] == TENANT
        assert payload["total_count"] == 2
        assert {t["trace_id"] for t in payload["traces"]} == set(
            story["disposition_ids"].values())

    def test_06_as_of_pinned_between_dispositions_shows_only_the_first(self, story, client):
        before = _get(client, "/api/dcl/traces", tenant_id=TENANT,
                      trace_type="conflict_disposition", as_of=story["t_mid"])
        assert before["total_count"] == 1
        assert before["traces"][0]["trace_id"] == story["disposition_ids"]["2025-Q1"]
        now_all = _get(client, "/api/dcl/traces", tenant_id=TENANT,
                       trace_type="conflict_disposition")
        assert now_all["total_count"] == 2


class TestPromotionStory:
    def test_07_propose_then_approve_with_exact_provenance(self, story, client):
        r = client.post(f"{DCL_BACKEND}/api/dcl/rules/propose",
                        json={"tenant_id": TENANT,
                              "conflict_class": story["conflict_class"],
                              "proposed_by": f"gate2a-e2e-agent-{TAG}"})
        assert r.status_code == 200, r.text
        rule = r.json()
        assert rule["status"] == "proposed"
        assert rule["rule_body"] == {"action": "accept_b", "winner_source": "sap"}
        assert sorted(rule["provenance_trace_ids"]) == sorted(
            story["disposition_ids"].values())
        assert "2 recurring" in rule["proposal_rationale"]
        story["rule_id"] = rule["rule_id"]

        r = client.post(
            f"{DCL_BACKEND}/api/dcl/rules/{rule['rule_id']}/decide",
            json={"tenant_id": TENANT, "decision": "approved",
                  "decided_by": "ilya",
                  "decision_rationale": "ERP-of-record standing policy"},
        )
        assert r.status_code == 200, r.text
        assert r.json()["status"] == "approved"

        listed = _get(client, "/api/dcl/rules", tenant_id=TENANT, status="approved")
        assert listed["total_count"] == 1
        got = listed["rules"][0]
        assert got["rule_id"] == rule["rule_id"]
        assert got["decided_by"] == "ilya"
        assert sorted(got["provenance_trace_ids"]) == sorted(
            story["disposition_ids"].values())

    def test_08_redecide_refused_with_readable_409(self, story, client):
        r = client.post(
            f"{DCL_BACKEND}/api/dcl/rules/{story['rule_id']}/decide",
            json={"tenant_id": TENANT, "decision": "rejected",
                  "decided_by": "ilya", "decision_rationale": "changed my mind"},
        )
        assert r.status_code == 409, r.text
        detail = r.json()["detail"]
        assert "already approved" in detail
        assert "exactly once" in detail

    def test_09_propose_without_precedent_readable_422(self, story, client):
        ghost = f"entity|ghost.{TAG}|amount|x+y"
        r = client.post(f"{DCL_BACKEND}/api/dcl/rules/propose",
                        json={"tenant_id": TENANT, "conflict_class": ghost,
                              "proposed_by": "gate2a-e2e-agent"})
        assert r.status_code == 422, r.text
        detail = r.json()["detail"]
        assert TENANT in detail and ghost in detail and "min_recurrence 2" in detail


class TestNegativeSurfaces:
    def test_10_missing_tenant_readable_422(self, client):
        r = client.get(f"{DCL_BACKEND}/api/dcl/traces")
        assert r.status_code == 422
        assert "requires tenant_id" in r.json()["detail"]
        assert "tenant-scoped" in r.json()["detail"]

    def test_11_cross_tenant_trace_get_readable_404(self, story, client):
        ghost_tenant = str(uuid.uuid4())
        tid = story["disposition_ids"]["2025-Q1"]
        r = client.get(f"{DCL_BACKEND}/api/dcl/traces/{tid}",
                       params={"tenant_id": ghost_tenant})
        assert r.status_code == 404
        assert f"Trace {tid} not found for tenant {ghost_tenant}" in r.json()["detail"]
