"""Gate 1A acceptance: value-level conflict detection + Conflict Register +
HITL dispositions + precedent (ContextOS_Blueprint_v1 §8).

Operator-visible outcome under test: sap and salesforce disagree on
ConflictProbe-T1's revenue.total for 2025-Q1 (100.0 vs 110.0, Δrel 9.1% over
the 0.5% policy); DCL registers an open value conflict whose claims drill to
both triples' provenance; the operator dispositions accept_a (sap) with a
rationale; the live view then serves exactly 100.0 from sap, an as-of read
pinned before the disposition still shows both claims, and a re-ingest of the
same disagreement arrives with the precedent attached proposing accept_a.
A same-value two-source pair lands in the same register as 'structural'.

Live-service integration tests: TestClient drives the real FastAPI app
against the aos-dev database.
"""

import datetime
import sys
import time
import uuid
from pathlib import Path

import pytest

_repo = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_repo))

from dotenv import load_dotenv
load_dotenv(_repo / ".env.development")

from fastapi.testclient import TestClient
from backend.api.main import app
from backend.core.db import get_connection

client = TestClient(app, raise_server_exceptions=False)

TEST_TENANT_ID = str(uuid.uuid5(uuid.NAMESPACE_DNS, "conflict-register-gate1a-test"))
ENTITY = "ConflictProbe-T1"
PIPE_SAP = "55555555-5555-4555-8555-555555555551"
PIPE_SF = "55555555-5555-4555-8555-555555555552"


def _triple(source: str, pipe: str, value: float, *, concept="revenue.total",
            prop="amount", period="2025-Q1"):
    return {
        "entity_id": ENTITY, "concept": concept, "property": prop,
        "value": value, "period": period, "source_system": source,
        "source_table": "conflict_probe", "source_field": prop, "pipe_id": pipe,
        "confidence_score": 0.95, "confidence_tier": "exact",
        "fabric_plane": "ipaas",
    }


def _push(run_id: str, triples: list[dict]):
    resp = client.post(
        "/api/dcl/ingest-triples",
        json={"tenant_id": TEST_TENANT_ID, "dcl_ingest_id": run_id,
              "entity_id": ENTITY,
              "snapshot_name": f"{ENTITY}-{run_id.replace('-', '')[:4]}",
              "triples": triples},
    )
    assert resp.status_code == 201, f"ingest failed: {resp.status_code} {resp.text}"
    return resp.json()


def _disagreeing_batch():
    return [
        # The value conflict: 100.0 vs 110.0 — Δrel ~9.1% over the 0.5% default.
        _triple("sap", PIPE_SAP, 100.0),
        _triple("salesforce", PIPE_SF, 110.0),
        # The structural pair: same fact, same value, two sources.
        _triple("sap", PIPE_SAP, 250.0, concept="workforce.headcount.total",
                prop="count", period="2025-Q1"),
        _triple("salesforce", PIPE_SF, 250.0, concept="workforce.headcount.total",
                prop="count", period="2025-Q1"),
    ]


def _cleanup():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM conflict_dispositions WHERE tenant_id = %s", (TEST_TENANT_ID,))
            cur.execute("DELETE FROM conflict_register WHERE tenant_id = %s", (TEST_TENANT_ID,))
            cur.execute("DELETE FROM semantic_triples WHERE tenant_id = %s", (TEST_TENANT_ID,))
            cur.execute("DELETE FROM tenant_runs WHERE tenant_id = %s", (TEST_TENANT_ID,))
            conn.commit()


@pytest.fixture(scope="module", autouse=True)
def conflict_flow():
    """Run A with the disagreement; module state carries ids across tests."""
    _cleanup()
    run_a = str(uuid.uuid4())
    ingest = _push(run_a, _disagreeing_batch())
    yield {"run_a": run_a, "ingest": ingest}
    _cleanup()


def _register(status=None):
    params = {"entity_id": ENTITY}
    if status:
        params["status"] = status
    resp = client.get("/api/dcl/conflicts", params=params)
    assert resp.status_code == 200, resp.text
    return resp.json()


class TestDetection:
    def test_01_ingest_hook_detects_both_classes(self, conflict_flow):
        assert conflict_flow["ingest"]["conflicts_detected"] == 2
        body = _register()
        assert body["tenant_id"] == TEST_TENANT_ID
        assert body["total_count"] == 2
        by_type = {c["conflict_type"]: c for c in body["conflicts"]}
        assert set(by_type) == {"value", "structural"}, (
            f"one register, two classes — got {list(by_type)}"
        )
        assert by_type["value"]["concept"] == "revenue.total"
        assert by_type["structural"]["concept"] == "workforce.headcount.total"
        assert by_type["value"]["status"] == "open"

    def test_02_claims_carry_full_provenance_drill(self, conflict_flow):
        value_conflict = next(c for c in _register()["conflicts"]
                              if c["conflict_type"] == "value")
        claims = value_conflict["claims"]
        assert [c["source_system"] for c in claims] == ["salesforce", "sap"]
        for c in claims:
            for key in ("triple_id", "confidence_score", "confidence_tier",
                        "ingested_at", "source_table", "source_field", "pipe_id"):
                assert c.get(key) is not None, f"claim missing {key}: {c}"
        values = {c["source_system"]: float(c["value"]) for c in claims}
        assert values == {"sap": 100.0, "salesforce": 110.0}

    def test_03_materiality_against_policy(self, conflict_flow):
        value_conflict = next(c for c in _register()["conflicts"]
                              if c["conflict_type"] == "value")
        m = value_conflict["materiality"]
        assert m["material"] is True
        assert abs(m["abs_delta"] - 10.0) < 1e-9
        assert abs(m["rel_delta"] - (10.0 / 110.0)) < 1e-9
        assert m["rel_threshold"] == 0.005

    def test_04_explanation_grounded_in_concept_metadata(self, conflict_flow):
        value_conflict = next(c for c in _register()["conflicts"]
                              if c["conflict_type"] == "value")
        assert value_conflict["root_cause_source"] == "concept_metadata"
        expl = value_conflict["root_cause_explanation"]
        assert "Concept 'revenue.total'" in expl
        assert "sap=100.0" in expl and "salesforce=110.0" in expl

    def test_05_no_authority_no_precedent_recommends_escalate(self, conflict_flow):
        value_conflict = next(c for c in _register()["conflicts"]
                              if c["conflict_type"] == "value")
        rec = value_conflict["recommended"]
        assert rec["action"] == "escalate"
        assert rec["basis"] == "none"


class TestDisposition:
    def test_06_accept_a_supersedes_loser_keeps_history(self, conflict_flow):
        value_conflict = next(c for c in _register()["conflicts"]
                              if c["conflict_type"] == "value")
        conflict_flow["t_before_disposition"] = datetime.datetime.now(
            datetime.timezone.utc).isoformat()
        time.sleep(1.1)
        resp = client.post(
            f"/api/dcl/conflicts/{value_conflict['conflict_id']}/disposition",
            json={"action": "accept_b", "decided_by": "ilya",
                  "rationale": "sap is the ERP of record for recognized revenue",
                  "entity_id": ENTITY},
        )
        # claims are [salesforce, sap] (source-ordered) — sap is claim B.
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["status"] == "dispositioned"
        assert body["winner_source"] == "sap"
        assert body["loser_sources"] == ["salesforce"]
        assert body["superseded_count"] == 1
        conflict_flow["disposition"] = body

    def test_07_current_view_serves_winner_asof_serves_both(self, conflict_flow):
        # Row-level reads go through the MCP query path — the browse endpoint
        # collapses to one row per coordinates by design, which is exactly the
        # ambiguity the register exists to surface.
        from backend.db.triple_store import TripleStore
        store = TripleStore()
        live = store.mcp_query_triples(
            TEST_TENANT_ID, concept="revenue.total", entity_id=ENTITY,
            period="2025-Q1", active_only=True,
        )
        assert len(live) == 1, f"live view must serve exactly the winner, got {len(live)}"
        assert live[0]["source_system"] == "sap"
        assert float(live[0]["value"]) == 100.0

        asof = store.mcp_query_triples(
            TEST_TENANT_ID, concept="revenue.total", entity_id=ENTITY,
            period="2025-Q1", as_of=conflict_flow["t_before_disposition"],
        )
        assert {r["source_system"] for r in asof} == {"sap", "salesforce"}, (
            "as-of before the disposition must still show both claims (Gate 0)"
        )
        values = {r["source_system"]: float(r["value"]) for r in asof}
        assert values == {"sap": 100.0, "salesforce": 110.0}

    def test_08_disposition_is_append_only(self, conflict_flow):
        cid = conflict_flow["disposition"]["conflict_id"]
        resp = client.post(
            f"/api/dcl/conflicts/{cid}/disposition",
            json={"action": "accept_a", "decided_by": "ilya",
                  "rationale": "second thoughts", "entity_id": ENTITY},
        )
        assert resp.status_code == 409
        assert "append-only" in resp.json()["detail"]

    def test_09_decision_trace_recorded(self, conflict_flow):
        cid = conflict_flow["disposition"]["conflict_id"]
        detail = client.get(f"/api/dcl/conflicts/{cid}",
                            params={"entity_id": ENTITY}).json()
        assert detail["status"] == "dispositioned"
        traces = detail["dispositions"]
        assert len(traces) == 1
        t = traces[0]
        assert t["decided_by"] == "ilya"
        assert t["rationale"] == "sap is the ERP of record for recognized revenue"
        assert t["winner_source"] == "sap"
        assert len(t["superseded_triple_ids"]) == 1


class TestPrecedent:
    def test_10_reingest_attaches_precedent_proposing_winner(self, conflict_flow):
        run_b = str(uuid.uuid4())
        _push(run_b, _disagreeing_batch())
        body = _register(status="open")
        fresh = [c for c in body["conflicts"]
                 if c["conflict_type"] == "value" and c["dcl_ingest_id"] == run_b]
        assert len(fresh) == 1, (
            f"re-ingest must register a fresh occurrence of the class, got {len(fresh)}"
        )
        rec = fresh[0]["recommended"]
        assert rec["basis"] == "precedent"
        assert rec["winner_source"] == "sap"
        assert rec["action"] == "accept_b"  # sap is claim B in source order
        assert rec["precedent"]["decided_by"] == "ilya"
        assert rec["precedent"]["rationale"] == "sap is the ERP of record for recognized revenue"
        conflict_flow["run_b"] = run_b

    def test_11_mcp_tools_serve_register_and_recommendation(self, conflict_flow):
        from backend.engine.mcp_tools import dispatch
        q = dispatch(TEST_TENANT_ID, "conflict_query",
                     {"entity_id": ENTITY, "conflict_type": "value"})
        assert q["total_count"] == 2  # run A (dispositioned) + run B (open)
        open_value = next(c for c in q["conflicts"] if c["status"] == "open")
        r = dispatch(TEST_TENANT_ID, "reconciliation_recommend",
                     {"conflict_id": open_value["conflict_id"]})
        assert r["precedent"]["winner_source"] == "sap"
        assert r["recommended"]["basis"] == "precedent"

    def test_12_detect_endpoint_idempotent_and_sweepable(self, conflict_flow):
        resp = client.post("/api/dcl/conflicts/detect",
                           json={"entity_id": ENTITY})
        assert resp.status_code == 200
        body = resp.json()
        assert body["detected_new"] == 0, "re-detection must not duplicate register rows"
        assert body["refreshed"] == 2
        assert {c["conflict_type"] for c in body["conflicts"]} == {"value", "structural"}


class TestNegatives:
    def test_13_manual_requires_winner_among_claims(self, conflict_flow):
        open_value = next(c for c in _register(status="open")["conflicts"]
                          if c["conflict_type"] == "value")
        resp = client.post(
            f"/api/dcl/conflicts/{open_value['conflict_id']}/disposition",
            json={"action": "manual", "decided_by": "ilya",
                  "rationale": "x", "winner_source": "netsuite",
                  "entity_id": ENTITY},
        )
        assert resp.status_code == 422
        assert "not among the claims" in resp.json()["detail"]

    def test_14_rationale_required(self, conflict_flow):
        open_value = next(c for c in _register(status="open")["conflicts"]
                          if c["conflict_type"] == "value")
        resp = client.post(
            f"/api/dcl/conflicts/{open_value['conflict_id']}/disposition",
            json={"action": "accept_a", "decided_by": "ilya",
                  "rationale": "   ", "entity_id": ENTITY},
        )
        assert resp.status_code == 422
        assert "rationale" in resp.json()["detail"]

    def test_15_unknown_conflict_readable_404(self):
        ghost = str(uuid.uuid4())
        resp = client.get(f"/api/dcl/conflicts/{ghost}",
                          params={"tenant_id": TEST_TENANT_ID})
        assert resp.status_code == 404
        assert "not found" in resp.json()["detail"]

    def test_16_identity_required_422(self):
        resp = client.get("/api/dcl/conflicts")
        assert resp.status_code == 422
        assert "I2" in str(resp.json()["detail"])


class TestDeterminism:
    def test_17_register_reads_twice_identical(self, conflict_flow):
        a, b = _register(), _register()
        assert a == b, "register reads must be deterministic (B14)"
