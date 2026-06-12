"""Gate 3A acceptance: Align proposal queue + canonical apply-on-approve
(ContextOS_Blueprint_v1 §4, migration 023).

Operator-visible outcome: an operator can POST a batch of Align-sourced proposals
and see them in GET /api/dcl/align/proposals each carrying confidence + provenance;
approve an authority_map proposal and immediately see the canonical row in
GET /api/dcl/conflicts/authority-map carrying align provenance; reject another
proposal and find zero canonical residue in every canonical store; duplicate proposals
are reported explicitly with 'duplicate_of: <proposal_id>' (never silently dropped);
a decision trace appears in GET /api/dcl/traces as trace_type='align_decision'.

Live-service integration tests: TestClient drives the real FastAPI app against the
aos-dev database (migration 023 applied there). All tenant IDs are per-run-unique
so the durable dev store is re-runnable without cleanup races (B14).
"""

import sys
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

TENANT = str(uuid.uuid4())
TAG = uuid.uuid4().hex[:6]
ENTITY = f"AlignGate3A-{TAG}"
TENANT_B = str(uuid.uuid4())  # isolation probe — never receives data


def _cleanup():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM alignment_decisions WHERE tenant_id = %s::uuid", (TENANT,)
            )
            cur.execute(
                "DELETE FROM alignment_proposals WHERE tenant_id = %s::uuid", (TENANT,)
            )
            cur.execute(
                "DELETE FROM tenant_concept_aliases WHERE tenant_id = %s::uuid", (TENANT,)
            )
            cur.execute(
                "DELETE FROM tenant_contour WHERE tenant_id = %s::uuid", (TENANT,)
            )
            cur.execute(
                "DELETE FROM tenant_authority_map WHERE tenant_id = %s", (TENANT,)
            )
            cur.execute(
                "DELETE FROM conflict_register "
                "WHERE tenant_id = %s::uuid AND source_class IN "
                "('stakeholder_system', 'stakeholder_stakeholder')",
                (TENANT,),
            )
        conn.commit()


@pytest.fixture(autouse=True)
def _clean():
    _cleanup()
    yield
    _cleanup()


# =============================================================================
# Helper payloads
# =============================================================================

def _authority_map_proposal(concept_prefix="cloud_spend", sources=None):
    return {
        "proposal_type": "authority_map",
        "payload": {
            "concept_prefix": concept_prefix,
            "ranked_sources": sources or ["aws_cost_explorer", "snowflake"],
        },
        "confidence": 0.92,
        "provenance": {
            "basis": "confirmed",
            "confirmed_by": "CFO",
            "align_session_id": f"sess-{TAG}",
        },
        "entity_id": ENTITY,
    }


def _conflict_candidate_proposal():
    return {
        "proposal_type": "conflict_candidate",
        "payload": {
            "concept": "revenue.total",
            "property": "amount",
            "period": "2025-Q1",
            "conflict_type": "value",
            "conflict_class": "stakeholder_reported",
            "source_class": "stakeholder_system",
            "entity_id": ENTITY,
            "claims": [
                {"source_system": "CFO", "value": 120.0, "basis": "confirmed"},
                {"source_system": "sap", "value": 115.0, "basis": "inferred"},
            ],
        },
        "confidence": 0.88,
        "provenance": {
            "basis": "confirmed",
            "confirmed_by": "CFO",
            "align_session_id": f"sess-{TAG}",
        },
        "entity_id": ENTITY,
    }


def _vocabulary_alias_proposal(alias="rev", concept_id="revenue"):
    return {
        "proposal_type": "vocabulary_alias",
        "payload": {"alias": alias, "concept_id": concept_id},
        "confidence": 0.95,
        "provenance": {
            "basis": "inferred",
            "confirmed_by": None,
            "align_session_id": f"sess-{TAG}",
        },
    }


def _org_hierarchy_proposal():
    return {
        "proposal_type": "org_hierarchy",
        "payload": {
            "dimension": "division",
            "roots": [
                {"name": "Cloud", "children": [{"name": "Cloud East"}, {"name": "Cloud West"}]},
                {"name": "Services"},
            ],
        },
        "confidence": 0.90,
        "provenance": {
            "basis": "confirmed",
            "confirmed_by": "COO",
            "align_session_id": f"sess-{TAG}",
        },
    }


def _intake(proposals):
    return client.post(
        "/api/dcl/align/proposals",
        json={"tenant_id": TENANT, "proposals": proposals},
    )


def _decide(proposal_id, decision, decided_by="operator-1"):
    return client.post(
        f"/api/dcl/align/proposals/{proposal_id}/decide",
        json={"tenant_id": TENANT, "decision": decision,
              "decided_by": decided_by, "note": f"{decision}-test-{TAG}"},
    )


# =============================================================================
# 422 negative cases
# =============================================================================

class TestNegative:

    def test_missing_tenant_id(self):
        r = client.post(
            "/api/dcl/align/proposals",
            json={"proposals": [_authority_map_proposal()]},
        )
        assert r.status_code == 422, r.text
        assert "tenant_id" in r.text.lower()

    def test_empty_batch(self):
        r = client.post(
            "/api/dcl/align/proposals",
            json={"tenant_id": TENANT, "proposals": []},
        )
        assert r.status_code == 422, r.text
        assert "empty" in r.text.lower()

    def test_unknown_proposal_type(self):
        r = _intake([{
            "proposal_type": "magic_bean",
            "payload": {"concept_prefix": "x"},
            "confidence": 0.5,
            "provenance": {"basis": "confirmed"},
        }])
        assert r.status_code == 422, r.text
        assert "magic_bean" in r.text or "proposal_type" in r.text.lower()

    def test_missing_confidence(self):
        p = _authority_map_proposal()
        del p["confidence"]
        r = _intake([p])
        assert r.status_code == 422, r.text
        assert "confidence" in r.text.lower()

    def test_missing_provenance_basis(self):
        p = _authority_map_proposal()
        p["provenance"] = {"confirmed_by": "CFO"}  # no basis
        r = _intake([p])
        assert r.status_code == 422, r.text
        assert "basis" in r.text.lower()

    def test_invalid_provenance_basis(self):
        p = _authority_map_proposal()
        p["provenance"] = {"basis": "guessed"}
        r = _intake([p])
        assert r.status_code == 422, r.text
        assert "basis" in r.text.lower()

    def test_decide_missing_tenant(self):
        dummy_id = str(uuid.uuid4())
        r = client.post(
            f"/api/dcl/align/proposals/{dummy_id}/decide",
            json={"decision": "approve", "decided_by": "x"},
        )
        assert r.status_code == 422, r.text

    def test_decide_invalid_decision(self):
        r = _intake([_authority_map_proposal()])
        assert r.status_code == 201
        pid = r.json()["proposals"][0]["proposal_id"]
        r2 = _decide(pid, "maybe")
        assert r2.status_code == 422, r2.text
        assert "decision" in r2.text.lower()

    def test_decide_missing_decided_by(self):
        r = _intake([_authority_map_proposal()])
        assert r.status_code == 201
        pid = r.json()["proposals"][0]["proposal_id"]
        r2 = client.post(
            f"/api/dcl/align/proposals/{pid}/decide",
            json={"tenant_id": TENANT, "decision": "approve", "decided_by": ""},
        )
        assert r2.status_code == 422, r2.text

    def test_concept_lookup_missing_tenant(self):
        r = client.get("/api/dcl/align/concept-lookup?alias=rev")
        assert r.status_code == 422, r.text

    def test_concept_lookup_missing_alias(self):
        r = client.get(f"/api/dcl/align/concept-lookup?tenant_id={TENANT}")
        assert r.status_code == 422, r.text

    def test_tenant_isolation_list(self):
        _intake([_authority_map_proposal()])
        r = client.get(f"/api/dcl/align/proposals?tenant_id={TENANT_B}")
        assert r.status_code == 200, r.text
        assert r.json()["total_count"] == 0, "TENANT_B must see no proposals from TENANT"


# =============================================================================
# Happy-path intake
# =============================================================================

class TestIntake:

    def test_happy_path_single(self):
        r = _intake([_authority_map_proposal()])
        assert r.status_code == 201, r.text
        body = r.json()
        assert body["accepted_count"] == 1
        assert body["duplicate_count"] == 0
        p = body["proposals"][0]
        assert p["status"] == "accepted"
        assert "proposal_id" in p

    def test_batch_intake(self):
        r = _intake([
            _authority_map_proposal("engineering"),
            _vocabulary_alias_proposal("rev", "revenue"),
            _org_hierarchy_proposal(),
        ])
        assert r.status_code == 201, r.text
        body = r.json()
        assert body["accepted_count"] == 3
        assert body["duplicate_count"] == 0

    def test_proposals_visible_in_list(self):
        _intake([_authority_map_proposal("cloud_spend")])
        r = client.get(f"/api/dcl/align/proposals?tenant_id={TENANT}&status=pending")
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["total_count"] >= 1
        types = [p["proposal_type"] for p in body["proposals"]]
        assert "authority_map" in types

    def test_proposal_carries_confidence_and_provenance(self):
        _intake([_authority_map_proposal()])
        r = client.get(f"/api/dcl/align/proposals?tenant_id={TENANT}&proposal_type=authority_map")
        assert r.status_code == 200, r.text
        p = r.json()["proposals"][0]
        assert float(p["confidence"]) == pytest.approx(0.92)
        prov = p["provenance"]
        assert prov["basis"] == "confirmed"
        assert prov["confirmed_by"] == "CFO"

    def test_inferred_provenance_accepted(self):
        r = _intake([_vocabulary_alias_proposal("rev_metric", "revenue")])
        assert r.status_code == 201, r.text
        assert r.json()["accepted_count"] == 1
        r2 = client.get(
            f"/api/dcl/align/proposals?tenant_id={TENANT}&proposal_type=vocabulary_alias"
        )
        p = r2.json()["proposals"][0]
        assert p["provenance"]["basis"] == "inferred"


# =============================================================================
# Explicit duplicate detection
# =============================================================================

class TestDuplicates:

    def test_duplicate_reported_not_silently_dropped(self):
        r1 = _intake([_authority_map_proposal("cloud_spend")])
        assert r1.status_code == 201
        first_pid = r1.json()["proposals"][0]["proposal_id"]

        r2 = _intake([_authority_map_proposal("cloud_spend")])
        assert r2.status_code == 201, r2.text
        body = r2.json()
        assert body["duplicate_count"] == 1
        assert body["accepted_count"] == 0
        dup = body["proposals"][0]
        assert dup["status"] == "duplicate"
        assert dup["duplicate_of"] == first_pid, (
            f"Expected duplicate_of={first_pid!r}, got {dup['duplicate_of']!r}"
        )

    def test_mixed_batch_accepted_and_duplicate(self):
        _intake([_authority_map_proposal("cloud_spend")])
        r = _intake([
            _authority_map_proposal("cloud_spend"),   # duplicate
            _authority_map_proposal("engineering"),   # new
        ])
        assert r.status_code == 201, r.text
        body = r.json()
        assert body["duplicate_count"] == 1
        assert body["accepted_count"] == 1
        statuses = {p["proposal_type"] + ":" + p.get("natural_key", ""): p["status"]
                    for p in body["proposals"]}
        assert any(v == "duplicate" for v in statuses.values())
        assert any(v == "accepted" for v in statuses.values())

    def test_rejected_proposal_can_be_reproposed(self):
        r1 = _intake([_authority_map_proposal("cloud_spend")])
        pid = r1.json()["proposals"][0]["proposal_id"]
        _decide(pid, "reject")

        r2 = _intake([_authority_map_proposal("cloud_spend")])
        assert r2.status_code == 201, r2.text
        body = r2.json()
        assert body["accepted_count"] == 1, (
            "A rejected proposal must be re-proposable — the partial unique "
            "index covers only status='pending'."
        )


# =============================================================================
# Approve → canonical visible through existing read surfaces
# =============================================================================

class TestApproveAuthority:

    def test_authority_map_canonical_visible_after_approve(self):
        r = _intake([_authority_map_proposal("cloud_spend", ["aws_cost_explorer", "snowflake"])])
        pid = r.json()["proposals"][0]["proposal_id"]

        r2 = _decide(pid, "approve", "CFO-operator")
        assert r2.status_code == 200, r2.text
        body = r2.json()
        assert body["decision"] == "approve"
        assert body["canonical_artifact_id"] is not None
        assert "authority_map" in body["canonical_artifact_id"]

        r3 = client.get(f"/api/dcl/conflicts/authority-map?tenant_id={TENANT}")
        assert r3.status_code == 200, r3.text
        amap = r3.json()["authority_map"]
        prefixes = {e["concept_prefix"]: e["ranked_sources"] for e in amap}
        assert "cloud_spend" in prefixes, (
            f"Approved authority_map proposal for 'cloud_spend' must appear in "
            f"GET /api/dcl/conflicts/authority-map; got: {list(prefixes)}"
        )
        assert prefixes["cloud_spend"][0] == "aws_cost_explorer"

    def test_approved_proposal_status_is_approved(self):
        r = _intake([_authority_map_proposal("engineering")])
        pid = r.json()["proposals"][0]["proposal_id"]
        _decide(pid, "approve")
        r2 = client.get(f"/api/dcl/align/proposals?tenant_id={TENANT}&status=approved")
        proposals = r2.json()["proposals"]
        pids = [p["proposal_id"] for p in proposals]
        assert pid in pids


class TestApproveConflictCandidate:

    def test_conflict_candidate_registers_in_conflict_register(self):
        r = _intake([_conflict_candidate_proposal()])
        pid = r.json()["proposals"][0]["proposal_id"]

        r2 = _decide(pid, "approve")
        assert r2.status_code == 200, r2.text
        assert r2.json()["canonical_artifact_id"].startswith("conflict:")

        r3 = client.get(
            f"/api/dcl/conflicts?tenant_id={TENANT}&entity_id={ENTITY}"
        )
        assert r3.status_code == 200, r3.text
        conflicts = r3.json()["conflicts"]
        stakeholder_conflicts = [
            c for c in conflicts
            if c.get("source_class") in ("stakeholder_system", "stakeholder_stakeholder")
        ]
        assert len(stakeholder_conflicts) >= 1, (
            f"Approved conflict_candidate must appear in GET /api/dcl/conflicts "
            f"with source_class='stakeholder_system'; got conflicts: {conflicts}"
        )
        assert stakeholder_conflicts[0]["concept"] == "revenue.total"


class TestApproveVocabularyAlias:

    def test_alias_resolves_after_approve(self):
        r = _intake([_vocabulary_alias_proposal("rev_alias_test", "revenue")])
        pid = r.json()["proposals"][0]["proposal_id"]

        r_before = client.get(
            f"/api/dcl/align/concept-lookup?tenant_id={TENANT}&alias=rev_alias_test"
        )
        assert r_before.status_code == 200, r_before.text
        assert r_before.json()["resolved"] is False, (
            "Alias must NOT resolve before approval"
        )

        _decide(pid, "approve")

        r_after = client.get(
            f"/api/dcl/align/concept-lookup?tenant_id={TENANT}&alias=rev_alias_test"
        )
        assert r_after.status_code == 200, r_after.text
        body = r_after.json()
        assert body["resolved"] is True, (
            "Alias must resolve after approval — vocabulary_alias apply-on-approve "
            "must write to tenant_concept_aliases."
        )
        assert body["concept_id"] == "revenue"

    def test_alias_case_insensitive(self):
        r = _intake([_vocabulary_alias_proposal("Rev_Alias_Case", "revenue")])
        pid = r.json()["proposals"][0]["proposal_id"]
        _decide(pid, "approve")

        r2 = client.get(
            f"/api/dcl/align/concept-lookup?tenant_id={TENANT}&alias=rev_alias_case"
        )
        assert r2.json()["resolved"] is True
        assert r2.json()["concept_id"] == "revenue"


class TestApproveOrgHierarchy:

    def test_hierarchy_visible_in_contour_after_approve(self):
        r = _intake([_org_hierarchy_proposal()])
        pid = r.json()["proposals"][0]["proposal_id"]

        r_contour_before = client.get(f"/api/dcl/align/contour?tenant_id={TENANT}")
        assert r_contour_before.status_code == 200
        assert r_contour_before.json()["contour_source"] == "none"

        _decide(pid, "approve")

        r_contour = client.get(f"/api/dcl/align/contour?tenant_id={TENANT}")
        assert r_contour.status_code == 200, r_contour.text
        body = r_contour.json()
        assert body["contour_source"] == "approved"
        assert "division" in body["hierarchy"], (
            "Approved org_hierarchy for 'division' must appear in contour hierarchy"
        )
        division_names = [n["name"] for n in body["hierarchy"]["division"]]
        assert "Cloud" in division_names

    def test_contour_sor_authority_projected_from_authority_map(self):
        r_auth = _intake([_authority_map_proposal("cloud_spend", ["aws_cost_explorer"])])
        _decide(r_auth.json()["proposals"][0]["proposal_id"], "approve")

        r_hier = _intake([_org_hierarchy_proposal()])
        _decide(r_hier.json()["proposals"][0]["proposal_id"], "approve")

        r_contour = client.get(f"/api/dcl/align/contour?tenant_id={TENANT}")
        body = r_contour.json()
        sor = body.get("sor_authority", {})
        assert "cloud_spend" in sor, (
            "sor_authority must be projected from tenant_authority_map, not stored "
            "in tenant_contour. The approved authority_map proposal must appear here."
        )
        assert sor["cloud_spend"]["system"] == "aws_cost_explorer"


# =============================================================================
# Reject → zero canonical residue
# =============================================================================

class TestReject:

    def test_reject_authority_map_no_canonical_residue(self):
        r = _intake([_authority_map_proposal("reject_prefix_test")])
        pid = r.json()["proposals"][0]["proposal_id"]

        r2 = _decide(pid, "reject")
        assert r2.status_code == 200, r2.text
        body = r2.json()
        assert body["decision"] == "reject"
        assert body["canonical_artifact_id"] is None

        r3 = client.get(f"/api/dcl/conflicts/authority-map?tenant_id={TENANT}")
        amap = r3.json()["authority_map"]
        prefixes = [e["concept_prefix"] for e in amap]
        assert "reject_prefix_test" not in prefixes, (
            "Rejected authority_map proposal must leave zero canonical residue "
            "in tenant_authority_map."
        )

    def test_reject_vocabulary_alias_no_residue(self):
        r = _intake([_vocabulary_alias_proposal("reject_alias_test", "revenue")])
        pid = r.json()["proposals"][0]["proposal_id"]
        _decide(pid, "reject")

        r2 = client.get(
            f"/api/dcl/align/concept-lookup?tenant_id={TENANT}&alias=reject_alias_test"
        )
        assert r2.json()["resolved"] is False, (
            "Rejected vocabulary_alias proposal must leave zero residue in "
            "tenant_concept_aliases; alias must not resolve."
        )

    def test_reject_conflict_candidate_no_residue(self):
        r = _intake([_conflict_candidate_proposal()])
        pid = r.json()["proposals"][0]["proposal_id"]
        _decide(pid, "reject")

        r2 = client.get(f"/api/dcl/conflicts?tenant_id={TENANT}&entity_id={ENTITY}")
        assert r2.status_code == 200
        conflicts = r2.json()["conflicts"]
        stakeholder_conflicts = [
            c for c in conflicts
            if c.get("source_class") in ("stakeholder_system", "stakeholder_stakeholder")
        ]
        assert len(stakeholder_conflicts) == 0, (
            "Rejected conflict_candidate must leave zero residue in conflict_register."
        )

    def test_double_decision_refused(self):
        r = _intake([_authority_map_proposal("double_decide_test")])
        pid = r.json()["proposals"][0]["proposal_id"]
        _decide(pid, "approve")
        r2 = _decide(pid, "reject")
        assert r2.status_code == 409, r2.text
        assert "already" in r2.text.lower() or "approved" in r2.text.lower()


# =============================================================================
# Decision trace visible via GET /api/dcl/traces
# =============================================================================

class TestDecisionTrace:

    def test_approve_trace_visible_in_traces(self):
        r = _intake([_authority_map_proposal("trace_test_prefix")])
        pid = r.json()["proposals"][0]["proposal_id"]
        _decide(pid, "approve", "trace-operator")

        r2 = client.get(f"/api/dcl/traces?tenant_id={TENANT}&trace_type=align_decision")
        assert r2.status_code == 200, r2.text
        traces = r2.json()["traces"]
        align_traces = [t for t in traces if t["trace_type"] == "align_decision"]
        assert len(align_traces) >= 1, (
            f"Approved align proposal must produce an align_decision trace visible via "
            f"GET /api/dcl/traces. Got trace_types: {[t['trace_type'] for t in traces]}"
        )
        trace = align_traces[0]
        assert trace["agent"] == "trace-operator"
        assert trace["decision_type"] == "approve"
        refs = trace.get("refs", {})
        assert refs.get("proposal_id") == pid

    def test_reject_trace_visible_in_traces(self):
        r = _intake([_authority_map_proposal("trace_reject_prefix")])
        pid = r.json()["proposals"][0]["proposal_id"]
        _decide(pid, "reject", "reject-operator")

        r2 = client.get(f"/api/dcl/traces?tenant_id={TENANT}&trace_type=align_decision")
        assert r2.status_code == 200, r2.text
        traces = r2.json()["traces"]
        reject_traces = [
            t for t in traces
            if t["trace_type"] == "align_decision" and t["decision_type"] == "reject"
        ]
        assert len(reject_traces) >= 1

    def test_trace_tenant_isolation(self):
        r = _intake([_authority_map_proposal("isolation_prefix")])
        pid = r.json()["proposals"][0]["proposal_id"]
        _decide(pid, "approve")

        r2 = client.get(f"/api/dcl/traces?tenant_id={TENANT_B}&trace_type=align_decision")
        assert r2.status_code == 200
        assert r2.json()["total_count"] == 0, (
            "TENANT_B must see no decision traces from TENANT — trace reads are tenant-scoped."
        )


class TestRebuildFailsLoudOnStoreError:
    """A contour-store FAILURE during graph rebuild must abort the rebuild.

    Absence (no approved contour → None → sample YAML) is legitimate; a store
    error silently downgraded to sample YAML is the banned silent-fallback
    class (fifth-instance guard, database layer). Sole carve-out: UndefinedTable
    on a pre-mig023 store proves zero approved contours exist (ledger #70)."""

    def test_store_failure_aborts_rebuild(self, monkeypatch):
        from backend.db.align_store import AlignStore
        from backend.engine import graph_store

        def _boom(self):
            raise RuntimeError("simulated contour-store failure (connection lost)")

        monkeypatch.setattr(
            AlignStore, "load_approved_contour_for_rebuild", _boom
        )
        with pytest.raises(RuntimeError, match="simulated contour-store failure"):
            graph_store.rebuild_graph()

    def test_pre_mig023_store_is_absence_not_failure(self, monkeypatch):
        from psycopg2 import errors as psycopg2_errors

        from backend.db.align_store import AlignStore
        from backend.engine import graph_store

        def _undefined(self):
            raise psycopg2_errors.UndefinedTable(
                'relation "tenant_contour" does not exist'
            )

        monkeypatch.setattr(
            AlignStore, "load_approved_contour_for_rebuild", _undefined
        )
        # Must complete (sample-YAML absence path), not raise — prod :8004
        # startup survives until the #70 prod migration gate applies mig023.
        graph_store.rebuild_graph()
        assert graph_store.get_semantic_graph() is not None
