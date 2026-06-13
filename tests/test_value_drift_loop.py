"""Gate 3B D2 acceptance: value_drift sweep + full HITL loop
(approve → canonicalize, reject → zero residue) + trend tracking + guard.

Operator-visible outcome: when SAP reports revenue.total.amount = 1 000 000 and
Salesforce reports 1 100 000 for the same entity / 2025-Q1 in the same ingest
run, the scheduled value-drift sweep detects it (trend: 0→1 open value
conflict), files a value_drift proposal; approving that proposal writes a
conflict_disposition that supersedes the Salesforce triple (SAP wins per the
authority map), sets canonical_artifact_id = "conflict_disposition:{uuid}", and
the full loop — detection → proposal → decision → canonical — is visible at
GET /api/dcl/traces as a proposal_decision trace.

Live-service integration tests: TestClient against aos-dev (mig025 applied).
Per-run-unique tenant/entity IDs (B14 / Gate 1B discipline).
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

# Per-run-unique identity (B14).
_TENANT = str(uuid.uuid4())
_TAG = uuid.uuid4().hex[:6]
_ENTITY = f"ValueDrift-{_TAG}"
_ENTITY_SD = f"StructDrift-{_TAG}"   # used for structural_drift reject test
_ENTITY_TREND = f"DriftTrend-{_TAG}" # used for trend test

_SAP = "sap"
_SALESFORCE = "salesforce"

# Values: SAP 1 000 000 vs Salesforce 1 100 000 → rel_delta = 10% >> 0.5% threshold.
_SAP_VALUE = 1_000_000
_SF_VALUE = 1_100_000


# =============================================================================
# Helpers
# =============================================================================

def _make_triple(entity_id, source_system, value, prop="amount", period="2025-Q1"):
    return {
        "entity_id": entity_id,
        "concept": "revenue.total",
        "property": prop,
        "value": value,
        "period": period,
        "source_system": source_system,
        "source_table": "vd_probe",
        "source_field": prop,
        "pipe_id": str(uuid.uuid4()),
        "fabric_plane": "ipaas",
        "confidence_score": 0.95,
        "confidence_tier": "exact",
    }


def _ingest(entity_id, triples, dcl_ingest_id=None):
    run_id = dcl_ingest_id or str(uuid.uuid4())
    resp = client.post(
        "/api/dcl/ingest-triples",
        json={
            "tenant_id": _TENANT,
            "dcl_ingest_id": run_id,
            "entity_id": entity_id,
            "triples": triples,
        },
    )
    assert resp.status_code == 201, f"ingest failed: {resp.text[:400]}"
    return run_id, resp.json()


def _setup_conflict_entity():
    """Ingest two runs for _ENTITY so _get_entity_pairs picks it up.
    Run 1: SAP only (establishes previous_run_id track).
    Run 2: SAP + Salesforce in SAME run (materially different values → value conflict).
    Returns (run_id_1, run_id_2).
    """
    run_id_1, _ = _ingest(_ENTITY, [_make_triple(_ENTITY, _SAP, _SAP_VALUE)])
    run_id_2, _ = _ingest(_ENTITY, [
        _make_triple(_ENTITY, _SAP, _SAP_VALUE),
        _make_triple(_ENTITY, _SALESFORCE, _SF_VALUE),
    ])
    return run_id_1, run_id_2


def _setup_authority_map_sap_wins():
    """Set up authority map so SAP outranks Salesforce for revenue.*."""
    resp = client.put(
        "/api/dcl/conflicts/authority-map",
        json={
            "tenant_id": _TENANT,
            "concept_prefix": "revenue",
            "ranked_sources": [_SAP, _SALESFORCE],
        },
    )
    assert resp.status_code == 200, f"authority map PUT failed: {resp.text[:300]}"


def _run_now(job_name):
    resp = client.post(f"/api/dcl/monitor/schedule/{job_name}/run-now")
    return resp


def _get_proposals(proposal_type=None, entity_id=None):
    params = {"tenant_id": _TENANT}
    if proposal_type:
        params["proposal_type"] = proposal_type
    resp = client.get("/api/dcl/proposals", params=params)
    assert resp.status_code == 200, resp.text
    proposals = resp.json()["proposals"]
    if entity_id:
        proposals = [p for p in proposals if p.get("entity_id") == entity_id]
    return proposals


def _decide(proposal_id, decision, decided_by="test-operator"):
    return client.post(
        f"/api/dcl/proposals/{proposal_id}/decide",
        json={"tenant_id": _TENANT, "decision": decision, "decided_by": decided_by},
    )


def _get_traces():
    resp = client.get(
        "/api/dcl/traces",
        params={"tenant_id": _TENANT, "trace_type": "proposal_decision"},
    )
    assert resp.status_code == 200, resp.text
    return resp.json().get("traces", [])


def _get_conflict(conflict_id):
    resp = client.get(
        f"/api/dcl/conflicts/{conflict_id}",
        params={"tenant_id": _TENANT},
    )
    return resp


def _cleanup():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM change_proposal_decisions WHERE tenant_id = %s::uuid", (_TENANT,))
            cur.execute("DELETE FROM change_proposals WHERE tenant_id = %s::uuid", (_TENANT,))
            cur.execute("DELETE FROM conflict_dispositions WHERE tenant_id = %s::uuid", (_TENANT,))
            cur.execute("DELETE FROM conflict_register WHERE tenant_id = %s::uuid", (_TENANT,))
            cur.execute("DELETE FROM semantic_triples WHERE tenant_id = %s::uuid", (_TENANT,))
            cur.execute("DELETE FROM tenant_runs WHERE tenant_id = %s::uuid", (_TENANT,))
            cur.execute("DELETE FROM ingest_log WHERE tenant_id = %s::uuid", (_TENANT,))
            cur.execute("DELETE FROM tenant_authority_map WHERE tenant_id = %s", (_TENANT,))
        conn.commit()


@pytest.fixture(autouse=True)
def _clean():
    _cleanup()
    yield
    _cleanup()


# =============================================================================
# Guard test (#91)
# =============================================================================

def test_scheduler_guard_arms_nothing_when_disabled(monkeypatch):
    """With DCL_SCHEDULER_ENABLED=false, _start_scheduler arms zero jobs
    even if a monitor_schedule row has enabled=true."""
    from backend.api.main import _start_scheduler
    from backend.api.scheduler import get_scheduler, set_scheduler

    monkeypatch.setenv("DCL_SCHEDULER_ENABLED", "false")

    prev = get_scheduler()
    set_scheduler(None)
    try:
        _start_scheduler()
        assert get_scheduler() is None, (
            "DCL_SCHEDULER_ENABLED=false: _start_scheduler must arm zero jobs"
        )
    finally:
        set_scheduler(prev)


def test_conftest_sets_scheduler_guard():
    """conftest.py sets DCL_SCHEDULER_ENABLED=false before any app import."""
    import os
    assert os.environ.get("DCL_SCHEDULER_ENABLED") == "false", (
        "conftest.py must export DCL_SCHEDULER_ENABLED=false so pytest never "
        "arms the ambient timer"
    )


# =============================================================================
# Schedule list: value_drift appears
# =============================================================================

def test_schedule_list_contains_value_drift():
    """GET /api/dcl/monitor/schedule lists the value_drift job (mig025 seeded it)."""
    resp = client.get("/api/dcl/monitor/schedule")
    assert resp.status_code == 200, resp.text
    jobs = {j["job_name"]: j for j in resp.json()["jobs"]}
    assert "value_drift" in jobs, f"value_drift not in jobs: {list(jobs)}"
    job = jobs["value_drift"]
    assert job["interval_seconds"] == 300
    assert job["enabled"] is False


# =============================================================================
# Value drift detection
# =============================================================================

def test_run_now_detects_value_drift():
    """After two ingests (run1: SAP only, run2: SAP + Salesforce with 10% delta),
    value_drift run-now files a proposal for the open value conflict."""
    _setup_conflict_entity()

    resp = _run_now("value_drift")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["job_name"] == "value_drift"
    assert body["status"] == "ok"
    assert body["entities_scanned"] >= 1
    assert body["drift_findings"] >= 1
    assert body["proposals_filed"] >= 1


def test_value_drift_proposal_payload():
    """The value_drift proposal carries: entity_id, concept, property, period,
    claims (both sources with values), conflict_id, trend dict, and namespaced
    provenance (no bare run_id — I1)."""
    _setup_conflict_entity()
    _run_now("value_drift")

    proposals = _get_proposals("value_drift", _ENTITY)
    assert len(proposals) >= 1, f"expected at least one value_drift proposal for {_ENTITY}"

    p = proposals[0]
    assert p["proposal_type"] == "value_drift"
    assert p["status"] == "pending"
    assert p["confidence"] == 1.0

    payload = p["payload"]
    assert payload["entity_id"] == _ENTITY
    assert payload["concept"] == "revenue.total"
    assert payload["property"] == "amount"
    assert payload["period"] == "2025-Q1"
    assert "run_id" not in payload, "I1 violation: run_id in payload"

    # Claims: both sources present
    sources_in_claims = {c["source_system"] for c in payload["claims"]}
    assert _SAP in sources_in_claims, f"{_SAP} not in claims: {sources_in_claims}"
    assert _SALESFORCE in sources_in_claims, f"{_SALESFORCE} not in claims: {sources_in_claims}"

    # Verify actual values from test ingest (B5)
    sap_claim = next(c for c in payload["claims"] if c["source_system"] == _SAP)
    sf_claim = next(c for c in payload["claims"] if c["source_system"] == _SALESFORCE)
    assert sap_claim["value"] == _SAP_VALUE, f"SAP value: expected {_SAP_VALUE}, got {sap_claim['value']}"
    assert sf_claim["value"] == _SF_VALUE, f"Salesforce value: expected {_SF_VALUE}, got {sf_claim['value']}"

    # conflict_id must be present and a UUID
    assert "conflict_id" in payload and payload["conflict_id"], "payload missing conflict_id"
    uuid.UUID(payload["conflict_id"])  # raises if not a valid UUID

    # Trend
    trend = payload["trend"]
    assert "prior_count" in trend and "current_count" in trend
    assert trend["current_count"] >= 1, f"current_count must be >= 1 after detection"

    # Provenance
    prov = p["provenance"]
    assert prov["basis"] == "inferred"
    assert prov["source"] == "value_drift_monitor"
    assert "dcl_ingest_id" in prov
    assert "run_id" not in prov, "I1 violation: run_id in provenance"


def test_value_drift_dedup_second_run_now():
    """A second run-now for the same entity must NOT refile a duplicate proposal.
    The dedup is explicit: proposals_deduped increases, proposals_filed stays 0."""
    _setup_conflict_entity()

    r1 = _run_now("value_drift")
    assert r1.status_code == 200, r1.text
    assert r1.json()["proposals_filed"] >= 1

    r2 = _run_now("value_drift")
    assert r2.status_code == 200, r2.text
    b2 = r2.json()
    assert b2["proposals_filed"] == 0, (
        f"Second run-now must not refile: proposals_filed={b2['proposals_filed']}"
    )
    assert b2["proposals_deduped"] >= 1, f"Second run-now must report deduped: {b2}"

    # Exactly one pending proposal
    proposals = _get_proposals("value_drift", _ENTITY)
    pending = [p for p in proposals if p["status"] == "pending"]
    assert len(pending) == 1, (
        f"Expected exactly 1 pending value_drift proposal, got {len(pending)}"
    )


# =============================================================================
# LOOP — approve: conflict dispositioned, SAP wins, Salesforce superseded
# =============================================================================

def test_approve_value_drift_dispositions_conflict():
    """Approve the value_drift proposal → conflict dispositioned via authority map
    (SAP wins, Salesforce triple superseded), canonical_artifact_id set,
    proposal_decision trace visible at GET /api/dcl/traces."""
    _setup_authority_map_sap_wins()
    _setup_conflict_entity()
    _run_now("value_drift")

    proposals = _get_proposals("value_drift", _ENTITY)
    pending = [p for p in proposals if p["status"] == "pending"]
    assert len(pending) >= 1, "need a pending value_drift proposal to approve"
    p = pending[0]
    proposal_id = p["proposal_id"]
    conflict_id = p["payload"]["conflict_id"]
    sf_claim = next(
        c for c in p["payload"]["claims"] if c["source_system"] == _SALESFORCE
    )
    sf_triple_id = sf_claim.get("triple_id")

    # Approve
    resp = _decide(proposal_id, "approve")
    assert resp.status_code == 200, f"approve failed: {resp.text}"
    dec = resp.json()
    assert dec["decision"] == "approve"
    artifact = dec.get("canonical_artifact_id", "")
    assert artifact.startswith("conflict_disposition:"), (
        f"canonical_artifact_id must start with 'conflict_disposition:'; got {artifact!r}"
    )
    disp_id = artifact.split("conflict_disposition:")[1]
    uuid.UUID(disp_id)  # must be a valid UUID

    # Conflict now dispositioned
    conf_resp = _get_conflict(conflict_id)
    assert conf_resp.status_code == 200, conf_resp.text
    conflict = conf_resp.json()
    assert conflict["status"] == "dispositioned", (
        f"conflict must be dispositioned after approve; got {conflict['status']}"
    )

    # Disposition row shows SAP as winner
    disps = conflict.get("dispositions", [])
    assert len(disps) >= 1, "expected at least one disposition row"
    d = disps[0]
    assert d["winner_source"] == _SAP, (
        f"authority-map winner must be SAP; got {d['winner_source']!r}"
    )
    assert _SALESFORCE in d["loser_sources"], "Salesforce must be in loser_sources"

    # Salesforce triple superseded
    if sf_triple_id:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT superseded_at FROM semantic_triples WHERE id = %s::uuid",
                    (sf_triple_id,),
                )
                row = cur.fetchone()
        assert row is not None and row[0] is not None, (
            f"Salesforce triple {sf_triple_id} must have superseded_at set after approve"
        )

    # Proposal shows canonical_artifact_id
    proposals_after = _get_proposals("value_drift", _ENTITY)
    approved = next((p for p in proposals_after if p["proposal_id"] == proposal_id), None)
    assert approved is not None and approved["status"] == "approved"
    assert approved["canonical_artifact_id"] == artifact

    # Trace visible: proposal_decision for this approval
    # canonical_artifact_id lives in the refs jsonb column of decision_traces
    traces = _get_traces()
    decision_traces = [
        t for t in traces
        if t.get("decision_type") == "approve"
        and (t.get("refs") or {}).get("canonical_artifact_id") == artifact
    ]
    assert len(decision_traces) >= 1, (
        f"Expected a proposal_decision trace for approve with refs.canonical_artifact_id={artifact!r}; "
        f"found: {[{'decision_type': t.get('decision_type'), 'refs': t.get('refs')} for t in traces]}"
    )


# =============================================================================
# LOOP — reject: zero canonical residue
# =============================================================================

def test_reject_structural_drift_leaves_zero_residue():
    """Reject a structural_drift proposal → zero canonical residue: no disposition
    created, no triples superseded, entity state unchanged, reject trace visible."""
    # Two ingests with structural change for the structural entity
    _ingest(_ENTITY_SD, [_make_triple(_ENTITY_SD, _SAP, 1000, prop="sd_prop_a")])
    _ingest(_ENTITY_SD, [_make_triple(_ENTITY_SD, _SAP, 1000, prop="sd_prop_b")])

    # Fire structural drift sweep to get a structural_drift proposal
    r = _run_now("structural_drift")
    assert r.status_code == 200, f"structural_drift run-now failed: {r.text}"

    sd_proposals = _get_proposals("structural_drift", _ENTITY_SD)
    pending_sd = [p for p in sd_proposals if p["status"] == "pending"]
    assert len(pending_sd) >= 1, (
        f"Expected at least one pending structural_drift proposal for {_ENTITY_SD}"
    )
    p = pending_sd[0]
    proposal_id = p["proposal_id"]

    # Snapshot triples BEFORE reject (B19 before/after)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, superseded_at FROM semantic_triples "
                "WHERE tenant_id = %s::uuid AND entity_id = %s AND is_active = true",
                (_TENANT, _ENTITY_SD),
            )
            before_triples = {str(r[0]): r[1] for r in cur.fetchall()}

    # Reject
    resp = _decide(proposal_id, "reject")
    assert resp.status_code == 200, f"reject failed: {resp.text}"
    dec = resp.json()
    assert dec["decision"] == "reject"
    assert dec.get("canonical_artifact_id") is None, (
        "Reject must leave zero canonical residue — canonical_artifact_id must be null"
    )

    # No disposition created
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM conflict_dispositions WHERE tenant_id = %s::uuid",
                (_TENANT,),
            )
            disp_count = cur.fetchone()[0]
    assert disp_count == 0, (
        f"Reject must write ZERO dispositions; found {disp_count}"
    )

    # Entity triples unchanged (no supersession)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, superseded_at FROM semantic_triples "
                "WHERE tenant_id = %s::uuid AND entity_id = %s AND is_active = true",
                (_TENANT, _ENTITY_SD),
            )
            after_triples = {str(r[0]): r[1] for r in cur.fetchall()}

    assert before_triples == after_triples, (
        "Structural drift reject must not change any triple's superseded_at. "
        f"Before: {before_triples}, After: {after_triples}"
    )

    # Reject trace visible
    traces = _get_traces()
    reject_traces = [t for t in traces if t.get("decision_type") == "reject"]
    assert len(reject_traces) >= 1, (
        f"Expected at least one reject trace; got: {[t.get('decision_type') for t in traces]}"
    )


# =============================================================================
# Trend: two sweeps with new conflict between them → N→M
# =============================================================================

def test_trend_count_reflects_new_conflict():
    """Trend payload carries accurate prior/current counts.

    Ingest calls detect_and_register for each batch, so by the time the sweep
    runs, conflicts are already registered in the conflict_register. The sweep's
    prior_count = count at sweep-start (post-ingest), current_count = count after
    the sweep's own detect_and_register refresh (idempotent). Both reflect the
    true DB state — the trend shows 'how many open conflicts existed when this
    sweep started, how many after this sweep's pass'.

    This test verifies: trend dict is present with both fields, current_count >= 1
    (the ingest-detected SAP vs Salesforce conflict is visible), and the sweep's
    count is consistent (current >= prior, no conflicts deleted during sweep).
    """
    # Run 1: SAP only — no conflict yet.
    _ingest(_ENTITY_TREND, [_make_triple(_ENTITY_TREND, _SAP, 1_000_000)])
    # Run 2: SAP only again — establishes previous_run_id in tenant_runs.
    _ingest(_ENTITY_TREND, [_make_triple(_ENTITY_TREND, _SAP, 1_000_000)])

    # Sweep 1: both runs SAP-only → no open value conflicts.
    r1 = _run_now("value_drift")
    assert r1.status_code == 200, r1.text
    proposals_before = _get_proposals("value_drift", _ENTITY_TREND)
    assert len(proposals_before) == 0, (
        f"Expected 0 proposals after SAP-only ingests; got {len(proposals_before)}"
    )

    # Run 3: SAP + Salesforce in same run → 10% delta. Ingest auto-detects the
    # conflict via detect_and_register (coords-filtered). By the time the next
    # sweep runs, count_open_value already reflects this conflict.
    _ingest(_ENTITY_TREND, [
        _make_triple(_ENTITY_TREND, _SAP, 1_000_000),
        _make_triple(_ENTITY_TREND, _SALESFORCE, 1_100_000),
    ])

    r2 = _run_now("value_drift")
    assert r2.status_code == 200, r2.text
    assert r2.json()["proposals_filed"] >= 1, (
        f"Expected at least 1 proposal filed in sweep 2: {r2.json()}"
    )

    proposals_after = _get_proposals("value_drift", _ENTITY_TREND)
    assert len(proposals_after) >= 1, "expected value_drift proposal after conflict ingest"

    trend = proposals_after[0]["payload"]["trend"]
    assert "prior_count" in trend and "current_count" in trend, (
        f"trend must have prior_count and current_count; got: {trend}"
    )
    # Ingest auto-detected the conflict → prior_count >= 1 when sweep ran.
    assert trend["prior_count"] >= 1, (
        f"ingest pre-registers conflicts via detect_and_register; "
        f"prior_count must be >= 1 at sweep time; got {trend['prior_count']}"
    )
    assert trend["current_count"] >= trend["prior_count"], (
        f"current_count must be >= prior_count (no deletions during sweep); "
        f"got prior={trend['prior_count']}, current={trend['current_count']}"
    )


# =============================================================================
# Negative: approve value_drift whose conflict is already dispositioned → 409
# =============================================================================

def test_approve_already_resolved_conflict_returns_409():
    """If the underlying conflict is already dispositioned (via the HITL route,
    not through the proposal), approving the value_drift proposal returns 409
    with a readable error — not a 500."""
    _setup_authority_map_sap_wins()
    _setup_conflict_entity()
    _run_now("value_drift")

    proposals = _get_proposals("value_drift", _ENTITY)
    pending = [p for p in proposals if p["status"] == "pending"]
    assert len(pending) >= 1
    p = pending[0]
    proposal_id = p["proposal_id"]
    conflict_id = p["payload"]["conflict_id"]

    # Disposition the conflict DIRECTLY via the HITL route (not through the proposal)
    claims = p["payload"]["claims"]
    sources = [c["source_system"] for c in claims]
    hitl_resp = client.post(
        f"/api/dcl/conflicts/{conflict_id}/disposition",
        json={
            "tenant_id": _TENANT,
            "action": "accept_a",
            "decided_by": "test-direct-hitl",
            "rationale": "Direct HITL disposition to test negative path.",
        },
    )
    assert hitl_resp.status_code == 200, f"HITL disposition failed: {hitl_resp.text}"

    # Now try to approve the still-pending value_drift proposal → 409
    resp = _decide(proposal_id, "approve", decided_by="test-double-approve")
    assert resp.status_code == 409, (
        f"Expected 409 (already dispositioned conflict), got {resp.status_code}: {resp.text[:300]}"
    )
    detail = resp.json().get("detail", "")
    assert "dispositioned" in detail.lower(), (
        f"Error message must mention 'dispositioned'; got: {detail!r}"
    )
