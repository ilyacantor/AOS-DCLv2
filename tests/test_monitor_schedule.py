"""Gate 3B D1 acceptance: structural drift monitor + operator schedule API
(ContextOS_Blueprint_v1 §11, DCL Gate 3B Dispatch 1).

Operator-visible outcome: after two ingests for one entity (run2 adds
revenue.total.run2_prop, drops revenue.total.run1_prop vs run1), POST
run-now reports the added and removed keys exactly; a structural_drift
proposal appears at GET /api/dcl/proposals carrying the correct payload
(added/removed, dcl_ingest_id_base/compare, provenance basis=inferred
source=structural_drift_monitor); a second run-now does NOT refile a
duplicate (explicit dedup asserted); pause flips enabled=false and the
schedule API reflects it; resume flips back. Negative test: an entity
with only ONE ingest produces no drift finding.

Live-service integration tests: TestClient drives the real FastAPI app
against the aos-dev database (migration 024 applied). All tenant/entity
IDs are per-run-unique (B14 / Gate 1B lesson).
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

# Per-run-unique identity so reruns don't collide (B14).
_TENANT = str(uuid.uuid4())
_TAG = uuid.uuid4().hex[:6]
_ENTITY = f"DriftMonitor-{_TAG}"          # two-run entity (drift)
_ENTITY_SINGLE = f"DriftSingle-{_TAG}"    # one-run entity (negative test)


def _make_triple(entity_id, prop):
    """Minimal valid triple. Uses revenue.total (valid concept + mapped domain).
    Varies only the property to create structural drift between runs."""
    return {
        "entity_id": entity_id,
        "concept": "revenue.total",
        "property": prop,
        "value": 1000,
        "period": "2025-Q1",
        "source_system": "sap",
        "source_table": "drift_probe",
        "source_field": prop,
        "pipe_id": str(uuid.uuid4()),
        "fabric_plane": "ipaas",
        "confidence_score": 0.95,
        "confidence_tier": "exact",
    }


def _ingest(entity_id, prop, dcl_ingest_id=None):
    """POST one triple to ingest-triples. Returns (dcl_ingest_id, resp_json)."""
    run_id = dcl_ingest_id or str(uuid.uuid4())
    resp = client.post(
        "/api/dcl/ingest-triples",
        json={
            "tenant_id": _TENANT,
            "dcl_ingest_id": run_id,
            "entity_id": entity_id,
            "triples": [_make_triple(entity_id, prop)],
        },
    )
    assert resp.status_code == 201, f"ingest failed: {resp.text[:300]}"
    return run_id, resp.json()


def _cleanup():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM change_proposals WHERE tenant_id = %s::uuid", (_TENANT,)
            )
            # Remove test entity triples + tenant_runs entries (B19: test-tenant cleanup)
            cur.execute(
                "DELETE FROM semantic_triples WHERE tenant_id = %s::uuid", (_TENANT,)
            )
            cur.execute(
                "DELETE FROM tenant_runs WHERE tenant_id = %s::uuid", (_TENANT,)
            )
            cur.execute(
                "DELETE FROM ingest_log WHERE tenant_id = %s::uuid", (_TENANT,)
            )
        conn.commit()


@pytest.fixture(autouse=True)
def _clean():
    _cleanup()
    yield
    _cleanup()


# =============================================================================
# Test: schedule list API
# =============================================================================

def test_schedule_list_contains_structural_drift():
    """GET /api/dcl/monitor/schedule returns the structural_drift job."""
    resp = client.get("/api/dcl/monitor/schedule")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    jobs = {j["job_name"]: j for j in body["jobs"]}
    assert "structural_drift" in jobs, f"structural_drift not in jobs: {list(jobs)}"
    job = jobs["structural_drift"]
    assert job["interval_seconds"] == 300
    assert isinstance(job["enabled"], bool)
    assert "last_run_at" in job
    assert "last_status" in job


# =============================================================================
# Test: two-run structural drift detection
# =============================================================================

def test_run_now_detects_structural_drift():
    """Seed entity with two runs (run1: run1_prop, run2: run2_prop).
    run-now must find the added + removed keys exactly."""
    run_id_1, _ = _ingest(_ENTITY, "run1_prop")
    run_id_2, _ = _ingest(_ENTITY, "run2_prop")

    resp = client.post("/api/dcl/monitor/schedule/structural_drift/run-now")
    assert resp.status_code == 200, resp.text
    body = resp.json()

    assert body["job_name"] == "structural_drift"
    assert body["status"] == "ok"
    assert body["entities_scanned"] >= 1
    assert body["drift_findings"] >= 1
    assert body["proposals_filed"] >= 1


def test_proposal_payload_correct():
    """After run-now, the structural_drift proposal carries the correct added/removed keys
    and the namespaced run identifiers (dcl_ingest_id_base/compare, NOT run_id)."""
    run_id_1, _ = _ingest(_ENTITY, "run1_prop")
    run_id_2, _ = _ingest(_ENTITY, "run2_prop")

    client.post("/api/dcl/monitor/schedule/structural_drift/run-now")

    resp = client.get(
        "/api/dcl/proposals",
        params={"tenant_id": _TENANT, "proposal_type": "structural_drift"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    proposals = body["proposals"]
    assert len(proposals) >= 1, "expected at least one structural_drift proposal"

    # Find the proposal for our entity
    target = next(
        (p for p in proposals if p.get("entity_id") == _ENTITY),
        None,
    )
    assert target is not None, (
        f"No structural_drift proposal for entity {_ENTITY}. "
        f"Found: {[p.get('entity_id') for p in proposals]}"
    )
    assert target["proposal_type"] == "structural_drift"
    assert target["status"] == "pending"
    assert target["confidence"] == 1.0

    payload = target["payload"]
    assert payload["entity_id"] == _ENTITY
    # I1: namespaced identifiers only — no bare 'run_id'
    assert "run_id" not in payload, "I1 violation: 'run_id' in payload"
    assert payload["dcl_ingest_id_base"] == run_id_1
    assert payload["dcl_ingest_id_compare"] == run_id_2

    # Structural delta: added run2_prop, removed run1_prop
    added_keys = {(a["concept"], a["property"]) for a in payload["added"]}
    removed_keys = {(a["concept"], a["property"]) for a in payload["removed"]}
    assert ("revenue.total", "run2_prop") in added_keys, (
        f"Expected added revenue.total.run2_prop; got added={payload['added']}"
    )
    assert ("revenue.total", "run1_prop") in removed_keys, (
        f"Expected removed revenue.total.run1_prop; got removed={payload['removed']}"
    )

    # Provenance
    prov = target["provenance"]
    assert prov["basis"] == "inferred"
    assert prov["source"] == "structural_drift_monitor"
    assert prov["dcl_ingest_id_base"] == run_id_1
    assert prov["dcl_ingest_id_compare"] == run_id_2
    assert "run_id" not in prov, "I1 violation: 'run_id' in provenance"


def test_run_now_dedup_second_call_does_not_refile():
    """A second run-now for the same entity pair must NOT file a duplicate proposal.
    The dedup is explicit: proposals_deduped increases, proposals_filed does not."""
    _ingest(_ENTITY, "run1_prop")
    _ingest(_ENTITY, "run2_prop")

    r1 = client.post("/api/dcl/monitor/schedule/structural_drift/run-now")
    assert r1.status_code == 200, r1.text
    filed_first = r1.json()["proposals_filed"]
    assert filed_first >= 1

    r2 = client.post("/api/dcl/monitor/schedule/structural_drift/run-now")
    assert r2.status_code == 200, r2.text
    b2 = r2.json()
    assert b2["proposals_filed"] == 0, (
        f"Second run-now must not refile: proposals_filed={b2['proposals_filed']}"
    )
    assert b2["proposals_deduped"] >= 1, (
        f"Second run-now must report deduped: {b2}"
    )

    # Only one pending proposal exists
    resp = client.get(
        "/api/dcl/proposals",
        params={"tenant_id": _TENANT, "proposal_type": "structural_drift", "status": "pending"},
    )
    entity_proposals = [p for p in resp.json()["proposals"] if p.get("entity_id") == _ENTITY]
    assert len(entity_proposals) == 1, (
        f"Expected exactly 1 pending structural_drift proposal, got {len(entity_proposals)}"
    )


# =============================================================================
# Test: negative — single ingest, no drift
# =============================================================================

def test_run_now_single_ingest_files_nothing():
    """An entity with only ONE ingest has no previous_run_id → no drift detected,
    no proposal filed. entities_scanned may or may not include it (bounded by
    TENANT_LIMIT), but proposals_filed for this entity must be 0."""
    _ingest(_ENTITY_SINGLE, "only_prop")

    # Confirm no previous_run_id
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT previous_run_id FROM tenant_runs "
                "WHERE tenant_id = %s::uuid AND entity_id = %s",
                (_TENANT, _ENTITY_SINGLE),
            )
            row = cur.fetchone()
    assert row is not None and row[0] is None, (
        f"Expected previous_run_id=NULL after single ingest, got: {row}"
    )

    r = client.post("/api/dcl/monitor/schedule/structural_drift/run-now")
    assert r.status_code == 200, r.text

    resp = client.get(
        "/api/dcl/proposals",
        params={"tenant_id": _TENANT, "proposal_type": "structural_drift"},
    )
    entity_proposals = [
        p for p in resp.json()["proposals"] if p.get("entity_id") == _ENTITY_SINGLE
    ]
    assert len(entity_proposals) == 0, (
        f"Single-ingest entity must not have drift proposals; got {entity_proposals}"
    )


# =============================================================================
# Test: pause / resume
# =============================================================================

def test_pause_sets_enabled_false():
    """POST pause flips enabled=false and is visible at GET schedule."""
    resp = client.post("/api/dcl/monitor/schedule/structural_drift/pause")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["action"] == "paused"
    assert body["job"]["enabled"] is False

    # Visible via list
    sched = client.get("/api/dcl/monitor/schedule").json()
    job = next(j for j in sched["jobs"] if j["job_name"] == "structural_drift")
    assert job["enabled"] is False, f"Expected enabled=false after pause; got {job}"


def test_resume_sets_enabled_true():
    """POST pause then resume restores enabled=true."""
    client.post("/api/dcl/monitor/schedule/structural_drift/pause")

    resp = client.post("/api/dcl/monitor/schedule/structural_drift/resume")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["action"] == "resumed"
    assert body["job"]["enabled"] is True

    sched = client.get("/api/dcl/monitor/schedule").json()
    job = next(j for j in sched["jobs"] if j["job_name"] == "structural_drift")
    assert job["enabled"] is True, f"Expected enabled=true after resume; got {job}"


def test_pause_unknown_job_returns_404():
    """404 on pause for a non-existent job name."""
    resp = client.post("/api/dcl/monitor/schedule/nonexistent_job/pause")
    assert resp.status_code == 404, resp.text


def test_resume_unknown_job_returns_404():
    """404 on resume for a non-existent job name."""
    resp = client.post("/api/dcl/monitor/schedule/nonexistent_job/resume")
    assert resp.status_code == 404, resp.text


# =============================================================================
# Test: schedule list state reflects last_run_at after run-now
# =============================================================================

def test_last_run_at_updated_after_run_now():
    """After run-now, last_run_at in monitor_schedule is non-null."""
    _ingest(_ENTITY, "run1_prop")
    _ingest(_ENTITY, "run2_prop")
    client.post("/api/dcl/monitor/schedule/structural_drift/run-now")

    sched = client.get("/api/dcl/monitor/schedule").json()
    job = next(j for j in sched["jobs"] if j["job_name"] == "structural_drift")
    assert job["last_run_at"] is not None, "last_run_at must be set after run-now"
    assert job["last_status"] == "ok", f"Expected last_status='ok'; got {job['last_status']}"
