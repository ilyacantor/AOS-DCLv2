"""
Tests for the DCL Ingest Guard and endpoint deprecation.

POST /api/dcl/ingest is deprecated (410 Gone) — Farm now pushes semantic
triples to POST /api/dcl/ingest-triples. Tests 1-3, 5, 7 verify the
deprecated endpoint returns 410. Tests 4 and 6 cover export-pipes (unchanged).
"""

import os

import pytest
from fastapi.testclient import TestClient

from backend.api.pipe_store import get_pipe_store
from backend.api.ingest import get_ingest_store


def _ingest_headers(**extra) -> dict:
    """Build ingest headers including x-api-key from env."""
    key = os.environ.get("DCL_INGEST_KEY", "")
    headers = {}
    if key:
        headers["x-api-key"] = key
    headers.update(extra)
    return headers


@pytest.fixture(autouse=True)
def _clean_stores():
    """Ensure each test starts with clean pipe and ingest stores."""
    pipe_store = get_pipe_store()
    ingest_store = get_ingest_store()
    pipe_store.reset()
    ingest_store.reset()
    yield
    pipe_store.reset()
    ingest_store.reset()


@pytest.fixture()
def client():
    from backend.api.main import app
    return TestClient(app)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

EXPORT_PAYLOAD = {
    "aod_run_id": "test-aod-001",
    "timestamp": "2026-02-16T00:00:00Z",
    "source": "aam",
    "total_connections": 2,
    "fabric_planes": [
        {
            "plane_type": "crm",
            "vendor": "salesforce",
            "connection_count": 1,
            "health": "healthy",
            "connections": [
                {
                    "pipe_id": "sf-crm-001",
                    "candidate_id": "cand-001",
                    "source_name": "Salesforce CRM",
                    "vendor": "salesforce",
                    "category": "crm",
                    "fields": ["id", "email", "revenue", "account_name"],
                    "health": "healthy",
                    "asset_key": "sf-crm",
                },
            ],
        },
        {
            "plane_type": "erp",
            "vendor": "netsuite",
            "connection_count": 1,
            "health": "healthy",
            "connections": [
                {
                    "pipe_id": "ns-erp-001",
                    "candidate_id": "cand-002",
                    "source_name": "NetSuite ERP",
                    "vendor": "netsuite",
                    "category": "erp",
                    "fields": ["invoice_id", "amount", "currency", "date"],
                    "health": "healthy",
                    "asset_key": "ns-erp",
                },
            ],
        },
    ],
}

INGEST_PAYLOAD = {
    "source_system": "salesforce",
    "tenant_id": "test-tenant",
    "snapshot_name": "test-snapshot",
    "run_timestamp": "2026-02-16T00:00:00Z",
    "schema_version": "1.0",
    "row_count": 2,
    "rows": [
        {"id": "001", "email": "a@test.com", "revenue": 100},
        {"id": "002", "email": "b@test.com", "revenue": 200},
    ],
}


# ---------------------------------------------------------------------------
# Test 1: Happy path — export then ingest with matching pipe_id
# ---------------------------------------------------------------------------

def test_export_then_ingest_deprecated(client):
    """POST /api/dcl/ingest is deprecated — returns 410 Gone."""
    # export-pipes still works
    resp = client.post("/api/dcl/export-pipes", json=EXPORT_PAYLOAD)
    assert resp.status_code == 200
    export_data = resp.json()
    assert export_data["status"] == "accepted"
    assert export_data["pipes_registered"] == 2

    # POST /api/dcl/ingest now returns 410
    resp = client.post(
        "/api/dcl/ingest",
        json=INGEST_PAYLOAD,
        headers=_ingest_headers(**{"x-pipe-id": "sf-crm-001", "x-run-id": "test-run-001"}),
    )
    assert resp.status_code == 410
    assert "deprecated" in resp.json()["detail"].lower()


# ---------------------------------------------------------------------------
# Test 2: Rejection — ingest with WRONG pipe_id → 422
# ---------------------------------------------------------------------------

def test_ingest_wrong_pipe_id_deprecated(client):
    """POST /api/dcl/ingest returns 410 regardless of pipe_id."""
    client.post("/api/dcl/export-pipes", json=EXPORT_PAYLOAD)

    resp = client.post(
        "/api/dcl/ingest",
        json=INGEST_PAYLOAD,
        headers=_ingest_headers(**{"x-pipe-id": "nonexistent-pipe-999", "x-run-id": "test-run-002"}),
    )
    assert resp.status_code == 410


# ---------------------------------------------------------------------------
# Test 3: Backward compat — ingest when no definitions registered → 200
# ---------------------------------------------------------------------------

def test_ingest_no_definitions_deprecated(client):
    """POST /api/dcl/ingest returns 410 even with no definitions registered."""
    resp = client.post(
        "/api/dcl/ingest",
        json=INGEST_PAYLOAD,
        headers=_ingest_headers(**{"x-pipe-id": "any-pipe", "x-run-id": "farm_test-run-003"}),
    )
    assert resp.status_code == 410


# ---------------------------------------------------------------------------
# Test 4: export-pipes validation — empty payload rejected
# ---------------------------------------------------------------------------

def test_export_no_connections_rejected(client):
    """Export with no valid connections → 400."""
    resp = client.post(
        "/api/dcl/export-pipes",
        json={
            "source": "aam",
            "total_connections": 0,
            "fabric_planes": [],
        },
    )
    assert resp.status_code == 400
    assert resp.json()["detail"]["error"] == "NO_PIPE_DEFINITIONS"


def test_export_connection_without_pipe_id_rejected(client):
    """Connections with empty pipe_id are skipped; if all skip → 400."""
    resp = client.post(
        "/api/dcl/export-pipes",
        json={
            "source": "aam",
            "total_connections": 1,
            "fabric_planes": [
                {
                    "plane_type": "crm",
                    "vendor": "test",
                    "connections": [
                        {
                            "pipe_id": "",
                            "source_name": "Bad Connection",
                            "fields": [],
                        },
                    ],
                },
            ],
        },
    )
    assert resp.status_code == 400


# ---------------------------------------------------------------------------
# Test 5: Enriched response fields present
# ---------------------------------------------------------------------------

def test_ingest_response_is_410(client):
    """POST /api/dcl/ingest returns 410 with deprecation message."""
    client.post("/api/dcl/export-pipes", json=EXPORT_PAYLOAD)

    resp = client.post(
        "/api/dcl/ingest",
        json=INGEST_PAYLOAD,
        headers=_ingest_headers(**{"x-pipe-id": "ns-erp-001", "x-run-id": "enriched-run"}),
    )
    assert resp.status_code == 410
    assert "ingest-triples" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# Test 6: GET /api/dcl/export-pipes lists registered definitions
# ---------------------------------------------------------------------------

def test_list_pipe_definitions(client):
    """GET /api/dcl/export-pipes returns all registered pipes."""
    # Start empty
    resp = client.get("/api/dcl/export-pipes")
    assert resp.status_code == 200
    assert resp.json()["pipe_count"] == 0

    # Register
    client.post("/api/dcl/export-pipes", json=EXPORT_PAYLOAD)

    # Now should list 2
    resp = client.get("/api/dcl/export-pipes")
    data = resp.json()
    assert data["pipe_count"] == 2
    pipe_ids = [p["pipe_id"] for p in data["pipes"]]
    assert "sf-crm-001" in pipe_ids
    assert "ns-erp-001" in pipe_ids


# ---------------------------------------------------------------------------
# Test 7: Second export overwrites existing definitions
# ---------------------------------------------------------------------------

def test_export_overwrites_definitions(client):
    """A second export-pipes call updates existing definitions."""
    client.post("/api/dcl/export-pipes", json=EXPORT_PAYLOAD)
    store = get_pipe_store()
    assert store.count() == 2

    # Push a new export with different pipes
    new_export = {
        "source": "aam",
        "total_connections": 1,
        "fabric_planes": [
            {
                "plane_type": "warehouse",
                "vendor": "snowflake",
                "connections": [
                    {
                        "pipe_id": "sf-crm-001",
                        "source_name": "Salesforce Updated",
                        "vendor": "salesforce",
                        "fields": ["id", "email", "revenue", "new_field"],
                        "health": "healthy",
                    },
                ],
            },
        ],
    }
    resp = client.post("/api/dcl/export-pipes", json=new_export)
    assert resp.status_code == 200

    # sf-crm-001 should be updated, ns-erp-001 should still exist
    assert store.count() == 2
    updated = store.lookup("sf-crm-001")
    assert "new_field" in updated.fields
    assert updated.source_name == "Salesforce Updated"


# ---------------------------------------------------------------------------
# Test 8: Guard activates for ALL pipes once any definition exists
# ---------------------------------------------------------------------------

def test_guard_activates_for_all_pipes_deprecated(client):
    """POST /api/dcl/ingest returns 410 regardless of registered pipes."""
    single_export = {
        "source": "aam",
        "total_connections": 1,
        "fabric_planes": [
            {
                "plane_type": "crm",
                "vendor": "salesforce",
                "connections": [
                    {
                        "pipe_id": "sf-crm-001",
                        "source_name": "Salesforce",
                        "vendor": "salesforce",
                        "fields": ["id"],
                        "health": "healthy",
                    },
                ],
            },
        ],
    }
    resp = client.post("/api/dcl/export-pipes", json=single_export)
    assert resp.status_code == 200

    # Both matching and non-matching pipe_ids get 410
    for pipe_id in ["sf-crm-001", "ns-erp-001"]:
        resp = client.post(
            "/api/dcl/ingest",
            json=INGEST_PAYLOAD,
            headers=_ingest_headers(**{"x-pipe-id": pipe_id, "x-run-id": f"run-{pipe_id}"}),
        )
        assert resp.status_code == 410
