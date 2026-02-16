"""
Tests for the DCL Ingest Guard (schema-on-write validation).

Covers the Trifecta architecture's core invariant:
  Content (Farm /ingest) must match Structure (AAM /export-pipes) on pipe_id.

Test matrix:
  1. export-pipes → ingest with matching pipe_id → 200 with matched_schema=true
  2. ingest with WRONG pipe_id when store has definitions → 422 NO_MATCHING_PIPE
  3. ingest when store is empty (no export-pipes called) → 200 (guard bypassed)
  4. export-pipes with empty/invalid payload → 400
  5. IngestResponse contains enriched fields (dcl_run_id, schema_fields, timestamp)
"""

import pytest
from fastapi.testclient import TestClient

from backend.api.pipe_store import get_pipe_store


@pytest.fixture(autouse=True)
def _clean_pipe_store():
    """Ensure each test starts with a clean pipe store."""
    store = get_pipe_store()
    store.clear()
    yield
    store.clear()


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

def test_export_then_ingest_matching_pipe(client):
    """Structure + Content with same pipe_id → 200 with matched_schema=true."""
    # Step 1: Register pipe definitions (Path 1 — Structure)
    resp = client.post("/api/dcl/export-pipes", json=EXPORT_PAYLOAD)
    assert resp.status_code == 200
    export_data = resp.json()
    assert export_data["status"] == "registered"
    assert export_data["pipes_registered"] == 2
    assert "sf-crm-001" in export_data["pipe_ids"]
    assert "ns-erp-001" in export_data["pipe_ids"]

    # Step 2: Push data with matching pipe_id (Path 3 — Content)
    resp = client.post(
        "/api/dcl/ingest",
        json=INGEST_PAYLOAD,
        headers={"x-pipe-id": "sf-crm-001", "x-run-id": "test-run-001"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ingested"
    assert data["matched_schema"] is True
    assert data["schema_fields"] == ["id", "email", "revenue", "account_name"]
    assert data["pipe_id"] == "sf-crm-001"
    assert data["rows_accepted"] == 2
    assert data["dcl_run_id"] == "test-run-001"
    assert data["timestamp"] != ""


# ---------------------------------------------------------------------------
# Test 2: Rejection — ingest with WRONG pipe_id → 422
# ---------------------------------------------------------------------------

def test_ingest_wrong_pipe_id_rejected(client):
    """Content with unknown pipe_id when definitions exist → 422."""
    # Register definitions first
    client.post("/api/dcl/export-pipes", json=EXPORT_PAYLOAD)

    # Push with non-existent pipe_id
    resp = client.post(
        "/api/dcl/ingest",
        json=INGEST_PAYLOAD,
        headers={"x-pipe-id": "nonexistent-pipe-999", "x-run-id": "test-run-002"},
    )
    assert resp.status_code == 422
    data = resp.json()
    detail = data["detail"]
    assert detail["error"] == "NO_MATCHING_PIPE"
    assert detail["pipe_id"] == "nonexistent-pipe-999"
    assert "sf-crm-001" in detail["available_pipes"]
    assert "ns-erp-001" in detail["available_pipes"]
    assert detail["timestamp"] != ""


# ---------------------------------------------------------------------------
# Test 3: Backward compat — ingest when no definitions registered → 200
# ---------------------------------------------------------------------------

def test_ingest_no_definitions_guard_bypassed(client):
    """No export-pipes called → guard bypassed, ingest succeeds."""
    # No export-pipes call — store is empty
    resp = client.post(
        "/api/dcl/ingest",
        json=INGEST_PAYLOAD,
        headers={"x-pipe-id": "any-pipe", "x-run-id": "test-run-003"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ingested"
    # matched_schema is false because no definition exists
    assert data["matched_schema"] is False
    assert data["schema_fields"] == []
    assert data["rows_accepted"] == 2


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

def test_ingest_response_has_enriched_fields(client):
    """IngestResponse includes dcl_run_id, matched_schema, schema_fields, timestamp."""
    client.post("/api/dcl/export-pipes", json=EXPORT_PAYLOAD)

    resp = client.post(
        "/api/dcl/ingest",
        json=INGEST_PAYLOAD,
        headers={"x-pipe-id": "ns-erp-001", "x-run-id": "enriched-run"},
    )
    assert resp.status_code == 200
    data = resp.json()

    # All enriched fields must be present
    assert "dcl_run_id" in data
    assert "matched_schema" in data
    assert "schema_fields" in data
    assert "timestamp" in data

    # Schema fields should come from the ns-erp-001 definition
    assert data["matched_schema"] is True
    assert set(data["schema_fields"]) == {"invoice_id", "amount", "currency", "date"}


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

def test_guard_activates_for_all_pipes(client):
    """Once even one pipe definition exists, ALL unregistered pipe_ids are rejected."""
    # Register only sf-crm-001
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
                        "fields": ["id"],
                        "health": "healthy",
                    },
                ],
            },
        ],
    }
    client.post("/api/dcl/export-pipes", json=single_export)

    # sf-crm-001 should work
    resp = client.post(
        "/api/dcl/ingest",
        json=INGEST_PAYLOAD,
        headers={"x-pipe-id": "sf-crm-001", "x-run-id": "run-ok"},
    )
    assert resp.status_code == 200

    # Any other pipe_id should be rejected
    resp = client.post(
        "/api/dcl/ingest",
        json=INGEST_PAYLOAD,
        headers={"x-pipe-id": "ns-erp-001", "x-run-id": "run-fail"},
    )
    assert resp.status_code == 422
    assert resp.json()["detail"]["error"] == "NO_MATCHING_PIPE"
