"""
Tests for the SOR (Systems of Record) pipeline chain.

Verifies the AOD-authoritative SOR flow through the 3-phase pipeline:
  1. export-pipes stores systems_of_record from AAM payload
  2. Ingest reads AOD SOR count from PipeStore (not app.state)
  3. Reconciliation reflects AOD authority
  4. Warnings surface in HTTP response bodies (not just logs)
  5. Old receipts without systems_of_record field deserialize safely

These tests complement test_ingest_guard.py (which covers pipe_id matching)
with SOR-specific behavior added in the SOR count fix commits.
"""

import os

import pytest
from fastapi.testclient import TestClient

from backend.api.pipe_store import get_pipe_store, ExportReceipt
from backend.api.ingest import get_ingest_store
from backend.core.mode_state import set_current_mode


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
    """Ensure each test starts with clean pipe store and ingest store."""
    pipe_store = get_pipe_store()
    pipe_store.clear()
    ingest_store = get_ingest_store()
    ingest_store.reset()
    yield
    pipe_store.clear()
    ingest_store.reset()


@pytest.fixture()
def client():
    from backend.api.main import app
    return TestClient(app)


# ---------------------------------------------------------------------------
# Test payloads
# ---------------------------------------------------------------------------

SOR_DECLARATIONS = [
    {"domain": "crm", "vendor": "Salesforce", "category": "crm", "confidence": "high", "source": "farm"},
    {"domain": "erp", "vendor": "NetSuite", "category": "erp", "confidence": "high", "source": "farm"},
    {"domain": "erp", "vendor": "SAP", "category": "erp", "confidence": "high", "source": "farm"},
    {"domain": "finops", "vendor": "Stripe", "category": "finops", "confidence": "high", "source": "farm"},
    {"domain": "infra", "vendor": "AWS", "category": "infra", "confidence": "high", "source": "farm"},
    {"domain": "hr", "vendor": "Workday", "category": "hr", "confidence": "high", "source": "farm"},
]

EXPORT_PAYLOAD_WITH_SOR = {
    "aod_run_id": "test-sor-001",
    "timestamp": "2026-02-20T00:00:00Z",
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
                    "fields": ["id", "email", "revenue"],
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
                    "fields": ["invoice_id", "amount", "currency"],
                    "health": "healthy",
                    "asset_key": "ns-erp",
                },
            ],
        },
    ],
    "systems_of_record": SOR_DECLARATIONS,
}

EXPORT_PAYLOAD_NO_SOR = {
    "aod_run_id": "test-sor-002",
    "timestamp": "2026-02-20T00:00:00Z",
    "source": "aam",
    "total_connections": 1,
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
                    "fields": ["id", "email"],
                    "health": "healthy",
                    "asset_key": "sf-crm",
                },
            ],
        },
    ],
    "systems_of_record": [],
}

INGEST_PAYLOAD = {
    "source_system": "salesforce",
    "tenant_id": "test-tenant",
    "snapshot_name": "test-sor-snapshot",
    "run_timestamp": "2026-02-20T00:00:00Z",
    "schema_version": "1.0",
    "row_count": 2,
    "rows": [
        {"id": "001", "email": "a@test.com", "revenue": 100},
        {"id": "002", "email": "b@test.com", "revenue": 200},
    ],
}


# ---------------------------------------------------------------------------
# Test 1: export-pipes with systems_of_record stores them in receipt
# ---------------------------------------------------------------------------

def test_export_pipes_stores_systems_of_record(client):
    """POST /export-pipes with 6 SOR entries → receipt stores them, get_aod_systems_of_record returns 6."""
    resp = client.post("/api/dcl/export-pipes", json=EXPORT_PAYLOAD_WITH_SOR)
    assert resp.status_code == 200
    data = resp.json()

    # Response reflects AOD SOR count
    assert data["aod_sor_count"] == 6
    assert data["warnings"] == []  # no warnings when SORs present

    # PipeStore has the SOR list
    pipe_store = get_pipe_store()
    receipts = pipe_store.get_export_receipts()
    assert len(receipts) >= 1
    latest = receipts[-1]
    assert latest.systems_of_record is not None
    assert len(latest.systems_of_record) == 6

    # get_aod_systems_of_record() returns the list
    aod_sors = pipe_store.get_aod_systems_of_record()
    assert len(aod_sors) == 6
    vendors = sorted(s["vendor"] for s in aod_sors)
    assert vendors == ["AWS", "NetSuite", "SAP", "Salesforce", "Stripe", "Workday"]


# ---------------------------------------------------------------------------
# Test 2: export-pipes with empty SOR → warning in response
# ---------------------------------------------------------------------------

def test_export_pipes_empty_sor_warns(client):
    """POST /export-pipes with 0 systems_of_record → response has warning, SOR count is 0."""
    resp = client.post("/api/dcl/export-pipes", json=EXPORT_PAYLOAD_NO_SOR)
    assert resp.status_code == 200
    data = resp.json()

    assert data["aod_sor_count"] == 0
    assert len(data["warnings"]) == 1
    assert "SOR count is 0" in data["warnings"][0]
    assert "No AOD SOR declarations" in data["warnings"][0]

    # PipeStore confirms empty SOR list
    pipe_store = get_pipe_store()
    assert pipe_store.get_aod_systems_of_record() == []


# ---------------------------------------------------------------------------
# Test 3: content ingest uses AOD SOR count from PipeStore
# ---------------------------------------------------------------------------

def test_content_ingest_uses_aod_sor_count(client):
    """POST export-pipes with 6 SORs, then ingest → activity entry has sors=6."""
    # Structure phase
    resp = client.post("/api/dcl/export-pipes", json=EXPORT_PAYLOAD_WITH_SOR)
    assert resp.status_code == 200

    # Content phase
    resp = client.post(
        "/api/dcl/ingest",
        json=INGEST_PAYLOAD,
        headers=_ingest_headers(**{"x-pipe-id": "sf-crm-001", "x-run-id": "test-sor-run-001"}),
    )
    assert resp.status_code == 200
    ingest_data = resp.json()
    assert ingest_data["status"] == "ingested"
    assert ingest_data["warnings"] == []  # no warnings when SORs present

    # Verify activity log: content entry has sors=6
    store = get_ingest_store()
    activity = store.get_activity_log()
    content_entries = [e for e in activity if e["phase"] == "content"]
    assert len(content_entries) >= 1
    assert content_entries[0]["sors"] == 6  # AOD-authoritative count


# ---------------------------------------------------------------------------
# Test 4: content before structure → warning in response
# ---------------------------------------------------------------------------

def test_content_before_structure_warns(client):
    """Ingest with no prior export-pipes → response includes broken-sequence warning.

    Uses farm_ run_id prefix to bypass the canonical source gate (Farm
    self-directed pushes are the realistic scenario for this race condition).
    """
    # No export-pipes call — pipe store is empty
    resp = client.post(
        "/api/dcl/ingest",
        json=INGEST_PAYLOAD,
        headers=_ingest_headers(**{"x-pipe-id": "any-pipe", "x-run-id": "farm_sor-run-002"}),
    )
    assert resp.status_code == 200
    data = resp.json()

    # Should have the broken-sequence warning
    assert len(data["warnings"]) >= 1
    assert any("3-phase sequence is broken" in w for w in data["warnings"])


# ---------------------------------------------------------------------------
# Test 5: old receipt deserialization without systems_of_record field
# ---------------------------------------------------------------------------

def test_old_receipt_deserialization_without_sor_field():
    """ExportReceipt created without systems_of_record → defaults to None, get_aod handles it."""
    # Simulate an old receipt that was serialized before the systems_of_record field existed
    old_receipt_data = {
        "aod_run_id": "old-run-001",
        "source": "aam",
        "total_connections": 5,
        "pipe_ids": ["pipe-a", "pipe-b"],
        "received_at": "2026-01-15T00:00:00Z",
        "snapshot_name": "old-snapshot",
        # Note: no systems_of_record key at all
    }

    receipt = ExportReceipt(**old_receipt_data)
    assert receipt.systems_of_record is None  # default

    # Manually inject the old receipt into the pipe store
    pipe_store = get_pipe_store()
    with pipe_store._lock:
        pipe_store._export_receipts.append(receipt)

    # get_aod_systems_of_record() should handle None gracefully → return []
    aod_sors = pipe_store.get_aod_systems_of_record()
    assert aod_sors == []


# ---------------------------------------------------------------------------
# Test 6: reconciliation shows AOD authority
# ---------------------------------------------------------------------------

def test_reconciliation_shows_aod_authority(client):
    """After export-pipes + ingest → cross-system reconciliation returns aod_authority.sor_count=6.

    The aod_authority block lives on the /api/dcl/reconciliation/cross-system
    endpoint (the unified stats view), not the per-dispatch reconciliation.
    """
    # Structure phase with 6 SOR declarations
    resp = client.post("/api/dcl/export-pipes", json=EXPORT_PAYLOAD_WITH_SOR)
    assert resp.status_code == 200

    # Content phase
    resp = client.post(
        "/api/dcl/ingest",
        json=INGEST_PAYLOAD,
        headers=_ingest_headers(**{"x-pipe-id": "sf-crm-001", "x-run-id": "test-sor-run-003"}),
    )
    assert resp.status_code == 200

    # Cross-system reconciliation (reads from PipeStore, no AAM_URL needed)
    resp = client.get("/api/dcl/reconciliation/cross-system")
    assert resp.status_code == 200
    recon = resp.json()

    # aod_authority block must exist with correct SOR count
    assert "aod_authority" in recon
    assert recon["aod_authority"]["sor_count"] == 6
    assert len(recon["aod_authority"]["systems_of_record"]) == 6
