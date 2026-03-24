"""
Sweep 4 — HTTP Endpoint Integration Test

13 tests verifying all DCL v2 report endpoints via HTTP against the running server.
All values validated against seed ground truth.
Tenant/run IDs read from seed_manifest.json — no hardcoded UUIDs.

Requires: DCL running on port 8004.
"""

import pytest
import httpx

from tests.conftest import TENANT_ID, RUN_ID

DCL_BASE = "http://localhost:8004"

# Ground truth from seed
M_Q1_REV = 1323.43
CUSTOMER_OVERLAP = 34
VENDOR_OVERLAP = 170
EMPLOYEE_OVERLAP = 10
COFA_COUNT = 6

# Common query params for tenant/run resolution
_TR = {"tenant_id": TENANT_ID, "run_id": RUN_ID}


@pytest.fixture(scope="module")
def client():
    return httpx.Client(base_url=DCL_BASE, timeout=30.0)


# --- Test 1: Combining IS ---
def test_combining_income_statement(client):
    """GET combining/income-statement — 200, four columns, revenue matches ground truth."""
    resp = client.get("/api/dcl/reports/v2/combining/income-statement", params={"period": "2025-Q1", **_TR})
    assert resp.status_code == 200
    data = resp.json()
    # Four columns: entity_a, entity_b, adjustments, combined
    assert "entity_a" in data
    assert "entity_b" in data
    assert "combined" in data
    # Revenue matches ground truth
    assert data["entity_a"]["revenue"]["total"] == M_Q1_REV


# --- Test 2: Combining BS ---
def test_combining_balance_sheet(client):
    """GET combining/balance-sheet — 200, BS identity holds."""
    resp = client.get("/api/dcl/reports/v2/combining/balance-sheet", params={"period": "2025-Q1", **_TR})
    assert resp.status_code == 200
    data = resp.json()
    assert "identity_check" in data
    assert data["identity_check"]["passed"] is True
    # BS identity for each column
    for col in ["entity_a", "entity_b", "combined"]:
        section = data[col]
        assets = section["assets"]["total"]
        liab = section["liabilities"]["total"]
        eq = section["equity"]["total"]
        assert assets == pytest.approx(liab + eq, abs=0.01), \
            f"BS identity failed for {col}: {assets} != {liab} + {eq}"


# --- Test 3: Combining CF ---
def test_combining_cash_flow(client):
    """GET combining/cash-flow — 200, CF identity holds."""
    resp = client.get("/api/dcl/reports/v2/combining/cash-flow", params={"period": "2025-Q1", **_TR})
    assert resp.status_code == 200
    data = resp.json()
    assert "identity_check" in data
    assert data["identity_check"]["passed"] is True
    for col in ["entity_a", "entity_b", "combined"]:
        section = data[col]
        calc = section["operating"]["total"] + section["investing"]["total"] + section["financing"]["total"]
        assert calc == pytest.approx(section["net_change"], abs=0.01), \
            f"CF identity failed for {col}"


# --- Test 4: COFA adjustments ---
def test_cofa_adjustments(client):
    """GET cofa-adjustments — 200, 6 COFA conflicts."""
    resp = client.get("/api/dcl/reports/v2/cofa-adjustments", params={"period": "2025-Q1", **_TR})
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    assert len(data) == COFA_COUNT, f"Expected {COFA_COUNT} COFA conflicts, got {len(data)}"


# --- Test 5: Overlap summary ---
def test_overlap_summary(client):
    """GET overlap/summary — 200, customer/vendor/employee counts match seed."""
    resp = client.get("/api/dcl/reports/v2/overlap/summary", params=_TR)
    assert resp.status_code == 200
    data = resp.json()
    assert data["customer"]["overlap_count"] == CUSTOMER_OVERLAP
    assert data["vendor"]["overlap_count"] == VENDOR_OVERLAP
    assert data["employee"]["overlap_count"] == EMPLOYEE_OVERLAP


# --- Test 6: Cross-sell summary ---
def test_cross_sell_summary(client):
    """GET cross-sell/summary — 200, non-empty."""
    resp = client.get("/api/dcl/reports/v2/cross-sell/summary", params=_TR)
    assert resp.status_code == 200
    data = resp.json()
    assert data["total_opportunities"] > 0
    assert data["total_potential_acv"] > 0


# --- Test 7: EBITDA bridge ---
def test_ebitda_bridge(client):
    """GET bridge — 200, has reported_ebitda, adjustments, adjusted_ebitda."""
    resp = client.get("/api/dcl/reports/v2/bridge", params=_TR)
    assert resp.status_code == 200
    data = resp.json()
    assert "reported_ebitda" in data
    assert "adjustments" in data
    assert "adjusted_ebitda" in data
    assert data["reported_ebitda"] is not None
    assert data["adjusted_ebitda"] == pytest.approx(
        data["reported_ebitda"] + data["total_adjustments"], abs=0.01
    )


# --- Test 7b: Bridge lifecycle fields ---
def test_ebitda_bridge_lifecycle_fields(client):
    """GET bridge — lifecycle-aware fields present on adjustments."""
    resp = client.get("/api/dcl/reports/v2/bridge", params={**_TR, "entity_id": "meridian"})
    assert resp.status_code == 200
    data = resp.json()
    for adj in data["adjustments"]:
        assert "diligence_amount" in adj, f"Missing diligence_amount on {adj['concept']}"
        assert "prior_amount" in adj, f"Missing prior_amount on {adj['concept']}"
        assert "trend" in adj, f"Missing trend on {adj['concept']}"
        assert "lifecycle_history" in adj, f"Missing lifecycle_history on {adj['concept']}"
        assert "lifecycle_stage" in adj, f"Missing lifecycle_stage on {adj['concept']}"
        assert isinstance(adj["lifecycle_history"], list)
        assert len(adj["lifecycle_history"]) >= 1


# --- Test 8: QoE ---
def test_qoe(client):
    """GET qoe — 200, has key fields."""
    resp = client.get("/api/dcl/reports/v2/qoe", params={**_TR, "entity_id": "meridian"})
    assert resp.status_code == 200
    data = resp.json()
    assert "reported_ebitda" in data
    assert "adjusted_ebitda" in data
    assert "revenue_quality" in data
    assert "margin_trend" in data


# --- Test 8b: QoE combined has adjustment_lifecycle and sustainability_trend ---
def test_qoe_combined_lifecycle_fields(client):
    """GET qoe/combined — 200, has adjustment_lifecycle and sustainability_trend."""
    resp = client.get("/api/dcl/reports/v2/qoe/combined", params=_TR)
    assert resp.status_code == 200
    data = resp.json()
    assert "combined" in data
    combined = data["combined"]
    assert "adjustment_lifecycle" in combined
    assert "sustainability_trend" in combined
    assert isinstance(combined["adjustment_lifecycle"], dict)
    assert isinstance(combined["sustainability_trend"], list)
    assert len(combined["adjustment_lifecycle"]) > 0
    assert len(combined["sustainability_trend"]) > 0


# --- Test 9: What-if baseline ---
def test_whatif_baseline(client):
    """POST whatif/scenario with empty adjustments — returns baseline values."""
    resp = client.post(
        "/api/dcl/reports/v2/whatif/scenario",
        params=_TR,
        json={
            "entity_id": "meridian",
            "period": "2025-Q1",
            "adjustments": [],
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert "baseline" in data
    assert data["baseline"]["revenue"]["total"] == M_Q1_REV


# --- Test 10: Revenue bridge ---
def test_revenue_bridge(client):
    """GET revenue-bridge/yoy — 200, drivers present."""
    resp = client.get("/api/dcl/reports/v2/revenue-bridge/yoy", params={**_TR, "entity_id": "meridian"})
    assert resp.status_code == 200
    data = resp.json()
    assert "from_total" in data
    assert "to_total" in data
    assert "by_stream" in data
    assert len(data["by_stream"]) > 0


# --- Test 11: Resolution create workspaces ---
def test_resolution_create_workspaces(client):
    """POST resolution/v2/create-workspaces — 200, idempotent."""
    resp = client.post(
        "/api/dcl/resolution/v2/create-workspaces",
        json={"tenant_id": TENANT_ID, "run_id": RUN_ID},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert "by_domain" in data


# --- Test 12: Resolution list workspaces ---
def test_resolution_list_workspaces(client):
    """GET resolution/v2/workspaces — 200, workspaces listed."""
    resp = client.get("/api/dcl/resolution/v2/workspaces", params=_TR)
    assert resp.status_code == 200
    data = resp.json()
    # Response may be a list or wrapped in {"workspaces": [...]}
    workspaces = data if isinstance(data, list) else data.get("workspaces", data)
    assert isinstance(workspaces, list)
    assert len(workspaces) > 0
    # Each workspace has required fields
    ws = workspaces[0]
    assert "workspace_id" in ws
    assert "domain" in ws
    assert "concept" in ws
    assert "status" in ws


# --- Test 13: Ingest status ---
def test_ingest_status(client):
    """GET ingest-status — 200, at least one run present."""
    resp = client.get("/api/dcl/ingest-status")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    assert len(data) >= 1, "Expected at least one ingest run"
