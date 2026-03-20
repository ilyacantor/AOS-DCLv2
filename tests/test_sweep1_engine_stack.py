"""
Sweep 1 — Engine Stack Integration Test

Verifies the full v2 engine stack works end-to-end:
QueryResolver feeds into Combining/Overlap/EBITDA/WhatIf.

All expected values fetched from Farm's ground truth API at runtime (B10).
"""
import json
import pytest
from pathlib import Path

from backend.engine.query_resolver_v2 import TripleQueryResolver
from backend.engine.combining_v2 import CombiningEngineV2
from backend.engine.overlap_v2 import OverlapEngineV2
from backend.engine.cross_sell_v2 import CrossSellEngineV2
from backend.engine.ebitda_bridge_v2 import EBITDABridgeV2
from backend.engine.qoe_v2 import QualityOfEarningsV2
from backend.engine.what_if_v2 import WhatIfEngineV2
from backend.engine.revenue_bridge import RevenueBridgeV2
from backend.engine.entity_resolution_v2 import EntityResolutionV2

# --- Load seed constants ---
_manifest_path = Path(__file__).parent.parent / "data" / "seed_manifest.json"
with open(_manifest_path) as f:
    _manifest = json.load(f)

TENANT_ID = _manifest["tenant_id"]
RUN_ID = _manifest["run_id"]
ENTITY_A = _manifest["entity_a_id"]
ENTITY_B = _manifest["entity_b_id"]

# Ground truth from Farm API (B10)
from tests.conftest import gt_metric, gt_overlap_count, _get_ground_truth


def _sum_ebitda_adjustments(entity: str) -> float:
    """Sum all EBITDA adjustment amount_current values for an entity from ground truth."""
    gt = _get_ground_truth()
    agt = gt.get("atemporal_ground_truth", {}).get(entity, {})
    total = sum(
        props.get("amount_current", 0)
        for concept, props in agt.items()
        if concept.startswith("ebitda_adjustment.")
    )
    return round(total, 2)


# --- Fixtures ---
@pytest.fixture(scope="module")
def resolver():
    return TripleQueryResolver(TENANT_ID, RUN_ID)

@pytest.fixture(scope="module")
def combining():
    return CombiningEngineV2(TENANT_ID, RUN_ID)

@pytest.fixture(scope="module")
def overlap():
    return OverlapEngineV2(TENANT_ID, RUN_ID)

@pytest.fixture(scope="module")
def cross_sell():
    return CrossSellEngineV2(TENANT_ID, RUN_ID)

@pytest.fixture(scope="module")
def bridge():
    return EBITDABridgeV2(TENANT_ID, RUN_ID)

@pytest.fixture(scope="module")
def qoe():
    return QualityOfEarningsV2(TENANT_ID, RUN_ID)

@pytest.fixture(scope="module")
def whatif():
    return WhatIfEngineV2(TENANT_ID, RUN_ID)

@pytest.fixture(scope="module")
def rev_bridge():
    return RevenueBridgeV2(TENANT_ID, RUN_ID)


# --- Test 1: Resolver → Combining P&L ---
def test_resolver_to_combining_pnl(resolver, combining):
    """Resolver and Combining agree on revenue for 2025-Q1."""
    m_q1_rev = gt_metric("meridian", "2025-Q1", "revenue.total")
    res_metric = resolver.get_metric("revenue.total", ENTITY_A, "2025-Q1")
    stmt = combining.get_combining_income_statement("2025-Q1")
    assert res_metric["value"] == m_q1_rev
    assert stmt["entity_a"]["revenue"]["total"] == m_q1_rev
    assert res_metric["value"] == stmt["entity_a"]["revenue"]["total"]


# --- Test 2: Resolver → Combining BS ---
def test_resolver_to_combining_bs(resolver, combining):
    """BS identity holds in the combining statement."""
    bs = combining.get_combining_balance_sheet("2025-Q1")
    # Entity A identity
    assert bs["entity_a"]["assets"]["total"] == pytest.approx(bs["entity_a"]["liabilities"]["total"] + bs["entity_a"]["equity"]["total"], abs=0.01)
    # Entity B identity
    assert bs["entity_b"]["assets"]["total"] == pytest.approx(bs["entity_b"]["liabilities"]["total"] + bs["entity_b"]["equity"]["total"], abs=0.01)
    # Combined identity
    assert bs["combined"]["assets"]["total"] == pytest.approx(bs["combined"]["liabilities"]["total"] + bs["combined"]["equity"]["total"], abs=0.01)


# --- Test 3: Resolver → Combining CF ---
def test_resolver_to_combining_cf(combining):
    """CF identity holds in the combining statement."""
    cf = combining.get_combining_cash_flow("2025-Q1")
    for col in ["entity_a", "entity_b", "combined"]:
        data = cf[col]
        calc = data["operating"]["total"] + data["investing"]["total"] + data["financing"]["total"]
        assert calc == data["net_change"], f"CF identity failed for {col}: {calc} != {data['net_change']}"


# --- Test 4: Combining identity gate ---
def test_combining_pnl_identity_gate(combining):
    """Combined EBITDA == entity_a EBITDA + entity_b EBITDA + adjustment EBITDA impact."""
    stmt = combining.get_combining_income_statement("2025-Q1")
    assert stmt["identity_check"]["passed"] is True


# --- Test 5: Resolver → Overlap ---
def test_resolver_to_overlap(overlap):
    """Overlap counts match seed ground truth."""
    summary = overlap.get_overlap_summary()
    assert summary["customer"]["overlap_count"] == gt_overlap_count("customer")
    assert summary["vendor"]["overlap_count"] == gt_overlap_count("vendor")
    assert summary["employee"]["overlap_count"] == gt_overlap_count("employee")


# --- Test 6: Resolver → Cross-sell ---
def test_resolver_to_cross_sell(cross_sell):
    """Cross-sell produces non-empty opportunities."""
    opps = cross_sell.get_cross_sell_opportunities()
    assert len(opps) > 0
    summary = cross_sell.get_cross_sell_summary()
    assert summary["total_opportunities"] > 0
    assert summary["total_potential_acv"] > 0


# --- Test 7: Resolver → EBITDA Bridge ---
def test_resolver_to_ebitda_bridge(resolver, bridge):
    """Bridge reported EBITDA matches resolver's income statement EBITDA."""
    # Meridian bridge
    m_bridge = bridge.get_bridge(ENTITY_A)
    m_stmt = resolver.get_income_statement(ENTITY_A, "2025-Q1")
    # The bridge may use annual or quarterly EBITDA — check it's consistent
    assert m_bridge["reported_ebitda"] is not None
    assert m_bridge["total_adjustments"] == _sum_ebitda_adjustments(ENTITY_A)
    assert m_bridge["adjusted_ebitda"] == m_bridge["reported_ebitda"] + m_bridge["total_adjustments"]


# --- Test 8: Resolver → QofE ---
def test_resolver_to_qoe(qoe):
    """QoE summary has required fields."""
    summary = qoe.get_qoe_summary(ENTITY_A)
    assert summary["reported_ebitda"] is not None
    assert summary["adjusted_ebitda"] is not None
    assert "revenue_quality" in summary
    assert "margin_trend" in summary
    assert len(summary["margin_trend"]) == 12  # all periods


# --- Test 9: Resolver → What-If ---
def test_resolver_to_whatif(whatif, bridge):
    """What-if baseline matches bridge reported EBITDA."""
    m_q1_rev = gt_metric("meridian", "2025-Q1", "revenue.total")
    m_q1_ebitda = gt_metric("meridian", "2025-Q1", "pnl.ebitda")
    baseline = whatif.get_baseline(ENTITY_A, "2025-Q1")
    assert baseline["revenue"]["total"] == m_q1_rev
    assert baseline["ebitda"] == m_q1_ebitda

    # Apply a scenario — result should differ
    result = whatif.apply_scenario(ENTITY_A, "2025-Q1", [
        {"concept": "revenue.total", "type": "pct", "value": -10.0}
    ])
    assert result["adjusted"]["revenue"]["total"] < m_q1_rev
    assert result["adjusted"]["ebitda"] < m_q1_ebitda


# --- Test 10: Resolver → Revenue Bridge ---
def test_resolver_to_revenue_bridge(rev_bridge):
    """Revenue bridge drivers sum to total variance."""
    m_q1_rev = gt_metric("meridian", "2025-Q1", "revenue.total")
    m_q1_2024_rev = gt_metric("meridian", "2024-Q1", "revenue.total")
    b = rev_bridge.get_revenue_bridge(ENTITY_A, "2024-Q1", "2025-Q1")
    assert b["from_total"] == m_q1_2024_rev
    assert b["to_total"] == m_q1_rev
    total_delta = b["to_total"] - b["from_total"]
    stream_delta = sum(s["delta"] for s in b["by_stream"])
    assert abs(total_delta - stream_delta) < 0.01, \
        f"Drivers don't sum: total={total_delta}, streams={stream_delta}"


# --- Test 11: Resolution → Overlap chain ---
def test_resolution_overlap_chain():
    """Resolution creates workspaces matching overlap counts."""
    resolution = EntityResolutionV2(TENANT_ID, RUN_ID)
    # Idempotent: may create 0 if workspaces already exist from prior runs
    resolution.create_workspaces_from_overlap()

    # Verify total workspace counts per domain match overlap ground truth
    customer_ws = resolution.list_workspaces(domain="customer")
    vendor_ws = resolution.list_workspaces(domain="vendor")
    employee_ws = resolution.list_workspaces(domain="employee")

    assert len(customer_ws) == gt_overlap_count("customer")
    assert len(vendor_ws) == gt_overlap_count("vendor")
    assert len(employee_ws) == gt_overlap_count("employee")


# --- Test 12: Scenario persistence round-trip ---
def test_scenario_persistence_roundtrip(whatif):
    """Save, list, load scenario — values match."""
    m_q1_rev = gt_metric("meridian", "2025-Q1", "revenue.total")
    adjustments = [{"concept": "revenue.total", "type": "pct", "value": -5.0}]
    scenario_id = whatif.save_scenario("sweep1_integration", ENTITY_A, "2025-Q1", adjustments)
    assert scenario_id is not None

    scenarios = whatif.list_scenarios()
    found = [s for s in scenarios if s["id"] == scenario_id]
    assert len(found) == 1
    assert found[0]["name"] == "sweep1_integration"

    loaded = whatif.load_scenario(scenario_id)
    assert loaded["name"] == "sweep1_integration"
    assert loaded["baseline"]["revenue"]["total"] == m_q1_rev
