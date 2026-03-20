"""
Stage 3E Harness — EBITDA Bridge + Quality of Earnings
Tests bridge construction and QoE analysis from ebitda_adjustment.* triples.
Expected values fetched from Farm's ground truth API at runtime (B10).
"""
import pytest
from backend.engine.ebitda_bridge_v2 import EBITDABridgeV2
from backend.engine.qoe_v2 import QualityOfEarningsV2

from tests.conftest import TENANT_ID, RUN_ID, gt_atemporal


def _sum_ebitda_adjustments(entity: str) -> float:
    """Sum all EBITDA adjustment amount_current values for an entity from ground truth."""
    from tests.conftest import _get_ground_truth
    gt = _get_ground_truth()
    agt = gt.get("atemporal_ground_truth", {}).get(entity, {})
    total = sum(
        props.get("amount_current", 0)
        for concept, props in agt.items()
        if concept.startswith("ebitda_adjustment.")
    )
    return round(total, 2)


@pytest.fixture
def bridge():
    return EBITDABridgeV2(TENANT_ID, RUN_ID)

@pytest.fixture
def qoe():
    return QualityOfEarningsV2(TENANT_ID, RUN_ID)


# --- Test 1: Meridian bridge total ---
def test_meridian_total_adjustments(bridge):
    b = bridge.get_bridge("meridian")
    assert b["total_adjustments"] == _sum_ebitda_adjustments("meridian")

# --- Test 2: Cascadia bridge total ---
def test_cascadia_total_adjustments(bridge):
    b = bridge.get_bridge("cascadia")
    assert b["total_adjustments"] == _sum_ebitda_adjustments("cascadia")

# --- Test 3: Combined bridge total ---
def test_combined_total_adjustments(bridge):
    b = bridge.get_bridge()  # None = combined
    expected = round(_sum_ebitda_adjustments("meridian") + _sum_ebitda_adjustments("cascadia"), 2)
    assert b["total_adjustments"] == expected

# --- Test 4: Adjustment count ---
def test_adjustment_count(bridge):
    b = bridge.get_bridge("meridian")
    assert len(b["adjustments"]) == 8

# --- Test 5: Individual adjustment values ---
def test_meridian_facility_adjustment(bridge):
    b = bridge.get_bridge("meridian")
    facility = next(a for a in b["adjustments"] if "facility" in a["concept"])
    assert facility["amount"] == gt_atemporal("meridian", "ebitda_adjustment.facility_consolidation")

def test_meridian_headcount_adjustment(bridge):
    b = bridge.get_bridge("meridian")
    headcount = next(a for a in b["adjustments"] if "headcount" in a["concept"])
    assert headcount["amount"] == gt_atemporal("meridian", "ebitda_adjustment.headcount_synergies")

# --- Test 6: Lever classification ---
def test_lever_classification(bridge):
    b = bridge.get_bridge("meridian")
    assert "normalization" in b["by_lever"]
    assert "cost_reduction" in b["by_lever"]
    assert "synergy" in b["by_lever"]

# --- Test 7: Bridge arithmetic ---
def test_bridge_arithmetic(bridge):
    b = bridge.get_bridge("meridian")
    assert b["adjusted_ebitda"] == b["reported_ebitda"] + b["total_adjustments"]

# --- Test 8: Confidence scores ---
def test_confidence_scores(bridge):
    b = bridge.get_bridge("meridian")
    legal = next(a for a in b["adjustments"] if "legal" in a["concept"])
    assert legal["confidence"] == gt_atemporal("meridian", "ebitda_adjustment.non_recurring_legal", "confidence")
    tech = next(a for a in b["adjustments"] if "technology" in a["concept"])
    assert tech["confidence"] == gt_atemporal("meridian", "ebitda_adjustment.technology_consolidation", "confidence")

# --- Test 9: Comparison ---
def test_bridge_comparison(bridge):
    comp = bridge.get_bridge_comparison()
    assert comp["entity_a"]["total_adjustments"] == _sum_ebitda_adjustments("meridian")
    assert comp["entity_b"]["total_adjustments"] == _sum_ebitda_adjustments("cascadia")

# --- Test 10: Sensitivity matrix ---
def test_sensitivity_matrix(bridge):
    matrix = bridge.get_sensitivity_matrix()
    assert len(matrix) > 0
    for row in matrix:
        assert "base" in row
        assert "low" in row
        assert "high" in row

# --- Test 11: QoE summary ---
def test_qoe_meridian(qoe):
    summary = qoe.get_qoe_summary("meridian")
    assert summary["reported_ebitda"] > 0
    assert summary["adjusted_ebitda"] > summary["reported_ebitda"]
    assert "revenue_quality" in summary
    assert "margin_trend" in summary

# --- Test 12: QoE margin trend ---
def test_qoe_margin_trend(qoe):
    summary = qoe.get_qoe_summary("meridian")
    assert len(summary["margin_trend"]) == 12  # all periods
    for point in summary["margin_trend"]:
        assert "period" in point
        assert "ebitda_margin" in point
