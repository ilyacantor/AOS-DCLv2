"""
Stage 3E Harness — EBITDA Bridge + Quality of Earnings
Tests bridge construction and QoE analysis from ebitda_adjustment.* triples.
"""
import pytest
from backend.engine.ebitda_bridge_v2 import EBITDABridgeV2
from backend.engine.qoe_v2 import QualityOfEarningsV2

from tests.conftest import TENANT_ID, RUN_ID

M_ADJ_TOTAL = 171.35
C_ADJ_TOTAL = 16.46
COMBINED_ADJ_TOTAL = 187.81
ADJUSTMENT_TYPE_COUNT = 8

M_ADJ_FACILITY = 23.92
M_ADJ_HEADCOUNT = 45.54
M_ADJ_LEGAL = 8.61
M_ADJ_PROF_FEES = 8.23
M_ADJ_OWNER_COMP = 24.98
M_ADJ_RELATED_PARTY = 4.45
M_ADJ_RUN_RATE = 37.83
M_ADJ_TECH = 17.79

CONF_LEGAL = 0.85
CONF_TECH = 0.65


@pytest.fixture
def bridge():
    return EBITDABridgeV2(TENANT_ID, RUN_ID)

@pytest.fixture
def qoe():
    return QualityOfEarningsV2(TENANT_ID, RUN_ID)


# --- Test 1: Meridian bridge total ---
def test_meridian_total_adjustments(bridge):
    b = bridge.get_bridge("meridian")
    assert b["total_adjustments"] == M_ADJ_TOTAL

# --- Test 2: Cascadia bridge total ---
def test_cascadia_total_adjustments(bridge):
    b = bridge.get_bridge("cascadia")
    assert b["total_adjustments"] == C_ADJ_TOTAL

# --- Test 3: Combined bridge total ---
def test_combined_total_adjustments(bridge):
    b = bridge.get_bridge()  # None = combined
    assert b["total_adjustments"] == COMBINED_ADJ_TOTAL

# --- Test 4: Adjustment count ---
def test_adjustment_count(bridge):
    b = bridge.get_bridge("meridian")
    assert len(b["adjustments"]) == ADJUSTMENT_TYPE_COUNT

# --- Test 5: Individual adjustment values ---
def test_meridian_facility_adjustment(bridge):
    b = bridge.get_bridge("meridian")
    facility = next(a for a in b["adjustments"] if "facility" in a["concept"])
    assert facility["amount"] == M_ADJ_FACILITY

def test_meridian_headcount_adjustment(bridge):
    b = bridge.get_bridge("meridian")
    headcount = next(a for a in b["adjustments"] if "headcount" in a["concept"])
    assert headcount["amount"] == M_ADJ_HEADCOUNT

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
    assert legal["confidence"] == CONF_LEGAL
    tech = next(a for a in b["adjustments"] if "technology" in a["concept"])
    assert tech["confidence"] == CONF_TECH

# --- Test 9: Comparison ---
def test_bridge_comparison(bridge):
    comp = bridge.get_bridge_comparison()
    assert comp["entity_a"]["total_adjustments"] == M_ADJ_TOTAL
    assert comp["entity_b"]["total_adjustments"] == C_ADJ_TOTAL

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
