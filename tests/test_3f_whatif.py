"""
Stage 3F Harness — What-If + Revenue Bridge + Scenarios
Tests scenario analysis and revenue bridge from triples.
"""
import pytest
from backend.engine.what_if_v2 import WhatIfEngineV2
from backend.engine.revenue_bridge import RevenueBridgeV2

from tests.conftest import TENANT_ID, RUN_ID

M_Q1_REV = 1323.43
M_Q1_EBITDA = 321.59
C_Q1_REV = 269.38
M_2024Q1_REV = 1250.00
M_2025Q1_REV = 1323.43
M_REV_CHANGE = 73.43
M_CONSULTING_2024Q1 = 812.50
M_CONSULTING_2025Q1 = 860.23
COMBINED_2024Q1_REV = 1500.00
COMBINED_2025Q1_REV = 1592.81


@pytest.fixture
def whatif():
    return WhatIfEngineV2(TENANT_ID, RUN_ID)

@pytest.fixture
def bridge():
    return RevenueBridgeV2(TENANT_ID, RUN_ID)


# --- Test 1: Baseline matches seed exactly ---
def test_baseline_revenue(whatif):
    baseline = whatif.get_baseline("meridian", "2025-Q1")
    assert baseline["revenue"]["total"] == M_Q1_REV

def test_baseline_ebitda(whatif):
    baseline = whatif.get_baseline("meridian", "2025-Q1")
    assert baseline["ebitda"] == M_Q1_EBITDA

# --- Test 2: Revenue bridge values ---
def test_revenue_bridge_total(bridge):
    b = bridge.get_revenue_bridge("meridian", "2024-Q1", "2025-Q1")
    assert b["from_total"] == M_2024Q1_REV
    assert b["to_total"] == M_2025Q1_REV
    assert b["total_change"] == M_REV_CHANGE

# --- Test 3: Revenue bridge by stream ---
def test_revenue_bridge_by_stream(bridge):
    b = bridge.get_revenue_bridge("meridian", "2024-Q1", "2025-Q1")
    consulting = next(s for s in b["by_stream"] if s["concept"] == "revenue.consulting")
    assert consulting["from"] == M_CONSULTING_2024Q1
    assert consulting["to"] == M_CONSULTING_2025Q1

# --- Test 4: YoY bridge ---
def test_yoy_bridge(bridge):
    b = bridge.get_yoy_bridge("meridian", "2025-Q1")
    assert b["from_total"] == M_2024Q1_REV
    assert b["to_total"] == M_2025Q1_REV

# --- Test 5: Combined revenue bridge ---
def test_combined_revenue_bridge(bridge):
    b = bridge.get_combined_revenue_bridge("2024-Q1", "2025-Q1")
    assert b["from_total"] == COMBINED_2024Q1_REV
    assert b["to_total"] == COMBINED_2025Q1_REV

# --- Test 6: What-if scenario ---
def test_revenue_decline_scenario(whatif):
    result = whatif.apply_scenario("meridian", "2025-Q1", [
        {"concept": "revenue.total", "type": "pct", "value": -10.0}
    ])
    assert result["baseline"]["revenue"]["total"] == M_Q1_REV
    # Adjusted revenue should be 10% less
    expected_adj_rev = round(M_Q1_REV * 0.90, 2)
    assert abs(result["adjusted"]["revenue"]["total"] - expected_adj_rev) < 0.01

# --- Test 7: Scenario comparison ---
def test_scenario_comparison(whatif):
    result = whatif.compare_scenarios("meridian", "2025-Q1", {
        "bear": [{"concept": "revenue.total", "type": "pct", "value": -20.0}],
        "base": [],
        "bull": [{"concept": "revenue.total", "type": "pct", "value": 10.0}],
    })
    assert result["scenarios"]["base"]["adjusted"]["revenue"]["total"] == M_Q1_REV
    assert result["scenarios"]["bear"]["adjusted"]["revenue"]["total"] < M_Q1_REV
    assert result["scenarios"]["bull"]["adjusted"]["revenue"]["total"] > M_Q1_REV

# --- Test 8: Sensitivity analysis ---
def test_sensitivity(whatif):
    results = whatif.sensitivity_analysis("meridian", "2025-Q1", "revenue.total", 20.0, 5)
    assert len(results) == 5
    # Middle point should be close to baseline
    ebitdas = [r["ebitda"] for r in results]
    assert ebitdas[0] < ebitdas[-1]  # lower revenue = lower EBITDA

# --- Test 9: Save and load scenario ---
def test_save_load_scenario(whatif):
    scenario_id = whatif.save_scenario("test_bear", "meridian", "2025-Q1", [
        {"concept": "revenue.total", "type": "pct", "value": -15.0}
    ])
    assert scenario_id is not None
    loaded = whatif.load_scenario(scenario_id)
    assert loaded["baseline"]["revenue"]["total"] == M_Q1_REV

# --- Test 10: List scenarios ---
def test_list_scenarios(whatif):
    whatif.save_scenario("test_list", "meridian", "2025-Q1", [])
    scenarios = whatif.list_scenarios()
    assert len(scenarios) > 0
    names = [s["name"] for s in scenarios]
    assert "test_list" in names

# --- Test 11: Absolute adjustment ---
def test_absolute_adjustment(whatif):
    result = whatif.apply_scenario("meridian", "2025-Q1", [
        {"concept": "revenue.total", "type": "abs", "value": -100.0}
    ])
    expected = M_Q1_REV - 100.0
    assert result["adjusted"]["revenue"]["total"] == expected

# --- Test 12: Cascadia baseline ---
def test_cascadia_baseline(whatif):
    baseline = whatif.get_baseline("cascadia", "2025-Q1")
    assert baseline["revenue"]["total"] == C_Q1_REV
