"""
Stage 3E Harness — EBITDA Bridge + Quality of Earnings
Tests bridge construction and QoE analysis from ebitda_adjustment.* triples.
Expected values fetched from Farm's ground truth API at runtime (B10).
"""
import pytest
from backend.engine.ebitda_bridge_v2 import EBITDABridgeV2, STAGE_ORDER
from backend.engine.qoe_v2 import QualityOfEarningsV2

from tests.conftest import TENANT_ID, RUN_ID, gt_atemporal


def _sum_ebitda_adjustments(entity: str) -> float:
    """Sum all EBITDA adjustment amount_current values for an entity from ground truth.

    Ground truth keys are base concepts (2-segment) with latest-stage values.
    """
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

# --- Test 13: Lifecycle history present ---
def test_lifecycle_history_present(bridge):
    """Bridge adjustments must include lifecycle_history array."""
    b = bridge.get_bridge("meridian")
    for adj in b["adjustments"]:
        assert "lifecycle_history" in adj, f"Missing lifecycle_history on {adj['concept']}"
        assert isinstance(adj["lifecycle_history"], list)
        assert len(adj["lifecycle_history"]) >= 1

# --- Test 14: Lifecycle history ordered by stage progression ---
def test_lifecycle_history_ordered(bridge):
    """lifecycle_history entries must be ordered by STAGE_ORDER."""
    b = bridge.get_bridge("meridian")
    for adj in b["adjustments"]:
        history = adj["lifecycle_history"]
        stage_indices = [STAGE_ORDER.get(h["stage"], 99) for h in history]
        assert stage_indices == sorted(stage_indices), (
            f"lifecycle_history not ordered for {adj['concept']}: "
            f"stages={[h['stage'] for h in history]}"
        )

# --- Test 15: Diligence and prior amounts ---
def test_diligence_and_prior_amounts(bridge):
    """Adjustments with 2 stages have diligence_amount (management) and prior_amount."""
    b = bridge.get_bridge("meridian")
    facility = next(a for a in b["adjustments"] if "facility" in a["concept"])
    # With management + initial_diligence stages:
    # diligence_amount = management amount, prior_amount = management amount
    # (prior = one step before latest; with 2 stages, prior = management)
    assert facility["diligence_amount"] is not None
    assert facility["prior_amount"] is not None
    assert isinstance(facility["diligence_amount"], (int, float))
    assert isinstance(facility["prior_amount"], (int, float))

# --- Test 16: Trend derivation ---
def test_trend_derivation(bridge):
    """trend field must be one of: increasing, decreasing, stable, neutral."""
    b = bridge.get_bridge("meridian")
    valid_trends = {"increasing", "decreasing", "stable", "neutral"}
    for adj in b["adjustments"]:
        assert adj["trend"] in valid_trends, (
            f"Invalid trend '{adj['trend']}' on {adj['concept']}"
        )

# --- Test 17: Lifecycle stage field ---
def test_lifecycle_stage_field(bridge):
    """Each adjustment has a lifecycle_stage naming the latest stage."""
    b = bridge.get_bridge("meridian")
    for adj in b["adjustments"]:
        assert "lifecycle_stage" in adj
        assert adj["lifecycle_stage"] in STAGE_ORDER, (
            f"Unknown lifecycle_stage '{adj['lifecycle_stage']}' on {adj['concept']}"
        )

# --- Test 18: QoE adjustment_lifecycle ---
def test_qoe_adjustment_lifecycle(qoe):
    """QoE summary must include adjustment_lifecycle with stage data."""
    summary = qoe.get_qoe_summary("meridian")
    assert "adjustment_lifecycle" in summary
    al = summary["adjustment_lifecycle"]
    assert isinstance(al, dict)
    assert len(al) > 0
    for category, stages in al.items():
        assert isinstance(stages, list)
        assert len(stages) >= 1
        for entry in stages:
            assert "stage" in entry
            assert "amount" in entry
            assert "confidence" in entry

# --- Test 19: QoE sustainability_trend ---
def test_qoe_sustainability_trend(qoe):
    """QoE summary must include sustainability_trend with period scores."""
    summary = qoe.get_qoe_summary("meridian")
    assert "sustainability_trend" in summary
    st = summary["sustainability_trend"]
    assert isinstance(st, list)
    assert len(st) >= 1
    for entry in st:
        assert "period" in entry
        assert "score" in entry
        assert "grade" in entry
        assert isinstance(entry["score"], int)
        assert entry["grade"] in {"A", "B", "C", "D", "F"}

# --- Test 20: QoE combined has new fields ---
def test_qoe_combined_new_fields(qoe):
    """Combined QoE must include adjustment_lifecycle and sustainability_trend."""
    combined = qoe.get_combined_qoe()
    assert "adjustment_lifecycle" in combined["combined"]
    assert "sustainability_trend" in combined["combined"]
    assert "adjustment_lifecycle" in combined["entity_a"]
    assert "adjustment_lifecycle" in combined["entity_b"]

# --- Test 21: Base concept in adjustments (not 3-segment) ---
def test_adjustment_concept_is_base(bridge):
    """Adjustment concept field must be 2-segment base concept."""
    b = bridge.get_bridge("meridian")
    for adj in b["adjustments"]:
        parts = adj["concept"].split(".")
        assert len(parts) == 2, (
            f"Adjustment concept should be 2-segment base, got '{adj['concept']}'"
        )
