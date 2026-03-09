"""
What-if sensitivity engine.

Provides 10 levers that allow scenario analysis on the EBITDA bridge.
Each lever recalculates affected bridge lines and produces updated
pro forma EBITDA and EV impact.

Performance target: <2 seconds — pure formula evaluation with preloaded data.
"""

import json
from pathlib import Path
from typing import Any

from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

_DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"


# ─────────────────────────────────────────────────────────────────────
# Lever definitions
# ─────────────────────────────────────────────────────────────────────

DEFAULT_LEVERS: dict[str, float] = {
    "m_utilization_rate": 78,
    "m_bench_rate": 18,
    "c_offshore_mix": 60,
    "c_attrition_rate": 18,
    "cross_sell_capture_rate": 50,
    "cross_sell_ramp_months": 18,
    "corporate_hc_reduction_pct": 20,
    "bench_cross_deploy_rate": 15,
    "integration_cost_M": 100,
    "ev_multiple": 12.5,
}

LEVER_DEFINITIONS: list[dict[str, Any]] = [
    {
        "name": "m_utilization_rate",
        "label": "M Utilization Rate",
        "min": 72,
        "max": 85,
        "default": 78,
        "unit": "%",
        "impact_per_point_M": 62,
    },
    {
        "name": "m_bench_rate",
        "label": "M Bench Rate",
        "min": 10,
        "max": 22,
        "default": 18,
        "unit": "%",
        "impact_per_point_M": 18,
    },
    {
        "name": "c_offshore_mix",
        "label": "C Offshore Mix",
        "min": 50,
        "max": 80,
        "default": 60,
        "unit": "%",
        "impact_per_point_M": 6,
    },
    {
        "name": "c_attrition_rate",
        "label": "C Attrition Rate",
        "min": 10,
        "max": 22,
        "default": 18,
        "unit": "%",
        "impact_per_point_M": 4,
    },
    {
        "name": "cross_sell_capture_rate",
        "label": "Cross-Sell Capture Rate",
        "min": 0,
        "max": 100,
        "default": 50,
        "unit": "%",
        "impact_per_point_M": None,
    },
    {
        "name": "cross_sell_ramp_months",
        "label": "Cross-Sell Ramp Months",
        "min": 6,
        "max": 36,
        "default": 18,
        "unit": "months",
        "impact_per_point_M": None,
    },
    {
        "name": "corporate_hc_reduction_pct",
        "label": "Corporate HC Reduction %",
        "min": 0,
        "max": 40,
        "default": 20,
        "unit": "%",
        "impact_per_point_M": None,
    },
    {
        "name": "bench_cross_deploy_rate",
        "label": "Bench Cross-Deploy Rate",
        "min": 0,
        "max": 50,
        "default": 15,
        "unit": "%",
        "impact_per_point_M": None,
    },
    {
        "name": "integration_cost_M",
        "label": "Integration Cost ($M)",
        "min": 50,
        "max": 150,
        "default": 100,
        "unit": "$M",
        "impact_per_point_M": 1,
    },
    {
        "name": "ev_multiple",
        "label": "EV Multiple",
        "min": 8,
        "max": 18,
        "default": 12.5,
        "unit": "x",
        "impact_per_point_M": None,
    },
]


# ─────────────────────────────────────────────────────────────────────
# Preset scenarios
# ─────────────────────────────────────────────────────────────────────

PRESETS: dict[str, dict[str, float]] = {
    "base": dict(DEFAULT_LEVERS),
    "conservative": {
        "m_utilization_rate": 75,
        "m_bench_rate": 20,
        "c_offshore_mix": 55,
        "c_attrition_rate": 20,
        "cross_sell_capture_rate": 25,
        "cross_sell_ramp_months": 18,
        "corporate_hc_reduction_pct": 10,
        "bench_cross_deploy_rate": 5,
        "integration_cost_M": 130,
        "ev_multiple": 10,
    },
    "aggressive": {
        "m_utilization_rate": 82,
        "m_bench_rate": 13,
        "c_offshore_mix": 72,
        "c_attrition_rate": 13,
        "cross_sell_capture_rate": 85,
        "cross_sell_ramp_months": 18,
        "corporate_hc_reduction_pct": 35,
        "bench_cross_deploy_rate": 40,
        "integration_cost_M": 65,
        "ev_multiple": 15,
    },
    "harmonize_to_m": {
        **DEFAULT_LEVERS,
        "c_offshore_mix": 78,
        "c_attrition_rate": 15,
    },
    "harmonize_to_c": {
        **DEFAULT_LEVERS,
        "m_utilization_rate": 75,
        "m_bench_rate": 20,
    },
}


# ─────────────────────────────────────────────────────────────────────
# Data preloading helpers
# ─────────────────────────────────────────────────────────────────────

def _load_people_overlap_hc() -> int:
    """Load total overlapping headcount from entity_overlap.json.

    Overlapping HC = sum of min(meridian_hc, cascadia_hc) per function.
    """
    path = _DATA_DIR / "entity_overlap.json"
    if not path.exists():
        raise FileNotFoundError(
            f"Entity overlap data not found at {path}. "
            f"Run scripts/generate_combining_data.py first."
        )
    with open(path) as f:
        overlap = json.load(f)

    total = 0
    for func in overlap.get("people_overlap", {}).get("functions", []):
        total += min(func.get("meridian_headcount", 0), func.get("cascadia_headcount", 0))
    return total


def _load_pipeline_acv(bridge: dict) -> float:
    """Extract the high-confidence pipeline ACV from the bridge's cross-sell synergy line.

    The bridge stores the cross-sell revenue contribution amount, which was computed as:
        pipeline_acv * 0.50 * 0.30 * (12/18)

    We back-solve for pipeline_acv so we can recompute with different lever values.
    """
    for syn in bridge.get("combination_synergies", []):
        if syn["name"] == "Cross-sell revenue contribution":
            # amount = acv * 0.50 * 0.30 * (12/18)
            # acv = amount / (0.50 * 0.30 * (12/18))
            default_factor = 0.50 * 0.30 * (12.0 / 18.0)
            if default_factor > 0:
                return syn["amount"] / default_factor
    return 0.0


# ─────────────────────────────────────────────────────────────────────
# Lever impact formulas
# ─────────────────────────────────────────────────────────────────────

def _apply_levers(
    levers: dict[str, float],
    bridge: dict,
    overlapping_hc: int,
    pipeline_acv: float,
) -> dict:
    """Recompute all lever-sensitive bridge lines and produce the what-if output.

    This is pure arithmetic — no I/O, no external calls.
    """
    avg_comp = 150_000

    # ── Entity-level adjustment deltas from levers ──
    # These represent CHANGES from the base entity adjustments in the bridge.
    # The bridge already includes base amounts for these; we compute the delta.

    # m_utilization_rate: each point from 78 = ~$62M
    util_delta = (levers["m_utilization_rate"] - 78) * 62_000_000

    # m_bench_rate: each point below 18 = ~$18M savings
    bench_rate_delta = (18 - levers["m_bench_rate"]) * 18_000_000

    # c_offshore_mix: each point above 60 = ~$6M savings
    offshore_delta = (levers["c_offshore_mix"] - 60) * 6_000_000

    # c_attrition_rate: each point below 18 = ~$4M savings
    attrition_delta = (18 - levers["c_attrition_rate"]) * 4_000_000

    # ── Recompute entity adjustments with lever impacts ──
    recomputed_adjustments = []
    for adj in bridge["entity_adjustments"]:
        entry = dict(adj)
        if adj["lever"] == "m_utilization_rate":
            entry["amount"] = adj["amount"] + util_delta
        elif adj["lever"] == "c_offshore_mix":
            entry["amount"] = adj["amount"] + offshore_delta
        elif adj["lever"] == "c_attrition_rate":
            entry["amount"] = adj["amount"] + attrition_delta
        recomputed_adjustments.append(entry)

    # Add bench rate delta as an entity-level adjustment if non-zero
    # (bench rate affects entity-level P&L, not synergies)
    if bench_rate_delta != 0:
        recomputed_adjustments.append({
            "name": "Bench rate impact (lever)",
            "category": "normalization",
            "entity": "meridian",
            "confidence": "medium",
            "amount": bench_rate_delta,
            "amount_low": bench_rate_delta,
            "amount_high": bench_rate_delta,
            "lever": "m_bench_rate",
            "support_reference": "What-if lever: M bench rate",
            "rationale": f"Impact of M bench rate at {levers['m_bench_rate']:.0f}% vs. base 18%.",
        })

    # ── Recompute synergies with lever impacts ──
    capture_rate = levers["cross_sell_capture_rate"] / 100.0
    ramp_months = levers["cross_sell_ramp_months"]
    ramp_factor = min(12.0 / ramp_months, 1.0)
    cross_sell_synergy = pipeline_acv * capture_rate * 0.30 * ramp_factor

    reduction_pct = levers["corporate_hc_reduction_pct"] / 100.0
    corporate_synergy = overlapping_hc * avg_comp * reduction_pct

    deploy_rate = levers["bench_cross_deploy_rate"] / 100.0
    bench_consulting = 4500 * 15_100 * 12 * deploy_rate * 0.5
    bench_delivery = 4200 * 1_800 * 12 * deploy_rate

    integration_cost = -levers["integration_cost_M"] * 1_000_000

    recomputed_synergies = []
    for syn in bridge["combination_synergies"]:
        entry = dict(syn)
        if syn["name"] == "Bench optimization — consulting":
            entry["amount"] = bench_consulting
        elif syn["name"] == "Bench optimization — delivery":
            entry["amount"] = bench_delivery
        elif syn["name"] == "Corporate function consolidation":
            entry["amount"] = corporate_synergy
        elif syn["name"] == "Cross-sell revenue contribution":
            entry["amount"] = cross_sell_synergy
        elif syn["name"] == "Integration costs Year 1":
            entry["amount"] = integration_cost
        # Vendor consolidation and Technology redundancy stay as-is (no lever)
        # Retention packages stay as-is (no lever)
        recomputed_synergies.append(entry)

    # ── Compute totals ──
    reported_ebitda = bridge["reported_ebitda"]["combined_reported"]
    entity_adj_total = sum(a["amount"] for a in recomputed_adjustments)
    entity_adjusted_ebitda = reported_ebitda + entity_adj_total

    synergy_total = sum(s["amount"] for s in recomputed_synergies if s["category"] != "dis_synergy")
    dis_synergy_total = sum(s["amount"] for s in recomputed_synergies if s["category"] == "dis_synergy")

    pro_forma_year_1 = entity_adjusted_ebitda + synergy_total + dis_synergy_total

    # Steady state: remove dis-synergies (they're year-1 only)
    pro_forma_steady_state = entity_adjusted_ebitda + synergy_total

    # EV
    ev_multiple = levers["ev_multiple"]
    ev_year_1 = pro_forma_year_1 * ev_multiple
    ev_steady_state = pro_forma_steady_state * ev_multiple

    logger.info(
        "[what_if] Pro forma Year 1: $%.1fM, Steady State: $%.1fM, EV Year 1: $%.1fB, EV SS: $%.1fB",
        pro_forma_year_1 / 1e6,
        pro_forma_steady_state / 1e6,
        ev_year_1 / 1e9,
        ev_steady_state / 1e9,
    )

    return {
        "levers": dict(levers),
        "lever_definitions": LEVER_DEFINITIONS,
        "reported_ebitda": reported_ebitda,
        "entity_adjusted_ebitda": entity_adjusted_ebitda,
        "adjustments": recomputed_adjustments,
        "synergies": recomputed_synergies,
        "pro_forma_ebitda": {
            "year_1": pro_forma_year_1,
            "steady_state": pro_forma_steady_state,
        },
        "ev_impact": {
            "year_1": ev_year_1,
            "steady_state": ev_steady_state,
        },
        "presets": PRESETS,
    }


# ─────────────────────────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────────────────────────

def compute_what_if(
    levers: dict[str, float] | None = None,
    bridge: dict | None = None,
) -> dict:
    """Run the what-if sensitivity engine.

    Args:
        levers: Dict of lever name → value. Missing levers use defaults.
            If None, all defaults are used.
        bridge: Output of compute_ebitda_bridge(). If None, computes it.

    Returns:
        Dict with levers, lever_definitions, reported_ebitda,
        entity_adjusted_ebitda, adjustments, synergies,
        pro_forma_ebitda, ev_impact, and presets.
    """
    # Merge provided levers with defaults
    effective_levers = dict(DEFAULT_LEVERS)
    if levers is not None:
        for k, v in levers.items():
            if k not in DEFAULT_LEVERS:
                logger.warning("[what_if] Unknown lever '%s' — ignoring.", k)
                continue
            effective_levers[k] = v

    # Get or compute bridge
    if bridge is None:
        from backend.engine.ebitda_bridge import compute_ebitda_bridge
        bridge = compute_ebitda_bridge()

    # Preload data needed for lever recomputation
    overlapping_hc = _load_people_overlap_hc()
    pipeline_acv = _load_pipeline_acv(bridge)

    logger.info(
        "[what_if] Running with levers: %s",
        {k: v for k, v in effective_levers.items() if v != DEFAULT_LEVERS.get(k)},
    )

    return _apply_levers(
        levers=effective_levers,
        bridge=bridge,
        overlapping_hc=overlapping_hc,
        pipeline_acv=pipeline_acv,
    )
