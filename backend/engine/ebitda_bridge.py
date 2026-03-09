"""
EBITDA bridge engine.

Produces a bridge from reported EBITDA (from the combining P&L) to
pro forma adjusted EBITDA by layering entity-level normalizations and
combination synergies.

Data sources:
  - data/combining_statements.json  (reported EBITDA)
  - data/entity_overlap.json        (vendor + people overlap → synergy sizing)
  - backend/engine/cross_sell.py    (pipeline ACV → revenue synergy)
"""

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

_DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"

# Latest quarter for annualization
_LATEST_QUARTER = "2025-Q4"

# EV multiple
_DEFAULT_EV_MULTIPLE = 12.5

# Corporate compensation assumptions
_AVG_CORPORATE_COMP = 150_000  # $150K blended average
_DEFAULT_HC_REDUCTION_PCT = 0.20  # 20% reduction


# ─────────────────────────────────────────────────────────────────────
# Data model
# ─────────────────────────────────────────────────────────────────────

@dataclass
class BridgeAdjustment:
    name: str
    category: str        # "normalization", "one_time", "run_rate", "cost_synergy", "revenue_synergy", "dis_synergy"
    entity: str          # "meridian", "cascadia", "combined"
    confidence: str      # "high", "medium", "low"
    amount: float        # the default/expected amount ($)
    amount_low: float    # low end of range ($)
    amount_high: float   # high end of range ($)
    lever: str | None    # which sensitivity lever controls this (None for static)
    support_reference: str  # what supports this
    rationale: str


# ─────────────────────────────────────────────────────────────────────
# Data loaders
# ─────────────────────────────────────────────────────────────────────

def _load_combining_statements() -> dict:
    """Load combining_statements.json and return the full dict."""
    path = _DATA_DIR / "combining_statements.json"
    if not path.exists():
        raise FileNotFoundError(
            f"Combining statements not found at {path}. "
            f"Run scripts/generate_combining_data.py first."
        )
    with open(path) as f:
        return json.load(f)


def _load_entity_overlap() -> dict:
    """Load entity_overlap.json and return the full dict."""
    path = _DATA_DIR / "entity_overlap.json"
    if not path.exists():
        raise FileNotFoundError(
            f"Entity overlap data not found at {path}. "
            f"Run scripts/generate_combining_data.py first."
        )
    with open(path) as f:
        return json.load(f)


def _get_reported_ebitda(combining: dict) -> dict[str, float]:
    """Extract reported EBITDA from the latest quarter and annualize (×4).

    Returns dict with meridian, cascadia, adjustments, combined — all annualized in dollars.
    """
    quarter_data = combining.get(_LATEST_QUARTER)
    if quarter_data is None:
        raise ValueError(f"Quarter {_LATEST_QUARTER} not found in combining statements.")

    for item in quarter_data["line_items"]:
        if item["line_item"] == "EBITDA":
            return {
                "meridian": item["meridian"] * 4 * 1_000_000,
                "cascadia": item["cascadia"] * 4 * 1_000_000,
                "adjustments": item["adjustments"] * 4 * 1_000_000,
                "combined": item["combined"] * 4 * 1_000_000,
            }

    raise ValueError(f"EBITDA line item not found in {_LATEST_QUARTER} combining statement.")


def _compute_vendor_savings(overlap: dict) -> float:
    """Sum estimated_savings_M for all vendors with consolidation_opportunity=True.

    Returns total savings in dollars.
    """
    total_savings = 0.0
    for vendor in overlap.get("vendor_overlap", {}).get("matches", []):
        if vendor.get("consolidation_opportunity", False):
            detail = vendor.get("consolidation_detail", {})
            total_savings += detail.get("estimated_savings_M", 0.0) * 1_000_000
    return total_savings


def _compute_people_synergy(overlap: dict, reduction_pct: float = _DEFAULT_HC_REDUCTION_PCT) -> tuple[float, int]:
    """Compute corporate function consolidation synergy from people overlap.

    For each function, the overlapping headcount is min(meridian_hc, cascadia_hc).
    Synergy = total_overlapping_hc × avg_comp × reduction_pct.

    Returns (synergy_dollars, total_overlapping_hc).
    """
    total_overlapping_hc = 0
    for func in overlap.get("people_overlap", {}).get("functions", []):
        m_hc = func.get("meridian_headcount", 0)
        c_hc = func.get("cascadia_headcount", 0)
        total_overlapping_hc += min(m_hc, c_hc)

    synergy = total_overlapping_hc * _AVG_CORPORATE_COMP * reduction_pct
    return synergy, total_overlapping_hc


def _compute_cross_sell_synergy(pipeline: dict) -> float:
    """Compute revenue synergy from cross-sell pipeline.

    Formula: pipeline_acv × 50% capture × 30% margin × (12/18) ramp.
    Returns synergy in dollars.
    """
    summary = pipeline.get("summary", {})
    pipeline_acv = summary.get("total_high_conf_acv", 0)
    capture_rate = 0.50
    margin = 0.30
    ramp_factor = 12.0 / 18.0
    return pipeline_acv * capture_rate * margin * ramp_factor


# ─────────────────────────────────────────────────────────────────────
# Bridge adjustments (static definitions)
# ─────────────────────────────────────────────────────────────────────

def _build_entity_adjustments() -> list[BridgeAdjustment]:
    """Build the list of entity-level adjustments (all static/hardcoded amounts)."""
    return [
        BridgeAdjustment(
            name="Above-market exec compensation — Meridian",
            category="normalization",
            entity="meridian",
            confidence="high",
            amount=18_000_000,
            amount_low=18_000_000,
            amount_high=18_000_000,
            lever=None,
            support_reference="Executive compensation analysis vs. industry benchmarks",
            rationale="Meridian CEO/CFO compensation $18M above market median for comparable firms.",
        ),
        BridgeAdjustment(
            name="Above-market exec compensation — Cascadia",
            category="normalization",
            entity="cascadia",
            confidence="high",
            amount=8_000_000,
            amount_low=8_000_000,
            amount_high=8_000_000,
            lever=None,
            support_reference="Executive compensation analysis vs. industry benchmarks",
            rationale="Cascadia founder/CEO compensation $8M above market median.",
        ),
        BridgeAdjustment(
            name="Non-recurring: M litigation reserve",
            category="one_time",
            entity="meridian",
            confidence="high",
            amount=22_000_000,
            amount_low=22_000_000,
            amount_high=22_000_000,
            lever=None,
            support_reference="Meridian 10-K Note 14 — Litigation contingencies",
            rationale="One-time litigation reserve for patent infringement case settled in Q3.",
        ),
        BridgeAdjustment(
            name="Non-recurring: C delivery center closure",
            category="one_time",
            entity="cascadia",
            confidence="high",
            amount=35_000_000,
            amount_low=35_000_000,
            amount_high=35_000_000,
            lever=None,
            support_reference="Cascadia 10-K Note 8 — Restructuring charges",
            rationale="One-time charge for Manila delivery center closure and staff transition.",
        ),
        BridgeAdjustment(
            name="Transaction costs — this deal (Meridian)",
            category="one_time",
            entity="meridian",
            confidence="high",
            amount=15_000_000,
            amount_low=15_000_000,
            amount_high=15_000_000,
            lever=None,
            support_reference="Merger proxy statement — Transaction expenses",
            rationale="Meridian-side investment banking, legal, and accounting fees for this transaction.",
        ),
        BridgeAdjustment(
            name="Transaction costs — this deal (Cascadia)",
            category="one_time",
            entity="cascadia",
            confidence="high",
            amount=8_000_000,
            amount_low=8_000_000,
            amount_high=8_000_000,
            lever=None,
            support_reference="Merger proxy statement — Transaction expenses",
            rationale="Cascadia-side advisory and legal fees for this transaction.",
        ),
        BridgeAdjustment(
            name="Related-party leases — C founder properties",
            category="normalization",
            entity="cascadia",
            confidence="high",
            amount=4_000_000,
            amount_low=4_000_000,
            amount_high=4_000_000,
            lever=None,
            support_reference="Cascadia related-party transaction disclosures",
            rationale="Cascadia leases 2 properties from founder entity at above-market rates; $4M annual excess.",
        ),
        BridgeAdjustment(
            name="Run-rate: new contracts not at full revenue (Meridian)",
            category="run_rate",
            entity="meridian",
            confidence="medium",
            amount=28_000_000,
            amount_low=28_000_000,
            amount_high=28_000_000,
            lever=None,
            support_reference="Meridian backlog analysis — contracts signed but not fully ramped",
            rationale="3 large contracts signed in Q3-Q4 not yet at full run-rate revenue.",
        ),
        BridgeAdjustment(
            name="Run-rate: new contracts not at full revenue (Cascadia)",
            category="run_rate",
            entity="cascadia",
            confidence="medium",
            amount=15_000_000,
            amount_low=15_000_000,
            amount_high=15_000_000,
            lever=None,
            support_reference="Cascadia backlog analysis — contracts signed but not fully ramped",
            rationale="2 BPM engagements ramping to full headcount in Q1-Q2 2026.",
        ),
        BridgeAdjustment(
            name="Utilization normalization — M Q2-Q3 dip",
            category="normalization",
            entity="meridian",
            confidence="medium",
            amount=45_000_000,
            amount_low=45_000_000,
            amount_high=45_000_000,
            lever="m_utilization_rate",
            support_reference="Meridian utilization reports — Q2/Q3 2025 vs. trailing 8-quarter avg",
            rationale="Q2-Q3 utilization dipped to 73% vs. normalized 78%; add-back for temporary bench buildup.",
        ),
        BridgeAdjustment(
            name="Offshore labor mix shift — C in progress",
            category="run_rate",
            entity="cascadia",
            confidence="medium",
            amount=12_000_000,
            amount_low=12_000_000,
            amount_high=12_000_000,
            lever="c_offshore_mix",
            support_reference="Cascadia workforce planning — offshore migration program",
            rationale="Cascadia migrating 15% of onshore delivery to offshore; $12M annualized savings at completion.",
        ),
        BridgeAdjustment(
            name="C attrition normalization 18% → 15%",
            category="normalization",
            entity="cascadia",
            confidence="low",
            amount=18_000_000,
            amount_low=18_000_000,
            amount_high=18_000_000,
            lever="c_attrition_rate",
            support_reference="Cascadia HR metrics — attrition trending down from 18% to 15% target",
            rationale="Cascadia attrition at 18% vs. 15% industry norm; excess recruiting/training cost add-back.",
        ),
    ]


def _build_combination_synergies(
    vendor_savings: float,
    people_synergy: float,
    cross_sell_synergy: float,
) -> list[BridgeAdjustment]:
    """Build the list of combination synergy adjustments.

    vendor_savings, people_synergy, cross_sell_synergy are in dollars.
    """
    return [
        BridgeAdjustment(
            name="Bench optimization — consulting",
            category="cost_synergy",
            entity="combined",
            confidence="medium",
            amount=100_000_000,
            amount_low=80_000_000,
            amount_high=120_000_000,
            lever="bench_cross_deploy_rate",
            support_reference="Bench utilization analysis — 4500 consultants × cross-deploy model",
            rationale="Cross-deploying Meridian bench consultants onto Cascadia delivery engagements.",
        ),
        BridgeAdjustment(
            name="Bench optimization — delivery",
            category="cost_synergy",
            entity="combined",
            confidence="medium",
            amount=35_000_000,
            amount_low=25_000_000,
            amount_high=45_000_000,
            lever="bench_cross_deploy_rate",
            support_reference="Delivery bench analysis — 4200 FTEs × cross-deploy model",
            rationale="Redeploying idle Cascadia delivery FTEs onto Meridian project support roles.",
        ),
        BridgeAdjustment(
            name="Corporate function consolidation",
            category="cost_synergy",
            entity="combined",
            confidence="medium",
            amount=people_synergy,
            amount_low=45_000_000,
            amount_high=70_000_000,
            lever="corporate_hc_reduction_pct",
            support_reference="People overlap analysis — Finance, HR, IT, Legal functions",
            rationale="Consolidating overlapping corporate functions; reduction based on min(M,C) headcount per function.",
        ),
        BridgeAdjustment(
            name="Vendor consolidation",
            category="cost_synergy",
            entity="combined",
            confidence="high",
            amount=vendor_savings,
            amount_low=15_000_000,
            amount_high=25_000_000,
            lever=None,
            support_reference="Vendor overlap analysis — 170 overlapping vendors with consolidation savings",
            rationale="Consolidating overlapping vendor contracts for volume discounts and eliminated redundancy.",
        ),
        BridgeAdjustment(
            name="Technology redundancy elimination",
            category="cost_synergy",
            entity="combined",
            confidence="medium",
            amount=10_000_000,
            amount_low=8_000_000,
            amount_high=12_000_000,
            lever=None,
            support_reference="Technology stack audit — overlapping SaaS, middleware, and dev tools",
            rationale="Eliminating duplicate SaaS licenses, middleware, and internal tools post-combination.",
        ),
        BridgeAdjustment(
            name="Cross-sell revenue contribution",
            category="revenue_synergy",
            entity="combined",
            confidence="low",
            amount=cross_sell_synergy,
            amount_low=35_000_000,
            amount_high=65_000_000,
            lever="cross_sell_capture_rate",
            support_reference="Cross-sell pipeline — high-confidence ACV × capture rate × margin × ramp",
            rationale="EBITDA contribution from cross-selling services to the other entity's non-overlapping clients.",
        ),
        BridgeAdjustment(
            name="Integration costs Year 1",
            category="dis_synergy",
            entity="combined",
            confidence="high",
            amount=-100_000_000,
            amount_low=-120_000_000,
            amount_high=-85_000_000,
            lever="integration_cost_M",
            support_reference="Integration management office budget — systems, people, branding",
            rationale="Year 1 integration costs: IT system migration, org redesign, rebranding, change management.",
        ),
        BridgeAdjustment(
            name="Retention packages",
            category="dis_synergy",
            entity="combined",
            confidence="high",
            amount=-25_000_000,
            amount_low=-30_000_000,
            amount_high=-20_000_000,
            lever=None,
            support_reference="Retention program — key talent across both entities",
            rationale="Year 1 retention bonuses for critical leadership and top performers.",
        ),
    ]


# ─────────────────────────────────────────────────────────────────────
# Main engine
# ─────────────────────────────────────────────────────────────────────

def compute_ebitda_bridge(cross_sell_pipeline: dict | None = None) -> dict:
    """Compute the full EBITDA bridge from reported to pro forma adjusted.

    Args:
        cross_sell_pipeline: Output of cross_sell.run_cross_sell_engine().to_dict().
            If None, runs the cross-sell engine to obtain it.

    Returns:
        Dict with reported_ebitda, entity_adjustments, entity_adjusted_ebitda,
        combination_synergies, pro_forma_ebitda, and ev_impact.
    """
    # ── Load data ──
    combining = _load_combining_statements()
    overlap = _load_entity_overlap()

    if cross_sell_pipeline is None:
        from backend.engine.cross_sell import run_cross_sell_engine
        pipeline_obj = run_cross_sell_engine()
        cross_sell_pipeline = pipeline_obj.to_dict()

    # ── Reported EBITDA (annualized) ──
    reported = _get_reported_ebitda(combining)

    logger.info(
        "[ebitda_bridge] Reported EBITDA (annualized): M=$%.1fM, C=$%.1fM, Combined=$%.1fM",
        reported["meridian"] / 1e6,
        reported["cascadia"] / 1e6,
        reported["combined"] / 1e6,
    )

    # ── Derived synergy inputs ──
    vendor_savings = _compute_vendor_savings(overlap)
    people_synergy, overlapping_hc = _compute_people_synergy(overlap)
    cross_sell_synergy = _compute_cross_sell_synergy(cross_sell_pipeline)

    logger.info(
        "[ebitda_bridge] Synergy inputs: vendor=$%.1fM, people=$%.1fM (%d overlapping HC), cross-sell=$%.1fM",
        vendor_savings / 1e6,
        people_synergy / 1e6,
        overlapping_hc,
        cross_sell_synergy / 1e6,
    )

    # ── Build adjustments ──
    entity_adjustments = _build_entity_adjustments()
    combination_synergies = _build_combination_synergies(
        vendor_savings=vendor_savings,
        people_synergy=people_synergy,
        cross_sell_synergy=cross_sell_synergy,
    )

    # ── Arithmetic ──
    entity_adj_total = sum(a.amount for a in entity_adjustments)

    # Entity-adjusted EBITDA per entity
    m_adj = sum(a.amount for a in entity_adjustments if a.entity == "meridian")
    c_adj = sum(a.amount for a in entity_adjustments if a.entity == "cascadia")
    entity_adjusted = {
        "meridian": reported["meridian"] + m_adj,
        "cascadia": reported["cascadia"] + c_adj,
        "combined": reported["combined"] + entity_adj_total,
    }

    # Synergy totals
    synergy_total = sum(a.amount for a in combination_synergies if a.category != "dis_synergy")
    dis_synergy_total = sum(a.amount for a in combination_synergies if a.category == "dis_synergy")

    # Pro forma Year 1 = entity_adjusted + synergies + dis-synergies
    pro_forma_year_1 = entity_adjusted["combined"] + synergy_total + dis_synergy_total

    # Low/high ranges
    synergy_low = sum(a.amount_low for a in combination_synergies if a.category != "dis_synergy")
    synergy_high = sum(a.amount_high for a in combination_synergies if a.category != "dis_synergy")
    dis_low = sum(a.amount_low for a in combination_synergies if a.category == "dis_synergy")  # more negative
    dis_high = sum(a.amount_high for a in combination_synergies if a.category == "dis_synergy")  # less negative

    pro_forma_year_1_low = entity_adjusted["combined"] + synergy_low + dis_low
    pro_forma_year_1_high = entity_adjusted["combined"] + synergy_high + dis_high

    # Steady state = year 1 + integration costs (they go away after year 1)
    # Integration costs and retention packages are dis_synergies in year 1 only
    integration_costs_year_1 = sum(
        a.amount for a in combination_synergies
        if a.category == "dis_synergy"
    )
    integration_costs_year_1_low = sum(
        a.amount_low for a in combination_synergies
        if a.category == "dis_synergy"
    )
    integration_costs_year_1_high = sum(
        a.amount_high for a in combination_synergies
        if a.category == "dis_synergy"
    )

    pro_forma_steady_state = pro_forma_year_1 - integration_costs_year_1  # remove the negative
    pro_forma_steady_state_low = pro_forma_year_1_low - integration_costs_year_1_low
    pro_forma_steady_state_high = pro_forma_year_1_high - integration_costs_year_1_high

    # EV impact
    multiple = _DEFAULT_EV_MULTIPLE

    logger.info(
        "[ebitda_bridge] Pro forma Year 1: $%.1fM (low=$%.1fM, high=$%.1fM)",
        pro_forma_year_1 / 1e6,
        pro_forma_year_1_low / 1e6,
        pro_forma_year_1_high / 1e6,
    )
    logger.info(
        "[ebitda_bridge] Pro forma Steady State: $%.1fM (low=$%.1fM, high=$%.1fM)",
        pro_forma_steady_state / 1e6,
        pro_forma_steady_state_low / 1e6,
        pro_forma_steady_state_high / 1e6,
    )

    return {
        "reported_ebitda": {
            "meridian": reported["meridian"],
            "cascadia": reported["cascadia"],
            "combined_reported": reported["combined"],
        },
        "entity_adjustments": [asdict(a) for a in entity_adjustments],
        "entity_adjusted_ebitda": entity_adjusted,
        "combination_synergies": [asdict(a) for a in combination_synergies],
        "pro_forma_ebitda": {
            "year_1": {
                "low": pro_forma_year_1_low,
                "high": pro_forma_year_1_high,
                "current": pro_forma_year_1,
            },
            "steady_state": {
                "low": pro_forma_steady_state_low,
                "high": pro_forma_steady_state_high,
                "current": pro_forma_steady_state,
            },
        },
        "ev_impact": {
            "multiple": multiple,
            "year_1_ev": {
                "low": pro_forma_year_1_low * multiple,
                "high": pro_forma_year_1_high * multiple,
                "current": pro_forma_year_1 * multiple,
            },
            "steady_state_ev": {
                "low": pro_forma_steady_state_low * multiple,
                "high": pro_forma_steady_state_high * multiple,
                "current": pro_forma_steady_state * multiple,
            },
        },
    }
