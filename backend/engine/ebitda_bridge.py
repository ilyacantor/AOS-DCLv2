"""
EBITDA bridge engine.

Produces a bridge from reported EBITDA (from the combining P&L) to
pro forma adjusted EBITDA by layering entity-level normalizations and
combination synergies.

Data sources:
  - data/combining_statements.json  (reported EBITDA)
  - data/ebitda_adjustments.json    (entity-level adjustments)
  - data/entity_overlap.json        (vendor + people overlap → synergy sizing)
  - backend/engine/cross_sell.py    (pipeline ACV → revenue synergy)
  - backend/engine/engagement.py    (entity IDs and display names)
"""

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from backend.engine.engagement import get_active_engagement
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
    entity: str          # entity_a_id, entity_b_id, or "combined"
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


def _load_ebitda_adjustments() -> list[dict]:
    """Load entity-level adjustments from data/ebitda_adjustments.json.

    Returns the list of adjustment dicts from the 'entity_adjustments' key.
    Raises FileNotFoundError if the file is missing — no silent fallbacks.
    """
    path = _DATA_DIR / "ebitda_adjustments.json"
    if not path.exists():
        raise FileNotFoundError(
            f"EBITDA adjustments file not found at {path}. "
            f"Expected data/ebitda_adjustments.json with 'entity_adjustments' array."
        )
    with open(path) as f:
        data = json.load(f)
    return data["entity_adjustments"]


def _get_reported_ebitda(combining: dict, entity_a_id: str, entity_b_id: str) -> dict[str, float]:
    """Extract reported EBITDA from the latest quarter and annualize (x4).

    Returns dict with entity_a_id, entity_b_id, "adjustments", "combined" — all annualized in dollars.
    """
    quarter_data = combining.get(_LATEST_QUARTER)
    if quarter_data is None:
        raise ValueError(f"Quarter {_LATEST_QUARTER} not found in combining statements.")

    for item in quarter_data["line_items"]:
        if item["line_item"] == "EBITDA":
            return {
                entity_a_id: item[entity_a_id] * 4 * 1_000_000,
                entity_b_id: item[entity_b_id] * 4 * 1_000_000,
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


def _compute_people_synergy(
    overlap: dict,
    entity_a_id: str,
    entity_b_id: str,
    reduction_pct: float = _DEFAULT_HC_REDUCTION_PCT,
) -> tuple[float, int]:
    """Compute corporate function consolidation synergy from people overlap.

    For each function, the overlapping headcount is min(entity_a_hc, entity_b_hc).
    Synergy = total_overlapping_hc x avg_comp x reduction_pct.

    Returns (synergy_dollars, total_overlapping_hc).
    """
    total_overlapping_hc = 0
    for func in overlap.get("people_overlap", {}).get("functions", []):
        a_hc = func.get(f"{entity_a_id}_headcount", 0)
        b_hc = func.get(f"{entity_b_id}_headcount", 0)
        total_overlapping_hc += min(a_hc, b_hc)

    synergy = total_overlapping_hc * _AVG_CORPORATE_COMP * reduction_pct
    return synergy, total_overlapping_hc


def _compute_cross_sell_synergy(pipeline: dict) -> float:
    """Compute revenue synergy from cross-sell pipeline.

    Formula: pipeline_acv x 50% capture x 30% margin x (12/18) ramp.
    Returns synergy in dollars.
    """
    summary = pipeline.get("summary", {})
    pipeline_acv = summary.get("total_high_conf_acv", 0)
    capture_rate = 0.50
    margin = 0.30
    ramp_factor = 12.0 / 18.0
    return pipeline_acv * capture_rate * margin * ramp_factor


# ─────────────────────────────────────────────────────────────────────
# Bridge adjustments
# ─────────────────────────────────────────────────────────────────────

def _build_entity_adjustments() -> list[BridgeAdjustment]:
    """Load entity-level adjustments from data/ebitda_adjustments.json.

    Each JSON entry is parsed into a BridgeAdjustment dataclass.
    """
    raw_adjustments = _load_ebitda_adjustments()
    return [
        BridgeAdjustment(
            name=adj["name"],
            category=adj["category"],
            entity=adj["entity"],
            confidence=adj["confidence"],
            amount=adj["amount"],
            amount_low=adj["amount_low"],
            amount_high=adj["amount_high"],
            lever=adj.get("lever"),
            support_reference=adj["support_reference"],
            rationale=adj["rationale"],
        )
        for adj in raw_adjustments
    ]


def _build_combination_synergies(
    vendor_savings: float,
    people_synergy: float,
    cross_sell_synergy: float,
    entity_a_name: str = "",
    entity_b_name: str = "",
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
            rationale=f"Cross-deploying {entity_a_name} bench consultants onto {entity_b_name} delivery engagements.",
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
            rationale=f"Redeploying idle {entity_b_name} delivery FTEs onto {entity_a_name} project support roles.",
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
    # ── Engagement config ──
    engagement = get_active_engagement()
    entity_a_id, entity_b_id = engagement.entity_ids()
    entity_a_name = engagement.entity_a.display_name
    entity_b_name = engagement.entity_b.display_name

    # ── Load data ──
    combining = _load_combining_statements()
    overlap = _load_entity_overlap()

    if cross_sell_pipeline is None:
        from backend.engine.cross_sell import run_cross_sell_engine
        pipeline_obj = run_cross_sell_engine()
        cross_sell_pipeline = pipeline_obj.to_dict()

    # ── Reported EBITDA (annualized) ──
    reported = _get_reported_ebitda(combining, entity_a_id, entity_b_id)

    logger.info(
        "[ebitda_bridge] Reported EBITDA (annualized): %s=$%.1fM, %s=$%.1fM, Combined=$%.1fM",
        entity_a_name,
        reported[entity_a_id] / 1e6,
        entity_b_name,
        reported[entity_b_id] / 1e6,
        reported["combined"] / 1e6,
    )

    # ── Derived synergy inputs ──
    vendor_savings = _compute_vendor_savings(overlap)
    people_synergy, overlapping_hc = _compute_people_synergy(overlap, entity_a_id, entity_b_id)
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
        entity_a_name=entity_a_name,
        entity_b_name=entity_b_name,
    )

    # ── Arithmetic ──
    entity_adj_total = sum(a.amount for a in entity_adjustments)

    # Entity-adjusted EBITDA per entity
    a_adj = sum(a.amount for a in entity_adjustments if a.entity == entity_a_id)
    b_adj = sum(a.amount for a in entity_adjustments if a.entity == entity_b_id)
    entity_adjusted = {
        entity_a_id: reported[entity_a_id] + a_adj,
        entity_b_id: reported[entity_b_id] + b_adj,
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
            entity_a_id: reported[entity_a_id],
            entity_b_id: reported[entity_b_id],
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
