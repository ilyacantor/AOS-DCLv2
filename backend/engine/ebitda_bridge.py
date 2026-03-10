"""
EBITDA bridge engine.

Produces a bridge from reported EBITDA (from the combining P&L) to
pro forma adjusted EBITDA by layering entity-level normalizations and
combination synergies.

Data sources:
  - data/combining_statements.json  (reported EBITDA)
  - data/entity_overlap.json        (vendor + people overlap → synergy sizing)
  - data/ebitda_adjustments.json    (adjustment definitions with template strings)
  - backend/engine/cross_sell.py    (pipeline ACV → revenue synergy)
  - backend/engine/engagement_config.py  (entity-agnostic config)
"""

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from backend.engine.engagement_config import EngagementConfig, get_engagement
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
    entity: str          # entity_a.id, entity_b.id, or "combined"
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


def _load_adjustments_json() -> dict:
    """Load ebitda_adjustments.json and return the full dict."""
    path = _DATA_DIR / "ebitda_adjustments.json"
    if not path.exists():
        raise FileNotFoundError(
            f"EBITDA adjustments not found at {path}. "
            f"Create from template in data/ebitda_adjustments.json."
        )
    with open(path) as f:
        return json.load(f)


def _resolve_template(value: str, engagement: EngagementConfig) -> str:
    """Replace template placeholders in a string with actual entity names.

    Supported placeholders:
      {entity_a}       → entity_a.name
      {entity_b}       → entity_b.name
      {entity_a_short} → entity_a.short_name
      {entity_b_short} → entity_b.short_name
    """
    if not isinstance(value, str):
        return value
    return (
        value
        .replace("{entity_a}", engagement.entity_a.name)
        .replace("{entity_b}", engagement.entity_b.name)
        .replace("{entity_a_short}", engagement.entity_a.short_name)
        .replace("{entity_b_short}", engagement.entity_b.short_name)
    )


def _resolve_entity_field(entity_value: str, engagement: EngagementConfig) -> str:
    """Map 'entity_a' / 'entity_b' / 'combined' to actual entity IDs."""
    if entity_value == "entity_a":
        return engagement.entity_a.id
    elif entity_value == "entity_b":
        return engagement.entity_b.id
    return entity_value  # "combined" or anything else passes through


def _get_reported_ebitda(combining: dict, engagement: EngagementConfig) -> dict[str, float]:
    """Extract reported EBITDA from the latest quarter and annualize (×4).

    Returns dict with entity_a.id, entity_b.id, adjustments, combined — all annualized in dollars.
    """
    quarter_data = combining.get(_LATEST_QUARTER)
    if quarter_data is None:
        raise ValueError(f"Quarter {_LATEST_QUARTER} not found in combining statements.")

    col_a = engagement.column_keys.entity_a
    col_b = engagement.column_keys.entity_b

    for item in quarter_data["line_items"]:
        if item["line_item"] == "EBITDA":
            return {
                engagement.entity_a.id: item[col_a] * 4 * 1_000_000,
                engagement.entity_b.id: item[col_b] * 4 * 1_000_000,
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
    engagement: EngagementConfig,
    reduction_pct: float = _DEFAULT_HC_REDUCTION_PCT,
) -> tuple[float, int]:
    """Compute corporate function consolidation synergy from people overlap.

    For each function, the overlapping headcount is min(entity_a_hc, entity_b_hc).
    Synergy = total_overlapping_hc × avg_comp × reduction_pct.

    Returns (synergy_dollars, total_overlapping_hc).
    """
    hc_key_a = engagement.overlap_keys.entity_a_headcount
    hc_key_b = engagement.overlap_keys.entity_b_headcount

    total_overlapping_hc = 0
    for func in overlap.get("people_overlap", {}).get("functions", []):
        a_hc = func.get(hc_key_a, 0)
        b_hc = func.get(hc_key_b, 0)
        total_overlapping_hc += min(a_hc, b_hc)

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
# Bridge adjustments (loaded from JSON + template resolution)
# ─────────────────────────────────────────────────────────────────────

def _load_entity_adjustments(engagement: EngagementConfig) -> list[BridgeAdjustment]:
    """Load entity-level adjustments from ebitda_adjustments.json and resolve templates."""
    raw = _load_adjustments_json()
    adjustments = []
    for item in raw.get("entity_adjustments", []):
        adjustments.append(BridgeAdjustment(
            name=_resolve_template(item["name"], engagement),
            category=item["category"],
            entity=_resolve_entity_field(item["entity"], engagement),
            confidence=item["confidence"],
            amount=item["amount"],
            amount_low=item["amount_low"],
            amount_high=item["amount_high"],
            lever=item.get("lever"),
            support_reference=_resolve_template(item["support_reference"], engagement),
            rationale=_resolve_template(item["rationale"], engagement),
        ))
    return adjustments


def _load_combination_synergies(
    engagement: EngagementConfig,
    vendor_savings: float,
    people_synergy: float,
    cross_sell_synergy: float,
) -> list[BridgeAdjustment]:
    """Load combination synergy adjustments from ebitda_adjustments.json.

    Resolves template strings and replaces __COMPUTED_* sentinels with actual values.
    vendor_savings, people_synergy, cross_sell_synergy are in dollars.
    """
    sentinel_map = {
        "__COMPUTED_PEOPLE_SYNERGY__": people_synergy,
        "__COMPUTED_VENDOR_SAVINGS__": vendor_savings,
        "__COMPUTED_CROSS_SELL__": cross_sell_synergy,
    }

    raw = _load_adjustments_json()
    synergies = []
    for item in raw.get("combination_synergies", []):
        # Resolve computed amount sentinels
        amount = item["amount"]
        if isinstance(amount, str) and amount in sentinel_map:
            amount = sentinel_map[amount]

        synergies.append(BridgeAdjustment(
            name=_resolve_template(item["name"], engagement),
            category=item["category"],
            entity=_resolve_entity_field(item["entity"], engagement),
            confidence=item["confidence"],
            amount=amount,
            amount_low=item["amount_low"],
            amount_high=item["amount_high"],
            lever=item.get("lever"),
            support_reference=_resolve_template(item["support_reference"], engagement),
            rationale=_resolve_template(item["rationale"], engagement),
        ))
    return synergies


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
    # ── Load engagement config ──
    engagement = get_engagement()
    ea_id = engagement.entity_a.id
    eb_id = engagement.entity_b.id

    # ── Load data ──
    combining = _load_combining_statements()
    overlap = _load_entity_overlap()

    if cross_sell_pipeline is None:
        from backend.engine.cross_sell import run_cross_sell_engine
        pipeline_obj = run_cross_sell_engine()
        cross_sell_pipeline = pipeline_obj.to_dict()

    # ── Reported EBITDA (annualized) ──
    reported = _get_reported_ebitda(combining, engagement)

    logger.info(
        "[ebitda_bridge] Reported EBITDA (annualized): %s=$%.1fM, %s=$%.1fM, Combined=$%.1fM",
        ea_id,
        reported[ea_id] / 1e6,
        eb_id,
        reported[eb_id] / 1e6,
        reported["combined"] / 1e6,
    )

    # ── Derived synergy inputs ──
    vendor_savings = _compute_vendor_savings(overlap)
    people_synergy, overlapping_hc = _compute_people_synergy(overlap, engagement)
    cross_sell_synergy = _compute_cross_sell_synergy(cross_sell_pipeline)

    logger.info(
        "[ebitda_bridge] Synergy inputs: vendor=$%.1fM, people=$%.1fM (%d overlapping HC), cross-sell=$%.1fM",
        vendor_savings / 1e6,
        people_synergy / 1e6,
        overlapping_hc,
        cross_sell_synergy / 1e6,
    )

    # ── Build adjustments ──
    entity_adjustments = _load_entity_adjustments(engagement)
    combination_synergies = _load_combination_synergies(
        engagement=engagement,
        vendor_savings=vendor_savings,
        people_synergy=people_synergy,
        cross_sell_synergy=cross_sell_synergy,
    )

    # ── Arithmetic ──
    entity_adj_total = sum(a.amount for a in entity_adjustments)

    # Entity-adjusted EBITDA per entity
    a_adj = sum(a.amount for a in entity_adjustments if a.entity == ea_id)
    b_adj = sum(a.amount for a in entity_adjustments if a.entity == eb_id)
    entity_adjusted = {
        ea_id: reported[ea_id] + a_adj,
        eb_id: reported[eb_id] + b_adj,
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
            ea_id: reported[ea_id],
            eb_id: reported[eb_id],
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
