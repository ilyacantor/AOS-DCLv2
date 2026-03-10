"""
Executive dashboard engine.

Produces aggregated data for 5 executive personas:
  - CFO: Financial overview, EBITDA bridge, cost synergies
  - CRO: Revenue & pipeline, cross-sell opportunities
  - COO: Operations & integration, vendor consolidation, people overlap
  - CTO: Technology & systems, tech redundancy
  - CHRO: People & talent, retention, headcount

Each dashboard pulls from the combining P&L, entity overlap, cross-sell
pipeline, and EBITDA bridge via the module-level engine cache, so the
expensive computations happen once and are reused across personas and requests.
"""

import time
from typing import Any

from backend.engine import _engine_cache
from backend.engine.engagement import get_active_engagement
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

_VALID_PERSONAS = {"cfo", "cro", "coo", "cto", "chro"}

# Vendor categories considered technology-related
_TECH_VENDOR_CATEGORIES = {
    "cloud_infrastructure",
    "collaboration",
    "technology",
    "telecom",
}


# ─────────────────────────────────────────────────────────────────────
# Engine data access — backed by module-level cache
# ─────────────────────────────────────────────────────────────────────

class _EngineCache:
    """Thin accessor over the module-level engine cache.

    All expensive computation (cross-sell, bridge, QofE) is done once in
    _engine_cache and reused across requests until the source data files
    change on disk.  This class is a convenience wrapper — it holds no
    per-instance state and can be instantiated freely.
    """

    def get_cross_sell_pipeline(self) -> dict:
        return _engine_cache.get("cross_sell")

    def get_bridge(self) -> dict:
        return _engine_cache.get("bridge")

    def get_qoe(self) -> dict:
        return _engine_cache.get("qoe")


# ─────────────────────────────────────────────────────────────────────
# Helpers — combining statements
# ─────────────────────────────────────────────────────────────────────

def _get_line_item(quarter_data: dict, line_item_name: str) -> dict | None:
    """Find a line item by name in a quarter's data."""
    for item in quarter_data.get("line_items", []):
        if item["line_item"] == line_item_name:
            return item
    return None


def _extract_quarterly_revenue(combining: dict, entity_a_id: str, entity_b_id: str) -> list[dict]:
    """Extract combined revenue for every quarter, sorted chronologically."""
    trend = []
    for period_key in sorted(k for k in combining.keys() if not k.startswith("_")):
        quarter_data = combining[period_key]
        rev = _get_line_item(quarter_data, "Total Revenue")
        if rev is not None:
            trend.append({
                "period": period_key,
                entity_a_id: rev[entity_a_id],
                entity_b_id: rev[entity_b_id],
                "combined": rev["combined"],
            })
    return trend


def _latest_quarter_key(combining: dict) -> str:
    """Return the latest quarter key (skip metadata keys like _periods, _cofa_adjustments)."""
    quarter_keys = [k for k in combining.keys() if not k.startswith("_")]
    if not quarter_keys:
        raise ValueError("No quarter data found in combining_statements.json")
    return sorted(quarter_keys)[-1]


def _annualize(quarterly_value: float) -> float:
    """Annualize a quarterly value (values in the JSON are in $M)."""
    return quarterly_value * 4


# ─────────────────────────────────────────────────────────────────────
# Helpers — entity overlap
# ─────────────────────────────────────────────────────────────────────

def _vendor_consolidation_summary(overlap: dict) -> list[dict]:
    """Return vendors with consolidation opportunity, sorted by savings descending."""
    results = []
    for vendor in overlap.get("vendor_overlap", {}).get("matches", []):
        if vendor.get("consolidation_opportunity", False):
            detail = vendor.get("consolidation_detail", {})
            results.append({
                "vendor": vendor["canonical_name"],
                "category": vendor.get("category", "unknown"),
                "combined_spend_M": vendor.get("combined_spend_M", 0),
                "estimated_savings_M": detail.get("estimated_savings_M", 0),
                "savings_rationale": detail.get("savings_rationale", ""),
            })
    results.sort(key=lambda v: v["estimated_savings_M"], reverse=True)
    return results


def _total_vendor_savings(overlap: dict) -> float:
    """Sum estimated_savings_M across all vendors with consolidation opportunity."""
    total = 0.0
    for vendor in overlap.get("vendor_overlap", {}).get("matches", []):
        if vendor.get("consolidation_opportunity", False):
            total += vendor.get("consolidation_detail", {}).get("estimated_savings_M", 0)
    return total


def _people_overlap_by_function(overlap: dict, entity_a_id: str, entity_b_id: str) -> list[dict]:
    """Return people overlap broken down by function."""
    results = []
    for func in overlap.get("people_overlap", {}).get("functions", []):
        a_hc = func.get(f"{entity_a_id}_headcount", 0)
        b_hc = func.get(f"{entity_b_id}_headcount", 0)
        results.append({
            "function": func["function"],
            f"{entity_a_id}_headcount": a_hc,
            f"{entity_b_id}_headcount": b_hc,
            "combined_headcount": func.get("combined_headcount", a_hc + b_hc),
            "overlapping_headcount": min(a_hc, b_hc),
            "role_overlap_examples": func.get("role_overlap_examples", []),
        })
    return results


def _total_people_overlap_hc(overlap: dict, entity_a_id: str, entity_b_id: str) -> int:
    """Sum of min(A, B) headcount across all functions."""
    total = 0
    for func in overlap.get("people_overlap", {}).get("functions", []):
        total += min(
            func.get(f"{entity_a_id}_headcount", 0),
            func.get(f"{entity_b_id}_headcount", 0),
        )
    return total


def _combined_headcount_estimate(overlap: dict) -> int:
    """Sum of combined_headcount across all functions."""
    total = 0
    for func in overlap.get("people_overlap", {}).get("functions", []):
        total += func.get("combined_headcount", 0)
    return total


# ─────────────────────────────────────────────────────────────────────
# Helpers — bridge
# ─────────────────────────────────────────────────────────────────────

def _bridge_synergies_by_category(bridge: dict, category: str) -> list[dict]:
    """Filter combination synergies by category."""
    return [
        s for s in bridge.get("combination_synergies", [])
        if s["category"] == category
    ]


def _bridge_synergy_by_name(bridge: dict, name: str) -> dict | None:
    """Find a specific synergy by name."""
    for s in bridge.get("combination_synergies", []):
        if s["name"] == name:
            return s
    return None


def _top_cost_synergies(bridge: dict, limit: int = 10) -> list[dict]:
    """Return top cost synergies sorted by amount descending."""
    synergies = _bridge_synergies_by_category(bridge, "cost_synergy")
    synergies.sort(key=lambda s: s["amount"], reverse=True)
    return synergies[:limit]


def _integration_cost_from_bridge(bridge: dict) -> dict:
    """Extract integration cost and retention package dis-synergies."""
    dis = _bridge_synergies_by_category(bridge, "dis_synergy")
    integration = None
    retention = None
    for d in dis:
        if "Integration" in d["name"]:
            integration = d
        elif "Retention" in d["name"]:
            retention = d
    return {
        "integration_cost": integration,
        "retention_packages": retention,
        "total_year_1": sum(d["amount"] for d in dis),
    }


# ─────────────────────────────────────────────────────────────────────
# Dashboard builders
# ─────────────────────────────────────────────────────────────────────

def _build_cfo_dashboard(
    combining: dict,
    overlap: dict,
    cache: _EngineCache,
    entity_a_id: str,
    entity_b_id: str,
) -> dict:
    """CFO — Financial overview."""
    bridge = cache.get_bridge()
    qoe = cache.get_qoe()
    latest_key = _latest_quarter_key(combining)
    latest = combining[latest_key]

    rev_item = _get_line_item(latest, "Total Revenue")
    ebitda_item = _get_line_item(latest, "EBITDA")
    if rev_item is None:
        raise ValueError(f"Total Revenue not found in {latest_key}")
    if ebitda_item is None:
        raise ValueError(f"EBITDA not found in {latest_key}")

    pf = bridge["pro_forma_ebitda"]
    ev = bridge["ev_impact"]

    # Working capital from QofE
    wc = qoe.get("working_capital", {})
    dso_current = wc["dso_trend"][-1]["value"] if wc.get("dso_trend") else None
    dpo_current = wc["dpo_trend"][-1]["value"] if wc.get("dpo_trend") else None

    return {
        "persona": "cfo",
        "title": "CFO Dashboard — Financial Overview",
        "kpis": {
            "combined_revenue_annualized": _annualize(rev_item["combined"]),
            "combined_ebitda_annualized": _annualize(ebitda_item["combined"]),
            "pro_forma_ebitda_year_1": pf["year_1"]["current"],
            "pro_forma_ebitda_steady_state": pf["steady_state"]["current"],
            "ev_at_current_multiple": ev["steady_state_ev"]["current"],
            "qoe_sustainability_score": qoe["sustainability_score"]["overall"],
        },
        "revenue_trend": _extract_quarterly_revenue(combining, entity_a_id, entity_b_id),
        "ebitda_bridge_summary": {
            "reported_ebitda": bridge["reported_ebitda"],
            "entity_adjusted_ebitda": bridge["entity_adjusted_ebitda"],
            "pro_forma_year_1": pf["year_1"],
            "pro_forma_steady_state": pf["steady_state"],
        },
        "cost_synergy_breakdown": _top_cost_synergies(bridge),
        "integration_cost_estimate": _integration_cost_from_bridge(bridge),
        "qoe_summary": {
            "sustainability_score": qoe["sustainability_score"]["overall"],
            "sustainability_grade": qoe["sustainability_score"]["grade"],
            "adjustment_status": qoe["summary"],
            "working_capital": {
                "dso_current": dso_current,
                "dpo_current": dpo_current,
            },
        },
    }


def _build_cro_dashboard(
    combining: dict,
    overlap: dict,
    cache: _EngineCache,
    entity_a_id: str,
    entity_b_id: str,
) -> dict:
    """CRO — Revenue & pipeline."""
    pipeline = cache.get_cross_sell_pipeline()
    bridge = cache.get_bridge()
    summary = pipeline["summary"]

    # Top 10 cross-sell opportunities by ACV across both directions
    all_candidates = pipeline["m_to_c"] + pipeline["c_to_m"]
    all_candidates.sort(key=lambda c: c["estimated_acv"], reverse=True)
    top_10 = all_candidates[:10]

    # Revenue synergy from bridge
    rev_synergy = _bridge_synergy_by_name(bridge, "Cross-sell revenue contribution")

    customer_overlap = overlap.get("customer_overlap", {})

    return {
        "persona": "cro",
        "title": "CRO Dashboard — Revenue & Pipeline",
        "kpis": {
            "total_cross_sell_candidates": summary["total_candidates"],
            "total_pipeline_acv": summary["total_pipeline_acv"],
            "m_to_c_candidates": summary["m_to_c_candidates"],
            "c_to_m_candidates": summary["c_to_m_candidates"],
            "customer_overlap_count": customer_overlap.get("total_overlapping", 0),
            "high_confidence_pipeline_acv": summary["total_high_conf_acv"],
        },
        "cross_sell_conversion": {
            "total_candidates": summary["total_candidates"],
            "total_pipeline_acv": summary["total_pipeline_acv"],
            "high_confidence_count": summary["m_to_c_high_conf_count"] + summary["c_to_m_high_conf_count"],
            "high_confidence_acv": summary["total_high_conf_acv"],
            "converted_count": 0,
            "converted_acv": 0,
            "conversion_rate_pct": 0,
        },
        "pipeline_by_direction": {
            "m_to_c": {
                "candidates": summary["m_to_c_candidates"],
                "total_acv": summary["m_to_c_total_acv"],
                "high_confidence_count": summary["m_to_c_high_conf_count"],
                "high_confidence_acv": summary["m_to_c_high_conf_acv"],
            },
            "c_to_m": {
                "candidates": summary["c_to_m_candidates"],
                "total_acv": summary["c_to_m_total_acv"],
                "high_confidence_count": summary["c_to_m_high_conf_count"],
                "high_confidence_acv": summary["c_to_m_high_conf_acv"],
            },
        },
        "top_10_cross_sell": [
            {
                "customer_name": c["customer_name"],
                "entity_id": c["entity_id"],
                "recommended_service": c["recommended_service"],
                "propensity_score": c["propensity_score"],
                "estimated_acv": c["estimated_acv"],
                "industry": c["industry"],
                "rationale": c["rationale"],
            }
            for c in top_10
        ],
        "revenue_synergy": rev_synergy,
        "customer_overlap_summary": {
            "total_overlapping": customer_overlap.get("total_overlapping", 0),
            f"overlap_pct_of_{entity_a_id}": customer_overlap.get(f"overlap_pct_of_{entity_a_id}", 0),
            f"overlap_pct_of_{entity_b_id}": customer_overlap.get(f"overlap_pct_of_{entity_b_id}", 0),
        },
    }


def _build_coo_dashboard(
    combining: dict,
    overlap: dict,
    cache: _EngineCache,
    entity_a_id: str,
    entity_b_id: str,
) -> dict:
    """COO — Operations & integration."""
    bridge = cache.get_bridge()

    vendor_overlap_data = overlap.get("vendor_overlap", {})
    vendor_consol = _vendor_consolidation_summary(overlap)
    vendors_with_opportunity = len(vendor_consol)
    total_savings = _total_vendor_savings(overlap)

    people_hc = _total_people_overlap_hc(overlap, entity_a_id, entity_b_id)
    people_by_func = _people_overlap_by_function(overlap, entity_a_id, entity_b_id)

    # Corporate HC reduction synergy from bridge
    corp_synergy = _bridge_synergy_by_name(bridge, "Corporate function consolidation")

    # Bench optimization synergies
    bench_consulting = _bridge_synergy_by_name(bridge, "Bench optimization — consulting")
    bench_delivery = _bridge_synergy_by_name(bridge, "Bench optimization — delivery")

    integration = _integration_cost_from_bridge(bridge)

    return {
        "persona": "coo",
        "title": "COO Dashboard — Operations & Integration",
        "kpis": {
            "vendor_overlap_count": vendor_overlap_data.get("total_overlapping", 0),
            "vendors_with_consolidation_opportunity": vendors_with_opportunity,
            "total_vendor_savings_M": round(total_savings, 2),
            "people_overlap_hc": people_hc,
            "corporate_hc_reduction_synergy": corp_synergy["amount"] if corp_synergy else 0,
        },
        "vendor_consolidation": vendor_consol[:10],
        "people_overlap_by_function": people_by_func,
        "bench_optimization": {
            "consulting": bench_consulting,
            "delivery": bench_delivery,
        },
        "integration_timeline_cost": integration,
    }


def _build_cto_dashboard(
    combining: dict,
    overlap: dict,
    cache: _EngineCache,
    entity_a_id: str,
    entity_b_id: str,
) -> dict:
    """CTO — Technology & systems."""
    bridge = cache.get_bridge()

    # Technology redundancy elimination from bridge
    tech_redundancy = _bridge_synergy_by_name(bridge, "Technology redundancy elimination")

    # Filter vendor overlap for tech-relevant categories
    vendor_matches = overlap.get("vendor_overlap", {}).get("matches", [])
    tech_vendors = [
        v for v in vendor_matches
        if v.get("category", "") in _TECH_VENDOR_CATEGORIES
    ]

    tech_vendors_with_consolidation = [
        {
            "vendor": v["canonical_name"],
            "category": v.get("category", "unknown"),
            f"{entity_a_id}_spend_M": v.get(f"{entity_a_id}_spend_M", 0),
            f"{entity_b_id}_spend_M": v.get(f"{entity_b_id}_spend_M", 0),
            "combined_spend_M": v.get("combined_spend_M", 0),
            "consolidation_opportunity": v.get("consolidation_opportunity", False),
            "estimated_savings_M": (
                v.get("consolidation_detail", {}).get("estimated_savings_M", 0)
                if v.get("consolidation_opportunity", False) else 0
            ),
        }
        for v in tech_vendors
    ]
    tech_vendors_with_consolidation.sort(
        key=lambda v: v["combined_spend_M"], reverse=True
    )

    return {
        "persona": "cto",
        "title": "CTO Dashboard — Technology & Systems",
        "kpis": {
            "technology_redundancy_elimination": (
                tech_redundancy["amount"] if tech_redundancy else 0
            ),
            "tech_vendor_overlap_count": len(tech_vendors),
        },
        "technology_synergy": tech_redundancy,
        "system_overlap_indicators": tech_vendors_with_consolidation,
    }


def _build_chro_dashboard(
    combining: dict,
    overlap: dict,
    cache: _EngineCache,
    entity_a_id: str,
    entity_b_id: str,
) -> dict:
    """CHRO — People & talent."""
    bridge = cache.get_bridge()

    people_by_func = _people_overlap_by_function(overlap, entity_a_id, entity_b_id)
    people_hc = _total_people_overlap_hc(overlap, entity_a_id, entity_b_id)
    combined_hc = _combined_headcount_estimate(overlap)

    # Corporate HC reduction synergy from bridge
    corp_synergy = _bridge_synergy_by_name(bridge, "Corporate function consolidation")

    # Retention packages from bridge
    retention = _bridge_synergy_by_name(bridge, "Retention packages")

    # Attrition info from entity adjustments
    a_attrition = None
    b_attrition = None
    for adj in bridge.get("entity_adjustments", []):
        if "attrition" in adj["name"].lower():
            if adj["entity"] == entity_b_id:
                b_attrition = adj
            elif adj["entity"] == entity_a_id:
                a_attrition = adj

    return {
        "persona": "chro",
        "title": "CHRO Dashboard — People & Talent",
        "kpis": {
            "total_people_overlap_hc": people_hc,
            "functions_with_overlap": len(people_by_func),
            "corporate_hc_reduction_synergy": corp_synergy["amount"] if corp_synergy else 0,
            "retention_package_cost": retention["amount"] if retention else 0,
            "combined_headcount_estimate": combined_hc,
        },
        "people_overlap_by_function": people_by_func,
        "attrition_comparison": {
            entity_a_id: a_attrition,
            entity_b_id: b_attrition,
        },
        "retention_package_detail": retention,
    }


# ─────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────

_BUILDERS = {
    "cfo": _build_cfo_dashboard,
    "cro": _build_cro_dashboard,
    "coo": _build_coo_dashboard,
    "cto": _build_cto_dashboard,
    "chro": _build_chro_dashboard,
}


def compute_dashboard(persona: str) -> dict:
    """Compute an executive dashboard for the given persona.

    Args:
        persona: One of "cfo", "cro", "coo", "cto", "chro".

    Returns:
        Plain dict suitable for JSON serialization.

    Raises:
        ValueError: If persona is not recognized.
        FileNotFoundError: If required data files are missing.
    """
    persona = persona.lower().strip()
    if persona not in _VALID_PERSONAS:
        raise ValueError(
            f"Unknown persona '{persona}'. Must be one of: {sorted(_VALID_PERSONAS)}"
        )

    t0 = time.monotonic()
    logger.info("[dashboards] Computing dashboard for persona=%s", persona)

    eng = get_active_engagement()
    entity_a_id, entity_b_id = eng.entity_ids()

    # All data from module-level engine cache (mtime-invalidated)
    combining = _engine_cache.get("combining")
    overlap = _engine_cache.get("overlap")
    cache = _EngineCache()

    builder = _BUILDERS[persona]
    result = builder(combining, overlap, cache, entity_a_id, entity_b_id)

    elapsed_ms = (time.monotonic() - t0) * 1000
    logger.info("[dashboards] Dashboard for %s complete in %.0fms", persona, elapsed_ms)
    return result
