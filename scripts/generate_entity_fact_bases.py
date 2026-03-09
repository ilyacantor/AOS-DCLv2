#!/usr/bin/env python3
"""
Generate per-entity fact_base files from combining_statements.json.

Reads:
  - data/combining_statements.json  (P&L by entity per quarter)
  - data/fact_base.json             (base ~120 metrics per quarter)

Writes:
  - data/fact_base_meridian.json
  - data/fact_base_cascadia.json
  - data/fact_base_combined.json
"""

import json
import copy
import sys
from pathlib import Path
from datetime import datetime

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR.parent / "data"

# ── Combining-statements line-item → fact_base metric mapping ──────────────

# Maps combining_statements line_item names to fact_base keys.
# Some are direct 1:1, some need aggregation (handled separately).
LINE_ITEM_MAP = {
    "Total Revenue": "revenue",
    "Total COGS": "cogs",
    "Gross Profit": "gross_profit",
    "G&A": "ga_expense",
    "Total OpEx": "opex",
    "EBITDA": "ebitda",
    "D&A": "da_expense",
    "Operating Profit": "operating_profit",
    "Tax": "tax_expense",
    "Net Income": "net_income",
}

# sm_expense = Sales + Marketing (summed from two line items)
SM_LINE_ITEMS = ["Sales", "Marketing"]

# rd_expense = whichever exists: "R&D / Technology" or "Technology & Automation"
RD_LINE_ITEMS = ["R&D / Technology", "Technology & Automation"]

# ── Metric classification ──────────────────────────────────────────────────

# Percentage/rate metrics: keep base values as-is (entity-agnostic defaults)
PERCENTAGE_METRICS = {
    "gross_margin_pct", "operating_margin_pct", "ebitda_margin_pct",
    "net_margin_pct", "nrr", "gross_churn_pct", "logo_churn_pct",
    "win_rate_pct", "quota_attainment_pct", "attrition_rate_pct",
    "tech_debt_pct", "cloud_spend_pct_revenue", "uptime_pct",
    "reps_at_quota_pct", "code_coverage_pct", "deployment_success_pct",
    "change_failure_rate", "bug_escape_rate", "engineering_utilization",
    "offer_acceptance_rate_pct", "internal_mobility_rate_pct",
}

# Ratio/score metrics: keep as-is from base
RATIO_METRICS = {
    "ltv_cac_ratio", "magic_number", "burn_multiple", "rule_of_40",
    "csat", "nps", "span_of_control", "engagement_score", "enps",
}

# Duration/time metrics: keep as-is from base
TIME_METRICS = {
    "sales_cycle_days", "first_response_hours", "resolution_hours",
    "lead_time_days", "time_to_fill", "mttr_p1_hours", "mttr_p2_hours",
    "training_hours_per_employee",
}

# Per-unit metrics: keep as-is
PER_UNIT_METRICS = {
    "revenue_per_employee", "arr_per_employee", "cost_per_employee",
    "acv", "ltv", "cac", "avg_deal_size",
}

# Metrics that should NOT be scaled — use base values directly
NO_SCALE_METRICS = PERCENTAGE_METRICS | RATIO_METRICS | TIME_METRICS | PER_UNIT_METRICS

# P&L metrics sourced from combining_statements (set after mapping)
PL_METRICS = set(LINE_ITEM_MAP.values()) | {"sm_expense", "rd_expense", "sga"}

# Headcount metrics — scaled by entity headcount ratio
HEADCOUNT_METRICS = {
    "headcount", "new_hires", "terminations",
    "engineering_headcount", "sales_headcount", "cs_headcount",
    "marketing_headcount", "product_headcount", "finance_headcount",
    "ga_headcount", "open_roles",
}

# Incident/count metrics scaled proportionally
COUNT_METRICS = {
    "support_tickets", "p1_incidents", "p2_incidents", "incident_count",
    "sprint_velocity", "story_points", "features_shipped",
    "security_vulns", "critical_bugs", "api_requests_millions",
    "downtime_hours",
}

# Boolean / structural keys — copy as-is
STRUCTURAL_KEYS = {"period", "year", "quarter", "is_forecast"}

# Derived metrics we compute ourselves (don't scale from base)
DERIVED_METRICS = {
    "gross_margin_pct", "operating_margin_pct", "ebitda_margin_pct",
    "net_margin_pct", "cash_from_operations", "fcf",
    "revenue_per_employee", "arr_per_employee",
}

# ── Entity headcount targets (from farm configs) ──────────────────────────

ENTITY_HEADCOUNT = {
    "meridian": 30_000,
    "cascadia": 38_000,
}


def load_json(path: Path) -> dict:
    with open(path, "r") as f:
        return json.load(f)


def extract_line_item(line_items: list, name: str) -> dict:
    """Find a line_item by name, return the dict or None."""
    for li in line_items:
        if li["line_item"] == name:
            return li
    return None


def get_entity_pl(quarter_data: dict, entity: str) -> dict:
    """Extract P&L metrics for an entity from a combining_statements quarter."""
    line_items = quarter_data["line_items"]
    result = {}

    # Direct mappings
    for li_name, metric_key in LINE_ITEM_MAP.items():
        li = extract_line_item(line_items, li_name)
        if li is not None:
            result[metric_key] = li[entity]

    # sm_expense = Sales + Marketing
    sm_total = 0.0
    for li_name in SM_LINE_ITEMS:
        li = extract_line_item(line_items, li_name)
        if li is not None:
            sm_total += li[entity]
    result["sm_expense"] = sm_total

    # rd_expense = R&D / Technology + Technology & Automation (one per entity)
    rd_total = 0.0
    for li_name in RD_LINE_ITEMS:
        li = extract_line_item(line_items, li_name)
        if li is not None:
            rd_total += li[entity]
    result["rd_expense"] = rd_total

    # sga = sm_expense + ga_expense
    result["sga"] = result["sm_expense"] + result.get("ga_expense", 0.0)

    return result


def build_entity_quarter(base_quarter: dict, pl_data: dict, entity: str,
                         base_revenue: float, entity_revenue: float,
                         base_headcount: float) -> dict:
    """Build a single quarter for an entity fact_base."""
    q = {}

    # Revenue scale factor for financial metrics
    rev_scale = entity_revenue / base_revenue if base_revenue else 1.0

    # Headcount scale factor
    target_hc = ENTITY_HEADCOUNT.get(entity, base_headcount)
    hc_scale = target_hc / base_headcount if base_headcount else 1.0

    for key, base_val in base_quarter.items():
        # Structural keys — copy as-is
        if key in STRUCTURAL_KEYS:
            q[key] = base_val
            continue

        # P&L metrics from combining_statements
        if key in pl_data:
            q[key] = round(pl_data[key], 2)
            continue

        # Derived metrics — skip for now, compute after
        if key in DERIVED_METRICS:
            continue

        # No-scale metrics (percentages, ratios, etc.)
        if key in NO_SCALE_METRICS:
            q[key] = base_val
            continue

        # Headcount metrics
        if key in HEADCOUNT_METRICS:
            if isinstance(base_val, int):
                q[key] = max(1, round(base_val * hc_scale))
            else:
                q[key] = round(base_val * hc_scale, 2)
            continue

        # Count metrics — scale with revenue
        if key in COUNT_METRICS:
            if isinstance(base_val, int):
                q[key] = max(0, round(base_val * rev_scale))
            else:
                q[key] = round(base_val * rev_scale, 2)
            continue

        # Everything else (financial/monetary metrics) — scale with revenue
        if isinstance(base_val, (int, float)) and not isinstance(base_val, bool):
            q[key] = round(base_val * rev_scale, 2)
        else:
            q[key] = base_val

    # ── Compute derived metrics ──────────────────────────────────────
    revenue = q.get("revenue", 0)
    if revenue and revenue != 0:
        q["gross_margin_pct"] = round(q.get("gross_profit", 0) / revenue * 100, 1)
        q["operating_margin_pct"] = round(q.get("operating_profit", 0) / revenue * 100, 1)
        q["ebitda_margin_pct"] = round(q.get("ebitda", 0) / revenue * 100, 1)
        q["net_margin_pct"] = round(q.get("net_income", 0) / revenue * 100, 1)
    else:
        q["gross_margin_pct"] = 0.0
        q["operating_margin_pct"] = 0.0
        q["ebitda_margin_pct"] = 0.0
        q["net_margin_pct"] = 0.0

    # Simplified cash flow
    q["cash_from_operations"] = round(
        q.get("net_income", 0) + q.get("da_expense", 0), 2
    )
    capex = q.get("capex", 0)
    if "capex" not in q:
        capex = round(base_quarter.get("capex", 0) * rev_scale, 2)
        q["capex"] = capex
    q["fcf"] = round(q["cash_from_operations"] - capex, 2)

    # Per-employee metrics (recompute from entity values)
    hc = q.get("headcount", 1)
    if hc and hc > 0:
        q["revenue_per_employee"] = round(revenue / hc, 4)
        arr = q.get("arr", 0)
        q["arr_per_employee"] = round(arr / hc, 4) if arr else 0.0
    else:
        q["revenue_per_employee"] = 0.0
        q["arr_per_employee"] = 0.0

    return q


def build_combined_quarter(meridian_q: dict, cascadia_q: dict,
                           base_quarter: dict, pl_combined: dict) -> dict:
    """Build combined entity quarter: P&L from combining_statements combined column,
    other additive metrics summed from meridian + cascadia."""
    q = {}

    for key, base_val in base_quarter.items():
        if key in STRUCTURAL_KEYS:
            q[key] = base_val
            continue

        # P&L from combining_statements combined column
        if key in pl_combined:
            q[key] = round(pl_combined[key], 2)
            continue

        if key in DERIVED_METRICS:
            continue

        # Percentage/ratio/time metrics — average of both entities (weighted by revenue)
        if key in NO_SCALE_METRICS:
            q[key] = base_val
            continue

        # Additive metrics: sum meridian + cascadia
        m_val = meridian_q.get(key, 0)
        c_val = cascadia_q.get(key, 0)
        if isinstance(base_val, (int, float)) and not isinstance(base_val, bool):
            if isinstance(base_val, int):
                q[key] = round(m_val + c_val) if isinstance(m_val, (int, float)) else m_val
            else:
                q[key] = round(m_val + c_val, 2) if isinstance(m_val, (int, float)) else m_val
        else:
            q[key] = base_val

    # Derived metrics
    revenue = q.get("revenue", 0)
    if revenue and revenue != 0:
        q["gross_margin_pct"] = round(q.get("gross_profit", 0) / revenue * 100, 1)
        q["operating_margin_pct"] = round(q.get("operating_profit", 0) / revenue * 100, 1)
        q["ebitda_margin_pct"] = round(q.get("ebitda", 0) / revenue * 100, 1)
        q["net_margin_pct"] = round(q.get("net_income", 0) / revenue * 100, 1)
    else:
        q["gross_margin_pct"] = 0.0
        q["operating_margin_pct"] = 0.0
        q["ebitda_margin_pct"] = 0.0
        q["net_margin_pct"] = 0.0

    q["cash_from_operations"] = round(
        q.get("net_income", 0) + q.get("da_expense", 0), 2
    )
    capex = q.get("capex", 0)
    if "capex" not in q:
        capex = round(meridian_q.get("capex", 0) + cascadia_q.get("capex", 0), 2)
        q["capex"] = capex
    q["fcf"] = round(q["cash_from_operations"] - capex, 2)

    hc = q.get("headcount", 1)
    if hc and hc > 0:
        q["revenue_per_employee"] = round(revenue / hc, 4)
        arr = q.get("arr", 0)
        q["arr_per_employee"] = round(arr / hc, 4) if arr else 0.0
    else:
        q["revenue_per_employee"] = 0.0
        q["arr_per_employee"] = 0.0

    return q


def scale_dimensional_section(section: dict, scale_factor: float, is_pct: bool = False) -> dict:
    """Scale a dimensional breakdown section (e.g., revenue_by_region).
    If is_pct, keep values as-is (they're percentages/rates)."""
    result = {}
    for key, val in section.items():
        if key == "source":
            result[key] = val
            continue
        if isinstance(val, dict):
            if is_pct:
                result[key] = val
            else:
                scaled = {}
                for k2, v2 in val.items():
                    if isinstance(v2, (int, float)) and not isinstance(v2, bool):
                        scaled[k2] = round(v2 * scale_factor, 2)
                    elif isinstance(v2, list):
                        # Lists of dicts (e.g., top_deals) — scale numeric fields
                        scaled[k2] = scale_list_of_dicts(v2, scale_factor)
                    else:
                        scaled[k2] = v2
                result[key] = scaled
        elif isinstance(val, list):
            result[key] = scale_list_of_dicts(val, scale_factor)
        else:
            result[key] = val
    return result


def scale_list_of_dicts(items: list, scale_factor: float) -> list:
    """Scale numeric values in a list of dicts (e.g., top_deals, quota_by_rep)."""
    result = []
    for item in items:
        if isinstance(item, dict):
            scaled = {}
            for k, v in item.items():
                if isinstance(v, (int, float)) and not isinstance(v, bool):
                    scaled[k] = round(v * scale_factor, 2)
                else:
                    scaled[k] = v
            result.append(scaled)
        else:
            result.append(item)
    return result


def combine_dimensional_sections(m_section: dict, c_section: dict) -> dict:
    """Sum two dimensional sections for combined entity."""
    result = {}
    for key in m_section:
        if key == "source":
            result[key] = m_section[key]
            continue
        m_val = m_section.get(key, {})
        c_val = c_section.get(key, {})
        if isinstance(m_val, dict) and isinstance(c_val, dict):
            combined = {}
            for k2 in set(list(m_val.keys()) + list(c_val.keys())):
                mv = m_val.get(k2, 0)
                cv = c_val.get(k2, 0)
                if isinstance(mv, (int, float)) and isinstance(cv, (int, float)):
                    combined[k2] = round(mv + cv, 2)
                else:
                    combined[k2] = mv
            result[key] = combined
        else:
            result[key] = m_val
    return result


# Dimensional sections that contain percentages/rates (don't scale values)
PCT_SECTIONS = {
    "win_rate_by_rep", "attrition_by_department", "engagement_by_department",
    "csat_by_segment", "enps_by_department",
}

# Dimensional sections with headcount data
HC_SECTIONS = {"headcount_by_department", "time_to_fill_by_department"}


def build_entity_fact_base(base_fb: dict, combining: dict, entity: str) -> dict:
    """Build complete fact_base for a single entity."""
    fb = {"metadata": copy.deepcopy(base_fb["metadata"])}
    fb["metadata"]["description"] = (
        f"Entity-specific fact base for {entity}. "
        f"Generated from combining_statements.json + base fact_base.json."
    )
    fb["metadata"]["generated_at"] = datetime.now(tz=None).strftime("%Y-%m-%dT%H:%M:%SZ")
    fb["metadata"]["generated_by"] = "generate_entity_fact_bases.py"
    fb["metadata"]["entity_id"] = entity

    periods = [
        f"{y}-Q{q}" for y in [2024, 2025, 2026] for q in [1, 2, 3, 4]
    ]

    quarterly = []
    for period in periods:
        base_q = next(
            (q for q in base_fb["quarterly"] if q["period"] == period), None
        )
        if base_q is None:
            continue

        cs_q = combining.get(period)
        if cs_q is None:
            continue

        pl = get_entity_pl(cs_q, entity)
        entity_revenue = pl.get("revenue", 0)
        base_revenue = base_q["revenue"]
        base_headcount = base_q.get("headcount", 235)

        eq = build_entity_quarter(
            base_q, pl, entity,
            base_revenue, entity_revenue, base_headcount
        )
        quarterly.append(eq)

    fb["quarterly"] = quarterly

    # Build annual summaries
    fb["annual"] = build_annual(quarterly)

    # Scale dimensional sections
    # Use average revenue scale across all quarters
    base_revenues = [q["revenue"] for q in base_fb["quarterly"]]
    entity_revenues = [q["revenue"] for q in quarterly]
    avg_rev_scale = (
        sum(entity_revenues) / sum(base_revenues)
        if sum(base_revenues) > 0 else 1.0
    )

    # Headcount scale
    target_hc = ENTITY_HEADCOUNT.get(entity, 235)
    base_hc = base_fb["quarterly"][0].get("headcount", 235)
    hc_scale = target_hc / base_hc if base_hc else 1.0

    for section_key in base_fb:
        if section_key in ("metadata", "quarterly", "annual"):
            continue
        section = base_fb[section_key]
        if not isinstance(section, dict):
            continue

        if section_key in PCT_SECTIONS:
            fb[section_key] = copy.deepcopy(section)
        elif section_key in HC_SECTIONS:
            fb[section_key] = scale_dimensional_section(section, hc_scale)
        else:
            fb[section_key] = scale_dimensional_section(section, avg_rev_scale)

    return fb


def build_combined_fact_base(base_fb: dict, combining: dict,
                             meridian_fb: dict, cascadia_fb: dict) -> dict:
    """Build combined entity fact_base."""
    fb = {"metadata": copy.deepcopy(base_fb["metadata"])}
    fb["metadata"]["description"] = (
        "Combined entity fact base (meridian + cascadia with adjustments). "
        "Generated from combining_statements.json + base fact_base.json."
    )
    fb["metadata"]["generated_at"] = datetime.now(tz=None).strftime("%Y-%m-%dT%H:%M:%SZ")
    fb["metadata"]["generated_by"] = "generate_entity_fact_bases.py"
    fb["metadata"]["entity_id"] = "combined"

    periods = [
        f"{y}-Q{q}" for y in [2024, 2025, 2026] for q in [1, 2, 3, 4]
    ]

    quarterly = []
    for period in periods:
        base_q = next(
            (q for q in base_fb["quarterly"] if q["period"] == period), None
        )
        m_q = next(
            (q for q in meridian_fb["quarterly"] if q["period"] == period), None
        )
        c_q = next(
            (q for q in cascadia_fb["quarterly"] if q["period"] == period), None
        )
        cs_q = combining.get(period)

        if not all([base_q, m_q, c_q, cs_q]):
            continue

        pl_combined = get_entity_pl(cs_q, "combined")
        cq = build_combined_quarter(m_q, c_q, base_q, pl_combined)
        quarterly.append(cq)

    fb["quarterly"] = quarterly
    fb["annual"] = build_annual(quarterly)

    # Dimensional sections: sum meridian + cascadia
    for section_key in base_fb:
        if section_key in ("metadata", "quarterly", "annual"):
            continue
        section = base_fb[section_key]
        if not isinstance(section, dict):
            continue

        m_section = meridian_fb.get(section_key, section)
        c_section = cascadia_fb.get(section_key, section)

        if section_key in PCT_SECTIONS:
            fb[section_key] = copy.deepcopy(section)
        else:
            fb[section_key] = combine_dimensional_sections(m_section, c_section)

    return fb


def build_annual(quarterly: list) -> list:
    """Build annual summaries from quarterly data."""
    by_year = {}
    for q in quarterly:
        year = q["year"]
        if year not in by_year:
            by_year[year] = []
        by_year[year].append(q)

    annual = []
    for year in sorted(by_year.keys()):
        quarters = by_year[year]
        if not quarters:
            continue

        ann = {"period": str(year), "year": year, "quarter": "FY"}

        # Collect all numeric keys
        numeric_keys = [
            k for k in quarters[0]
            if isinstance(quarters[0][k], (int, float))
            and not isinstance(quarters[0][k], bool)
            and k not in STRUCTURAL_KEYS
        ]

        # Summed metrics (flow metrics: revenue, expenses, etc.)
        # vs. averaged metrics (stock/rate metrics: percentages, ratios)
        for key in numeric_keys:
            values = [q.get(key, 0) for q in quarters]
            if key in NO_SCALE_METRICS or key in DERIVED_METRICS:
                # Average for rates/percentages/derived
                ann[key] = round(sum(values) / len(values), 2)
            elif key in {"headcount", "engineering_headcount", "sales_headcount",
                         "cs_headcount", "marketing_headcount", "product_headcount",
                         "finance_headcount", "ga_headcount", "open_roles",
                         "cash", "ar", "total_assets", "total_liabilities",
                         "retained_earnings", "stockholders_equity",
                         "deferred_revenue", "deferred_revenue_current",
                         "deferred_revenue_lt", "pp_e", "intangibles", "goodwill",
                         "unbilled_revenue", "prepaid_expenses", "ap",
                         "accrued_expenses", "customer_count",
                         "beginning_arr", "arr", "mrr"}:
                # Point-in-time / stock metrics: use last quarter
                ann[key] = quarters[-1].get(key, 0)
            else:
                # Flow metrics: sum
                if isinstance(quarters[0].get(key), int):
                    ann[key] = round(sum(values))
                else:
                    ann[key] = round(sum(values), 2)

        ann["is_forecast"] = any(q.get("is_forecast", False) for q in quarters)
        annual.append(ann)

    return annual


def main():
    print("Loading source data...")
    base_fb = load_json(DATA_DIR / "fact_base.json")
    combining = load_json(DATA_DIR / "combining_statements.json")

    # Filter out non-quarter keys from combining_statements
    quarter_keys = [k for k in combining if k.startswith("20")]
    combining_quarters = {k: combining[k] for k in quarter_keys}

    print(f"  Base fact_base: {len(base_fb['quarterly'])} quarters, "
          f"{len(base_fb['quarterly'][0])} metrics each")
    print(f"  Combining statements: {len(combining_quarters)} quarters")

    # Spot check: show Q1 2024 revenue for each entity
    q1 = combining_quarters["2024-Q1"]
    rev_li = extract_line_item(q1["line_items"], "Total Revenue")
    print(f"\n  2024-Q1 Revenue — Meridian: ${rev_li['meridian']}M, "
          f"Cascadia: ${rev_li['cascadia']}M, Combined: ${rev_li['combined']}M")
    print(f"  Base fact_base 2024-Q1 revenue: ${base_fb['quarterly'][0]['revenue']}M")

    # Build entity fact bases
    print("\nGenerating meridian fact_base...")
    meridian_fb = build_entity_fact_base(base_fb, combining_quarters, "meridian")

    print("Generating cascadia fact_base...")
    cascadia_fb = build_entity_fact_base(base_fb, combining_quarters, "cascadia")

    print("Generating combined fact_base...")
    combined_fb = build_combined_fact_base(
        base_fb, combining_quarters, meridian_fb, cascadia_fb
    )

    # Write output files
    entities = {
        "meridian": meridian_fb,
        "cascadia": cascadia_fb,
        "combined": combined_fb,
    }

    for entity, fb in entities.items():
        out_path = DATA_DIR / f"fact_base_{entity}.json"
        with open(out_path, "w") as f:
            json.dump(fb, f, indent=2)
        n_quarters = len(fb["quarterly"])
        n_metrics = len(fb["quarterly"][0]) if fb["quarterly"] else 0
        print(f"  Wrote {out_path.name}: {n_quarters} quarters, {n_metrics} metrics")

    # Validation summary
    print("\n── Validation ──")
    for entity in ["meridian", "cascadia", "combined"]:
        fb = entities[entity]
        q1 = fb["quarterly"][0]
        q12 = fb["quarterly"][-1]
        print(f"\n  {entity.upper()}")
        print(f"    Q1 2024: rev=${q1['revenue']}M, gp=${q1['gross_profit']}M, "
              f"gm={q1['gross_margin_pct']}%, ebitda=${q1['ebitda']}M, "
              f"ni=${q1['net_income']}M, hc={q1['headcount']}")
        print(f"    Q4 2026: rev=${q12['revenue']}M, gp=${q12['gross_profit']}M, "
              f"gm={q12['gross_margin_pct']}%, ebitda=${q12['ebitda']}M, "
              f"ni=${q12['net_income']}M, hc={q12['headcount']}")

    # Cross-check: combined should roughly equal meridian + cascadia for additive metrics
    print("\n  CROSS-CHECK (Q1 2024 — combined vs meridian+cascadia):")
    mq1 = entities["meridian"]["quarterly"][0]
    cq1 = entities["cascadia"]["quarterly"][0]
    xq1 = entities["combined"]["quarterly"][0]
    for metric in ["revenue", "cogs", "gross_profit", "opex", "ebitda",
                   "net_income", "headcount"]:
        m_plus_c = round(mq1[metric] + cq1[metric], 2)
        combined_val = xq1[metric]
        diff = round(combined_val - m_plus_c, 2)
        flag = " (adjustments)" if abs(diff) > 0.01 else ""
        print(f"    {metric}: M+C={m_plus_c}, combined={combined_val}, "
              f"diff={diff}{flag}")

    print("\nDone.")


if __name__ == "__main__":
    main()
