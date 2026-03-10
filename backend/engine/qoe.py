"""
Quality of Earnings (QofE) Engine.

Produces a quarterly QofE report with 6 sections:
1. EBITDA bridge with temporal context (current/diligence/prior per adjustment)
2. Adjustment lifecycle tracking
3. Revenue quality analysis (concentration, contract quality, mix, retention, cross-sell)
4. Earnings sustainability score (0-100 composite)
5. Working capital analysis (DSO, DPO, bench cost trends)
6. New item detection

QofE is not a one-time diligence artifact — it runs every quarter against the
latest financials and produces an updated assessment of earnings quality.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any

from backend.engine.engagement import get_active_engagement

logger = logging.getLogger("dcl.backend.engine.qoe")

_DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"
_PRIOR_FILE = _DATA_DIR / "qoe_prior.json"

# Diligence quarter — the quarter when original bridge was produced
_DILIGENCE_QUARTER = "2025-Q4"
# Latest available quarter
_LATEST_QUARTER = "2025-Q4"


# ── Data Loading ─────────────────────────────────────────────────────────────


def _load_json(filename: str) -> dict:
    path = _DATA_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"QofE data file not found: {path}")
    with open(path) as f:
        return json.load(f)


def _load_combining() -> dict:
    return _load_json("combining_statements.json")


def _load_overlap() -> dict:
    return _load_json("entity_overlap.json")


def _load_customers() -> dict:
    return _load_json("customer_profiles.json")


def _load_prior_snapshot() -> dict | None:
    if _PRIOR_FILE.exists():
        with open(_PRIOR_FILE) as f:
            return json.load(f)
    return None


# ── Section 1: EBITDA Bridge with Temporal Context ───────────────────────────


@dataclass
class QofEAdjustmentRow:
    name: str
    category: str
    entity: str
    confidence: str
    current_amount: float
    diligence_amount: float | None
    prior_amount: float | None
    amount_low: float
    amount_high: float
    lever: str | None
    support_reference: str
    rationale: str
    status: str          # active | resolved | new | changed
    lifecycle_stage: str  # identified | validated | ongoing | resolved | escalated | new
    trend: str           # improving | stable | worsening

    def to_dict(self) -> dict:
        return asdict(self)


def _compute_adjustment_status(
    current: float,
    prior: float | None,
    diligence: float | None,
) -> tuple[str, str, str]:
    """Determine status, lifecycle_stage, and trend for an adjustment."""
    # No prior → first run or new item
    if prior is None and diligence is None:
        return "new", "identified", "stable"

    if prior is None:
        prior = diligence  # use diligence as baseline

    # Resolved — current is effectively zero
    if abs(current) < 1000:  # <$1K = resolved
        return "resolved", "resolved", "improving"

    # Changed — >10% delta from prior
    if prior and abs(prior) > 1000:
        delta_pct = abs(current - prior) / abs(prior)
        if delta_pct > 0.10:
            trend = "improving" if abs(current) < abs(prior) else "worsening"
            lifecycle = "escalated" if trend == "worsening" else "ongoing"
            return "changed", lifecycle, trend

    # Active — still applies, roughly same magnitude
    # Determine trend from diligence if available
    trend = "stable"
    if diligence and abs(diligence) > 1000:
        delta_from_diligence = abs(current) - abs(diligence)
        if delta_from_diligence < -abs(diligence) * 0.05:
            trend = "improving"
        elif delta_from_diligence > abs(diligence) * 0.05:
            trend = "worsening"
    return "active", "ongoing", trend


def _build_bridge_with_context(
    bridge: dict,
    prior_snapshot: dict | None,
) -> list[dict]:
    """Wrap each bridge adjustment with temporal context."""
    prior_adjs = {}
    if prior_snapshot and "ebitda_bridge" in prior_snapshot:
        for pa in prior_snapshot["ebitda_bridge"]:
            prior_adjs[pa["name"]] = pa

    diligence_adjs = {}
    if prior_snapshot and "diligence_bridge" in prior_snapshot:
        for da in prior_snapshot["diligence_bridge"]:
            diligence_adjs[da["name"]] = da

    rows = []
    all_adjustments = bridge.get("entity_adjustments", []) + bridge.get("combination_synergies", [])

    for adj in all_adjustments:
        name = adj["name"]
        current_amount = adj["amount"]
        prior_adj = prior_adjs.get(name)
        diligence_adj = diligence_adjs.get(name)

        prior_amount = prior_adj["current_amount"] if prior_adj else None
        diligence_amount = diligence_adj.get("current_amount", diligence_adj.get("amount")) if diligence_adj else None

        # On first run (no prior), diligence = current
        if prior_amount is None and diligence_amount is None:
            diligence_amount = current_amount

        status, lifecycle, trend = _compute_adjustment_status(
            current_amount, prior_amount, diligence_amount,
        )

        rows.append(QofEAdjustmentRow(
            name=name,
            category=adj["category"],
            entity=adj["entity"],
            confidence=adj["confidence"],
            current_amount=current_amount,
            diligence_amount=diligence_amount,
            prior_amount=prior_amount,
            amount_low=adj["amount_low"],
            amount_high=adj["amount_high"],
            lever=adj.get("lever"),
            support_reference=adj.get("support_reference", ""),
            rationale=adj.get("rationale", ""),
            status=status,
            lifecycle_stage=lifecycle,
            trend=trend,
        ).to_dict())

    return rows


# ── Section 2: Adjustment Lifecycle Tracking ─────────────────────────────────


def _compute_lifecycle_summary(bridge_rows: list[dict]) -> dict:
    """Summary counts and lists per lifecycle stage."""
    stages = {"identified": [], "validated": [], "ongoing": [], "resolved": [], "escalated": [], "new": []}
    for row in bridge_rows:
        stage = row["lifecycle_stage"]
        if stage in stages:
            stages[stage].append(row["name"])

    status_counts = {"active": 0, "resolved": 0, "new": 0, "changed": 0}
    for row in bridge_rows:
        s = row["status"]
        if s in status_counts:
            status_counts[s] += 1

    return {
        "lifecycle_stages": {k: {"count": len(v), "items": v} for k, v in stages.items()},
        "status_counts": status_counts,
        "total_adjustments": len(bridge_rows),
    }


# ── Section 3: Revenue Quality Analysis ──────────────────────────────────────


def _compute_revenue_quality(
    combining: dict,
    overlap: dict,
    customers: dict,
    cross_sell: dict,
) -> dict:
    """Revenue quality metrics: concentration, contract quality, mix, retention, cross-sell."""

    # --- Customer concentration ---
    eng = get_active_engagement()
    entity_a_id, entity_b_id = eng.entity_ids()
    all_customers = customers.get(f"{entity_a_id}_customers", []) + customers.get(f"{entity_b_id}_customers", [])
    revenues = sorted([c["engagement_value_M"] for c in all_customers if c.get("engagement_value_M", 0) > 0], reverse=True)
    total_rev = sum(revenues)
    top_10_rev = sum(revenues[:10])
    top_20_rev = sum(revenues[:20])
    top_50_rev = sum(revenues[:50])
    top_10_pct = (top_10_rev / total_rev * 100) if total_rev > 0 else 0
    top_20_pct = (top_20_rev / total_rev * 100) if total_rev > 0 else 0
    top_50_pct = (top_50_rev / total_rev * 100) if total_rev > 0 else 0

    # HHI (Herfindahl-Hirschman Index) — sum of squared market shares
    shares = [(r / total_rev * 100) for r in revenues] if total_rev > 0 else []
    hhi = sum(s * s for s in shares)

    # Concentration threshold alerts
    threshold_alerts = []
    for c in all_customers:
        if total_rev > 0 and c.get("engagement_value_M", 0) > 0:
            pct = c["engagement_value_M"] / total_rev * 100
            if pct >= 10:
                threshold_alerts.append({"customer": c["customer_name"], "pct": round(pct, 2), "threshold": "10%"})
            elif pct >= 5:
                threshold_alerts.append({"customer": c["customer_name"], "pct": round(pct, 2), "threshold": "5%"})

    # --- Contract quality ---
    contract_counts = {"MSA": 0, "SOW": 0, "T&M": 0}
    tenure_sum = 0
    tenure_count = 0
    for c in all_customers:
        ct = c.get("contract_type", "")
        if ct in contract_counts:
            contract_counts[ct] += 1
        yrs = c.get("years_as_client", 0)
        if yrs > 0:
            tenure_sum += yrs
            tenure_count += 1

    total_contracts = sum(contract_counts.values()) or 1
    msa_pct = contract_counts["MSA"] / total_contracts * 100
    avg_tenure = tenure_sum / tenure_count if tenure_count > 0 else 0

    # --- Revenue mix ---
    latest_q = combining.get(_LATEST_QUARTER, {})
    line_items = latest_q.get("line_items", [])
    li_map = {li["line_item"]: li for li in line_items}

    total_revenue_q = li_map.get("Total Revenue", {}).get("combined", 0)
    advisory_rev = li_map.get("Advisory & Consulting Revenue", {}).get("combined", 0)
    managed_rev = li_map.get("Managed Services Revenue", {}).get("combined", 0)
    per_fte_rev = li_map.get("Per-FTE Revenue", {}).get("combined", 0)
    per_txn_rev = li_map.get("Per-Transaction Revenue", {}).get("combined", 0)

    recurring_rev = managed_rev + per_fte_rev + per_txn_rev
    non_recurring_rev = advisory_rev
    recurring_pct = (recurring_rev / total_revenue_q * 100) if total_revenue_q > 0 else 0

    # --- Cohort retention ---
    cohorts: dict[int, float] = {}
    for c in all_customers:
        yrs = c.get("years_as_client", 0)
        val = c.get("engagement_value_M", 0)
        if yrs > 0 and val > 0:
            cohorts.setdefault(yrs, 0)
            cohorts[yrs] += val

    cohort_retention = [
        {"years_as_client": yr, "total_revenue_M": round(rev, 2)}
        for yr, rev in sorted(cohorts.items())
    ]

    # --- Cross-sell penetration ---
    cs_summary = cross_sell.get("summary", {})
    total_candidates = cs_summary.get("total_candidates", 0)
    total_pipeline_acv = cs_summary.get("total_pipeline_acv", 0)
    # Conversion is tracked over time; first QofE has 0 converted
    converted_count = 0  # placeholder — updated in ongoing QofE runs
    converted_acv = 0

    return {
        "customer_concentration": {
            "hhi": round(hhi, 1),
            "top_10_pct": round(top_10_pct, 2),
            "top_20_pct": round(top_20_pct, 2),
            "top_50_pct": round(top_50_pct, 2),
            "threshold_alerts": threshold_alerts[:10],  # top 10
            "total_customers": len(all_customers),
        },
        "contract_quality": {
            "msa_pct": round(msa_pct, 1),
            "sow_pct": round(contract_counts["SOW"] / total_contracts * 100, 1),
            "t_and_m_pct": round(contract_counts["T&M"] / total_contracts * 100, 1),
            "avg_tenure_years": round(avg_tenure, 1),
        },
        "revenue_mix": {
            "recurring_pct": round(recurring_pct, 1),
            "non_recurring_pct": round(100 - recurring_pct, 1),
            "advisory_consulting_M": round(advisory_rev, 2),
            "managed_services_M": round(managed_rev, 2),
            "per_fte_M": round(per_fte_rev, 2),
            "per_transaction_M": round(per_txn_rev, 2),
        },
        "cohort_retention": cohort_retention,
        "cross_sell_penetration": {
            "total_candidates": total_candidates,
            "total_pipeline_acv_M": round(total_pipeline_acv / 1_000_000, 2) if total_pipeline_acv > 1_000_000 else round(total_pipeline_acv, 2),
            "converted_count": converted_count,
            "converted_acv_M": converted_acv,
            "conversion_rate_pct": 0,
        },
    }


# ── Section 4: Earnings Sustainability Score ─────────────────────────────────


def _compute_sustainability_score(
    bridge_rows: list[dict],
    revenue_quality: dict,
    working_capital: dict,
    combining: dict,
) -> dict:
    """
    Composite 0-100 score: how much of this EBITDA is sustainable?
    6 components, each 0-100, weighted.
    """

    # 1. Contract duration proxy (0-25 pts) — % MSA contracts (long-term)
    msa_pct = revenue_quality["contract_quality"]["msa_pct"]
    # MSA >50% → full score; linear below
    contract_score = min(100, msa_pct * 2)

    # 2. Customer concentration below thresholds (0-15 pts)
    hhi = revenue_quality["customer_concentration"]["hhi"]
    # Lower HHI = better. HHI < 200 = unconcentrated, > 2500 = highly concentrated
    if hhi < 200:
        concentration_score = 100
    elif hhi < 1500:
        concentration_score = 100 - ((hhi - 200) / 1300) * 60
    else:
        concentration_score = max(0, 40 - ((hhi - 1500) / 1000) * 40)

    # 3. Adjustment stability (0-20 pts) — fewer/smaller adjustments = higher
    total_adj = len(bridge_rows)
    new_or_changed = sum(1 for r in bridge_rows if r["status"] in ("new", "changed"))
    resolved = sum(1 for r in bridge_rows if r["status"] == "resolved")
    if total_adj > 0:
        stability_ratio = 1.0 - (new_or_changed / total_adj)
        resolved_bonus = resolved / total_adj * 0.2
        adjustment_score = min(100, (stability_ratio + resolved_bonus) * 100)
    else:
        adjustment_score = 100

    # 4. Working capital health (0-15 pts) — DSO/DPO not deteriorating
    dso_trend = working_capital.get("dso_trend", [])
    if len(dso_trend) >= 2:
        recent_dso = dso_trend[-1]["value"]
        prior_dso = dso_trend[-2]["value"]
        if recent_dso <= prior_dso:
            wc_score = 80 + min(20, (prior_dso - recent_dso))
        else:
            wc_score = max(0, 80 - (recent_dso - prior_dso) * 4)
    else:
        wc_score = 60  # insufficient data

    # 5. Margin stability (0-15 pts) — gross and EBITDA margin not declining
    periods = sorted([k for k in combining.keys() if not k.startswith("_")])
    if len(periods) >= 2:
        curr = combining[periods[-1]]["line_items"]
        prev = combining[periods[-2]]["line_items"]
        curr_map = {li["line_item"]: li for li in curr}
        prev_map = {li["line_item"]: li for li in prev}

        curr_gp = curr_map.get("Gross Profit", {}).get("combined", 0)
        curr_rev = curr_map.get("Total Revenue", {}).get("combined", 0)
        prev_gp = prev_map.get("Gross Profit", {}).get("combined", 0)
        prev_rev = prev_map.get("Total Revenue", {}).get("combined", 0)

        curr_margin = (curr_gp / curr_rev * 100) if curr_rev else 0
        prev_margin = (prev_gp / prev_rev * 100) if prev_rev else 0

        if curr_margin >= prev_margin:
            margin_score = 90
        else:
            decline = prev_margin - curr_margin
            margin_score = max(0, 90 - decline * 10)
    else:
        margin_score = 60

    # 6. Revenue from organic sources (0-10 pts) — recurring % proxy
    recurring_pct = revenue_quality["revenue_mix"]["recurring_pct"]
    organic_score = min(100, recurring_pct * 1.5)

    # Weighted composite
    components = [
        {"name": "Contract Duration", "score": round(contract_score, 1), "weight": 25, "max_points": 25},
        {"name": "Customer Concentration", "score": round(concentration_score, 1), "weight": 15, "max_points": 15},
        {"name": "Adjustment Stability", "score": round(adjustment_score, 1), "weight": 20, "max_points": 20},
        {"name": "Working Capital Health", "score": round(wc_score, 1), "weight": 15, "max_points": 15},
        {"name": "Margin Stability", "score": round(margin_score, 1), "weight": 15, "max_points": 15},
        {"name": "Organic Revenue", "score": round(organic_score, 1), "weight": 10, "max_points": 10},
    ]

    overall = sum(c["score"] * c["weight"] / 100 for c in components)

    return {
        "overall": round(overall, 1),
        "components": components,
        "grade": "A" if overall >= 80 else "B" if overall >= 65 else "C" if overall >= 50 else "D" if overall >= 35 else "F",
    }


# ── Section 5: Working Capital Analysis ──────────────────────────────────────


def _compute_working_capital(combining: dict) -> dict:
    """DSO, DPO, bench cost, working capital % — all trended across quarters."""
    periods = sorted([k for k in combining.keys() if not k.startswith("_")])

    dso_trend = []
    dpo_trend = []
    bench_trend = []
    wc_pct_trend = []
    margin_trend = []

    for period in periods:
        q = combining[period]
        li_map = {li["line_item"]: li for li in q.get("line_items", [])}

        revenue = li_map.get("Total Revenue", {}).get("combined", 0)
        cogs = li_map.get("Total COGS", {}).get("combined", 0)
        bench = li_map.get("Bench Costs", {}).get("combined", 0)
        gp = li_map.get("Gross Profit", {}).get("combined", 0)
        ebitda = li_map.get("EBITDA", {}).get("combined", 0)

        # DSO = AR / (Revenue / 90) for quarterly — proxy AR as 15% of quarterly revenue
        # (No AR line in combining IS, so use standard proxy)
        ar_proxy = revenue * 0.15
        dso = (ar_proxy / (revenue / 90)) if revenue > 0 else 0

        # DPO = AP / (COGS / 90) — proxy AP as 12% of quarterly COGS
        ap_proxy = abs(cogs) * 0.12
        dpo = (ap_proxy / (abs(cogs) / 90)) if cogs != 0 else 0

        # Working capital as % of revenue
        wc = ar_proxy - ap_proxy
        wc_pct = (wc / revenue * 100) if revenue > 0 else 0

        # Margins
        gp_margin = (gp / revenue * 100) if revenue > 0 else 0
        ebitda_margin = (ebitda / revenue * 100) if revenue > 0 else 0

        dso_trend.append({"period": period, "value": round(dso, 1)})
        dpo_trend.append({"period": period, "value": round(dpo, 1)})
        bench_trend.append({"period": period, "value": round(bench, 2)})
        wc_pct_trend.append({"period": period, "value": round(wc_pct, 2)})
        margin_trend.append({
            "period": period,
            "gross_margin_pct": round(gp_margin, 2),
            "ebitda_margin_pct": round(ebitda_margin, 2),
        })

    return {
        "dso_trend": dso_trend,
        "dpo_trend": dpo_trend,
        "bench_cost_trend": bench_trend,
        "working_capital_pct_trend": wc_pct_trend,
        "margin_trend": margin_trend,
    }


# ── Section 6: New Item Detection ────────────────────────────────────────────


def _detect_new_items(
    bridge_rows: list[dict],
    combining: dict,
    prior_snapshot: dict | None,
) -> list[dict]:
    """Detect items not in prior QofE or material changes without corresponding adjustments."""
    new_items = []

    # 1. New bridge adjustments (not in prior)
    prior_adj_names = set()
    if prior_snapshot and "ebitda_bridge" in prior_snapshot:
        for pa in prior_snapshot["ebitda_bridge"]:
            prior_adj_names.add(pa["name"])

    for row in bridge_rows:
        if row["status"] == "new" and row["name"] not in prior_adj_names:
            new_items.append({
                "type": "new_adjustment",
                "description": f"New adjustment: {row['name']}",
                "amount": row["current_amount"],
                "category": row["category"],
                "classification_suggestion": "one_time" if row["category"] in ("one_time", "dis_synergy") else "run_rate",
                "recommended_action": "add_to_bridge" if abs(row["current_amount"]) > 1_000_000 else "investigate",
            })

    # 2. Material changes in line items vs prior quarter
    periods = sorted([k for k in combining.keys() if not k.startswith("_")])
    if len(periods) >= 2:
        curr_q = combining[periods[-1]]
        prev_q = combining[periods[-2]]
        curr_map = {li["line_item"]: li for li in curr_q.get("line_items", [])}
        prev_map = {li["line_item"]: li for li in prev_q.get("line_items", [])}

        for li_name, curr_li in curr_map.items():
            curr_val = curr_li.get("combined", 0)
            prev_li = prev_map.get(li_name)
            if prev_li is None:
                # New line item
                if abs(curr_val) > 0.5:  # >$500K quarterly
                    new_items.append({
                        "type": "new_line_item",
                        "description": f"New line item: {li_name}",
                        "amount": curr_val * 1_000_000,  # combining is in $M
                        "category": "unknown",
                        "classification_suggestion": "investigate",
                        "recommended_action": "investigate",
                    })
            elif prev_li:
                prev_val = prev_li.get("combined", 0)
                if abs(prev_val) > 0.5:  # only flag if prior was material
                    delta_pct = abs(curr_val - prev_val) / abs(prev_val)
                    if delta_pct > 0.10:
                        # Check if there's already an adjustment covering this
                        adj_names_lower = {r["name"].lower() for r in bridge_rows}
                        li_lower = li_name.lower()
                        covered = any(li_lower in an or an in li_lower for an in adj_names_lower)
                        if not covered:
                            new_items.append({
                                "type": "material_change",
                                "description": f"{li_name}: {prev_val:.1f}M → {curr_val:.1f}M ({delta_pct*100:.0f}% change)",
                                "amount": (curr_val - prev_val) * 1_000_000,
                                "category": "investigation_needed",
                                "classification_suggestion": "normalization" if delta_pct > 0.25 else "investigate",
                                "recommended_action": "investigate",
                            })

    return new_items


# ── Main Entry Point ─────────────────────────────────────────────────────────


def compute_qoe(prior_snapshot: dict | None = None) -> dict:
    """
    Compute the full QofE report.

    Args:
        prior_snapshot: Previous QofE output for temporal comparison.
            If None, loads from data/qoe_prior.json. If that doesn't exist,
            treats this as the initial diligence run (all items are "new").

    Returns:
        Complete QofE report dict with 6 sections.
    """
    logger.info("[qoe] Computing Quality of Earnings report")

    if prior_snapshot is None:
        prior_snapshot = _load_prior_snapshot()
        if prior_snapshot:
            logger.info("[qoe] Loaded prior snapshot from %s", _PRIOR_FILE)
        else:
            logger.info("[qoe] No prior snapshot — initial diligence run")

    # Load data sources
    combining = _load_combining()
    overlap = _load_overlap()
    customers = _load_customers()

    # Compute EBITDA bridge (dependency)
    from backend.engine.ebitda_bridge import compute_ebitda_bridge
    from backend.engine.cross_sell import run_cross_sell_engine

    cross_sell = run_cross_sell_engine().to_dict()
    bridge = compute_ebitda_bridge(cross_sell)

    logger.info(
        "[qoe] Bridge loaded: %d entity adjustments, %d synergies",
        len(bridge.get("entity_adjustments", [])),
        len(bridge.get("combination_synergies", [])),
    )

    # Section 1: Bridge with temporal context
    bridge_rows = _build_bridge_with_context(bridge, prior_snapshot)
    logger.info(
        "[qoe] Bridge context: %d adjustments — %s",
        len(bridge_rows),
        {s: sum(1 for r in bridge_rows if r["status"] == s) for s in ("active", "resolved", "new", "changed")},
    )

    # Section 2: Lifecycle summary
    lifecycle = _compute_lifecycle_summary(bridge_rows)

    # Section 3: Revenue quality
    revenue_quality = _compute_revenue_quality(combining, overlap, customers, cross_sell)
    logger.info(
        "[qoe] Revenue quality: HHI=%.0f, top10=%.1f%%, recurring=%.1f%%",
        revenue_quality["customer_concentration"]["hhi"],
        revenue_quality["customer_concentration"]["top_10_pct"],
        revenue_quality["revenue_mix"]["recurring_pct"],
    )

    # Section 5: Working capital (need it for sustainability score)
    working_capital = _compute_working_capital(combining)

    # Section 4: Sustainability score
    sustainability = _compute_sustainability_score(bridge_rows, revenue_quality, working_capital, combining)
    logger.info("[qoe] Sustainability score: %.1f (%s)", sustainability["overall"], sustainability["grade"])

    # Section 6: New item detection
    new_items = _detect_new_items(bridge_rows, combining, prior_snapshot)
    logger.info("[qoe] New items detected: %d", len(new_items))

    result = {
        "period": _LATEST_QUARTER,
        "is_initial_diligence": prior_snapshot is None,
        "ebitda_bridge": bridge_rows,
        "adjustment_lifecycle": lifecycle,
        "revenue_quality": revenue_quality,
        "sustainability_score": sustainability,
        "working_capital": working_capital,
        "new_items": new_items,
        # Include bridge summary for quick reference
        "summary": {
            "reported_ebitda": bridge["reported_ebitda"]["combined_reported"],
            "entity_adjusted_ebitda": bridge["entity_adjusted_ebitda"]["combined"],
            "pro_forma_year_1": bridge["pro_forma_ebitda"]["year_1"]["current"],
            "pro_forma_steady_state": bridge["pro_forma_ebitda"]["steady_state"]["current"],
            "total_adjustments": len(bridge_rows),
            "active_adjustments": sum(1 for r in bridge_rows if r["status"] == "active"),
            "resolved_adjustments": sum(1 for r in bridge_rows if r["status"] == "resolved"),
            "new_adjustments": sum(1 for r in bridge_rows if r["status"] == "new"),
            "changed_adjustments": sum(1 for r in bridge_rows if r["status"] == "changed"),
            "sustainability_score": sustainability["overall"],
            "sustainability_grade": sustainability["grade"],
        },
    }

    return result


def save_qoe_snapshot(qoe_result: dict) -> None:
    """Save the current QofE output as the prior snapshot for next quarter."""
    snapshot = {
        "period": qoe_result["period"],
        "ebitda_bridge": qoe_result["ebitda_bridge"],
        "diligence_bridge": qoe_result.get("diligence_bridge", qoe_result["ebitda_bridge"]),
        "sustainability_score": qoe_result["sustainability_score"]["overall"],
        "summary": qoe_result["summary"],
    }
    with open(_PRIOR_FILE, "w") as f:
        json.dump(snapshot, f, indent=2)
    logger.info("[qoe] Snapshot saved to %s", _PRIOR_FILE)
