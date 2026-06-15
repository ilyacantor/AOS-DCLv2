"""Operational metrics records -> canonical concept triples (records-path SE cutover, op half).

Sibling of financial_records_aggregator. The financial statements are one source plane (ERP);
the operational KPIs (sales/pipeline, workforce, engineering, uptime/incidents, support, customer
retention) are the BI-metrics plane: the source computes them per period and exports them as a
metrics record, and DCL forms the canonical concept the dashboards resolve. Concept formation
lives in DCL (this map) — not pre-formed in Farm and pushed via ingest-triples (the crutch).

domain == "operations" pipe shape: records = [ {"period": "2024-Q1", "sales_pipeline_total": ...,
"headcount_total": ..., "p1_incidents": ..., "nrr": ..., ...}, ... ] (one per period). DCL maps
each known field to its (concept, property) and emits a period-stamped triple. Unknown fields
warn loud (A1). Concepts/properties are exactly what nlq/config/metric_concept_map.yaml resolves,
so the CRO/COO/CTO/CHRO tiles light up with zero NLQ change. Deterministic 0.95/exact.
"""
from __future__ import annotations

from typing import Any, Optional

from backend.api.routes.ingest_triples import TriplePayload

_CONF = 0.95
_TIER = "exact"

# Source metric field -> (canonical concept, property, unit). The metric catalog DCL owns —
# the operational counterpart to the chart-of-accounts map. Source-flavoured field names (the
# BI/warehouse export columns); the canonical concept is DCL's to assign. Concepts/properties
# match metric_concept_map.yaml exactly; values are produced by Farm's SE generators.
OPERATIONAL_FIELD_CONCEPTS: dict[str, tuple[str, str, str]] = {
    # ── Sales (CRO) ──────────────────────────────────────────────────
    "sales_pipeline_total": ("sales.pipeline.total", "amount", "usd"),
    "bookings_total": ("sales.bookings.total", "amount", "usd"),
    "avg_deal_size": ("sales.avg_deal_size", "amount", "usd"),
    "win_rate": ("sales.win_rate", "rate", "pct"),
    "sales_cycle_days": ("sales.cycle_days", "days", "days"),
    "quota_attainment": ("sales.quota_attainment", "rate", "pct"),
    # ── Pipeline stages (CRO) ────────────────────────────────────────
    "pipeline_lead": ("customer.pipeline.lead", "amount", "usd"),
    "pipeline_qualified": ("customer.pipeline.qualified", "amount", "usd"),
    "pipeline_proposal": ("customer.pipeline.proposal", "amount", "usd"),
    "pipeline_negotiation": ("customer.pipeline.negotiation", "amount", "usd"),
    "pipeline_closed_won": ("customer.pipeline.closed_won", "amount", "usd"),
    # ── Revenue mix (CRO) ────────────────────────────────────────────
    "new_logo_revenue": ("revenue.new_logo", "amount", "usd"),
    "expansion_revenue": ("revenue.expansion", "amount", "usd"),
    "renewal_revenue": ("revenue.renewal", "amount", "usd"),
    "arr_ending": ("arr.ending", "amount", "usd"),
    "arr_beginning": ("arr.beginning", "amount", "usd"),
    # ── Customer retention / CS (CRO/CCO) ────────────────────────────
    "new_customers": ("customer.count.new", "count", "count"),
    "nrr": ("customer.nrr", "rate", "pct"),
    "gross_churn_rate": ("customer.gross_churn_rate", "rate", "pct"),
    "logo_churn_rate": ("customer.logo_churn_rate", "rate", "pct"),
    "ltv_cac_ratio": ("customer.ltv_cac_ratio", "rate", "ratio"),
    "nps": ("customer.nps", "score", "score"),
    "csat": ("support.csat", "score", "score"),
    "resolution_hours": ("support.resolution_time", "hours", "hours"),
    "first_response_hours": ("support.first_response_time", "hours", "hours"),
    # ── Workforce (CHRO/COO) ─────────────────────────────────────────
    "headcount_total": ("workforce.headcount.total", "count", "count"),
    "headcount_engineering": ("workforce.headcount.engineering", "count", "count"),
    "headcount_sales": ("workforce.headcount.sales", "count", "count"),
    "headcount_customer_success": ("workforce.headcount.by_department.customer_success", "count", "count"),
    "headcount_product": ("workforce.headcount.by_department.product", "count", "count"),
    "headcount_marketing": ("workforce.headcount.by_department.marketing", "count", "count"),
    "headcount_g_and_a": ("workforce.headcount.by_department.g&a", "count", "count"),
    "hires": ("workforce.hires", "count", "count"),
    "terminations": ("workforce.terminations", "count", "count"),
    "attrition_rate": ("workforce.attrition_rate", "rate", "pct"),
    # ── Engineering / reliability (CTO) ──────────────────────────────
    "sprint_velocity": ("sprint_velocity.team", "points", "points"),
    "features_shipped": ("engineering.features_shipped", "count", "count"),
    "tech_debt_rate": ("engineering.tech_debt_rate", "rate", "pct"),
    "deploy_frequency": ("deploy_frequency.quarterly", "count", "count"),
    "p1_incidents": ("incident_count.p1", "count", "count"),
    "downtime_hours": ("infrastructure.downtime", "hours", "hours"),
    "mttr_p1_hours": ("infrastructure.mttr.p1", "hours", "hours"),
    "uptime_overall": ("uptime_pct.overall", "rate", "pct"),
    "uptime_trend": ("uptime_trend.quarterly", "rate", "pct"),
    "uptime_auth_api": ("uptime_by_service.auth-api", "rate", "pct"),
}

# Dimensional breakdowns: nested {member: value} fields -> concept "<metric>.by_<dimension>" with
# property=member (the shape NLQ's dashboard_data_resolver._resolve_triple_breakdown looks for, so
# the map/bar/donut tiles populate instead of "No <dim> breakdown data for '<metric>'").
_NESTED_BREAKDOWNS = {
    "revenue_by_region": ("revenue.by_region", "usd"),
    "arr_by_region": ("arr.by_region", "usd"),
    "arr_by_customer": ("arr.by_customer", "usd"),
    "headcount_by_department": ("headcount.by_department", "count"),
    "uptime_pct_by_service": ("uptime_pct.by_service", "pct"),
    # ── Attrition drivers (CHRO) — the "what's driving attrition" data ────────
    # Trailing-window exit cohort by reason (dept->count), internal comp band
    # (dept->usd), and external market median (job_family->usd). property=member
    # (dept / job_family); currency="USD" emitted for the usd breakdowns. The
    # comp-gap compare = comp_band.median (workday_hr) vs market_benchmark.median
    # (radford_comp) — the cross-source beat resolved in DCL.
    "exit_compensation_by_department": ("workforce.exit_theme.compensation.by_department", "count"),
    "exit_growth_by_department": ("workforce.exit_theme.growth.by_department", "count"),
    "exit_management_by_department": ("workforce.exit_theme.management.by_department", "count"),
    "exit_worklife_by_department": ("workforce.exit_theme.work_life.by_department", "count"),
    "comp_band_median_by_department": ("comp_band.median.by_department", "usd"),
    "market_median_by_job_family": ("market_benchmark.median.by_job_family", "usd"),
    # ── Team/band structure + attrition (ContextOS #3) ────────────────────────
    # GENERIC team cuts — the source column names carry no entity specifics.
    # A workforce-departure feed cut by org team and by team+band (property=
    # team / property="<team>:<band>"); band-level internal comp + market
    # median (property="<dept>:<band>" / "<family>:<band>"). The team/band/dept
    # KEYS are data (the entity's), never named in any derivation rule. These
    # let the stitched graph walk org -> department -> team and let the
    # synthesis rule tie a team's band-concentrated departures to a band driver.
    "departures_by_team": ("workforce.departures.by_team", "count"),
    "departures_by_team_band": ("workforce.departures.by_team_band", "count"),
    "comp_band_median_by_department_band": ("comp_band.median.by_department_band", "usd"),
    "market_median_by_job_family_band": ("market_benchmark.median.by_job_family_band", "usd"),
}

# String-valued breakdowns: nested {member: "<text>"} fields whose VALUE is a
# business key / declared mapping, not a number. Same property=member shape as
# _NESTED_BREAKDOWNS but the value is stored as text (jsonb string) — the _num
# numeric path would drop these, so they get their own classifier branch. No
# unit/currency (the value is a key, not a measure).
#   workforce.team.member_of  — value "<parent_department>|<senior_roster>": a
#       team's parent department (DATA the membership rule reads) + its senior
#       bench size. The structural department->team hop derives from this.
#   comp_band.resolution.department_to_job_family — value the external job family
#       an internal department resolves to: the DECLARED mapping the gap rule
#       reads so the internal->external comp pair is resolved from data, not a
#       dict. Nested under the registered comp_band root (comp-domain metadata —
#       no new ontology root, no Convergence-coordinated concept addition).
_STRING_BREAKDOWNS: dict[str, str] = {
    "team_membership": "workforce.team.member_of",
    "comp_resolution_department_to_job_family": "comp_band.resolution.department_to_job_family",
}

_PERIOD_KEY = "period"
_STRUCTURAL_KEYS = frozenset({_PERIOD_KEY, "id"})


def _num(v: Any) -> Optional[float]:
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        return None
    return float(v)


def aggregate_operational_records(
    *, entity_id: str, pipe: dict, records: list[dict], warnings: list[dict],
) -> list[TriplePayload]:
    """Classify period-keyed operational-metric records into canonical concept triples."""
    source_system = pipe.get("source_system")
    fabric_plane = pipe.get("fabric_plane")
    fabric_product = pipe.get("fabric_product")
    pipe_id = str(pipe.get("pipe_id"))
    raw_source = pipe.get("source_system")

    payloads: list[TriplePayload] = []
    for rec_idx, record in enumerate(records):
        period = record.get(_PERIOD_KEY)
        period = str(period) if period is not None and str(period).strip() else None

        for fname, raw_value in record.items():
            if fname in _STRUCTURAL_KEYS:
                continue
            if fname in _STRING_BREAKDOWNS:
                base = _STRING_BREAKDOWNS[fname]
                if isinstance(raw_value, dict):
                    for member, mval in raw_value.items():
                        # The value is a business key/declared mapping — keep it as
                        # text. Empty/None is a data integrity gap, warn loud (A1):
                        # a string breakdown with a blank value cannot carry a key.
                        if mval is None or not str(mval).strip():
                            warnings.append({
                                "type": "empty_string_breakdown_value", "pipe_id": pipe_id,
                                "field": fname, "member": str(member), "record_index": rec_idx,
                                "detail": (
                                    f"string-valued breakdown '{fname}' member '{member}' has "
                                    f"an empty value; cannot form concept '{base}' — not converted"
                                ),
                            })
                            continue
                        payloads.append(TriplePayload(
                            entity_id=entity_id, concept=base, property=str(member),
                            value=str(mval), period=period, currency=None, unit=None,
                            source_system=source_system, source_table=f"fabric_via:{raw_source}",
                            source_field=f"{fname}.{member}", pipe_id=pipe_id,
                            confidence_score=_CONF, confidence_tier=_TIER,
                            fabric_plane=fabric_plane, fabric_product=fabric_product,
                        ))
                continue
            if fname in _NESTED_BREAKDOWNS:
                base, unit = _NESTED_BREAKDOWNS[fname]
                # The MAP resolver wants concept="<metric>.by_region.<region>" (region in the
                # concept); the bar/donut resolver wants concept="<metric>.by_<dim>" with
                # property=member. Region breakdowns take the former, all others the latter.
                region_in_concept = base.endswith(".by_region")
                if isinstance(raw_value, dict):
                    for member, mval in raw_value.items():
                        v = _num(mval)
                        if v is None:
                            continue
                        concept = f"{base}.{member}" if region_in_concept else base
                        prop = "amount" if region_in_concept else str(member)
                        payloads.append(TriplePayload(
                            entity_id=entity_id, concept=concept, property=prop, value=v,
                            period=period, currency="USD" if unit == "usd" else None, unit=unit,
                            source_system=source_system, source_table=f"fabric_via:{raw_source}",
                            source_field=f"{fname}.{member}", pipe_id=pipe_id,
                            confidence_score=_CONF, confidence_tier=_TIER,
                            fabric_plane=fabric_plane, fabric_product=fabric_product,
                        ))
                continue
            mapping = OPERATIONAL_FIELD_CONCEPTS.get(fname)
            if mapping is None:
                warnings.append({
                    "type": "unmapped_operational_field", "pipe_id": pipe_id,
                    "field": fname, "record_index": rec_idx,
                    "detail": (
                        f"operational field '{fname}' has no concept mapping in "
                        f"OPERATIONAL_FIELD_CONCEPTS; not converted"
                    ),
                })
                continue
            value = _num(raw_value)
            if value is None:
                continue
            concept, prop, unit = mapping
            payloads.append(TriplePayload(
                entity_id=entity_id, concept=concept, property=prop, value=value,
                period=period, currency="USD" if unit == "usd" else None, unit=unit,
                source_system=source_system, source_table=f"fabric_via:{raw_source}",
                source_field=fname, pipe_id=pipe_id,
                confidence_score=_CONF, confidence_tier=_TIER,
                fabric_plane=fabric_plane, fabric_product=fabric_product,
            ))
    return payloads
