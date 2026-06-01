"""Cloud-spend fleet aggregation — raw resource records -> cloud_spend.* triples.

NLQ's cloud-spend metrics (nlq config/metric_concept_map.yaml) are direct lookups
of PRE-AGGREGATED concepts: cloud_spend.summary.total_cost,
cloud_spend.by_service.<svc>, cloud_spend.utilization.underutilized_count,
cloud_spend.savings.opportunity_count. The Farm direct-push generator emits only
raw per-resource triples, so nothing fed those metrics and NLQ answered "no data".

When AAM's records-path hands off a cloud-spend pipe (domain="cloud_spend"), DCL
computes the fleet aggregates here — cloud-spend aggregation lives in DCL ingest.
The total-spend aggregate is exact (sum of monthly_cost_usd); utilization/savings
use documented heuristics (the total metric is the verification gate).

Aggregates are atemporal (period=None) — "current state" totals — so NLQ's
period fallback (dcl_semantic_client_v2._get_metric_single_period retry-without-
period) resolves them regardless of the requested quarter. Every triple carries
the pipe's transport provenance (source_system, pipe_id, fabric_plane,
fabric_product) so the answer traces to source. confidence 0.95/exact: these are
deterministic aggregates, not heuristic field mappings.
"""
from __future__ import annotations

from typing import Any, Optional

from backend.api.routes.ingest_triples import TriplePayload

_CONCEPT_ROOT = "cloud_spend"
_CONF = 0.95
_TIER = "exact"

# A compute resource is underutilized when CPU utilization is below this percent.
# Storage (S3) carries no CPU signal and is excluded from the underutilized count.
_UNDERUTILIZED_CPU_PCT = 40.0
# Rightsizing an underutilized resource is modeled to recover this share of its
# monthly cost (documented heuristic; the total-spend metric is exact).
_RIGHTSIZE_SAVINGS_FRACTION = 0.5


def _num(v: Any) -> Optional[float]:
    # Records carry JSON numbers (monthly_cost_usd, cpu_pct). Non-numeric (None,
    # str, dict) -> None so the caller skips it; no exception-swallowing default.
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        return None
    return float(v)


def _slug(s: str) -> str:
    out = "".join(ch.lower() if ch.isalnum() else "_" for ch in str(s))
    while "__" in out:
        out = out.replace("__", "_")
    return out.strip("_") or "unknown"


def aggregate_cloud_spend(
    *, entity_id: str, pipe: dict, records: list[dict],
) -> list[TriplePayload]:
    """Compute cloud_spend.* aggregate TriplePayloads from raw resource records.

    `records` are flat field->value dicts (the AWS resource rows AAM transported).
    Returns the aggregate triples NLQ's cloud-spend metrics resolve against.
    """
    source_system = pipe.get("source_system") or "aws_cost_explorer"
    fabric_plane = pipe.get("fabric_plane")
    fabric_product = pipe.get("fabric_product")
    pipe_id = str(pipe.get("pipe_id"))

    def _t(concept: str, prop: str, value: Any, *,
           unit: Optional[str] = None, currency: Optional[str] = None,
           source_field: Optional[str] = None) -> TriplePayload:
        return TriplePayload(
            entity_id=entity_id, concept=concept, property=prop, value=value,
            period=None, currency=currency, unit=unit,
            source_system=source_system, source_table="aws_cost_explorer",
            source_field=source_field or prop, pipe_id=pipe_id,
            confidence_score=_CONF, confidence_tier=_TIER,
            fabric_plane=fabric_plane, fabric_product=fabric_product,
        )

    total = 0.0
    by_service: dict[str, float] = {}
    by_team: dict[str, float] = {}
    underutilized = 0
    potential_savings = 0.0

    for r in records:
        cost = _num(r.get("monthly_cost_usd")) or 0.0
        total += cost
        svc = str(r.get("service_code") or "unknown")
        by_service[svc] = by_service.get(svc, 0.0) + cost
        team = str(r.get("owner_cost_center") or "unknown")
        by_team[team] = by_team.get(team, 0.0) + cost
        util = r.get("utilization")
        cpu = _num(util.get("cpu_pct")) if isinstance(util, dict) else None
        if cpu is not None and cpu < _UNDERUTILIZED_CPU_PCT:
            underutilized += 1
            potential_savings += cost * _RIGHTSIZE_SAVINGS_FRACTION

    out: list[TriplePayload] = []
    # Primary metric: total cloud spend (NLQ metric cloud_spend / cloud_spend_monthly_total).
    out.append(_t(f"{_CONCEPT_ROOT}.summary", "total_cost", round(total, 2),
                  unit="usd", currency="USD", source_field="UnblendedCost"))
    out.append(_t(f"{_CONCEPT_ROOT}.summary", "resource_count", len(records),
                  unit="count"))
    # Per-service + per-team breakdown (NLQ bar-chart resolver: cloud_spend.by_service).
    for svc, amt in sorted(by_service.items(), key=lambda kv: kv[1], reverse=True):
        out.append(_t(f"{_CONCEPT_ROOT}.by_service", svc, round(amt, 2),
                      unit="usd", currency="USD"))
    for team, amt in sorted(by_team.items(), key=lambda kv: kv[1], reverse=True):
        out.append(_t(f"{_CONCEPT_ROOT}.by_team", _slug(team), round(amt, 2),
                      unit="usd", currency="USD"))
    # Utilization + savings (documented heuristics; not the exact total gate).
    out.append(_t(f"{_CONCEPT_ROOT}.utilization", "underutilized_count",
                  underutilized, unit="count"))
    out.append(_t(f"{_CONCEPT_ROOT}.savings", "opportunity_count", underutilized,
                  unit="count"))
    out.append(_t(f"{_CONCEPT_ROOT}.savings", "potential_amount",
                  round(potential_savings, 2), unit="usd", currency="USD"))
    # NLQ "top"/summary metrics on their own DCL-registered concept roots
    # (cloud_spend_by_service / cloud_spend_by_team / cloud_savings_opportunities_amount
    # are registered in ontology_concepts.yaml + persona_domains.yaml under CTO),
    # property "amount" per nlq metric_concept_map.
    if by_service:
        _, top_svc_amt = max(by_service.items(), key=lambda kv: kv[1])
        out.append(_t("cloud_spend_by_service.top_service", "amount",
                      round(top_svc_amt, 2), unit="usd", currency="USD"))
    if by_team:
        _, top_team_amt = max(by_team.items(), key=lambda kv: kv[1])
        out.append(_t("cloud_spend_by_team.top_team", "amount",
                      round(top_team_amt, 2), unit="usd", currency="USD"))
    out.append(_t("cloud_savings_opportunities_amount.summary", "amount",
                  round(potential_savings, 2), unit="usd", currency="USD"))
    return out
