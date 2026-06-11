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

Aggregates are atemporal (period=None) for records that carry no period —
"current state" totals — so NLQ's period fallback (dcl_semantic_client_v2.
_get_metric_single_period retry-without-period) resolves them regardless of the
requested quarter. Records that DO carry a `period` field (the §13 dual-source
scenario feeds: monthly billing exports vs GL allocations) aggregate PER PERIOD
— same concepts, period="2026-03" — which is what lets two pipes' monthly
totals land on the same (entity, concept, property, period) coordinates and the
conflict engine compare them. A mixed batch yields both: atemporal totals for
the period-less records, per-period rows for the rest. Every triple carries the
pipe's transport provenance (source_system, pipe_id, fabric_plane,
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
    """Compute cloud_spend.* aggregate TriplePayloads from raw cost records.

    `records` are flat field->value dicts (AWS resource rows, billing export
    lines, GL allocation rows — whatever AAM transported). Records without a
    `period` field aggregate atemporally (the original fleet behavior, byte-
    identical); records with one aggregate per period. Returns the aggregate
    triples NLQ's cloud-spend metrics resolve against — and, for periodized
    dual feeds, the per-month coordinates the conflict engine compares.
    """
    source_system = pipe.get("source_system") or "aws_cost_explorer"
    fabric_plane = pipe.get("fabric_plane")
    fabric_product = pipe.get("fabric_product")
    pipe_id = str(pipe.get("pipe_id"))
    # Unchanged for the fleet pipe (source_system == aws_cost_explorer);
    # honest for any other transported source.
    source_table = source_system

    # Per-period aggregation is a DECLARED pipe semantic, not an inference:
    # only pipes whose record_key_field is "period" (monthly financial feeds —
    # billing exports, GL allocations) group by period. The fleet scan's
    # records also CARRY a period column (a filterable attribute of a
    # current-state scan, record_key_field="resource_id") — inferring from the
    # column's presence would periodize the fleet total onto the financial
    # feeds' coordinates and fabricate a three-way conflict.
    periodize = (pipe.get("record_key_field") == "period")

    atemporal: list[dict] = []
    by_period: dict[str, list[dict]] = {}
    for r in records:
        period = r.get("period") if periodize else None
        if period is None or not str(period).strip():
            atemporal.append(r)
        else:
            by_period.setdefault(str(period), []).append(r)

    out: list[TriplePayload] = []
    if atemporal:
        out.extend(_aggregate_group(
            entity_id=entity_id, records=atemporal, period=None,
            source_system=source_system, source_table=source_table,
            fabric_plane=fabric_plane, fabric_product=fabric_product,
            pipe_id=pipe_id,
        ))
    for period in sorted(by_period):
        out.extend(_aggregate_group(
            entity_id=entity_id, records=by_period[period], period=period,
            source_system=source_system, source_table=source_table,
            fabric_plane=fabric_plane, fabric_product=fabric_product,
            pipe_id=pipe_id,
        ))
    return out


def _aggregate_group(
    *, entity_id: str, records: list[dict], period: Optional[str],
    source_system: str, source_table: str, fabric_plane: Optional[str],
    fabric_product: Optional[str], pipe_id: str,
) -> list[TriplePayload]:
    """One aggregation pass over one period group (period=None = atemporal)."""

    def _t(concept: str, prop: str, value: Any, *,
           unit: Optional[str] = None, currency: Optional[str] = None,
           source_field: Optional[str] = None) -> TriplePayload:
        return TriplePayload(
            entity_id=entity_id, concept=concept, property=prop, value=value,
            period=period, currency=currency, unit=unit,
            source_system=source_system, source_table=source_table,
            source_field=source_field or prop, pipe_id=pipe_id,
            confidence_score=_CONF, confidence_tier=_TIER,
            fabric_plane=fabric_plane, fabric_product=fabric_product,
        )

    total = 0.0
    by_service: dict[str, float] = {}
    by_team: dict[str, float] = {}
    underutilized = 0
    potential_savings = 0.0
    has_util_signal = False

    for r in records:
        cost = _num(r.get("monthly_cost_usd")) or 0.0
        total += cost
        svc = str(r.get("service_code") or "unknown")
        by_service[svc] = by_service.get(svc, 0.0) + cost
        team = str(r.get("owner_cost_center") or "unknown")
        by_team[team] = by_team.get(team, 0.0) + cost
        util = r.get("utilization")
        cpu = _num(util.get("cpu_pct")) if isinstance(util, dict) else None
        if isinstance(util, dict):
            has_util_signal = True
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
    # A feed that carries no service/team dimension lands everything in "unknown" —
    # emit only dimensions the records actually carry.
    if set(by_service) != {"unknown"}:
        for svc, amt in sorted(by_service.items(), key=lambda kv: kv[1], reverse=True):
            out.append(_t(f"{_CONCEPT_ROOT}.by_service", svc, round(amt, 2),
                          unit="usd", currency="USD"))
    if set(by_team) != {"unknown"}:
        for team, amt in sorted(by_team.items(), key=lambda kv: kv[1], reverse=True):
            out.append(_t(f"{_CONCEPT_ROOT}.by_team", _slug(team), round(amt, 2),
                          unit="usd", currency="USD"))
    # Utilization + savings (documented heuristics) — only when the records
    # carry a utilization signal at all; a billing/GL export claims nothing
    # about utilization and must not assert a zero count.
    if has_util_signal:
        out.append(_t(f"{_CONCEPT_ROOT}.utilization", "underutilized_count",
                      underutilized, unit="count"))
        out.append(_t(f"{_CONCEPT_ROOT}.savings", "opportunity_count", underutilized,
                      unit="count"))
        out.append(_t(f"{_CONCEPT_ROOT}.savings", "potential_amount",
                      round(potential_savings, 2), unit="usd", currency="USD"))
        out.append(_t("cloud_savings_opportunities_amount.summary", "amount",
                      round(potential_savings, 2), unit="usd", currency="USD"))
    # NLQ "top"/summary metrics on their own DCL-registered concept roots
    # (cloud_spend_by_service / cloud_spend_by_team are registered in
    # ontology_concepts.yaml + persona_domains.yaml under CTO), property
    # "amount" per nlq metric_concept_map.
    if by_service and set(by_service) != {"unknown"}:
        _, top_svc_amt = max(by_service.items(), key=lambda kv: kv[1])
        out.append(_t("cloud_spend_by_service.top_service", "amount",
                      round(top_svc_amt, 2), unit="usd", currency="USD"))
    if by_team and set(by_team) != {"unknown"}:
        _, top_team_amt = max(by_team.items(), key=lambda kv: kv[1])
        out.append(_t("cloud_spend_by_team.top_team", "amount",
                      round(top_team_amt, 2), unit="usd", currency="USD"))
    return out
