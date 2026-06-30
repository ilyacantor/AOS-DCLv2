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

# Cost-field aliases — TOLERATE schema drift across cost sources (a billing
# export's `cost_usd` vs the warehouse scan's `monthly_cost_usd`, an amortized
# vs unblended column). Tried in order; the FIRST present numeric wins. A cost
# record carrying NONE of these is surfaced LOUDLY (never silently zero'd — that
# would drop a real cost, A1). Add a new source's cost column here, not a branch.
_COST_FIELD_ALIASES = (
    "monthly_cost_usd", "cost_usd", "monthly_cost",
    "unblended_cost_usd", "amortized_cost_usd",
)

# Per-team delivery OUTPUT signal (the efficiency DENOMINATOR). A cloud_spend
# pipe whose records carry this field is an OUTPUT pipe (from a delivery/DORA
# analytics warehouse), NOT a cost pipe: DCL emits cloud_spend.output_by_team and
# — when the same team also has cost — cloud_spend.efficiency_by_team = cost ÷
# output. The team field is `team` (a delivery source's own column); it joins to
# the cost fleet's owner_cost_center by the same slug.
_OUTPUT_FIELD = "deploys_per_month"
_OUTPUT_TEAM_FIELD = "team"
_OUTPUT_UNIT = "deploys"


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
    warnings: Optional[list] = None,
) -> list[TriplePayload]:
    """Compute cloud_spend.* aggregate TriplePayloads from raw records.

    Two pipe roles, routed by record CONTENT (one source of record per pipe):
      * COST pipe — records carry a cost field (monthly_cost_usd / cost_usd / …):
        summary.total_cost, by_service, by_team, utilization, savings. Cost
        extraction is drift-tolerant across sources and surfaces an unmappable
        cost LOUDLY into `warnings` (never silently zero'd, A1).
      * OUTPUT pipe — records carry the per-team delivery signal
        (deploys_per_month): cloud_spend.output_by_team.<team> only (no
        fabricated cost summary). The efficiency join (cost ÷ output) is a
        cross-pipe post-pass — compute_team_efficiency — run once per ingest.

    `records` are flat field->value dicts. COST records without a `period` field
    aggregate atemporally (the original fleet behavior, byte-identical); records
    with one aggregate per period (the periodized dual feeds the conflict engine
    compares). A pipe carrying BOTH cost and output fields is ambiguous → raise.
    """
    if warnings is None:
        warnings = []
    if _is_output_pipe(records):
        return _aggregate_output_pipe(entity_id=entity_id, pipe=pipe, records=records)

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
            pipe_id=pipe_id, warnings=warnings,
        ))
    for period in sorted(by_period):
        out.extend(_aggregate_group(
            entity_id=entity_id, records=by_period[period], period=period,
            source_system=source_system, source_table=source_table,
            fabric_plane=fabric_plane, fabric_product=fabric_product,
            pipe_id=pipe_id, warnings=warnings,
        ))
    return out


def _aggregate_group(
    *, entity_id: str, records: list[dict], period: Optional[str],
    source_system: str, source_table: str, fabric_plane: Optional[str],
    fabric_product: Optional[str], pipe_id: str, warnings: list,
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
        # Drift-tolerant: first present cost alias wins. None = no recognized
        # cost field → surface LOUDLY and exclude from totals (never silently
        # treat as 0, which would drop a real cost, A1).
        cost, cost_field = _extract_cost(r)
        if cost is None:
            warnings.append({
                "type": "unmappable_cost_field",
                "pipe_id": pipe_id,
                "record_key": str(
                    r.get("resource_id") or r.get("source_record_id") or "?"
                ),
                "available_fields": sorted(str(k) for k in r.keys()),
                "tried": list(_COST_FIELD_ALIASES),
                "detail": (
                    "no recognized cost field on a cloud_spend cost record; "
                    "excluded from totals and surfaced (not silently zero'd, A1). "
                    "Add the source's cost column to _COST_FIELD_ALIASES."
                ),
            })
            cost = 0.0
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


def _extract_cost(r: dict) -> tuple[Optional[float], Optional[str]]:
    """First present numeric cost alias wins (drift-tolerant across cost sources).

    Returns (value, field_name), or (None, None) when the record carries NONE of
    the recognized cost columns — the caller surfaces that loudly (A1).
    """
    for fld in _COST_FIELD_ALIASES:
        v = _num(r.get(fld))
        if v is not None:
            return v, fld
    return None, None


def _is_output_pipe(records: list[dict]) -> bool:
    """True when the pipe carries the per-team OUTPUT signal (deploys_per_month)
    rather than cost. A pipe carrying BOTH is ambiguous → raise (A1): cost and
    output must be separate sources so provenance and efficiency stay honest."""
    if not records:
        return False
    keys: set[str] = set()
    for r in records[:100]:
        if isinstance(r, dict):
            keys.update(r.keys())
    has_output = _OUTPUT_FIELD in keys
    has_cost = any(f in keys for f in _COST_FIELD_ALIASES)
    if has_output and has_cost:
        raise ValueError(
            "cloud_spend pipe carries BOTH a cost field and the output field "
            f"({_OUTPUT_FIELD!r}) — ambiguous. Cost and output must arrive as "
            "separate pipes/sources so per-source provenance and the efficiency "
            "join stay honest (A1)."
        )
    return has_output


def _aggregate_output_pipe(
    *, entity_id: str, pipe: dict, records: list[dict],
) -> list[TriplePayload]:
    """Per-team delivery OUTPUT aggregates — cloud_spend.output_by_team.<team>.

    Atemporal (a current-state delivery rate) to align with the fleet's atemporal
    cost by_team so efficiency can be joined per team. Honest guard (mirrors
    has_util_signal): only teams that carry a real output value are emitted — no
    fabricated zeros, and NO cost summary (an output feed asserts nothing about
    spend). Confidence exact: a deterministic sum, not a heuristic mapping.
    """
    source_system = pipe.get("source_system") or "deploy_analytics"
    fabric_plane = pipe.get("fabric_plane")
    fabric_product = pipe.get("fabric_product")
    pipe_id = str(pipe.get("pipe_id"))

    output_by_team: dict[str, float] = {}
    for r in records:
        team = str(r.get(_OUTPUT_TEAM_FIELD) or r.get("owner_cost_center") or "unknown")
        val = _num(r.get(_OUTPUT_FIELD))
        if team == "unknown" or val is None:
            continue
        output_by_team[team] = output_by_team.get(team, 0.0) + val

    out: list[TriplePayload] = []
    for team, amt in sorted(output_by_team.items(), key=lambda kv: kv[1], reverse=True):
        out.append(TriplePayload(
            entity_id=entity_id, concept=f"{_CONCEPT_ROOT}.output_by_team",
            property=_slug(team), value=round(amt, 2), period=None,
            currency=None, unit=_OUTPUT_UNIT,
            source_system=source_system, source_table=source_system,
            source_field=_OUTPUT_FIELD, pipe_id=pipe_id,
            confidence_score=_CONF, confidence_tier=_TIER,
            fabric_plane=fabric_plane, fabric_product=fabric_product,
        ))
    return out


def compute_team_efficiency(
    entity_id: str, payloads: list[TriplePayload],
) -> list[TriplePayload]:
    """Cross-pipe efficiency = round(team_cost / team_output, 2) per team.

    Emitted ONLY for teams present in BOTH the atemporal cost by_team AND
    output_by_team (no fabricated rows; a team with cost but no output, or output
    but no cost, gets none). Reads the already-emitted aggregates so the numbers
    are exactly the sums DCL wrote — no recomputation drift. Provenance is
    cost-anchored (the warehouse pipe) with source_field marking the derivation;
    confidence exact (deterministic division). Run once per ingest over all
    cloud_spend payloads, so cost and output must ride the SAME ingest envelope.
    """
    cost: dict[str, TriplePayload] = {}
    output: dict[str, float] = {}
    for p in payloads:
        if p.period is not None:
            continue
        if p.concept == f"{_CONCEPT_ROOT}.by_team":
            v = _num(p.value)
            if v is not None:
                cost[p.property] = p
        elif p.concept == f"{_CONCEPT_ROOT}.output_by_team":
            v = _num(p.value)
            if v is not None:
                output[p.property] = v

    out: list[TriplePayload] = []
    for team in sorted(set(cost) & set(output)):
        out_val = output[team]
        if out_val <= 0:  # guard div-by-zero; a real team ships at least one deploy
            continue
        cost_p = cost[team]
        cost_val = _num(cost_p.value)
        if cost_val is None:
            continue
        out.append(TriplePayload(
            entity_id=entity_id, concept=f"{_CONCEPT_ROOT}.efficiency_by_team",
            property=team, value=round(cost_val / out_val, 2), period=None,
            currency="USD", unit=f"usd_per_{_OUTPUT_FIELD}",
            source_system=cost_p.source_system,
            source_table=f"derived:cost_per_{_OUTPUT_FIELD}",
            source_field="efficiency_by_team", pipe_id=cost_p.pipe_id,
            confidence_score=_CONF, confidence_tier=_TIER,
            fabric_plane=cost_p.fabric_plane, fabric_product=cost_p.fabric_product,
        ))
    return out
