"""Stage-3 edge derivation — the stitched graph (ContextOS).

Stage 1 ingests source records into triples. Stage 2 (semantic_triples_current)
is the canonical current-state surface. Stage 3 — HERE — DERIVES typed edges
ACROSS resolved entities and persists them in entity_edges via EdgeStore.

The governing gate: every derived edge must carry information NO single source
record holds. The hero edge is the cross-source comp-gap (BELOW_MARKET): it is
synthesized by JOINING an internal comp_band median (source workday_hr) against
a market_benchmark median for the resolved job_family (source radford_comp).
Neither source record holds the gap — it exists only in the join, across the
declared department -> job_family resolution.

Three edge types are derived:

  BELOW_MARKET (derived)  department -> job_family
      the cross-source comp-gap; gap_usd / gap_pct / below_market are computed
      by joining workday_hr comp_band with radford_comp market across the
      resolution map. THIS is the gate edge.
  DRIVEN_BY (derived)     department -> exit_theme
      the dominant exit reason per department, synthesized by ranking the four
      workforce.exit_theme.<reason> counts (no single count triple asserts
      which reason dominates — the ranking is the new information).
  RESOLVES_TO (declared)  department -> job_family
      the declared department -> job_family identity mapping that ENABLES the
      cross-source comp-gap. Declared (the mapping is asserted, not computed).

Identity (tenant_id + entity_id) and one dcl_ingest_id ride every edge (I1/I2).
Fail loud (A1): a department with a comp_band median but NO resolvable market
median is a derivation gap — it RAISES, never silently skips.
"""

from __future__ import annotations

from typing import Any, Optional

from backend.core.db import get_connection
from backend.db.edge_store import EdgeStore, put_edge_type
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

# The declared department -> job_family resolution. This mapping is what lets a
# workday_hr comp_band (keyed by department) be compared against a radford_comp
# market_benchmark (keyed by job_family). It is asserted (declared), not derived.
DEPT_TO_JOB_FAMILY = {
    "engineering": "software_engineering",
    "sales": "sales",
    "customer_success": "customer_success",
    "g&a": "general_admin",
}

# Comp/market medians are stable across months in the source (one band per
# department, atemporal); we read them at the headline period so the derivation
# is deterministic and produces one edge per department.
_HEADLINE_PERIOD = "2026-03"

_COMP_CONCEPT = "comp_band.median.by_department"
_MARKET_CONCEPT = "market_benchmark.median.by_job_family"
_COMP_SOURCE = "workday_hr"
_MARKET_SOURCE = "radford_comp"

# The four exit-theme reasons, in declared priority order. Order is the
# deterministic tie-break when two reasons share the top count (the rank is
# otherwise by count desc); it is NOT a value judgement — it makes the
# dominant-reason pick reproducible (B14).
_EXIT_REASONS = ("compensation", "growth", "management", "work_life")
_EXIT_CONCEPT_TMPL = "workforce.exit_theme.{reason}.by_department"
_EXIT_SOURCE = "workday_hr"


class EdgeDerivationError(RuntimeError):
    """A derivation could not complete with the integrity the gate requires
    (e.g. a department carrying a comp_band but no resolvable market median).
    Surfaces loud (A1) — never a silent skip."""


def _read_current_triples(tenant_id: str, entity_id: str, concept: str, period: str) -> dict[str, dict]:
    """All active (Stage-2) triples for one concept at one period, keyed by
    property (the department / job_family). source_system rides along so the
    derivation can assert provenance on the edge it builds."""
    out: dict[str, dict] = {}
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT property, value, source_system FROM semantic_triples_current "
                "WHERE tenant_id = %s AND entity_id = %s AND concept = %s AND period = %s",
                [tenant_id, entity_id, concept, period],
            )
            for prop, value, source_system in cur.fetchall():
                out[prop] = {"value": value, "source_system": source_system}
    return out


def _as_int(value: Any) -> int:
    """Coerce a store value (jsonb → float for whole-dollar/whole-count facts)
    to int. Raises if the value is not a finite whole number — a non-integer
    median or count is a data-integrity problem the gate must not paper over."""
    f = float(value)
    if f != int(f):
        raise EdgeDerivationError(
            f"expected a whole-number median/count, got {value!r} — refusing to "
            f"round-trip a fractional value through an int edge property"
        )
    return int(f)


def _derive_comp_gap(
    tenant_id: str, entity_id: str, dcl_ingest_id: str,
    comp: dict[str, dict], market: dict[str, dict],
) -> list[dict]:
    """THE GATE EDGE. For each department with an internal comp_band median AND
    a market_benchmark median for its resolved job_family, emit a BELOW_MARKET
    edge whose properties carry the SYNTHESIZED gap (gap_usd / gap_pct /
    below_market) — values no single source record holds.

    Fail loud (A1): a department with a comp_band but no resolvable market
    median RAISES — the comp-gap cannot be honestly synthesized without both
    sides, and a silent skip would hide a broken resolution."""
    edges: list[dict] = []
    for dept in sorted(comp):
        internal = _as_int(comp[dept]["value"])
        internal_source = comp[dept]["source_system"]

        job_family = DEPT_TO_JOB_FAMILY.get(dept)
        if job_family is None:
            raise EdgeDerivationError(
                f"department {dept!r} has a {_COMP_CONCEPT} median ({internal}) but is "
                f"not in the department->job_family resolution map "
                f"({sorted(DEPT_TO_JOB_FAMILY)}); cannot resolve a market median to "
                f"synthesize its comp-gap — fix the resolution map, do not skip"
            )
        if job_family not in market:
            raise EdgeDerivationError(
                f"department {dept!r} resolves to job_family {job_family!r} but no "
                f"{_MARKET_CONCEPT} median exists for it at period {_HEADLINE_PERIOD} "
                f"(market keys present: {sorted(market)}); cannot synthesize the "
                f"cross-source comp-gap — missing market side, failing loud"
            )

        market_median = _as_int(market[job_family]["value"])
        market_source = market[job_family]["source_system"]
        gap_usd = market_median - internal
        gap_pct = round((market_median - internal) / market_median * 100, 2)
        below_market = market_median > internal

        edges.append({
            "src_type": "department", "src_key": dept,
            "edge_type": "BELOW_MARKET",
            "dst_type": "job_family", "dst_key": job_family,
            "properties": {
                "internal_median": internal,
                "market_median": market_median,
                "gap_usd": gap_usd,
                "gap_pct": gap_pct,
                "below_market": below_market,
                "internal_source": internal_source,
                "market_source": market_source,
                "consumed": [_COMP_CONCEPT, _MARKET_CONCEPT],
            },
            "source_system": "dcl_derived",
            "source_table": None, "source_field": None,
            "pipe_id": None, "source_run_tag": None,
            "dcl_ingest_id": dcl_ingest_id,
            "confidence_score": 0.95, "confidence_tier": "exact",
            "fabric_plane": None, "fabric_product": None,
            "derivation": "derived",
        })
    return edges


def _derive_exit_driver(
    tenant_id: str, entity_id: str, dcl_ingest_id: str,
    exit_by_reason: dict[str, dict[str, dict]],
) -> list[dict]:
    """For each department, rank its four exit-theme counts and emit ONE
    DRIVEN_BY edge to the dominant reason. The ranking — which reason dominates
    — is the synthesized information; no single count triple asserts it.

    Departments are the union of those appearing in any reason concept. A
    department missing a given reason is treated as a 0 count for that reason
    (an absent count is genuinely zero exits for that reason)."""
    departments: set[str] = set()
    for reason in _EXIT_REASONS:
        departments.update(exit_by_reason.get(reason, {}).keys())

    edges: list[dict] = []
    for dept in sorted(departments):
        breakdown = {
            reason: _as_int(exit_by_reason.get(reason, {}).get(dept, {}).get("value", 0))
            for reason in _EXIT_REASONS
        }
        total = sum(breakdown.values())
        if total == 0:
            # No exits at all for this department — there is no driver to assert.
            # This is not a gap (the data is complete: every reason is zero); a
            # DRIVEN_BY edge would fabricate a dominant reason out of all-zero
            # counts. Skip with a logged reason (the absence is the truth).
            logger.info(
                "[edge_derivation] exit_driver: dept=%s has zero total exits across "
                "all reasons — no dominant-reason edge emitted (no driver exists)",
                dept,
            )
            continue
        # Rank by count desc, then declared reason order (deterministic tie-break).
        dominant = max(_EXIT_REASONS, key=lambda r: (breakdown[r], -_EXIT_REASONS.index(r)))
        count = breakdown[dominant]
        edges.append({
            "src_type": "department", "src_key": dept,
            "edge_type": "DRIVEN_BY",
            "dst_type": "exit_theme", "dst_key": dominant,
            "properties": {
                "count": count,
                "total": total,
                "share": round(count / total, 3),
                "rank": 1,
                "breakdown": breakdown,
                "source": _EXIT_SOURCE,
            },
            "source_system": "dcl_derived",
            "source_table": None, "source_field": None,
            "pipe_id": None, "source_run_tag": None,
            "dcl_ingest_id": dcl_ingest_id,
            "confidence_score": 0.95, "confidence_tier": "exact",
            "fabric_plane": None, "fabric_product": None,
            "derivation": "derived",
        })
    return edges


def _derive_resolution(
    tenant_id: str, entity_id: str, dcl_ingest_id: str,
    comp: dict[str, dict],
) -> list[dict]:
    """The declared department -> job_family identity mapping that ENABLES the
    cross-source comp-gap. Emitted for every department that holds a comp_band
    median (the departments the graph actually resolves). derivation='declared'
    — the mapping is asserted, not computed."""
    edges: list[dict] = []
    for dept in sorted(comp):
        job_family = DEPT_TO_JOB_FAMILY.get(dept)
        if job_family is None:
            raise EdgeDerivationError(
                f"department {dept!r} holds a {_COMP_CONCEPT} median but has no "
                f"declared job_family in the resolution map "
                f"({sorted(DEPT_TO_JOB_FAMILY)}) — cannot assert RESOLVES_TO"
            )
        edges.append({
            "src_type": "department", "src_key": dept,
            "edge_type": "RESOLVES_TO",
            "dst_type": "job_family", "dst_key": job_family,
            "properties": {"mapping": "department->job_family"},
            "source_system": "dcl_derived",
            "source_table": None, "source_field": None,
            "pipe_id": None, "source_run_tag": None,
            "dcl_ingest_id": dcl_ingest_id,
            "confidence_score": 1.0, "confidence_tier": "exact",
            "fabric_plane": None, "fabric_product": None,
            "derivation": "declared",
        })
    return edges


def _register_edge_types(tenant_id: str) -> None:
    """Register the three derived edge types with correct allowed_pairs and
    cardinality BEFORE asserting, so the demo edges are ACCEPTED — not flagged
    to the conflict register's structural class."""
    put_edge_type(
        tenant_id, "BELOW_MARKET",
        "Cross-source comp-gap: an internal comp_band median (workday_hr) sits "
        "below the market_benchmark median (radford_comp) for the resolved "
        "job_family. Properties carry the synthesized gap.",
        cardinality="many_to_one",
        allowed_pairs=[["department", "job_family"]],
    )
    put_edge_type(
        tenant_id, "DRIVEN_BY",
        "A department's attrition is driven primarily by one exit theme — the "
        "dominant reason synthesized by ranking the four exit_theme counts.",
        cardinality="many_to_one",
        allowed_pairs=[["department", "exit_theme"]],
    )
    put_edge_type(
        tenant_id, "RESOLVES_TO",
        "Declared department -> job_family identity mapping; enables the "
        "cross-source comp-gap join.",
        cardinality="one_to_one",
        allowed_pairs=[["department", "job_family"]],
    )


def derive_edges(tenant_id: str, entity_id: str, dcl_ingest_id: str) -> dict:
    """Derive the Stage-3 stitched graph for one entity and persist it.

    Reads Stage-2 current-state triples, derives BELOW_MARKET (the gate edge),
    DRIVEN_BY, and RESOLVES_TO edges, registers their edge types, and writes the
    batch via EdgeStore.assert_edges(replace=True). Returns the counts and the
    derived edges.

    Identity (tenant_id + entity_id) is required (I2); one dcl_ingest_id stamps
    the whole batch (I1). Raises EdgeDerivationError (A1) if a department carries
    a comp_band median with no resolvable market median.
    """
    if not tenant_id or not str(tenant_id).strip():
        raise EdgeDerivationError("tenant_id is required for edge derivation (I2)")
    if not entity_id or not str(entity_id).strip():
        raise EdgeDerivationError("entity_id is required for edge derivation (I2)")
    if not dcl_ingest_id or not str(dcl_ingest_id).strip():
        raise EdgeDerivationError("dcl_ingest_id is required to stamp the edge batch (I1)")

    comp = _read_current_triples(tenant_id, entity_id, _COMP_CONCEPT, _HEADLINE_PERIOD)
    market = _read_current_triples(tenant_id, entity_id, _MARKET_CONCEPT, _HEADLINE_PERIOD)
    exit_by_reason: dict[str, dict[str, dict]] = {
        reason: _read_current_triples(
            tenant_id, entity_id, _EXIT_CONCEPT_TMPL.format(reason=reason), _HEADLINE_PERIOD
        )
        for reason in _EXIT_REASONS
    }

    if not comp:
        raise EdgeDerivationError(
            f"no {_COMP_CONCEPT} triples found for tenant={tenant_id} entity={entity_id} "
            f"at period {_HEADLINE_PERIOD} — Stage-2 current state holds no comp_band to "
            f"derive a graph from; refusing to write an empty graph silently"
        )

    comp_gap_edges = _derive_comp_gap(tenant_id, entity_id, dcl_ingest_id, comp, market)
    exit_driver_edges = _derive_exit_driver(tenant_id, entity_id, dcl_ingest_id, exit_by_reason)
    resolution_edges = _derive_resolution(tenant_id, entity_id, dcl_ingest_id, comp)

    all_edges = comp_gap_edges + exit_driver_edges + resolution_edges

    _register_edge_types(tenant_id)
    store = EdgeStore()
    result = store.assert_edges(tenant_id, entity_id, all_edges, replace=True)

    if result.violations:
        # Edge types were registered with correct allowed_pairs/cardinality, so
        # the demo edges must be accepted. A violation here means a real
        # structural problem — surface it loud, do not swallow (A1).
        raise EdgeDerivationError(
            f"edge derivation produced {len(result.violations)} constraint "
            f"violation(s) despite registered edge types — structural conflict, "
            f"not silently dropped: {result.violations}"
        )

    counts = {
        "comp_gap": len(comp_gap_edges),
        "exit_driver": len(exit_driver_edges),
        "resolution": len(resolution_edges),
        "total": len(all_edges),
        "written": result.written,
        "superseded": result.superseded,
    }
    logger.info(
        "[edge_derivation] tenant=%s entity=%s ingest=%s: derived %s",
        tenant_id, entity_id, dcl_ingest_id, counts,
    )
    return {
        "tenant_id": tenant_id,
        "entity_id": entity_id,
        "dcl_ingest_id": dcl_ingest_id,
        "counts": counts,
        "edges": all_edges,
    }
