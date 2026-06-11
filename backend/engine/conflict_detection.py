"""Value-level conflict detection + Conflict Register population (Gate 1A, §8).

A VALUE conflict: the same (entity, concept, property, period) carries materially
different values from different sources in the live view. Materiality comes from
the tenant's policy (tenant_conflict_policy: absolute and/or relative thresholds —
either crossing makes it material). A STRUCTURAL conflict: multiple sources claim
the same fact without material value disagreement (the pre-Gate-1A collision
class) — both classes land in the one register.

Root cause explanations are grounded in ontology concept metadata
(recognition_basis / timing_semantics / scope_boundaries from
config/ontology_concepts.yaml). When a concept has no such metadata the
explanation falls back to the numeric evidence alone and we log a warning —
using heuristic classification instead of concept metadata.

Recommendation order: precedent (latest non-escalate disposition of the same
conflict class) > authority map (tenant_authority_map) > escalate. Precedent is
a PROPOSAL only — HITL decides. Rule promotion (precedent → standing auto-rule)
is deliberately out of Gate 1A; the disposition trace carries everything a
future rule engine needs.

Scope note: coordinate groups where one source contributes multiple rows
(per-record ledger detail like invoice.billing) are not value-comparable —
they are registered as structural with per-source row summaries.
"""

from __future__ import annotations

from typing import Any, Optional

from backend.db.conflict_store import ConflictStore
from backend.core.db import get_connection
from backend.registry.concept_registry import ConceptRegistry
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

_store = ConflictStore()
_registry = ConceptRegistry()

# Entity-type slot in the conflict class key. SE entities are all companies
# today; the slot exists so class keys survive an entity-type taxonomy later
# (precedent matching is per class across entities within a tenant).
_ENTITY_TYPE = "company"

# Claims detail cap: beyond this many rows in one coordinate group, claims
# carry per-source summaries instead of per-row provenance (the register stays
# bounded; the drill still works via dcl_ingest_id + coordinates).
_CLAIMS_DETAIL_CAP = 24


def conflict_class_key(concept: str, property: str, sources: list[str]) -> str:
    pair = "+".join(sorted(sources))
    return f"{_ENTITY_TYPE}|{concept}|{property}|{pair}"


def _numeric(v: Any) -> Optional[float]:
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        return None
    return float(v)


def _concept_metadata_explanation(
    concept: str, claims: list[dict], abs_delta: float | None,
    rel_delta: float | None,
) -> tuple[str, str]:
    """Build the root-cause explanation from ontology concept metadata.

    Returns (explanation, root_cause_source). Falls back to the numeric
    evidence with a logged warning when the concept carries no metadata —
    using heuristic classification instead of concept metadata.
    """
    parts = []
    for c in claims:
        parts.append(f"{c['source_system']}={c.get('value')!r}")
    evidence = ", ".join(parts)
    delta_txt = ""
    if abs_delta is not None:
        delta_txt = f" Δabs={abs_delta:g}"
        if rel_delta is not None:
            delta_txt += f" Δrel={rel_delta:.2%}"

    root = concept.split(".", 1)[0]
    meta = _registry.get_concept(concept) or _registry.get_concept(root)
    if meta:
        hints = []
        for key in ("recognition_basis", "timing_semantics", "scope_boundaries"):
            if meta.get(key):
                hints.append(f"{key.replace('_', ' ')}: {meta[key]}")
        if hints:
            return (
                f"Concept '{concept}' — sources disagree ({evidence}).{delta_txt} "
                f"Likely cause per concept metadata — " + "; ".join(hints),
                "concept_metadata",
            )
    logger.warning(
        "[conflict-detection] concept %r has no recognition/timing/scope metadata — "
        "using heuristic classification for the root-cause explanation", concept,
    )
    return (
        f"Sources disagree on '{concept}' ({evidence}).{delta_txt} "
        f"No concept metadata available — classified heuristically by value delta.",
        "heuristic",
    )


def _recommend(
    precedent: Optional[dict], concept: str, claims: list[dict],
    amap: dict[str, list[str]],
) -> dict:
    """Precedent > authority > escalate. Proposal only — HITL decides."""
    sources = [c["source_system"] for c in claims]
    if precedent:
        action = precedent["action"]
        # A precedent's winner may sit at a different claim index here; propose
        # by winner_source when it is present, else carry the action as-is.
        if precedent.get("winner_source") in sources:
            idx = sources.index(precedent["winner_source"])
            action = "accept_a" if idx == 0 else ("accept_b" if idx == 1 else "manual")
        return {
            "action": action,
            "winner_source": precedent.get("winner_source"),
            "basis": "precedent",
            "precedent": precedent,
        }

    best_prefix_len = -1
    ranked: list[str] | None = None
    for prefix, sources_ranked in amap.items():
        if concept.startswith(prefix) and len(prefix) > best_prefix_len:
            best_prefix_len = len(prefix)
            ranked = sources_ranked
    if ranked:
        for winner in ranked:  # highest authority present among the claims wins
            if winner in sources:
                idx = sources.index(winner)
                return {
                    "action": "accept_a" if idx == 0 else ("accept_b" if idx == 1 else "manual"),
                    "winner_source": winner,
                    "basis": "authority",
                    "authority_prefix": next(p for p in amap if amap[p] == ranked),
                }
    return {"action": "escalate", "winner_source": None, "basis": "none"}


def detect_and_register(
    tenant_id: str, entity_id: str, dcl_ingest_id: str,
    coords: list[tuple[str, str, str]] | None = None,
) -> dict:
    """Detect conflicts in one run's live rows and upsert the register.

    coords (optional): restrict the scan to these (concept, property,
    period-or-'') coordinates — the ingest hook passes each batch's own
    coordinates so per-batch cost scales with the batch, not the accumulated
    run. Detection stays complete across batches: a conflict only ever
    involves coordinates that some batch carried, and that batch's pass sees
    the run's full row set for those coordinates. None = full-run scan (the
    explicit /api/dcl/conflicts/detect surface).

    Idempotent per (coords, run): re-detection refreshes claims. Returns
    counts + the registered conflicts.
    """
    policy = _store.load_policy(tenant_id)
    amap = _store.load_authority_map(tenant_id)
    precedents = _store.latest_precedents(tenant_id)
    abs_thr, rel_thr = policy["abs_threshold"], policy["rel_threshold"]

    coord_clause = ""
    params: list = [tenant_id, entity_id, dcl_ingest_id]
    if coords is not None:
        if not coords:
            return {"tenant_id": str(tenant_id), "entity_id": entity_id,
                    "dcl_ingest_id": str(dcl_ingest_id),
                    "detected_new": 0, "refreshed": 0, "conflicts": []}
        coord_clause = "AND (concept, property, COALESCE(period, '')) IN %s"
        params.append(tuple((c, p, per or "") for (c, p, per) in coords))

    sql = f"""
        SELECT concept, property, period,
               COUNT(*) AS n_rows,
               COUNT(DISTINCT source_system) AS n_sources,
               jsonb_agg(jsonb_build_object(
                   'source_system', source_system,
                   'value', value,
                   'triple_id', id,
                   'confidence_score', confidence_score,
                   'confidence_tier', confidence_tier,
                   'ingested_at', ingested_at,
                   'source_table', source_table,
                   'source_field', source_field,
                   'pipe_id', pipe_id
               ) ORDER BY source_system, id) AS claims
        FROM semantic_triples
        WHERE tenant_id = %s AND entity_id = %s AND run_id = %s AND is_active = true
        {coord_clause}
        GROUP BY concept, property, period
        HAVING COUNT(DISTINCT source_system) > 1
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            groups = cur.fetchall()

    pending_rows: list[dict] = []
    conflicts: list[dict] = []
    for concept, prop, period, n_rows, n_sources, claims in groups:
        scalar_per_source = n_rows == n_sources
        values = [_numeric(c.get("value")) for c in claims]
        numeric = scalar_per_source and all(v is not None for v in values)

        abs_delta = rel_delta = None
        material = False
        if numeric:
            vmax, vmin = max(values), min(values)
            abs_delta = round(vmax - vmin, 10)
            denom = max(abs(vmax), abs(vmin))
            rel_delta = (abs_delta / denom) if denom > 0 else (0.0 if abs_delta == 0 else None)
            material = (
                (abs_thr is not None and abs_delta >= abs_thr)
                or (rel_thr is not None and rel_delta is not None and rel_delta >= rel_thr)
            )
        elif scalar_per_source:
            # Non-numeric scalars: any inequality is a value conflict.
            distinct = {repr(c.get("value")) for c in claims}
            material = len(distinct) > 1

        if not scalar_per_source:
            # Per-record detail (e.g. ledger rows): summarize per source.
            by_src: dict[str, dict] = {}
            for c in claims:
                s = by_src.setdefault(
                    c["source_system"],
                    {"source_system": c["source_system"], "row_count": 0,
                     "sample_triple_id": c["triple_id"]},
                )
                s["row_count"] += 1
            claims = sorted(by_src.values(), key=lambda s: s["source_system"])
        elif len(claims) > _CLAIMS_DETAIL_CAP:
            claims = claims[:_CLAIMS_DETAIL_CAP]

        conflict_type = "value" if material else "structural"
        sources = [c["source_system"] for c in claims]
        cls = conflict_class_key(concept, prop, sources)
        explanation, root_src = _concept_metadata_explanation(
            concept, claims, abs_delta, rel_delta,
        )
        recommended = _recommend(precedents.get(cls), concept, claims, amap)
        materiality = {
            "abs_delta": abs_delta, "rel_delta": rel_delta,
            "abs_threshold": abs_thr, "rel_threshold": rel_thr,
            "material": material,
            "basis": "numeric" if numeric else ("non_numeric" if scalar_per_source else "per_record"),
        }
        pending_rows.append({
            "tenant_id": tenant_id, "entity_id": entity_id,
            "conflict_type": conflict_type, "conflict_class": cls,
            "concept": concept, "property": prop, "period": period,
            "dcl_ingest_id": dcl_ingest_id, "claims": claims,
            "materiality": materiality, "recommended": recommended,
            "root_cause_explanation": explanation, "root_cause_source": root_src,
        })
        conflicts.append({
            "conflict_type": conflict_type,
            "conflict_class": cls, "concept": concept, "property": prop,
            "period": period, "field": concept, "claims": claims,
            "materiality": materiality, "recommended": recommended,
            "root_cause_explanation": explanation, "root_cause_source": root_src,
        })

    # ONE batched upsert + one commit — detection runs inside the ingest path,
    # so round trips must not scale with group count.
    results = _store.upsert_conflicts(pending_rows)
    detected = sum(1 for (_id, created) in results if created)
    refreshed = len(results) - detected
    for c, (conflict_id, _created) in zip(conflicts, results):
        c["conflict_id"] = conflict_id

    return {
        "tenant_id": str(tenant_id), "entity_id": entity_id,
        "dcl_ingest_id": str(dcl_ingest_id),
        "detected_new": detected, "refreshed": refreshed,
        "conflicts": conflicts,
    }
