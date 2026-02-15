from typing import Dict, Any, List
from backend.aam.ingress import normalize_source_id
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

# Minimum quality-score gap between the top two sources for an entity
# before we consider the winner "clear."  Below this threshold the entity
# needs explicit SOR resolution.
_QUALITY_GAP_THRESHOLD = 0.05


def reconcile_sor(bindings: list, metrics: list, entities: list, loaded_sources: list[str]) -> dict:
    """
    Reconcile semantic-layer bindings against actually-loaded sources.

    Changes from the original implementation:
    1. All source-name comparisons use canonical IDs (normalize_source_id)
       so display-name drift ("Salesforce" vs "Salesforce CRM") cannot
       create ghost orphans/missing entries.
    2. Multi-source coverage is HEALTHY — it only becomes a true SOR
       conflict when no clear quality winner exists (gap < 5 %).
    """
    if not loaded_sources:
        return {
            "status": "no_data",
            "summary": {
                "totalBindings": 0,
                "totalEntities": 0,
                "totalMetrics": 0,
                "loadedSources": 0,
                "bindingSources": 0,
                "orphanSources": 0,
                "missingSources": 0,
                "entityCoverageGaps": 0,
                "sorConflicts": 0,
                "resolvedEntities": 0,
            },
            "coverageMatrix": [],
            "sorConflicts": [],
            "resolvedEntities": [],
            "orphanSources": [],
            "missingSources": [],
            "entityGaps": [],
        }

    # ------------------------------------------------------------------
    # 1. Build entity/pack lookup
    # ------------------------------------------------------------------
    entity_map: Dict[str, str] = {}
    entity_packs: Dict[str, str] = {}
    for e in entities:
        eid = e.get("id", "")
        entity_map[eid] = e.get("name", eid)
        entity_packs[eid] = e.get("pack", "")

    # ------------------------------------------------------------------
    # 2. Canonical-ID sets for source matching  (Bug 2 fix)
    # ------------------------------------------------------------------
    # Build a canonical→display map for binding sources so we can report
    # display names while comparing on canonical IDs.
    binding_canonical_to_display: Dict[str, str] = {}
    for b in bindings:
        raw = b.get("source_system", "")
        if raw:
            binding_canonical_to_display[normalize_source_id(raw)] = raw
    binding_canonical_set = set(binding_canonical_to_display.keys())

    loaded_canonical_to_display: Dict[str, str] = {}
    for src in loaded_sources:
        if src:
            cid = normalize_source_id(src)
            loaded_canonical_to_display[cid] = src
    loaded_canonical_set = set(loaded_canonical_to_display.keys())

    orphan_canonical = sorted(loaded_canonical_set - binding_canonical_set)
    missing_canonical = sorted(binding_canonical_set - loaded_canonical_set)

    # Report with the best available display name
    orphan_sources = [loaded_canonical_to_display.get(c, c) for c in orphan_canonical]
    missing_sources = [binding_canonical_to_display.get(c, c) for c in missing_canonical]

    # ------------------------------------------------------------------
    # 3. Entity→sources mapping (using canonical IDs for "loaded" check)
    # ------------------------------------------------------------------
    entity_sources: Dict[str, List[Dict[str, Any]]] = {}
    for b in bindings:
        raw_source = b.get("source_system", "")
        source_cid = normalize_source_id(raw_source) if raw_source else ""
        event = b.get("canonical_event", "")
        quality = b.get("quality_score", 0.0)
        dims = b.get("dims_coverage", {})
        for dim, covered in dims.items():
            if covered:
                if dim not in entity_sources:
                    entity_sources[dim] = []
                entity_sources[dim].append({
                    "source": raw_source,
                    "event": event,
                    "qualityScore": quality,
                    "loaded": source_cid in loaded_canonical_set,
                })

    # ------------------------------------------------------------------
    # 4. Metric→dimension reference map
    # ------------------------------------------------------------------
    metric_dims: Dict[str, List[str]] = {}
    for m in metrics:
        for dim in m.get("allowed_dims", []):
            if dim not in metric_dims:
                metric_dims[dim] = []
            metric_dims[dim].append(m.get("id", ""))

    all_entity_ids = set(entity_map.keys()) | set(entity_sources.keys()) | set(metric_dims.keys())

    # ------------------------------------------------------------------
    # 5. Build coverage matrix + conflict/resolution detection (Bug 1 fix)
    # ------------------------------------------------------------------
    coverage_matrix = []
    sor_conflicts = []        # TRUE conflicts: ambiguous authority
    resolved_entities = []    # Multi-source with clear winner
    entity_gaps = []

    for entity_id in sorted(all_entity_ids):
        sources_list = entity_sources.get(entity_id, [])
        entity_name = entity_map.get(entity_id, entity_id)

        has_binding = len(sources_list) > 0
        is_covered = any(s["loaded"] for s in sources_list)
        loaded_claimants = [s for s in sources_list if s["loaded"]]

        if has_binding:
            # ---- Classify multi-source entities -----------------------
            # Old logic: len(sources_list) > 1 → conflict.  WRONG.
            # New logic: only flag as conflict when the quality gap between
            # the best and second-best source is too small to declare a
            # clear winner.
            resolution_status = "single_source"
            conflict_count = 0

            if len(sources_list) > 1:
                sorted_by_quality = sorted(
                    sources_list, key=lambda s: s["qualityScore"], reverse=True
                )
                best_score = sorted_by_quality[0]["qualityScore"]
                second_score = sorted_by_quality[1]["qualityScore"]
                gap = best_score - second_score

                if gap >= _QUALITY_GAP_THRESHOLD:
                    # Clear winner — this is HEALTHY multi-source coverage.
                    resolution_status = "resolved"
                else:
                    # Ambiguous — no clear SOR authority.
                    resolution_status = "needs_resolution"
                    conflict_count = len(sources_list)

            coverage_matrix.append({
                "entity": entity_id,
                "entityName": entity_name,
                "sources": sources_list,
                "isCovered": is_covered,
                "conflictCount": conflict_count,
                "resolutionStatus": resolution_status,
            })

            if resolution_status == "needs_resolution":
                best = max(sources_list, key=lambda s: s["qualityScore"])
                sor_conflicts.append({
                    "entity": entity_id,
                    "entityName": entity_name,
                    "claimants": [
                        {"source": s["source"], "event": s["event"], "qualityScore": s["qualityScore"]}
                        for s in sources_list
                    ],
                    "recommendation": best["source"],
                })
            elif resolution_status == "resolved":
                best = max(sources_list, key=lambda s: s["qualityScore"])
                resolved_entities.append({
                    "entity": entity_id,
                    "entityName": entity_name,
                    "winner": best["source"],
                    "sourceCount": len(sources_list),
                })

        # Entity gap: referenced by metrics but has no binding at all
        if entity_id in metric_dims and not has_binding:
            pack = entity_packs.get(entity_id, "")
            entity_gaps.append({
                "entity": entity_id,
                "entityName": entity_name,
                "referencedBy": sorted(metric_dims[entity_id]),
                "pack": pack,
            })

    # ------------------------------------------------------------------
    # 6. Status determination
    # ------------------------------------------------------------------
    if sor_conflicts:
        status = "conflicts"
    elif missing_sources or entity_gaps:
        status = "gaps"
    else:
        status = "synced"

    return {
        "status": status,
        "summary": {
            "totalBindings": len(bindings),
            "totalEntities": len(entities),
            "totalMetrics": len(metrics),
            "loadedSources": len(loaded_sources),
            "bindingSources": len(binding_canonical_set),
            "orphanSources": len(orphan_sources),
            "missingSources": len(missing_sources),
            "entityCoverageGaps": len(entity_gaps),
            "sorConflicts": len(sor_conflicts),
            "resolvedEntities": len(resolved_entities),
        },
        "coverageMatrix": coverage_matrix,
        "sorConflicts": sor_conflicts,
        "resolvedEntities": resolved_entities,
        "orphanSources": orphan_sources,
        "missingSources": missing_sources,
        "entityGaps": entity_gaps,
    }
