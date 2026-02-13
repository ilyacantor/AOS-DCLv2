from typing import Dict, Any, List
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)


def reconcile_sor(bindings: list, metrics: list, entities: list, loaded_sources: list[str]) -> dict:
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
            },
            "coverageMatrix": [],
            "sorConflicts": [],
            "orphanSources": [],
            "missingSources": [],
            "entityGaps": [],
        }

    entity_map = {}
    entity_packs = {}
    for e in entities:
        eid = e.get("id", "")
        entity_map[eid] = e.get("name", eid)
        entity_packs[eid] = e.get("pack", "")

    binding_sources = set()
    for b in bindings:
        binding_sources.add(b.get("source_system", ""))

    loaded_set = set(loaded_sources)

    orphan_sources = sorted(loaded_set - binding_sources)
    missing_sources = sorted(binding_sources - loaded_set)

    entity_sources: Dict[str, List[Dict[str, Any]]] = {}
    for b in bindings:
        source = b.get("source_system", "")
        event = b.get("canonical_event", "")
        quality = b.get("quality_score", 0.0)
        dims = b.get("dims_coverage", {})
        for dim, covered in dims.items():
            if covered:
                if dim not in entity_sources:
                    entity_sources[dim] = []
                entity_sources[dim].append({
                    "source": source,
                    "event": event,
                    "qualityScore": quality,
                    "loaded": source in loaded_set,
                })

    metric_dims: Dict[str, List[str]] = {}
    for m in metrics:
        for dim in m.get("allowed_dims", []):
            if dim not in metric_dims:
                metric_dims[dim] = []
            metric_dims[dim].append(m.get("id", ""))

    all_entity_ids = set(entity_map.keys()) | set(entity_sources.keys()) | set(metric_dims.keys())

    coverage_matrix = []
    sor_conflicts = []
    entity_gaps = []

    for entity_id in sorted(all_entity_ids):
        sources_list = entity_sources.get(entity_id, [])
        entity_name = entity_map.get(entity_id, entity_id)

        has_binding = len(sources_list) > 0
        is_covered = any(s["loaded"] for s in sources_list)
        loaded_claimants = [s for s in sources_list if s["loaded"]]
        conflict_count = len(loaded_claimants) if len(loaded_claimants) > 1 else 0

        if has_binding:
            coverage_matrix.append({
                "entity": entity_id,
                "entityName": entity_name,
                "sources": sources_list,
                "isCovered": is_covered,
                "conflictCount": conflict_count,
            })

            if len(sources_list) > 1:
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

        if entity_id in metric_dims and not has_binding:
            pack = entity_packs.get(entity_id, "")
            entity_gaps.append({
                "entity": entity_id,
                "entityName": entity_name,
                "referencedBy": sorted(metric_dims[entity_id]),
                "pack": pack,
            })

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
            "bindingSources": len(binding_sources),
            "orphanSources": len(orphan_sources),
            "missingSources": len(missing_sources),
            "entityCoverageGaps": len(entity_gaps),
            "sorConflicts": len(sor_conflicts),
        },
        "coverageMatrix": coverage_matrix,
        "sorConflicts": sor_conflicts,
        "orphanSources": orphan_sources,
        "missingSources": missing_sources,
        "entityGaps": entity_gaps,
    }
