"""Concept authority ranking — determines the primary source per concept prefix.

Gate 1A: the authority map is PER-TENANT DATA (tenant_authority_map, migration
018; tenant '*' rows are the platform defaults, tenant rows override per
prefix). The hardcoded module-level table this file used to carry is retired —
its six entries were seeded into the '*' tier by the migration.

Callers load the effective map once per operation
(ConflictStore.load_authority_map(tenant_id)) and pass it in — no per-collision
queries. First entry in a ranked list = highest authority; longest concept
prefix wins; sources absent from the map rank lowest (999).
"""

from __future__ import annotations

from typing import Dict, List, Tuple

AuthorityMap = Dict[str, List[str]]


def get_authority_rank(concept: str, source_system: str, amap: AuthorityMap) -> int:
    """Authority rank for a source writing a concept. Lower = higher authority;
    999 = unranked. Longest-prefix matching: 'infrastructure.incidents.p1'
    matches 'infrastructure.incidents' before 'infrastructure'."""
    best_match_len = 0
    best_rank = 999
    for prefix, sources in amap.items():
        if concept.startswith(prefix) and len(prefix) > best_match_len:
            best_match_len = len(prefix)
            best_rank = sources.index(source_system) if source_system in sources else 999
    return best_rank


def pick_primary(
    concept: str,
    source_systems: List[str],
    amap: AuthorityMap,
) -> Tuple[str, List[str]]:
    """Given a concept and the sources that write it, return
    (primary_source, alternative_sources) under the tenant's authority map.
    With no ranked source, falls back to first-alphabetical (deterministic)."""
    if len(source_systems) <= 1:
        return (source_systems[0] if source_systems else "unknown", [])
    ranked = sorted(source_systems, key=lambda s: (get_authority_rank(concept, s, amap), s))
    return (ranked[0], ranked[1:])


def has_authority_entry(concept: str, amap: AuthorityMap) -> bool:
    """True if the concept matches any prefix in the effective map."""
    return any(concept.startswith(prefix) for prefix in amap)
