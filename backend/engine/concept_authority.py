"""Concept authority ranking — determines primary source per concept domain.

When multiple source_systems write the same concept (e.g., both netsuite and
jira emit engineering.sprint_velocity), this table determines which source
is authoritative. The first entry in the list is highest authority.

Used by triple_store.get_concept_collisions() to detect and rank overlapping
writes. The Sankey graph shows ALL sources; authority ranking determines which
is labeled "primary" in the collisions response.

Sources not listed here default to lowest authority. The ERP SoR (netsuite,
oracle, sap, etc.) is the default authority for domains not in this table.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple

# concept prefix → ordered list of authoritative source_systems.
# First entry = highest authority. Checked via longest-prefix match.
_CONCEPT_AUTHORITY: Dict[str, List[str]] = {
    # Engineering — Jira is the system of record
    "engineering": ["jira", "github_actions"],

    # Infrastructure monitoring — Datadog is the system of record
    "infrastructure.incidents": ["datadog", "pagerduty"],
    "infrastructure.mttr": ["datadog", "pagerduty"],
    "infrastructure.uptime": ["datadog"],
    "infrastructure.downtime": ["datadog"],

    # Cloud spend — AWS Cost Explorer is the system of record
    "infrastructure.cloud_spend": ["aws_cost_explorer"],
}


def get_authority_rank(concept: str, source_system: str) -> int:
    """Return authority rank for a source_system writing a given concept.

    Lower rank = higher authority. Returns 0 for the top authority,
    1 for second, etc. Returns 999 for unranked sources (default/ERP).

    Uses longest-prefix matching: "infrastructure.cloud_spend.total"
    matches "infrastructure.cloud_spend" before "infrastructure".
    """
    best_match_len = 0
    best_rank = 999

    for prefix, sources in _CONCEPT_AUTHORITY.items():
        if concept.startswith(prefix) and len(prefix) > best_match_len:
            best_match_len = len(prefix)
            if source_system in sources:
                best_rank = sources.index(source_system)
            else:
                best_rank = 999

    return best_rank


def pick_primary(
    concept: str,
    source_systems: List[str],
) -> Tuple[str, List[str]]:
    """Given a concept and list of source_systems that write it, return
    (primary_source, alternative_sources) based on authority ranking.

    If no source is in the authority table, returns the first source
    alphabetically (deterministic fallback).
    """
    if len(source_systems) <= 1:
        return (source_systems[0] if source_systems else "unknown", [])

    ranked = sorted(source_systems, key=lambda s: (get_authority_rank(concept, s), s))
    return (ranked[0], ranked[1:])


def has_authority_entry(concept: str) -> bool:
    """Return True if the concept matches any entry in the authority table."""
    for prefix in _CONCEPT_AUTHORITY:
        if concept.startswith(prefix):
            return True
    return False
