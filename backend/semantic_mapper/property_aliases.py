"""Canonical field -> property normalization for the records-path.

DCL owns field->concept mapping AND field->property normalization (RACI
decision (c) / A10). AAM's records-path (POST /api/dcl/ingest-records) hands
raw records; record_converter maps the concept (Live Semantic Mapper) and
normalizes the property predicate through THIS layer, so a business record's
triple carries a canonical property (customer.name, invoice.payment_status)
instead of the raw source field (customer.company_name, invoice.status).

The vocabulary is ported from AAM's app/ingest/mappings.py to preserve the
established triple shape across the AAM->DCL ingest cutover (aam_deferred_work
#59, Option A) -- no consumer migrates. The mapping lives in
config/concept_property_aliases.yaml: an addressable, registered DATA SOURCE.
Option C (#59 -- learned/ontology-driven normalization) replaces what backs
canonical_property() WITHOUT re-plumbing record_converter's call site.

Default (no registered alias for a field): property = source field name,
which is provenance-faithful -- not a silent fallback, but the defined
behavior. A missing or malformed registry file fails loud (A1).
"""
from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional

import yaml

_YAML_PATH = Path(__file__).resolve().parent.parent.parent / "config" / "concept_property_aliases.yaml"

# concept -> {source_field -> canonical_property}, loaded once.
_aliases: Optional[Dict[str, Dict[str, str]]] = None


def _load() -> Dict[str, Dict[str, str]]:
    global _aliases
    if _aliases is None:
        if not _YAML_PATH.is_file():
            raise RuntimeError(
                f"FATAL: concept_property_aliases registry missing at {_YAML_PATH}. "
                "The records-path property normalization (RACI decision (c)) requires it."
            )
        with _YAML_PATH.open() as fh:
            data = yaml.safe_load(fh) or {}
        table = data.get("concept_property_aliases")
        if not isinstance(table, dict):
            raise RuntimeError(
                f"FATAL: {_YAML_PATH} has no 'concept_property_aliases' mapping "
                f"(got {type(table).__name__}). Fix the registry."
            )
        norm: Dict[str, Dict[str, str]] = {}
        for concept, fields in table.items():
            if not isinstance(fields, dict):
                raise RuntimeError(
                    f"FATAL: concept_property_aliases[{concept!r}] must be a mapping, "
                    f"got {type(fields).__name__}."
                )
            norm[str(concept)] = {str(k): str(v) for k, v in fields.items()}
        _aliases = norm
    return _aliases


def canonical_property(concept: str, source_field: str) -> str:
    """Canonical property predicate for (concept, source_field).

    Returns the registered alias when one exists, else the raw source_field
    (the defined provenance-faithful default for fields with no remap).
    """
    by_concept = _load().get(concept)
    if by_concept:
        aliased = by_concept.get(source_field)
        if aliased:
            return aliased
    return source_field


def reload_aliases() -> None:
    """Drop the cached table so the next call re-reads the registry (tests/ops)."""
    global _aliases
    _aliases = None
