"""
Ontology loader — reads config/ontology_concepts.yaml as the single source of truth,
builds and validates OntologyConcept instances at first access.
"""

import os
from pathlib import Path
from typing import List, Optional

import yaml

from backend.domain import OntologyConcept
from backend.domain.models import VALID_DOMAINS


_CORE_ONTOLOGY: Optional[List[OntologyConcept]] = None

_CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "ontology_concepts.yaml"


def _load_ontology_from_yaml(path: Path = _CONFIG_PATH) -> List[OntologyConcept]:
    """Load ontology concepts from YAML and validate each entry."""
    if not path.exists():
        raise FileNotFoundError(f"Ontology config not found: {path}")

    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    if not raw or "concepts" not in raw:
        raise ValueError(f"Ontology YAML missing 'concepts' key: {path}")

    concepts: List[OntologyConcept] = []
    seen_ids: set = set()
    seen_concept_ids: set = set()

    for entry in raw["concepts"]:
        cid = entry.get("id")
        concept_id = entry.get("concept_id", "")

        # Required fields
        for field in ("id", "name", "description", "domain"):
            if not entry.get(field):
                raise ValueError(f"Concept '{cid}' missing required field: {field}")

        # Domain validation
        domain = entry["domain"]
        if domain not in VALID_DOMAINS:
            raise ValueError(
                f"Concept '{cid}' has invalid domain '{domain}'. "
                f"Valid: {VALID_DOMAINS}"
            )

        # Uniqueness
        if cid in seen_ids:
            raise ValueError(f"Duplicate concept id: {cid}")
        seen_ids.add(cid)

        if concept_id:
            if concept_id in seen_concept_ids:
                raise ValueError(f"Duplicate concept_id: {concept_id}")
            seen_concept_ids.add(concept_id)

        concept = OntologyConcept(
            id=cid,
            concept_id=concept_id,
            name=entry["name"],
            description=entry["description"],
            domain=domain,
            cluster=entry.get("cluster", ""),
            example_fields=entry.get("example_fields", []),
            aliases=entry.get("aliases", []),
            expected_type=entry.get("expected_type"),
            typical_source_systems=entry.get("typical_source_systems", []),
            persona_relevance=entry.get("persona_relevance", {}),
        )
        concepts.append(concept)

    if not concepts:
        raise ValueError("Ontology YAML contains zero concepts")

    return concepts


def get_ontology() -> List[OntologyConcept]:
    """Return the full ontology concept list (lazy-loaded, cached)."""
    global _CORE_ONTOLOGY
    if _CORE_ONTOLOGY is None:
        _CORE_ONTOLOGY = _load_ontology_from_yaml()
    return _CORE_ONTOLOGY


def get_ontology_by_id(ontology_id: str) -> OntologyConcept:
    """Look up a single concept by its short id (e.g. 'revenue')."""
    for concept in get_ontology():
        if concept.id == ontology_id:
            return concept
    raise ValueError(f"Ontology concept not found: {ontology_id}")


def reload_ontology() -> List[OntologyConcept]:
    """Force-reload from YAML (useful after config changes or in tests)."""
    global _CORE_ONTOLOGY
    _CORE_ONTOLOGY = None
    return get_ontology()
