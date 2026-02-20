"""
Ontology concept definitions.

Loads from config/ontology_concepts.yaml (source of truth).
Falls back to a minimal hardcoded list only if the YAML is missing.
"""
import os
from pathlib import Path
from typing import List, Dict, Optional

from backend.domain import OntologyConcept
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

_YAML_PATH = Path(__file__).parent.parent.parent / "config" / "ontology_concepts.yaml"

VALID_DOMAINS = {
    "finance", "sales", "hr", "customer_success",
    "product_eng", "it_infra", "operations", "marketing", "compliance",
}

# Minimal fallback — only used if YAML file is missing
_FALLBACK_ONTOLOGY: List[OntologyConcept] = [
    OntologyConcept(id="account", name="Account", description="Business account or customer entity",
                    example_fields=["account_id", "account_name", "company", "customer_id"], expected_type="string"),
    OntologyConcept(id="revenue", name="Revenue", description="Revenue or monetary amount",
                    example_fields=["amount", "revenue", "total", "value", "price"], expected_type="float"),
    OntologyConcept(id="date", name="Date/Timestamp", description="Date or timestamp field",
                    example_fields=["date", "timestamp", "created_at", "updated_at"], expected_type="datetime"),
]

_cached_ontology: Optional[List[OntologyConcept]] = None


def _load_from_yaml() -> List[OntologyConcept]:
    """Parse config/ontology_concepts.yaml into OntologyConcept objects."""
    import yaml

    with open(_YAML_PATH) as f:
        data = yaml.safe_load(f)

    concepts: List[OntologyConcept] = []
    seen_ids: set = set()
    seen_concept_ids: set = set()

    for entry in data.get("concepts", []):
        cid = entry.get("id")
        if not cid:
            raise ValueError("Concept entry missing required field: id")
        if cid in seen_ids:
            raise ValueError(f"Duplicate concept id: {cid}")
        seen_ids.add(cid)

        concept_id = entry.get("concept_id", "")
        if concept_id:
            if concept_id in seen_concept_ids:
                raise ValueError(f"Duplicate concept_id: {concept_id}")
            seen_concept_ids.add(concept_id)

        domain = entry.get("domain", "")
        if domain and domain not in VALID_DOMAINS:
            raise ValueError(f"Invalid domain '{domain}' for concept '{cid}'. Valid: {VALID_DOMAINS}")

        name = entry.get("name")
        if not name:
            raise ValueError(f"Concept '{cid}' missing required field: name")

        description = entry.get("description", "")
        if not description:
            raise ValueError(f"Concept '{cid}' missing required field: description")

        concept = OntologyConcept(
            id=cid,
            concept_id=concept_id,
            name=name,
            description=description,
            domain=domain,
            cluster=entry.get("cluster", ""),
            example_fields=entry.get("example_fields", []),
            aliases=entry.get("aliases", []),
            expected_type=entry.get("expected_type", "string"),
            typical_source_systems=entry.get("typical_source_systems", []),
            persona_relevance=entry.get("persona_relevance", {}),
        )
        concepts.append(concept)

    if not concepts:
        raise ValueError(f"No concepts found in {_YAML_PATH}")

    return concepts


def get_ontology() -> List[OntologyConcept]:
    """Return ontology concepts, loading from YAML on first call."""
    global _cached_ontology
    if _cached_ontology is not None:
        return _cached_ontology

    if _YAML_PATH.exists():
        try:
            _cached_ontology = _load_from_yaml()
            logger.info(f"[Ontology] Loaded {len(_cached_ontology)} concepts from {_YAML_PATH.name}")
            return _cached_ontology
        except Exception as e:
            logger.error(f"[Ontology] Failed to load YAML ({e}), using fallback", exc_info=True)

    logger.warning("[Ontology] YAML not found, using minimal fallback list")
    _cached_ontology = list(_FALLBACK_ONTOLOGY)
    return _cached_ontology


def get_ontology_by_id(ontology_id: str) -> OntologyConcept:
    for concept in get_ontology():
        if concept.id == ontology_id:
            return concept
    raise ValueError(f"Ontology concept not found: {ontology_id}")


def reload_ontology() -> List[OntologyConcept]:
    """Force re-read from YAML. Useful after config changes."""
    global _cached_ontology
    _cached_ontology = None
    return get_ontology()
