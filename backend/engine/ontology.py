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

# Minimal fallback — only used if YAML file is missing
_FALLBACK_ONTOLOGY: List[OntologyConcept] = [
    OntologyConcept(id="account", name="Account", description="Business account or customer entity",
                    example_fields=["account_id", "account_name", "company", "customer_id"], expected_type="string"),
    OntologyConcept(id="opportunity", name="Opportunity", description="Sales opportunity or deal",
                    example_fields=["opportunity_id", "deal_id", "pipeline", "stage"], expected_type="string"),
    OntologyConcept(id="revenue", name="Revenue", description="Revenue or monetary amount",
                    example_fields=["amount", "revenue", "total", "value", "price"], expected_type="float"),
    OntologyConcept(id="cost", name="Cost", description="Cost or expense amount",
                    example_fields=["cost", "spend", "expense", "fee"], expected_type="float"),
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
    for entry in data.get("concepts", []):
        metadata = entry.get("metadata", {})
        concept = OntologyConcept(
            id=entry["id"],
            name=entry["name"],
            description=entry.get("description", ""),
            example_fields=metadata.get("example_fields", []),
            expected_type=metadata.get("expected_type", "string"),
        )
        concepts.append(concept)

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
