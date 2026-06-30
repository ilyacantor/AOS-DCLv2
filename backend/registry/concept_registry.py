"""
ConceptRegistry — loads and validates concepts from ontology_concepts.yaml.

Provides prefix-based validation: if 'revenue' is registered, then
'revenue.total', 'revenue.consulting.managed_services' are all valid.
"""

from functools import lru_cache
from pathlib import Path
import yaml
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

_YAML_DEFAULT = Path(__file__).resolve().parent.parent.parent / "config" / "ontology_concepts.yaml"


@lru_cache(maxsize=None)
def _load_concepts(path: Path) -> dict[str, dict]:
    """Parse the ontology YAML once per path and share it across every
    ConceptRegistry instance.

    Four route/engine modules (conflict_detection, recon_checks,
    ingest_triples, triple_monitor) each construct a ConceptRegistry at import
    time. Without this memo that is four full 164-concept YAML parses on every
    boot and every --reload. The returned dict is READ-ONLY by contract — all
    consumers call is_valid_concept / get_concept / list_concepts / get_domain
    and never mutate it (audited 2026-06-30) — so a single shared instance is
    safe. Mirrors the get_ontology() cache in backend/engine/ontology.py.
    lru_cache does not memoize the raise below, so a missing file still fails
    loud on every attempt until it exists.
    """
    if not path.exists():
        raise FileNotFoundError(
            f"ConceptRegistry: ontology file not found at {path}. "
            "Cannot validate concepts without the ontology."
        )
    with open(path) as f:
        data = yaml.safe_load(f)

    concepts: dict[str, dict] = {}
    for entry in data.get("concepts", []):
        cid = entry.get("id")
        if cid:
            concepts[cid] = entry

    logger.info(f"[ConceptRegistry] Loaded {len(concepts)} concepts from {path.name}")
    return concepts


class ConceptRegistry:
    def __init__(self, yaml_path: str | None = None):
        """Load registered concepts from YAML (memoized per path)."""
        path = Path(yaml_path) if yaml_path else _YAML_DEFAULT
        self._concepts = _load_concepts(path)

    def is_valid_concept(self, concept: str) -> bool:
        """Prefix-based validation.

        The root segment (before the first dot) must match a registered concept id.
        So if 'revenue' is registered, 'revenue', 'revenue.total',
        'revenue.consulting.managed_services' are all valid.
        """
        if not concept:
            return False
        root = concept.split(".")[0]
        return root in self._concepts

    def list_concepts(self) -> list[str]:
        """All registered root concept names."""
        return sorted(self._concepts.keys())

    def get_domain(self, concept: str) -> str | None:
        """Root segment of the concept."""
        if not concept:
            return None
        root = concept.split(".")[0]
        entry = self._concepts.get(root)
        if entry:
            return entry.get("domain")
        return None

    def get_concept(self, concept_id: str) -> dict | None:
        """Get full concept entry by root id."""
        return self._concepts.get(concept_id)
