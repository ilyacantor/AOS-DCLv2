"""
ConceptRegistry — loads and validates concepts from ontology_concepts.yaml.

Provides prefix-based validation: if 'revenue' is registered, then
'revenue.total', 'revenue.consulting.managed_services' are all valid.
"""

from pathlib import Path
import yaml
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

_YAML_DEFAULT = Path(__file__).resolve().parent.parent.parent / "config" / "ontology_concepts.yaml"


class ConceptRegistry:
    def __init__(self, yaml_path: str | None = None):
        """Load registered concepts from YAML."""
        path = Path(yaml_path) if yaml_path else _YAML_DEFAULT
        if not path.exists():
            raise FileNotFoundError(
                f"ConceptRegistry: ontology file not found at {path}. "
                "Cannot validate concepts without the ontology."
            )
        with open(path) as f:
            data = yaml.safe_load(f)

        self._concepts: dict[str, dict] = {}
        for entry in data.get("concepts", []):
            cid = entry.get("id")
            if cid:
                self._concepts[cid] = entry

        logger.info(f"[ConceptRegistry] Loaded {len(self._concepts)} concepts from {path.name}")

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
