import os
from typing import List, Dict, Set, Optional
from backend.domain import Persona
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)


def _load_persona_domains() -> Dict[str, List[str]]:
    """Load persona→domain mappings from config/persona_domains.yaml.

    This is the single source of truth for which triple domains each
    persona cares about.  Raises RuntimeError if the file is missing
    or unparseable — no silent fallback.
    """
    import yaml
    yaml_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        "config", "persona_domains.yaml",
    )
    try:
        with open(yaml_path, "r") as f:
            data = yaml.safe_load(f)
    except FileNotFoundError:
        raise RuntimeError(
            f"persona_domains.yaml not found at {yaml_path} — "
            "cannot determine persona→domain mapping"
        )
    except Exception as e:
        raise RuntimeError(
            f"Failed to parse persona_domains.yaml at {yaml_path}: {e}"
        )

    personas_block = data.get("personas")
    if not personas_block or not isinstance(personas_block, dict):
        raise RuntimeError(
            f"persona_domains.yaml at {yaml_path} has no valid 'personas' mapping"
        )

    result: Dict[str, List[str]] = {}
    for persona_key, cfg in personas_block.items():
        domains = cfg.get("domains", [])
        if not isinstance(domains, list) or not domains:
            raise RuntimeError(
                f"persona_domains.yaml: persona '{persona_key}' has no domains list"
            )
        result[persona_key] = domains

    logger.info(f"Loaded persona domain mapping for {len(result)} personas from {yaml_path}")
    return result


def get_persona_domain_mapping() -> Dict[str, List[str]]:
    """Return persona→domain mapping, reloading from YAML each call."""
    return _load_persona_domains()


class PersonaView:
    """Read-only view of persona → triple-domain mapping.

    Source of truth: config/persona_domains.yaml. No DB path, no cache,
    no silent fallback — the YAML load either succeeds or raises.
    """

    def get_relevant_concepts(
        self,
        personas: List[Persona],
        available_concepts: Optional[Set[str]] = None
    ) -> Dict[str, List[str]]:
        if not personas:
            return {}
        mapping = get_persona_domain_mapping()
        result: Dict[str, List[str]] = {}
        for persona in personas:
            concepts = mapping.get(persona.value, [])
            if available_concepts is not None:
                concepts = [c for c in concepts if c in available_concepts]
            result[persona.value] = list(concepts)
        return result

    def get_all_relevant_concept_ids(
        self,
        personas: List[Persona],
        available_concepts: Optional[Set[str]] = None
    ) -> Set[str]:
        persona_concepts = self.get_relevant_concepts(personas, available_concepts)
        all_concepts: Set[str] = set()
        for concepts in persona_concepts.values():
            all_concepts.update(concepts)
        return all_concepts

    def get_persona_relevance_score(
        self,
        persona: Persona,
        concept_id: str
    ) -> float:
        concepts = get_persona_domain_mapping().get(persona.value, [])
        return 0.8 if concept_id in concepts else 0.0
