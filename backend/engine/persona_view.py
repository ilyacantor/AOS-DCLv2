import os
import time
from typing import List, Dict, Set, Optional
from backend.domain import Persona
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

def _load_persona_concepts_from_yaml() -> Dict[str, List[str]]:
    """Load persona→concept mappings from config/persona_profiles.yaml."""
    import yaml
    yaml_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        "config", "persona_profiles.yaml"
    )
    try:
        with open(yaml_path, "r") as f:
            data = yaml.safe_load(f)
        result: Dict[str, List[str]] = {}
        for persona in data.get("personas", []):
            key = persona.get("persona_key", "")
            concepts = [
                cr["concept_id"]
                for cr in persona.get("concept_relevance", [])
            ]
            result[key] = concepts
        return result
    except Exception as e:
        logger.warning(f"Failed to load persona_profiles.yaml: {e}")
        return {}


# Hardcoded fallback using actual ontology concept IDs
_HARDCODED_DEFAULTS = {
    "CFO": ["revenue", "cost", "subscription", "account", "invoice", "date"],
    "CRO": ["account", "opportunity", "revenue", "health", "date"],
    "COO": ["usage", "health", "ticket", "employee", "account", "date"],
    "CTO": ["aws_resource", "incident", "engineering_work", "usage", "cost", "health", "date"],
    "CHRO": ["employee", "date"],
}

# Try YAML first, fall back to hardcoded
DEFAULT_PERSONA_CONCEPTS = _load_persona_concepts_from_yaml() or _HARDCODED_DEFAULTS


class PersonaView:
    
    _concepts_cache: Optional[Dict[str, List[str]]] = None
    _cache_time: float = 0
    CACHE_TTL: float = 300.0

    def __init__(self):
        self.database_url = os.getenv('DATABASE_URL')
        if not self.database_url:
            logger.warning("DATABASE_URL not set - using default persona concepts")
            self._use_defaults = True
        else:
            self._use_defaults = False
    
    def _get_pool(self):
        try:
            from backend.semantic_mapper.persist_mappings import MappingPersistence
            persistence = MappingPersistence()
            return persistence
        except Exception as e:
            logger.warning(f"Failed to get connection pool: {e}")
            return None

    def get_relevant_concepts(
        self,
        personas: List[Persona],
        available_concepts: Optional[Set[str]] = None
    ) -> Dict[str, List[str]]:

        if not personas:
            return {}

        if self._use_defaults:
            return self._get_defaults(personas, available_concepts)
        
        now = time.time()
        if PersonaView._concepts_cache is not None and (now - PersonaView._cache_time) < self.CACHE_TTL:
            return self._filter_cached_concepts(personas, available_concepts)

        pool = self._get_pool()
        if pool is None:
            return self._get_defaults(personas, available_concepts)
        
        try:
            with pool._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT pp.persona_key, pcr.concept_id, pcr.relevance
                        FROM persona_profiles pp
                        JOIN persona_concept_relevance pcr ON pp.id = pcr.persona_id
                        ORDER BY pp.persona_key, pcr.relevance DESC
                    """)
                    
                    all_concepts: Dict[str, List[str]] = {}
                    for row in cursor.fetchall():
                        persona_key = row[0]
                        concept_id = row[1]
                        
                        if persona_key not in all_concepts:
                            all_concepts[persona_key] = []
                        all_concepts[persona_key].append(concept_id)
                    
                    PersonaView._concepts_cache = all_concepts
                    PersonaView._cache_time = time.time()
                    logger.info(f"Cached persona concepts for {len(all_concepts)} personas")
                    
                    return self._filter_cached_concepts(personas, available_concepts)
                    
        except Exception as e:
            logger.warning(f"Failed to load persona concepts from DB: {e}. Using defaults.")
            return self._get_defaults(personas, available_concepts)
    
    def _get_defaults(
        self,
        personas: List[Persona],
        available_concepts: Optional[Set[str]] = None
    ) -> Dict[str, List[str]]:
        result = {}
        for persona in personas:
            concepts = DEFAULT_PERSONA_CONCEPTS.get(persona.value, [])
            if available_concepts is not None:
                concepts = [c for c in concepts if c in available_concepts]
            result[persona.value] = concepts
        return result
    
    def _filter_cached_concepts(
        self,
        personas: List[Persona],
        available_concepts: Optional[Set[str]] = None
    ) -> Dict[str, List[str]]:
        if PersonaView._concepts_cache is None:
            return self._get_defaults(personas, available_concepts)
        
        result = {}
        for persona in personas:
            concepts = PersonaView._concepts_cache.get(persona.value, 
                       DEFAULT_PERSONA_CONCEPTS.get(persona.value, []))
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

        all_concepts = set()
        for concepts in persona_concepts.values():
            all_concepts.update(concepts)

        return all_concepts

    def get_persona_relevance_score(
        self,
        persona: Persona,
        concept_id: str
    ) -> float:

        if self._use_defaults:
            concepts = DEFAULT_PERSONA_CONCEPTS.get(persona.value, [])
            return 0.8 if concept_id in concepts else 0.0
        
        if PersonaView._concepts_cache is not None:
            concepts = PersonaView._concepts_cache.get(persona.value, [])
            return 0.8 if concept_id in concepts else 0.0

        pool = self._get_pool()
        if pool is None:
            concepts = DEFAULT_PERSONA_CONCEPTS.get(persona.value, [])
            return 0.8 if concept_id in concepts else 0.0
        
        try:
            with pool._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT pcr.relevance
                        FROM persona_profiles pp
                        JOIN persona_concept_relevance pcr ON pp.id = pcr.persona_id
                        WHERE pp.persona_key = %s AND pcr.concept_id = %s
                    """, (persona.value, concept_id))

                    row = cursor.fetchone()
                    return row[0] if row else 0.0
        except Exception as e:
            logger.warning(f"Failed to get relevance score: {e}")
            concepts = DEFAULT_PERSONA_CONCEPTS.get(persona.value, [])
            return 0.8 if concept_id in concepts else 0.0
    
    @classmethod
    def clear_cache(cls):
        cls._concepts_cache = None
        cls._cache_time = 0
