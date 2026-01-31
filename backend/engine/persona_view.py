import os
import psycopg2
from typing import List, Dict, Set, Optional
from backend.domain import Persona
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

# Default concept mappings when database is not available
DEFAULT_PERSONA_CONCEPTS = {
    "CFO": ["Revenue", "Cost", "Margin", "CustomerValue", "ARR", "Churn"],
    "CRO": ["Revenue", "Pipeline", "Conversion", "CustomerValue", "LeadScore"],
    "COO": ["Throughput", "Efficiency", "Cost", "AssetUtilization", "SLA"],
    "CTO": ["SystemHealth", "Latency", "Uptime", "SecurityScore", "TechDebt"],
}


class PersonaView:

    def __init__(self):
        self.database_url = os.getenv('DATABASE_URL')
        if not self.database_url:
            logger.warning("DATABASE_URL not set - using default persona concepts")
            self._use_defaults = True
        else:
            self._use_defaults = False

    def get_relevant_concepts(
        self,
        personas: List[Persona],
        available_concepts: Optional[Set[str]] = None
    ) -> Dict[str, List[str]]:

        if not personas:
            return {}

        # Helper function for fallback to defaults
        def use_default_concepts():
            result = {}
            for persona in personas:
                concepts = DEFAULT_PERSONA_CONCEPTS.get(persona.value, [])
                if available_concepts is not None:
                    concepts = [c for c in concepts if c in available_concepts]
                result[persona.value] = concepts
            return result

        # Use defaults if database not available
        if self._use_defaults:
            return use_default_concepts()

        try:
            conn = psycopg2.connect(self.database_url)
        except Exception as e:
            logger.warning(f"Database connection failed, using defaults: {e}")
            return use_default_concepts()

        cursor = conn.cursor()

        try:
            persona_keys = [p.value for p in personas]

            cursor.execute("""
                SELECT pp.persona_key, pcr.concept_id, pcr.relevance
                FROM persona_profiles pp
                JOIN persona_concept_relevance pcr ON pp.id = pcr.persona_id
                WHERE pp.persona_key = ANY(%s)
                ORDER BY pp.persona_key, pcr.relevance DESC
            """, (persona_keys,))

            result = {}
            for row in cursor.fetchall():
                persona_key = row[0]
                concept_id = row[1]

                if available_concepts is None or concept_id in available_concepts:
                    if persona_key not in result:
                        result[persona_key] = []
                    result[persona_key].append(concept_id)

            return result

        finally:
            cursor.close()
            conn.close()

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

        # Use default score if database not available
        if self._use_defaults:
            concepts = DEFAULT_PERSONA_CONCEPTS.get(persona.value, [])
            return 0.8 if concept_id in concepts else 0.0

        conn = psycopg2.connect(self.database_url)
        cursor = conn.cursor()

        try:
            cursor.execute("""
                SELECT pcr.relevance
                FROM persona_profiles pp
                JOIN persona_concept_relevance pcr ON pp.id = pcr.persona_id
                WHERE pp.persona_key = %s AND pcr.concept_id = %s
            """, (persona.value, concept_id))

            row = cursor.fetchone()
            return row[0] if row else 0.0

        finally:
            cursor.close()
            conn.close()
