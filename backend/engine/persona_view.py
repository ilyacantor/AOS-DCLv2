"""
PersonaView — resolves which ontology concepts are relevant to each persona.

Data path is selected by the caller via the `app_mode` parameter:
  - "Demo" → YAML config (config/persona_profiles.yaml). No DB touched.
  - "Farm" → PostgreSQL (persona_profiles + persona_concept_relevance). Requires DATABASE_URL.

No environment sniffing. No fallbacks.
If the required resource for the requested mode is missing, a structured error is raised.
"""

import os
import logging
from pathlib import Path
from typing import Dict, List, Literal, Optional, Set

import yaml

from backend.domain import Persona

logger = logging.getLogger(__name__)

CONFIG_DIR = Path(__file__).parent.parent.parent / "config"
YAML_PATH = CONFIG_DIR / "persona_profiles.yaml"

AppMode = Literal["Demo", "Farm"]


class PersonaViewError(Exception):
    """Structured error with machine-readable context."""

    def __init__(self, reason: str, missing_dependency: str, resolution: str):
        self.reason = reason
        self.missing_dependency = missing_dependency
        self.resolution = resolution
        super().__init__(
            f"{reason} | missing: {missing_dependency} | fix: {resolution}"
        )


class PersonaView:

    def __init__(self):
        # Eagerly load YAML so Demo never hits I/O at query time.
        # DB URL is read once and held; connection is made per-request.
        self._yaml_profiles: Dict[str, List[Dict]] = self._load_yaml_profiles()
        self._database_url: Optional[str] = os.getenv("DATABASE_URL")

    # ------------------------------------------------------------------
    # Public API — every method takes app_mode
    # ------------------------------------------------------------------

    def get_relevant_concepts(
        self,
        personas: List[Persona],
        app_mode: AppMode = "Demo",
        available_concepts: Optional[Set[str]] = None,
    ) -> Dict[str, List[str]]:
        if not personas:
            return {}
        if app_mode == "Demo":
            self._require_yaml()
            return self._get_relevant_concepts_yaml(personas, available_concepts)
        self._require_db()
        return self._get_relevant_concepts_db(personas, available_concepts)

    def get_all_relevant_concept_ids(
        self,
        personas: List[Persona],
        app_mode: AppMode = "Demo",
        available_concepts: Optional[Set[str]] = None,
    ) -> Set[str]:
        persona_concepts = self.get_relevant_concepts(personas, app_mode, available_concepts)
        all_concepts: Set[str] = set()
        for concepts in persona_concepts.values():
            all_concepts.update(concepts)
        return all_concepts

    def get_persona_relevance_score(
        self,
        persona: Persona,
        concept_id: str,
        app_mode: AppMode = "Demo",
    ) -> float:
        if app_mode == "Demo":
            self._require_yaml()
            return self._get_relevance_score_yaml(persona, concept_id)
        self._require_db()
        return self._get_relevance_score_db(persona, concept_id)

    # ------------------------------------------------------------------
    # Precondition checks
    # ------------------------------------------------------------------

    def _require_yaml(self) -> None:
        if not self._yaml_profiles:
            raise PersonaViewError(
                reason="Demo mode requires persona YAML config but it is missing or empty",
                missing_dependency=str(YAML_PATH),
                resolution="Create config/persona_profiles.yaml with persona definitions",
            )

    def _require_db(self) -> None:
        if not self._database_url:
            raise PersonaViewError(
                reason="Farm mode requires DATABASE_URL but it is not set",
                missing_dependency="DATABASE_URL environment variable",
                resolution="Set DATABASE_URL to a PostgreSQL connection string (e.g. Supabase)",
            )

    # ------------------------------------------------------------------
    # YAML implementation (Demo)
    # ------------------------------------------------------------------

    @staticmethod
    def _load_yaml_profiles() -> Dict[str, List[Dict]]:
        """Load persona profiles from YAML. Returns empty dict if file missing."""
        if not YAML_PATH.exists():
            return {}
        with open(YAML_PATH) as f:
            data = yaml.safe_load(f)
        if not data or "personas" not in data:
            return {}
        profiles: Dict[str, List[Dict]] = {}
        for p in data["personas"]:
            profiles[p["persona_key"]] = p.get("concept_relevance", [])
        return profiles

    def _get_relevant_concepts_yaml(
        self,
        personas: List[Persona],
        available_concepts: Optional[Set[str]] = None,
    ) -> Dict[str, List[str]]:
        result: Dict[str, List[str]] = {}
        for persona in personas:
            key = persona.value
            entries = self._yaml_profiles.get(key, [])
            concepts = [
                e["concept_id"]
                for e in sorted(entries, key=lambda x: -x.get("relevance", 0))
                if available_concepts is None or e["concept_id"] in available_concepts
            ]
            if concepts:
                result[key] = concepts
        return result

    def _get_relevance_score_yaml(self, persona: Persona, concept_id: str) -> float:
        entries = self._yaml_profiles.get(persona.value, [])
        for e in entries:
            if e["concept_id"] == concept_id:
                return float(e.get("relevance", 0.0))
        return 0.0

    # ------------------------------------------------------------------
    # DB implementation (Farm)
    # ------------------------------------------------------------------

    def _get_relevant_concepts_db(
        self,
        personas: List[Persona],
        available_concepts: Optional[Set[str]] = None,
    ) -> Dict[str, List[str]]:
        import psycopg2

        conn = psycopg2.connect(self._database_url)
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

            result: Dict[str, List[str]] = {}
            for row in cursor.fetchall():
                persona_key, concept_id, _relevance = row
                if available_concepts is None or concept_id in available_concepts:
                    result.setdefault(persona_key, []).append(concept_id)
            return result
        finally:
            cursor.close()
            conn.close()

    def _get_relevance_score_db(self, persona: Persona, concept_id: str) -> float:
        import psycopg2

        conn = psycopg2.connect(self._database_url)
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
