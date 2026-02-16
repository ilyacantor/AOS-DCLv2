"""
MappingPersistence — stores and retrieves field-to-concept mappings.

Data path is selected by the caller via the `app_mode` parameter:
  - "Demo" → In-memory store + ontology from config/ontology_concepts.yaml. No DB touched.
  - "Farm" → PostgreSQL (field_concept_mappings + ontology_concepts). Requires DATABASE_URL.

No environment sniffing. No fallbacks.
If the required resource for the requested mode is missing, a structured error is raised.
"""

import os
import logging
from pathlib import Path
from typing import Dict, List, Literal, Any, Optional

import yaml

from backend.domain import Mapping

logger = logging.getLogger(__name__)

CONFIG_DIR = Path(__file__).parent.parent.parent / "config"
ONTOLOGY_YAML_PATH = CONFIG_DIR / "ontology_concepts.yaml"

AppMode = Literal["Demo", "Farm"]


class MappingPersistenceError(Exception):
    """Structured error with machine-readable context."""

    def __init__(self, reason: str, missing_dependency: str, resolution: str):
        self.reason = reason
        self.missing_dependency = missing_dependency
        self.resolution = resolution
        super().__init__(
            f"{reason} | missing: {missing_dependency} | fix: {resolution}"
        )


class MappingPersistence:

    def __init__(self):
        # In-memory store for Demo mode, keyed by source_system
        self._mem_mappings: Dict[str, List[Mapping]] = {}
        # DB URL read once at init; connection is made per-request
        self._database_url: Optional[str] = os.getenv("DATABASE_URL")

    # ------------------------------------------------------------------
    # Public API — every method takes app_mode
    # ------------------------------------------------------------------

    def save_mappings(
        self, mappings: List[Mapping], app_mode: AppMode = "Demo", clear_existing: bool = False
    ) -> int:
        if not mappings:
            return 0
        if app_mode == "Demo":
            return self._save_mappings_mem(mappings, clear_existing)
        self._require_db()
        return self._save_mappings_db(mappings, clear_existing)

    def load_mappings(self, app_mode: AppMode = "Demo", source_id: str = None) -> List[Mapping]:
        if app_mode == "Demo":
            return self._load_mappings_mem(source_id)
        self._require_db()
        return self._load_mappings_db(source_id)

    def get_ontology_concepts(self, app_mode: AppMode = "Demo") -> List[Dict[str, Any]]:
        if app_mode == "Demo":
            return self._get_ontology_concepts_yaml()
        self._require_db()
        return self._get_ontology_concepts_db()

    # ------------------------------------------------------------------
    # Precondition checks
    # ------------------------------------------------------------------

    def _require_db(self) -> None:
        if not self._database_url:
            raise MappingPersistenceError(
                reason="Farm mode requires DATABASE_URL but it is not set",
                missing_dependency="DATABASE_URL environment variable",
                resolution="Set DATABASE_URL to a PostgreSQL connection string (e.g. Supabase)",
            )

    # ==================================================================
    # In-memory implementation (Demo)
    # ==================================================================

    def _save_mappings_mem(self, mappings: List[Mapping], clear_existing: bool) -> int:
        if clear_existing:
            source_ids = {m.source_system for m in mappings}
            for sid in source_ids:
                self._mem_mappings.pop(sid, None)

        saved = 0
        for m in mappings:
            self._mem_mappings.setdefault(m.source_system, []).append(m)
            saved += 1
        return saved

    def _load_mappings_mem(self, source_id: str = None) -> List[Mapping]:
        if source_id:
            return sorted(
                self._mem_mappings.get(source_id, []),
                key=lambda m: -m.confidence,
            )
        all_mappings: List[Mapping] = []
        for mappings in self._mem_mappings.values():
            all_mappings.extend(mappings)
        return sorted(all_mappings, key=lambda m: (m.source_system, -m.confidence))

    @staticmethod
    def _get_ontology_concepts_yaml() -> List[Dict[str, Any]]:
        if not ONTOLOGY_YAML_PATH.exists():
            raise MappingPersistenceError(
                reason="Demo mode requires ontology YAML config but it is missing",
                missing_dependency=str(ONTOLOGY_YAML_PATH),
                resolution="Create config/ontology_concepts.yaml with concept definitions",
            )
        with open(ONTOLOGY_YAML_PATH) as f:
            data = yaml.safe_load(f)
        if not data or "concepts" not in data:
            raise MappingPersistenceError(
                reason="Ontology YAML is empty or malformed (missing 'concepts' key)",
                missing_dependency=str(ONTOLOGY_YAML_PATH),
                resolution="Ensure the file has a top-level 'concepts' list",
            )
        return [
            {
                "id": c["id"],
                "name": c["name"],
                "description": c.get("description", ""),
                "cluster": c.get("cluster", ""),
                "metadata": c.get("metadata"),
            }
            for c in data["concepts"]
        ]

    # ==================================================================
    # Database implementation (Farm)
    # ==================================================================

    def _save_mappings_db(self, mappings: List[Mapping], clear_existing: bool) -> int:
        import psycopg2

        conn = psycopg2.connect(self._database_url)
        cursor = conn.cursor()

        try:
            if clear_existing and mappings:
                source_ids = list({m.source_system for m in mappings})
                if source_ids:
                    cursor.execute(
                        "DELETE FROM field_concept_mappings WHERE source_id = ANY(%s)",
                        (source_ids,),
                    )

            saved = 0
            for mapping in mappings:
                cursor.execute("""
                    INSERT INTO field_concept_mappings
                        (source_id, table_name, field_name, concept_id, confidence, reason, method)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (source_id, table_name, field_name, concept_id) DO UPDATE SET
                        confidence = EXCLUDED.confidence,
                        reason = EXCLUDED.reason,
                        method = EXCLUDED.method,
                        updated_at = CURRENT_TIMESTAMP
                """, (
                    mapping.source_system,
                    mapping.source_table,
                    mapping.source_field,
                    mapping.ontology_concept,
                    mapping.confidence,
                    f"{mapping.method}: {mapping.source_field} -> {mapping.ontology_concept}",
                    mapping.method,
                ))
                saved += 1

            conn.commit()
            return saved
        finally:
            cursor.close()
            conn.close()

    def _load_mappings_db(self, source_id: str = None) -> List[Mapping]:
        import psycopg2

        conn = psycopg2.connect(self._database_url)
        cursor = conn.cursor()

        try:
            if source_id:
                cursor.execute("""
                    SELECT source_id, table_name, field_name, concept_id, confidence, method
                    FROM field_concept_mappings
                    WHERE source_id = %s
                    ORDER BY confidence DESC
                """, (source_id,))
            else:
                cursor.execute("""
                    SELECT source_id, table_name, field_name, concept_id, confidence, method
                    FROM field_concept_mappings
                    ORDER BY source_id, confidence DESC
                """)

            mappings = []
            for row in cursor.fetchall():
                mappings.append(Mapping(
                    id=f"{row[0]}_{row[1]}_{row[2]}_{row[3]}",
                    source_system=row[0],
                    source_table=row[1],
                    source_field=row[2],
                    ontology_concept=row[3],
                    confidence=row[4],
                    method=row[5],
                    status="ok",
                ))
            return mappings
        finally:
            cursor.close()
            conn.close()

    def _get_ontology_concepts_db(self) -> List[Dict[str, Any]]:
        import psycopg2

        conn = psycopg2.connect(self._database_url)
        cursor = conn.cursor()

        try:
            cursor.execute("""
                SELECT id, name, description, cluster, metadata
                FROM ontology_concepts
                ORDER BY cluster, name
            """)

            concepts = []
            for row in cursor.fetchall():
                concepts.append({
                    "id": row[0],
                    "name": row[1],
                    "description": row[2],
                    "cluster": row[3],
                    "metadata": row[4],
                })
            return concepts
        finally:
            cursor.close()
            conn.close()
