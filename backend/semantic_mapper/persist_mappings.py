import time
from typing import List, Dict, Any, Optional
from contextlib import contextmanager
from backend.domain import Mapping
from backend.utils.log_utils import get_logger
from backend.core.db import get_connection, close_pool as _close_shared_pool
from backend.core.constants import (
    ONTOLOGY_CACHE_TTL as _ONTOLOGY_CACHE_TTL,
    MAPPINGS_CACHE_TTL as _MAPPINGS_CACHE_TTL,
)

logger = get_logger(__name__)


class MappingPersistence:

    _ontology_cache: Optional[List[Dict[str, Any]]] = None
    _ontology_cache_time: float = 0
    _mappings_cache: Optional[Dict[str, List[Mapping]]] = None
    _mappings_cache_time: float = 0

    ONTOLOGY_CACHE_TTL = _ONTOLOGY_CACHE_TTL
    MAPPINGS_CACHE_TTL = _MAPPINGS_CACHE_TTL

    def __init__(self):
        pass  # pool is managed by backend.core.db

    @contextmanager
    def _get_connection(self):
        """Borrow a connection from the shared pool.

        Yields a live connection or raises OperationalError when the
        database is unavailable (preserves existing caller expectations
        for modules like schema_loader that catch exceptions).
        """
        with get_connection() as conn:
            if conn is None:
                try:
                    import psycopg2
                    raise psycopg2.OperationalError("Connection pool not available")
                except ImportError:
                    raise RuntimeError("Connection pool not available")
            yield conn

    def save_mappings(self, mappings: List[Mapping], clear_existing: bool = False) -> int:
        if not mappings:
            return 0

        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                try:
                    if clear_existing and mappings:
                        source_ids = set(m.source_system for m in mappings)
                        if source_ids:
                            cursor.execute("""
                                DELETE FROM field_concept_mappings
                                WHERE source_id = ANY(%s)
                            """, (list(source_ids),))

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
                            mapping.method
                        ))
                        saved += 1

                    conn.commit()
                    MappingPersistence._invalidate_mappings_cache()
                    return saved
                except Exception as e:
                    conn.rollback()
                    raise

    @classmethod
    def _invalidate_mappings_cache(cls):
        cls._mappings_cache = None
        cls._mappings_cache_time = 0

    def load_mappings(self, source_id: Optional[str] = None) -> List[Mapping]:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
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
                        status="ok"
                    ))

                return mappings

    def load_all_mappings_grouped(self) -> Dict[str, List[Mapping]]:
        now = time.time()
        if (MappingPersistence._mappings_cache is not None and
            (now - MappingPersistence._mappings_cache_time) < self.MAPPINGS_CACHE_TTL):
            logger.debug("Using cached mappings")
            return dict(MappingPersistence._mappings_cache)

        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT source_id, table_name, field_name, concept_id, confidence, method
                    FROM field_concept_mappings
                    ORDER BY source_id, confidence DESC
                """)

                grouped: Dict[str, List[Mapping]] = {}
                for row in cursor.fetchall():
                    source_id = row[0]
                    mapping = Mapping(
                        id=f"{row[0]}_{row[1]}_{row[2]}_{row[3]}",
                        source_system=row[0],
                        source_table=row[1],
                        source_field=row[2],
                        ontology_concept=row[3],
                        confidence=row[4],
                        method=row[5],
                        status="ok"
                    )
                    if source_id not in grouped:
                        grouped[source_id] = []
                    grouped[source_id].append(mapping)

                MappingPersistence._mappings_cache = grouped
                MappingPersistence._mappings_cache_time = time.time()
                logger.info(f"Cached {sum(len(v) for v in grouped.values())} mappings for {len(grouped)} sources")

                return dict(grouped)

    def get_ontology_concepts(self) -> List[Dict[str, Any]]:
        now = time.time()
        if (MappingPersistence._ontology_cache is not None and
            (now - MappingPersistence._ontology_cache_time) < self.ONTOLOGY_CACHE_TTL):
            logger.debug("Using cached ontology concepts")
            return list(MappingPersistence._ontology_cache)

        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT id, name, description, cluster, metadata,
                           synonyms, example_fields
                    FROM ontology_concepts
                    ORDER BY cluster, name
                """)

                concepts = []
                for row in cursor.fetchall():
                    metadata = row[4] or {}
                    synonyms = row[5] or []
                    example_fields = row[6] or []
                    concepts.append({
                        'id': row[0],
                        'name': row[1],
                        'description': row[2],
                        'cluster': row[3],
                        'metadata': metadata,
                        'aliases': synonyms,
                        'example_fields': example_fields,
                    })

                MappingPersistence._ontology_cache = concepts
                MappingPersistence._ontology_cache_time = time.time()
                logger.info(f"Cached {len(concepts)} ontology concepts")

                return list(concepts)

    @classmethod
    def clear_all_caches(cls):
        cls._ontology_cache = None
        cls._ontology_cache_time = 0
        cls._mappings_cache = None
        cls._mappings_cache_time = 0
        logger.info("All caches cleared")

    @classmethod
    def close_pool(cls):
        cls.clear_all_caches()
        _close_shared_pool()
