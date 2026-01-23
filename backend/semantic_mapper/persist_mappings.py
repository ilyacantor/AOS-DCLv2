import os
import psycopg2
from typing import List, Dict, Any
from backend.domain import Mapping


class MappingPersistence:
    
    _connection = None
    
    def __init__(self):
        self.database_url = os.getenv('DATABASE_URL')
        if not self.database_url:
            raise ValueError("DATABASE_URL not set")
    
    def _get_connection(self):
        try:
            if MappingPersistence._connection is None or MappingPersistence._connection.closed:
                MappingPersistence._connection = psycopg2.connect(self.database_url)
            else:
                MappingPersistence._connection.cursor().execute("SELECT 1")
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            MappingPersistence._connection = psycopg2.connect(self.database_url)
        return MappingPersistence._connection
    
    def save_mappings(self, mappings: List[Mapping], clear_existing: bool = False) -> int:
        if not mappings:
            return 0
        
        conn = psycopg2.connect(self.database_url)
        cursor = conn.cursor()
        
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
            return saved
        
        finally:
            cursor.close()
            conn.close()
    
    def load_mappings(self, source_id: str = None) -> List[Mapping]:
        conn = self._get_connection()
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
                    status="ok"
                ))
            
            return mappings
        
        finally:
            cursor.close()
    
    def load_all_mappings_grouped(self) -> Dict[str, List[Mapping]]:
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
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
            
            return grouped
        
        finally:
            cursor.close()
    
    def get_ontology_concepts(self) -> List[Dict[str, Any]]:
        conn = self._get_connection()
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
                    'id': row[0],
                    'name': row[1],
                    'description': row[2],
                    'cluster': row[3],
                    'metadata': row[4]
                })
            
            return concepts
        
        finally:
            cursor.close()
            conn.close()
