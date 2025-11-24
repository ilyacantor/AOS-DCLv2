import os
import psycopg2
from typing import List, Dict, Set
from backend.domain import Persona


class PersonaView:
    
    def __init__(self):
        self.database_url = os.getenv('DATABASE_URL')
        if not self.database_url:
            raise ValueError("DATABASE_URL not set")
    
    def get_relevant_concepts(
        self,
        personas: List[Persona],
        available_concepts: Set[str] = None
    ) -> Dict[str, List[str]]:
        
        if not personas:
            return {}
        
        conn = psycopg2.connect(self.database_url)
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
                relevance = row[2]
                
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
        available_concepts: Set[str] = None
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
