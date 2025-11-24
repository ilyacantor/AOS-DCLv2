from typing import List, Optional, Dict, Any
from backend.domain import SourceSystem, Mapping


class HeuristicMapper:
    
    def __init__(self, ontology_concepts: List[Dict[str, Any]]):
        self.concepts = ontology_concepts
    
    def create_mappings(self, sources: List[SourceSystem]) -> List[Mapping]:
        mappings = []
        
        for source in sources:
            for table in source.tables:
                for field in table.fields:
                    matched_concept = self._match_field_to_concept(
                        field.name,
                        field.semantic_hint or "",
                        field.type
                    )
                    
                    if matched_concept:
                        confidence = self._calculate_confidence(
                            field.name,
                            matched_concept
                        )
                        
                        mapping = Mapping(
                            id=f"{source.id}_{table.name}_{field.name}_{matched_concept['id']}",
                            source_field=field.name,
                            source_table=table.name,
                            source_system=source.id,
                            ontology_concept=matched_concept['id'],
                            confidence=confidence,
                            method="heuristic",
                            status="ok"
                        )
                        mappings.append(mapping)
        
        return mappings
    
    def _match_field_to_concept(
        self,
        field_name: str,
        semantic_hint: str,
        field_type: str
    ) -> Optional[Dict[str, Any]]:
        field_lower = field_name.lower()
        
        for concept in self.concepts:
            metadata = concept.get('metadata', {})
            example_fields = metadata.get('example_fields', [])
            synonyms = metadata.get('synonyms', [])
            
            for example in example_fields:
                if example.lower() in field_lower or field_lower in example.lower():
                    return concept
            
            for synonym in synonyms:
                if synonym.lower() in field_lower or field_lower in synonym.lower():
                    return concept
            
            if concept['id'] in field_lower or field_lower in concept['id']:
                return concept
        
        if semantic_hint == "amount":
            for concept in self.concepts:
                if concept['id'] in ["revenue", "cost"]:
                    return concept
        
        if semantic_hint == "id":
            for concept in self.concepts:
                if "account" in field_lower and concept['id'] == "account":
                    return concept
                if "opportunity" in field_lower and concept['id'] == "opportunity":
                    return concept
        
        return None
    
    def _calculate_confidence(self, field_name: str, concept: Dict[str, Any]) -> float:
        field_lower = field_name.lower()
        concept_id = concept['id']
        metadata = concept.get('metadata', {})
        example_fields = metadata.get('example_fields', [])
        
        if concept_id in field_lower:
            return 0.95
        
        for example in example_fields:
            if example.lower() == field_lower:
                return 0.90
            if example.lower() in field_lower or field_lower in example.lower():
                return 0.75
        
        return 0.60
