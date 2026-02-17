from typing import List, Dict, Optional
import os
from backend.domain import SourceSystem, OntologyConcept, Mapping, RunMetrics
from backend.engine.narration_service import NarrationService
from backend.engine.rag_service import RAGService
from backend.core.constants import (
    CONFIDENCE_CONCEPT_MATCH, CONFIDENCE_EXAMPLE_EXACT,
    CONFIDENCE_EXAMPLE_PARTIAL, CONFIDENCE_DEFAULT,
)


class MappingService:
    
    def __init__(self, run_mode: str, run_id: str, narration: NarrationService):
        self.run_mode = run_mode
        self.run_id = run_id
        self.narration = narration
        self.metrics = RunMetrics()
    
    def create_mappings(
        self, 
        sources: List[SourceSystem], 
        ontology: List[OntologyConcept]
    ) -> List[Mapping]:
        
        mappings = []
        
        for source in sources:
            for table in source.tables:
                for field in table.fields:
                    matched_concept = self._match_field_to_ontology(
                        field.name, 
                        field.semantic_hint or "",
                        ontology
                    )
                    
                    if matched_concept:
                        mapping = Mapping(
                            id=f"{source.id}_{table.name}_{field.name}_{matched_concept.id}",
                            source_field=field.name,
                            source_table=table.name,
                            source_system=source.id,
                            ontology_concept=matched_concept.id,
                            confidence=self._calculate_confidence(field.name, matched_concept),
                            method="heuristic",
                            status="ok"
                        )
                        mappings.append(mapping)
        
        # Store mapping lessons in RAG for both Dev and Prod
        rag_service = RAGService(self.run_mode, self.run_id, self.narration)
        lessons_stored = rag_service.store_mapping_lessons(mappings)
        
        self.metrics.rag_writes += lessons_stored
        
        if self.run_mode == "Prod":
            self.narration.add_message(self.run_id, "RAG", "Prod mode: LLM enhancements enabled")
            # In Prod, RAGService uses OpenAI embeddings (counted as LLM calls)
            self.metrics.llm_calls += lessons_stored  # Each embedding = 1 LLM call
            self.metrics.rag_reads += 3  # RAG lookup attempts
        else:
            self.narration.add_message(self.run_id, "Engine", "Dev mode: Using heuristics with mock embeddings")
            self.metrics.rag_reads += 0  # No RAG reads in Dev
        
        return mappings
    
    def _match_field_to_ontology(
        self, 
        field_name: str, 
        semantic_hint: str,
        ontology: List[OntologyConcept]
    ) -> Optional[OntologyConcept]:
        
        field_lower = field_name.lower()
        
        for concept in ontology:
            for example in concept.example_fields:
                if example.lower() in field_lower or field_lower in example.lower():
                    return concept
            
            if concept.id in field_lower or field_lower in concept.id:
                return concept
        
        if semantic_hint == "amount":
            for concept in ontology:
                if concept.id in ["revenue", "cost"]:
                    return concept
        
        if semantic_hint == "id":
            for concept in ontology:
                if "account" in field_lower and concept.id == "account":
                    return concept
                if "opportunity" in field_lower and concept.id == "opportunity":
                    return concept
        
        return None
    
    def _calculate_confidence(self, field_name: str, concept: OntologyConcept) -> float:
        field_lower = field_name.lower()
        
        if concept.id in field_lower:
            return CONFIDENCE_CONCEPT_MATCH

        for example in concept.example_fields:
            if example.lower() == field_lower:
                return CONFIDENCE_EXAMPLE_EXACT
            if example.lower() in field_lower or field_lower in example.lower():
                return CONFIDENCE_EXAMPLE_PARTIAL

        return CONFIDENCE_DEFAULT
