from typing import List, Dict, Optional
import os
import json
from datetime import datetime
from backend.domain import Mapping
from backend.engine.narration_service import NarrationService
from backend.core.constants import (
    RAG_CONFIDENCE_THRESHOLD, PINECONE_INDEX_NAME,
    PINECONE_DIMENSION, PINECONE_CLOUD, PINECONE_REGION,
)


class RAGService:
    
    def __init__(self, run_mode: str, run_id: str, narration: NarrationService):
        self.run_mode = run_mode
        self.run_id = run_id
        self.narration = narration
        self.pinecone_enabled = bool(os.getenv("PINECONE_API_KEY"))
        self.openai_enabled = bool(os.getenv("OPENAI_API_KEY"))
        
    def store_mapping_lessons(self, mappings: List[Mapping]) -> int:
        if self.run_mode != "Prod":
            self.narration.add_message(
                self.run_id,
                "RAG",
                "Dev mode — RAG writes skipped (read-only in Dev)"
            )
            return 0

        if not self.pinecone_enabled:
            self.narration.add_message(
                self.run_id, 
                "RAG", 
                "Pinecone API key not found - skipping lesson storage"
            )
            return 0
        
        high_confidence_mappings = [m for m in mappings if m.confidence >= RAG_CONFIDENCE_THRESHOLD]
        
        if not high_confidence_mappings:
            self.narration.add_message(
                self.run_id,
                "RAG",
                "No high-confidence mappings to store as lessons"
            )
            return 0
        
        unique_mappings = self._deduplicate_mappings(high_confidence_mappings)
        
        if len(unique_mappings) < len(high_confidence_mappings):
            duplicates_removed = len(high_confidence_mappings) - len(unique_mappings)
            self.narration.add_message(
                self.run_id,
                "RAG",
                f"Removed {duplicates_removed} duplicate lessons, storing {len(unique_mappings)} unique lessons"
            )
        
        try:
            self.narration.add_message(
                self.run_id,
                "RAG", 
                f"Storing {len(unique_mappings)} mapping lessons in vector DB"
            )
            
            lessons_stored = self._store_to_pinecone(unique_mappings)
            
            self.narration.add_message(
                self.run_id,
                "RAG",
                f"Successfully stored {lessons_stored} lessons in Pinecone"
            )
            
            return lessons_stored
            
        except Exception as e:
            self.narration.add_message(
                self.run_id,
                "RAG",
                f"Error storing lessons: {str(e)}"
            )
            return 0
    
    def _deduplicate_mappings(self, mappings: List[Mapping]) -> List[Mapping]:
        seen = {}
        unique = []
        
        for mapping in mappings:
            key = f"{mapping.source_field.lower()}:{mapping.ontology_concept}"
            
            if key not in seen:
                seen[key] = mapping
                unique.append(mapping)
            else:
                existing = seen[key]
                if mapping.confidence > existing.confidence:
                    unique.remove(existing)
                    unique.append(mapping)
                    seen[key] = mapping
        
        return unique
    
    def _store_to_pinecone(self, mappings: List[Mapping]) -> int:
        try:
            from pinecone import Pinecone, ServerlessSpec
            
            pc = Pinecone(api_key=os.getenv("PINECONE_API_KEY"))
            
            index_name = PINECONE_INDEX_NAME

            existing_indexes = pc.list_indexes()
            if index_name not in [idx.name for idx in existing_indexes]:
                self.narration.add_message(
                    self.run_id,
                    "RAG",
                    f"Creating new Pinecone index: {index_name}"
                )
                pc.create_index(
                    name=index_name,
                    dimension=PINECONE_DIMENSION,
                    metric='cosine',
                    spec=ServerlessSpec(cloud=PINECONE_CLOUD, region=PINECONE_REGION)
                )

            index = pc.Index(index_name)
            
            if self.run_mode == "Prod" and self.openai_enabled:
                vectors = self._create_embeddings_openai(mappings)
            else:
                self.narration.add_message(
                    self.run_id,
                    "RAG",
                    "Dev mode: storing metadata with mock embeddings (fast)"
                )
                vectors = self._create_mock_embeddings(mappings)
            
            index.upsert(vectors=vectors)
            
            return len(vectors)
            
        except ImportError:
            self.narration.add_message(
                self.run_id,
                "RAG",
                "Pinecone package not installed - storage skipped (0 stored)"
            )
            return 0
        except Exception as e:
            self.narration.add_message(
                self.run_id,
                "RAG",
                f"Pinecone error: {str(e)} - storage failed (0 stored)"
            )
            return 0
    
    def _create_embeddings_openai(self, mappings: List[Mapping]) -> List[tuple]:
        try:
            from openai import OpenAI
            
            client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
            
            vectors = []
            for mapping in mappings:
                text = f"{mapping.source_field} maps to {mapping.ontology_concept}"
                
                response = client.embeddings.create(
                    input=text,
                    model="text-embedding-3-small"
                )
                
                lesson_id = f"lesson:{mapping.source_field.lower()}:{mapping.ontology_concept}"
                
                vectors.append((
                    lesson_id,
                    response.data[0].embedding,
                    {
                        "source_field": mapping.source_field,
                        "source_table": mapping.source_table,
                        "source_system": mapping.source_system,
                        "ontology_concept": mapping.ontology_concept,
                        "confidence": mapping.confidence,
                        "method": mapping.method,
                        "timestamp": datetime.now().isoformat()
                    }
                ))
            
            self.narration.add_message(
                self.run_id,
                "LLM",
                f"Generated {len(vectors)} embeddings via OpenAI"
            )
            
            return vectors
            
        except Exception as e:
            from backend.utils.log_utils import get_logger as _gl
            _gl(__name__).error(f"OpenAI embedding failed, falling back to mock embeddings: {e}", exc_info=True)
            self.narration.add_message(
                self.run_id,
                "RAG",
                f"OpenAI embedding error: {str(e)} - FALLING BACK to mock embeddings (results degraded)"
            )
            return self._create_mock_embeddings(mappings)
    
    def _create_mock_embeddings(self, mappings: List[Mapping]) -> List[tuple]:
        import random
        
        vectors = []
        for mapping in mappings:
            mock_embedding = [random.random() for _ in range(1536)]
            
            lesson_id = f"lesson:{mapping.source_field.lower()}:{mapping.ontology_concept}"
            
            vectors.append((
                lesson_id,
                mock_embedding,
                {
                    "source_field": mapping.source_field,
                    "source_table": mapping.source_table,
                    "source_system": mapping.source_system,
                    "ontology_concept": mapping.ontology_concept,
                    "confidence": mapping.confidence,
                    "method": mapping.method,
                    "timestamp": datetime.now().isoformat()
                }
            ))
        
        return vectors
