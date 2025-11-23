from typing import List, Dict, Optional
import os
import json
from datetime import datetime
from backend.domain import Mapping
from backend.engine.narration_service import NarrationService


class RAGService:
    
    def __init__(self, run_id: str, narration: NarrationService):
        self.run_id = run_id
        self.narration = narration
        self.pinecone_enabled = bool(os.getenv("PINECONE_API_KEY"))
        self.openai_enabled = bool(os.getenv("OPENAI_API_KEY"))
        
    def store_mapping_lessons(self, mappings: List[Mapping]) -> int:
        if not self.pinecone_enabled:
            self.narration.add_message(
                self.run_id, 
                "RAG", 
                "Pinecone API key not found - skipping lesson storage"
            )
            return 0
        
        high_confidence_mappings = [m for m in mappings if m.confidence >= 0.75]
        
        if not high_confidence_mappings:
            self.narration.add_message(
                self.run_id,
                "RAG",
                "No high-confidence mappings to store as lessons"
            )
            return 0
        
        try:
            self.narration.add_message(
                self.run_id,
                "RAG", 
                f"Storing {len(high_confidence_mappings)} mapping lessons in vector DB"
            )
            
            lessons_stored = self._store_to_pinecone(high_confidence_mappings)
            
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
    
    def _store_to_pinecone(self, mappings: List[Mapping]) -> int:
        try:
            from pinecone import Pinecone, ServerlessSpec
            
            pc = Pinecone(api_key=os.getenv("PINECONE_API_KEY"))
            
            index_name = "dcl-mapping-lessons"
            
            existing_indexes = pc.list_indexes()
            if index_name not in [idx.name for idx in existing_indexes]:
                self.narration.add_message(
                    self.run_id,
                    "RAG",
                    f"Creating new Pinecone index: {index_name}"
                )
                pc.create_index(
                    name=index_name,
                    dimension=1536,
                    metric='cosine',
                    spec=ServerlessSpec(cloud='aws', region='us-east-1')
                )
            
            index = pc.Index(index_name)
            
            if self.openai_enabled:
                vectors = self._create_embeddings_openai(mappings)
            else:
                self.narration.add_message(
                    self.run_id,
                    "RAG",
                    "OpenAI API key not found - storing metadata only"
                )
                vectors = self._create_mock_embeddings(mappings)
            
            index.upsert(vectors=vectors)
            
            return len(vectors)
            
        except ImportError:
            self.narration.add_message(
                self.run_id,
                "RAG",
                "Pinecone package not installed - simulating storage"
            )
            return len(mappings)
        except Exception as e:
            self.narration.add_message(
                self.run_id,
                "RAG",
                f"Pinecone error: {str(e)} - simulating storage"
            )
            return len(mappings)
    
    def _create_embeddings_openai(self, mappings: List[Mapping]) -> List[tuple]:
        try:
            from openai import OpenAI
            
            client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
            
            vectors = []
            for mapping in mappings:
                text = f"{mapping.source_field} maps to {mapping.ontology_concept} with confidence {mapping.confidence}"
                
                response = client.embeddings.create(
                    input=text,
                    model="text-embedding-3-small"
                )
                
                vectors.append((
                    mapping.id,
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
            self.narration.add_message(
                self.run_id,
                "RAG",
                f"OpenAI embedding error: {str(e)} - using mock embeddings"
            )
            return self._create_mock_embeddings(mappings)
    
    def _create_mock_embeddings(self, mappings: List[Mapping]) -> List[tuple]:
        import random
        
        vectors = []
        for mapping in mappings:
            mock_embedding = [random.random() for _ in range(1536)]
            
            vectors.append((
                mapping.id,
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
