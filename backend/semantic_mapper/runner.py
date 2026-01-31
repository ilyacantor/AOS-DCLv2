import logging
from typing import List, Literal, Dict, Optional
from backend.domain import SourceSystem, Mapping
from .heuristic_mapper import HeuristicMapper
from .persist_mappings import MappingPersistence

logger = logging.getLogger("dcl.semantic_mapper")


class SemanticMapper:
    
    def __init__(self):
        self.persistence: Optional[MappingPersistence] = None
        try:
            self.persistence = MappingPersistence()
        except Exception as e:
            logger.warning(f"Database not available: {e}. Running in memory-only mode.")
    
    def run_mapping(
        self,
        sources: List[SourceSystem],
        mode: Literal["heuristic", "full"] = "heuristic",
        clear_existing: bool = False
    ) -> tuple[List[Mapping], dict]:
        
        ontology_concepts = []
        if self.persistence:
            try:
                ontology_concepts = self.persistence.get_ontology_concepts()
            except Exception as e:
                logger.warning(f"Failed to load ontology concepts: {e}")
        
        stats = {
            'sources_processed': len(sources),
            'mappings_created': 0,
            'heuristic_mappings': 0,
            'rag_enhanced': 0,
            'llm_refined': 0
        }
        
        heuristic_mapper = HeuristicMapper(ontology_concepts)
        mappings = heuristic_mapper.create_mappings(sources)
        stats['heuristic_mappings'] = len(mappings)
        
        if mode == "full":
            pass
        
        if self.persistence:
            try:
                saved = self.persistence.save_mappings(mappings, clear_existing=clear_existing)
                stats['mappings_created'] = saved
            except Exception as e:
                logger.warning(f"Failed to save mappings: {e}")
                stats['mappings_created'] = 0
        
        return mappings, stats
    
    def get_stored_mappings(self, source_id: str = None) -> List[Mapping]:
        if not self.persistence:
            return []
        try:
            return self.persistence.load_mappings(source_id)
        except Exception as e:
            logger.warning(f"Failed to load mappings: {e}")
            return []
    
    def get_all_mappings_grouped(self) -> Dict[str, List[Mapping]]:
        if not self.persistence:
            return {}
        try:
            return self.persistence.load_all_mappings_grouped()
        except Exception as e:
            logger.warning(f"Failed to load grouped mappings: {e}")
            return {}
