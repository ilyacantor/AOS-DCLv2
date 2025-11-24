from typing import List, Literal
from backend.domain import SourceSystem, Mapping
from .heuristic_mapper import HeuristicMapper
from .persist_mappings import MappingPersistence


class SemanticMapper:
    
    def __init__(self):
        self.persistence = MappingPersistence()
    
    def run_mapping(
        self,
        sources: List[SourceSystem],
        mode: Literal["heuristic", "full"] = "heuristic",
        clear_existing: bool = False
    ) -> tuple[List[Mapping], dict]:
        
        ontology_concepts = self.persistence.get_ontology_concepts()
        
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
        
        saved = self.persistence.save_mappings(mappings, clear_existing=clear_existing)
        stats['mappings_created'] = saved
        
        return mappings, stats
    
    def get_stored_mappings(self, source_id: str = None) -> List[Mapping]:
        return self.persistence.load_mappings(source_id)
