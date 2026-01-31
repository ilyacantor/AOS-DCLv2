from typing import List, Literal, Optional
from backend.domain import SourceSystem, Mapping
from backend.engine.ontology import get_ontology
from backend.utils.log_utils import get_logger
from .heuristic_mapper import HeuristicMapper
from .persist_mappings import MappingPersistence

logger = get_logger(__name__)


class SemanticMapper:
    
    def __init__(self):
        self._persistence = None
    
    @property
    def persistence(self):
        if self._persistence is None:
            try:
                self._persistence = MappingPersistence()
            except Exception as e:
                logger.warning(f"Failed to initialize DB persistence: {e}")
                self._persistence = None
        return self._persistence
    
    def run_mapping(
        self,
        sources: List[SourceSystem],
        mode: Literal["heuristic", "full"] = "heuristic",
        clear_existing: bool = False
    ) -> tuple[List[Mapping], dict]:
        
        ontology_concepts = []
        try:
            if self.persistence:
                ontology_concepts = self.persistence.get_ontology_concepts()
        except Exception as e:
            logger.warning(f"Failed to load ontology from DB: {e}. Using local ontology.")
        
        if not ontology_concepts:
            local_ontology = get_ontology()
            ontology_concepts = [
                {'id': c.id, 'name': c.name, 'description': c.description}
                for c in local_ontology
            ]
        
        stats = {
            'sources_processed': len(sources),
            'mappings_created': 0,
            'heuristic_mappings': 0,
            'rag_enhanced': 0,
            'llm_refined': 0,
            'db_available': self.persistence is not None
        }
        
        heuristic_mapper = HeuristicMapper(ontology_concepts)
        mappings = heuristic_mapper.create_mappings(sources)
        stats['heuristic_mappings'] = len(mappings)
        
        if mode == "full":
            pass
        
        try:
            if self.persistence:
                saved = self.persistence.save_mappings(mappings, clear_existing=clear_existing)
                stats['mappings_created'] = saved
            else:
                stats['mappings_created'] = len(mappings)
                logger.info(f"DB unavailable - {len(mappings)} mappings created in-memory only")
        except Exception as e:
            logger.warning(f"Failed to save mappings to DB: {e}. Mappings available in-memory.")
            stats['mappings_created'] = len(mappings)
        
        return mappings, stats
    
    def get_stored_mappings(self, source_id: Optional[str] = None) -> List[Mapping]:
        try:
            if self.persistence:
                return self.persistence.load_mappings(source_id)
        except Exception as e:
            logger.warning(f"Failed to load mappings from DB: {e}")
        return []
    
    def get_all_mappings_grouped(self) -> dict:
        try:
            if self.persistence:
                return self.persistence.load_all_mappings_grouped()
        except Exception as e:
            logger.warning(f"Failed to load grouped mappings from DB: {e}")
        return {}
