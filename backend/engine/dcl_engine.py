import time
import uuid
from typing import List, Literal, Dict, Any, Optional
from backend.domain import (
    Persona, SourceSystem, GraphSnapshot, GraphNode, GraphLink, 
    RunMetrics, Mapping, OntologyConcept
)
from backend.engine.schema_loader import SchemaLoader
from backend.engine.ontology import get_ontology
from backend.engine.mapping_service import MappingService
from backend.engine.narration_service import NarrationService
from backend.engine.persona_view import PersonaView
from backend.semantic_mapper import SemanticMapper


class DCLEngine:
    
    def __init__(self):
        self.narration = NarrationService()
        self.persona_view = PersonaView()
    
    def build_graph_snapshot(
        self,
        mode: Literal["Demo", "Farm"],
        run_mode: Literal["Dev", "Prod"],
        personas: List[Persona],
        run_id: str,
        sample_limit: int = 5
    ) -> tuple[GraphSnapshot, RunMetrics]:
        
        start_time = time.time()
        metrics = RunMetrics()
        
        self.narration.add_message(run_id, "Engine", f"Starting DCL engine in {mode} mode, {run_mode} run mode")
        
        if mode == "Demo":
            sources = SchemaLoader.load_demo_schemas()
            self.narration.add_message(run_id, "Engine", f"Loaded {len(sources)} Demo sources")
        else:
            sources = SchemaLoader.load_farm_schemas(self.narration, run_id, sample_limit=sample_limit)
            self.narration.add_message(run_id, "Engine", f"Loaded {len(sources)} Farm sources (sample_limit={sample_limit})")
        
        ontology = get_ontology()
        self.narration.add_message(run_id, "Engine", f"Loaded {len(ontology)} ontology concepts")
        
        semantic_mapper = SemanticMapper()
        source_ids = [s.id for s in sources]
        stored_mappings = []
        for sid in source_ids:
            stored_mappings.extend(semantic_mapper.get_stored_mappings(sid))
        
        if stored_mappings:
            self.narration.add_message(run_id, "Engine", f"Using {len(stored_mappings)} stored mappings from database")
            mappings = stored_mappings
        else:
            self.narration.add_message(run_id, "Engine", "No stored mappings found - running semantic mapper to create and persist mappings")
            mappings, stats = semantic_mapper.run_mapping(sources, mode="heuristic", clear_existing=False)
            self.narration.add_message(
                run_id, "Engine",
                f"Created and persisted {stats['mappings_created']} mappings using heuristics"
            )
        
        graph = self._build_graph(mode, sources, ontology, mappings, personas, run_id)
        
        processing_time = (time.time() - start_time) * 1000
        metrics.processing_ms = processing_time
        
        render_start = time.time()
        snapshot = GraphSnapshot(
            nodes=graph["nodes"],
            links=graph["links"],
            meta={
                "mode": mode,
                "run_id": run_id,
                "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "stats": {
                    "sources": len(sources),
                    "ontology_concepts": len(ontology),
                    "mappings": len(mappings),
                    "personas": [p.value for p in personas]
                }
            }
        )
        
        render_time = (time.time() - render_start) * 1000
        metrics.render_ms = render_time
        
        self.narration.add_message(
            run_id, "Engine",
            f"Graph built: {len(graph['nodes'])} nodes, {len(graph['links'])} links in {processing_time:.0f}ms"
        )
        
        return snapshot, metrics
    
    def _build_graph(
        self,
        mode: str,
        sources: List[SourceSystem],
        ontology: List[OntologyConcept],
        mappings: List[Mapping],
        personas: List[Persona],
        run_id: str
    ) -> Dict[str, List]:
        
        nodes: List[GraphNode] = []
        links: List[GraphLink] = []
        
        pipe_id = f"pipe_{mode.lower()}"
        nodes.append(GraphNode(
            id=pipe_id,
            label=f"{mode} Pipeline",
            level="L0",
            kind="pipe",
            group=mode,
            status="ok",
            metrics={"source_count": len(sources)}
        ))
        
        source_mapping_count = {}
        for source in sources:
            source_id = f"source_{source.id}"
            table_count = len(source.tables)
            field_count = sum(len(t.fields) for t in source.tables)
            
            nodes.append(GraphNode(
                id=source_id,
                label=source.name,
                level="L1",
                kind="source",
                group=source.type,
                status="ok",
                metrics={
                    "tables": table_count,
                    "fields": field_count,
                    "type": source.type
                }
            ))
            
            links.append(GraphLink(
                id=f"link_pipe_{source.id}",
                source=pipe_id,
                target=source_id,
                value=float(table_count),
                flow_type="schema",
                info_summary=f"{table_count} tables, {field_count} fields"
            ))
            
            source_mapping_count[source.id] = 0
        
        relevant_concept_ids = self.persona_view.get_all_relevant_concept_ids(personas)
        
        ontology_mapping_count = {}
        concept_field_mappings = {}
        for concept in ontology:
            if concept.id in relevant_concept_ids:
                concept_id = f"ontology_{concept.id}"
                concept_field_mappings[concept.id] = []
                ontology_mapping_count[concept.id] = 0
        
        for mapping in mappings:
            if mapping.ontology_concept in relevant_concept_ids:
                source_id = f"source_{mapping.source_system}"
                concept_id = f"ontology_{mapping.ontology_concept}"
                
                if source_id and concept_id:
                    link_id = f"link_{mapping.source_system}_{mapping.ontology_concept}_{uuid.uuid4().hex[:8]}"
                    links.append(GraphLink(
                        id=link_id,
                        source=source_id,
                        target=concept_id,
                        value=mapping.confidence,
                        confidence=mapping.confidence,
                        flow_type="mapping",
                        info_summary=f"{mapping.source_field} → {mapping.ontology_concept} ({mapping.method}, {mapping.confidence:.2f})"
                    ))
                    
                    if mapping.source_system in source_mapping_count:
                        source_mapping_count[mapping.source_system] += 1
                    if mapping.ontology_concept in ontology_mapping_count:
                        ontology_mapping_count[mapping.ontology_concept] += 1
                    if mapping.ontology_concept in concept_field_mappings:
                        concept_field_mappings[mapping.ontology_concept].append({
                            "field": mapping.source_field,
                            "table": mapping.source_table,
                            "source": mapping.source_system,
                            "confidence": mapping.confidence
                        })
        
        for concept in ontology:
            if concept.id in relevant_concept_ids:
                mapping_count = ontology_mapping_count.get(concept.id, 0)
                if mapping_count == 0:
                    continue
                
                concept_id = f"ontology_{concept.id}"
                field_list = concept_field_mappings.get(concept.id, [])
                contributing_fields = [f"{m['table']}.{m['field']}" for m in field_list[:3]]
                
                source_hierarchy = {}
                for m in field_list:
                    src = m['source']
                    tbl = m['table']
                    if src not in source_hierarchy:
                        source_hierarchy[src] = {}
                    if tbl not in source_hierarchy[src]:
                        source_hierarchy[src][tbl] = []
                    source_hierarchy[src][tbl].append({
                        "field": m['field'],
                        "confidence": m['confidence']
                    })
                
                nodes.append(GraphNode(
                    id=concept_id,
                    label=concept.name,
                    level="L2",
                    kind="ontology",
                    group="Ontology",
                    status="ok",
                    metrics={
                        "description": concept.description,
                        "input_count": mapping_count,
                        "explanation": f"Derived from {len(field_list)} field(s)",
                        "contributing_fields": contributing_fields,
                        "source_hierarchy": source_hierarchy
                    }
                ))
        
        persona_concepts = self.persona_view.get_relevant_concepts(personas)
        
        for persona in personas:
            bll_id = f"bll_{persona.value.lower()}"
            nodes.append(GraphNode(
                id=bll_id,
                label=f"BLL {persona.value}",
                level="L3",
                kind="bll",
                group="Business Logic",
                status="ok",
                metrics={"persona": persona.value}
            ))
            
            relevant_concepts = persona_concepts.get(persona.value, [])
            for concept_id in relevant_concepts:
                concept_node_id = f"ontology_{concept_id}"
                if ontology_mapping_count.get(concept_id, 0) > 0:
                    link_id = f"link_{concept_id}_{persona.value}_{uuid.uuid4().hex[:8]}"
                    weight = ontology_mapping_count.get(concept_id, 1)
                    links.append(GraphLink(
                        id=link_id,
                        source=concept_node_id,
                        target=bll_id,
                        value=float(weight),
                        flow_type="consumption",
                        info_summary=f"{concept_id} consumed by {persona.value} BLL"
                    ))
        
        return {"nodes": nodes, "links": links}
