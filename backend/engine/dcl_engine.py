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


class DCLEngine:
    
    def __init__(self):
        self.narration = NarrationService()
    
    def build_graph_snapshot(
        self,
        mode: Literal["Demo", "Farm"],
        run_mode: Literal["Dev", "Prod"],
        personas: List[Persona],
        run_id: str
    ) -> tuple[GraphSnapshot, RunMetrics]:
        
        start_time = time.time()
        metrics = RunMetrics()
        
        self.narration.add_message(run_id, "Engine", f"Starting DCL engine in {mode} mode, {run_mode} run mode")
        
        if mode == "Demo":
            sources = SchemaLoader.load_demo_schemas()
            self.narration.add_message(run_id, "Engine", f"Loaded {len(sources)} Demo sources")
        else:
            sources = SchemaLoader.load_farm_schemas(self.narration, run_id)
            self.narration.add_message(run_id, "Engine", f"Loaded {len(sources)} Farm sources")
        
        ontology = get_ontology()
        self.narration.add_message(run_id, "Engine", f"Loaded {len(ontology)} ontology concepts")
        
        mapping_service = MappingService(run_mode, run_id, self.narration)
        mappings = mapping_service.create_mappings(sources, ontology)
        
        metrics.llm_calls = mapping_service.metrics.llm_calls
        metrics.rag_reads = mapping_service.metrics.rag_reads
        metrics.rag_writes = mapping_service.metrics.rag_writes
        
        self.narration.add_message(
            run_id, "Engine", 
            f"Created {len(mappings)} mappings (LLM: {metrics.llm_calls}, RAG reads: {metrics.rag_reads}, RAG writes: {metrics.rag_writes})"
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
        
        persona_mappings = {
            Persona.CFO: ["revenue", "cost"],
            Persona.CRO: ["account", "opportunity", "revenue"],
            Persona.COO: ["usage", "health"],
            Persona.CTO: ["aws_resource", "usage", "cost"]
        }
        
        relevant_concept_ids = set()
        for persona in personas:
            relevant_concept_ids.update(persona_mappings.get(persona, []))
        
        ontology_mapping_count = {}
        for concept in ontology:
            if concept.id in relevant_concept_ids:
                concept_id = f"ontology_{concept.id}"
                nodes.append(GraphNode(
                    id=concept_id,
                    label=concept.name,
                    level="L2",
                    kind="ontology",
                    group="Ontology",
                    status="ok",
                    metrics={"description": concept.description}
                ))
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
            
            relevant_concepts = persona_mappings.get(persona, [])
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
