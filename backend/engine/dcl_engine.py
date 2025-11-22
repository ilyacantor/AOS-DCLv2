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
            sources = SchemaLoader.load_farm_schemas()
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
        persona_views = self._generate_persona_views(personas, sources, ontology, mappings)
        
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
                },
                "persona_views": persona_views
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
        
        ontology_mapping_count = {}
        for concept in ontology:
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
        
        persona_mappings = {
            Persona.CFO: ["revenue", "cost"],
            Persona.CRO: ["account", "opportunity", "revenue"],
            Persona.COO: ["usage", "health"],
            Persona.CTO: ["aws_resource", "usage", "cost"]
        }
        
        for persona in personas:
            bll_id = f"bll_{persona.value.lower()}"
            nodes.append(GraphNode(
                id=bll_id,
                label=persona.value,
                level="L3",
                kind="bll",
                group="Business Logic",
                status="ok",
                metrics={"persona": persona.value},
                persona_id=persona.value
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
    
    def _generate_persona_views(
        self,
        personas: List[Persona],
        sources: List[SourceSystem],
        ontology: List[OntologyConcept],
        mappings: List[Mapping]
    ) -> List[Dict]:
        """Generate persona-specific views with metrics, insights, and alerts"""
        import random
        
        persona_configs = {
            Persona.CFO: {
                "title": "Chief Financial Officer View",
                "focus_areas": ["Revenue", "Cost", "P&L"],
                "key_entities": ["revenue", "cost"],
                "sample_metrics": [
                    {"label": "Total Revenue", "value": 2450000, "unit": "$", "trend": "up", "delta": 8.5},
                    {"label": "Operating Cost", "value": 1820000, "unit": "$", "trend": "down", "delta": 3.2},
                    {"label": "Profit Margin", "value": 25.7, "unit": "%", "trend": "up", "delta": 2.1},
                    {"label": "Data Quality", "value": 94, "unit": "%", "trend": "flat", "delta": 0}
                ],
                "sample_insights": [
                    "Revenue variance increased 8.5% this quarter",
                    "Operating costs reduced by 3.2% year-over-year"
                ],
                "sample_alerts": [
                    "2 revenue fields missing from NetSuite integration"
                ]
            },
            Persona.CRO: {
                "title": "Chief Revenue Officer View",
                "focus_areas": ["Accounts", "Opportunities", "Pipeline"],
                "key_entities": ["account", "opportunity", "revenue"],
                "sample_metrics": [
                    {"label": "Active Accounts", "value": 1847, "unit": "", "trend": "up", "delta": 12.3},
                    {"label": "Pipeline Value", "value": 4200000, "unit": "$", "trend": "up", "delta": 15.7},
                    {"label": "Win Rate", "value": 32.5, "unit": "%", "trend": "up", "delta": 4.2},
                    {"label": "Data Coverage", "value": 96, "unit": "%", "trend": "up", "delta": 1.5}
                ],
                "sample_insights": [
                    "Pipeline value up 15.7% month-over-month",
                    "Win rate improved to 32.5% from 28.3%"
                ],
                "sample_alerts": []
            },
            Persona.COO: {
                "title": "Chief Operating Officer View",
                "focus_areas": ["Operations", "Usage", "Health"],
                "key_entities": ["usage", "health"],
                "sample_metrics": [
                    {"label": "System Uptime", "value": 99.8, "unit": "%", "trend": "flat", "delta": 0},
                    {"label": "Active Users", "value": 8420, "unit": "", "trend": "up", "delta": 6.8},
                    {"label": "Avg Response", "value": 145, "unit": "ms", "trend": "down", "delta": 8.3},
                    {"label": "Data Freshness", "value": 92, "unit": "%", "trend": "up", "delta": 2.1}
                ],
                "sample_insights": [
                    "Active users grew 6.8% this period",
                    "System uptime maintained at 99.8%"
                ],
                "sample_alerts": [
                    "1 usage data source showing increased latency"
                ]
            },
            Persona.CTO: {
                "title": "Chief Technology Officer View",
                "focus_areas": ["Infrastructure", "Resources", "Costs"],
                "key_entities": ["aws_resource", "usage", "cost"],
                "sample_metrics": [
                    {"label": "Active Resources", "value": 342, "unit": "", "trend": "up", "delta": 5.2},
                    {"label": "Infra Cost", "value": 45200, "unit": "$", "trend": "up", "delta": 3.7},
                    {"label": "CPU Utilization", "value": 68, "unit": "%", "trend": "up", "delta": 4.1},
                    {"label": "Schema Coverage", "value": 89, "unit": "%", "trend": "up", "delta": 3.2}
                ],
                "sample_insights": [
                    "Infrastructure costs increased 3.7% due to scaling",
                    "CPU utilization trending up to 68% capacity"
                ],
                "sample_alerts": [
                    "Cost allocation incomplete for 3 AWS resource types"
                ]
            }
        }
        
        views = []
        for persona in personas:
            config = persona_configs.get(persona)
            if not config:
                continue
            
            metrics = [
                {
                    "id": f"{persona.value.lower()}_metric_{i}",
                    "label": m["label"],
                    "value": m["value"],
                    "unit": m.get("unit"),
                    "trend": m["trend"],
                    "trend_delta_pct": m.get("delta")
                }
                for i, m in enumerate(config["sample_metrics"])
            ]
            
            insights = [
                {
                    "id": f"{persona.value.lower()}_insight_{i}",
                    "severity": "info",
                    "message": insight
                }
                for i, insight in enumerate(config["sample_insights"])
            ]
            
            alerts = [
                {
                    "id": f"{persona.value.lower()}_alert_{i}",
                    "severity": "medium",
                    "message": alert
                }
                for i, alert in enumerate(config["sample_alerts"])
            ]
            
            views.append({
                "persona_id": persona.value,
                "title": config["title"],
                "focus_areas": config["focus_areas"],
                "key_entities": config["key_entities"],
                "metrics": metrics,
                "insights": insights,
                "alerts": alerts
            })
        
        return views
