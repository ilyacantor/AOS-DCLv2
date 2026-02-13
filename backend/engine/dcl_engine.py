import time
import uuid
import os
from typing import List, Literal, Dict, Any, Optional
from backend.domain import (
    Persona, SourceSystem, GraphSnapshot, GraphNode, GraphLink,
    RunMetrics, Mapping, MappingDetail, OntologyConcept
)
from backend.engine.schema_loader import SchemaLoader
from backend.engine.ontology import get_ontology
from backend.engine.mapping_service import MappingService
from backend.engine.rag_service import RAGService
from backend.engine.narration_service import NarrationService
from backend.engine.persona_view import PersonaView
from backend.semantic_mapper import SemanticMapper
from backend.eval.mapping_evaluator import MappingEvaluator
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)


class DCLEngine:
    
    def __init__(self):
        self.narration = NarrationService()
        self.persona_view = PersonaView()
    
    def build_graph_snapshot(
        self,
        mode: Literal["Demo", "Farm", "AAM"],
        run_mode: Literal["Dev", "Prod"],
        personas: List[Persona],
        run_id: str,
        source_limit: int = 1000,
        aod_run_id: Optional[str] = None
    ) -> tuple[GraphSnapshot, RunMetrics]:
        
        start_time = time.time()
        metrics = RunMetrics()
        
        self.narration.add_message(run_id, "Engine", f"Starting DCL engine in {mode} mode, {run_mode} run mode")
        
        if mode == "Demo":
            sources = SchemaLoader.load_demo_schemas(self.narration, run_id)
            self.narration.add_message(run_id, "Engine", f"Loaded {len(sources)} Demo sources")
        elif mode == "AAM":
            sources = SchemaLoader.load_aam_schemas(self.narration, run_id, source_limit=source_limit, aod_run_id=aod_run_id)
            self.narration.add_message(run_id, "Engine", f"Loaded {len(sources)} AAM sources (source_limit={source_limit})")
        else:
            sources = SchemaLoader.load_farm_schemas(self.narration, run_id, source_limit=source_limit)
            self.narration.add_message(run_id, "Engine", f"Loaded {len(sources)} Farm sources (source_limit={source_limit})")
        
        if mode != "AAM":
            stream_sources = SchemaLoader.load_stream_sources(self.narration, run_id)
            if stream_sources:
                sources.extend(stream_sources)
                self.narration.add_message(run_id, "Engine", f"Loaded {len(stream_sources)} real-time stream sources")
        
        ontology = get_ontology()
        self.narration.add_message(run_id, "Engine", f"Loaded {len(ontology)} ontology concepts")
        
        semantic_mapper = SemanticMapper()
        
        try:
            all_mappings_grouped = semantic_mapper.get_all_mappings_grouped()
        except Exception as e:
            logger.warning(f"Failed to load stored mappings from DB: {e}. Will generate fresh mappings.")
            self.narration.add_message(run_id, "Engine", "DB unavailable - generating fresh mappings")
            all_mappings_grouped = {}
        
        stored_mappings = []
        sources_with_mappings = set()
        sources_needing_mappings = []
        
        for source in sources:
            source_stored = all_mappings_grouped.get(source.id, [])
            if source_stored:
                stored_mappings.extend(source_stored)
                sources_with_mappings.add(source.id)
            else:
                sources_needing_mappings.append(source)
        
        if stored_mappings:
            self.narration.add_message(run_id, "Engine", f"Loaded {len(stored_mappings)} stored mappings for {len(sources_with_mappings)} sources")
        
        if sources_needing_mappings:
            self.narration.add_message(
                run_id, "Engine", 
                f"Running semantic mapper for {len(sources_needing_mappings)} sources without stored mappings: {[s.id for s in sources_needing_mappings]}"
            )
            new_mappings, stats = semantic_mapper.run_mapping(sources_needing_mappings, mode="heuristic", clear_existing=False)
            stored_mappings.extend(new_mappings)
            self.narration.add_message(
                run_id, "Engine",
                f"Created and persisted {stats['mappings_created']} new mappings using heuristics"
            )
        
        mappings = stored_mappings
        
        evaluator = MappingEvaluator()
        mapping_dicts = [
            {'source_id': m.source_system, 'table_name': m.source_table, 
             'field_name': m.source_field, 'concept_id': m.ontology_concept, 
             'confidence': m.confidence}
            for m in mappings
        ]
        issues = evaluator.evaluate_mappings(mapping_dicts)
        eval_summary = evaluator.get_summary()
        
        if eval_summary['total_issues'] > 0:
            self.narration.add_message(
                run_id, "Eval",
                f"Mapping evaluation: {eval_summary['total_issues']} issues found ({eval_summary['high_severity']} high, {eval_summary['medium_severity']} medium)"
            )
        else:
            self.narration.add_message(run_id, "Eval", "Mapping evaluation: All mappings passed validation")
        
        if run_mode == "Prod":
            llm_available = bool(os.getenv('OPENAI_API_KEY') or os.getenv('AI_INTEGRATIONS_OPENAI_API_KEY'))
            
            if llm_available:
                self.narration.add_message(run_id, "LLM", "Prod mode: Running LLM validation on low-confidence mappings...")
                
                try:
                    from backend.llm.mapping_validator import validate_mappings_prod_mode
                    
                    ontology_dicts = [
                        {'id': c.id, 'name': c.name, 'description': c.description}
                        for c in ontology
                    ]
                    
                    def narration_callback(msg):
                        self.narration.add_message(run_id, "LLM", msg)
                    
                    corrected_dicts, llm_stats = validate_mappings_prod_mode(
                        mapping_dicts, ontology_dicts, narration_callback
                    )
                    
                    metrics.llm_calls = llm_stats.get('total_validated', 0)
                    
                    if llm_stats.get('corrections_made', 0) > 0:
                        corrected_mappings = []
                        for m_dict in corrected_dicts:
                            corrected_mappings.append(Mapping(
                                id=f"{m_dict['source_id']}_{m_dict['table_name']}_{m_dict['field_name']}_{m_dict.get('concept_id', m_dict.get('ontology_concept'))}",
                                source_field=m_dict['field_name'],
                                source_table=m_dict['table_name'],
                                source_system=m_dict['source_id'],
                                ontology_concept=m_dict.get('concept_id', m_dict.get('ontology_concept')),
                                confidence=m_dict['confidence'],
                                method=m_dict.get('method', 'llm_validated'),
                                status="ok"
                            ))
                        mappings = corrected_mappings
                        
                except Exception as e:
                    logger.error(f"LLM validation failed for run {run_id}: {e}", exc_info=True)
                    self.narration.add_message(run_id, "LLM", f"LLM validation error: {str(e)}")
            else:
                self.narration.add_message(
                    run_id, "LLM", 
                    "Prod mode: LLM validation skipped - OPENAI_API_KEY not configured"
                )
        
        # Store mapping lessons in RAG (both Dev and Prod modes)
        rag_service = RAGService(run_mode, run_id, self.narration)
        lessons_stored = rag_service.store_mapping_lessons(mappings)
        metrics.rag_writes = lessons_stored
        
        if run_mode == "Prod" and lessons_stored > 0:
            # OpenAI embeddings count as additional LLM calls
            metrics.llm_calls += lessons_stored
            metrics.rag_reads = 3  # Attempted RAG lookups during mapping
        elif lessons_stored > 0:
            # Dev mode uses mock embeddings (no LLM calls)
            metrics.rag_reads = 0
        
        metrics.total_mappings = len(mappings)
        
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
        
        # Initialize tracking for ontology and personas
        relevant_concept_ids = self.persona_view.get_all_relevant_concept_ids(personas)
        ontology_mapping_count = {}
        concept_field_mappings = {}
        for concept in ontology:
            if concept.id in relevant_concept_ids:
                concept_id = f"ontology_{concept.id}"
                concept_field_mappings[concept.id] = []
                ontology_mapping_count[concept.id] = 0
        
        # Determine if we should aggregate by fabric or show individual sources
        use_fabric_aggregation = len(sources) > 20
        
        if use_fabric_aggregation and mode == "AAM":
            self.narration.add_message(
                run_id, "Graph",
                f"⚡ {len(sources)} sources detected - aggregating by fabric plane for clean visualization"
            )
            # Aggregate sources by fabric_type only (vendors are grouped within)
            fabric_groups = {}
            for source in sources:
                # Extract fabric type from tags
                fabric_type = None
                for tag in source.tags:
                    if tag in ["ipaas", "warehouse", "gateway", "eventbus"]:
                        fabric_type = tag
                        break
                
                if not fabric_type:
                    fabric_type = "ipaas"  # Default
                
                if fabric_type not in fabric_groups:
                    fabric_groups[fabric_type] = []
                fabric_groups[fabric_type].append(source)
            
            # Create fabric plane nodes - one per fabric type
            fabric_labels = {
                "ipaas": "iPaaS",
                "warehouse": "Warehouse",
                "gateway": "API Gateway",
                "eventbus": "Event Bus"
            }
            
            for fabric_type, group_sources in fabric_groups.items():
                fabric_id = f"fabric_{fabric_type}"
                source_count = len(group_sources)
                total_fields = sum(sum(len(t.fields) for t in s.tables) for s in group_sources)
                
                # Collect unique vendors within this fabric type
                vendors = sorted(set(s.vendor for s in group_sources if hasattr(s, 'vendor') and s.vendor))
                vendor_summary = ", ".join(vendors) if vendors else "Multiple"
                
                nodes.append(GraphNode(
                    id=fabric_id,
                    label=f"{fabric_labels[fabric_type]} ({source_count} sources)",
                    level="L1",
                    kind="fabric",
                    group=fabric_type,
                    status="ok",
                    metrics={
                        "source_count": source_count,
                        "total_fields": total_fields,
                        "fabric_type": fabric_type,
                        "vendors": vendors,
                        "sources": [s.name for s in group_sources[:10]]  # Top 10 for hover
                    }
                ))
                
                links.append(GraphLink(
                    id=f"link_pipe_{fabric_type}",
                    source=pipe_id,
                    target=fabric_id,
                    value=float(source_count),
                    flow_type="schema",
                    info_summary=f"{vendor_summary}: {source_count} sources, {total_fields} fields"
                ))
            
            # Now create mappings from fabric planes to concepts
            # Map each source to its fabric plane (by type only)
            source_to_fabric = {}
            for source in sources:
                fabric_type = next((tag for tag in source.tags if tag in ["ipaas", "warehouse", "gateway", "eventbus"]), "ipaas")
                source_to_fabric[source.id] = f"fabric_{fabric_type}"
            
            # Aggregate mappings by fabric → concept
            fabric_concept_mappings = {}
            for mapping in mappings:
                fabric_id = source_to_fabric.get(mapping.source_system)
                if not fabric_id:
                    continue
                
                concept = mapping.ontology_concept
                key = (fabric_id, concept)
                if key not in fabric_concept_mappings:
                    fabric_concept_mappings[key] = []
                fabric_concept_mappings[key].append(mapping)
            
            # Create aggregated links from fabric → concept            
            for (fabric_id, concept_id), mappings_list in fabric_concept_mappings.items():
                if concept_id in relevant_concept_ids:
                    concept_node_id = f"ontology_{concept_id}"
                    avg_confidence = sum(m.confidence for m in mappings_list) / len(mappings_list)
                    field_count = len(mappings_list)
                    
                    link_id = f"link_{fabric_id}_{concept_id}"
                    links.append(GraphLink(
                        id=link_id,
                        source=fabric_id,
                        target=concept_node_id,
                        value=avg_confidence,
                        confidence=avg_confidence,
                        flow_type="mapping",
                        info_summary=f"{field_count} fields → {concept_id} (avg {avg_confidence:.2f})",
                        mapping_detail=None  # Aggregated, no single detail
                    ))
                    
                    # Update counts for ontology node creation
                    if concept_id in ontology_mapping_count:
                        ontology_mapping_count[concept_id] += field_count
                    
                    # Track field mappings for ontology metrics
                    if concept_id in concept_field_mappings:
                        for m in mappings_list:
                            concept_field_mappings[concept_id].append({
                                "field": m.source_field,
                                "table": m.source_table,
                                "source": m.source_system,
                                "confidence": m.confidence
                            })
        else:
            # Normal path: show individual sources
            source_mapping_count = {}
            for source in sources:
                source_id = f"source_{source.id}"
                table_count = len(source.tables)
                field_count = sum(len(t.fields) for t in source.tables)
                
                discovery_status = getattr(source, 'discovery_status', None)
                discovery_value = discovery_status.value if discovery_status else "canonical"
                status = "ok" if discovery_value == "canonical" else "pending"
                
                resolution_type = getattr(source, 'resolution_type', None)
                resolution_value = resolution_type.value if resolution_type else "exact"
                
                nodes.append(GraphNode(
                    id=source_id,
                    label=source.name,
                    level="L1",
                    kind="source",
                    group=source.type,
                    status=status,
                    metrics={
                        "tables": table_count,
                        "fields": field_count,
                        "type": source.type,
                        "canonical_id": getattr(source, 'canonical_id', source.id),
                        "raw_id": getattr(source, 'raw_id', source.id),
                        "discovery_status": discovery_value,
                        "resolution_type": resolution_value,
                        "trust_score": getattr(source, 'trust_score', 50),
                        "data_quality_score": getattr(source, 'data_quality_score', 50),
                        "vendor": getattr(source, 'vendor', None),
                        "category": getattr(source, 'category', None),
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
            
            # Normal path: create individual source→concept mapping links
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
                            info_summary=f"{mapping.source_field} → {mapping.ontology_concept} ({mapping.method}, {mapping.confidence:.2f})",
                            mapping_detail=MappingDetail(
                                source_field=mapping.source_field,
                                source_table=mapping.source_table,
                                target_concept=mapping.ontology_concept,
                                method=mapping.method,
                                confidence=mapping.confidence
                            )
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
