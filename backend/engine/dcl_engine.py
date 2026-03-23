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
from backend.engine.edge_index import EdgeIndex
from backend.semantic_mapper import SemanticMapper
from backend.eval.mapping_evaluator import MappingEvaluator
from backend.db.triple_store import TripleStore
from backend.utils.log_utils import get_logger
from backend.core.constants import utc_now

logger = get_logger(__name__)


def _display_entity(entity_id: str) -> str:
    """Format entity_id for human-readable display.

    Preserves IDs that already contain uppercase or hyphens (e.g. SysHub-NUU2).
    Title-cases plain lowercase IDs (e.g. meridian → Meridian).
    """
    if not entity_id:
        return entity_id
    if any(c.isupper() for c in entity_id) or "-" in entity_id:
        return entity_id
    return entity_id.replace("_", " ").title()


class DCLEngine:
    
    def __init__(self):
        self.narration = NarrationService()
        self.persona_view = PersonaView()
    
    def build_graph_snapshot(
        self,
        mode: Literal["Farm", "AAM"],
        run_mode: Literal["Dev", "Prod"],
        personas: List[Persona],
        run_id: str,
        source_limit: int = 1000,
        aod_run_id: Optional[str] = None
    ) -> tuple[GraphSnapshot, RunMetrics]:

        start_time = time.time()
        metrics = RunMetrics()

        self.narration.add_message(run_id, "Engine", f"Starting DCL engine in {mode} mode, {run_mode} run mode")

        # ── Farm mode: build graph directly from semantic_triples in PG ──
        if mode == "Farm":
            return self._build_from_triples_or_empty(
                run_id=run_id,
                run_mode=run_mode,
                personas=personas,
                start_time=start_time,
                metrics=metrics,
            )

        payload_kpis: Optional[Dict[str, Any]] = None

        # AAM pipe exports — used only when mode=AAM
        sources, payload_kpis = SchemaLoader.load_aam_schemas(self.narration, run_id, source_limit=source_limit, aod_run_id=aod_run_id)
        self.narration.add_message(run_id, "Engine", f"Loaded {len(sources)} sources (source_limit={source_limit})")

        # Auto-discover AOD run ID and snapshot name from PipeStore if not provided
        receipt_snapshot_name = None
        if not aod_run_id:
            from backend.api.pipe_store import get_pipe_store
            receipts = get_pipe_store().get_export_receipts()
            for r in reversed(receipts):
                if r.aod_run_id:
                    aod_run_id = r.aod_run_id
                    receipt_snapshot_name = r.snapshot_name
                    break

        ontology = get_ontology()
        self.narration.add_message(run_id, "Engine", f"Loaded {len(ontology)} ontology concepts")

        # --- Tier 0: Fetch AAM semantic edges ---
        edge_index = EdgeIndex([])
        try:
            from backend.aam.client import get_aam_client, AAMEdgeFetchError
            aam_client = get_aam_client()
            edges = aam_client.get_semantic_edges()
            if edges:
                edge_index = EdgeIndex(edges)
                metrics.aam_edge_total = len(edges)
                metrics.aam_cache_hit = aam_client._edge_cache is not None and aam_client._edge_cache is edges
                self.narration.add_message(
                    run_id, "AAM",
                    f"Loaded {len(edges)} semantic edges from AAM ({edge_index.coverage['total_edges']} indexed)"
                )
            else:
                self.narration.add_message(run_id, "AAM", "No semantic edges available from AAM")
        except ValueError:
            # AAM_URL not configured — expected in standalone DCL deployments
            self.narration.add_message(run_id, "AAM", "AAM not configured (AAM_URL not set) — Tier 0 skipped")
            metrics.aam_unavailable = True
        except AAMEdgeFetchError as e:
            logger.warning(f"AAM edge fetch failed: {e}")
            self.narration.add_message(run_id, "AAM", f"AAM unavailable: {e} — Tier 0 skipped")
            metrics.aam_unavailable = True

        semantic_mapper = SemanticMapper()
        
        try:
            all_mappings_grouped = semantic_mapper.get_all_mappings_grouped()
        except Exception as e:
            logger.warning(f"Failed to load stored mappings from DB: {e}. Will generate fresh mappings.")
            self.narration.add_message(run_id, "Engine", "DB unavailable - generating fresh mappings")
            all_mappings_grouped = {}
            metrics.db_fallback = True
        
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
            new_mappings, stats = semantic_mapper.run_mapping(
                sources_needing_mappings, mode="heuristic", clear_existing=False,
                edge_index=edge_index,
            )
            stored_mappings.extend(new_mappings)
            metrics.aam_edge_hits = stats.get('aam_edge_hits', 0)
            metrics.aam_edge_misses = stats.get('aam_edge_misses', 0)
            tier0_msg = f" (Tier 0: {metrics.aam_edge_hits} AAM edge hits)" if metrics.aam_edge_hits > 0 else ""
            self.narration.add_message(
                run_id, "Engine",
                f"Created and persisted {stats['mappings_created']} new mappings{tier0_msg}"
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
                    metrics.llm_fallback = True
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
        
        metrics.total_mappings = len(mappings)
        
        if mode == "AAM" and payload_kpis is not None:
            metrics.payload_kpis = payload_kpis
            if payload_kpis.get("fabrics", 0) == 0:
                metrics.data_status = "empty"
            elif payload_kpis.get("unpipedCount", 0) > 0:
                metrics.data_status = "partial"
            else:
                metrics.data_status = "ok"
        
        graph = self._build_graph(mode, sources, ontology, mappings, personas, run_id)
        
        processing_time = (time.time() - start_time) * 1000
        metrics.processing_ms = processing_time
        
        render_start = time.time()
        snapshot = GraphSnapshot(
            nodes=graph["nodes"],
            links=graph["links"],
            meta={
                "mode": mode,
                "runId": aod_run_id or run_id,
                "snapshotName": payload_kpis.get("snapshotName", "") if payload_kpis else (receipt_snapshot_name or ""),
                "aodRunId": aod_run_id or "",
                "generatedAt": utc_now(),
                "stats": {
                    "sources": len(sources),
                    "ontology_concepts": len(ontology),
                    "mappings": len(mappings),
                    "personas": [p.value for p in personas]
                },
                "sourceCanonicalIds": [s.id for s in sources],
                "sourceNames": [s.name for s in sources],
                "sourceFabricPlanes": sorted(set(
                    f"{s.fabric_plane}:{s.vendor}"
                    for s in sources
                    if getattr(s, 'fabric_plane', None)
                )),
            }
        )
        
        render_time = (time.time() - render_start) * 1000
        metrics.render_ms = render_time
        
        self.narration.add_message(
            run_id, "Engine",
            f"Graph built: {len(graph['nodes'])} nodes, {len(graph['links'])} links in {processing_time:.0f}ms"
        )
        
        return snapshot, metrics
    
    def _build_from_triples_or_empty(
        self,
        run_id: str,
        run_mode: str,
        personas: List[Persona],
        start_time: float,
        metrics: RunMetrics,
    ) -> tuple[GraphSnapshot, RunMetrics]:
        """Build the Sankey graph from semantic_triples in PG.

        If triples exist, builds the full 4-layer graph.
        If no triples exist, returns a diagnostic empty-state snapshot
        (no silent fallback to AAM or any other data path).
        """
        triple_store = TripleStore()

        # Check for active triples
        try:
            triple_count = triple_store.count_active()
        except Exception as e:
            logger.error(f"Triple count check failed: {e}", exc_info=True)
            raise RuntimeError(
                f"DCL could not query semantic_triples table: {e}. "
                f"Check DATABASE_URL and Supabase connectivity."
            ) from e

        if triple_count == 0:
            self.narration.add_message(
                run_id, "Engine",
                "No semantic triples in PG. "
                "Run Farm enterprise generator and ingest triples via POST /api/dcl/ingest-triples."
            )
            processing_time = (time.time() - start_time) * 1000
            metrics.processing_ms = processing_time
            metrics.total_mappings = 0

            # Return empty-state graph with diagnostic metadata — no nodes, no links,
            # no spinner, no fallback. The frontend shows "No data ingested" message.
            snapshot = GraphSnapshot(
                nodes=[],
                links=[],
                meta={
                    "mode": "Farm",
                    "runId": run_id,
                    "snapshotName": "",
                    "aodRunId": "",
                    "generatedAt": utc_now(),
                    "status": "no_data",
                    "diagnostics": {
                        "triple_count": 0,
                        "message": (
                            "No semantic triples in PG. "
                            "Run Farm enterprise generator and ingest triples "
                            "via POST /api/dcl/ingest-triples."
                        ),
                    },
                    "stats": {
                        "sources": 0,
                        "ontology_concepts": 0,
                        "mappings": 0,
                        "triple_count": 0,
                        "personas": [p.value for p in personas],
                        "entities": [],
                    },
                    "sourceCanonicalIds": [],
                    "sourceNames": [],
                    "sourceFabricPlanes": [],
                }
            )
            return snapshot, metrics

        self.narration.add_message(
            run_id, "Engine",
            f"Found {triple_count:,} active semantic triples in PG — building Sankey from triple data"
        )

        try:
            sankey_rows = triple_store.get_sankey_aggregation()
        except Exception as e:
            logger.error(f"Sankey aggregation query failed: {e}", exc_info=True)
            raise RuntimeError(
                f"DCL could not query semantic_triples for Sankey aggregation: {e}. "
                f"Check DATABASE_URL and Supabase connectivity."
            ) from e

        if not sankey_rows:
            # count_active returned >0 but aggregation is empty — data inconsistency
            raise RuntimeError(
                f"DCL found {triple_count} active triples but Sankey aggregation returned 0 rows. "
                f"Possible data inconsistency in semantic_triples table."
            )

        graph = self._build_graph_from_triples(sankey_rows, personas, run_id)

        processing_time = (time.time() - start_time) * 1000
        metrics.processing_ms = processing_time
        metrics.total_mappings = len(sankey_rows)

        # Collect distinct sources and domains for meta
        source_names = sorted({r["source_system"] for r in sankey_rows})
        domains = sorted({r["domain"] for r in sankey_rows})
        entities = sorted({r["entity_id"] for r in sankey_rows if r.get("entity_id")})

        # Resolve the Farm source run_id from semantic_triples for provenance
        source_run_ids = triple_store.get_source_run_ids()
        if len(source_run_ids) == 1:
            source_run_id = str(source_run_ids[0]["run_id"])
        elif len(source_run_ids) > 1:
            source_run_id = str(source_run_ids[0]["run_id"])  # most recent
        else:
            source_run_id = ""

        # Build provenance scoped to the current (latest) source run
        if source_run_id:
            run_entities = triple_store.get_run_entities(source_run_id)
            snapshot_label = " · ".join(_display_entity(e) for e in run_entities) if run_entities else ""
        else:
            run_entities = entities
            snapshot_label = " · ".join(_display_entity(e) for e in entities) if entities else ""

        snapshot = GraphSnapshot(
            nodes=graph["nodes"],
            links=graph["links"],
            meta={
                "mode": "Farm",
                "runId": run_id,
                "sourceRunId": source_run_id,
                "snapshotName": snapshot_label,
                "aodRunId": "",
                "generatedAt": utc_now(),
                "stats": {
                    "sources": len(source_names),
                    "ontology_concepts": len(domains),
                    "mappings": len(sankey_rows),
                    "triple_count": triple_count,
                    "personas": [p.value for p in personas],
                    "entities": run_entities,
                },
                "sourceCanonicalIds": source_names,
                "sourceNames": source_names,
                "sourceFabricPlanes": [],
            }
        )

        self.narration.add_message(
            run_id, "Engine",
            f"Graph built from triples: {len(graph['nodes'])} nodes, "
            f"{len(graph['links'])} links, {len(source_names)} sources, "
            f"{len(domains)} domains, {len(entities)} entities "
            f"in {processing_time:.0f}ms"
        )

        return snapshot, metrics

    def _build_graph_from_triples(
        self,
        sankey_rows: List[Dict[str, Any]],
        personas: List[Persona],
        run_id: str,
    ) -> Dict[str, List]:
        """Build the 4-layer Sankey graph from pre-aggregated triple data.

        Each sankey_row has: source_system, domain, entity_id, triple_count.
        """
        nodes: List[GraphNode] = []
        links: List[GraphLink] = []

        # ── L0: Pipeline root ──
        pipe_id = "pipe_farm"
        source_systems: Dict[str, int] = {}  # source → total triple count
        domains: Dict[str, int] = {}          # domain → total triple count
        for row in sankey_rows:
            src = row["source_system"]
            dom = row["domain"]
            cnt = row["triple_count"]
            source_systems[src] = source_systems.get(src, 0) + cnt
            domains[dom] = domains.get(dom, 0) + cnt

        nodes.append(GraphNode(
            id=pipe_id,
            label="Farm Pipeline",
            level="L0",
            kind="pipe",
            group="Farm",
            status="ok",
            metrics={"source_count": len(source_systems)}
        ))

        # ── L1: Source system nodes ──
        for src, total_count in sorted(source_systems.items()):
            source_id = f"source_{src}"
            nodes.append(GraphNode(
                id=source_id,
                label=src,
                level="L1",
                kind="source",
                group="Farm",
                status="ok",
                metrics={
                    "triple_count": total_count,
                    "tables": 0,
                    "fields": 0,
                }
            ))
            links.append(GraphLink(
                id=f"link_pipe_{src}",
                source=pipe_id,
                target=source_id,
                value=float(total_count),
                flow_type="schema",
                info_summary=f"{total_count:,} triples from {src}",
            ))

        # ── L1→L2: Source→Domain mapping links ──
        # Aggregate by (source_system, domain) across entities
        source_domain_agg: Dict[tuple, int] = {}
        for row in sankey_rows:
            key = (row["source_system"], row["domain"])
            source_domain_agg[key] = source_domain_agg.get(key, 0) + row["triple_count"]

        for (src, dom), count in source_domain_agg.items():
            source_id = f"source_{src}"
            concept_id = f"ontology_{dom}"
            link_id = f"link_{src}_{dom}_{uuid.uuid4().hex[:8]}"
            links.append(GraphLink(
                id=link_id,
                source=source_id,
                target=concept_id,
                value=float(count),
                confidence=1.0,
                flow_type="mapping",
                info_summary=f"{src} → {dom} ({count:,} triples)",
            ))

        # ── L2: Concept domain nodes ──
        for dom, total_count in sorted(domains.items()):
            concept_id = f"ontology_{dom}"
            # Collect source hierarchy for this domain
            source_hierarchy: Dict[str, Dict] = {}
            for row in sankey_rows:
                if row["domain"] == dom:
                    src = row["source_system"]
                    if src not in source_hierarchy:
                        source_hierarchy[src] = {}
                    eid = row.get("entity_id", "unknown")
                    source_hierarchy[src][eid] = row["triple_count"]

            nodes.append(GraphNode(
                id=concept_id,
                label=dom.replace("_", " ").title(),
                level="L2",
                kind="ontology",
                group="Ontology",
                status="ok",
                metrics={
                    "description": f"Semantic domain: {dom}",
                    "input_count": total_count,
                    "explanation": f"Derived from {total_count:,} triples",
                    "contributing_fields": [],
                    "source_hierarchy": source_hierarchy,
                }
            ))

        # ── L3: Persona nodes + L2→L3 consumption links ──
        persona_concepts = self.persona_view.get_relevant_concepts(personas)

        # Count how many personas consume each domain so we can split
        # the flow proportionally and preserve the Sankey invariant:
        # total flow into each L2 node == total flow out.
        domain_consumer_count: Dict[str, int] = {}
        for persona in personas:
            for concept_id in persona_concepts.get(persona.value, []):
                if concept_id in domains:
                    domain_consumer_count[concept_id] = domain_consumer_count.get(concept_id, 0) + 1

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
                if concept_id in domains:
                    split_value = float(domains[concept_id]) / domain_consumer_count[concept_id]
                    link_id = f"link_{concept_id}_{persona.value}_{uuid.uuid4().hex[:8]}"
                    links.append(GraphLink(
                        id=link_id,
                        source=f"ontology_{concept_id}",
                        target=bll_id,
                        value=split_value,
                        flow_type="consumption",
                        info_summary=f"{concept_id} consumed by {persona.value} BLL",
                    ))

        return {"nodes": nodes, "links": links}

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

        # Decide aggregation strategy: fabric-level for 30+ pipes,
        # individual nodes when under threshold for readability.
        # Both AAM and Farm sources carry fabric_plane from pipe_store
        # (populated by AAM's export-pipes).
        use_fabric_aggregation = (
            mode in ("AAM", "Farm")
            and len(sources) >= 30
            and any(s.fabric_plane for s in sources)
        )

        # Build source_id → fabric_plane lookup (needed for routing mappings)
        source_to_fabric: Dict[str, str] = {}
        for source in sources:
            plane = (source.fabric_plane or "unmapped").upper()
            source_to_fabric[source.id] = plane

        if use_fabric_aggregation:
            # ── Fabric-level aggregation: group sources by fabric_plane ──
            fabric_groups: Dict[str, List[SourceSystem]] = {}
            for source in sources:
                plane = source_to_fabric[source.id]
                if plane not in fabric_groups:
                    fabric_groups[plane] = []
                fabric_groups[plane].append(source)

            for plane, plane_sources in sorted(fabric_groups.items()):
                fabric_node_id = f"fabric_{plane.lower()}"
                pipe_count = len(plane_sources)
                total_fields = sum(
                    sum(len(t.fields) for t in s.tables) for s in plane_sources
                )
                governed = sum(1 for s in plane_sources if "governed" in s.tags)
                avg_trust = (
                    sum(s.trust_score for s in plane_sources) // pipe_count
                    if pipe_count else 0
                )

                nodes.append(GraphNode(
                    id=fabric_node_id,
                    label=f"{plane} ({pipe_count})",
                    level="L1",
                    kind="fabric",
                    group=plane,
                    status="ok",
                    metrics={
                        "pipe_count": pipe_count,
                        "fields": total_fields,
                        "governed": governed,
                        "ungoverned": pipe_count - governed,
                        "trust_score": avg_trust,
                        "sources": [s.name for s in plane_sources],
                    }
                ))

                links.append(GraphLink(
                    id=f"link_pipe_{plane.lower()}",
                    source=pipe_id,
                    target=fabric_node_id,
                    value=float(pipe_count),
                    flow_type="schema",
                    info_summary=f"{pipe_count} pipes, {total_fields} fields"
                ))

            # Aggregate mapping links: one link per (fabric, concept) pair
            fabric_concept_agg: Dict[tuple, list] = {}
            for mapping in mappings:
                if mapping.ontology_concept not in relevant_concept_ids:
                    continue
                plane = source_to_fabric.get(mapping.source_system, "UNMAPPED")
                key = (plane, mapping.ontology_concept)
                if key not in fabric_concept_agg:
                    fabric_concept_agg[key] = []
                fabric_concept_agg[key].append(mapping)

            for (plane, concept_key), agg_mappings in fabric_concept_agg.items():
                fabric_node_id = f"fabric_{plane.lower()}"
                concept_id = f"ontology_{concept_key}"
                count = len(agg_mappings)
                avg_conf = sum(m.confidence for m in agg_mappings) / count

                links.append(GraphLink(
                    id=f"link_{plane.lower()}_{concept_key}_{uuid.uuid4().hex[:8]}",
                    source=fabric_node_id,
                    target=concept_id,
                    value=float(count),
                    confidence=avg_conf,
                    flow_type="mapping",
                    info_summary=f"{count} mappings (avg conf {avg_conf:.2f})"
                ))

                if concept_key in ontology_mapping_count:
                    ontology_mapping_count[concept_key] += count
                if concept_key in concept_field_mappings:
                    for m in agg_mappings:
                        concept_field_mappings[concept_key].append({
                            "field": m.source_field,
                            "table": m.source_table,
                            "source": m.source_system,
                            "confidence": m.confidence
                        })

        else:
            # ── Individual source nodes: <30 pipes ──
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

            # Create individual source→concept mapping links
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
