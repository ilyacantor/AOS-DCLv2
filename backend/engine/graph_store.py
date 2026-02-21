"""
Singleton store for the semantic graph and query resolver.

The graph is built at engine startup and rebuilt when data changes.
Route handlers and other callers access the graph via get_semantic_graph()
and get_query_resolver().
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from backend.utils.log_utils import get_logger

if TYPE_CHECKING:
    from backend.engine.query_resolver import QueryResolver
    from backend.engine.semantic_graph import SemanticGraph

logger = get_logger(__name__)

_graph: Optional[SemanticGraph] = None
_resolver: Optional[QueryResolver] = None


def get_semantic_graph() -> Optional[SemanticGraph]:
    """Return the current semantic graph (None if not yet built)."""
    return _graph


def get_query_resolver() -> Optional[QueryResolver]:
    """Return the current query resolver (None if not yet built)."""
    return _resolver


def set_semantic_graph(graph: SemanticGraph) -> None:
    """Replace the singleton graph and rebuild the resolver."""
    global _graph, _resolver
    from backend.engine.query_resolver import QueryResolver

    _graph = graph
    _resolver = QueryResolver(graph)
    logger.info(f"[GraphStore] Graph set: {graph.stats}")


def rebuild_graph() -> None:
    """Full graph rebuild from all data sources.

    Called at startup and when underlying data changes (new classification,
    AAM edge refresh, contour map approval).
    """
    from backend.engine.semantic_graph import SemanticGraph

    graph = SemanticGraph()

    # 1. Ontology pairings (always available)
    graph.load_from_ontology()

    # 2. Contour map (sample in dev, approved map in prod)
    graph.load_from_contour_map()

    # 3. Normalizer mappings (from DB, with in-memory fallback)
    all_mappings = []
    try:
        from backend.semantic_mapper import SemanticMapper
        mapper = SemanticMapper()
        all_grouped = mapper.get_all_mappings_grouped()
        all_mappings = [m for group in all_grouped.values() for m in group]
        if all_mappings:
            logger.info(f"[GraphStore] Loaded {len(all_mappings)} normalizer mappings from DB")
    except Exception as e:
        logger.warning(f"[GraphStore] Could not load normalizer mappings from DB: {e}")

    if not all_mappings:
        try:
            from backend.semantic_mapper import SemanticMapper
            from backend.engine.schema_loader import SchemaLoader
            mapper = SemanticMapper()
            sources = SchemaLoader.load_demo_schemas()
            if sources:
                all_mappings, stats = mapper.run_mapping(sources, mode="heuristic")
                logger.info(
                    f"[GraphStore] Generated {len(all_mappings)} in-memory mappings "
                    f"from {len(sources)} demo sources (DB unavailable)"
                )
        except Exception as e:
            logger.warning(f"[GraphStore] In-memory mapping fallback failed: {e}")

    if all_mappings:
        graph.load_from_normalizer(all_mappings)
        logger.info(f"[GraphStore] Graph loaded {len(all_mappings)} normalizer mappings")

    # 4. AAM semantic edges
    try:
        from backend.aam.client import get_aam_client
        client = get_aam_client()
        edges = client.get_semantic_edges()
        if edges:
            graph.load_from_aam(edges)
            logger.info(f"[GraphStore] Loaded {len(edges)} AAM edges")
    except ValueError:
        logger.info("[GraphStore] AAM not configured — skipping AAM edges")
    except Exception as e:
        logger.warning(f"[GraphStore] Could not load AAM edges: {e}")

    set_semantic_graph(graph)
