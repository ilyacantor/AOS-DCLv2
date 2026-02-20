"""
Resolves NLQ queries against the semantic graph.

Takes a QueryIntent (concepts, dimensions, filters) and runs the
8-step resolution flow: concept location → dimension validity →
dimension source → join path → filter resolution → confidence →
response assembly.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from backend.engine.graph_types import (
    FieldLocation,
    JoinPath,
    QueryFilter,
    QueryIntent,
    QueryResolution,
    ResolvedFilter,
    SystemAuthority,
)
from backend.utils.log_utils import get_logger

if TYPE_CHECKING:
    from backend.engine.semantic_graph import SemanticGraph

logger = get_logger(__name__)


class QueryResolver:
    """Resolves NLQ queries against the semantic graph."""

    def __init__(self, graph: SemanticGraph) -> None:
        self.graph = graph

    def resolve(self, intent: QueryIntent) -> QueryResolution:
        """Run the 8-step resolution flow.

        Steps 1 (intent parsing) is handled by NLQ before calling DCL.
        """
        # Step 2: Concept location
        sources = self._locate_concepts(intent.concepts)
        if not sources:
            return QueryResolution(
                can_answer=False,
                reason=f"No sources found for concepts: {intent.concepts}",
            )

        # Step 3: Dimension validity
        invalid = self._check_dimensions(intent.concepts, intent.dimensions)
        if invalid:
            pairs_str = ", ".join(f"{c}×{d}" for c, d in invalid)
            return QueryResolution(
                can_answer=False,
                reason=f"Invalid concept-dimension pairings: {pairs_str}",
                concept_sources=sources,
            )

        # Step 4: Dimension source resolution
        dim_authorities = self._resolve_dimension_sources(intent.dimensions)

        # Step 5: Join path discovery
        join_paths, join_warnings = self._find_join_paths(sources, dim_authorities)

        # Step 6: Filter resolution
        resolved_filters = self._resolve_filters(intent.filters)

        # Steps 7-8 added in subsequent commits
        return QueryResolution(
            can_answer=True,
            concept_sources=sources,
            dimension_authorities=dim_authorities,
            join_paths=join_paths,
            resolved_filters=resolved_filters,
            warnings=join_warnings,
        )

    # ------------------------------------------------------------------
    # Step 2: Concept location
    # ------------------------------------------------------------------

    def _locate_concepts(self, concepts: list[str]) -> list[FieldLocation]:
        """Find the best source for each concept."""
        all_sources: list[FieldLocation] = []
        for concept in concepts:
            sources = self.graph.find_concept_sources(concept)
            if sources:
                all_sources.extend(sources)
            else:
                logger.warning(f"[Resolver] No sources for concept: {concept}")
        return all_sources

    # ------------------------------------------------------------------
    # Step 3: Dimension validity
    # ------------------------------------------------------------------

    def _check_dimensions(
        self, concepts: list[str], dimensions: list[str],
    ) -> list[tuple[str, str]]:
        """Return list of invalid (concept, dimension) pairs."""
        invalid: list[tuple[str, str]] = []
        for concept in concepts:
            for dim in dimensions:
                if not self.graph.check_dimension_validity(concept, dim):
                    invalid.append((concept, dim))
        return invalid

    # ------------------------------------------------------------------
    # Step 4: Dimension source resolution
    # ------------------------------------------------------------------

    def _resolve_dimension_sources(
        self, dimensions: list[str],
    ) -> dict[str, SystemAuthority]:
        """Find the authoritative system for each dimension."""
        authorities: dict[str, SystemAuthority] = {}
        for dim in dimensions:
            auth = self.graph.find_dimension_authority(dim)
            if auth:
                authorities[dim] = auth
            else:
                # Fallback: mark as inferred with low confidence
                authorities[dim] = SystemAuthority(
                    system="unknown",
                    dimension=dim,
                    confidence=0.0,
                    source="default",
                )
        return authorities

    # ------------------------------------------------------------------
    # Step 5: Join path discovery
    # ------------------------------------------------------------------

    def _find_join_paths(
        self,
        concept_sources: list[FieldLocation],
        dim_authorities: dict[str, SystemAuthority],
    ) -> tuple[list[JoinPath], list[str]]:
        """Find paths connecting concept systems to dimension systems.

        For each dimension whose authoritative system differs from the
        concept's primary system, find a cross-system join path via
        MAPS_TO edges.  Returns (paths, warnings).
        """
        if not concept_sources or not dim_authorities:
            return [], []

        # Primary concept system = highest confidence source
        primary_system = concept_sources[0].system

        paths: list[JoinPath] = []
        warnings: list[str] = []

        for dim, auth in dim_authorities.items():
            if auth.system == "unknown":
                warnings.append(f"No authoritative system for dimension '{dim}'")
                continue

            if auth.system == primary_system:
                # Same system — direct join, no cross-system path needed
                paths.append(JoinPath(
                    hops=[], total_confidence=1.0,
                    description=f"{dim}: same system ({primary_system})",
                ))
                continue

            # Cross-system — find MAPS_TO path
            path = self.graph.find_join_path(primary_system, auth.system)
            if path is not None:
                paths.append(path)
                if path.total_confidence < 0.9:
                    warnings.append(
                        f"{dim} join ({primary_system}→{auth.system}) "
                        f"confidence {path.total_confidence:.2f}"
                    )
            else:
                warnings.append(
                    f"No data path from {primary_system} to {auth.system} "
                    f"for dimension '{dim}'"
                )

        return paths, warnings

    # ------------------------------------------------------------------
    # Step 6: Filter resolution
    # ------------------------------------------------------------------

    def _resolve_filters(
        self, filters: list[QueryFilter],
    ) -> list[ResolvedFilter]:
        """Resolve hierarchy and management overlay for each filter."""
        resolved: list[ResolvedFilter] = []
        for f in filters:
            values = f.value if isinstance(f.value, list) else [f.value]
            for val in values:
                rf = self.graph.resolve_dimension_filter(f.dimension, val)
                resolved.append(rf)
        return resolved
