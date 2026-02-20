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
    ConfidenceBreakdown,
    DataQueryHint,
    FieldLocation,
    JoinPath,
    QueryFilter,
    QueryIntent,
    QueryResolution,
    ResolvedFilter,
    SGraphEdge,
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

        # Step 7: Confidence scoring
        confidence = self._score_path(sources, dim_authorities, join_paths)

        # Step 8: Response assembly
        return self._assemble_response(
            sources, dim_authorities, join_paths,
            resolved_filters, confidence, join_warnings,
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

    # ------------------------------------------------------------------
    # Step 7: Confidence scoring
    # ------------------------------------------------------------------

    def _score_path(
        self,
        sources: list[FieldLocation],
        dim_authorities: dict[str, SystemAuthority],
        join_paths: list[JoinPath],
    ) -> ConfidenceBreakdown:
        """Product of edge confidences along the traversal path."""
        per_hop: dict[str, float] = {}
        overall = 1.0
        weakest = ""
        weakest_conf = 1.0

        # Concept source confidence (best source)
        if sources:
            best = sources[0]
            key = f"{best.concept}_source"
            per_hop[key] = best.confidence
            overall *= best.confidence
            if best.confidence < weakest_conf:
                weakest_conf = best.confidence
                weakest = key

        # Dimension authority confidence
        for dim, auth in dim_authorities.items():
            key = f"{dim}_sor"
            per_hop[key] = auth.confidence
            overall *= auth.confidence
            if auth.confidence < weakest_conf:
                weakest_conf = auth.confidence
                weakest = key

        # Join path confidence
        for jp in join_paths:
            if jp.hops:  # Skip same-system (no hops)
                key = f"join_{jp.description}"
                per_hop[key] = jp.total_confidence
                overall *= jp.total_confidence
                if jp.total_confidence < weakest_conf:
                    weakest_conf = jp.total_confidence
                    weakest = key

        return ConfidenceBreakdown(
            overall=round(overall, 4),
            per_hop=per_hop,
            weakest_link=weakest,
            weakest_confidence=round(weakest_conf, 4),
        )

    # ------------------------------------------------------------------
    # Step 8: Response assembly
    # ------------------------------------------------------------------

    def _assemble_response(
        self,
        sources: list[FieldLocation],
        dim_authorities: dict[str, SystemAuthority],
        join_paths: list[JoinPath],
        resolved_filters: list[ResolvedFilter],
        confidence: ConfidenceBreakdown,
        warnings: list[str],
    ) -> QueryResolution:
        """Build full response with provenance and data query hint."""
        primary = sources[0]

        # Build provenance string
        prov_parts = [
            f"{primary.concept} from {primary.system} "
            f"{primary.object_name}.{primary.field}"
        ]
        for dim, auth in dim_authorities.items():
            prov_parts.append(f"{dim} from {auth.system}")
        for jp in join_paths:
            if jp.hops:
                prov_parts.append(f"joined via {jp.description}")
        provenance = "; ".join(prov_parts)

        # Build answer path — collect all edges traversed
        answer_path: list[SGraphEdge] = []
        # CLASSIFIED_AS edge for primary concept
        concept_id = f"concept:{primary.concept}"
        field_id = (
            f"field:{primary.system}.{primary.object_name}.{primary.field}"
        )
        answer_path.append(SGraphEdge(
            source_id=field_id, target_id=concept_id,
            type="CLASSIFIED_AS", confidence=primary.confidence,
            provenance="resolver",
        ))
        # SLICEABLE_BY edges
        for dim in dim_authorities:
            answer_path.append(SGraphEdge(
                source_id=concept_id, target_id=f"dimension:{dim}",
                type="SLICEABLE_BY", confidence=1.0,
                provenance="resolver",
            ))

        # Build data query hint
        tables = [f"{primary.object_name}"]
        join_keys: list[dict] = []
        for jp in join_paths:
            for hop in jp.hops:
                join_keys.append({
                    "from": hop.from_field,
                    "to": hop.to_field,
                    "via": hop.via,
                })

        filter_dicts: list[dict] = []
        for rf in resolved_filters:
            filter_dicts.append({
                "dimension": rf.dimension,
                "values": rf.resolved_values,
                "type": rf.resolution_type,
            })

        data_query = DataQueryHint(
            primary_system=primary.system,
            tables=tables,
            join_keys=join_keys,
            filters=filter_dicts,
            description=provenance,
        )

        # Low-confidence warning
        if confidence.overall < 0.5:
            warnings.append(
                f"Overall confidence {confidence.overall:.2f} is below threshold; "
                f"weakest: {confidence.weakest_link} ({confidence.weakest_confidence:.2f})"
            )

        return QueryResolution(
            can_answer=True,
            answer_path=answer_path,
            confidence=confidence,
            provenance=provenance,
            data_query=data_query,
            warnings=warnings,
            concept_sources=sources,
            dimension_authorities=dim_authorities,
            join_paths=join_paths,
            resolved_filters=resolved_filters,
        )
