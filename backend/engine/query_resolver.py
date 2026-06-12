"""
Resolves NLQ queries against the semantic graph.

Takes a QueryIntent (concepts, dimensions, filters) and runs the
8-step resolution flow: concept location → dimension validity →
dimension source → join path → filter resolution → confidence →
response assembly.
"""

from __future__ import annotations

import json
import time
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


_DEFAULT_CACHE_TTL = 300  # 5 minutes

# Dimension aliases — common names that map to canonical dimension IDs
_DIMENSION_ALIASES: dict[str, str] = {
    "geography": "region",
    "geo": "region",
    "location": "region",
    "team": "department",
    "business_unit": "division",
    "bu": "division",
    "entity": "legal_entity",
}


class QueryResolver:
    """Resolves NLQ queries against the semantic graph."""

    def __init__(
        self, graph: SemanticGraph, cache_ttl: int = _DEFAULT_CACHE_TTL,
    ) -> None:
        self.graph = graph
        self._cache_ttl = cache_ttl
        self._path_cache: dict[str, tuple[float, QueryResolution]] = {}

    # ------------------------------------------------------------------
    # Cache helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _cache_key(intent: QueryIntent) -> str:
        """Deterministic key from intent (concepts + dimensions + filters, sorted)."""
        filters_repr = sorted(
            (f.dimension, f.operator, json.dumps(f.value, sort_keys=True))
            for f in intent.filters
        )
        return json.dumps({
            "c": sorted(intent.concepts),
            "d": sorted(intent.dimensions),
            "f": filters_repr,
            "p": intent.persona or "",
        }, sort_keys=True)

    def invalidate_cache(self) -> None:
        """Clear the path cache (called on graph rebuild)."""
        self._path_cache.clear()

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def resolve(self, intent: QueryIntent) -> QueryResolution:
        """Run the 8-step resolution flow with caching.

        Steps 1 (intent parsing) is handled by NLQ before calling DCL.
        """
        key = self._cache_key(intent)
        cached = self._path_cache.get(key)
        if cached is not None:
            ts, result = cached
            if time.monotonic() - ts < self._cache_ttl:
                return result
            del self._path_cache[key]

        result = self._resolve_uncached(intent)
        self._path_cache[key] = (time.monotonic(), result)
        return result

    @staticmethod
    def _canonicalize_dimensions(dimensions: list[str]) -> list[str]:
        """Map dimension aliases to canonical names."""
        return [_DIMENSION_ALIASES.get(d, d) for d in dimensions]

    @staticmethod
    def _canonicalize_filters(filters: list[QueryFilter]) -> list[QueryFilter]:
        """Map filter dimension aliases to canonical names."""
        return [
            QueryFilter(
                dimension=_DIMENSION_ALIASES.get(f.dimension, f.dimension),
                operator=f.operator,
                value=f.value,
            )
            for f in filters
        ]

    def _resolve_uncached(self, intent: QueryIntent) -> QueryResolution:
        """Full 8-step resolution without cache."""
        # Canonicalize dimension aliases (region→geography, etc.)
        intent = QueryIntent(
            concepts=intent.concepts,
            dimensions=self._canonicalize_dimensions(intent.dimensions),
            filters=self._canonicalize_filters(intent.filters),
            persona=intent.persona,
        )

        # Step 2: Concept location (persona-scoped when intent.persona is set)
        sources, persona_excluded = self._locate_concepts(
            intent.concepts, intent.persona,
        )
        persona_warnings: list[str] = []
        if persona_excluded:
            excluded_desc = ", ".join(
                f"'{c}' (domain '{c.split('.', 1)[0]}')" for c in persona_excluded
            )
            from backend.engine.persona_view import resolve_persona_domains
            allowed = resolve_persona_domains(intent.persona)
            persona_warnings.append(
                f"Persona '{intent.persona}' scoping excluded concepts "
                f"outside its domains: {excluded_desc}. Allowed domains: "
                f"[{', '.join(allowed)}]."
            )
        if not sources:
            if persona_excluded and len(persona_excluded) == len(intent.concepts):
                # ALL requested concepts fall outside the persona's domains —
                # existing not-found shape, reason names the scoping in detail.
                return QueryResolution(
                    can_answer=False,
                    reason=(
                        f"Persona scoping excluded all requested concepts: "
                        f"{persona_warnings[0]} Nothing remains to resolve "
                        f"for persona '{intent.persona}'."
                    ),
                )
            in_scope = [c for c in intent.concepts if c not in persona_excluded]
            # Distinguish unknown concepts from unmapped ones
            unknown = [
                c for c in in_scope
                if f"concept:{c}" not in self.graph.nodes
            ]
            if unknown:
                return QueryResolution(
                    can_answer=False,
                    reason=f"Concept not recognized: {', '.join(unknown)}",
                    warnings=persona_warnings,
                )
            return QueryResolution(
                can_answer=False,
                reason=f"No sources found for concepts: {in_scope}",
                warnings=persona_warnings,
            )

        # Step 3: Dimension validity (over in-scope concepts only — persona
        # exclusions are disclosed in warnings, not re-judged here)
        located_concepts = [
            c for c in intent.concepts if c not in persona_excluded
        ]
        invalid = self._check_dimensions(located_concepts, intent.dimensions)
        if invalid:
            pairs_str = ", ".join(f"{c}×{d}" for c, d in invalid)
            return QueryResolution(
                can_answer=False,
                reason=f"Invalid concept-dimension pairings: {pairs_str}",
                concept_sources=sources,
                warnings=persona_warnings,
            )

        # Step 4: Dimension source resolution
        dim_authorities = self._resolve_dimension_sources(intent.dimensions)

        # Step 5: Join path discovery
        join_paths, join_warnings = self._find_join_paths(sources, dim_authorities)

        # Step 6: Filter resolution
        resolved_filters = self._resolve_filters(intent.filters)

        # Step 7: Confidence scoring
        confidence = self._score_path(sources, dim_authorities, join_paths)

        # Step 8: Response assembly (persona exclusions disclosed in warnings)
        return self._assemble_response(
            sources, dim_authorities, join_paths,
            resolved_filters, confidence, persona_warnings + join_warnings,
        )

    # ------------------------------------------------------------------
    # Step 2: Concept location (with alias fallback)
    # ------------------------------------------------------------------

    def _locate_concepts(
        self, concepts: list[str], persona: str | None = None,
    ) -> tuple[list[FieldLocation], list[str]]:
        """Find the best source for each concept, with alias fallback.

        persona (Gate 2B): concepts whose domain (first dotted segment)
        is outside the persona's domain list are EXCLUDED from location —
        returned in the second tuple element so the caller can disclose
        them (warnings on partial exclusion, detailed not-found reason
        when everything is excluded). Unknown persona raises
        UnknownPersonaError (the route 422s before reaching here; direct
        callers fail loudly). persona=None excludes nothing.
        """
        persona_excluded: list[str] = []
        if persona is not None:
            from backend.engine.persona_view import resolve_persona_domains
            allowed = set(resolve_persona_domains(persona))
            persona_excluded = [
                c for c in concepts if c.split(".", 1)[0] not in allowed
            ]
            if persona_excluded:
                logger.info(
                    f"[Resolver] Persona '{persona}' excluded concepts "
                    f"outside its domains: {persona_excluded}"
                )
                concepts = [c for c in concepts if c not in persona_excluded]
        all_sources: list[FieldLocation] = []
        for concept in concepts:
            sources = self.graph.find_concept_sources(concept)
            if not sources:
                # Alias fallback: check if this concept is an alias of another
                primary = self._resolve_concept_alias(concept)
                if primary and primary != concept:
                    sources = self.graph.find_concept_sources(primary)
                    # Re-label sources with the requested concept name
                    sources = [
                        FieldLocation(
                            system=s.system, object_name=s.object_name,
                            field=s.field, concept=concept,
                            confidence=s.confidence * 0.95,
                        )
                        for s in sources
                    ]
            if sources:
                all_sources.extend(sources)
            else:
                logger.warning(f"[Resolver] No sources for concept: {concept}")
        return all_sources, persona_excluded

    def _resolve_concept_alias(self, concept_name: str) -> str | None:
        """Check ontology for a concept that lists this name as an alias."""
        try:
            from backend.engine.ontology import get_ontology
            for c in get_ontology():
                if concept_name in [a.lower() for a in (c.aliases or [])]:
                    return c.id
        except Exception as e:
            logger.error(
                f"[query_resolver] Ontology alias resolution failed: {e}. "
                "Returning None — query may degrade silently.",
                exc_info=True
            )
        return None

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

            # Enrich with hierarchy metadata from DimensionHierarchyStore
            try:
                from backend.engine.dimension_hierarchy import get_hierarchy_store
                store = get_hierarchy_store()
                dim_ids = store.get_dimension_ids()
                if dim in dim_ids:
                    max_depth = store.get_max_depth(dim)
                    roots = store.get_roots(dim)
                    authorities[dim].metadata["hierarchy_max_depth"] = max_depth
                    authorities[dim].metadata["hierarchy_root_count"] = len(roots)
            except RuntimeError:
                logger.warning(
                    f"[Resolver] DimensionHierarchyStore unavailable for "
                    f"dimension metadata enrichment (dimension={dim}); "
                    "continuing without hierarchy metadata"
                )
            except Exception:
                logger.warning(
                    f"[Resolver] DimensionHierarchyStore lookup failed for "
                    f"dimension metadata enrichment (dimension={dim}); "
                    "continuing without hierarchy metadata",
                    exc_info=True,
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

        # Build hierarchy_context if any filter used hierarchy expansion
        hierarchy_context: dict | None = None
        for rf in resolved_filters:
            if rf.resolution_type == "hierarchy_expansion":
                hierarchy_context = {
                    "dimension_id": rf.dimension,
                    "resolved_depth": len(rf.resolved_values),
                    "rollup_from": rf.original_value,
                }
                break  # Use the first hierarchy-expanded filter

        data_query = DataQueryHint(
            primary_system=primary.system,
            tables=tables,
            join_keys=join_keys,
            filters=filter_dicts,
            description=provenance,
            hierarchy_context=hierarchy_context,
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
