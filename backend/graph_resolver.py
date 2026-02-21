"""
Graph Resolver — resolves a QueryIntent against the DCL semantic graph.

Traversal steps:
  1. Resolve concepts against the ontology + semantic catalog
  2. Validate concept-dimension pairings
  3. Find source systems via provenance
  4. Check cross-system join paths if multiple SORs are involved
  5. Resolve filter values (hierarchy drill-down, management overlay)
  6. Compute path confidence and return QueryResolution
"""

import importlib
import importlib.util
import yaml
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from backend.domain import (
    FilterClause,
    FilterResolution,
    JoinPath,
    ProvenanceStep,
    QueryIntent,
    QueryResolution,
)
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

_PAIRINGS_PATH = Path(__file__).parent.parent / "config" / "concept_dimension_pairings.yaml"


def _import_engine_module(module_name: str):
    """Import a module from backend/engine/ without triggering __init__.py."""
    mod_path = Path(__file__).parent / "engine" / f"{module_name}.py"
    spec = importlib.util.spec_from_file_location(
        f"backend.engine.{module_name}", str(mod_path),
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

# ── Lazy-loaded caches ────────────────────────────────────────────────

_pairings_cache: Optional[Dict[str, List[str]]] = None


def _load_pairings() -> Dict[str, List[str]]:
    """Load concept→dimension pairings from YAML (cached)."""
    global _pairings_cache
    if _pairings_cache is not None:
        return _pairings_cache
    if not _PAIRINGS_PATH.exists():
        logger.warning("[GraphResolver] concept_dimension_pairings.yaml not found")
        _pairings_cache = {}
        return _pairings_cache
    with open(_PAIRINGS_PATH) as f:
        data = yaml.safe_load(f)
    _pairings_cache = data.get("pairings", {})
    logger.info(f"[GraphResolver] Loaded {len(_pairings_cache)} concept-dimension pairings")
    return _pairings_cache


# ── Concept resolution ────────────────────────────────────────────────

def _resolve_concepts(
    requested: List[str],
) -> Tuple[List[str], List[str], float]:
    """
    Resolve requested concept names against the ontology and semantic catalog.

    Returns (found_concepts, not_found, avg_confidence).
    Uses both the ontology (107 concepts) and the published metrics catalog.
    """
    ontology_mod = _import_engine_module("ontology")
    from backend.api.semantic_export import resolve_metric, PUBLISHED_METRICS

    ontology = ontology_mod.get_ontology()
    ontology_ids = {c.id for c in ontology}
    ontology_aliases: Dict[str, str] = {}
    for c in ontology:
        for alias in c.aliases:
            ontology_aliases[alias.lower()] = c.id

    metric_ids = {m.id for m in PUBLISHED_METRICS}
    metric_aliases: Dict[str, str] = {}
    for m in PUBLISHED_METRICS:
        for alias in m.aliases:
            metric_aliases[alias.lower()] = m.id

    found: List[str] = []
    not_found: List[str] = []
    confidences: List[float] = []

    for concept in requested:
        concept_lower = concept.lower().strip()

        # Exact match in ontology
        if concept_lower in ontology_ids:
            found.append(concept_lower)
            confidences.append(0.95)
            continue

        # Exact match in metrics catalog
        if concept_lower in metric_ids:
            found.append(concept_lower)
            confidences.append(0.95)
            continue

        # Alias match in ontology
        if concept_lower in ontology_aliases:
            found.append(ontology_aliases[concept_lower])
            confidences.append(0.90)
            continue

        # Alias match in metrics
        if concept_lower in metric_aliases:
            found.append(metric_aliases[concept_lower])
            confidences.append(0.90)
            continue

        # Fuzzy match via semantic export
        metric_def = resolve_metric(concept_lower)
        if metric_def:
            found.append(metric_def.id)
            confidences.append(0.80)
            continue

        not_found.append(concept)

    avg_conf = sum(confidences) / len(confidences) if confidences else 0.0
    return found, not_found, avg_conf


# ── Dimension validation ──────────────────────────────────────────────

def _validate_dimensions(
    concepts: List[str],
    dimensions: List[str],
) -> Tuple[List[str], List[str], List[str]]:
    """
    Validate that each dimension can be paired with at least one concept.

    Checks in order:
      1. Management overlay dimensions (always valid)
      2. Concept-dimension pairings YAML (direct pairing)
      3. Org dimensions exist anywhere in the pairings matrix (join-path pairing)
      4. Metric catalog allowed_dims

    Returns (valid_dims, invalid_dims, warnings).
    """
    from backend.api.semantic_export import resolve_metric

    pairings = _load_pairings()

    # Build set of all known org dimensions from the pairings matrix
    all_known_dims: Set[str] = set()
    for concept_dims in pairings.values():
        if isinstance(concept_dims, list):
            all_known_dims.update(concept_dims)

    valid: List[str] = []
    invalid: List[str] = []
    warnings: List[str] = []

    # Normalize dimension aliases
    dim_aliases = {
        "region": "geography",
        "geo": "geography",
        "cost_center": "cost_center",
        "dept": "department",
    }

    for dim in dimensions:
        dim_norm = dim_aliases.get(dim.lower(), dim.lower())
        paired = False

        # Management overlay dimensions are always valid
        if dim_norm in _MANAGEMENT_OVERLAY:
            valid.append(dim)
            continue

        # Check concept-dimension pairings YAML (direct pairing)
        for concept in concepts:
            concept_dims = pairings.get(concept, [])
            if dim_norm in concept_dims or dim.lower() in concept_dims:
                paired = True
                break

        # Fallback: allow org dimensions if ANY concept in the pairings
        # matrix supports them — this represents a cross-system join path.
        # But only for concepts that have at least one pairing themselves
        # (avoids matching pure infra/eng concepts with finance dimensions).
        if not paired and dim_norm in all_known_dims:
            for concept in concepts:
                concept_dims = pairings.get(concept, [])
                # Only allow cross-join if concept has known pairings
                # (i.e. it's a business concept, not purely technical)
                if concept_dims:
                    paired = True
                    break

        # Fallback: check metric catalog allowed_dims
        if not paired:
            for concept in concepts:
                metric_def = resolve_metric(concept)
                if metric_def and dim.lower() in [d.lower() for d in metric_def.allowed_dims]:
                    paired = True
                    break

        if paired:
            valid.append(dim)
        else:
            invalid.append(dim)
            warnings.append(
                f"'{dim}' cannot be sliced by concepts {concepts}"
            )

    return valid, invalid, warnings


# ── Provenance resolution ─────────────────────────────────────────────

def _resolve_provenance(
    concepts: List[str],
) -> Tuple[List[ProvenanceStep], Optional[str]]:
    """
    Find source system provenance for each concept.

    Returns (provenance_steps, primary_system).
    """
    provenance_mod = _import_engine_module("provenance_service")
    get_provenance = provenance_mod.get_provenance

    steps: List[ProvenanceStep] = []
    sor_system: Optional[str] = None

    for concept in concepts:
        trace = get_provenance(concept)
        if trace and trace.sources:
            for src in trace.sources:
                steps.append(ProvenanceStep(
                    concept=concept,
                    source_system=src.source_system,
                    table=src.table_or_collection,
                    field=src.field_name,
                    confidence=src.quality_score,
                    is_sor=src.is_sor,
                ))
                if src.is_sor and sor_system is None:
                    sor_system = src.source_system

    return steps, sor_system


# ── Cross-system join paths ───────────────────────────────────────────

def _find_join_paths(
    provenance: List[ProvenanceStep],
) -> List[JoinPath]:
    """
    If concepts/dimensions come from different source systems, find join paths.

    Currently uses a heuristic: any two systems can be joined via AAM semantic
    edge with reduced confidence. In production, this would query AAM for actual
    edges.
    """
    systems: Set[str] = set()
    for step in provenance:
        systems.add(step.source_system)

    if len(systems) <= 1:
        return []

    paths: List[JoinPath] = []
    system_list = sorted(systems)
    for i in range(len(system_list)):
        for j in range(i + 1, len(system_list)):
            paths.append(JoinPath(
                from_system=system_list[i],
                to_system=system_list[j],
                join_type="aam_edge",
                confidence=0.6,
            ))

    return paths


# ── Filter resolution (hierarchy, management overlay) ─────────────────

# Sample hierarchy data for integration testing
_HIERARCHY_DATA: Dict[str, Dict[str, List[str]]] = {
    "division": {
        "cloud": ["Cloud East", "Cloud West"],
        "enterprise": ["Enterprise North", "Enterprise South"],
    },
    "cost_center": {
        "engineering": ["Cloud Engineering", "Platform Engineering"],
        "sales": ["Direct Sales", "Channel Sales"],
    },
}

# Management overlay: maps board-level terms to dimensions
_MANAGEMENT_OVERLAY: Dict[str, Tuple[str, List[str]]] = {
    "board segment": ("division", ["Cloud East", "Cloud West", "Enterprise North", "Enterprise South"]),
    "board_segment": ("division", ["Cloud East", "Cloud West", "Enterprise North", "Enterprise South"]),
}


def _resolve_filters(
    filters: List[FilterClause],
) -> Tuple[List[FilterResolution], bool, List[str]]:
    """
    Resolve filter values against hierarchy data and management overlays.

    Returns (filter_resolutions, management_overlay_used, warnings).
    """
    resolutions: List[FilterResolution] = []
    overlay_used = False
    warnings: List[str] = []

    for f in filters:
        dim_lower = f.dimension.lower()
        val_lower = f.value.lower()

        # Check management overlay
        overlay_key = f"{dim_lower}"
        if overlay_key in _MANAGEMENT_OVERLAY:
            mapped_dim, mapped_values = _MANAGEMENT_OVERLAY[overlay_key]
            resolutions.append(FilterResolution(
                dimension=f.dimension,
                value=f.value,
                resolved_to=mapped_values,
                method="management_overlay",
            ))
            overlay_used = True
            continue

        # Check hierarchy data
        hierarchy = _HIERARCHY_DATA.get(dim_lower, {})
        if val_lower in hierarchy:
            resolutions.append(FilterResolution(
                dimension=f.dimension,
                value=f.value,
                resolved_to=hierarchy[val_lower],
                method="hierarchy",
            ))
            continue

        # Exact value — no expansion needed
        resolutions.append(FilterResolution(
            dimension=f.dimension,
            value=f.value,
            resolved_to=[f.value],
            method="exact",
        ))

    return resolutions, overlay_used, warnings


# ── Main resolver ─────────────────────────────────────────────────────

def resolve(intent: QueryIntent) -> QueryResolution:
    """
    Resolve a QueryIntent against the DCL semantic graph.

    This is the main entry point called by POST /api/dcl/resolve.
    """
    warnings: List[str] = []

    # Step 1: Resolve concepts
    if not intent.concepts:
        return QueryResolution(
            can_answer=False,
            reason="No concepts specified in query intent",
        )

    found_concepts, not_found, concept_confidence = _resolve_concepts(intent.concepts)

    if not found_concepts:
        return QueryResolution(
            can_answer=False,
            reason=f"Concepts not recognized: {', '.join(not_found)}",
            warnings=[f"'{c}' is not recognized in the ontology" for c in not_found],
        )

    if not_found:
        warnings.append(f"Partially resolved — not found: {', '.join(not_found)}")

    # Step 2: Validate dimensions
    valid_dims: List[str] = []
    if intent.dimensions:
        valid_dims, invalid_dims, dim_warnings = _validate_dimensions(
            found_concepts, intent.dimensions,
        )
        warnings.extend(dim_warnings)

        if invalid_dims and not valid_dims:
            return QueryResolution(
                can_answer=False,
                reason=f"Dimensions cannot be sliced by the requested concepts: {', '.join(invalid_dims)}",
                concepts_found=found_concepts,
                warnings=warnings,
            )

    # Step 3: Find provenance
    provenance, primary_system = _resolve_provenance(found_concepts)

    if not provenance:
        warnings.append("No provenance data found — confidence reduced")

    # Step 4: Cross-system join paths
    join_paths: List[JoinPath] = []
    if provenance:
        join_paths = _find_join_paths(provenance)
        if join_paths:
            warnings.append(
                f"Cross-system join required: {', '.join(j.from_system + ' <-> ' + j.to_system for j in join_paths)}"
            )

    # Step 5: Resolve filters (including management overlay dimensions)
    filter_resolutions: List[FilterResolution] = []
    overlay_used = False

    # Check if any dimension is a management overlay — if so, resolve it
    all_filters = list(intent.filters)
    for dim in valid_dims:
        dim_lower = dim.lower()
        if dim_lower in _MANAGEMENT_OVERLAY:
            mapped_dim, mapped_values = _MANAGEMENT_OVERLAY[dim_lower]
            filter_resolutions.append(FilterResolution(
                dimension=dim,
                value=dim,
                resolved_to=mapped_values,
                method="management_overlay",
            ))
            overlay_used = True

    if all_filters:
        more_resolutions, more_overlay, filter_warnings = _resolve_filters(all_filters)
        filter_resolutions.extend(more_resolutions)
        overlay_used = overlay_used or more_overlay
        warnings.extend(filter_warnings)

    # Step 6: Compute path confidence
    confidence = _compute_confidence(
        concept_confidence=concept_confidence,
        provenance=provenance,
        join_paths=join_paths,
        valid_dims=valid_dims,
        requested_dims=intent.dimensions,
    )

    return QueryResolution(
        can_answer=True,
        concepts_found=found_concepts,
        dimensions_used=valid_dims,
        confidence=round(confidence, 2),
        provenance=provenance,
        join_paths=join_paths,
        filters_resolved=filter_resolutions,
        warnings=warnings,
        primary_system=primary_system,
        management_overlay_used=overlay_used,
    )


def _compute_confidence(
    concept_confidence: float,
    provenance: List[ProvenanceStep],
    join_paths: List[JoinPath],
    valid_dims: List[str],
    requested_dims: List[str],
) -> float:
    """
    Compute overall path confidence.

    Factors:
    - Concept match confidence (weight 0.4)
    - Provenance quality scores (weight 0.3)
    - Join path penalty (weight 0.2)
    - Dimension coverage (weight 0.1)
    """
    # Concept confidence
    c_score = concept_confidence

    # Provenance average quality
    if provenance:
        p_score = sum(s.confidence for s in provenance) / len(provenance)
    else:
        p_score = 0.3

    # Join penalty — each cross-system join reduces confidence
    if join_paths:
        j_score = max(0.3, 1.0 - 0.15 * len(join_paths))
    else:
        j_score = 1.0

    # Dimension coverage
    if requested_dims:
        d_score = len(valid_dims) / len(requested_dims) if requested_dims else 1.0
    else:
        d_score = 1.0

    return (0.4 * c_score) + (0.3 * p_score) + (0.2 * j_score) + (0.1 * d_score)
