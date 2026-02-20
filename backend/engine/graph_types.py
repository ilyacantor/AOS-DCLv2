"""
Type definitions for the DCL semantic graph traversal engine.

These types model the in-memory semantic graph (nodes, edges) and the
query resolution pipeline (intent, resolution, confidence, provenance).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, Optional


# ---------------------------------------------------------------------------
# Graph structure types
# ---------------------------------------------------------------------------

NodeType = Literal["concept", "dimension", "system", "field", "dimension_value"]

EdgeType = Literal[
    "CLASSIFIED_AS",
    "LIVES_IN",
    "MAPS_TO",
    "SLICEABLE_BY",
    "HIERARCHY_PARENT",
    "AUTHORITATIVE_FOR",
    "REPORTS_AS",
]


@dataclass(slots=True)
class SGraphNode:
    """A node in the semantic graph.

    Prefixed with 'S' to avoid collision with the existing domain GraphNode
    used by the Sankey renderer.
    """
    id: str
    type: NodeType
    label: str
    metadata: dict = field(default_factory=dict)


@dataclass(slots=True)
class SGraphEdge:
    """A directed edge in the semantic graph."""
    source_id: str
    target_id: str
    type: EdgeType
    confidence: float = 1.0
    provenance: str = ""
    metadata: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Query resolution types
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class FieldLocation:
    """A field that backs a concept, with its system and confidence."""
    system: str
    object_name: str
    field: str
    concept: str
    confidence: float


@dataclass(slots=True)
class SystemAuthority:
    """Which system is authoritative for a given dimension."""
    system: str
    dimension: str
    confidence: float
    source: Literal["contour_map", "inferred", "default"] = "default"


@dataclass(slots=True)
class JoinHop:
    """One hop in a cross-system join path."""
    from_system: str
    from_field: str
    to_system: str
    to_field: str
    via: str  # "direct" | "workato_recipe_4782" | "snowflake_join" | etc.
    confidence: float


@dataclass(slots=True)
class JoinPath:
    """A path connecting two systems through one or more hops."""
    hops: list[JoinHop] = field(default_factory=list)
    total_confidence: float = 0.0
    description: str = ""


@dataclass(slots=True)
class ResolvedFilter:
    """A dimension filter after hierarchy / management overlay expansion."""
    dimension: str
    original_value: str
    resolved_values: list[str] = field(default_factory=list)
    resolution_type: str = "exact"  # "exact" | "hierarchy_expansion" | "management_overlay"


@dataclass(slots=True)
class QueryFilter:
    """A single filter in a query intent."""
    dimension: str
    operator: str = "equals"  # "equals" | "in" | "not"
    value: str | list[str] = ""


@dataclass(slots=True)
class QueryIntent:
    """Parsed intent from NLQ — what concepts, dimensions, and filters."""
    concepts: list[str] = field(default_factory=list)
    dimensions: list[str] = field(default_factory=list)
    filters: list[QueryFilter] = field(default_factory=list)
    persona: str | None = None


@dataclass(slots=True)
class ConfidenceBreakdown:
    """Detailed confidence scoring for a resolved query path."""
    overall: float = 0.0
    per_hop: dict[str, float] = field(default_factory=dict)
    weakest_link: str = ""
    weakest_confidence: float = 1.0


@dataclass(slots=True)
class DataQueryHint:
    """Hint for NLQ/Runner on how to actually fetch the data."""
    primary_system: str = ""
    tables: list[str] = field(default_factory=list)
    join_keys: list[dict] = field(default_factory=list)
    filters: list[dict] = field(default_factory=list)
    description: str = ""


@dataclass(slots=True)
class QueryResolution:
    """Full result of resolving a query against the semantic graph."""
    can_answer: bool = False
    answer_path: list[SGraphEdge] = field(default_factory=list)
    confidence: ConfidenceBreakdown = field(default_factory=ConfidenceBreakdown)
    provenance: str = ""
    data_query: DataQueryHint | None = None
    warnings: list[str] = field(default_factory=list)
    reason: str | None = None

    # Populated during resolution for downstream use
    concept_sources: list[FieldLocation] = field(default_factory=list)
    dimension_authorities: dict[str, SystemAuthority] = field(default_factory=dict)
    join_paths: list[JoinPath] = field(default_factory=list)
    resolved_filters: list[ResolvedFilter] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class GraphStats:
    """Summary statistics for the semantic graph."""
    concept_nodes: int = 0
    dimension_nodes: int = 0
    system_nodes: int = 0
    field_nodes: int = 0
    dimension_value_nodes: int = 0
    edges_by_type: dict[str, int] = field(default_factory=dict)
    connected_systems: int = 0
    avg_path_confidence: float = 0.0
