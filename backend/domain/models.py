from typing import List, Optional, Dict, Any, Literal
from pydantic import BaseModel, Field
from datetime import datetime
from enum import Enum

from backend.domain.base import CamelCaseModel


class Persona(str, Enum):
    CFO = "CFO"
    CRO = "CRO"
    COO = "COO"
    CTO = "CTO"
    CHRO = "CHRO"


class DiscoveryStatus(str, Enum):
    CANONICAL = "canonical"
    PENDING_TRIAGE = "pending_triage"
    CUSTOM = "custom"
    REJECTED = "rejected"


class ResolutionType(str, Enum):
    EXACT = "exact"
    ALIAS = "alias"
    PATTERN = "pattern"
    FUZZY = "fuzzy"
    DISCOVERED = "discovered"


class FieldSchema(BaseModel):
    name: str
    type: str
    semantic_hint: Optional[str] = None
    nullable: bool = True
    distinct_count: Optional[int] = None
    null_percent: Optional[float] = None
    sample_values: Optional[List[Any]] = None


class TableSchema(BaseModel):
    id: str
    system_id: str
    name: str
    fields: List[FieldSchema]
    record_count: Optional[int] = None
    stats: Optional[Dict[str, Any]] = None


class SourceSystem(BaseModel):
    id: str
    name: str
    type: str
    tags: List[str] = Field(default_factory=list)
    tables: List[TableSchema] = Field(default_factory=list)
    canonical_id: Optional[str] = None
    raw_id: Optional[str] = None
    discovery_status: DiscoveryStatus = DiscoveryStatus.CANONICAL
    resolution_type: Optional[ResolutionType] = None
    trust_score: int = 50
    data_quality_score: int = 50
    vendor: Optional[str] = None
    category: Optional[str] = None
    fabric_plane: Optional[str] = None
    entities: List[str] = Field(default_factory=list)


class OntologyConcept(BaseModel):
    id: str
    concept_id: str = ""
    name: str
    description: str
    domain: str = ""
    cluster: str = ""
    example_fields: List[str] = Field(default_factory=list)
    aliases: List[str] = Field(default_factory=list)
    expected_type: Optional[str] = None
    typical_source_systems: List[str] = Field(default_factory=list)
    persona_relevance: Dict[str, float] = Field(default_factory=dict)


class SemanticEdge(BaseModel):
    """AAM-produced field-to-field mapping from real integration infrastructure."""
    source_system: str
    source_object: str
    source_field: str
    target_system: str
    target_object: str
    target_field: str
    edge_type: Literal["DIRECT_MAP", "TRANSFORMED", "CONDITIONAL", "INFERRED"]
    confidence: float = Field(ge=0.0, le=1.0)
    fabric_plane: str
    extraction_source: str
    transformation: Optional[str] = None


class Mapping(BaseModel):
    id: str
    source_field: str
    source_table: str
    source_system: str
    ontology_concept: str
    confidence: float = Field(ge=0.0, le=1.0)
    method: Literal["heuristic", "rag", "llm", "llm_validated", "aam_edge"]
    status: Literal["ok", "conflict", "warning"] = "ok"
    rationale: Optional[str] = None
    provenance: Optional[str] = None
    cross_system_mapping: Optional[Dict[str, Any]] = None


class MappingDetail(CamelCaseModel):
    """
    Structured mapping information for graph links.
    Replaces string-based info_summary for mapping flow types.
    """
    source_field: str
    source_table: str
    target_concept: str
    method: Literal["heuristic", "rag", "llm", "llm_validated", "aam_edge"]
    confidence: float


class GraphNode(CamelCaseModel):
    id: str
    label: str
    level: Literal["L0", "L1", "L2", "L3"]
    kind: Literal["pipe", "source", "ontology", "bll", "fabric"]
    group: Optional[str] = None
    status: Optional[str] = "ok"
    metrics: Optional[Dict[str, Any]] = None


class GraphLink(CamelCaseModel):
    id: str
    source: str
    target: str
    value: float
    confidence: Optional[float] = None
    flow_type: Optional[str] = None
    info_summary: Optional[str] = None  # Kept for backward compatibility
    mapping_detail: Optional[MappingDetail] = None  # New structured field


class RunMetrics(CamelCaseModel):
    llm_calls: int = 0
    rag_reads: int = 0
    rag_writes: int = 0
    total_mappings: int = 0
    processing_ms: float = 0
    render_ms: float = 0
    data_status: Optional[str] = None
    payload_kpis: Optional[Dict[str, Any]] = None
    db_fallback: bool = False
    llm_fallback: bool = False
    aam_edge_hits: int = 0
    aam_edge_misses: int = 0
    aam_edge_total: int = 0
    aam_cache_hit: bool = False
    aam_unavailable: bool = False


class GraphSnapshot(CamelCaseModel):
    nodes: List[GraphNode]
    links: List[GraphLink]
    meta: Dict[str, Any]


# ── Graph Resolution Models (NLQ → DCL resolve pipeline) ──────────────


class FilterClause(BaseModel):
    """A single filter condition from the parsed query intent."""
    dimension: str
    value: str
    operator: str = "eq"


class QueryIntent(BaseModel):
    """Structured intent parsed from a natural-language question."""
    concepts: List[str] = Field(default_factory=list)
    dimensions: List[str] = Field(default_factory=list)
    filters: List[FilterClause] = Field(default_factory=list)
    grain: Optional[str] = None


class ProvenanceStep(BaseModel):
    """One hop in the provenance chain for a resolved concept."""
    concept: str
    source_system: str
    table: str
    field: str
    confidence: float
    is_sor: bool = False


class JoinPath(BaseModel):
    """A cross-system join path connecting two source systems."""
    from_system: str
    to_system: str
    join_type: str = "aam_edge"
    confidence: float = 0.5


class FilterResolution(BaseModel):
    """How a filter value was resolved (e.g. hierarchy expansion)."""
    dimension: str
    value: str
    resolved_to: List[str] = Field(default_factory=list)
    method: str = "exact"


class QueryResolution(BaseModel):
    """Full resolution result from the DCL graph resolver."""
    can_answer: bool
    reason: Optional[str] = None
    concepts_found: List[str] = Field(default_factory=list)
    dimensions_used: List[str] = Field(default_factory=list)
    confidence: float = 0.0
    provenance: List[ProvenanceStep] = Field(default_factory=list)
    join_paths: List[JoinPath] = Field(default_factory=list)
    filters_resolved: List[FilterResolution] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
    primary_system: Optional[str] = None
    management_overlay_used: bool = False
