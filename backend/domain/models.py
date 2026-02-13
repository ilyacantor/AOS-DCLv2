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
    entities: List[str] = Field(default_factory=list)


class OntologyConcept(BaseModel):
    id: str
    name: str
    description: str
    example_fields: List[str] = Field(default_factory=list)
    expected_type: Optional[str] = None


class Mapping(BaseModel):
    id: str
    source_field: str
    source_table: str
    source_system: str
    ontology_concept: str
    confidence: float = Field(ge=0.0, le=1.0)
    method: Literal["heuristic", "rag", "llm", "llm_validated"]
    status: Literal["ok", "conflict", "warning"] = "ok"
    rationale: Optional[str] = None


class MappingDetail(CamelCaseModel):
    """
    Structured mapping information for graph links.
    Replaces string-based info_summary for mapping flow types.
    """
    source_field: str
    source_table: str
    target_concept: str
    method: Literal["heuristic", "rag", "llm", "llm_validated"]
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


class GraphSnapshot(CamelCaseModel):
    nodes: List[GraphNode]
    links: List[GraphLink]
    meta: Dict[str, Any]
