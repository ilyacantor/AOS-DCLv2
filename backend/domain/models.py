from typing import List, Optional, Dict, Any, Literal
from pydantic import BaseModel, Field
from datetime import datetime
from enum import Enum


class Persona(str, Enum):
    CFO = "CFO"
    CRO = "CRO"
    COO = "COO"
    CTO = "CTO"


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
    method: Literal["heuristic", "rag", "llm"]
    status: Literal["ok", "conflict", "warning"] = "ok"
    rationale: Optional[str] = None


class GraphNode(BaseModel):
    id: str
    label: str
    level: Literal["L0", "L1", "L2", "L3"]
    kind: Literal["pipe", "source", "ontology", "bll"]
    group: Optional[str] = None
    status: Optional[str] = "ok"
    metrics: Optional[Dict[str, Any]] = None


class GraphLink(BaseModel):
    id: str
    source: str
    target: str
    value: float
    confidence: Optional[float] = None
    flow_type: Optional[str] = None
    info_summary: Optional[str] = None


class GraphSnapshot(BaseModel):
    nodes: List[GraphNode]
    links: List[GraphLink]
    meta: Dict[str, Any]


class RunMetrics(BaseModel):
    llm_calls: int = 0
    rag_reads: int = 0
    rag_writes: int = 0
    processing_ms: float = 0
    render_ms: float = 0
