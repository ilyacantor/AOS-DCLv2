"""
BLL Contract Models - Domain models for BLL consumption contracts.
"""
from datetime import datetime
from typing import Any, Dict, List, Optional
from enum import Enum
from pydantic import BaseModel, Field


class DefinitionCategory(str, Enum):
    FINOPS = "finops"
    AOD = "aod"
    CRM = "crm"
    INFRA = "infra"


class ColumnSchema(BaseModel):
    name: str
    dtype: str
    nullable: bool = True
    description: Optional[str] = None


class SourceReference(BaseModel):
    source_id: str
    table_id: str
    columns: List[str]


class JoinSpec(BaseModel):
    left_table: str
    right_table: str
    left_key: str
    right_key: str
    join_type: str = "inner"


class FilterSpec(BaseModel):
    column: str
    operator: str
    value: Any


class Definition(BaseModel):
    definition_id: str
    name: str
    description: str
    category: DefinitionCategory
    version: str = "1.0.0"
    output_schema: List[ColumnSchema]
    sources: List[SourceReference]
    joins: Optional[List[JoinSpec]] = None
    default_filters: Optional[List[FilterSpec]] = None
    dimensions: List[str] = Field(default_factory=list)
    metrics: List[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class TimeWindow(BaseModel):
    start: Optional[datetime] = None
    end: Optional[datetime] = None


class ExecuteRequest(BaseModel):
    dataset_id: str = Field(default="demo9", alias="datasetId")
    definition_id: str = Field(alias="definitionId")
    version: Optional[str] = None
    time_window: Optional[TimeWindow] = Field(default=None, alias="timeWindow")
    dimensions: Optional[List[str]] = None
    filters: Optional[List[FilterSpec]] = None
    limit: int = 1000
    offset: int = 0
    
    model_config = {"populate_by_name": True}


class QualityMetrics(BaseModel):
    completeness: float = Field(ge=0.0, le=1.0)
    freshness_hours: float
    row_count: int
    null_percentage: float = Field(ge=0.0, le=100.0)


class LineageReference(BaseModel):
    source_id: str
    table_id: str
    columns_used: List[str]
    row_contribution: int


class ExecuteMetadata(BaseModel):
    dataset_id: str
    definition_id: str
    version: str
    executed_at: datetime
    execution_time_ms: int
    row_count: int
    result_schema: List[ColumnSchema]


class ComputedSummary(BaseModel):
    """Pre-computed aggregations and answer summary."""
    answer: str  # Human-readable answer like "Your current ARR is $1,230,000"
    aggregations: Dict[str, Any] = {}  # e.g., {"total_arr": 1230000, "deal_count": 7}
    currency: Optional[str] = "USD"


class ExecuteResponse(BaseModel):
    data: List[Dict[str, Any]]
    metadata: ExecuteMetadata
    quality: QualityMetrics
    lineage: List[LineageReference]
    summary: Optional[ComputedSummary] = None  # Computed answer for NLQ


class ProofBreadcrumb(BaseModel):
    step: int
    action: str
    details: Dict[str, Any]


class ProofResponse(BaseModel):
    definition_id: str
    version: str
    generated_at: datetime
    breadcrumbs: List[ProofBreadcrumb]
    sql_equivalent: Optional[str] = None


class DefinitionListItem(BaseModel):
    definition_id: str
    name: str
    category: DefinitionCategory
    version: str
    description: str
