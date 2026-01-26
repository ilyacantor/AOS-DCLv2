"""
BLL Contract Models - Domain models for BLL consumption contracts.
"""
import os
from datetime import datetime
from typing import Any, Dict, List, Optional
from enum import Enum
from pydantic import BaseModel, Field

# Default dataset - Farm if FARM_SCENARIO_ID is set, otherwise demo9
_FARM_SCENARIO = os.environ.get("FARM_SCENARIO_ID")
_DEFAULT_DATASET = f"farm:{_FARM_SCENARIO}" if _FARM_SCENARIO else "demo9"


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


class OrderBySpec(BaseModel):
    """Sort specification with concrete column name."""
    field: str  # Concrete column name (e.g., "AnnualRevenue", not "revenue")
    direction: str = "desc"  # "asc" or "desc"


class DefinitionCapabilities(BaseModel):
    """Capabilities that a definition supports for operator extraction."""
    supports_top_n: bool = True  # Can be limited/ranked (most definitions)
    supports_delta: bool = False  # Supports MoM/QoQ/YoY change comparison
    supports_trend: bool = False  # Supports time-series trending
    supports_aggregation: bool = True  # Can compute totals/averages
    primary_metric: Optional[str] = None  # Primary metric label (e.g., "revenue", "cost")
    entity_type: Optional[str] = None  # What the rows represent (e.g., "customer", "resource")
    # Production-grade ordering fields
    default_order_by: List["OrderBySpec"] = Field(default_factory=list)  # Concrete columns for TopN
    allowed_order_by: List[str] = Field(default_factory=list)  # Whitelist of override columns
    tie_breaker: Optional[str] = None  # Deterministic secondary sort column
    # Output shape hint - helps intent matcher prefer this definition for matching queries
    # "scalar" = aggregate totals, "ranked" = top-N lists, "table" = tabular data, "status" = health checks
    output_shape: Optional[str] = None  # None means infer from supports_top_n


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
    keywords: List[str] = Field(default_factory=list)  # Core keywords for NLQ matching
    capabilities: DefinitionCapabilities = Field(default_factory=DefinitionCapabilities)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class TimeWindow(BaseModel):
    start: Optional[datetime] = None
    end: Optional[datetime] = None


class ExecuteRequest(BaseModel):
    dataset_id: str = Field(default=_DEFAULT_DATASET, alias="datasetId")
    definition_id: str = Field(alias="definitionId")
    version: Optional[str] = None
    time_window: Optional[TimeWindow] = Field(default=None, alias="timeWindow")
    # NLQ-extracted time window string (e.g., "last_year", "this_quarter")
    # Used for Farm integration where string-based time filters are supported
    time_window_str: Optional[str] = Field(default=None, alias="timeWindowStr")
    dimensions: Optional[List[str]] = None
    filters: Optional[List[FilterSpec]] = None
    order_by: Optional[List[OrderBySpec]] = Field(default=None, alias="orderBy")
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
    """
    Pre-computed aggregations for NLQ consumption.

    Contract:
    - aggregations: Required metrics (population_total, population_count, topn_total, share_of_total_pct)
    - warnings: List of warnings (e.g., missing limit for ranked queries)
    - debug_summary: Optional human-readable summary (NOT for display in NLQ answers)

    The 'answer' field is DEPRECATED - BLL/NLQ should generate human-readable
    text from aggregations, NOT use this field.
    """
    aggregations: Dict[str, Any] = {}  # e.g., {"population_total": 1230000, "topn_total": 500000}
    warnings: List[str] = []  # e.g., ["No limit specified for ranked list query"]
    debug_summary: Optional[str] = None  # For debugging only, NOT for NLQ display
    currency: Optional[str] = "USD"
    # DEPRECATED - use debug_summary. Kept for backward compatibility.
    answer: Optional[str] = None


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
    keywords: List[str] = Field(default_factory=list)
