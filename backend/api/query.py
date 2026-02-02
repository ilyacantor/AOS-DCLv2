"""
DCL Query Endpoint - executes data queries against the fact base.

This module handles:
- Query validation against the semantic catalog
- Data retrieval from fact_base.json (Demo mode)
- Filtering and aggregation based on dimensions and time ranges
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field

from backend.api.semantic_export import PUBLISHED_METRICS, resolve_metric
from backend.core.mode_state import get_current_mode


class QueryRequest(BaseModel):
    """Request model for DCL queries."""
    metric: str
    dimensions: List[str] = Field(default_factory=list)
    filters: Dict[str, Union[str, List[str]]] = Field(default_factory=dict)
    time_range: Optional[Dict[str, str]] = None
    grain: Optional[str] = None


class QueryDataPoint(BaseModel):
    """Single data point in query results."""
    period: str
    value: float
    dimensions: Dict[str, str] = Field(default_factory=dict)


class QueryMetadata(BaseModel):
    """Metadata about the query execution."""
    sources: List[str]
    freshness: str
    quality_score: float
    mode: str
    record_count: int


class QueryResponse(BaseModel):
    """Response model for DCL queries."""
    metric: str
    metric_name: str
    dimensions: List[str]
    grain: str
    unit: str
    data: List[QueryDataPoint]
    metadata: QueryMetadata


class QueryError(BaseModel):
    """Error response for invalid queries."""
    error: str
    code: str
    details: Optional[Dict[str, Any]] = None


FACT_BASE_PATH = Path("data/fact_base.json")

_fact_base_cache: Optional[Dict] = None
_fact_base_loaded_at: Optional[datetime] = None


def load_fact_base() -> Dict[str, Any]:
    """Load fact base with caching."""
    global _fact_base_cache, _fact_base_loaded_at
    
    if _fact_base_cache is not None:
        file_mtime = datetime.fromtimestamp(FACT_BASE_PATH.stat().st_mtime)
        if _fact_base_loaded_at and file_mtime <= _fact_base_loaded_at:
            return _fact_base_cache
    
    with open(FACT_BASE_PATH, "r") as f:
        _fact_base_cache = json.load(f)
    _fact_base_loaded_at = datetime.now()
    
    if _fact_base_cache is None:
        return {}
    return _fact_base_cache


METRIC_TO_FACTBASE_KEY = {
    "arr": "arr",
    "mrr": None,
    "revenue": "revenue",
    "services_revenue": None,
    "ar": "ar",
    "dso": None,
    "burn_rate": None,
    "gross_margin": "gross_margin_pct",
    "pipeline": "pipeline",
    "win_rate": "win_rate",
    "churn_rate": "churn_pct",
    "nrr": "nrr",
    "throughput": None,
    "cycle_time": None,
    "sla_compliance": None,
    "deploy_frequency": "deploys_per_week",
    "mttr": "mttr_p1_hours",
    "uptime": "uptime_pct",
    "slo_attainment": None,
    "cloud_spend": "cloud_spend",
}

METRIC_UNIT_MAP = {
    "arr": "USD (millions)",
    "mrr": "USD (millions)",
    "revenue": "USD (millions)",
    "services_revenue": "USD (millions)",
    "ar": "USD (millions)",
    "dso": "days",
    "burn_rate": "USD (millions)",
    "gross_margin": "percent",
    "pipeline": "USD (millions)",
    "win_rate": "percent",
    "churn_rate": "percent",
    "nrr": "percent",
    "throughput": "count",
    "cycle_time": "days",
    "sla_compliance": "percent",
    "deploy_frequency": "count/week",
    "mttr": "hours",
    "uptime": "percent",
    "slo_attainment": "percent",
    "cloud_spend": "USD (millions)",
}

DIMENSION_TO_FACTBASE_KEY = {
    "region": {
        "revenue": "revenue_by_region",
        "pipeline": "pipeline_by_region",
        "ebitda": "ebitda_by_region",
    },
    "segment": {
        "revenue": "revenue_by_segment",
        "csat": "csat_by_segment",
        "implementation": "implementation_by_segment",
    },
    "product": {
        "revenue": "revenue_by_product",
    },
    "stage": {
        "pipeline": "pipeline_by_stage",
    },
    "rep": {
        "pipeline": "pipeline_by_rep",
        "win_rate": "win_rate_by_rep",
        "quota": "quota_by_rep",
    },
    "team": {
        "velocity": "velocity_by_team",
        "engineering": "engineering_by_team",
    },
    "department": {
        "headcount": "headcount_by_department",
        "attrition": "attrition_by_department",
        "engagement": "engagement_by_department",
        "time_to_fill": "time_to_fill_by_department",
    },
    "service": {
        "incidents": "incidents_by_service",
    },
    "category": {
        "cloud_spend": "cloud_spend_by_category",
        "support_tickets": "support_tickets_by_category",
    },
    "tier": {
        "support_tickets": "support_tickets_by_tier",
    },
}


def validate_query(request: QueryRequest) -> Optional[QueryError]:
    """Validate query against semantic catalog."""
    metric_def = resolve_metric(request.metric)
    if metric_def is None:
        available = [m.id for m in PUBLISHED_METRICS]
        return QueryError(
            error=f"Metric '{request.metric}' not found",
            code="METRIC_NOT_FOUND",
            details={"available_metrics": available}
        )
    
    for dim in request.dimensions:
        if dim not in metric_def.allowed_dims:
            return QueryError(
                error=f"Dimension '{dim}' not valid for metric '{request.metric}'",
                code="INVALID_DIMENSION",
                details={
                    "metric": request.metric,
                    "requested_dimension": dim,
                    "valid_dimensions": metric_def.allowed_dims
                }
            )
    
    if request.grain:
        if request.grain not in metric_def.allowed_grains:
            return QueryError(
                error=f"Grain '{request.grain}' not valid for metric '{request.metric}'",
                code="INVALID_GRAIN",
                details={
                    "metric": request.metric,
                    "requested_grain": request.grain,
                    "valid_grains": metric_def.allowed_grains
                }
            )
    
    return None


def filter_periods(data: Dict, time_range: Optional[Dict[str, str]]) -> List[str]:
    """Get list of periods that match the time range filter."""
    fb = load_fact_base()
    all_periods = [q["period"] for q in fb.get("quarterly", [])]
    
    if not time_range:
        return all_periods
    
    start = time_range.get("start", "")
    end = time_range.get("end", "")
    
    filtered = []
    for period in all_periods:
        if start and period < start:
            continue
        if end and period > end:
            continue
        filtered.append(period)
    
    return filtered if filtered else all_periods


def execute_query(request: QueryRequest) -> QueryResponse:
    """Execute a validated query against the fact base."""
    fb = load_fact_base()
    metric_def = resolve_metric(request.metric)
    
    if metric_def is None:
        raise ValueError(f"Metric '{request.metric}' not found")
    
    grain = request.grain or metric_def.default_grain or "quarter"
    unit = METRIC_UNIT_MAP.get(request.metric, "unknown")
    
    periods = filter_periods(fb, request.time_range)
    data_points: List[QueryDataPoint] = []
    
    if request.dimensions:
        dim = request.dimensions[0]
        
        dim_key = None
        if dim in DIMENSION_TO_FACTBASE_KEY:
            metric_dims = DIMENSION_TO_FACTBASE_KEY[dim]
            if request.metric in metric_dims:
                dim_key = metric_dims[request.metric]
            elif "revenue" in metric_dims and request.metric in ["arr", "mrr", "revenue"]:
                dim_key = metric_dims["revenue"]
            elif "pipeline" in metric_dims and request.metric == "pipeline":
                dim_key = metric_dims["pipeline"]
        
        if dim_key and dim_key in fb:
            dim_data = fb[dim_key]
            for period in periods:
                if period in dim_data:
                    for dim_value, value in dim_data[period].items():
                        if request.filters:
                            filter_val = request.filters.get(dim)
                            if filter_val:
                                if isinstance(filter_val, list) and dim_value not in filter_val:
                                    continue
                                elif isinstance(filter_val, str) and dim_value != filter_val:
                                    continue
                        
                        data_points.append(QueryDataPoint(
                            period=period,
                            value=float(value),
                            dimensions={dim: dim_value}
                        ))
        else:
            fb_key = METRIC_TO_FACTBASE_KEY.get(request.metric)
            for q in fb.get("quarterly", []):
                if q["period"] in periods:
                    if fb_key and fb_key in q:
                        data_points.append(QueryDataPoint(
                            period=q["period"],
                            value=float(q[fb_key]),
                            dimensions={}
                        ))
    else:
        fb_key = METRIC_TO_FACTBASE_KEY.get(request.metric)
        for q in fb.get("quarterly", []):
            if q["period"] in periods:
                if fb_key and fb_key in q:
                    data_points.append(QueryDataPoint(
                        period=q["period"],
                        value=float(q[fb_key]),
                        dimensions={}
                    ))
    
    mode = get_current_mode()
    
    return QueryResponse(
        metric=request.metric,
        metric_name=metric_def.name,
        dimensions=request.dimensions,
        grain=grain,
        unit=unit,
        data=data_points,
        metadata=QueryMetadata(
            sources=["demo"] if mode.data_mode == "Demo" else ["farm"],
            freshness=datetime.utcnow().isoformat() + "Z",
            quality_score=1.0,
            mode=mode.data_mode,
            record_count=len(data_points)
        )
    )


def handle_query(request: QueryRequest) -> Union[QueryResponse, QueryError]:
    """Main entry point for query handling."""
    error = validate_query(request)
    if error:
        return error
    
    return execute_query(request)
