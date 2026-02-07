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
    order_by: Optional[str] = None
    limit: Optional[int] = None
    persona: Optional[str] = None
    entity: Optional[str] = None


class QueryDataPoint(BaseModel):
    """Single data point in query results."""
    period: str
    value: float
    dimensions: Dict[str, str] = Field(default_factory=dict)
    rank: Optional[int] = None


class ProvenanceInfo(BaseModel):
    """Provenance information in query response."""
    source_system: str
    freshness: str
    quality_score: float

class EntityInfo(BaseModel):
    """Entity resolution information in query response."""
    resolved_name: str
    candidates: List[str] = Field(default_factory=list)
    confidence: float = 1.0
    match_type: str = "exact"

class ConflictInfo(BaseModel):
    """Conflict information in query response."""
    systems: List[str]
    values: Dict[str, Any]
    root_cause: str
    severity: float
    trust_recommendation: str

class TemporalWarningInfo(BaseModel):
    """Temporal warning information in query response."""
    metric: str
    change_date: str
    old_definition: str
    new_definition: str
    message: str

class QueryMetadata(BaseModel):
    """Metadata about the query execution."""
    sources: List[str]
    freshness: str
    quality_score: float
    mode: str
    record_count: int
    total_count: Optional[int] = None
    ranking_type: Optional[str] = None
    order: Optional[str] = None
    persona: Optional[str] = None
    persona_definition: Optional[str] = None


class QueryResponse(BaseModel):
    """Response model for DCL queries."""
    metric: str
    metric_name: str
    dimensions: List[str]
    grain: str
    unit: str
    data: List[QueryDataPoint]
    metadata: QueryMetadata
    provenance: Optional[List[ProvenanceInfo]] = None
    entity: Optional[EntityInfo] = None
    conflicts: Optional[List[ConflictInfo]] = None
    temporal_warning: Optional[TemporalWarningInfo] = None


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
    "mrr": "mrr",
    "revenue": "revenue",
    "services_revenue": "services_revenue",
    "ar": "ar",
    "dso": "dso",
    "burn_rate": "burn_rate",
    "gross_margin": "gross_profit",
    "ar_aging": "ar_aging",
    "pipeline": "pipeline",
    "pipeline_value": "pipeline",
    "win_rate": "win_rate",
    "quota_attainment": "quota",
    "churn_rate": "churn_pct",
    "churn_risk": "churn_risk",
    "nrr": "nrr",
    "nrr_by_cohort": "nrr_by_cohort",
    "throughput": "throughput",
    "cycle_time": "cycle_time",
    "sla_compliance": "sla_compliance",
    "deploy_frequency": "deploys_per_week",
    "mttr": "mttr_p1_hours",
    "uptime": "uptime_pct",
    "slo_attainment": "slo_attainment",
    "cloud_spend": "cloud_spend",
    "cloud_cost": "cloud_spend",
    "headcount": "headcount",
    "attrition_rate": "attrition_rate",
    "time_to_fill": "time_to_fill_days",
    "engagement_score": "engagement_score",
    "compensation_ratio": None,
    "training_hours": "training_hours_per_employee",
    "promotion_rate": None,
    "diversity_index": None,
    "offer_acceptance_rate": "offer_acceptance_rate",
    "internal_mobility_rate": "internal_mobility_rate",
    "span_of_control": "span_of_control",
    "enps": "enps",
    "customers": "customer_count",
}

METRIC_UNIT_MAP = {
    "arr": "USD (millions)",
    "mrr": "USD (millions)",
    "revenue": "USD (millions)",
    "services_revenue": "USD (millions)",
    "ar": "USD (millions)",
    "ar_aging": "USD (millions)",
    "dso": "days",
    "burn_rate": "USD (millions)",
    "gross_margin": "percent",
    "pipeline": "USD (millions)",
    "pipeline_value": "USD (millions)",
    "win_rate": "percent",
    "churn_rate": "percent",
    "churn_risk": "score (0-100)",
    "nrr": "percent",
    "nrr_by_cohort": "%",
    "throughput": "count",
    "cycle_time": "days",
    "sla_compliance": "percent",
    "deploy_frequency": "count/week",
    "mttr": "hours",
    "uptime": "percent",
    "slo_attainment": "percent",
    "cloud_spend": "USD (millions)",
    "cloud_cost": "USD (millions)",
    "headcount": "count",
    "attrition_rate": "percent",
    "time_to_fill": "days",
    "engagement_score": "percent",
    "compensation_ratio": "percent",
    "training_hours": "hours",
    "promotion_rate": "percent",
    "diversity_index": "percent",
    "offer_acceptance_rate": "percent",
    "internal_mobility_rate": "percent",
    "span_of_control": "ratio",
    "enps": "score",
    "customers": "count",
    "quota_attainment": "percent",
}

DIMENSION_TO_FACTBASE_KEY = {
    "region": {
        "revenue": "revenue_by_region",
        "pipeline": "pipeline_by_region",
        "ebitda": "ebitda_by_region",
        "services_revenue": "services_revenue_by_region",
        "win_rate": "win_rate_by_region",
    },
    "segment": {
        "revenue": "revenue_by_segment",
        "csat": "csat_by_segment",
        "implementation": "implementation_by_segment",
        "mrr": "mrr_by_segment",
        "dso": "dso_by_segment",
        "churn_rate": "churn_by_segment",
        "gross_margin": "gross_margin_by_product",
    },
    "product": {
        "revenue": "revenue_by_product",
        "gross_margin": "gross_margin_by_product",
        "nrr": "nrr_by_product",
    },
    "stage": {
        "pipeline": "pipeline_by_stage",
    },
    "rep": {
        "pipeline": "pipeline_by_rep",
        "win_rate": "win_rate_by_rep",
        "quota_attainment": "quota_by_rep",
        "quota": "quota_by_rep",
    },
    "team": {
        "velocity": "velocity_by_team",
        "engineering": "engineering_by_team",
        "headcount": "headcount_by_team",
        "attrition_rate": "attrition_by_team",
        "engagement_score": "engagement_by_team",
        "throughput": "throughput_by_team",
        "sla_compliance": "sla_compliance_by_team",
    },
    "department": {
        "headcount": "headcount_by_department",
        "attrition_rate": "attrition_by_department",
        "engagement_score": "engagement_by_department",
        "time_to_fill": "time_to_fill_by_department",
        "training_hours": "training_by_department",
    },
    "service": {
        "incidents": "incidents_by_service",
        "deploy_frequency": "deploy_frequency_by_service",
        "uptime": "uptime_by_service",
        "slo_attainment": "slo_attainment_by_service",
        "mttr": "mttr_by_service",
    },
    "category": {
        "cloud_spend": "cloud_spend_by_category",
        "support_tickets": "support_tickets_by_category",
    },
    "tier": {
        "support_tickets": "support_tickets_by_tier",
    },
    "aging_bucket": {
        "ar_aging": "ar_aging",
    },
    "cohort": {
        "nrr": "nrr_by_cohort",
        "nrr_by_cohort": "nrr_by_cohort",
    },
    "customer": {
        "churn_risk": "churn_risk_by_customer",
    },
    "project_type": {
        "cycle_time": "cycle_time_by_project_type",
    },
    "severity": {
        "mttr": "mttr_by_severity",
    },
    "resource_type": {
        "cloud_spend": "cloud_spend_by_resource",
        "cloud_cost": "cloud_spend_by_resource",
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


NESTED_VALUE_KEY_MAP: Dict[tuple, str] = {
    ("quota_attainment", "quota_by_rep"): "attainment_pct",
    ("pipeline", "pipeline_by_rep"): "pipeline",
}


def _get_value_key_for_metric(metric: str, dim_key: str) -> str:
    """Get the value field name for a metric in array-format dimensional data."""
    VALUE_KEY_MAP = {
        "mrr_by_segment": "mrr",
        "dso_by_segment": "dso",
        "ar_aging": "amount",
        "services_revenue_by_region": "revenue",
        "gross_margin_by_product": "gross_margin",
        "win_rate_by_region": "win_rate",
        "nrr_by_cohort": "nrr",
        "nrr_by_product": "nrr",
        "churn_by_segment": "churn_pct",
        "churn_risk_by_customer": "churn_risk",
        "throughput_by_team": "throughput",
        "cycle_time_by_project_type": "cycle_time",
        "sla_compliance_by_team": "sla_compliance",
        "deploy_frequency_by_service": "deploy_frequency",
        "mttr_by_severity": "mttr",
        "mttr_by_service": "mttr",
        "uptime_by_service": "uptime",
        "cloud_spend_by_resource": "cloud_spend",
        "slo_attainment_by_service": "slo_attainment",
    }
    return VALUE_KEY_MAP.get(dim_key, metric)


def _extract_value(metric: str, dim_key: str, raw_value: Any) -> float:
    """Extract numeric value from raw data, handling nested dicts."""
    if isinstance(raw_value, dict):
        nested_key = NESTED_VALUE_KEY_MAP.get((metric, dim_key))
        if nested_key and nested_key in raw_value:
            return float(raw_value[nested_key])
        if metric in raw_value:
            return float(raw_value[metric])
        return float(list(raw_value.values())[0])
    return float(raw_value)


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
            if isinstance(dim_data, list):
                value_key = _get_value_key_for_metric(request.metric, dim_key)
                for record in dim_data:
                    if record.get("period") not in periods:
                        continue
                    dim_value = record.get(dim)
                    value = record.get(value_key, record.get(request.metric))
                    if dim_value is not None and value is not None:
                        if request.filters:
                            filter_val = request.filters.get(dim)
                            if filter_val:
                                if isinstance(filter_val, list) and dim_value not in filter_val:
                                    continue
                                elif isinstance(filter_val, str) and dim_value != filter_val:
                                    continue
                        data_points.append(QueryDataPoint(
                            period=record["period"],
                            value=float(value),
                            dimensions={dim: dim_value}
                        ))
            else:
                for period in periods:
                    if period in dim_data:
                        for dim_value, raw_value in dim_data[period].items():
                            if request.filters:
                                filter_val = request.filters.get(dim)
                                if filter_val:
                                    if isinstance(filter_val, list) and dim_value not in filter_val:
                                        continue
                                    elif isinstance(filter_val, str) and dim_value != filter_val:
                                        continue

                            data_points.append(QueryDataPoint(
                                period=period,
                                value=_extract_value(request.metric, dim_key, raw_value),
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

    # Apply persona-contextual definitions
    persona_label = None
    persona_definition_text = None
    if request.persona:
        from backend.engine.persona_definitions import get_persona_definition_store
        pcd_store = get_persona_definition_store()
        persona_label = request.persona.upper()

        pcd_def = pcd_store.get_definition(request.metric, persona_label)
        if pcd_def:
            persona_definition_text = pcd_def.definition

            # Apply value adjustments
            if pcd_def.value_override is not None:
                # For override metrics like "customers", replace all data points
                if data_points:
                    for dp in data_points:
                        dp.value = pcd_def.value_override
                else:
                    data_points = [QueryDataPoint(
                        period="current",
                        value=pcd_def.value_override,
                        dimensions={},
                    )]
            elif pcd_def.value_multiplier is not None and pcd_def.value_multiplier != 1.0:
                for dp in data_points:
                    dp.value = round(dp.value * pcd_def.value_multiplier, 2)

    mode = get_current_mode()

    total_count = len(data_points)
    ranking_type = None
    order = None

    if request.order_by:
        order = request.order_by.lower()
        reverse = order == "desc"
        data_points.sort(key=lambda dp: dp.value, reverse=reverse)

        for i, dp in enumerate(data_points):
            dp.rank = i + 1

        if request.limit:
            if request.limit == 1:
                ranking_type = "max" if reverse else "min"
            else:
                ranking_type = "top_n" if reverse else "bottom_n"
            data_points = data_points[:request.limit]

    # Build enriched response fields
    # Provenance
    provenance_info = None
    try:
        from backend.engine.provenance_service import get_provenance
        trace = get_provenance(request.metric)
        if trace and trace.sources:
            provenance_info = [
                ProvenanceInfo(
                    source_system=s.source_system,
                    freshness=s.freshness,
                    quality_score=s.quality_score,
                )
                for s in trace.sources
            ]
    except Exception:
        pass

    # Entity resolution
    entity_info = None
    if request.entity:
        try:
            from backend.engine.entity_resolution import get_entity_store
            er_store = get_entity_store()
            results = er_store.browse_entities(request.entity)
            if results:
                confirmed = [r for r in results if r.get("match_status") == "confirmed"]
                entity_info = EntityInfo(
                    resolved_name=confirmed[0]["name"] if confirmed else results[0]["name"],
                    candidates=[r["name"] for r in results],
                    confidence=confirmed[0].get("confidence", 1.0) if confirmed else results[0].get("confidence", 0.5),
                    match_type="confirmed" if confirmed else "candidate",
                )
        except Exception:
            pass

    # Conflict info
    conflicts_info = None
    if request.entity:
        try:
            from backend.engine.conflict_detection import get_conflict_store
            cd_store = get_conflict_store()
            active = cd_store.get_active_conflicts()
            entity_conflicts = [
                c for c in active
                if request.entity.lower() in c.entity_name.lower()
                and c.metric == request.metric
            ]
            if entity_conflicts:
                conflicts_info = [
                    ConflictInfo(
                        systems=[v.source_system for v in c.values],
                        values={v.source_system: v.value for v in c.values},
                        root_cause=c.root_cause,
                        severity=c.severity,
                        trust_recommendation=c.trust_recommendation.get("system", "unknown"),
                    )
                    for c in entity_conflicts
                ]
        except Exception:
            pass

    # Temporal warning
    temporal_warning = None
    if request.time_range:
        try:
            from backend.engine.temporal_versioning import get_temporal_store
            tv_store = get_temporal_store()
            warning = tv_store.check_temporal_warning(request.metric, request.time_range)
            if warning:
                temporal_warning = TemporalWarningInfo(
                    metric=warning.metric,
                    change_date=warning.change_date,
                    old_definition=warning.old_definition,
                    new_definition=warning.new_definition,
                    message=warning.message,
                )
        except Exception:
            pass

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
            record_count=len(data_points),
            total_count=total_count if request.order_by else None,
            ranking_type=ranking_type,
            order=order,
            persona=persona_label,
            persona_definition=persona_definition_text,
        ),
        provenance=provenance_info,
        entity=entity_info,
        conflicts=conflicts_info,
        temporal_warning=temporal_warning,
    )


def handle_query(request: QueryRequest) -> Union[QueryResponse, QueryError]:
    """Main entry point for query handling."""
    error = validate_query(request)
    if error:
        return error
    
    return execute_query(request)
