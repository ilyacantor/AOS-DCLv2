"""
DCL Query Endpoint - executes data queries against the fact base.

This module handles:
- Query validation against the semantic catalog
- Data retrieval from fact_base.json (Demo mode) OR the ingest buffer (Runner-pushed data)
- Filtering and aggregation based on dimensions and time ranges

Data path priority:
  1. Ingest buffer (rows pushed by AAM Runners via POST /api/dcl/ingest)
  2. fact_base.json (static seed data)
  When ingested rows exist for the requested metric, they take priority.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from pydantic import BaseModel, Field

from backend.api.semantic_export import PUBLISHED_METRICS, resolve_metric
from backend.core.mode_state import get_current_mode, get_data_mode


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
    data_mode: Optional[str] = None
    tenant_id: Optional[str] = None
    # WS1.3: Entity tagging across pipeline
    entity_id: Optional[str] = None    # filter to a specific entity's data
    consolidate: bool = False          # if True, sum across entities; if False (default), return per-entity


class QueryDataPoint(BaseModel):
    """Single data point in query results."""
    period: str
    value: float
    dimensions: Dict[str, str] = Field(default_factory=dict)
    rank: Optional[int] = None
    entity_id: Optional[str] = None  # WS1.3: entity provenance for this data point


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
    severity: str
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
    source: str = "fact_base"
    run_id: Optional[str] = None
    entity_id: Optional[str] = None
    tenant_id: str = "default"
    snapshot_name: Optional[str] = None
    run_timestamp: Optional[str] = None
    total_count: Optional[int] = None
    ranking_type: Optional[str] = None
    order: Optional[str] = None
    persona: Optional[str] = None
    persona_definition: Optional[str] = None
    error: Optional[str] = None


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


FACT_BASE_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "fact_base.json"
DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"

_fact_base_cache: Optional[Dict] = None
_fact_base_loaded_at: Optional[datetime] = None

# WS1.3: Per-entity fact_base caches
_entity_fact_base_cache: Dict[str, Dict] = {}
_entity_fact_base_loaded_at: Dict[str, datetime] = {}

_query_logger = logging.getLogger(__name__)


def _should_use_fact_base(request_data_mode: Optional[str]) -> bool:
    """Determine if fact_base.json should be used for this query.

    fact_base is ONLY served when:
    1. Global mode is "Demo" AND no explicit data_mode override, OR
    2. Caller explicitly requests data_mode="demo"

    In ALL other cases (Ingest/Farm/AAM), fact_base is forbidden.
    """
    explicit = (request_data_mode or "").lower()
    if explicit == "demo":
        return True
    if explicit == "live":
        return False
    # No explicit override — consult global mode
    return get_data_mode() == "Demo"


def load_fact_base(entity_id: Optional[str] = None) -> Dict[str, Any]:
    """Load fact base with caching.

    WS1.3 Entity-scoped loading:
    - entity_id=None  → load default data/fact_base.json (backward compatible)
    - entity_id="xyz" → load data/fact_base_xyz.json if it exists, else fall back
                         to data/fact_base.json (single-entity default)

    The default fact_base.json represents the single-entity data in demo mode.
    Entity-specific files (fact_base_{entity_id}.json) are loaded when they exist.
    """
    if entity_id is None:
        # Default path: original behavior, no entity scoping
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

    # WS1.3: Entity-specific fact_base loading
    entity_path = DATA_DIR / f"fact_base_{entity_id}.json"
    if not entity_path.exists():
        _query_logger.warning(f"fact_base_{entity_id}.json not found — falling back to default fact_base.json")
        return load_fact_base(entity_id=None)

    cached = _entity_fact_base_cache.get(entity_id)
    if cached is not None:
        file_mtime = datetime.fromtimestamp(entity_path.stat().st_mtime)
        loaded_at = _entity_fact_base_loaded_at.get(entity_id)
        if loaded_at and file_mtime <= loaded_at:
            return cached

    with open(entity_path, "r") as f:
        data = json.load(f)
    _entity_fact_base_cache[entity_id] = data
    _entity_fact_base_loaded_at[entity_id] = datetime.now()
    return data if data else {}


def get_all_entity_ids() -> List[str]:
    """Return all available entity IDs by scanning for fact_base_{entity_id}.json files.

    WS1.3: Used when consolidate=False and entity_id=None to discover all entities.
    Returns empty list if only the default fact_base.json exists (single-entity mode).
    """
    entity_ids = []
    for path in DATA_DIR.glob("fact_base_*.json"):
        # Extract entity_id from fact_base_{entity_id}.json
        stem = path.stem  # e.g. "fact_base_entityname"
        entity_id = stem[len("fact_base_"):]
        if entity_id:
            entity_ids.append(entity_id)
    return sorted(entity_ids)


# ---------------------------------------------------------------------------
# Data-driven metric-to-factbase key mapping
#
# Instead of a hardcoded whitelist, we build the mapping at module load from:
#   1. The fact_base.json quarterly keys (what data actually exists)
#   2. The metrics.yaml catalog IDs (what the semantic layer publishes)
#   3. Explicit overrides for the handful of cases where the catalog metric
#      ID intentionally differs from the fact_base column name.
#
# Any metric whose catalog ID matches a fact_base key directly needs no
# explicit entry — the identity mapping is applied automatically.
# ---------------------------------------------------------------------------

# Explicit overrides: catalog metric_id -> fact_base quarterly key
# Only needed when the catalog name differs from the v5.0 fact_base key.
# Metrics whose catalog ID matches the fact_base key use identity mapping
# (handled by _build_factbase_key_map).
_FACTBASE_KEY_OVERRIDES: Dict[str, Optional[str]] = {
    "churn_rate_pct": "gross_churn_pct",
    "pipeline_value": "pipeline",
    "mttr": "mttr_p1_hours",
    "training_hours": "training_hours_per_employee",
    "customers": "customer_count",
    # Ground truth uses short names; fact_base uses suffixed names
    "attrition_rate": "attrition_rate_pct",
    "cfo": "cash_from_operations",
    "quota_attainment": "quota_attainment_pct",
    "win_rate": "win_rate_pct",
}


def _build_factbase_key_map() -> Dict[str, Optional[str]]:
    """Build the complete metric_id -> fact_base key mapping.

    Strategy:
      1. Start with explicit overrides for known mismatches.
      2. For every catalog metric not yet mapped, check if its ID
         appears directly as a quarterly key in fact_base — if so,
         use identity mapping.
      3. Metrics with no fact_base data get None (no data to serve).
    """
    fb = load_fact_base()
    quarterly = fb.get("quarterly", [])
    fb_keys = set()
    if quarterly:
        fb_keys = set(k for k in quarterly[0].keys()
                      if k not in ("year", "quarter", "period"))

    mapping: Dict[str, Optional[str]] = dict(_FACTBASE_KEY_OVERRIDES)

    for metric_def in PUBLISHED_METRICS:
        mid = metric_def.id
        if mid in mapping:
            continue  # already has an explicit override
        if mid in fb_keys:
            mapping[mid] = mid  # identity mapping
        else:
            mapping[mid] = None  # no fact_base data for this metric

    return mapping


METRIC_TO_FACTBASE_KEY: Dict[str, Optional[str]] = _build_factbase_key_map()


# ---------------------------------------------------------------------------
# Unit resolution — read from the metric catalog, not a hardcoded map.
# The canonical unit string comes from metrics.yaml via MetricDefinition.
# ---------------------------------------------------------------------------

_UNIT_DISPLAY: Dict[str, str] = {
    "usd_millions": "USD (millions)",
    "pct": "percent",
    "count": "count",
    "days": "days",
    "hours": "hours",
    "minutes": "minutes",
    "ratio": "ratio",
    "score": "score",
    "index": "index",
    "per_week": "count/week",
}


def _resolve_unit(metric_def: Any) -> str:
    """Resolve the display unit from a MetricDefinition's unit field."""
    raw = getattr(metric_def, "unit", None)
    if raw:
        return _UNIT_DISPLAY.get(raw, raw)
    return "unknown"

DIMENSION_TO_FACTBASE_KEY = {
    "region": {
        "revenue": "revenue_by_region",
        "pipeline": "pipeline_by_region",
        "ebitda": "ebitda_by_region",
        "services_revenue": "services_revenue_by_region",
        "win_rate": "win_rate_by_region",
        "win_rate_pct": "win_rate_by_region",
    },
    "segment": {
        "revenue": "revenue_by_segment",
        "csat": "csat_by_segment",
        "implementation": "implementation_by_segment",
        "mrr": "mrr_by_segment",
        "dso": "dso_by_segment",
        "churn_rate": "churn_by_segment",
        "churn_rate_pct": "churn_by_segment",
        "gross_margin": "gross_margin_by_product",
        "gross_margin_pct": "gross_margin_by_product",
    },
    "product": {
        "revenue": "revenue_by_product",
        "gross_margin": "gross_margin_by_product",
        "gross_margin_pct": "gross_margin_by_product",
        "nrr": "nrr_by_product",
    },
    "stage": {
        "pipeline": "pipeline_by_stage",
        "pipeline_value": "pipeline_by_stage",
    },
    "rep": {
        "pipeline": "pipeline_by_rep",
        "win_rate": "win_rate_by_rep",
        "win_rate_pct": "win_rate_by_rep",
        "quota_attainment": "quota_by_rep",
        "quota_attainment_pct": "quota_by_rep",
        "quota": "quota_by_rep",
    },
    "team": {
        "velocity": "velocity_by_team",
        "engineering": "engineering_by_team",
        "headcount": "headcount_by_team",
        "attrition_rate": "attrition_by_team",
        "attrition_rate_pct": "attrition_by_team",
        "engagement_score": "engagement_by_team",
        "throughput": "throughput_by_team",
        "sla_compliance": "sla_compliance_by_team",
        "sla_compliance_pct": "sla_compliance_by_team",
    },
    "department": {
        "headcount": "headcount_by_department",
        "attrition_rate": "attrition_by_department",
        "attrition_rate_pct": "attrition_by_department",
        "engagement_score": "engagement_by_department",
        "time_to_fill": "time_to_fill_by_department",
        "enps": "enps_by_department",
        "training_hours": "training_by_department",
    },
    "service": {
        "incidents": "incidents_by_service",
        "deploy_frequency": "deploy_frequency_by_service",
        "uptime": "uptime_by_service",
        "uptime_pct": "uptime_by_service",
        "slo_attainment": "slo_attainment_by_service",
        "slo_attainment_pct": "slo_attainment_by_service",
        "mttr": "mttr_by_service",
    },
    "category": {
        "cloud_spend": "cloud_spend_by_resource_type",
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
        "cloud_spend": "cloud_spend_by_resource_type",
        "cloud_cost": "cloud_spend_by_resource_type",
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


def _period_in_range(period: str, start: str, end: str) -> bool:
    """Check if a period (e.g. '2025-Q1') falls within start/end range.

    Handles year-only boundaries correctly: when start or end is a 4-digit
    year string (e.g. '2025'), comparison is done by year extraction so that
    '2025-Q1' is correctly matched as within year '2025'.  Plain string
    comparison fails here because '2025-Q1' > '2025' due to the dash.
    """
    period_year = period[:4]
    if start:
        if len(start) == 4 and start.isdigit():
            if period_year < start:
                return False
        elif period < start:
            return False
    if end:
        if len(end) == 4 and end.isdigit():
            if period_year > end:
                return False
        elif period > end:
            return False
    return True


def filter_periods(data: Dict, time_range: Optional[Dict[str, str]]) -> List[str]:
    """Get list of periods that match the time range filter."""
    fb = load_fact_base()
    all_periods = [q["period"] for q in fb.get("quarterly", [])]

    if not time_range:
        return all_periods

    start = time_range.get("start", "")
    end = time_range.get("end", "")

    filtered = [p for p in all_periods if _period_in_range(p, start, end)]

    return filtered if filtered else all_periods


NESTED_VALUE_KEY_MAP: Dict[tuple, str] = {
    ("quota_attainment", "quota_by_rep"): "attainment_pct",
    ("quota_attainment_pct", "quota_by_rep"): "attainment_pct",
    ("pipeline", "pipeline_by_rep"): "pipeline",
    ("pipeline_value", "pipeline_by_rep"): "pipeline",
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
        "quota_by_rep": "quota_attainment_pct",
        "pipeline_by_rep": "pipeline",
        "win_rate_by_rep": "win_rate",
    }
    return VALUE_KEY_MAP.get(dim_key, metric)


# Dimension name → record field name for array-format fact_base data
# (e.g., dimension "rep" → field "rep_name" in quota_by_rep records)
_DIM_TO_RECORD_FIELD: Dict[str, str] = {
    "rep": "rep_name",
    "team": "team",
    "stage": "stage",
    "product": "product",
    "segment": "segment",
    "region": "region",
}


def _get_dim_field(dim: str) -> str:
    """Get the record field name for a dimension."""
    return _DIM_TO_RECORD_FIELD.get(dim, dim)


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


def _query_ingest_store(
    metric: str,
    dimensions: List[str],
    filters: Dict[str, Union[str, List[str]]],
    time_range: Optional[Dict[str, str]],
    tenant_id: Optional[str] = None,
    entity_id: Optional[str] = None,
    consolidate: bool = False,
) -> Tuple[List[QueryDataPoint], Optional["RunReceipt"]]:
    """
    Query the materialized metric data points from the ingest buffer.

    The MetricMaterializer transforms raw source-system rows (Salesforce
    Amount, NetSuite amount, etc.) into canonical metric data points
    using ontology concepts. This function reads those materialized
    points.

    Falls back to direct raw-row scanning if no materialized data
    exists (backward compat for any pre-aggregated rows).

    Returns:
        (data_points, receipt) — receipt is the most-recent run that
        contributed rows, or None if no ingested data matches.
    """
    from backend.api.ingest import get_ingest_store, RunReceipt, get_canonical_sources
    from backend.aam.ingress import normalize_source_id as _norm_src

    import logging as _log
    _logger = _log.getLogger(__name__)

    store = get_ingest_store()
    all_receipts = store.get_all_receipts()
    if not all_receipts:
        return [], None

    # --- Tenant isolation ---
    if tenant_id and tenant_id != "default":
        all_receipts = [r for r in all_receipts if r.tenant_id == tenant_id]
        if not all_receipts:
            return [], None
    elif tenant_id == "default" or not tenant_id:
        unique_tenants = sorted({r.tenant_id for r in all_receipts})
        if len(unique_tenants) == 1:
            _logger.warning(
                f"tenant_id='default' with single tenant '{unique_tenants[0]}' — auto-selecting. "
                f"Set tenant_id explicitly to suppress this warning."
            )
        elif len(unique_tenants) > 1 and entity_id:
            # entity_id provided — query across all tenants and let the
            # entity_id filter (below) narrow the results.  The entity's
            # rows carry _entity_id so they will self-select.
            _logger.info(
                f"Multiple tenants {unique_tenants} but entity_id='{entity_id}' — "
                f"querying across all tenants; entity_id filter will narrow results."
            )
        elif len(unique_tenants) > 1:
            raise ValueError(
                f"Multiple tenants found: {unique_tenants}. "
                f"Specify entity_id or tenant_id to select one."
            )

    # --- Primary path: materialized data points ---
    mat_points = store.get_materialized_points(
        metric=metric,
        dimensions=dimensions if dimensions else None,
        filters=filters if filters else None,
        time_range=time_range,
        tenant_id=tenant_id,
    )

    # Filter to canonical sources only — reject AAM demo data
    if mat_points:
        mat_points = [
            pt for pt in mat_points
            if _norm_src(pt.get("source_system", "")) in get_canonical_sources()
        ]

    # WS1.3: Filter materialized points by entity_id when specified.
    # When entity_id is None, all points pass through (backward compatible).
    # When entity_id IS specified:
    #   - If ANY points have _entity_id set: strict filter (only matching).
    #     Points with _entity_id=None are rejected to prevent cross-entity contamination.
    #   - If ALL points are unscoped (_entity_id=None): pass all through.
    #     This handles legacy/AAM data that predates entity tagging.
    if mat_points and entity_id:
        has_entity_scoped = any(pt.get("_entity_id") is not None for pt in mat_points)
        if has_entity_scoped:
            mat_points = [
                pt for pt in mat_points
                if pt.get("_entity_id") == entity_id
            ]
        # else: all points are unscoped — pass through as-is

    if mat_points:
        # --- Deduplicate across pipeline runs ---
        # Multiple runs produce separate materialized keys (run_id:pipe_id).
        # Without dedup, the same metric/period/source gets summed N times
        # (once per run), inflating values by Nx.
        #
        # Dedup key: (period, source_system, dim_key)
        # If same key appears from multiple runs or pipes, keep the LATEST
        # (by materialized_at timestamp).  Different source_systems for the
        # same metric/period are kept — they represent genuinely different
        # data (e.g. netsuite revenue + salesforce revenue).
        # pipe_id is intentionally excluded: multiple pipes from the same
        # source_system for the same metric/period are duplicates (e.g.,
        # two financial_summary pushes under different pipe_ids).
        _dedup: dict = {}
        for pt in mat_points:
            period = pt.get("period", "current")
            src = pt.get("source_system", "")
            dim_key = tuple(sorted(pt.get("dimensions", {}).items()))
            dedup_key = (period, src, dim_key)
            existing = _dedup.get(dedup_key)
            if existing is None:
                _dedup[dedup_key] = pt
            else:
                # Keep the point with the later materialized_at timestamp
                if pt.get("materialized_at", "") > existing.get("materialized_at", ""):
                    _dedup[dedup_key] = pt
        mat_points = list(_dedup.values())

        # Aggregate across pipes: group by (period, dim_key).
        # Each pipe's materialization already aggregated its own rows;
        # this step combines the same metric from multiple source pipes.
        #
        # Additive metrics (revenue, headcount, etc.) → SUM
        # Non-additive metrics (_pct, _ratio, _score, _days, etc.) → AVERAGE
        from collections import defaultdict as _dd
        _NON_ADDITIVE_UNITS = {"percent", "pct", "ratio", "score", "days", "hours", "months", "index"}
        _metric_unit = None
        try:
            _mdef = resolve_metric(metric)
            if _mdef and _mdef.unit:
                _metric_unit = _mdef.unit.lower()
        except Exception:
            pass
        _is_additive = _metric_unit not in _NON_ADDITIVE_UNITS

        # WS1.3: Determine unique entity IDs in the result set.
        # When consolidate=False and multiple entities exist, group by entity.
        # When consolidate=True or only one entity, aggregate across entities.
        _unique_entities = {pt.get("_entity_id") for pt in mat_points}
        _unique_entities.discard(None)
        _multi_entity = len(_unique_entities) > 1
        _group_by_entity = _multi_entity and not consolidate

        # ── Prevent total+regional double-counting ──────────────────
        # Farm's financial_summary pipe pushes a total row (no dimensions)
        # plus regional rows (with territory dimension) per period.  When
        # the query requests no dimensions, both kinds collapse to the same
        # aggregation key and get summed → 2× the real value.
        #
        # Fix: when no dimensions are requested, prefer undimensioned
        # (pre-aggregated total) points.  Only fall through to dimensioned
        # points if no totals exist for a given period.
        if not dimensions:
            _has_total: set = set()   # periods that have an undimensioned point
            _has_detail: set = set()  # periods that have dimensioned points
            for pt in mat_points:
                period = pt.get("period", "current")
                if pt.get("dimensions"):
                    _has_detail.add(period)
                else:
                    _has_total.add(period)
            # For periods where BOTH exist, drop the dimensioned rows
            _overlapping = _has_total & _has_detail
            if _overlapping:
                mat_points = [
                    pt for pt in mat_points
                    if pt.get("dimensions") is None
                    or not pt.get("dimensions")
                    or pt.get("period", "current") not in _overlapping
                ]

        agg: dict = _dd(float)
        agg_count: dict = _dd(int)
        agg_entity: dict = {}  # track entity_id per aggregation key
        for pt in mat_points:
            if dimensions:
                # Only keep the requested dimensions in the grouping key
                pt_dims = pt.get("dimensions", {})
                dim_vals = {d: pt_dims[d] for d in dimensions if d in pt_dims}
            else:
                # No dimensions requested → aggregate ALL into a single total per period
                dim_vals = {}
            period = pt.get("period", "current")
            pt_entity = pt.get("_entity_id")

            # WS1.3: Include entity_id in grouping key when not consolidating across entities
            if _group_by_entity:
                key = (period, tuple(sorted(dim_vals.items())), pt_entity)
            else:
                key = (period, tuple(sorted(dim_vals.items())), None)
            agg[key] += float(pt["value"])
            agg_count[key] += 1
            # Track entity for single-entity results (carry provenance even when not grouping)
            if pt_entity and key not in agg_entity:
                agg_entity[key] = pt_entity

        if _is_additive:
            data_points = [
                QueryDataPoint(
                    period=k[0], value=round(v, 6), dimensions=dict(k[1]),
                    entity_id=k[2] or agg_entity.get(k),
                )
                for k, v in sorted(agg.items())
            ]
        else:
            # Average for rate/percentage/score metrics
            data_points = [
                QueryDataPoint(
                    period=k[0], value=round(agg[k] / agg_count[k], 6), dimensions=dict(k[1]),
                    entity_id=k[2] or agg_entity.get(k),
                )
                for k in sorted(agg.keys())
            ]

        # Pick the receipt whose source actually contributed data (not just
        # the most-recent receipt from any source).  Materialized points carry
        # source_system — match against receipts.
        _contributing_sources = {pt.get("source_system") for pt in mat_points if pt.get("source_system")}
        _candidate_receipts = [r for r in all_receipts if r.source_system in _contributing_sources]
        if _candidate_receipts:
            contributing_receipt = max(_candidate_receipts, key=lambda r: r.received_at)
        else:
            contributing_receipt = max(all_receipts, key=lambda r: r.received_at)
        return data_points, contributing_receipt

    # --- Fallback: scan raw rows (legacy path) ---
    # Kept for backward compat with any rows that were pushed with
    # canonical field names (e.g. {"revenue": 50.0, "period": "2026-Q4"})
    data_points: List[QueryDataPoint] = []
    contributing_receipt: Optional[RunReceipt] = None

    for receipt in reversed(all_receipts):
        # Skip non-canonical sources in fallback path too
        if _norm_src(receipt.source_system) not in get_canonical_sources():
            continue
        rows = store.get_rows(receipt.run_id, receipt.pipe_id)
        for row in rows:
            if metric not in row:
                continue

            # WS1.3: entity_id filter in fallback path
            if entity_id and row.get("_entity_id") != entity_id:
                continue

            value = row[metric]
            if not isinstance(value, (int, float)):
                continue

            period = row.get("period", "current")

            if time_range:
                start = time_range.get("start", "")
                end = time_range.get("end", "")
                if start and period < start:
                    continue
                if end and period > end:
                    continue

            dim_vals: Dict[str, str] = {}
            skip = False
            for dim in dimensions:
                dv = row.get(dim)
                if dv is None:
                    skip = True
                    break
                dim_vals[dim] = str(dv)

                fv = filters.get(dim)
                if fv:
                    if isinstance(fv, list) and str(dv) not in fv:
                        skip = True
                        break
                    elif isinstance(fv, str) and str(dv) != fv:
                        skip = True
                        break
            if skip:
                continue

            data_points.append(QueryDataPoint(
                period=period,
                value=float(value),
                dimensions=dim_vals,
                entity_id=row.get("_entity_id"),  # WS1.3: carry entity provenance
            ))
            contributing_receipt = receipt

    return data_points, contributing_receipt


def execute_query(request: QueryRequest) -> QueryResponse:
    """Execute a validated query against the fact base or ingest buffer.

    WS1.3 Entity behavior:
    - entity_id=None, single entity → identical to Phase 0 (backward compatible)
    - entity_id="xyz" → load entity-specific fact_base, filter ingest by entity
    - consolidate=False (default) → per-entity results when multiple entities exist
    - consolidate=True → sum across entities (requires explicit opt-in)
    """
    fb = None  # Only loaded when fact_base path is selected
    metric_def = resolve_metric(request.metric)

    if metric_def is None:
        raise ValueError(f"Metric '{request.metric}' not found")

    resolved_id = metric_def.id  # canonical catalog ID after fuzzy resolution
    grain = request.grain or metric_def.default_grain or "quarter"
    unit = _resolve_unit(metric_def)

    use_fact_base = _should_use_fact_base(request.data_mode)

    # ------------------------------------------------------------------
    # Path B: ingest buffer (when NOT in fact_base mode)
    # If Runners have pushed rows containing this metric, serve those
    # and tag the response with the Runner's provenance.
    # ------------------------------------------------------------------
    ingest_receipt = None
    data_points: List[QueryDataPoint] = []

    if not use_fact_base:
        ingested_points, ingest_receipt = _query_ingest_store(
            metric=request.metric,
            dimensions=request.dimensions,
            filters=request.filters,
            time_range=request.time_range,
            tenant_id=request.tenant_id,
            entity_id=request.entity_id,        # WS1.3
            consolidate=request.consolidate,     # WS1.3
        )
        data_points = ingested_points

    # ------------------------------------------------------------------
    # Path B-empty: non-demo mode with no ingested data — fail loud
    # Never silently fall back to fact_base.json outside Demo mode.
    # ------------------------------------------------------------------
    if not data_points and not use_fact_base:
        current_mode = get_data_mode()
        error_msg = (
            f"No ingested data available for metric='{request.metric}', "
            f"entity_id='{request.entity_id}'. "
            f"Global mode is '{current_mode}' — fact_base.json is disabled outside Demo mode. "
            f"Run the pipeline to ingest data, or set data_mode='demo' to use demo data."
        )
        _query_logger.warning(error_msg)
        return QueryResponse(
            metric=request.metric,
            metric_name=metric_def.name,
            dimensions=request.dimensions,
            grain=grain,
            unit=unit,
            data=[],
            metadata=QueryMetadata(
                sources=["ingest"],
                freshness=datetime.utcnow().isoformat() + "Z",
                quality_score=0.0,
                mode=current_mode,
                record_count=0,
                source="no_data_error",
                entity_id=request.entity_id,
                error=error_msg,
            ),
        )

    # ------------------------------------------------------------------
    # Path A: fact_base.json
    # Only used when _should_use_fact_base() returns True (Demo mode
    # or explicit data_mode="demo" override).
    # ------------------------------------------------------------------
    if not data_points:
        ingest_receipt = None   # no ingest provenance to carry
        fb = load_fact_base(entity_id=request.entity_id)
        periods = filter_periods(fb, request.time_range)

        if request.dimensions:
            dim = request.dimensions[0]

            dim_key = None
            if dim in DIMENSION_TO_FACTBASE_KEY:
                metric_dims = DIMENSION_TO_FACTBASE_KEY[dim]
                if resolved_id in metric_dims:
                    dim_key = metric_dims[resolved_id]
                elif request.metric in metric_dims:
                    dim_key = metric_dims[request.metric]
                elif "revenue" in metric_dims and resolved_id in ["arr", "mrr", "revenue"]:
                    dim_key = metric_dims["revenue"]
                elif "pipeline" in metric_dims and resolved_id == "pipeline":
                    dim_key = metric_dims["pipeline"]

            if dim_key and dim_key in fb:
                dim_data = fb[dim_key]
                if isinstance(dim_data, list):
                    value_key = _get_value_key_for_metric(request.metric, dim_key)
                    dim_field = _get_dim_field(dim)
                    for record in dim_data:
                        if record.get("period") not in periods:
                            continue
                        dim_value = record.get(dim_field)
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
                    value_key = _get_value_key_for_metric(request.metric, dim_key)
                    for period in periods:
                        if period not in dim_data:
                            continue
                        period_data = dim_data[period]

                        # Dict-of-lists: {period: [{dim: val, metric: val}, ...]}
                        if isinstance(period_data, list):
                            dim_field = _get_dim_field(dim)
                            for record in period_data:
                                dim_value = record.get(dim_field)
                                value = record.get(value_key, record.get(request.metric))
                                if dim_value is None or value is None:
                                    continue
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
                            # Dict-of-dicts: {period: {dim_value: raw_value}}
                            for dim_value, raw_value in period_data.items():
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
                fb_key = METRIC_TO_FACTBASE_KEY.get(resolved_id)
                for q in fb.get("quarterly", []):
                    if q["period"] in periods:
                        if fb_key and fb_key in q:
                            data_points.append(QueryDataPoint(
                                period=q["period"],
                                value=float(q[fb_key]),
                                dimensions={}
                            ))
        else:
            fb_key = METRIC_TO_FACTBASE_KEY.get(resolved_id)
            for q in fb.get("quarterly", []):
                if q["period"] in periods:
                    if fb_key and fb_key in q:
                        data_points.append(QueryDataPoint(
                            period=q["period"],
                            value=float(q[fb_key]),
                            dimensions={}
                        ))

    # WS1.3: Stamp entity_id on fact_base data points.
    # When entity_id is explicitly requested, all points from that entity's fact_base
    # carry the entity tag. When entity_id is None (single-entity default), points
    # carry no entity_id — identical to Phase 0 behavior.
    if request.entity_id and not ingest_receipt:
        for dp in data_points:
            if dp.entity_id is None:
                dp.entity_id = request.entity_id

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

    data_source_label = "ingest" if ingest_receipt else "fact_base"

    if ingest_receipt:
        run_id = ingest_receipt.run_id
        run_timestamp = ingest_receipt.run_timestamp
        snapshot_name = ingest_receipt.snapshot_name
        tenant_id = ingest_receipt.tenant_id
        source_label = ingest_receipt.source_system
    else:
        fb_meta = fb.get("metadata", {}) if fb is not None else {}
        run_id = mode.last_run_id
        run_timestamp = mode.last_updated
        snapshot_name = f"{mode.data_mode}-v{fb_meta.get('version', 'unknown')}"
        tenant_id = "default"
        source_label = None

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
    provenance_info = []
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
            all_conflicts = cd_store.get_all_conflicts()
            entity_conflicts = [
                c for c in all_conflicts
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
            sources=[source_label] if source_label else (
                ["demo"] if use_fact_base else ["farm"]
            ),
            freshness=datetime.utcnow().isoformat() + "Z",
            quality_score=1.0,
            mode="Ingest" if ingest_receipt else ("Demo" if use_fact_base else mode.data_mode),
            record_count=len(data_points),
            source=data_source_label,
            run_id=run_id,
            entity_id=request.entity_id,
            tenant_id=tenant_id,
            snapshot_name=snapshot_name,
            run_timestamp=run_timestamp,
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

    try:
        return execute_query(request)
    except ValueError as exc:
        return QueryError(
            error=str(exc),
            code="QUERY_ERROR",
        )
