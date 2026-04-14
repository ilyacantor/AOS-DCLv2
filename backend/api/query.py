"""
DCL query endpoint — executes metric queries against the triple store.

Post–store-rebuild, the only read path is ``current_triples`` scoped by
``tenant_id`` (+ optional ``entity_id``). The metric→concept mapping follows
the ConceptRegistry: metric id matches the root segment of ``concept`` (so
metric ``revenue`` hits every triple where ``split_part(concept,'.',1) =
'revenue'``).

Identity contract (I1/I2):
- tenant_id is required. If missing, we try to auto-resolve from entity_id
  or a single-tenant store; on failure we raise ValueError so the FastAPI
  wrapper returns 422.
- No ``run_id`` leaks out — ``QueryMetadata`` carries ``dcl_ingest_id``.
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)

from pydantic import BaseModel, Field

from backend.api.semantic_export import PUBLISHED_METRICS, resolve_metric
from backend.core.db import get_connection
from backend.core.mode_state import get_current_mode
from backend.db.triple_store import TripleStore


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
    confidence_score: Optional[float] = None
    confidence_tier: Optional[str] = None
    mapping_source: Optional[str] = None
    mapping_status: Optional[str] = None


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
    source: str = "ingest"
    dcl_ingest_id: Optional[str] = None
    entity_id: Optional[str] = None
    tenant_id: Optional[str] = None
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
    status: str = "ok"  # "ok", "no_data", "no_results"
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
    enrichment_errors: Optional[Dict[str, str]] = None


class QueryError(BaseModel):
    """Error response for invalid queries."""
    error: str
    code: str
    details: Optional[Dict[str, Any]] = None


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


def validate_query(request: QueryRequest) -> Optional[QueryError]:
    """Validate query against semantic catalog."""
    metric_def = resolve_metric(request.metric)
    if metric_def is None:
        from backend.api.semantic_export import _score_match, PUBLISHED_METRICS as _all_metrics
        scored = []
        for m in _all_metrics:
            s = _score_match(request.metric, m)
            if s > 0:
                # Secondary sort: longest common prefix between query and metric ID
                q_low = request.metric.lower().strip()
                prefix_len = 0
                for a, b in zip(q_low, m.id):
                    if a == b:
                        prefix_len += 1
                    else:
                        break
                scored.append((s, prefix_len, m.id))
        scored.sort(key=lambda x: (x[0], x[1]), reverse=True)
        details: Dict[str, Any] = {}
        if scored:
            details["closest_match"] = scored[0][2]
            details["closest_score"] = scored[0][0]
            details["message"] = f"No exact metric found. Did you mean '{scored[0][2]}'?"
        return QueryError(
            error=f"Metric '{request.metric}' not found",
            code="METRIC_NOT_FOUND",
            details=details or None,
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


_NON_ADDITIVE_UNITS = {
    "percent", "pct", "ratio", "score", "days", "hours", "months", "index",
}


def _resolve_tenant_id(
    explicit_tenant_id: Optional[str],
    entity_id: Optional[str],
) -> str:
    """Resolve tenant_id per I2.

    Raises ValueError (→ 422) when no tenant can be determined. Never returns
    a sentinel string like ``'default'`` — that was the silent-fallback the
    store rebuild removed.
    """
    store = TripleStore()
    if explicit_tenant_id:
        return explicit_tenant_id
    if entity_id:
        return store.resolve_tenant_for_entity(entity_id)
    return store.resolve_single_tenant()


def _tenant_has_triples(tenant_id: str) -> bool:
    """Return True if the tenant has any rows in current_triples.

    Used to distinguish ``no_data`` (store empty for this tenant) from
    ``no_results`` (store populated but the specific query matched zero).
    """
    sql = "SELECT 1 FROM current_triples WHERE tenant_id = %s LIMIT 1"
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (tenant_id,))
            return cur.fetchone() is not None


def _extract_numeric(v: Any) -> Optional[float]:
    """Pull a numeric out of a JSONB value field.

    current_triples.value is JSONB; Farm writes either a raw number or an
    object like {"amount": N}. We tolerate both and return None when the
    payload is non-numeric (e.g. string metadata).
    """
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, dict):
        for k in ("amount", "value", "number"):
            inner = v.get(k)
            if isinstance(inner, (int, float)):
                return float(inner)
        return None
    if isinstance(v, str):
        try:
            return float(v)
        except ValueError:
            return None
    return None


def _query_current_triples(
    metric: str,
    time_range: Optional[Dict[str, str]],
    tenant_id: str,
    entity_id: Optional[str],
    consolidate: bool,
    is_additive: bool,
) -> Tuple[List[QueryDataPoint], Dict[str, Any]]:
    """Read metric data points from current_triples for a resolved tenant.

    Mirrors ``triple_monitor.get_sankey_aggregation``:
    - WHERE tenant_id + ``split_part(concept,'.',1) = metric`` + optional
      entity_id and period range
    - Aggregates by period (and entity_id when multiple entities are
      present and ``consolidate`` is False)
    - SUM for additive metrics; AVG for rate/percentage/score metrics

    Returns ``(data_points, meta)``. ``meta`` carries ``dcl_ingest_id``,
    ``snapshot_name``, ``run_timestamp``, ``source_system``, ``source_list``,
    ``entity_id``, and ``tenant_id``. When zero rows match, returns an empty
    list plus a meta dict with tenant_id/entity_id only.
    """
    where = ["tenant_id = %s", "split_part(concept, '.', 1) = %s"]
    params: List[Any] = [tenant_id, metric]

    if entity_id:
        where.append("entity_id = %s")
        params.append(entity_id)

    if time_range:
        start = (time_range.get("start") or "").strip()
        end = (time_range.get("end") or "").strip()
        if start:
            if len(start) == 4 and start.isdigit():
                where.append("LEFT(period, 4) >= %s")
            else:
                where.append("period >= %s")
            params.append(start)
        if end:
            if len(end) == 4 and end.isdigit():
                where.append("LEFT(period, 4) <= %s")
            else:
                where.append("period <= %s")
            params.append(end)

    sql = (
        "SELECT period, entity_id, source_system, value, "
        "confidence_score, confidence_tier "
        "FROM current_triples WHERE " + " AND ".join(where)
    )

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

    if not rows:
        return [], {"tenant_id": tenant_id, "entity_id": entity_id}

    unique_entities = {r[1] for r in rows if r[1]}
    group_by_entity = len(unique_entities) > 1 and not consolidate

    agg_sum: Dict[Tuple[str, Optional[str]], float] = {}
    agg_count: Dict[Tuple[str, Optional[str]], int] = {}
    agg_conf: Dict[Tuple[str, Optional[str]], Tuple[Optional[float], Optional[str]]] = {}
    sources_by_key: Dict[Tuple[str, Optional[str]], set] = {}

    for period, row_entity, source_system, raw_value, conf_score, conf_tier in rows:
        num = _extract_numeric(raw_value)
        if num is None:
            continue
        period_key = period or "current"
        key = (period_key, row_entity if group_by_entity else None)
        agg_sum[key] = agg_sum.get(key, 0.0) + num
        agg_count[key] = agg_count.get(key, 0) + 1
        score_f = float(conf_score) if conf_score is not None else None
        existing = agg_conf.get(key)
        if existing is None or (
            score_f is not None
            and (existing[0] is None or score_f < existing[0])
        ):
            agg_conf[key] = (score_f, conf_tier)
        if source_system:
            sources_by_key.setdefault(key, set()).add(source_system)

    if not agg_sum:
        return [], {"tenant_id": tenant_id, "entity_id": entity_id}

    data_points: List[QueryDataPoint] = []
    for key in sorted(agg_sum.keys(), key=lambda k: (k[0], k[1] or "")):
        value = agg_sum[key] if is_additive else agg_sum[key] / agg_count[key]
        conf = agg_conf.get(key, (None, None))
        data_points.append(QueryDataPoint(
            period=key[0],
            value=round(value, 6),
            dimensions={},
            entity_id=key[1],
            confidence_score=conf[0],
            confidence_tier=conf[1],
        ))

    meta_sql = (
        "SELECT entity_id, current_run_id::text, current_snapshot_name, "
        "updated_at FROM tenant_runs "
        "WHERE tenant_id::text = %s "
    )
    meta_params: List[Any] = [tenant_id]
    if entity_id:
        meta_sql += "AND entity_id = %s "
        meta_params.append(entity_id)
    meta_sql += "ORDER BY updated_at DESC LIMIT 1"

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(meta_sql, meta_params)
            meta_row = cur.fetchone()

    all_sources: set = set()
    for srcs in sources_by_key.values():
        all_sources.update(srcs)
    source_list = sorted(s for s in all_sources if s)
    primary_source = source_list[0] if source_list else None

    meta: Dict[str, Any] = {
        "tenant_id": tenant_id,
        "entity_id": entity_id,
        "source_system": primary_source,
        "source_list": source_list,
    }
    if meta_row:
        tr_entity, tr_ingest_id, tr_snapshot, tr_updated = meta_row
        meta["entity_id"] = entity_id or tr_entity
        meta["dcl_ingest_id"] = tr_ingest_id
        meta["snapshot_name"] = tr_snapshot
        meta["run_timestamp"] = tr_updated.isoformat() if tr_updated else None
    return data_points, meta


def execute_query(request: QueryRequest) -> QueryResponse:
    """Execute a validated query against ``current_triples``.

    Entity behavior:
    - entity_id=None, single entity → backward compatible
    - entity_id="xyz" → filter to that entity's rows
    - consolidate=False (default) → per-entity results when multiple entities exist
    - consolidate=True → sum across entities (requires explicit opt-in)

    Identity:
    - tenant_id must be resolvable; if not, raises ValueError → 422 (I2).
    - Response metadata carries ``dcl_ingest_id`` (I1), never bare run_id.
    """
    metric_def = resolve_metric(request.metric)

    if metric_def is None:
        raise ValueError(f"Metric '{request.metric}' not found")

    resolved_id = metric_def.id
    grain = request.grain or metric_def.default_grain or "quarter"
    unit = _resolve_unit(metric_def)

    metric_unit = (getattr(metric_def, "unit", None) or "").lower()
    is_additive = metric_unit not in _NON_ADDITIVE_UNITS

    tenant_id = _resolve_tenant_id(request.tenant_id, request.entity_id)

    data_points, meta = _query_current_triples(
        metric=resolved_id,
        time_range=request.time_range,
        tenant_id=tenant_id,
        entity_id=request.entity_id,
        consolidate=request.consolidate,
        is_additive=is_additive,
    )

    current_mode = get_current_mode()

    if not data_points:
        has_any = _tenant_has_triples(tenant_id)
        if not has_any:
            status = "no_data"
            error_msg = (
                f"No triples in current_triples for tenant_id={tenant_id}. "
                f"Run the Farm→DCL pipeline to ingest data."
            )
        else:
            status = "no_results"
            error_msg = (
                f"No results for metric='{request.metric}', "
                f"entity_id='{request.entity_id}', tenant_id='{tenant_id}'. "
                f"current_triples has rows for this tenant but none match the query."
            )

        logger.warning(error_msg)
        return QueryResponse(
            status=status,
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
                mode=current_mode.data_mode,
                record_count=0,
                source="ingest",
                tenant_id=tenant_id,
                entity_id=request.entity_id,
                error=error_msg,
            ),
        )

    persona_label = None
    persona_definition_text = None
    if request.persona:
        from backend.engine.persona_definitions import get_persona_definition_store
        pcd_store = get_persona_definition_store()
        persona_label = request.persona.upper()

        pcd_def = pcd_store.get_definition(request.metric, persona_label)
        if pcd_def:
            persona_definition_text = pcd_def.definition

            if pcd_def.value_override is not None:
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

    enrichment_errors: Dict[str, str] = {}
    provenance_info: List[ProvenanceInfo] = []
    entity_info = None
    conflicts_info = None

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
        except Exception as e:
            logger.warning(
                f"[query] Temporal warning check failed for metric={request.metric}: {e}",
                exc_info=True,
            )
            enrichment_errors["temporal"] = "Temporal analysis unavailable"

    source_list = meta.get("source_list") or []
    sources_for_metadata = source_list if source_list else ["ingest"]

    return QueryResponse(
        status="ok",
        metric=request.metric,
        metric_name=metric_def.name,
        dimensions=request.dimensions,
        grain=grain,
        unit=unit,
        data=data_points,
        metadata=QueryMetadata(
            sources=sources_for_metadata,
            freshness=datetime.utcnow().isoformat() + "Z",
            quality_score=1.0,
            mode=current_mode.data_mode,
            record_count=len(data_points),
            source="ingest",
            dcl_ingest_id=meta.get("dcl_ingest_id"),
            entity_id=meta.get("entity_id") or request.entity_id,
            tenant_id=meta.get("tenant_id"),
            snapshot_name=meta.get("snapshot_name"),
            run_timestamp=meta.get("run_timestamp"),
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
        enrichment_errors=enrichment_errors or None,
    )


def handle_query(request: QueryRequest) -> Union[QueryResponse, QueryError]:
    """Main entry point for query handling.

    ValueError from _resolve_tenant_id surfaces as code=IDENTITY_MISSING so
    the FastAPI layer maps it to 422 (I2: no silent tenant fallback).
    """
    error = validate_query(request)
    if error:
        return error

    try:
        return execute_query(request)
    except ValueError as exc:
        return QueryError(
            error=str(exc),
            code="IDENTITY_MISSING",
        )
