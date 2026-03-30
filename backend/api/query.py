"""
DCL Query Endpoint - executes data queries against the ingest buffer.

This module handles:
- Query validation against the semantic catalog
- Data retrieval from the ingest buffer (Runner-pushed data)
- Filtering and aggregation based on dimensions and time ranges

Data path: ingest buffer only (rows pushed by AAM Runners via POST /api/dcl/ingest).
If no data has been ingested, queries return status="no_data" with an explicit message.
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)

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
    # When entity_id IS specified: strict filter — only points tagged with
    # that entity_id pass. Unscoped data (_entity_id=None) is rejected to
    # prevent cross-entity contamination.
    if mat_points and entity_id:
        mat_points = [
            pt for pt in mat_points
            if pt.get("_entity_id") == entity_id
        ]

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
            # Include _tenant_id in dedup key to prevent cross-tenant overwrites.
            # Without this, data from different tenants for the same entity_id
            # would dedup by timestamp, letting stale AAM pipe data overwrite
            # correct Farm multi-entity data.
            tid = pt.get("_tenant_id", "")
            dedup_key = (period, src, dim_key, tid)
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
        _metric_unit_error = None
        try:
            _mdef = resolve_metric(metric)
            if _mdef and _mdef.unit:
                _metric_unit = _mdef.unit.lower()
        except Exception as e:
            logger.warning(f"[query] Metric unit resolution failed for metric={metric}: {e}", exc_info=True)
            _metric_unit_error = "Unit detection unavailable"
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
        # Track mapping metadata per aggregation key (use min confidence across merged points)
        agg_confidence: dict = {}  # key → (score, tier, source, status)
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

            # Propagate mapping metadata — use min confidence when merging
            pt_conf = pt.get("confidence_score")
            if pt_conf is not None:
                existing = agg_confidence.get(key)
                if existing is None or pt_conf < existing[0]:
                    agg_confidence[key] = (
                        pt_conf,
                        pt.get("confidence_tier"),
                        pt.get("mapping_source"),
                        pt.get("mapping_status"),
                    )

        def _build_point(k, value):
            conf = agg_confidence.get(k)
            return QueryDataPoint(
                period=k[0], value=round(value, 6), dimensions=dict(k[1]),
                entity_id=k[2] or agg_entity.get(k),
                confidence_score=conf[0] if conf else None,
                confidence_tier=conf[1] if conf else None,
                mapping_source=conf[2] if conf else None,
                mapping_status=conf[3] if conf else None,
            )

        if _is_additive:
            data_points = [
                _build_point(k, v)
                for k, v in sorted(agg.items())
            ]
        else:
            # Average for rate/percentage/score metrics
            data_points = [
                _build_point(k, agg[k] / agg_count[k])
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
    """Execute a validated query against the ingest buffer.

    WS1.3 Entity behavior:
    - entity_id=None, single entity → backward compatible
    - entity_id="xyz" → filter ingest by entity
    - consolidate=False (default) → per-entity results when multiple entities exist
    - consolidate=True → sum across entities (requires explicit opt-in)
    """
    _metric_unit_error = None
    metric_def = resolve_metric(request.metric)

    if metric_def is None:
        raise ValueError(f"Metric '{request.metric}' not found")

    resolved_id = metric_def.id
    grain = request.grain or metric_def.default_grain or "quarter"
    unit = _resolve_unit(metric_def)

    # ------------------------------------------------------------------
    # Query ingest buffer — the only data path
    # ------------------------------------------------------------------
    data_points, ingest_receipt = _query_ingest_store(
        metric=resolved_id,
        dimensions=request.dimensions,
        filters=request.filters,
        time_range=request.time_range,
        tenant_id=request.tenant_id,
        entity_id=request.entity_id,
        consolidate=request.consolidate,
    )

    # ------------------------------------------------------------------
    # No data — determine if store is empty (no_data) or query matched
    # nothing (no_results), and return explicit status
    # ------------------------------------------------------------------
    if not data_points:
        from backend.api.ingest import get_ingest_store
        store = get_ingest_store()
        store_stats = store.get_stats()
        total_rows = store_stats.get("total_rows_buffered", 0)
        current_mode = get_current_mode()

        if total_rows == 0:
            status = "no_data"
            error_msg = (
                f"No ingested data available. The ingest buffer is empty "
                f"(mode='{current_mode.data_mode}'). "
                f"Run the Farm→DCL pipeline to ingest data."
            )
        else:
            status = "no_results"
            error_msg = (
                f"No results for metric='{request.metric}', "
                f"entity_id='{request.entity_id}'. "
                f"The ingest buffer has {total_rows} rows but none matched this query."
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
                entity_id=request.entity_id,
                error=error_msg,
            ),
        )

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

    run_id = ingest_receipt.run_id
    run_timestamp = ingest_receipt.run_timestamp
    snapshot_name = ingest_receipt.snapshot_name
    tenant_id = ingest_receipt.tenant_id
    source_label = ingest_receipt.source_system

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
    enrichment_errors: Dict[str, str] = {}
    if _metric_unit_error:
        enrichment_errors["metric_unit"] = _metric_unit_error

    # Provenance comes from ingest pipeline receipts only
    provenance_info = []

    # Entity resolution and conflict detection moved to Convergence service.
    entity_info = None
    conflicts_info = None

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
        except Exception as e:
            logger.warning(f"[query] Temporal warning check failed for metric={request.metric}: {e}", exc_info=True)
            enrichment_errors["temporal"] = "Temporal analysis unavailable"

    return QueryResponse(
        status="ok",
        metric=request.metric,
        metric_name=metric_def.name,
        dimensions=request.dimensions,
        grain=grain,
        unit=unit,
        data=data_points,
        metadata=QueryMetadata(
            sources=[source_label] if source_label else ["ingest"],
            freshness=datetime.utcnow().isoformat() + "Z",
            quality_score=1.0,
            mode=mode.data_mode,
            record_count=len(data_points),
            source="ingest",
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
        enrichment_errors=enrichment_errors or None,
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
