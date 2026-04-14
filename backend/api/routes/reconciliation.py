"""
DCL Reconciliation routes — AAM vs DCL source comparison.

Handles:
  GET  /api/dcl/reconciliation              — mode-aware reconciliation
  GET  /api/dcl/reconciliation/sor          — SOR reconciliation
  GET  /api/dcl/reconciliation/cross-system — cross-system stats reconciliation
  POST /api/reconcile                       — stateless AAM reconciliation
"""

from pathlib import Path

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any

from backend.api.ingest import get_ingest_store
from backend.api.pipe_store import get_pipe_store
from backend.core.db import get_connection
from backend.core.mode_state import get_current_mode
from backend.core.constants import utc_now
from backend.db.triple_store import TripleStore
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

router = APIRouter(tags=["Reconciliation"])


# ---------------------------------------------------------------------------
# GET /api/dcl/reconciliation
# ---------------------------------------------------------------------------

@router.get("/api/dcl/reconciliation")
def get_reconciliation(
    aod_run_id: Optional[str] = None,
    dispatch_id: Optional[str] = None,
):
    """Mode-aware reconciliation — Farm uses IngestStore, AAM uses AAM client."""
    try:
        current_mode = get_current_mode()
        if current_mode.data_mode == "Farm":
            return _farm_reconciliation(dispatch_id=dispatch_id)
        return _aam_reconciliation(aod_run_id)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Reconciliation failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# GET /api/dcl/reconciliation/sor
# ---------------------------------------------------------------------------

@router.get("/api/dcl/reconciliation/sor")
def get_sor_reconciliation():
    try:
        import yaml
        from backend.engine.sor_reconciliation import reconcile_sor
        from backend.api.main import app

        config_dir = Path(__file__).parent.parent.parent / "config" / "definitions"

        bindings_path = config_dir / "bindings.yaml"
        metrics_path = config_dir / "metrics.yaml"
        entities_path = config_dir / "entities.yaml"

        bindings = []
        if bindings_path.exists():
            with open(bindings_path) as f:
                bindings = yaml.safe_load(f).get("bindings", [])

        metrics_list = []
        if metrics_path.exists():
            with open(metrics_path) as f:
                metrics_list = yaml.safe_load(f).get("metrics", [])

        entities_list = []
        if entities_path.exists():
            with open(entities_path) as f:
                entities_list = yaml.safe_load(f).get("entities", [])

        loaded_source_ids = list(app.state.loaded_source_ids)
        loaded_sources = loaded_source_ids if loaded_source_ids else list(app.state.loaded_sources)

        if not loaded_sources and bindings:
            loaded_sources = sorted(set(
                b.get("source_system", "") for b in bindings if b.get("source_system")
            ))
            logger.info(f"[SOR] No prior run — derived {len(loaded_sources)} sources from bindings")

        result = reconcile_sor(bindings, metrics_list, entities_list, loaded_sources)

        # --- AOD SOR pipeline coverage ---
        # Show which AOD-identified SORs have complete pipeline coverage
        # (AOD → AAM → Farm → DCL). Post–store-rebuild, DCL presence is
        # determined by scanning current_triples.source_system for a live
        # row; Farm receipts come from ingest_log.
        pipe_store = get_pipe_store()
        all_pipe_defs = pipe_store.get_all_definitions()
        loaded_canonical_set = set(loaded_sources)

        sor_store = TripleStore()
        try:
            sor_tenant_id = sor_store.resolve_single_tenant()
        except ValueError:
            sor_tenant_id = None

        live_sources_lower: set = set()
        log_sources_lower: set = set()
        if sor_tenant_id:
            live_sql = (
                "SELECT DISTINCT LOWER(source_system) FROM current_triples "
                "WHERE tenant_id = %s AND source_system IS NOT NULL"
            )
            log_sources_sql = (
                "SELECT DISTINCT LOWER(src) FROM ingest_log, "
                "UNNEST(source_systems) AS src WHERE tenant_id::text = %s"
            )
            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(live_sql, (sor_tenant_id,))
                    live_sources_lower = {r[0] for r in cur.fetchall() if r[0]}
                    cur.execute(log_sources_sql, (sor_tenant_id,))
                    log_sources_lower = {r[0] for r in cur.fetchall() if r[0]}

        aod_sor_coverage = []
        for pipe_def in all_pipe_defs:
            if not pipe_def.sor_tagging:
                continue
            import json as _json
            sor_confidence = "unknown"
            try:
                parsed = _json.loads(pipe_def.sor_tagging)
                if isinstance(parsed, dict):
                    sor_confidence = parsed.get("confidence", "unknown")
            except (ValueError, TypeError):
                sor_confidence = "tagged"

            vendor_lower = (pipe_def.vendor or "").lower()
            dcl_has_data = bool(vendor_lower) and (
                vendor_lower in live_sources_lower
                or vendor_lower in {s.lower() for s in loaded_canonical_set}
            )
            farm_has_receipt = bool(vendor_lower) and vendor_lower in log_sources_lower

            aod_sor_coverage.append({
                "pipe_id": pipe_def.pipe_id,
                "vendor": pipe_def.vendor,
                "category": pipe_def.category,
                "aod_confidence": sor_confidence,
                "aam_has_pipe": True,
                "farm_has_receipt": farm_has_receipt,
                "dcl_has_data": dcl_has_data,
            })
        result["aodSorCoverage"] = aod_sor_coverage

        sor_current_mode = get_current_mode()
        sor_snapshot_name = getattr(app.state, "aam_snapshot_name", None)
        if not sor_snapshot_name:
            raise HTTPException(
                status_code=500,
                detail="No snapshot_name available. Run DCL in AAM mode first so a snapshot name is established."
            )
        result["reconMeta"] = {
            "dcl_ingest_id": sor_current_mode.last_run_id,
            "dclRunAt": sor_current_mode.last_updated,
            "reconAt": utc_now(),
            "dataMode": sor_current_mode.data_mode,
            "loadedSourceCount": len(loaded_sources),
            "snapshotName": sor_snapshot_name,
        }

        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"SOR Reconciliation failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# POST /api/reconcile
# ---------------------------------------------------------------------------

class ReconcileRequest(BaseModel):
    """Request to reconcile AAM payload against DCL's ingested state."""
    aod_run_id: Optional[str] = Field(None, description="AOD run ID to reconcile against")
    aam_source_ids: Optional[List[str]] = Field(
        None,
        description="Expected source IDs from AAM payload. If omitted, fetches live from AAM.",
    )


@router.post("/api/reconcile")
def reconcile_aam(request: ReconcileRequest):
    """
    Compare what AAM sent (expected) vs what DCL would ingest (actual).

    Stateless — fetches fresh from AAM each time, no dependency on prior run.
    Uses AAMIngressAdapter for consistent normalization on both sides.
    """
    from backend.aam.ingress import AAMIngressAdapter, normalize_source_id
    from backend.aam.client import get_aam_client

    adapter = AAMIngressAdapter()
    aam_client = get_aam_client()

    # Auto-discover aod_run_id from latest push if not provided
    effective_run_id = request.aod_run_id
    if not effective_run_id:
        try:
            pushes_raw = aam_client.get_push_history()
            pushes = adapter.ingest_push_history(pushes_raw)
            if pushes and pushes[0].aod_run_id:
                effective_run_id = pushes[0].aod_run_id
                logger.info(f"[Reconcile] Auto-discovered aod_run_id={effective_run_id} from latest push")
        except Exception as e:
            logger.warning(f"[Reconcile] Push history unavailable: {e}")

    # ── 1. Build "expected" set from AAM ────────────────────────────────
    expected_sources: Dict[str, Dict[str, Any]] = {}
    payload = None

    if request.aam_source_ids:
        for sid in request.aam_source_ids:
            canonical = normalize_source_id(sid)
            expected_sources[canonical] = {"source_id": canonical, "origin": "caller"}
    else:
        try:
            pipes_data = aam_client.get_pipes(aod_run_id=effective_run_id)
            payload = adapter.ingest_pipes(pipes_data)

            for pipe in payload.pipes:
                expected_sources[pipe.canonical_id] = {
                    "source_id": pipe.canonical_id,
                    "source_name": pipe.display_name,
                    "plane_type": pipe.fabric_plane,
                    "field_count": pipe.field_count,
                    "origin": "aam_live",
                }
        except Exception as e:
            raise HTTPException(
                status_code=502,
                detail=f"Cannot reach AAM to fetch expected sources: {e}",
            )

    if not expected_sources:
        raise HTTPException(
            status_code=400,
            detail="No expected sources — provide aam_source_ids or ensure AAM returns data.",
        )

    # ── 2. Build "actual" set — what DCL would load from AAM ────────────
    if payload is not None:
        actual_canonical_ids = {p.canonical_id for p in payload.pipes}
    else:
        try:
            pipes_data = aam_client.get_pipes(aod_run_id=effective_run_id)
            payload = adapter.ingest_pipes(pipes_data)
            actual_canonical_ids = {p.canonical_id for p in payload.pipes}
        except Exception as e:
            raise HTTPException(
                status_code=502,
                detail=f"Cannot reach AAM to fetch actual sources: {e}",
            )

    # ── 3. Reconcile ────────────────────────────────────────────────────
    matched = []
    missing_in_dcl = []
    extra_in_dcl = []

    for sid, aam_info in expected_sources.items():
        if sid in actual_canonical_ids:
            matched.append({
                "source_id": sid,
                "aam": aam_info,
                "status": "matched",
            })
        else:
            missing_in_dcl.append({
                "source_id": sid,
                "aam": aam_info,
                "status": "missing_in_dcl",
            })

    for cid in sorted(actual_canonical_ids):
        if cid not in expected_sources:
            extra_in_dcl.append({
                "source_id": cid,
                "status": "extra_in_dcl",
            })

    total_expected = len(expected_sources)
    total_actual = len(actual_canonical_ids)
    match_count = len(matched)

    if match_count == total_expected and not extra_in_dcl:
        verdict = "fully_reconciled"
    elif match_count == total_expected:
        verdict = "reconciled_with_extras"
    elif missing_in_dcl:
        verdict = "drift_detected"
    else:
        verdict = "partial_match"

    return {
        "status": verdict,
        "dcl_ingest_id": None,
        "aod_run_id": effective_run_id,
        "expected_count": total_expected,
        "actual_count": total_actual,
        "matched_count": match_count,
        "missing_in_dcl_count": len(missing_in_dcl),
        "extra_in_dcl_count": len(extra_in_dcl),
        "matched": matched,
        "missing_in_dcl": missing_in_dcl,
        "extra_in_dcl": extra_in_dcl,
    }


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _invalidate_aam_caches():
    """Clear all caches that could return stale AAM data on a new run."""
    try:
        from backend.semantic_mapper.persist_mappings import MappingPersistence
        MappingPersistence.clear_all_caches()
        logger.info("[AAM] Cleared mapping persistence caches")
    except Exception as e:
        logger.warning(f"[AAM] Failed to clear mapping caches: {e}")

    try:
        import backend.aam.client as aam_mod
        if aam_mod._aam_client is not None:
            aam_mod._aam_client.close()
            aam_mod._aam_client = None
            logger.info("[AAM] Reset AAM client singleton")
    except Exception as e:
        logger.warning(f"[AAM] Failed to reset AAM client: {e}")

    from backend.engine.schema_loader import SchemaLoader
    SchemaLoader._demo_cache = None
    SchemaLoader._stream_cache = None
    SchemaLoader._cache_time = 0
    SchemaLoader._aam_cache = None
    SchemaLoader._aam_cache_time = 0
    logger.info("[AAM] All stale caches invalidated for fresh AAM run")


def _farm_reconciliation(dispatch_id: Optional[str] = None) -> Dict[str, Any]:
    """Reconcile Farm pushes against DCL ``current_triples``.

    Post-store-rebuild:
    - Push receipts live in ``ingest_log`` (one row per
      POST /api/dcl/ingest-triples call) — triples_received/written plus
      source_systems and duration_ms.
    - Content lives in ``current_triples`` — source_system + entity_id give
      a live row count per source.
    - There is no concept of dispatch in Farm mode; the parameter is
      accepted for backward compatibility but ignored.
    """
    from backend.aam.ingress import NormalizedPipe
    from backend.engine.reconciliation import reconcile
    from backend.api.main import app

    current_mode = get_current_mode()
    now = utc_now()

    store = TripleStore()
    try:
        tenant_id = store.resolve_single_tenant()
    except ValueError:
        tenant_id = None

    empty_shell = {
        "status": "empty",
        "summary": {
            "aamConnections": 0, "dclLoadedSources": 0, "matched": 0,
            "inAamNotDcl": 0, "inDclNotAam": 0, "unmappedCount": 0,
        },
        "diffCauses": [{
            "cause": "NO_PUSH", "severity": "info", "count": 0,
            "description": "No Farm data in current_triples — push from Farm first",
        }],
        "fabricBreakdown": [], "inAamNotDcl": [], "inDclNotAam": [],
        "pushMeta": None,
        "reconMeta": {
            "dcl_ingest_id": current_mode.last_run_id,
            "dclRunAt": current_mode.last_updated,
            "reconAt": now, "aodRunId": None,
            "dataMode": "Farm", "dclSourceCount": 0, "aamConnectionCount": 0,
        },
        "trace": {
            "aamPipeNames": [], "dclLoadedSourceNames": [],
            "exportPipeCount": 0, "pushPipeCount": 0, "unmappedCount": 0,
        },
    }

    if not tenant_id:
        return empty_shell

    sources_sql = (
        "SELECT source_system, "
        "ARRAY_AGG(DISTINCT entity_id) FILTER (WHERE entity_id IS NOT NULL) AS entities, "
        "ARRAY_AGG(DISTINCT COALESCE(fabric_plane, '')) FILTER (WHERE fabric_plane IS NOT NULL) AS planes, "
        "COUNT(*) AS total_rows "
        "FROM current_triples WHERE tenant_id = %s "
        "GROUP BY source_system"
    )
    log_sql = (
        "SELECT run_id::text, entity_id, triples_received, triples_written, "
        "triples_rejected, source_systems, duration_ms, created_at "
        "FROM ingest_log WHERE tenant_id::text = %s "
        "ORDER BY created_at DESC"
    )

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sources_sql, (tenant_id,))
            source_rows = cur.fetchall()
            cur.execute(log_sql, (tenant_id,))
            log_rows = cur.fetchall()

    if not source_rows and not log_rows:
        return empty_shell

    source_groups: Dict[str, Dict[str, Any]] = {}
    total_records = 0
    for source_system, entities, planes, row_count in source_rows:
        if not source_system:
            continue
        canonical_id = source_system.lower().strip().replace(" ", "_").replace("-", "_")
        category = (planes or ["unknown"])[0] or "unknown"
        source_groups[canonical_id] = {
            "canonical_id": canonical_id,
            "display_name": source_system,
            "category": category,
            "trust_score": None,
            "data_quality_score": None,
            "pipes": list(entities or []),
            "total_records": int(row_count or 0),
            "fields": set(),
        }
        total_records += int(row_count or 0)

    farm_pipes: List[NormalizedPipe] = []
    for canonical_id, grp in source_groups.items():
        farm_pipes.append(NormalizedPipe(
            canonical_id=canonical_id,
            display_name=grp["display_name"],
            pipe_id=canonical_id,
            fabric_plane=grp["category"],
            vendor=grp["display_name"],
            fields=[],
            field_count=0,
            category=grp["category"],
            governance_status="canonical",
            trust_score=grp["trust_score"],
            data_quality_score=grp["data_quality_score"],
        ))

    dcl_ids = list(app.state.loaded_source_ids) if app.state.loaded_source_ids else list(app.state.loaded_sources)
    if not dcl_ids:
        dcl_ids = sorted(source_groups.keys())

    result = reconcile(farm_pipes, dcl_ids)

    result["sourceBreakdown"] = [
        {
            "sourceName": grp["display_name"], "canonicalId": cid,
            "category": grp["category"],
            "trustScore": grp["trust_score"],
            "pipeCount": len(grp["pipes"]),
            "recordCount": grp["total_records"],
            "fieldCount": 0,
            "loaded": cid in set(dcl_ids),
        }
        for cid, grp in sorted(source_groups.items())
    ]

    latest_log = log_rows[0] if log_rows else None
    first_log = log_rows[-1] if log_rows else None
    total_triples_received = sum(int(r[2] or 0) for r in log_rows)
    push_count = len(log_rows)
    if latest_log:
        result["pushMeta"] = {
            "dispatchId": None,
            "pushId": latest_log[0],
            "pushedAt": latest_log[7].isoformat() if latest_log[7] else None,
            "firstReceivedAt": first_log[7].isoformat() if first_log and first_log[7] else None,
            "pipeCount": push_count,
            "totalRows": total_triples_received or total_records,
            "payloadHash": None,
            "aodRunId": None,
        }
    else:
        result["pushMeta"] = None

    result["reconMeta"] = {
        "dcl_ingest_id": current_mode.last_run_id,
        "dclRunAt": current_mode.last_updated,
        "reconAt": now,
        "aodRunId": None,
        "dataMode": "Farm",
        "dispatchId": None,
        "dclSourceCount": len(dcl_ids),
        "aamConnectionCount": len(farm_pipes),
    }

    farm_pipe_names = sorted(grp["display_name"] for grp in source_groups.values())
    result["trace"] = {
        "aamPipeNames": farm_pipe_names,
        "dclLoadedSourceNames": dcl_ids,
        "exportPipeCount": push_count,
        "pushPipeCount": push_count,
        "unmappedCount": 0,
    }

    return result


def _diagnose_pipe_failure(
    pipe_def,
    pipe_id: str,
    dispatch_entry: Optional[Dict[str, Any]],
    all_receipt_pipe_ids: Optional[Dict[str, list]] = None,
    drops_by_pipe: Optional[Dict[str, list]] = None,
    current_snapshot: Optional[str] = None,
) -> str:
    """Return a plain-English reason why this pipe has no content receipt.

    Enhanced with cross-snapshot receipt lookup and drop log inspection
    to distinguish between snapshot scoping, DCL rejection, and true
    failures (never received).
    """
    # Check 1: Receipt exists under a DIFFERENT snapshot
    if all_receipt_pipe_ids and pipe_id in all_receipt_pipe_ids:
        receipt_snapshots = all_receipt_pipe_ids[pipe_id]
        if current_snapshot and current_snapshot not in receipt_snapshots:
            return (
                f"Receipt EXISTS but under snapshot '{receipt_snapshots[0]}', "
                f"not the current snapshot '{current_snapshot}'. "
                f"This pipe was successfully pushed to DCL in a previous dispatch."
            )
        # Receipt exists under current snapshot but wasn't counted (shouldn't happen)
        return f"Receipt exists under current snapshot — possible counting error."

    # Check 2: Pipe was dropped by DCL (in drop log)
    if drops_by_pipe and pipe_id in drops_by_pipe:
        drop = drops_by_pipe[pipe_id][0]  # latest drop
        return (
            f"Rejected by DCL: {drop.get('error_code', 'UNKNOWN')} — "
            f"{drop.get('reason', 'no reason recorded')}. "
            f"Farm pushed this pipe but DCL's schema-on-write guard rejected it."
        )

    # Check 3: No definition at all
    if pipe_def is None:
        return (
            f"Pipe '{pipe_id}' has no definition in DCL's pipe store — "
            f"it was listed in the export but never registered. "
            f"AAM may have sent a stale or malformed pipe ID."
        )

    # Check 4: Definition but missing required fields
    if not pipe_def.vendor:
        return (
            f"Pipe definition exists but vendor is empty — "
            f"Farm cannot identify which source system to generate data for."
        )
    if not pipe_def.category or pipe_def.category.lower() in ("", "unknown"):
        return (
            f"Pipe has vendor '{pipe_def.vendor}' but no category — "
            f"Farm may have skipped it because the data type is unclassified."
        )

    # Check 5: No dispatch activity at all
    if dispatch_entry is None:
        return (
            f"No dispatch activity recorded — AAM may not have dispatched "
            f"work orders to Farm for this run."
        )

    # Default: dispatched but never arrived at DCL
    return (
        f"No receipt and no drop recorded. Pipe was dispatched to Farm "
        f"(vendor: {pipe_def.vendor}, category: {pipe_def.category}) "
        f"but never reached DCL. Likely cause: Farm idempotency guard returned "
        f"'completed' from a previous run without re-pushing to DCL."
    )


def _classify_failure(
    pipe_id: str,
    all_receipt_pipe_ids: Optional[Dict[str, list]],
    drops_by_pipe: Optional[Dict[str, list]],
    current_snapshot: Optional[str] = None,
    pipe_def=None,
) -> str:
    """Return a machine-readable classification for a pipe failure."""
    if all_receipt_pipe_ids and pipe_id in all_receipt_pipe_ids:
        receipt_snapshots = all_receipt_pipe_ids[pipe_id]
        if current_snapshot and current_snapshot not in receipt_snapshots:
            return "snapshot_mismatch"
        return "snapshot_mismatch"  # exists somewhere
    if drops_by_pipe and pipe_id in drops_by_pipe:
        return "dcl_rejected"
    if pipe_def is None:
        return "no_definition"
    return "never_received"


def _aam_reconciliation(aod_run_id: Optional[str] = None) -> Dict[str, Any]:
    """Original AAM reconciliation — fetches from AAM fresh each time."""
    from backend.aam.client import get_aam_client
    from backend.aam.ingress import AAMIngressAdapter
    from backend.engine.reconciliation import reconcile
    from backend.api.main import app

    _invalidate_aam_caches()

    adapter = AAMIngressAdapter()
    client = get_aam_client()

    # Discover aod_run_id from latest push if not provided
    push_meta = None
    push_pipe_count = 0
    try:
        pushes_raw = client.get_push_history()
        pushes = adapter.ingest_push_history(pushes_raw)
        if pushes:
            latest = pushes[0]
            push_pipe_count = latest.pipe_count
            push_meta = {
                "pushId": latest.push_id,
                "pushedAt": latest.pushed_at,
                "pipeCount": push_pipe_count,
                "payloadHash": latest.payload_hash,
                "aodRunId": latest.aod_run_id,
            }
            if not aod_run_id and latest.aod_run_id:
                aod_run_id = latest.aod_run_id
                logger.info(f"[Recon] Auto-discovered aod_run_id={aod_run_id} from latest push")
    except Exception as e:
        logger.warning(f"Push history unavailable: {e}")

    aam_export = client.get_pipes(aod_run_id=aod_run_id)
    payload = adapter.ingest_pipes(aam_export)

    if push_meta:
        push_meta["payloadHash"] = push_meta["payloadHash"] or payload.payload_hash

    dcl_canonical_ids = sorted(p.canonical_id for p in payload.pipes)

    result = reconcile(payload.pipes, dcl_canonical_ids)

    if not push_meta:
        push_meta = {
            "pushId": "export-pipes",
            "pushedAt": utc_now(),
            "pipeCount": payload.total_connections_actual,
            "payloadHash": payload.payload_hash,
            "aodRunId": aod_run_id,
        }

    result["pushMeta"] = push_meta

    current_mode = get_current_mode()
    snapshot_name = payload.snapshot_name or getattr(app.state, "aam_snapshot_name", None)
    if not snapshot_name:
        raise HTTPException(
            status_code=500,
            detail="No snapshot_name available. AAM payload must include snapshot_name, or run DCL in AAM mode first."
        )

    result["reconMeta"] = {
        "dcl_ingest_id": current_mode.last_run_id,
        "dclRunAt": current_mode.last_updated,
        "reconAt": utc_now(),
        "aodRunId": aod_run_id,
        "dataMode": current_mode.data_mode,
        "dclSourceCount": len(dcl_canonical_ids),
        "aamConnectionCount": payload.total_connections_actual,
        "snapshotName": snapshot_name,
    }

    aam_names = sorted(p.display_name for p in payload.pipes)
    result["trace"] = {
        "aamPipeNames": aam_names,
        "dclLoadedSourceNames": dcl_canonical_ids,
        "exportPipeCount": payload.total_connections_actual,
        "pushPipeCount": push_pipe_count,
        "unmappedCount": sum(1 for p in payload.pipes if p.fabric_plane == "unmapped"),
    }

    return result


# ---------------------------------------------------------------------------
# GET /api/dcl/reconciliation/cross-system
# ---------------------------------------------------------------------------

# Module-level cache for the cross-system endpoint.
# Keyed by (generation, snapshot_param) so different snapshot views are cached independently.
_xsys_cache: Dict[Optional[str], dict] = {}
_xsys_cache_gen: int = -1


def _get_revenue_2025(snapshot_name: Optional[str] = None) -> Optional[float]:
    """Return 2025 annual revenue from ``current_triples``.

    Reads rows where ``split_part(concept,'.',1) = 'revenue'`` for any period
    starting with '2025' and returns the summed value across quarters.
    snapshot_name is accepted for compatibility but ignored — current_triples
    is already scoped to the live run per (tenant, entity).
    """
    store = TripleStore()
    try:
        tenant_id = store.resolve_single_tenant()
    except ValueError:
        return None

    sql = (
        "SELECT period, value FROM current_triples "
        "WHERE tenant_id = %s "
        "AND split_part(concept, '.', 1) = 'revenue' "
        "AND period LIKE '2025%%'"
    )
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (tenant_id,))
            rows = cur.fetchall()

    if not rows:
        return None

    total = 0.0
    counted = 0
    for _period, raw in rows:
        num = None
        if isinstance(raw, (int, float)):
            num = float(raw)
        elif isinstance(raw, dict):
            for key in ("amount", "value", "number"):
                inner = raw.get(key)
                if isinstance(inner, (int, float)):
                    num = float(inner)
                    break
        elif isinstance(raw, str):
            try:
                num = float(raw)
            except ValueError:
                num = None
        if num is not None:
            total += num
            counted += 1

    if counted == 0:
        return None
    return round(total, 2) if total > 0 else None


@router.get("/api/dcl/reconciliation/cross-system")
def get_cross_system_reconciliation(
    http_request: Request,
    snapshot: Optional[str] = Query(None, description="Snapshot name to scope recon to (default: latest)"),
):
    """Cross-system stats reconciliation — read-only aggregation.

    Pulls numbers from:
      - PipeDefinitionStore (structure phase: definitions, vendors, fabrics)
      - IngestStore activity log (3-phase entries)
      - IngestStore drop log (rejected pipes)
      - IngestStore receipts (content phase: ingested pipes)
      - AOD-authoritative systems_of_record (from app.state, set by export-pipes)
      - Materialized revenue data scoped to the selected snapshot

    Returns a unified view with per-system stats, deltas, and explanations.
    No data is mutated — this is purely a read endpoint.
    Cached until IngestStore mutates (generation counter changes).
    """
    global _xsys_cache, _xsys_cache_gen
    store = get_ingest_store()
    current_gen = store.generation
    if _xsys_cache_gen == current_gen and snapshot in _xsys_cache:
        return _xsys_cache[snapshot]
    # Generation changed — clear all cached snapshots
    if _xsys_cache_gen != current_gen:
        _xsys_cache = {}
        _xsys_cache_gen = current_gen
    pipe_store = get_pipe_store()
    now = utc_now()

    # --- Structure phase (from PipeDefinitionStore) ---
    pipe_stats = pipe_store.get_stats()
    all_definitions = pipe_store.get_all_definitions()
    export_receipts = pipe_store.get_export_receipts()
    latest_export = max(export_receipts, key=lambda r: r.received_at) if export_receipts else None

    structure_pipes = pipe_stats["total_definitions"]
    structure_vendors = pipe_stats["vendors"]  # list of vendor names
    structure_fabrics = pipe_stats["fabric_planes"]  # list of fabric names

    # Category breakdown from definitions
    category_counts: Dict[str, int] = {}
    for d in all_definitions:
        cat = (d.category or "").lower() or "(empty)"
        category_counts[cat] = category_counts.get(cat, 0) + 1

    # Governance breakdown from definitions
    governed_count = sum(
        1 for d in all_definitions
        if getattr(d, "governance_status", None) == "governed"
    )

    # --- Activity log (3-phase entries) ---
    activity = store.get_activity_log()
    structure_entry = next((e for e in activity if e["phase"] == "structure"), None)
    dispatch_entry = next((e for e in activity if e["phase"] == "dispatch"), None)
    content_entry = next((e for e in activity if e["phase"] == "content"), None)

    # --- Drop log ---
    drops = store.get_drop_log()
    unique_drop_pipes = sorted(set(d["pipe_id"] for d in drops))
    drops_by_error: Dict[str, int] = {}
    for d in drops:
        code = d.get("error_code", "UNKNOWN")
        drops_by_error[code] = drops_by_error.get(code, 0) + 1

    # --- Content phase (post-rebuild: from current_triples + ingest_log) ---
    # Store rebuild eliminated the multi-dispatch/multi-snapshot model. Every
    # row in current_triples is the live state for its tenant; ingest_log keeps
    # the push receipts. receipt_pipe_id_set = pipe_ids that landed in the
    # mirror; content_rows / content_sources come from the same table.
    try:
        tenant_id = TripleStore().resolve_single_tenant()
    except ValueError:
        tenant_id = None

    snapshot_name_filter = snapshot if snapshot else (latest_export.snapshot_name if latest_export else None)

    receipt_pipe_id_set: set = set()
    content_rows = 0
    content_sources: List[str] = []
    dispatch_id = ""
    all_receipt_pipe_ids: Dict[str, list] = {}
    snapshot_pipe_counts: Dict[str, int] = {}

    if tenant_id:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT DISTINCT pipe_id::text FROM current_triples "
                    "WHERE tenant_id = %s AND pipe_id IS NOT NULL",
                    (tenant_id,),
                )
                receipt_pipe_id_set = {row[0] for row in cur.fetchall() if row[0]}

                cur.execute(
                    "SELECT COUNT(*) FROM current_triples WHERE tenant_id = %s",
                    (tenant_id,),
                )
                content_rows = int((cur.fetchone() or [0])[0] or 0)

                cur.execute(
                    "SELECT DISTINCT source_system FROM current_triples "
                    "WHERE tenant_id = %s AND source_system IS NOT NULL "
                    "ORDER BY source_system",
                    (tenant_id,),
                )
                content_sources = [row[0] for row in cur.fetchall() if row[0]]

                cur.execute(
                    "SELECT run_id::text FROM ingest_log "
                    "WHERE tenant_id::text = %s "
                    "ORDER BY created_at DESC LIMIT 1",
                    (tenant_id,),
                )
                row = cur.fetchone()
                dispatch_id = row[0] if row and row[0] else ""

                cur.execute(
                    "SELECT tr.current_snapshot_name, "
                    "  COUNT(DISTINCT ct.pipe_id) "
                    "FROM tenant_runs tr "
                    "LEFT JOIN current_triples ct "
                    "  ON ct.tenant_id = tr.tenant_id::text "
                    " AND ct.entity_id = tr.entity_id "
                    " AND ct.pipe_id IS NOT NULL "
                    "WHERE tr.tenant_id::text = %s "
                    "  AND tr.current_snapshot_name IS NOT NULL "
                    "GROUP BY tr.current_snapshot_name",
                    (tenant_id,),
                )
                for snap_label, pipe_count in cur.fetchall():
                    key = snap_label or "(unnamed)"
                    snapshot_pipe_counts[key] = int(pipe_count or 0)

    # Every pipe_id in current_triples belongs to the one active snapshot per
    # entity (post-rebuild invariant). If snapshot_name_filter does not match
    # the active snapshot, treat the scoped receipt set as empty — that matches
    # the pre-rebuild "snapshot did not match" branch.
    if snapshot_name_filter and snapshot_name_filter not in snapshot_pipe_counts:
        receipt_pipe_id_set = set()

    snapshot_label_for_lookup = snapshot_name_filter or (
        next(iter(snapshot_pipe_counts.keys()), "") if snapshot_pipe_counts else ""
    )
    for pid in receipt_pipe_id_set:
        all_receipt_pipe_ids.setdefault(pid, [])
        if snapshot_label_for_lookup and snapshot_label_for_lookup not in all_receipt_pipe_ids[pid]:
            all_receipt_pipe_ids[pid].append(snapshot_label_for_lookup)

    # Build pipe_id → drop entries lookup
    drops_by_pipe: Dict[str, list] = {}
    for d_entry in drops:
        drops_by_pipe.setdefault(d_entry["pipe_id"], []).append(d_entry)

    # --- Build per-system view ---
    # AAM numbers (from structure + dispatch activity)
    aam_total = structure_entry["pipes"] if structure_entry else structure_pipes
    aam_dispatched = dispatch_entry["pipes"] if dispatch_entry else 0
    aam_sors = structure_entry["sors"] if structure_entry else 0
    aam_fabrics = structure_entry["fabrics"] if structure_entry else len(structure_fabrics)

    # DCL numbers — receipt-based count (ground truth), not activity log.
    # Each unique pipe_id with a receipt = one pipe that DCL durably ingested.
    dcl_total = aam_total  # DCL received same total via export-pipes
    dcl_ingested = len(receipt_pipe_id_set)
    dcl_sors = content_entry["sors"] if content_entry else 0
    dcl_tooling = content_entry.get("tooling_pipes", 0) if content_entry else 0
    dcl_fabrics = content_entry["fabrics"] if content_entry else 0
    dcl_mapped = content_entry.get("mapped_pipes", 0) if content_entry else 0
    dcl_unmapped = content_entry.get("unmapped_pipes", 0) if content_entry else 0
    dcl_sor_pipes = content_entry.get("sor_pipes", 0) if content_entry else 0
    dcl_other_pipes = content_entry.get("other_pipes", 0) if content_entry else 0
    dcl_rows = content_entry.get("rows", 0) if content_entry else content_rows
    dcl_drops_total = len(drops)
    dcl_drops_unique = len(unique_drop_pipes)

    # --- Deltas & explanations ---
    deltas: List[Dict[str, Any]] = []

    # Delta: AAM dispatched vs DCL ingested (current snapshot)
    farm_pushed = dcl_ingested + dcl_drops_unique  # best estimate of what Farm actually pushed
    farm_failed = aam_dispatched - farm_pushed if aam_dispatched > farm_pushed else 0
    if aam_dispatched > 0 and aam_dispatched != farm_pushed:
        deltas.append({
            "label": "Pipes without DCL receipt (current snapshot)",
            "left": f"AAM dispatched {aam_dispatched}",
            "right": f"DCL receipted {farm_pushed} (current snapshot '{snapshot_name_filter}')",
            "delta": farm_failed,
            "explanation": (
                f"{farm_failed} pipes dispatched by AAM have no DCL receipt for "
                f"snapshot '{snapshot_name_filter}'. "
                f"See failed_pipes list and pipeline_waterfall for per-pipe "
                f"classification (snapshot mismatch, DCL rejected, never received)."
            ),
            "severity": "warning" if farm_failed > 0 else "info",
        })

    # Delta: AAM total vs AAM dispatched
    if aam_total > 0 and aam_dispatched > 0 and aam_total != aam_dispatched:
        aam_failed = aam_total - aam_dispatched
        deltas.append({
            "label": "AAM pre-dispatch failures",
            "left": f"AAM total {aam_total}",
            "right": f"AAM dispatched {aam_dispatched}",
            "delta": aam_failed,
            "explanation": (
                f"{aam_failed} pipes were defined but not dispatched. "
                f"AAM rejected these before telling Farm to run "
                f"(unhealthy connections, missing credentials, or vendor failures)."
            ),
            "severity": "warning" if aam_failed > 0 else "info",
        })

    # Delta: DCL drops
    if dcl_drops_unique > 0:
        deltas.append({
            "label": "DCL schema-on-write rejections",
            "left": f"Farm pushed {farm_pushed}",
            "right": f"DCL ingested {dcl_ingested}",
            "delta": dcl_drops_unique,
            "explanation": (
                f"{dcl_drops_unique} unique pipes rejected by DCL's schema-on-write guard "
                f"(NO_MATCHING_PIPE). These pipe_ids from Farm did not match any "
                f"pipe definition registered via /export-pipes. "
                f"Total drop events: {dcl_drops_total} (includes retries)."
            ),
            "severity": "error" if dcl_drops_unique > 5 else "warning",
        })

    # Delta: fabric counts (structure vs content)
    if aam_fabrics != dcl_fabrics and aam_fabrics > 0:
        deltas.append({
            "label": "Fabric plane coverage gap",
            "left": f"Structure defined {aam_fabrics} fabrics",
            "right": f"Content received {dcl_fabrics} fabrics",
            "delta": aam_fabrics - dcl_fabrics,
            "explanation": (
                f"Structure phase registered {aam_fabrics} fabric planes "
                f"({', '.join(structure_fabrics)}), but only {dcl_fabrics} had pipes "
                f"that successfully pushed content data. This is expected if no "
                f"pipes are currently routed through those fabric planes."
            ),
            "severity": "info",
        })

    # Delta: SOR counts — compare against AOD authority.
    # Per RACI v6 rows 166-167, AOD owns SOR identification.
    # Both structure and content phases now derive their SOR count from
    # AOD's sor_tagging.  The old code compared vendor-count (structure)
    # vs category-count (content), which was comparing two wrong numbers.
    # Read from PipeStore (shared across workers via Redis), not app.state (per-worker).
    aod_systems_of_record = get_pipe_store().get_aod_systems_of_record()
    aod_sor_count = len(aod_systems_of_record)

    if aod_sor_count > 0:
        if aam_sors != aod_sor_count:
            deltas.append({
                "label": "Structure SOR count vs AOD authority",
                "left": f"AOD: {aod_sor_count} SORs (authoritative)",
                "right": f"Structure: {aam_sors} SORs (received)",
                "delta": aod_sor_count - aam_sors,
                "explanation": (
                    f"AOD identified {aod_sor_count} authoritative Systems of Record. "
                    f"Structure phase received {aam_sors}. These should match — "
                    f"if they don't, check that AAM is forwarding systems_of_record "
                    f"in the export-pipes payload."
                ),
                "severity": "warning",
            })
        if dcl_sors != aod_sor_count and dcl_sors >= 0:
            deltas.append({
                "label": "Content SOR count vs AOD authority",
                "left": f"AOD: {aod_sor_count} SORs (authoritative)",
                "right": f"Content: {dcl_sors} SORs (ingested)",
                "delta": aod_sor_count - dcl_sors,
                "explanation": (
                    f"AOD identified {aod_sor_count} authoritative Systems of Record. "
                    f"Content phase ingested data from {dcl_sors} SOR pipes. "
                    f"Gap of {abs(aod_sor_count - dcl_sors)} — check failed_pipes "
                    f"for SOR pipes that didn't push content."
                ),
                "severity": "warning" if dcl_sors < aod_sor_count else "info",
            })
    elif aam_sors > 0 or dcl_sors > 0:
        deltas.append({
            "label": "Missing AOD SOR authority",
            "left": "AOD: 0 SORs (no systems_of_record received)",
            "right": f"Structure: {aam_sors}, Content: {dcl_sors}",
            "delta": 0,
            "explanation": (
                "No AOD-authoritative systems_of_record in the export payload. "
                "AAM may not be forwarding SOR declarations from the handoff. "
                "SOR counts are unreliable without AOD authority."
            ),
            "severity": "warning",
        })

    # --- Per-pipe failure list ---
    # Compute which pipe_ids from structure phase have no receipt in the
    # CURRENT snapshot.  Uses enhanced classification to explain each gap.
    structure_pipe_ids = set(latest_export.pipe_ids) if latest_export else set()
    drop_pipe_id_set = set(unique_drop_pipes)

    # Failed = defined in structure but neither receipted (current snapshot) nor dropped
    failed_pipe_ids = structure_pipe_ids - receipt_pipe_id_set - drop_pipe_id_set
    failed_pipes: List[Dict[str, Any]] = []
    for pid in sorted(failed_pipe_ids):
        pipe_def = pipe_store.lookup(pid)
        reason = _diagnose_pipe_failure(
            pipe_def, pid, dispatch_entry,
            all_receipt_pipe_ids, drops_by_pipe, snapshot_name_filter,
        )
        classification = _classify_failure(
            pid, all_receipt_pipe_ids, drops_by_pipe,
            snapshot_name_filter, pipe_def,
        )
        failed_pipes.append({
            "pipe_id": pid,
            "vendor": pipe_def.vendor if pipe_def else "unknown",
            "category": pipe_def.category if pipe_def else "unknown",
            "fabric_plane": pipe_def.fabric_plane if pipe_def else "unknown",
            "reason": reason,
            "classification": classification,
        })

    # Per-classification counts for the pipeline waterfall
    snapshot_mismatch_count = sum(1 for p in failed_pipes if p["classification"] == "snapshot_mismatch")
    dcl_rejected_count = sum(1 for p in failed_pipes if p["classification"] == "dcl_rejected")
    never_received_count = sum(1 for p in failed_pipes if p["classification"] == "never_received")
    no_definition_count = sum(1 for p in failed_pipes if p["classification"] == "no_definition")

    # Snapshot identity
    snapshot_name = ""
    aod_run_id = ""
    if latest_export:
        snapshot_name = latest_export.snapshot_name or ""
        aod_run_id = latest_export.aod_run_id or ""

    # --- Snapshot provenance ---
    # snapshot_pipe_counts is computed above from tenant_runs + current_triples.
    other_snapshots: List[Dict[str, Any]] = []
    for snap, count in sorted(snapshot_pipe_counts.items()):
        if snap == snapshot_name_filter:
            continue
        other_snapshots.append({"snapshot_name": snap, "pipe_count": count})

    # --- Fabric names with content vs without ---
    # Which fabric planes (from structure) actually have content receipts?
    fabrics_with_content: set = set()
    for pid in receipt_pipe_id_set:
        pd = pipe_store.lookup(pid)
        if pd and pd.fabric_plane:
            fabrics_with_content.add(pd.fabric_plane)

    fabrics_missing_content: List[str] = sorted(
        f for f in structure_fabrics if f not in fabrics_with_content
    )

    # --- Farm failure summary ---
    # Aggregate failure classifications into a summary dict and identify
    # the dominant failure reason (most common classification across all
    # failed pipes). This gives operators a single-glance answer to
    # "why didn't all pipes make it through?"
    farm_failure_summary: Dict[str, int] = {}
    for fp in failed_pipes:
        cls = fp["classification"]
        farm_failure_summary[cls] = farm_failure_summary.get(cls, 0) + 1

    farm_dominant_failure_reason: Optional[str] = None
    if farm_failure_summary:
        farm_dominant_failure_reason = max(
            farm_failure_summary, key=farm_failure_summary.get  # type: ignore[arg-type]
        )

    # --- 2025 FY Revenue (from snapshot's materialized data) ---
    revenue_2025 = _get_revenue_2025(snapshot_name=snapshot_name_filter)

    result = {
        "snapshot_name": snapshot_name,
        "snapshot_filter": snapshot_name_filter,
        "other_snapshots": other_snapshots,
        "revenue_2025": revenue_2025,
        "aod_run_id": aod_run_id,
        "dispatch_id": dispatch_id,
        "recon_at": now,
        "systems": {
            "aam": {
                "total_pipes": aam_total,
                "dispatched": aam_dispatched,
                "failed_pre_dispatch": aam_total - aam_dispatched if aam_dispatched > 0 else 0,
                "sors": aam_sors,
                "fabrics": aam_fabrics,
                "fabric_names": structure_fabrics,
                "vendor_names": structure_vendors,
            },
            "farm": {
                "total_received": aam_dispatched,
                "pushed_to_dcl": farm_pushed,
                "failed_execution": aam_dispatched - farm_pushed if aam_dispatched > farm_pushed else 0,
                "failure_summary": farm_failure_summary,
                "dominant_failure_reason": farm_dominant_failure_reason,
            },
            "dcl": {
                "total_definitions": dcl_total,
                "ingested": dcl_ingested,
                "sors": dcl_sors,
                "sor_pipes": dcl_sor_pipes,
                "tooling_pipes": dcl_tooling,
                "fabrics_active": dcl_fabrics,
                "fabrics_defined": aam_fabrics,
                "fabrics_missing_content": fabrics_missing_content,
                "mapped_pipes": dcl_mapped,
                "unmapped_pipes": dcl_unmapped,
                "other_pipes": dcl_other_pipes,
                "rows": dcl_rows,
                "drops_total": dcl_drops_total,
                "drops_unique_pipes": dcl_drops_unique,
                "drop_pipe_ids": unique_drop_pipes,
            },
        },
        "aod_authority": {
            "sor_count": aod_sor_count,
            "systems_of_record": aod_systems_of_record,
        },
        "category_breakdown": category_counts,
        "governance": {
            "governed": governed_count,
            "ungoverned": structure_pipes - governed_count,
        },
        "drops_by_error": drops_by_error,
        "deltas": deltas,
        "failed_pipes": failed_pipes,
        "pipeline_waterfall": {
            "aam_dispatched": aam_dispatched,
            "dcl_ingested_current_snapshot": dcl_ingested,
            "dcl_drops": dcl_drops_unique,
            "unaccounted": farm_failed,
            "unaccounted_by_reason": {
                "snapshot_mismatch": snapshot_mismatch_count,
                "dcl_rejected": dcl_rejected_count,
                "never_received": never_received_count,
                "no_definition": no_definition_count,
            },
        },
        "activity": {
            "structure": structure_entry,
            "dispatch": dispatch_entry,
            "content": content_entry,
        },
    }
    _xsys_cache[snapshot] = result
    return result
