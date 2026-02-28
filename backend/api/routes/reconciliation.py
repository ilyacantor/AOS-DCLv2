"""
DCL Reconciliation routes — AAM vs DCL source comparison.

Handles:
  GET  /api/dcl/reconciliation              — mode-aware reconciliation
  GET  /api/dcl/reconciliation/sor          — SOR reconciliation
  GET  /api/dcl/reconciliation/cross-system — cross-system stats reconciliation
  POST /api/reconcile                       — stateless AAM reconciliation
"""

from pathlib import Path

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any

from backend.api.ingest import get_ingest_store
from backend.api.pipe_store import get_pipe_store
from backend.core.mode_state import get_current_mode
from backend.core.constants import utc_now
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

        # --- AOD SOR pipeline coverage (Fix 10) ---
        # Show which AOD-identified SORs have complete pipeline coverage
        # (AOD → AAM → Farm → DCL). Uses sor_tagging from pipe definitions.
        pipe_store = get_pipe_store()
        ingest_store = get_ingest_store()
        all_pipe_defs = pipe_store.get_all_definitions()
        all_receipts = ingest_store.get_all_receipts()
        receipt_pipe_ids = set(r.pipe_id for r in all_receipts)
        loaded_canonical_set = set(loaded_sources)

        aod_sor_coverage = []
        for pipe_def in all_pipe_defs:
            if not pipe_def.sor_tagging:
                continue
            # Parse sor_tagging to extract confidence
            import json as _json
            sor_confidence = "unknown"
            try:
                parsed = _json.loads(pipe_def.sor_tagging)
                if isinstance(parsed, dict):
                    sor_confidence = parsed.get("confidence", "unknown")
            except (ValueError, TypeError):
                sor_confidence = "tagged"  # legacy string format

            aod_sor_coverage.append({
                "pipe_id": pipe_def.pipe_id,
                "vendor": pipe_def.vendor,
                "category": pipe_def.category,
                "aod_confidence": sor_confidence,
                "aam_has_pipe": True,  # it's in the export, so AAM has it
                "farm_has_receipt": pipe_def.pipe_id in receipt_pipe_ids,
                "dcl_has_data": pipe_def.vendor.lower() in {s.lower() for s in loaded_canonical_set} if pipe_def.vendor else False,
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
            "dclRunId": sor_current_mode.last_run_id,
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
        "run_id": None,
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
    """Reconcile Farm push receipts against DCL loaded sources — per dispatch."""
    from backend.aam.ingress import NormalizedPipe
    from backend.api.pipe_store import get_pipe_store
    from backend.engine.reconciliation import reconcile
    from backend.api.main import app

    store = get_ingest_store()
    all_receipts = store.get_all_receipts()
    current_mode = get_current_mode()
    now = utc_now()

    if not all_receipts:
        return {
            "status": "empty",
            "summary": {
                "aamConnections": 0, "dclLoadedSources": 0, "matched": 0,
                "inAamNotDcl": 0, "inDclNotAam": 0, "unmappedCount": 0,
            },
            "diffCauses": [{
                "cause": "NO_PUSH", "severity": "info", "count": 0,
                "description": "No Farm data received — push from Farm first",
            }],
            "fabricBreakdown": [], "inAamNotDcl": [], "inDclNotAam": [],
            "pushMeta": None,
            "reconMeta": {
                "dclRunId": current_mode.last_run_id,
                "dclRunAt": current_mode.last_updated,
                "reconAt": now, "aodRunId": None,
                "dataMode": "Farm", "dclSourceCount": 0, "aamConnectionCount": 0,
            },
            "trace": {
                "aamPipeNames": [], "dclLoadedSourceNames": [],
                "exportPipeCount": 0, "pushPipeCount": 0, "unmappedCount": 0,
            },
        }

    # Isolate by dispatch
    if dispatch_id:
        receipts = store.get_receipts_by_dispatch(dispatch_id)
        logger.info(f"[FarmRecon] Using dispatch_id={dispatch_id} ({len(receipts)} pipes)")
    else:
        dispatches = store.get_dispatches()
        if dispatches:
            latest_dispatch = dispatches[0]
            dispatch_id = latest_dispatch["dispatch_id"]
            receipts = store.get_receipts_by_dispatch(dispatch_id)
            logger.info(
                f"[FarmRecon] Auto-selected latest dispatch={dispatch_id} "
                f"({len(receipts)} pipes, {latest_dispatch['total_rows']:,} rows)"
            )
        else:
            latest = max(all_receipts, key=lambda r: r.received_at)
            receipts = [r for r in all_receipts if r.run_id == latest.run_id]
            logger.info(f"[FarmRecon] Fallback: latest run_id={latest.run_id} ({len(receipts)} pipes)")

    # Group receipts by canonical source via pipe_store
    pipe_store = get_pipe_store()
    source_groups: Dict[str, Dict[str, Any]] = {}
    unmapped_pipes: List[str] = []
    schema_registry = store.get_schema_registry()
    total_records = 0

    for receipt in receipts:
        total_records += receipt.row_count
        pipe_def = pipe_store.lookup(receipt.pipe_id)
        if not pipe_def:
            unmapped_pipes.append(receipt.pipe_id)
            continue
        canonical_id = pipe_def.source_name.lower().strip().replace(" ", "_").replace("-", "_")
        display_name = pipe_def.source_name or canonical_id
        category = pipe_def.category or "unknown"
        grp = source_groups.setdefault(canonical_id, {
            "canonical_id": canonical_id, "display_name": display_name,
            "category": category,
            "trust_score": pipe_def.trust_score,
            "data_quality_score": pipe_def.data_quality_score,
            "pipes": [], "total_records": 0, "fields": set(),
        })
        grp["pipes"].append(receipt.pipe_id)
        grp["total_records"] += receipt.row_count
        schema = schema_registry.get(receipt.pipe_id)
        if schema:
            grp["fields"].update(f for f in schema.field_names if not f.startswith("_"))

    # Build NormalizedPipe objects so we can reuse reconcile()
    farm_pipes: List[NormalizedPipe] = []
    for canonical_id, grp in source_groups.items():
        farm_pipes.append(NormalizedPipe(
            canonical_id=canonical_id,
            display_name=grp["display_name"],
            pipe_id=canonical_id,
            fabric_plane=grp["category"],
            vendor=grp["display_name"],
            fields=sorted(grp["fields"]),
            field_count=len(grp["fields"]),
            category=grp["category"],
            governance_status="canonical",
            trust_score=grp["trust_score"],
            data_quality_score=grp["data_quality_score"],
        ))

    # DCL side: what was actually loaded
    dcl_ids = list(app.state.loaded_source_ids) if app.state.loaded_source_ids else list(app.state.loaded_sources)

    result = reconcile(farm_pipes, dcl_ids)

    # Extra diff causes for Farm-specific issues
    if unmapped_pipes:
        result["diffCauses"].append({
            "cause": "UNMAPPED_PIPES",
            "description": f"{len(unmapped_pipes)} pipes have no entry in pipe_store: {', '.join(unmapped_pipes)}",
            "severity": "warning",
            "count": len(unmapped_pipes),
        })
    drift_events = store.get_drift_events()
    if drift_events:
        result["diffCauses"].append({
            "cause": "SCHEMA_DRIFT",
            "description": f"{len(drift_events)} schema drift events detected across pushes",
            "severity": "info",
            "count": len(drift_events),
        })

    # Source breakdown with record counts
    result["sourceBreakdown"] = [
        {
            "sourceName": grp["display_name"], "canonicalId": cid,
            "category": grp["category"],
            "trustScore": grp["trust_score"],
            "pipeCount": len(grp["pipes"]), "recordCount": grp["total_records"],
            "fieldCount": len(grp["fields"]), "loaded": cid in set(dcl_ids),
        }
        for cid, grp in sorted(source_groups.items())
    ]

    # Push metadata
    latest_receipt = max(receipts, key=lambda r: r.received_at)
    first_receipt = min(receipts, key=lambda r: r.received_at)
    result["pushMeta"] = {
        "dispatchId": dispatch_id,
        "pushId": latest_receipt.run_id,
        "pushedAt": latest_receipt.received_at,
        "firstReceivedAt": first_receipt.received_at,
        "pipeCount": len(receipts),
        "totalRows": total_records,
        "payloadHash": None,
        "aodRunId": None,
    }

    result["reconMeta"] = {
        "dclRunId": current_mode.last_run_id,
        "dclRunAt": current_mode.last_updated,
        "reconAt": now,
        "aodRunId": None,
        "dataMode": "Farm",
        "dispatchId": dispatch_id,
        "dclSourceCount": len(dcl_ids),
        "aamConnectionCount": len(farm_pipes),
    }

    farm_pipe_names = sorted(grp["display_name"] for grp in source_groups.values())
    result["trace"] = {
        "aamPipeNames": farm_pipe_names,
        "dclLoadedSourceNames": dcl_ids,
        "exportPipeCount": len(receipts),
        "pushPipeCount": len(receipts),
        "unmappedCount": len(unmapped_pipes),
    }

    return result


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
        "dclRunId": current_mode.last_run_id,
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

@router.get("/api/dcl/reconciliation/cross-system")
def get_cross_system_reconciliation():
    """Cross-system stats reconciliation — read-only aggregation.

    Pulls numbers from:
      - PipeDefinitionStore (structure phase: definitions, vendors, fabrics)
      - IngestStore activity log (3-phase entries)
      - IngestStore drop log (rejected pipes)
      - IngestStore receipts (content phase: ingested pipes)

    Returns a unified view with per-system stats, deltas, and explanations.
    No data is mutated — this is purely a read endpoint.
    """
    store = get_ingest_store()
    pipe_store = get_pipe_store()
    now = utc_now()

    # --- Structure phase (from PipeDefinitionStore) ---
    pipe_stats = pipe_store.get_stats()
    all_definitions = pipe_store.get_all_definitions()
    export_receipts = pipe_store.get_export_receipts()
    latest_export = export_receipts[-1] if export_receipts else None

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

    # --- Content phase (from receipts) ---
    all_receipts = store.get_all_receipts()
    dispatches = store.get_dispatches()

    # Determine the snapshot to scope receipt counting
    snapshot_name_filter = latest_export.snapshot_name if latest_export else None

    # Count unique receipt pipe_ids matching this snapshot across ALL dispatches.
    # This is the ground truth — each receipt is proof DCL accepted and stored data
    # for that pipe_id. The activity log accumulates across dispatches and overcounts.
    receipt_pipe_id_set: set = set()
    content_rows = 0
    content_sources: List[str] = []
    dispatch_id = ""
    for d in dispatches:
        d_snapshot = d.get("snapshot_name", "")
        # Match by snapshot if available, else include all
        if snapshot_name_filter:
            if isinstance(d_snapshot, str) and d_snapshot != snapshot_name_filter:
                continue
            if isinstance(d_snapshot, list) and snapshot_name_filter not in d_snapshot:
                continue
        for pid in d.get("pipe_ids", []):
            receipt_pipe_id_set.add(pid)
        content_rows += d["total_rows"]
        content_sources.extend(d["unique_sources"])
        if not dispatch_id:
            dispatch_id = d["dispatch_id"]  # use first matching dispatch for identity
    content_sources = sorted(set(content_sources))

    # --- Build per-system view ---
    # AAM numbers (from structure + dispatch activity)
    aam_total = structure_entry["pipes"] if structure_entry else structure_pipes
    aam_dispatched = dispatch_entry["pipes"] if dispatch_entry else 0
    aam_sors = structure_entry["sors"] if structure_entry else len(structure_vendors)
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

    # Delta: AAM dispatched vs DCL ingested
    farm_pushed = dcl_ingested + dcl_drops_unique  # best estimate of what Farm actually pushed
    if aam_dispatched > 0 and aam_dispatched != farm_pushed:
        farm_failed = aam_dispatched - farm_pushed
        deltas.append({
            "label": "Farm execution failures",
            "left": f"AAM dispatched {aam_dispatched}",
            "right": f"Farm pushed {farm_pushed}",
            "delta": farm_failed,
            "explanation": (
                f"{farm_failed} pipes were dispatched but have no DCL receipt. "
                f"Causes: Farm generation failure, DCL push failure, or receipt "
                f"lost before DCL persisted (process restart). "
                f"See failed_pipes list below for per-pipe detail."
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

    # Delta: SOR counts (structure vs content)
    if aam_sors != dcl_sors and aam_sors > 0:
        deltas.append({
            "label": "SOR count discrepancy",
            "left": f"Structure: {aam_sors} SORs (vendor-based)",
            "right": f"Content: {dcl_sors} SORs (category-based)",
            "delta": aam_sors - dcl_sors,
            "explanation": (
                f"Structure phase counts SORs as unique vendors ({aam_sors}). "
                f"Content phase counts SORs as unique non-tooling source_systems ({dcl_sors}). "
                f"This is a definition difference, not a counting error — AAM uses "
                f"vendor names, DCL uses category-based classification "
                f"(crm, erp, finops, infra, aod)."
            ),
            "severity": "info",
        })

    # --- Per-pipe failure list (Fix 2) ---
    # Compute which pipe_ids from structure phase have no receipt across
    # any dispatch. This gives operators exact visibility into failures.
    structure_pipe_ids = set(latest_export.pipe_ids) if latest_export else set()
    drop_pipe_id_set = set(unique_drop_pipes)

    # Failed = defined in structure but neither receipted nor dropped
    failed_pipe_ids = structure_pipe_ids - receipt_pipe_id_set - drop_pipe_id_set
    failed_pipes: List[Dict[str, Any]] = []
    for pid in sorted(failed_pipe_ids):
        pipe_def = pipe_store.lookup(pid)
        failed_pipes.append({
            "pipe_id": pid,
            "vendor": pipe_def.vendor if pipe_def else "unknown",
            "category": pipe_def.category if pipe_def else "unknown",
            "fabric_plane": pipe_def.fabric_plane if pipe_def else "unknown",
        })

    # Snapshot identity
    snapshot_name = ""
    aod_run_id = ""
    if latest_export:
        snapshot_name = latest_export.snapshot_name or ""
        aod_run_id = latest_export.aod_run_id or ""

    return {
        "snapshot_name": snapshot_name,
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
            },
            "dcl": {
                "total_definitions": dcl_total,
                "ingested": dcl_ingested,
                "sors_category": dcl_sors,
                "sors_governed": dcl_sor_pipes,
                "tooling_pipes": dcl_tooling,
                "fabrics_active": dcl_fabrics,
                "fabrics_defined": aam_fabrics,
                "mapped_pipes": dcl_mapped,
                "unmapped_pipes": dcl_unmapped,
                "other_pipes": dcl_other_pipes,
                "rows": dcl_rows,
                "drops_total": dcl_drops_total,
                "drops_unique_pipes": dcl_drops_unique,
                "drop_pipe_ids": unique_drop_pipes,
            },
        },
        "category_breakdown": category_counts,
        "governance": {
            "governed": governed_count,
            "ungoverned": structure_pipes - governed_count,
        },
        "drops_by_error": drops_by_error,
        "deltas": deltas,
        "failed_pipes": failed_pipes,
        "activity": {
            "structure": structure_entry,
            "dispatch": dispatch_entry,
            "content": content_entry,
        },
    }
