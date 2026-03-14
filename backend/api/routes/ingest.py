"""
DCL Ingestion routes — Runner push endpoint + query helpers.

Handles:
  GET  /api/dcl/ingest              — connectivity ping
  POST /api/dcl/ingest              — accept data push from Farm
  GET  /api/dcl/ingest/runs         — list all run receipts
  GET  /api/dcl/ingest/batches      — list batches by snapshot
  GET  /api/dcl/ingest/runs/{id}    — detail for one run
  GET  /api/dcl/ingest/drift        — schema drift events
  GET  /api/dcl/ingest/stats        — store summary
  GET  /api/dcl/ingest/dispatches   — dispatches list
  GET  /api/dcl/ingest/dispatches/X — dispatch detail
"""

import asyncio
import json
import os
import uuid
from concurrent.futures import ThreadPoolExecutor

from fastapi import APIRouter, HTTPException, Header, Request
from typing import Optional

from backend.core.constants import utc_now
from backend.api.ingest import (
    ActivityEntry,
    DropEntry,
    IngestRequest,
    IngestResponse,
    get_canonical_sources,
    get_ingest_store,
    compute_schema_hash,
    _derive_dispatch_id,
)
from backend.aam.ingress import normalize_source_id
from backend.api.pipe_store import get_pipe_store
from backend.core.mode_state import get_current_mode, set_current_mode
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/api/dcl/ingest", tags=["Ingestion"])

# Background thread pool for deferred materialization (avoids blocking ingest response)
_materialize_pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="materialize")


# ---------------------------------------------------------------------------
# Helpers extracted from the ingest god-function
# ---------------------------------------------------------------------------

def _normalize_ingest_body(raw_body: dict) -> dict:
    """Remap camelCase / alternate field names to the canonical snake_case schema."""
    body = dict(raw_body)
    if "source_system" not in body and "source" in body:
        body["source_system"] = body.pop("source")
    if "tenant_id" not in body:
        body.setdefault("tenant_id", body.get("tenantId", body.get("tenant", "default")))
    if "snapshot_name" not in body:
        alt = body.get("snapshotName") or body.get("snapshot")
        if not alt:
            raise HTTPException(
                status_code=422,
                detail="Missing required field: snapshot_name. Every ingest push must include a snapshot_name (or snapshotName)."
            )
        body["snapshot_name"] = alt
    if "run_timestamp" not in body:
        body.setdefault("run_timestamp", body.get("runTimestamp", body.get("timestamp", utc_now())))
    if "schema_version" not in body:
        body.setdefault("schema_version", body.get("schemaVersion", body.get("schema_ver", "1.0")))
    if "rows" not in body:
        if "data" in body:
            body["rows"] = body.pop("data")
        elif "records" in body:
            body["rows"] = body.pop("records")
        elif "payload" in body:
            body["rows"] = body.pop("payload")
        else:
            body["rows"] = []
    if "row_count" not in body:
        body["row_count"] = body.get("rowCount", len(body.get("rows", [])))
    if "runner_id" not in body and "runnerId" in body:
        body["runner_id"] = body.pop("runnerId")
    return body


def _validate_pipe_guard(pipe_id: str, run_id: str, source_system: str, now: str,
                         tenant_id: str = "", snapshot_name: str = ""):
    """Schema-on-write gate. Returns (pipe_def, guard_active) or raises 422.

    Farm self-directed pushes (run_id starts with 'farm_') bypass the guard
    because Farm generates its own pipe_ids without AAM pipe registration.
    AAM-dispatched pushes are still validated against registered blueprints.
    """
    pipe_store = get_pipe_store()
    pipe_def = pipe_store.lookup_from_store(pipe_id)
    guard_active = pipe_store.count() > 0

    if guard_active and pipe_def is None:
        # Farm self-directed pushes bypass the AAM pipe guard
        if run_id.startswith("farm_"):
            logger.info(
                f"[Ingest] Farm self-directed bypass: pipe_id={pipe_id} "
                f"(run_id={run_id}, source={source_system}) — no AAM blueprint "
                f"required for Farm pushes."
            )
            return None, False

        logger.error(
            f"[Ingest] REJECTED: No matching pipe definition for pipe_id={pipe_id} "
            f"(run_id={run_id}, source={source_system}). "
            f"Available pipes: {pipe_store.list_pipe_ids()}"
        )
        get_ingest_store().record_drop(DropEntry(
            pipe_id=pipe_id,
            reason=f"No schema blueprint exists for pipe_id: {pipe_id}.",
            error_code="NO_MATCHING_PIPE",
            source_system=source_system,
            timestamp=now,
            run_id=run_id,
            tenant_id=tenant_id,
            snapshot_name=snapshot_name,
        ))
        raise HTTPException(
            status_code=422,
            detail={
                "error": "NO_MATCHING_PIPE",
                "pipe_id": pipe_id,
                "message": f"No schema blueprint exists for pipe_id: {pipe_id}.",
                "hint": "Ensure AAM has run /export-pipes and that the pipe_id "
                        "matches between Export and Runner manifest.",
                "available_pipes": pipe_store.list_pipe_ids(),
                "timestamp": now,
            },
        )

    if not guard_active:
        logger.warning(
            "[Ingest] Ingest guard BYPASSED — no pipe definitions registered. "
            "Run AAM /export-pipes to activate schema-on-write validation."
        )

    return pipe_def, guard_active


def _resolve_export_identity(pipe_store) -> tuple:
    """Look up the latest AAM export receipt to get canonical snapshot_name and run_id.

    Returns (snapshot_name, aod_run_id, dispatch_id) from the latest export.
    All 3 phases (structure, dispatch, content) must share these identifiers
    so they group correctly in the Ingest tab.

    If no export receipts exist, returns empty strings and logs ERROR —
    this means Farm is pushing data before AAM sent /export-pipes, which
    is a sequencing violation.
    """
    receipts = pipe_store.get_export_receipts(force_sync=True)
    if not receipts:
        logger.error(
            "[Activity] No AAM export receipts found. Farm is pushing content "
            "before AAM sent /export-pipes — the 3-phase sequence is broken."
        )
        return "", "", ""

    latest = receipts[-1]
    aod_run_id = latest.aod_run_id or ""
    snapshot_name = latest.snapshot_name or ""
    if not aod_run_id:
        logger.error(
            "[Activity] Latest export receipt has no aod_run_id. "
            "AAM payload is missing this identifier."
        )
    if not snapshot_name:
        logger.warning(
            "[Activity] Latest export receipt has no snapshot_name. "
            "Falling back to aod_run_id for grouping."
        )
        snapshot_name = aod_run_id
    dispatch_id = f"aam_{aod_run_id[:20]}" if aod_run_id else ""
    return snapshot_name, aod_run_id, dispatch_id


def _is_tooling_pipe(pipe_def) -> bool:
    """Check if a pipe is a tooling/infrastructure pipe.

    A pipe is tooling only if its category explicitly indicates tooling,
    test, or staging infrastructure.  All other pipes are business system
    pipes — whether they are SOR or not is determined separately by AOD's
    sor_tagging (line 261-275), not by this function.
    """
    if not pipe_def:
        return False
    category = (pipe_def.category or "").lower()
    TOOLING_CATEGORIES = {"tooling", "test", "staging", "sandbox"}
    return category in TOOLING_CATEGORIES


def _record_ingest_activity(
    store,
    dispatch_id: str,
    snapshot_name: str,
    run_id: str,
    pipe_id: str,
    source_system: str,
    rows: int,
    matched_schema: bool,
    now: str,
    aod_sor_count: int = 0,
) -> None:
    """Record Path 3 (content) activity entries.

    Path 2 (dispatch) is now handled by a dedicated /export-pipes/dispatch
    endpoint that AAM calls before launching the Runner.

    Path 3 — "content": one entry per dispatch, incremented on each
    successive pipe push so the Ingest tab shows accumulated totals.

    snapshot_name comes from the AAM export receipt (same as structure
    phase) so all 3 phases group together. run_id on the content entry
    is Farm's native run_id so the Ingest tab shows the real value.

    Tooling pipes (category='tooling' or other non-SOR categories) are
    tracked separately in the tooling_pipes field and not counted in SOR totals.
    """
    pipe_store = get_pipe_store()

    # Resolve canonical identity from the AAM export receipt
    export_snap, export_run_id, export_dispatch_id = _resolve_export_identity(pipe_store)
    # Use export identity for snapshot_name (grouping key) and dispatch_id;
    # keep Farm's native run_id on the content entry.
    snap = export_snap or snapshot_name
    did = export_dispatch_id or dispatch_id

    # --- Path 3: Content activity ---
    # Use has_phase("content") so a prior structure entry with the same
    # dispatch_id doesn't shadow content creation.
    # Resolve fabric plane and category for this pipe from the pipe definition store.
    # lookup_from_store() reads Postgres directly — immune to per-worker cache staleness.
    pipe_def = pipe_store.lookup_from_store(pipe_id)

    if pipe_def is None:
        logger.warning(
            f"[Activity] pipe_def not found for pipe_id={pipe_id} "
            f"(source_system={source_system}, dispatch_id={did}). "
            f"Counting as unmapped with source_system attribution."
        )
        vendor_source = source_system
        pipe_fabric = None
        is_tooling = False
        is_sor = False
    else:
        vendor_source = pipe_def.vendor or source_system
        if not pipe_def.vendor:
            logger.warning(
                f"[Activity] pipe_def for pipe_id={pipe_id} has empty vendor. "
                f"Using source_system={source_system} for attribution."
            )
        pipe_fabric = pipe_def.fabric_plane if pipe_def.fabric_plane else None
        is_tooling = _is_tooling_pipe(pipe_def)

        # Classify pipe as SOR using AOD's sor_tagging (authoritative).
        # Per RACI v6 rows 166-167, AOD owns SOR identification.
        is_sor = False
        if pipe_def.sor_tagging:
            try:
                tagging = json.loads(pipe_def.sor_tagging)
                is_sor = tagging.get("confidence") in ("high", "medium")
            except (json.JSONDecodeError, TypeError):
                logger.warning(
                    f"[Activity] pipe_id={pipe_id} has unparseable sor_tagging: "
                    f"{pipe_def.sor_tagging!r} -- treating as non-SOR"
                )

    if did and not store.has_phase(did, "content"):
        # First pipe for this dispatch — create the content entry
        # Sync from Redis before mutating so we merge any pipes tracked by other workers
        store._sync_content_sets(did)

        store._content_pipes.setdefault(did, set()).add(pipe_id)

        # Track tooling pipes separately
        if is_tooling:
            store._content_tooling.setdefault(did, set()).add(pipe_id)
        else:
            # Only track mapped/unmapped for SOR pipes
            if matched_schema:
                store._content_mapped.setdefault(did, set()).add(pipe_id)
            else:
                store._content_unmapped.setdefault(did, set()).add(pipe_id)

        content_fabrics = store._content_fabrics.setdefault(did, set())
        if pipe_fabric:
            content_fabrics.add(pipe_fabric)
        # Track SOR vs other
        if is_sor:
            store._content_sor_pipes.setdefault(did, set()).add(pipe_id)
        else:
            store._content_other_pipes.setdefault(did, set()).add(pipe_id)
        store._content_sources.setdefault(did, set()).add(vendor_source)

        # Persist to Redis AFTER mutation so other workers see updated sets
        store._persist_content_sets(did)

        sor_set = store._content_sor_pipes.get(did, set())
        other_set = store._content_other_pipes.get(did, set())
        # entry.sors = AOD authority (same basis as structure phase).
        # sor_pipes/other_pipes track per-pipe tagging for sub-row detail.
        created = store.record_activity(ActivityEntry(
            phase="content",
            source="Farm",
            snapshot_name=snap,
            run_id=run_id,
            timestamp=now,
            pipes=1,
            sors=aod_sor_count,
            tooling_pipes=1 if is_tooling else 0,
            fabrics=len(content_fabrics),
            mapped_pipes=0 if is_tooling else (1 if matched_schema else 0),
            unmapped_pipes=0 if is_tooling or matched_schema else 1,
            sor_pipes=len(sor_set),
            other_pipes=len(other_set),
            rows=rows,
            records=rows,
            dispatch_id=did,
            aod_run_id=export_run_id,
        ))
        if not created:
            # Another worker beat us — fall through to update so this
            # pipe's data still gets counted in the existing entry.
            store.update_content_activity(did, rows, pipe_id)
    elif did:
        # Subsequent push — update the existing content entry
        # Sync from Redis BEFORE reading counts to get other worker's pipes
        store._sync_content_sets(did)

        store.update_content_activity(did, rows, pipe_id)

        # Track tooling pipes separately
        if is_tooling:
            store._content_tooling.setdefault(did, set()).add(pipe_id)
        else:
            # Only track mapped/unmapped for SOR pipes
            if matched_schema:
                store._content_mapped.setdefault(did, set()).add(pipe_id)
            else:
                store._content_unmapped.setdefault(did, set()).add(pipe_id)

        # Track fabric planes
        fabrics_set = store._content_fabrics.setdefault(did, set())
        if pipe_fabric:
            fabrics_set.add(pipe_fabric)
        # Track SOR vs other
        if is_sor:
            store._content_sor_pipes.setdefault(did, set()).add(pipe_id)
        else:
            store._content_other_pipes.setdefault(did, set()).add(pipe_id)
        # Track sources (using pipe_def vendor, not raw source_system)
        sources = store._content_sources.setdefault(did, set())
        sources.add(vendor_source)

        # Persist to Redis AFTER mutation so other workers see updated sets
        store._persist_content_sets(did)

        # Read counts AFTER sync+mutate — reflects the union of all workers
        mapped_set = store._content_mapped.get(did, set())
        unmapped_set = store._content_unmapped.get(did, set())
        tooling_set = store._content_tooling.get(did, set())
        sor_set = store._content_sor_pipes.get(did, set())
        other_set = store._content_other_pipes.get(did, set())
        with store._lock:
            for entry in reversed(store._activity_log):
                if entry.phase == "content" and entry.dispatch_id == did:
                    entry.sors = aod_sor_count  # AOD authority — same basis as structure phase
                    entry.tooling_pipes = len(tooling_set)
                    entry.fabrics = len(fabrics_set)
                    entry.mapped_pipes = len(mapped_set)
                    entry.unmapped_pipes = len(unmapped_set)
                    entry.sor_pipes = len(sor_set)
                    entry.other_pipes = len(other_set)
                    break
        store._mark_activity_dirty()
    else:
        # No dispatch_id and no export receipt — standalone push
        logger.error(
            f"[Activity] Content push from {source_system} pipe={pipe_id} "
            f"has no dispatch_id and no AAM export receipt. "
            f"Cannot link to a 3-phase cycle."
        )
        store.record_activity(ActivityEntry(
            phase="content",
            source="Farm",
            snapshot_name=snapshot_name,
            run_id=run_id,
            timestamp=now,
            pipes=1,
            sors=aod_sor_count,
            tooling_pipes=1 if is_tooling else 0,
            mapped_pipes=0 if is_tooling else (1 if matched_schema else 0),
            unmapped_pipes=0 if is_tooling or matched_schema else 1,
            rows=rows,
            records=rows,
            dispatch_id="",
        ))


# ---------------------------------------------------------------------------
# Connectivity ping
# ---------------------------------------------------------------------------

@router.get("")
def dcl_ingest_ping():
    """Connectivity check — Farm can GET this to verify the ingest endpoint is reachable."""
    store = get_ingest_store()
    stats = store.get_stats()
    return {
        "status": "ready",
        "message": "POST payloads to this URL. GET is for connectivity testing only.",
        "ingest_stats": stats,
    }


# ---------------------------------------------------------------------------
# Data push (Path 3 — Content Path)
# ---------------------------------------------------------------------------

def _process_ingest_sync(
    *,
    raw_body: dict,
    now: str,
    x_run_id: str | None,
    x_pipe_id: str | None,
    x_dispatch_id: str | None,
    x_schema_hash: str | None,
    x_api_key: str | None,
) -> IngestResponse:
    """Synchronous ingest processing — runs in thread pool to keep event loop free.

    All Pydantic validation, Redis lookups, schema hashing, zlib compression,
    and store persistence happen here. HTTPException propagates back through
    the executor to FastAPI's exception handler.
    """
    if isinstance(raw_body, dict):
        try:
            body = _normalize_ingest_body(raw_body)
        except HTTPException as e:
            pipe_id = x_pipe_id or raw_body.get("pipe_id", "unknown")
            source = raw_body.get("source_system") or raw_body.get("source", "unknown")
            tenant = raw_body.get("tenant_id") or raw_body.get("tenantId", "")
            get_ingest_store().record_drop(DropEntry(
                pipe_id=pipe_id,
                reason=e.detail if isinstance(e.detail, str) else str(e.detail),
                error_code="MISSING_SNAPSHOT",
                source_system=source,
                timestamp=now,
                run_id=x_run_id or "",
                tenant_id=tenant,
            ))
            raise
    else:
        body = raw_body

    try:
        ingest_req = IngestRequest(**body)
    except Exception as e:
        logger.error(f"[Ingest] Validation failed: {e} | keys={list(body.keys()) if isinstance(body, dict) else 'N/A'}")
        pipe_id = x_pipe_id or (body.get("pipe_id", "unknown") if isinstance(body, dict) else "unknown")
        source = (body.get("source_system", "unknown") if isinstance(body, dict) else "unknown")
        snapshot = (body.get("snapshot_name", "") if isinstance(body, dict) else "")
        tenant = (body.get("tenant_id", "") or body.get("tenantId", "")) if isinstance(body, dict) else ""
        get_ingest_store().record_drop(DropEntry(
            pipe_id=pipe_id,
            reason=str(e),
            error_code="VALIDATION_ERROR",
            source_system=source,
            timestamp=now,
            run_id=x_run_id or "",
            snapshot_name=snapshot,
            tenant_id=tenant,
        ))
        raise HTTPException(status_code=422, detail=str(e))

    # ── Farm self-directed bypass ──
    _is_farm_push = (x_run_id or "").startswith("farm_")

    # ── Source system allowlist gate ──
    canonical_sources = get_canonical_sources()
    if not canonical_sources and not _is_farm_push:
        raise HTTPException(
            status_code=503,
            detail={
                "error": "DCL not initialized",
                "action": "Run AAM /export-pipes to register pipe definitions before ingesting",
            },
        )

    if not _is_farm_push:
        pipe_id = x_pipe_id or f"pipe_{ingest_req.source_system}"
        pipe_store = get_pipe_store()
        pipe_def_for_src = pipe_store.lookup(pipe_id)
        if pipe_def_for_src and pipe_def_for_src.vendor:
            canonical_src = normalize_source_id(pipe_def_for_src.vendor)
            if canonical_src != normalize_source_id(ingest_req.source_system):
                logger.info(
                    f"[Ingest] Source override: request said '{ingest_req.source_system}', "
                    f"pipe_def vendor='{pipe_def_for_src.vendor}' → canonical='{canonical_src}' "
                    f"(pipe_id={pipe_id})"
                )
        else:
            canonical_src = normalize_source_id(ingest_req.source_system)

        if canonical_src not in canonical_sources:
            logger.warning(
                f"[Ingest] REJECTED non-canonical source: '{ingest_req.source_system}' "
                f"(canonical: '{canonical_src}') tenant={ingest_req.tenant_id} "
                f"snapshot={ingest_req.snapshot_name} rows={ingest_req.row_count}"
            )
            get_ingest_store().record_drop(DropEntry(
                pipe_id=pipe_id,
                reason=f"Non-canonical source: '{ingest_req.source_system}' (canonical: '{canonical_src}')",
                error_code="NON_CANONICAL_SOURCE",
                source_system=ingest_req.source_system,
                timestamp=now,
                run_id=x_run_id or "",
                snapshot_name=ingest_req.snapshot_name,
                tenant_id=ingest_req.tenant_id,
            ))
            raise HTTPException(
                status_code=422,
                detail=f"Source '{ingest_req.source_system}' (canonical: '{canonical_src}') "
                       f"is not in the canonical allowlist. "
                       f"Allowed sources: {sorted(canonical_sources)}"
            )
    else:
        canonical_src = normalize_source_id(ingest_req.source_system)
        logger.info(
            f"[Ingest] Farm self-directed push: skipping canonical source "
            f"gate (run_id={x_run_id}, source={ingest_req.source_system})"
        )

    expected_key = os.environ.get("DCL_INGEST_KEY")
    if expected_key and x_api_key != expected_key:
        pipe_id = x_pipe_id or f"pipe_{ingest_req.source_system}"
        get_ingest_store().record_drop(DropEntry(
            pipe_id=pipe_id,
            reason="Invalid or missing x-api-key",
            error_code="AUTH_FAILED",
            source_system=ingest_req.source_system,
            timestamp=now,
            run_id=x_run_id or "",
            snapshot_name=ingest_req.snapshot_name,
            tenant_id=ingest_req.tenant_id,
        ))
        raise HTTPException(status_code=401, detail="Invalid or missing x-api-key")

    run_id = x_run_id or str(uuid.uuid4())
    pipe_id = x_pipe_id or f"pipe_{ingest_req.source_system}"

    # ── Dedup gate ──
    store = get_ingest_store()
    existing_receipt = store.get_receipt(run_id, pipe_id)
    if existing_receipt is not None:
        logger.info(
            f"[Ingest] DEDUP: pipe_id={pipe_id} run_id={run_id} already ingested "
            f"({existing_receipt.row_count} rows at {existing_receipt.received_at}). "
            f"Returning cached receipt."
        )
        pipe_def = get_pipe_store().lookup(pipe_id)
        return IngestResponse(
            status="deduplicated",
            dcl_run_id=run_id,
            run_id=run_id,
            dispatch_id=existing_receipt.dispatch_id,
            pipe_id=pipe_id,
            rows_accepted=existing_receipt.row_count,
            schema_drift=existing_receipt.schema_drift,
            drift_fields=existing_receipt.drift_fields,
            matched_schema=pipe_def is not None,
            schema_fields=pipe_def.fields if pipe_def else [],
            timestamp=existing_receipt.received_at,
            warnings=["Duplicate push detected — returning cached receipt"],
        )

    if x_dispatch_id:
        dispatch_id = x_dispatch_id
    else:
        dispatch_id = _derive_dispatch_id(
            ingest_req.run_timestamp, ingest_req.tenant_id, ingest_req.snapshot_name
        )

    # ── Schema-on-write gate ──
    pipe_def, _guard_active = _validate_pipe_guard(
        pipe_id, run_id, ingest_req.source_system, now,
        tenant_id=ingest_req.tenant_id, snapshot_name=ingest_req.snapshot_name,
    )

    # ── Proceed with ingest ──
    if x_schema_hash:
        schema_hash = x_schema_hash
    else:
        schema_hash = compute_schema_hash(ingest_req.rows)

    actual_rows = len(ingest_req.rows)
    if actual_rows != ingest_req.row_count:
        logger.warning(
            f"[Ingest] Row count mismatch: declared={ingest_req.row_count} "
            f"actual={actual_rows} pipe={pipe_id} run={run_id}"
        )

    try:
        receipt = store.ingest(
            run_id=run_id,
            pipe_id=pipe_id,
            schema_hash=schema_hash,
            request=ingest_req,
            dispatch_id=dispatch_id,
            canonical_source_override=canonical_src,
        )
    except Exception as e:
        logger.error(f"[Ingest] Failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

    # Auto-promote mode from Empty → Ingest when real data arrives.
    if get_current_mode().data_mode == "Empty":
        set_current_mode("Ingest", run_id=run_id)
        logger.info(f"[Ingest] Mode auto-promoted: Empty → Ingest (run_id={run_id})")

    matched_schema = pipe_def is not None
    schema_fields = pipe_def.fields if pipe_def else []

    pipe_store_for_sor = get_pipe_store()
    aod_sor_list = pipe_store_for_sor.get_aod_systems_of_record()
    aod_sor_count = len(aod_sor_list)

    ingest_warnings: list[str] = []
    if not pipe_store_for_sor.get_export_receipts():
        ingest_warnings.append(
            "Content received before structure — 3-phase sequence is broken. "
            "AAM must push /export-pipes before Farm pushes /ingest."
        )
    elif aod_sor_count == 0:
        ingest_warnings.append(
            "No AOD SOR declarations found in latest export receipt. "
            "SOR count is 0 — check that AAM forwards systems_of_record from AOD."
        )

    _record_ingest_activity(
        store=store,
        dispatch_id=dispatch_id,
        snapshot_name=ingest_req.snapshot_name,
        run_id=run_id,
        pipe_id=pipe_id,
        source_system=ingest_req.source_system,
        rows=actual_rows,
        matched_schema=matched_schema,
        now=now,
        aod_sor_count=aod_sor_count,
    )

    # Materialize metric data points (fire-and-forget in background thread)
    def _materialize_sync():
        try:
            from backend.engine.metric_materializer import get_materializer
            materializer = get_materializer()
            mat_points = materializer.materialize(
                pipe_id=pipe_id,
                source_system=ingest_req.source_system,
                rows=ingest_req.rows,
                dispatch_id=dispatch_id,
            )
            if mat_points:
                mat_key = f"{run_id}:{pipe_id}"
                store.store_materialized(mat_key, mat_points)
                logger.info(
                    f"[Ingest] Materialized {len(mat_points)} data points "
                    f"from {pipe_id} ({ingest_req.source_system})"
                )
        except Exception as e:
            logger.error(
                f"[Ingest] Materialization failed for pipe_id={pipe_id} "
                f"source={ingest_req.source_system} run_id={run_id}: {e}"
            )

    _materialize_pool.submit(_materialize_sync)

    logger.info(
        f"[Ingest] Accepted {actual_rows} rows from {ingest_req.source_system} "
        f"pipe={pipe_id} run={run_id} drift={receipt.schema_drift} "
        f"matched_schema={matched_schema}"
    )

    return IngestResponse(
        status="ingested",
        dcl_run_id=run_id,
        run_id=run_id,
        dispatch_id=dispatch_id,
        pipe_id=pipe_id,
        rows_accepted=actual_rows,
        schema_drift=receipt.schema_drift,
        drift_fields=receipt.drift_fields,
        matched_schema=matched_schema,
        schema_fields=schema_fields,
        timestamp=now,
        warnings=ingest_warnings,
    )


@router.post("", response_model=IngestResponse)
async def dcl_ingest(
    request: Request,
    x_run_id: Optional[str] = Header(None),
    x_pipe_id: Optional[str] = Header(None),
    x_schema_hash: Optional[str] = Header(None),
    x_api_key: Optional[str] = Header(None),
    x_dispatch_id: Optional[str] = Header(None, alias="x-dispatch-id"),
):
    """
    Accept a data push from Farm (Path 3 — Content Path).

    Schema-on-write validation: before accepting any payload, checks
    that a matching pipe definition exists (registered via /export-pipes).
    If no match, returns HTTP 422 NO_MATCHING_PIPE.

    If no pipe definitions have been registered at all (export-pipes
    not yet called), the guard is bypassed with a WARNING log so
    existing Farm self-directed flows continue working.
    """
    now = utc_now()

    # Log raw request info for debugging connectivity issues
    client_host = request.client.host if request.client else "unknown"
    content_type = request.headers.get("content-type", "missing")
    content_length = request.headers.get("content-length", "missing")
    logger.info(
        f"[Ingest] Incoming POST from {client_host} | "
        f"content-type={content_type} content-length={content_length} | "
        f"x-run-id={x_run_id} x-pipe-id={x_pipe_id}"
    )

    try:
        # Read body bytes async, then offload JSON parse to thread pool.
        # json.loads() on a 26MB payload takes 2-5s and blocks the event loop,
        # starving health checks and queries during ingest bursts.
        raw_bytes = await request.body()
        loop = asyncio.get_running_loop()
        raw_body = await loop.run_in_executor(None, json.loads, raw_bytes)
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        logger.error(
            f"[Ingest] JSON parse failed from {client_host}: {e} | "
            f"raw body ({len(raw_bytes)} bytes): {raw_bytes[:500]!r}"
        )
        raise HTTPException(status_code=400, detail=f"Invalid JSON body: {e}")
    except Exception as e:
        logger.error(f"[Ingest] Body read/parse failed from {client_host}: {e}")
        raise HTTPException(status_code=400, detail=f"Invalid request body: {e}")

    logger.info(f"[Ingest] Received keys: {list(raw_body.keys()) if isinstance(raw_body, dict) else type(raw_body).__name__}")

    # ── Offload ALL sync-heavy processing to thread pool ──
    # With 89 concurrent ingest requests, even small sync work per request
    # (Pydantic validation, Redis lookups, schema hashing, zlib compression)
    # starves the event loop and blocks health checks. A single executor call
    # per request keeps the event loop free for health/query endpoints.
    result = await loop.run_in_executor(
        None,
        lambda: _process_ingest_sync(
            raw_body=raw_body,
            now=now,
            x_run_id=x_run_id,
            x_pipe_id=x_pipe_id,
            x_dispatch_id=x_dispatch_id,
            x_schema_hash=x_schema_hash,
            x_api_key=x_api_key,
        ),
    )
    return result


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------

@router.get("/runs")
def list_ingest_runs():
    """List all ingestion run receipts (metadata only)."""
    store = get_ingest_store()
    receipts = store.get_all_receipts()
    return {
        "runs": [
            {
                "run_id": r.run_id,
                "dispatch_id": r.dispatch_id,
                "pipe_id": r.pipe_id,
                "source_system": r.source_system,
                "canonical_source_id": r.canonical_source_id,
                "tenant_id": r.tenant_id,
                "snapshot_name": r.snapshot_name,
                "run_timestamp": r.run_timestamp,
                "received_at": r.received_at,
                "schema_version": r.schema_version,
                "row_count": r.row_count,
                "schema_drift": r.schema_drift,
                "drift_fields": r.drift_fields,
                "runner_id": r.runner_id,
            }
            for r in receipts
        ],
        "stats": store.get_stats(),
    }


@router.get("/batches")
def list_ingest_batches():
    """List ingestion batches grouped by snapshot_name."""
    store = get_ingest_store()
    return {"batches": store.get_batches()}


@router.get("/runs/{run_id}")
def get_ingest_run(run_id: str):
    """Get all pipe receipts for a Farm run_id."""
    store = get_ingest_store()
    receipts = store.get_receipts_by_run(run_id)
    if not receipts:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")
    pipes = []
    for receipt in receipts:
        rows = store.get_rows(receipt.run_id, receipt.pipe_id)
        pipes.append({
            "run_id": receipt.run_id,
            "pipe_id": receipt.pipe_id,
            "source_system": receipt.source_system,
            "canonical_source_id": receipt.canonical_source_id,
            "row_count": receipt.row_count,
            "rows_buffered": len(rows),
            "schema_drift": receipt.schema_drift,
        })
    return {
        "run_id": run_id,
        "pipe_count": len(pipes),
        "total_rows": sum(p["row_count"] for p in pipes),
        "pipes": pipes,
    }


@router.get("/drift")
def list_schema_drift():
    """List all schema drift events."""
    store = get_ingest_store()
    events = store.get_drift_events()
    return {
        "drift_events": [
            {
                "pipe_id": e.pipe_id,
                "run_id": e.run_id,
                "previous_hash": e.previous_hash,
                "incoming_hash": e.incoming_hash,
                "added_fields": e.added_fields,
                "removed_fields": e.removed_fields,
                "detected_at": e.detected_at,
            }
            for e in events
        ],
        "total": len(events),
    }


# Module-level cache for ingest stats.
_stats_cache: Optional[dict] = None
_stats_cache_gen: int = -1


@router.get("/stats")
def get_ingest_stats():
    """Quick summary of what's in the ingest store."""
    global _stats_cache, _stats_cache_gen
    store = get_ingest_store()
    current_gen = store.generation
    if _stats_cache is not None and _stats_cache_gen == current_gen:
        return _stats_cache
    result = store.get_stats()
    _stats_cache = result
    _stats_cache_gen = current_gen
    return result


@router.get("/activity")
def list_activity(snapshot_name: Optional[str] = None):
    """Return the 3-phase activity log — discrete events for Structure, Dispatch, Content.

    Each entry represents one phase of the data flow:
      - structure: AAM pushed pipe schemas via /export-pipes
      - dispatch:  AAM/Farm manifest activated a dispatch
      - content:   Farm pushed actual row data via /ingest

    Optional ?snapshot_name= filter.
    """
    store = get_ingest_store()
    entries = store.get_activity_log(snapshot_name=snapshot_name)

    # Group by snapshot_name for easy frontend rendering
    grouped: dict = {}
    for e in entries:
        snap = e["snapshot_name"]
        grouped.setdefault(snap, []).append(e)

    return {
        "activity": entries,
        "by_snapshot": grouped,
        "total": len(entries),
    }


@router.get("/drops")
def list_drops(snapshot_name: Optional[str] = None):
    """Return the drop log — rejected ingestion attempts.

    Optional ?snapshot_name= filter.
    """
    store = get_ingest_store()
    entries = store.get_drop_log(snapshot_name=snapshot_name)

    grouped: dict = {}
    for e in entries:
        snap = e["snapshot_name"] or "(no snapshot)"
        grouped.setdefault(snap, []).append(e)

    return {
        "drops": entries,
        "by_snapshot": grouped,
        "total": len(entries),
    }


@router.get("/dispatches")
def list_dispatches(snapshot_name: Optional[str] = None):
    """List all Farm dispatches — each dispatch groups pipes from one manifest push.

    Optional ?snapshot_name= filter to show only dispatches from a specific
    Farm generation (e.g. 'cloudedge-a1b2').
    """
    store = get_ingest_store()
    return {"dispatches": store.get_dispatches(snapshot_name=snapshot_name)}


@router.get("/dispatches/{dispatch_id}")
def get_dispatch_detail(dispatch_id: str):
    """Get detailed breakdown for a single Farm dispatch."""
    store = get_ingest_store()
    summary = store.get_dispatch_summary(dispatch_id)
    if not summary:
        raise HTTPException(status_code=404, detail=f"Dispatch {dispatch_id} not found")
    return summary


# ---------------------------------------------------------------------------
# Materialization endpoints
# ---------------------------------------------------------------------------

@router.post("/materialize")
def backfill_materialize():
    """Re-run materialization on all existing buffered rows.

    Use this to backfill materialized data points for rows that were
    ingested before the materializer was deployed. Safe to run multiple
    times — overwrites previous materialization for each key.
    """
    from backend.engine.metric_materializer import get_materializer

    store = get_ingest_store()
    materializer = get_materializer()

    total_points = 0
    total_keys = 0
    metrics_found: set = set()

    all_receipts = store.get_all_receipts()
    for receipt in all_receipts:
        key = f"{receipt.run_id}:{receipt.pipe_id}"
        rows = store.get_rows(receipt.run_id, receipt.pipe_id)
        if not rows:
            continue

        points = materializer.materialize(
            pipe_id=receipt.pipe_id,
            source_system=receipt.source_system,
            rows=rows,
            dispatch_id=receipt.dispatch_id,
        )
        if points:
            store.store_materialized(key, points)
            total_points += len(points)
            total_keys += 1
            for pt in points:
                metrics_found.add(pt["metric"])

    store._save_to_disk()

    logger.info(
        f"[Materialize] Backfill complete: {total_points} points "
        f"from {total_keys} pipe pushes, {len(metrics_found)} metrics"
    )

    return {
        "status": "complete",
        "total_points": total_points,
        "total_keys": total_keys,
        "metrics": sorted(metrics_found),
        "receipts_scanned": len(all_receipts),
    }


@router.get("/materialized/stats")
def get_materialized_stats():
    """Show materialized metric counts, period ranges, source systems."""
    store = get_ingest_store()
    return store.get_materialized_stats()


@router.get("/sample")
def sample_rows(pipe_id: Optional[str] = None, limit: int = 3):
    """Sample raw rows from the ingest buffer for debugging.

    Optional ?pipe_id= to filter by pipe. Returns up to `limit` rows
    from matching receipts.
    """
    store = get_ingest_store()
    all_receipts = store.get_all_receipts()

    samples = []
    for receipt in reversed(all_receipts):
        if pipe_id and receipt.pipe_id != pipe_id:
            continue
        rows = store.get_rows(receipt.run_id, receipt.pipe_id)
        if not rows:
            continue
        sample_rows = rows[:limit]
        # Strip internal tags for readability
        cleaned = [
            {k: v for k, v in row.items() if not k.startswith("_")}
            for row in sample_rows
        ]
        samples.append({
            "pipe_id": receipt.pipe_id,
            "source_system": receipt.source_system,
            "total_rows": len(rows),
            "sample": cleaned,
        })
        if len(samples) >= 3:
            break

    return {"samples": samples}


# ---------------------------------------------------------------------------
# Flush ingest store (admin maintenance)
# ---------------------------------------------------------------------------

@router.post("/flush")
async def flush_ingest_store():
    """Flush all ingest data AND pipe definitions.

    Clears: IngestStore (memory + Redis + disk) and PipeStore (memory +
    Redis + Postgres + disk).  After flush the schema-on-write guard is
    inactive (pipe count = 0), allowing the next Farm push through
    without pre-registered AAM blueprints.

    Both reset() methods now handle their own Redis/Postgres/disk cleanup
    internally — no duplicate cleanup needed here.
    """
    store = get_ingest_store()
    pipe_store = get_pipe_store()

    # Snapshot before counts
    stats_before = store.get_stats()
    mat_before = store.get_materialized_stats()
    pipes_before = pipe_store.count()

    # reset() handles memory + Redis + disk for each store
    store.reset()
    pipe_store.reset()  # handles memory + Redis + Postgres + disk

    # Reset mode to Empty — no data in memory
    set_current_mode("Empty")

    return {
        "status": "flushed",
        "mode": "Empty",
        "message": "No data in memory. Run a new snapshot or initiate an enterprise scan.",
        "before": {
            "total_runs": stats_before.get("total_runs", 0),
            "total_rows": stats_before.get("total_rows_buffered", 0),
            "materialized_points": mat_before.get("total_points", 0),
            "unique_sources": stats_before.get("unique_sources", 0),
            "pipe_definitions": pipes_before,
        },
        "after": {
            "total_runs": 0,
            "total_rows": 0,
            "materialized_points": 0,
            "unique_sources": 0,
            "pipe_definitions": 0,
        },
    }


# ---------------------------------------------------------------------------
# Seed endpoint — replay remote activity/drops into local store (dev only)
# ---------------------------------------------------------------------------

@router.post("/seed", summary="Seed local IngestStore from remote snapshot (dev)")
async def seed_ingest_store(request: Request):
    """Accept a JSON payload with 'activity' and/or 'drops' arrays
    and inject them directly into the local IngestStore.
    """
    payload = await request.json()
    store = get_ingest_store()

    activity_items = payload.get("activity", [])
    drop_items = payload.get("drops", [])

    added_activity = 0
    added_drops = 0

    with store._lock:
        for item in activity_items:
            entry = ActivityEntry(
                phase=item.get("phase", ""),
                source=item.get("source", ""),
                snapshot_name=item.get("snapshot_name", ""),
                run_id=item.get("run_id", ""),
                timestamp=item.get("timestamp", ""),
                pipes=item.get("pipes", 0),
                sors=item.get("sors", 0),
                tooling_pipes=item.get("tooling_pipes", 0),
                fabrics=item.get("fabrics", 0),
                mapped_pipes=item.get("mapped_pipes", 0),
                unmapped_pipes=item.get("unmapped_pipes", 0),
                rows=item.get("rows", 0),
                records=item.get("records", 0),
                sor_pipes=item.get("sor_pipes", 0),
                other_pipes=item.get("other_pipes", 0),
                dispatch_id=item.get("dispatch_id", ""),
                aod_run_id=item.get("aod_run_id", ""),
            )
            store._activity_log.append(entry)
            added_activity += 1

        for item in drop_items:
            entry = DropEntry(
                pipe_id=item.get("pipe_id", ""),
                reason=item.get("reason", ""),
                error_code=item.get("error_code", ""),
                source_system=item.get("source_system", ""),
                timestamp=item.get("timestamp", ""),
                run_id=item.get("run_id", ""),
                dispatch_id=item.get("dispatch_id", ""),
                snapshot_name=item.get("snapshot_name", ""),
                tenant_id=item.get("tenant_id", ""),
            )
            store._drop_log.append(entry)
            added_drops += 1

    # Persist to disk
    store._save_to_disk()

    return {
        "status": "seeded",
        "activity_added": added_activity,
        "drops_added": added_drops,
    }
