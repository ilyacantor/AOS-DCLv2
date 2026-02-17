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

import os
import uuid

from fastapi import APIRouter, HTTPException, Header, Request
from typing import Optional

from backend.core.constants import utc_now
from backend.api.ingest import (
    ActivityEntry,
    IngestRequest,
    IngestResponse,
    get_ingest_store,
    compute_schema_hash,
    _derive_dispatch_id,
)
from backend.api.pipe_store import get_pipe_store
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/api/dcl/ingest", tags=["Ingestion"])


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


def _validate_pipe_guard(pipe_id: str, run_id: str, source_system: str, now: str):
    """Schema-on-write gate. Returns (pipe_def, guard_active) or raises 422."""
    pipe_store = get_pipe_store()
    pipe_def = pipe_store.lookup(pipe_id)
    guard_active = pipe_store.count() > 0

    if guard_active and pipe_def is None:
        logger.error(
            f"[Ingest] REJECTED: No matching pipe definition for pipe_id={pipe_id} "
            f"(run_id={run_id}, source={source_system}). "
            f"Available pipes: {pipe_store.list_pipe_ids()}"
        )
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
) -> None:
    """Record Path 2 (dispatch) and Path 3 (content) activity entries.

    Path 2 — "dispatch": recorded ONCE when the first pipe for a new
    dispatch_id arrives. This signals that the AAM/Farm manifest has
    been activated and data is flowing.

    Path 3 — "content": one entry per dispatch, incremented on each
    successive pipe push so the Ingest tab shows accumulated totals.
    """
    pipe_store = get_pipe_store()

    # --- Path 2: Dispatch activity (first push for this dispatch) ---
    if dispatch_id and not store.has_dispatch_activity(dispatch_id):
        total_expected = pipe_store.count()
        store.record_activity(ActivityEntry(
            phase="dispatch",
            source="AAM/Farm",
            snapshot_name=snapshot_name,
            run_id=run_id,
            timestamp=now,
            pipes=total_expected,
            dispatch_id=dispatch_id,
        ))

        # --- Path 3: Create initial content entry for this dispatch ---
        store.record_activity(ActivityEntry(
            phase="content",
            source="Farm",
            snapshot_name=snapshot_name,
            run_id=run_id,
            timestamp=now,
            pipes=1,
            mapped_pipes=1 if matched_schema else 0,
            unmapped_pipes=0 if matched_schema else 1,
            rows=rows,
            records=rows,
            dispatch_id=dispatch_id,
        ))
    elif dispatch_id:
        # Subsequent push — update the existing content entry
        store.update_content_activity(dispatch_id, rows, pipe_id)
        # Also update mapped/unmapped on the content entry
        with store._lock:
            for entry in reversed(store._activity_log):
                if entry.phase == "content" and entry.dispatch_id == dispatch_id:
                    if matched_schema:
                        entry.mapped_pipes += 1
                    else:
                        entry.unmapped_pipes += 1
                    break
    else:
        # No dispatch_id — standalone push, record as content directly
        store.record_activity(ActivityEntry(
            phase="content",
            source="Farm",
            snapshot_name=snapshot_name,
            run_id=run_id,
            timestamp=now,
            pipes=1,
            mapped_pipes=1 if matched_schema else 0,
            unmapped_pipes=0 if matched_schema else 1,
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
    existing Demo/Farm self-directed flows continue working.
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
        raw_body = await request.json()
    except Exception as e:
        raw_bytes = await request.body()
        logger.error(
            f"[Ingest] JSON parse failed from {client_host}: {e} | "
            f"raw body ({len(raw_bytes)} bytes): {raw_bytes[:500]!r}"
        )
        raise HTTPException(status_code=400, detail=f"Invalid JSON body: {e}")

    logger.info(f"[Ingest] Received keys: {list(raw_body.keys()) if isinstance(raw_body, dict) else type(raw_body).__name__}")

    if isinstance(raw_body, dict):
        body = _normalize_ingest_body(raw_body)
    else:
        body = raw_body

    try:
        ingest_req = IngestRequest(**body)
    except Exception as e:
        logger.error(f"[Ingest] Validation failed: {e} | keys={list(body.keys()) if isinstance(body, dict) else 'N/A'}")
        raise HTTPException(status_code=422, detail=str(e))

    expected_key = os.environ.get("DCL_INGEST_KEY")
    if expected_key and x_api_key != expected_key:
        raise HTTPException(status_code=401, detail="Invalid or missing x-api-key")

    run_id = x_run_id or str(uuid.uuid4())
    pipe_id = x_pipe_id or f"pipe_{ingest_req.source_system}"

    if x_dispatch_id:
        dispatch_id = x_dispatch_id
    else:
        dispatch_id = _derive_dispatch_id(
            ingest_req.run_timestamp, ingest_req.tenant_id, ingest_req.snapshot_name
        )

    # ── Schema-on-write gate ──
    pipe_def, _guard_active = _validate_pipe_guard(pipe_id, run_id, ingest_req.source_system, now)

    # ── Proceed with ingest ──────────────────────────────────────────
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

    store = get_ingest_store()
    try:
        receipt = store.ingest(
            run_id=run_id,
            pipe_id=pipe_id,
            schema_hash=schema_hash,
            request=ingest_req,
            dispatch_id=dispatch_id,
        )
    except Exception as e:
        logger.error(f"[Ingest] Failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

    # Build enriched response with schema join confirmation
    matched_schema = pipe_def is not None
    schema_fields = pipe_def.fields if pipe_def else []

    # --- Record Path 2 + Path 3 activity ---
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
    )

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
    )


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


@router.get("/stats")
def get_ingest_stats():
    """Quick summary of what's in the ingest store."""
    store = get_ingest_store()
    return store.get_stats()


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
