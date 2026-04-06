"""
Semantic triple ingest endpoint.

POST   /api/dcl/ingest-triples         — batch ingest triples
GET    /api/dcl/ingest-status/{run_id}  — run status
GET    /api/dcl/ingest-status           — list all runs
GET    /api/dcl/ingest-log              — ingest activity log
DELETE /api/dcl/purge-inactive          — hard-delete deactivated triples
"""

import json
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field
from typing import Optional

from backend.core.db import get_connection
from backend.db.triple_store import TripleStore
from backend.registry.concept_registry import ConceptRegistry
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

router = APIRouter(tags=["Triple Ingest"])

_triple_store = TripleStore()
_concept_registry = ConceptRegistry()

# Defense-in-depth: reject ME entity_ids at ingest boundary.
# DCL is SE-only — ME data routes to Convergence (port 8010).
# Parsed once at import time from comma-separated env var.
_blocked_raw = os.environ.get("DCL_BLOCKED_ENTITY_IDS", "").strip()
_BLOCKED_ENTITY_IDS: frozenset[str] = frozenset(
    eid.strip().lower() for eid in _blocked_raw.split(",") if eid.strip()
)


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------

class TriplePayload(BaseModel):
    entity_id: str
    concept: str
    property: str
    value: object
    period: Optional[str] = None
    currency: Optional[str] = "USD"
    unit: Optional[str] = None
    source_system: str
    source_table: Optional[str] = None
    source_field: Optional[str] = None
    pipe_id: Optional[str] = None
    confidence_score: float
    confidence_tier: str
    canonical_id: Optional[str] = None
    resolution_method: Optional[str] = None
    resolution_confidence: Optional[float] = None
    fabric_plane: Optional[str] = None
    fabric_product: Optional[str] = None


class IngestRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    tenant_id: str
    dcl_ingest_id: str = Field(..., alias="run_id")
    source_run_tag: Optional[str] = None
    source_farm_manifest_id: Optional[str] = None
    entity_id: Optional[str] = None
    source_rows: Optional[int] = None
    snapshot_name: Optional[str] = None
    triples: list[TriplePayload]


class IngestResponse(BaseModel):
    dcl_ingest_id: str
    tenant_id: str
    entity_id: Optional[str] = None
    source_farm_manifest_id: Optional[str] = None
    triple_count: int
    concept_summary: dict
    source_rows: int
    triples_written: int
    expansion_factor: float


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

def _validate_uuid(value: str, field_name: str) -> None:
    """Raise HTTPException if value is not a valid UUID."""
    try:
        uuid.UUID(value)
    except (ValueError, AttributeError):
        raise HTTPException(
            status_code=400,
            detail={
                "error": "VALIDATION_FAILED",
                "message": f"'{field_name}' must be a valid UUID. Got: {value!r}",
            },
        )


_VALID_TIERS = {"exact", "high", "medium", "low"}
_VALID_RESOLUTION_METHODS = {"deterministic", "fuzzy", "manual", None}


def _validate_triple(t: TriplePayload, index: int) -> None:
    """Validate a single triple. Raises HTTPException on failure."""
    if not t.entity_id or not t.entity_id.strip():
        raise HTTPException(
            status_code=400,
            detail={
                "error": "VALIDATION_FAILED",
                "message": f"Triple #{index}: entity_id is required and must be non-empty.",
            },
        )

    if not t.concept or not t.concept.strip():
        raise HTTPException(
            status_code=400,
            detail={
                "error": "VALIDATION_FAILED",
                "message": f"Triple #{index}: concept is required and must be non-empty.",
            },
        )

    if not _concept_registry.is_valid_concept(t.concept):
        raise HTTPException(
            status_code=400,
            detail={
                "error": "INVALID_CONCEPT",
                "message": f"Triple #{index}: concept '{t.concept}' is not a registered concept. "
                           f"Root segment must match a known ontology concept.",
                "concept": t.concept,
            },
        )

    if not t.property or not t.property.strip():
        raise HTTPException(
            status_code=400,
            detail={
                "error": "VALIDATION_FAILED",
                "message": f"Triple #{index}: property is required and must be non-empty.",
            },
        )

    if t.value is None:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "VALIDATION_FAILED",
                "message": f"Triple #{index}: value is required and must not be null.",
            },
        )

    if not t.source_system or not t.source_system.strip():
        raise HTTPException(
            status_code=400,
            detail={
                "error": "VALIDATION_FAILED",
                "message": f"Triple #{index}: source_system is required and must be non-empty.",
            },
        )

    if not (0.0 <= t.confidence_score <= 1.0):
        raise HTTPException(
            status_code=400,
            detail={
                "error": "VALIDATION_FAILED",
                "message": f"Triple #{index}: confidence_score must be between 0.0 and 1.0. Got: {t.confidence_score}",
            },
        )

    if t.confidence_tier not in _VALID_TIERS:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "VALIDATION_FAILED",
                "message": f"Triple #{index}: confidence_tier must be one of {_VALID_TIERS}. Got: {t.confidence_tier!r}",
            },
        )

    if t.resolution_method is not None and t.resolution_method not in {"deterministic", "fuzzy", "manual"}:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "VALIDATION_FAILED",
                "message": f"Triple #{index}: resolution_method must be deterministic/fuzzy/manual or null. Got: {t.resolution_method!r}",
            },
        )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("/api/dcl/ingest-triples", status_code=201, response_model=IngestResponse)
def ingest_triples(
    req: IngestRequest,
    replace: bool = Query(False),
    append: bool = Query(False),
):
    """
    Batch ingest semantic triples.

    - Validates all triples before inserting any (atomic batch).
    - If run_id already exists: returns 409 unless ?replace=true or ?append=true.
    - With ?replace=true: deactivates old triples, inserts new ones.
    - With ?append=true: skips idempotency check, adds triples to existing run.
      Use this for multi-batch ingestion where the caller sends the same run_id
      across multiple requests (e.g. Farm pushing 18K triples in 1K batches).
    """
    _validate_uuid(req.tenant_id, "tenant_id")
    _validate_uuid(req.dcl_ingest_id, "dcl_ingest_id")

    if not req.triples:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "VALIDATION_FAILED",
                "message": "triples list must not be empty.",
            },
        )

    # Validate every triple BEFORE any DB writes (batch atomicity)
    for i, t in enumerate(req.triples):
        _validate_triple(t, i)

    # ME entity_id boundary guard — DCL is SE-only.
    if _BLOCKED_ENTITY_IDS:
        blocked_found = {
            t.entity_id for t in req.triples
            if t.entity_id.lower() in _BLOCKED_ENTITY_IDS
        }
        if blocked_found:
            raise HTTPException(
                status_code=422,
                detail={
                    "error": "ME_ENTITY_REJECTED",
                    "message": (
                        f"DCL rejected entity_ids {sorted(blocked_found)} — "
                        f"ME data routes to Convergence (port 8010), not DCL (port 8004). "
                        f"Check Farm routing config or Console pipeline orchestrator."
                    ),
                    "blocked_entity_ids": sorted(blocked_found),
                },
            )

    # Idempotency check — skipped when append=true (multi-batch ingestion)
    run_exists = _triple_store.run_exists(req.dcl_ingest_id)
    if run_exists and not replace and not append:
        raise HTTPException(
            status_code=409,
            detail={
                "error": "RUN_ALREADY_EXISTS",
                "message": f"dcl_ingest_id {req.dcl_ingest_id} already has triples in the store. "
                           "Use ?replace=true to deactivate old triples and re-ingest, "
                           "or ?append=true to add more triples to this run.",
                "dcl_ingest_id": req.dcl_ingest_id,
            },
        )

    # When replace=true, all existing triples for this tenant are atomically
    # deleted and replaced with the new batch inside a single transaction.
    # The tenant_runs pointer is updated after the replace completes.
    if run_exists and replace:
        logger.info(
            f"[ingest-triples] replace=true for existing dcl_ingest_id={req.dcl_ingest_id}; "
            f"inserting new triples, pointer will be updated after insert"
        )

    # Build triple dicts for insertion
    rows = []
    for t in req.triples:
        rows.append({
            "tenant_id": req.tenant_id,
            "entity_id": t.entity_id,
            "concept": t.concept,
            "property": t.property,
            "value": t.value,
            "period": t.period,
            "currency": t.currency,
            "unit": t.unit,
            "source_system": t.source_system,
            "source_table": t.source_table,
            "source_field": t.source_field,
            "pipe_id": t.pipe_id,
            "run_id": req.dcl_ingest_id,  # DB column
            "source_run_tag": req.source_run_tag,
            "confidence_score": t.confidence_score,
            "confidence_tier": t.confidence_tier,
            "canonical_id": t.canonical_id,
            "resolution_method": t.resolution_method,
            "resolution_confidence": t.resolution_confidence,
            "fabric_plane": t.fabric_plane,
            "fabric_product": t.fabric_product,
        })

    # --- Instrumentation: capture timing around the write ---
    triples_received = len(rows)
    entity_ids = list({r["entity_id"] for r in rows if r.get("entity_id")})
    source_systems = sorted({r["source_system"] for r in rows if r.get("source_system")})

    start_ts = time.monotonic()
    try:
        if replace:
            count = _triple_store.replace_tenant_triples(str(req.tenant_id), rows)
        else:
            count = _triple_store.insert_triples(rows)
    except Exception as db_err:
        duration_ms = int((time.monotonic() - start_ts) * 1000)
        logger.error(
            f"[ingest-triples] DB write failed after {duration_ms}ms for "
            f"dcl_ingest_id={req.dcl_ingest_id}, tenant_id={req.tenant_id}, "
            f"triples_attempted={triples_received}: {db_err}",
            exc_info=True,
        )
        err_str = str(db_err)
        if "statement timeout" in err_str or "canceling statement" in err_str:
            raise HTTPException(
                status_code=504,
                detail={
                    "error": "INGEST_STATEMENT_TIMEOUT",
                    "message": (
                        f"Triple INSERT timed out after {duration_ms}ms "
                        f"({triples_received} triples). The database statement "
                        f"timeout was exceeded — the batch may be too large for "
                        f"current Supabase PG capacity."
                    ),
                    "triples_attempted": triples_received,
                    "duration_ms": duration_ms,
                },
            )
        raise HTTPException(
            status_code=503,
            detail={
                "error": "INGEST_DB_ERROR",
                "message": f"Database write failed: {err_str[:300]}",
                "triples_attempted": triples_received,
                "duration_ms": duration_ms,
            },
        )
    duration_ms = int((time.monotonic() - start_ts) * 1000)

    # Atomic pointer swap + deactivation — single transaction.
    # Not set for append=true (multi-batch ingest of the same run_id keeps
    # whatever pointer was set by the initial replace ingest).
    if not append:
        previous_run_id, deactivated = _triple_store.swap_and_deactivate(
            str(req.tenant_id), str(req.dcl_ingest_id),
            snapshot_name=req.snapshot_name,
        )
        logger.info(
            f"[ingest-triples] tenant_runs updated: tenant_id={req.tenant_id} "
            f"→ current_run_id={req.dcl_ingest_id} (previous={previous_run_id}, "
            f"deactivated={deactivated})"
        )

    concept_summary = _triple_store.count_by_domain(req.tenant_id, run_id=req.dcl_ingest_id)

    logger.info(
        f"[ingest-triples] Ingested {count} triples for dcl_ingest_id={req.dcl_ingest_id}, "
        f"tenant_id={req.tenant_id}, concepts={concept_summary}, duration={duration_ms}ms"
    )

    # Safety warning: flag if table is growing beyond expected bounds
    if replace:
        try:
            total = _triple_store.count_total_rows()
            if total > 200_000:
                logger.warning(
                    "[ingest-triples] semantic_triples has %s total rows "
                    "(threshold: 200,000). Consider running POST /api/dcl/purge-old-runs "
                    "for tenant_id=%s.",
                    f"{total:,}", req.tenant_id,
                )
        except Exception:
            logger.warning("[ingest-triples] Failed to check total row count", exc_info=True)

    # Record to ingest_log — observability only, never fails the ingest
    _record_ingest_log(
        run_id=req.dcl_ingest_id,
        tenant_id=req.tenant_id,
        entity_id=entity_ids[0] if len(entity_ids) == 1 else None,
        source_systems=source_systems,
        triples_received=triples_received,
        triples_written=count,
        duration_ms=duration_ms,
    )

    # On replace ingest, update seed_manifest.json so tests point at the live run
    if replace and not run_exists:
        _update_seed_manifest(req.tenant_id, req.dcl_ingest_id, count, concept_summary)

    # Determine batch-level entity_id: explicit request field takes priority,
    # then infer from triples if all share a single entity_id.
    batch_entity_id = req.entity_id
    if batch_entity_id is None and len(entity_ids) == 1:
        batch_entity_id = entity_ids[0]

    source_rows_val = req.source_rows if req.source_rows is not None else triples_received
    expansion = round(count / source_rows_val, 1) if source_rows_val > 0 else 0.0

    return IngestResponse(
        dcl_ingest_id=req.dcl_ingest_id,
        tenant_id=req.tenant_id,
        entity_id=batch_entity_id,
        source_farm_manifest_id=req.source_farm_manifest_id,
        triple_count=count,
        concept_summary=concept_summary,
        source_rows=source_rows_val,
        triples_written=count,
        expansion_factor=expansion,
    )


@router.get("/api/dcl/ingest-status/{run_id}")
def get_ingest_status(run_id: str):
    """Get ingest status for a specific run."""
    _validate_uuid(run_id, "run_id")

    info = _triple_store.get_run_info(run_id)
    if info is None:
        raise HTTPException(
            status_code=404,
            detail={
                "error": "RUN_NOT_FOUND",
                "message": f"No triples found for run_id={run_id}.",
            },
        )

    concept_summary = _triple_store.count_by_domain(tenant_id=None, run_id=run_id)
    return {
        "dcl_ingest_id": str(info["run_id"]),
        "triple_count": info["triple_count"],
        "concept_summary": concept_summary,
        "created_at": info["created_at"].isoformat() if info["created_at"] else None,
        "is_active": info["is_active"],
    }


@router.get("/api/dcl/ingest-status")
def list_ingest_status():
    """List all ingest runs, most recent first."""
    runs = _triple_store.list_runs()
    result = []
    for r in runs:
        result.append({
            "dcl_ingest_id": str(r["run_id"]),
            "tenant_id": str(r["tenant_id"]),
            "triple_count": r["triple_count"],
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            "is_active": r["is_active"],
        })
    return result


# ---------------------------------------------------------------------------
# Ingest activity log
# ---------------------------------------------------------------------------

def _record_ingest_log(
    run_id: str,
    tenant_id: str,
    entity_id: str | None,
    source_systems: list[str],
    triples_received: int,
    triples_written: int,
    duration_ms: int,
    triples_rejected: int = 0,
    rejection_reasons: list | None = None,
) -> None:
    """Write a row to ingest_log. Failure is logged, never raised."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO ingest_log "
                    "(run_id, entity_id, tenant_id, triples_received, triples_written, "
                    " triples_rejected, rejection_reasons, source_systems, duration_ms) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (
                        run_id, entity_id, tenant_id,
                        triples_received, triples_written,
                        triples_rejected,
                        json.dumps(rejection_reasons or []),
                        source_systems,
                        duration_ms,
                    ),
                )
                conn.commit()
    except Exception as e:
        logger.warning(f"[ingest-log] Failed to record ingest log: {e}")


@router.get("/api/dcl/ingest-log")
def get_ingest_log(
    limit: int = Query(20, ge=1, le=200),
    entity_id: Optional[str] = Query(None),
    run_id: Optional[str] = Query(None),
):
    """Return recent ingest log entries, newest first."""
    clauses = []
    params: list = []

    if entity_id:
        clauses.append("entity_id = %s")
        params.append(entity_id)
    if run_id:
        clauses.append("run_id = %s")
        params.append(run_id)

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""

    sql = (
        f"SELECT id, run_id, entity_id, tenant_id, "
        f"triples_received, triples_written, triples_rejected, "
        f"rejection_reasons, source_systems, duration_ms, created_at "
        f"FROM ingest_log {where} "
        f"ORDER BY created_at DESC LIMIT %s"
    )
    params.append(limit)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            columns = [desc[0] for desc in cur.description]
            rows = []
            for row in cur.fetchall():
                d = dict(zip(columns, row))
                d["id"] = str(d["id"])
                d["dcl_ingest_id"] = str(d.pop("run_id"))
                d["tenant_id"] = str(d["tenant_id"])
                if d["created_at"]:
                    d["created_at"] = d["created_at"].isoformat()
                rows.append(d)

    return rows


# ---------------------------------------------------------------------------
# Purge inactive triples
# ---------------------------------------------------------------------------

@router.delete("/api/dcl/purge-inactive")
def purge_inactive(confirm: bool = Query(False)):
    """Hard-delete all deactivated triples from the database.

    Requires ?confirm=true as a safety gate — this is a maintenance operation
    that permanently removes historical data.
    """
    if not confirm:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "CONFIRMATION_REQUIRED",
                "message": (
                    "This will permanently delete all inactive triples. "
                    "Pass ?confirm=true to proceed."
                ),
            },
        )

    deleted = _triple_store.delete_inactive()
    logger.info(f"[purge-inactive] Hard-deleted {deleted} inactive triples")
    return {"deleted": deleted}


@router.post("/api/dcl/purge-old-runs")
def purge_old_runs(tenant_id: str, keep_runs: int = 2):
    """Hard-delete triples from old runs for a tenant, keeping the N most recent.

    The current run (pointed to by tenant_runs.current_run_id) is always among
    the kept runs — it is the most recent by definition.

    Args:
        tenant_id: Tenant UUID.
        keep_runs: Number of most recent run_ids to keep (default 2).
    """
    _validate_uuid(tenant_id, "tenant_id")
    if keep_runs < 1:
        raise HTTPException(
            status_code=400,
            detail={"error": "INVALID_PARAM", "message": "keep_runs must be >= 1"},
        )
    deleted = _triple_store.purge_old_runs(tenant_id, keep_runs)
    logger.info(
        f"[purge-old-runs] Deleted {deleted} triples for tenant_id={tenant_id}, "
        f"kept_runs={keep_runs}"
    )
    return {"deleted": deleted, "tenant_id": tenant_id, "kept_runs": keep_runs}


@router.post("/api/dcl/admin/purge-stale")
def purge_stale_all_tenants():
    """Hard-delete all non-current-run triples across every known tenant.

    Iterates all tenant_ids from tenant_runs, calls purge_old_runs(keep_runs=1)
    for each. Current run data is always preserved.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT tenant_id FROM tenant_runs")
            tenant_ids = [str(row[0]) for row in cur.fetchall()]
    total_deleted = 0
    for tid in tenant_ids:
        total_deleted += _triple_store.purge_old_runs(tid, keep_runs=1)
    logger.warning(
        "[purge-stale-all] Deleted %d stale triples across %d tenant(s)",
        total_deleted, len(tenant_ids),
    )
    return {"deleted": total_deleted, "tenants_purged": len(tenant_ids)}


# ---------------------------------------------------------------------------
# Seed manifest update
# ---------------------------------------------------------------------------

_MANIFEST_PATH = Path(__file__).resolve().parents[3] / "data" / "seed_manifest.json"


def _update_seed_manifest(
    tenant_id: str,
    run_id: str,
    triple_count: int,
    concept_summary: dict,
) -> None:
    """Update data/seed_manifest.json with current active run info.

    Two-gate protection:
    1. Only overwrites if the new run has at least as many concept domains as
       the existing manifest (prevents thin pipeline runs from stomping richer data).
    2. Rejects runs whose entity_ids are all UUID-format strings — those are
       pipeline runs with misconfigured entity_id.
    """
    try:
        existing = {}
        if _MANIFEST_PATH.exists():
            existing = json.loads(_MANIFEST_PATH.read_text())

        existing_concept_count = len(existing.get("concept_summary", {}))
        new_concept_count = len(concept_summary)
        if existing_concept_count > new_concept_count:
            logger.info(
                f"[ingest-triples] Skipping seed_manifest.json update: existing has "
                f"{existing_concept_count} concept domains vs {new_concept_count} in new run"
            )
            return

        # Gate 2: reject runs where all entity_ids look like UUIDs.
        # Valid runs have human-readable entity_ids (e.g. "CloudLabs-9OSV").
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT DISTINCT entity_id FROM semantic_triples "
                    "WHERE run_id = %s AND entity_id IS NOT NULL LIMIT 20",
                    (run_id,),
                )
                entity_ids = [str(r[0]) for r in cur.fetchall()]

        _UUID_RE = __import__("re").compile(
            r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
            __import__("re").IGNORECASE,
        )
        all_uuid = entity_ids and all(_UUID_RE.match(eid) for eid in entity_ids)
        if all_uuid:
            logger.info(
                f"[ingest-triples] Skipping seed_manifest.json update: run {run_id} "
                f"has UUID-format entity_ids {entity_ids[:3]} — likely a misconfigured pipeline run"
            )
            return

        existing.update({
            "seed_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "dcl_ingest_id": run_id,
            "tenant_id": tenant_id,
            "total_triples": triple_count,
            "concept_summary": concept_summary,
        })

        _MANIFEST_PATH.write_text(json.dumps(existing, indent=2) + "\n")
        logger.info(f"[ingest-triples] Updated seed_manifest.json: run_id={run_id}")
    except Exception as e:
        # Manifest update is informational — log but don't fail the ingest
        logger.warning(f"[ingest-triples] Failed to update seed_manifest.json: {e}")
