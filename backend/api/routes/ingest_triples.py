"""
Semantic triple ingest endpoint.

POST   /api/dcl/ingest-triples          — batch ingest triples
GET    /api/dcl/ingest-status/{run_id}  — run status
GET    /api/dcl/ingest-status           — list all runs
GET    /api/dcl/ingest-log              — ingest activity log
"""

import json
import os
import time
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import httpx
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field
from typing import Optional

from backend.core.db import get_connection, PoolExhausted
from backend.db.triple_store import TripleStore
from backend.engine.persona_view import get_persona_domain_mapping
from backend.farm.client import get_farm_client
from backend.registry.concept_registry import ConceptRegistry
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

router = APIRouter(tags=["Triple Ingest"])

_triple_store = TripleStore()
_concept_registry = ConceptRegistry()

# Union of every domain prefix mapped to a persona, built at import time.
# Farm-emitted triples whose concept prefix is absent from this set are
# mapping drift — the ingest refuses them with 422 so the error surfaces
# at the seam between Farm and DCL, not downstream in graph build.
_MAPPED_DOMAIN_PREFIXES: frozenset[str] = frozenset(
    domain
    for domains in get_persona_domain_mapping().values()
    for domain in domains
)

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

    domain_prefix = t.concept.split(".", 1)[0]
    if domain_prefix not in _MAPPED_DOMAIN_PREFIXES:
        raise HTTPException(
            status_code=422,
            detail={
                "error": "UNMAPPED_DOMAIN",
                "message": (
                    f"Triple #{index}: domain prefix '{domain_prefix}' from concept "
                    f"'{t.concept}' is not mapped to any persona in "
                    f"config/persona_domains.yaml. This is Farm generator drift — "
                    f"add '{domain_prefix}' to the correct persona in the YAML "
                    f"so the graph builder can route L3 nodes to L4 personas."
                ),
                "domain_prefix": domain_prefix,
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

    # Build triple dicts grouped by entity — each entity gets its own swap call.
    # Farm ingests one entity per batch today; the grouping generalization keeps
    # us safe against future multi-entity batches without adding a separate code path.
    by_entity: dict[str, list[dict]] = {}
    for t in req.triples:
        by_entity.setdefault(t.entity_id, []).append({
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
    # Envelope entity_id must match the batch when it is single-entity.
    # For multi-entity batches, envelope entity_id is treated as a rollup label
    # and is echoed back in the response without constraint.
    if req.entity_id and len(by_entity) == 1 and req.entity_id not in by_entity:
        raise HTTPException(
            status_code=422,
            detail={
                "error": "ENTITY_ID_MISMATCH",
                "message": (
                    f"Envelope entity_id={req.entity_id} does not match the single "
                    f"batch entity {sorted(by_entity.keys())[0]}."
                ),
            },
        )
    entity_ids = sorted(by_entity.keys())
    source_systems = sorted({t.source_system for t in req.triples if t.source_system})

    # --- Instrumentation: capture timing around the write ---
    triples_received = sum(len(v) for v in by_entity.values())
    count = 0
    previous_run_ids: dict[str, str | None] = {}
    archived_totals = 0
    start_ts = time.monotonic()
    try:
        for eid in entity_ids:
            erows = by_entity[eid]
            if append:
                n = _triple_store.append_rows_for_entity(
                    tenant_id=str(req.tenant_id),
                    entity_id=eid,
                    new_run_id=str(req.dcl_ingest_id),
                    new_rows=erows,
                )
                count += n
                previous_run_ids[eid] = None
            else:
                prev_run, archived, _new_row_count = _triple_store.swap_and_delete(
                    tenant_id=str(req.tenant_id),
                    entity_id=eid,
                    new_run_id=str(req.dcl_ingest_id),
                    snapshot_name=req.snapshot_name,
                    new_rows=erows,
                    replace=replace,
                )
                count += len(erows)
                previous_run_ids[eid] = prev_run
                archived_totals += archived
    except ValueError as ve:
        raise HTTPException(
            status_code=422,
            detail={"error": "INGEST_VALIDATION", "message": str(ve)},
        )
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

    logger.info(
        f"[ingest-triples] tenant_id={req.tenant_id} "
        f"run_id={req.dcl_ingest_id} entities={entity_ids} "
        f"mode={'append' if append else 'swap'} inserted={count} "
        f"archived={archived_totals} previous_run_ids={previous_run_ids} "
        f"duration={duration_ms}ms"
    )

    # concept_summary is derived from the already-validated in-memory batch,
    # not a post-txn SELECT. The request has authoritative ground truth for
    # what we just wrote — a round-trip to semantic_triples to re-count
    # domains is pure overhead in the hot path.
    concept_summary = dict(
        Counter(t.concept.split(".", 1)[0] for t in req.triples)
    )

    logger.info(
        f"[ingest-triples] Ingested {count} triples for dcl_ingest_id={req.dcl_ingest_id}, "
        f"tenant_id={req.tenant_id}, concepts={concept_summary}, duration={duration_ms}ms"
    )

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

    # On replace ingest, update seed_manifest.json so tests point at the live run.
    # Gated off in the live Farm/Console loop — the file is a diagnostic pointer
    # used by tests/test_s1_seed.py and scripts/seed_database.py, not a hot-path
    # artifact. Running file I/O + an extra SELECT per chunked POST is pure
    # overhead. Operators that want live manifest updates set
    # DCL_AUTO_UPDATE_SEED_MANIFEST=1 in the dcl-backend env.
    if (
        replace
        and not run_exists
        and os.environ.get("DCL_AUTO_UPDATE_SEED_MANIFEST") == "1"
    ):
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
        })
    return result


# ---------------------------------------------------------------------------
# POST /api/dcl/refresh-from-farm
#
# Compare DCL's per-entity `tenant_runs.updated_at` against Farm's triple-run
# timestamps. For each (tenant, entity) pair where Farm has a newer SE run
# (mode != multi_entity), trigger Farm's existing push-to-dcl endpoint so
# Farm streams the triples through the normal /api/dcl/ingest-triples write
# path (swap_and_delete, cap enforcement). Sequential per-entity to keep
# tenant_runs cap enforcement atomic (deferred item #12).
# ---------------------------------------------------------------------------

class RefreshIngestedEntity(BaseModel):
    entity_id: str
    tenant_id: str
    farm_manifest_id: str
    dcl_ingest_id: Optional[str] = None
    triples_written: Optional[int] = None
    farm_timestamp: Optional[str] = None


class RefreshSkippedEntity(BaseModel):
    entity_id: str
    tenant_id: Optional[str] = None
    farm_manifest_id: Optional[str] = None
    reason: str


class RefreshFromFarmResponse(BaseModel):
    ingested: list[RefreshIngestedEntity]
    skipped: list[RefreshSkippedEntity]
    message: str


def _read_dcl_tenant_runs_state() -> dict[tuple[str, str], datetime]:
    """Return {(tenant_id, entity_id): updated_at} for every live row."""
    sql = "SELECT tenant_id, entity_id, updated_at FROM tenant_runs"
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
    except PoolExhausted as e:
        raise HTTPException(
            status_code=503,
            detail=(
                f"DCL database pool exhausted while reading tenant_runs — "
                f"too many concurrent requests. {e}"
            ),
        )
    state: dict[tuple[str, str], datetime] = {}
    for tenant_id, entity_id, updated_at in rows:
        if not entity_id or updated_at is None:
            continue
        state[(str(tenant_id), str(entity_id))] = updated_at
    return state


def _parse_farm_timestamp(raw: Optional[str]) -> Optional[datetime]:
    """Parse Farm's ISO timestamp to an aware datetime, or None."""
    if not raw:
        return None
    value = raw.replace("Z", "+00:00") if raw.endswith("Z") else raw
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _select_newer_farm_runs(
    farm_runs: list[dict],
    dcl_state: dict[tuple[str, str], datetime],
) -> tuple[list[dict], list[RefreshSkippedEntity]]:
    """Pick the newest completed Farm manifest run per (tenant, entity) that
    beats DCL's updated_at.

    `farm_runs` is Farm's `/api/runs` feed — manifest_runs rows with single
    entity_id per row (SE by construction). Non-completed rows are skipped
    silently. The row with the largest created_at wins per (tenant, entity),
    identified by its `farm_run_id` (which is the key Farm's push-to-dcl
    endpoint expects).

    Refresh updates entities DCL already knows about; it does not backfill
    arbitrary history. An entity absent from DCL's tenant_runs is recorded as
    skipped with a clear reason so the operator sees it in the UI summary
    — introducing new entities into DCL is the pipeline's job, not Refresh's.
    """
    candidates: dict[tuple[str, str], dict] = {}
    skipped: list[RefreshSkippedEntity] = []
    unknown_to_dcl: set[tuple[str, str]] = set()

    for run in farm_runs:
        status = run.get("status")
        if status != "completed":
            continue  # Only replay successful runs.

        farm_run_id = run.get("farm_run_id")
        tenant_id = run.get("tenant_id")
        entity_id = run.get("entity_id")
        run_id_logical = run.get("run_id")

        if not farm_run_id or not tenant_id or not entity_id:
            continue

        farm_ts = _parse_farm_timestamp(run.get("created_at"))
        if farm_ts is None:
            skipped.append(RefreshSkippedEntity(
                entity_id=str(entity_id),
                tenant_id=str(tenant_id),
                farm_manifest_id=str(farm_run_id),
                reason="Farm manifest_runs row is missing a parseable created_at.",
            ))
            continue

        key = (str(tenant_id), str(entity_id))
        dcl_ts = dcl_state.get(key)
        if dcl_ts is None:
            # Unknown to DCL — deduped; recorded once in the outer pass.
            unknown_to_dcl.add(key)
            continue
        if farm_ts <= dcl_ts:
            continue
        existing = candidates.get(key)
        if existing is None or farm_ts > existing["farm_ts"]:
            candidates[key] = {
                "farm_run_id": str(farm_run_id),
                "run_id": str(run_id_logical) if run_id_logical else str(farm_run_id),
                "tenant_id": str(tenant_id),
                "entity_id": str(entity_id),
                "farm_ts": farm_ts,
            }

    for tenant_id, entity_id in unknown_to_dcl:
        skipped.append(RefreshSkippedEntity(
            entity_id=entity_id,
            tenant_id=tenant_id,
            farm_manifest_id=None,
            reason=(
                "Entity is present in Farm but not in DCL tenant_runs. "
                "Refresh only updates entities DCL already tracks; run the "
                "full pipeline to introduce this entity to DCL."
            ),
        ))

    return list(candidates.values()), skipped


def _extract_ingest_summary(push_result: dict) -> tuple[Optional[str], Optional[int]]:
    """Pull dcl_ingest_id and triples_written out of Farm's push-to-dcl response.

    Farm returns a flat object with `dcl_ingest_id` (UUID string) and
    `rows_accepted`/`pushed` (count of triples actually written).
    """
    if not isinstance(push_result, dict):
        return None, None
    dcl_ingest_id = push_result.get("dcl_ingest_id") or push_result.get("dcl_run_id")
    # Prefer rows_accepted (DCL-confirmed write count) over pushed (Farm-sent).
    count = push_result.get("rows_accepted")
    if count is None:
        count = push_result.get("pushed")
    if count is None:
        count = push_result.get("rows_pushed")
    triples_written: Optional[int] = None
    if count is not None:
        try:
            triples_written = int(count)
        except (TypeError, ValueError):
            triples_written = None
    return (
        str(dcl_ingest_id) if dcl_ingest_id is not None else None,
        triples_written,
    )


@router.post("/api/dcl/refresh-from-farm", response_model=RefreshFromFarmResponse)
def refresh_from_farm():
    """Pull Farm SE manifest runs newer than DCL's per-entity updated_at and
    re-ingest them.

    Flow:
      1. SELECT (tenant_id, entity_id, updated_at) FROM tenant_runs.
      2. GET Farm /api/runs — SE manifest_runs feed. No mode filter needed;
         /api/runs is SE by construction (per-row entity_id). ME manifests
         live in /api/business-data/triple-runs and route to Convergence.
      3. For each completed run, if Farm created_at > DCL updated_at for that
         (tenant, entity), keep it as a candidate. Newest wins per key.
      4. Sequentially POST Farm /api/runs/{farm_run_id}/push-to-dcl. Farm
         reconstructs the JobManifest, regenerates (deterministic seed), and
         pushes to DCL /api/dcl/ingest-triples (swap_and_delete + LIFO cap).
      5. Return a summary of ingested + skipped entities.

    Sequential per-entity pushes avoid the tenant_runs cap race (deferred #12).
    Farm unreachable / malformed responses surface as 502 with plain-English
    messages — no silent fallback (A1).
    """
    farm_client = get_farm_client()

    try:
        # Pull a wide window. The selector keys off DCL's tenant_runs, so
        # rows for entities DCL doesn't track are filtered out cheaply and
        # don't cause churn. The limit just bounds how far back we'll look
        # for each DCL-known entity's latest Farm run.
        farm_payload = farm_client.list_manifest_runs(limit=500)
    except httpx.ConnectError as e:
        raise HTTPException(
            status_code=502,
            detail=(
                f"DCL could not reach Farm at {farm_client.base_url}/api/runs "
                f"— connection refused ({e}). Start the Farm service (port 8003) and retry."
            ),
        )
    except httpx.TimeoutException as e:
        raise HTTPException(
            status_code=504,
            detail=(
                f"DCL timed out waiting for Farm at {farm_client.base_url}/api/runs "
                f"({e}). Retry once Farm responds."
            ),
        )
    except httpx.HTTPStatusError as e:
        raise HTTPException(
            status_code=502,
            detail=(
                f"Farm returned HTTP {e.response.status_code} for /api/runs: "
                f"{e.response.text[:300]}"
            ),
        )

    # /api/runs returns a bare list of manifest_run rows.
    if not isinstance(farm_payload, list):
        raise HTTPException(
            status_code=502,
            detail=(
                f"Farm /api/runs returned malformed payload "
                f"(expected list of manifest_runs rows, got {type(farm_payload).__name__})."
            ),
        )
    farm_runs = farm_payload

    dcl_state = _read_dcl_tenant_runs_state()
    candidates, skipped = _select_newer_farm_runs(farm_runs, dcl_state)

    if not candidates:
        return RefreshFromFarmResponse(
            ingested=[],
            skipped=skipped,
            message="No Farm runs newer than DCL — nothing to ingest.",
        )

    ingested: list[RefreshIngestedEntity] = []

    for candidate in candidates:
        try:
            push_result = farm_client.push_run_to_dcl(candidate["farm_run_id"])
        except httpx.HTTPStatusError as e:
            skipped.append(RefreshSkippedEntity(
                entity_id=candidate["entity_id"],
                tenant_id=candidate["tenant_id"],
                farm_manifest_id=candidate["farm_run_id"],
                reason=(
                    f"Farm push-to-dcl returned HTTP {e.response.status_code}: "
                    f"{e.response.text[:200]}"
                ),
            ))
            continue
        except (httpx.ConnectError, httpx.TimeoutException) as e:
            skipped.append(RefreshSkippedEntity(
                entity_id=candidate["entity_id"],
                tenant_id=candidate["tenant_id"],
                farm_manifest_id=candidate["farm_run_id"],
                reason=f"Farm push-to-dcl transport error: {e}",
            ))
            continue
        except Exception as e:  # noqa: BLE001 — surface any push failure in skipped, do not swallow
            logger.error(
                f"[refresh-from-farm] unexpected error pushing farm_run_id={candidate['farm_run_id']}: {e}",
                exc_info=True,
            )
            skipped.append(RefreshSkippedEntity(
                entity_id=candidate["entity_id"],
                tenant_id=candidate["tenant_id"],
                farm_manifest_id=candidate["farm_run_id"],
                reason=f"Farm push-to-dcl raised {type(e).__name__}: {str(e)[:200]}",
            ))
            continue

        dcl_ingest_id, triples_written = _extract_ingest_summary(push_result)
        ingested.append(RefreshIngestedEntity(
            entity_id=candidate["entity_id"],
            tenant_id=candidate["tenant_id"],
            farm_manifest_id=candidate["farm_run_id"],
            dcl_ingest_id=dcl_ingest_id,
            triples_written=triples_written,
            farm_timestamp=candidate["farm_ts"].isoformat(),
        ))

    parts = [f"Ingested {len(ingested)} new Farm run(s)."]
    if skipped:
        parts.append(f"{len(skipped)} skipped — see details.")
    message = " ".join(parts)

    logger.info(
        f"[refresh-from-farm] candidates={len(candidates)} ingested={len(ingested)} "
        f"skipped={len(skipped)}"
    )

    return RefreshFromFarmResponse(
        ingested=ingested,
        skipped=skipped,
        message=message,
    )


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
