"""Structural drift sweep — Gate 3B D1.

run_structural_drift_sweep() is the job body called by:
  - APScheduler on its interval (via drift_job() wrapper)
  - POST /api/dcl/monitor/schedule/structural_drift/run-now (same function,
    synchronous HTTP path — NOT a test-only backdoor, same codepath as the
    scheduler fires)

Detection:
  Bounded query (#56) for all tenant/entity pairs that have BOTH a current
  and previous ingest run (previous_run_id IS NOT NULL in tenant_runs).
  For each pair, calls triple_store.diff_runs() and extracts structural changes:
  concept·property keys PRESENT in one run and ABSENT in the other.
  'changed' rows (same key, different value) are value drift — excluded here.

Filing:
  One structural_drift proposal per entity drift event (tenant-scoped).
  Dedup is explicit: check_duplicates finds any pending proposal for the same
  (tenant, 'structural_drift', natural_key) and reports it — never ON CONFLICT
  DO NOTHING (A1: silent fallbacks forbidden; duplicate suppression must be
  visible in the return value).

Errors:
  Each entity is wrapped individually. Errors are collected and surfaced in the
  return dict; the sweep itself completes (no bail-out on the first error). The
  APScheduler wrapper (drift_job) updates last_status='error' when any entity
  failed, so the monitor/schedule API shows degraded state.
"""

import json
from datetime import datetime, timezone
from typing import Any

from backend.core.db import get_connection
from backend.db.triple_store import TripleStore
from backend.db.proposal_store import ProposalStore
from backend.db.conflict_store import ConflictStore
from backend.db.monitor_store import MonitorStore
from backend.engine.conflict_detection import detect_and_register
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

_TENANT_LIMIT = 100      # bounded (#56): max entity pairs scanned per sweep
_DIFF_SAMPLE_LIMIT = 500  # bounded (#56): max samples from diff_runs per entity


def _get_entity_pairs(limit: int) -> list[tuple[str, str, str, str]]:
    """Bounded query: returns (tenant_id, entity_id, current_run_id, previous_run_id)
    for all entities that have a base→compare run pair. Ordered by most-recently
    updated first so freshest drift surfaces first when the list is truncated."""
    sql = """
        SELECT tenant_id::text, entity_id,
               current_run_id::text, previous_run_id::text
        FROM tenant_runs
        WHERE previous_run_id IS NOT NULL
        ORDER BY updated_at DESC NULLS LAST
        LIMIT %s
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (max(1, min(limit, 1000)),))
            return cur.fetchall()


def run_structural_drift_sweep() -> dict[str, Any]:
    """One pass of the structural drift sweep. Returns a summary dict.

    Raises RuntimeError if any entity scan failed (after completing all others)
    so the APScheduler job records status='error' and the error detail is visible
    at GET /api/dcl/monitor/schedule. A1: failures are never silent.
    """
    triple_store = TripleStore()
    proposal_store = ProposalStore()

    pairs = _get_entity_pairs(limit=_TENANT_LIMIT)

    entities_scanned = 0
    drift_findings = 0
    proposals_filed = 0
    proposals_deduped = 0
    errors: list[str] = []

    now_iso = datetime.now(timezone.utc).isoformat()

    for tenant_id, entity_id, current_run_id, previous_run_id in pairs:
        entities_scanned += 1
        try:
            diff = triple_store.diff_runs(
                tenant_id, entity_id,
                base_run_id=previous_run_id,
                compare_run_id=current_run_id,
                limit=_DIFF_SAMPLE_LIMIT,
            )

            added = [
                {"concept": s["concept"], "property": s["property"]}
                for s in diff["samples"]["added"]
            ]
            removed = [
                {"concept": s["concept"], "property": s["property"]}
                for s in diff["samples"]["removed"]
            ]

            if not added and not removed:
                continue

            drift_findings += 1

            natural_key = f"{entity_id.lower()}|{previous_run_id.lower()}|{current_run_id.lower()}"
            dup_map = proposal_store.check_duplicates(
                tenant_id, [("structural_drift", natural_key)]
            )
            existing_id = dup_map.get(("structural_drift", natural_key))
            if existing_id:
                proposals_deduped += 1
                logger.info(
                    "[structural_drift_monitor] entity=%s tenant=%s — "
                    "pending proposal already exists: %s (deduped)",
                    entity_id, tenant_id, existing_id,
                )
                continue

            payload = {
                "entity_id": entity_id,
                "tenant_id": tenant_id,
                "dcl_ingest_id_base": previous_run_id,
                "dcl_ingest_id_compare": current_run_id,
                "added": added,
                "removed": removed,
            }
            provenance = {
                "basis": "inferred",
                "source": "structural_drift_monitor",
                "detected_at": now_iso,
                "dcl_ingest_id_base": previous_run_id,
                "dcl_ingest_id_compare": current_run_id,
            }

            proposal_store.insert_proposals([{
                "tenant_id": tenant_id,
                "entity_id": entity_id,
                "proposal_type": "structural_drift",
                "natural_key": natural_key,
                "payload": payload,
                "confidence": 1.0,
                "provenance": provenance,
            }])
            proposals_filed += 1

            logger.info(
                "[structural_drift_monitor] entity=%s tenant=%s — "
                "filed structural_drift proposal: added=%d removed=%d",
                entity_id, tenant_id, len(added), len(removed),
            )

        except Exception as exc:
            msg = (
                f"entity={entity_id} tenant={tenant_id} "
                f"base={previous_run_id} compare={current_run_id}: {exc}"
            )
            logger.error(
                "[structural_drift_monitor] entity scan FAILED: %s", msg, exc_info=True
            )
            errors.append(msg)

    result = {
        "entities_scanned": entities_scanned,
        "drift_findings": drift_findings,
        "proposals_filed": proposals_filed,
        "proposals_deduped": proposals_deduped,
        "errors": errors,
    }

    if errors:
        raise RuntimeError(
            f"[structural_drift_monitor] {len(errors)} entity scan(s) failed "
            f"(scanned={entities_scanned} findings={drift_findings} "
            f"filed={proposals_filed}): {errors[:3]}"
        )

    return result


def _vd_natural_key(entity_id: str, conflict: dict) -> str:
    """Natural key for a value_drift proposal: entity·concept·property·period.
    Stable across re-detections — same conceptual conflict → same key → dedup."""
    concept = conflict.get("concept", "")
    prop = conflict.get("property", "")
    period = conflict.get("period") or ""
    return f"{entity_id.lower()}|{concept.lower()}|{prop.lower()}|{period.lower()}"


def run_value_drift_sweep() -> dict[str, Any]:
    """One pass of the value drift sweep. Returns a summary dict.

    For each tenant·entity pair that has a current run, calls the Gate 1A
    detect_and_register engine to refresh the conflict_register, then files
    a value_drift proposal for each newly-open value conflict that has no
    pending proposal yet.

    Trend framing: the payload carries prior_count (open value conflicts before
    detection) and current_count (after), so an operator sees drift direction,
    not just a snapshot.

    Dedup (explicit — never ON CONFLICT DO NOTHING, A1):
      - skip if conflict_register status is already 'dispositioned'
      - skip if a pending value_drift proposal already exists for this key

    Bounded (#56): at most _TENANT_LIMIT entity pairs per sweep.
    Raises RuntimeError (collected) on any per-entity failure so the scheduler
    records last_status='error' — failures are never silent (A1).
    """
    proposal_store = ProposalStore()
    conflict_store = ConflictStore()

    pairs = _get_entity_pairs(limit=_TENANT_LIMIT)

    entities_scanned = 0
    drift_findings = 0
    proposals_filed = 0
    proposals_deduped = 0
    errors: list[str] = []

    now_iso = datetime.now(timezone.utc).isoformat()

    for tenant_id, entity_id, current_run_id, _previous_run_id in pairs:
        entities_scanned += 1
        try:
            # Count open value conflicts BEFORE detection (trend: prior_count).
            prior_count = conflict_store.count_open_value(tenant_id, entity_id)

            # Gate 1A engine: detect + upsert conflict_register for this run.
            det = detect_and_register(tenant_id, entity_id, current_run_id)

            # Filter for value conflicts only (structural conflicts are #56-bounded
            # separately; they don't file value_drift proposals).
            value_conflicts = [c for c in det["conflicts"] if c.get("conflict_type") == "value"]
            if not value_conflicts:
                continue

            # Count after detection (trend: current_count includes newly-found ones).
            current_count = conflict_store.count_open_value(tenant_id, entity_id)
            drift_findings += len(value_conflicts)

            # Batch-read statuses to skip already-dispositioned conflicts.
            conflict_ids = [c["conflict_id"] for c in value_conflicts]
            statuses = conflict_store.get_conflict_statuses(tenant_id, conflict_ids)
            open_conflicts = [
                c for c in value_conflicts
                if statuses.get(c["conflict_id"]) == "open"
            ]
            if not open_conflicts:
                continue

            # Batch dedup: one query for all open conflicts of this entity.
            dedup_keys = [("value_drift", _vd_natural_key(entity_id, c)) for c in open_conflicts]
            dup_map = proposal_store.check_duplicates(tenant_id, dedup_keys)

            for c in open_conflicts:
                nk = _vd_natural_key(entity_id, c)
                existing_id = dup_map.get(("value_drift", nk))
                if existing_id:
                    proposals_deduped += 1
                    logger.info(
                        "[value_drift_monitor] entity=%s concept=%s.%s — "
                        "pending proposal already exists: %s (deduped)",
                        entity_id, c["concept"], c.get("property"), existing_id,
                    )
                    continue

                proposal_store.insert_proposals([{
                    "tenant_id": tenant_id,
                    "entity_id": entity_id,
                    "proposal_type": "value_drift",
                    "natural_key": nk,
                    "payload": {
                        "entity_id": entity_id,
                        "tenant_id": tenant_id,
                        "concept": c["concept"],
                        "property": c.get("property", ""),
                        "period": c.get("period") or "",
                        "claims": c["claims"],
                        "conflict_id": c["conflict_id"],
                        "conflict_class": c.get("conflict_class", ""),
                        "trend": {
                            "prior_count": prior_count,
                            "current_count": current_count,
                        },
                    },
                    "confidence": 1.0,
                    "provenance": {
                        "basis": "inferred",
                        "source": "value_drift_monitor",
                        "detected_at": now_iso,
                        "dcl_ingest_id": current_run_id,
                    },
                }])
                proposals_filed += 1
                logger.info(
                    "[value_drift_monitor] entity=%s concept=%s.%s — "
                    "filed value_drift proposal; trend=%d→%d",
                    entity_id, c["concept"], c.get("property"),
                    prior_count, current_count,
                )

        except Exception as exc:
            msg = f"entity={entity_id} tenant={tenant_id}: {exc}"
            logger.error(
                "[value_drift_monitor] entity scan FAILED: %s", msg, exc_info=True
            )
            errors.append(msg)

    result = {
        "entities_scanned": entities_scanned,
        "drift_findings": drift_findings,
        "proposals_filed": proposals_filed,
        "proposals_deduped": proposals_deduped,
        "errors": errors,
    }

    if errors:
        raise RuntimeError(
            f"[value_drift_monitor] {len(errors)} entity scan(s) failed "
            f"(scanned={entities_scanned} findings={drift_findings} "
            f"filed={proposals_filed}): {errors[:3]}"
        )

    return result


def value_drift_job() -> None:
    """APScheduler job entry point for value drift. Same pattern as drift_job().

    Called by the scheduler on its interval. Also called directly by
    run-now (same function). Any unhandled exception is re-raised so
    APScheduler logs it; last_status='error' is recorded first (A1).
    """
    monitor_store = MonitorStore()

    job_row = monitor_store.get_job("value_drift")
    if job_row and not job_row["enabled"]:
        return

    try:
        result = run_value_drift_sweep()
        detail = (
            f"scanned={result['entities_scanned']} "
            f"findings={result['drift_findings']} "
            f"filed={result['proposals_filed']} "
            f"deduped={result['proposals_deduped']}"
        )
        status = "ok"
    except RuntimeError as exc:
        logger.error(
            "[value_drift_monitor] value_drift_job FAILED: %s", exc, exc_info=True
        )
        detail = str(exc)[:2000]
        status = "error"
        monitor_store.record_run("value_drift", status, detail)
        raise
    except Exception as exc:
        logger.error(
            "[value_drift_monitor] value_drift_job UNEXPECTED ERROR: %s",
            exc, exc_info=True,
        )
        detail = f"{type(exc).__name__}: {exc}"[:2000]
        status = "error"
        monitor_store.record_run("value_drift", status, detail)
        raise

    monitor_store.record_run("value_drift", status, detail)


def drift_job() -> None:
    """APScheduler job entry point. Runs the sweep and records the outcome.

    Called by the scheduler on its interval. Also called directly by the
    run-now route (same function — no test-only fork). Any unhandled exception
    is re-raised so APScheduler logs it; last_status='error' is recorded first.
    """
    monitor_store = MonitorStore()

    job_row = monitor_store.get_job("structural_drift")
    if job_row and not job_row["enabled"]:
        # Belt-and-suspenders: pause removes the APScheduler job, but
        # the enabled check here handles any scheduling race.
        return

    try:
        result = run_structural_drift_sweep()
        detail = (
            f"scanned={result['entities_scanned']} "
            f"findings={result['drift_findings']} "
            f"filed={result['proposals_filed']} "
            f"deduped={result['proposals_deduped']}"
        )
        status = "ok"
    except RuntimeError as exc:
        logger.error(
            "[structural_drift_monitor] drift_job FAILED: %s", exc, exc_info=True
        )
        detail = str(exc)[:2000]
        status = "error"
        monitor_store.record_run("structural_drift", status, detail)
        raise
    except Exception as exc:
        logger.error(
            "[structural_drift_monitor] drift_job UNEXPECTED ERROR: %s",
            exc, exc_info=True,
        )
        detail = f"{type(exc).__name__}: {exc}"[:2000]
        status = "error"
        monitor_store.record_run("structural_drift", status, detail)
        raise

    monitor_store.record_run("structural_drift", status, detail)
