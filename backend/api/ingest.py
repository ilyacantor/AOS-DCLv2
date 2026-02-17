"""
DCL Ingestion Endpoint — accepts data pushes from Farm / AAM Runners.

Architecture:
  Farm pushes pipe payloads → DCL stores in-memory + writes through to Redis.
  On backend restart, IngestStore rehydrates from Redis automatically.

Persistence:
  - Redis write-through: receipts, rows, schema registry, drift events.
  - Redis TTL: 24 hours (synthetic data, auto-expires).
  - If Redis is unavailable, in-memory only (logs warning at startup).
"""

import hashlib
import json
import time
from collections import OrderedDict
from dataclasses import dataclass, field as dc_field, asdict
from datetime import datetime, timezone
from threading import Lock
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from backend.aam.ingress import normalize_source_id
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

_REDIS_PREFIX = "dcl:ingest:"
_REDIS_TTL = 86400  # 24 hours


# ---------------------------------------------------------------------------
# Request / Response Models
# ---------------------------------------------------------------------------

class IngestRequest(BaseModel):
    """
    The strict contract for data pushed from AAM Runners to DCL.

    Mirrors the Runner's output after it applies the Job Manifest's
    transform.schema_map.
    """
    source_system: str = Field(..., description="e.g. 'salesforce', 'netsuite'")
    tenant_id: str = Field(..., description="Tenant this data belongs to")
    snapshot_name: str = Field(..., description="Logical dataset name, e.g. 'revenue_q1'")
    run_timestamp: str = Field(..., description="ISO-8601 UTC: when extraction started")
    schema_version: str = Field(..., description="Version of the schema map used")
    row_count: int = Field(..., ge=0, description="Expected len(rows) — validated server-side")
    rows: List[Dict[str, Any]] = Field(..., description="Transformed records from Runner")
    runner_id: Optional[str] = None


class IngestResponse(BaseModel):
    """Acknowledgement returned to the Runner."""
    status: str                        # "ingested" | "rejected"
    dcl_run_id: str                    # DCL's internal run ID
    run_id: str                        # alias kept for backward compat
    dispatch_id: str = ""
    pipe_id: str
    rows_accepted: int
    schema_drift: bool = False
    drift_fields: List[str] = Field(default_factory=list)
    matched_schema: bool = False       # confirms structure+content join succeeded
    schema_fields: List[str] = Field(default_factory=list)  # fields from export blueprint
    timestamp: str = ""                # ISO-8601 when DCL accepted
    message: Optional[str] = None


# ---------------------------------------------------------------------------
# Schema Drift Detection
# ---------------------------------------------------------------------------

@dataclass
class SchemaRecord:
    """Tracks the last-seen schema hash for a pipe."""
    pipe_id: str
    schema_hash: str
    field_names: List[str]
    last_seen: str           # ISO-8601
    run_id: str


@dataclass
class SchemaDriftEvent:
    """Logged when a Runner's schema diverges from the stored fingerprint."""
    pipe_id: str
    run_id: str
    previous_hash: str
    incoming_hash: str
    added_fields: List[str]
    removed_fields: List[str]
    detected_at: str         # ISO-8601


# ---------------------------------------------------------------------------
# Run Receipt (metadata that DCL "owns")
# ---------------------------------------------------------------------------

@dataclass
class RunReceipt:
    """Durable metadata record for an ingestion run."""
    run_id: str
    pipe_id: str
    source_system: str
    canonical_source_id: str
    tenant_id: str
    snapshot_name: str
    run_timestamp: str       # from Runner
    received_at: str         # when DCL accepted it
    schema_version: str
    schema_hash: str
    row_count: int
    schema_drift: bool = False
    drift_fields: List[str] = dc_field(default_factory=list)
    runner_id: Optional[str] = None
    dispatch_id: str = ""    # groups pipes from one Farm manifest dispatch


# ---------------------------------------------------------------------------
# Redis helpers
# ---------------------------------------------------------------------------

def _get_redis():
    """Try to connect to Redis. Returns client or None."""
    try:
        import redis
        r = redis.Redis(host="localhost", port=6379, decode_responses=True)
        r.ping()
        return r
    except Exception as e:
        logger.warning(f"[IngestStore] Redis unavailable: {e}")
        return None


# ---------------------------------------------------------------------------
# Ingest Store with Redis write-through
# ---------------------------------------------------------------------------

_MAX_RUNS = 500          # keep last N run receipts
_MAX_BUFFERED_ROWS = 200_000   # total rows across all runs
_MAX_DRIFT_EVENTS = 1000


def _make_key(run_id: str, pipe_id: str) -> str:
    """Composite storage key — unique per push (run_id is shared across pipes)."""
    return f"{run_id}:{pipe_id}"


class IngestStore:
    """
    In-memory store for ingested data with Redis write-through.

    Primary reads: always from in-memory (fast).
    Writes: in-memory + Redis (if available).
    Startup: rehydrates from Redis so data survives backend restarts.

    Bounded by _MAX_RUNS and _MAX_BUFFERED_ROWS to prevent OOM.
    Oldest runs are evicted first (FIFO).
    """

    def __init__(self) -> None:
        self._lock = Lock()

        # Metadata
        self._receipts: OrderedDict[str, RunReceipt] = OrderedDict()
        self._schema_registry: Dict[str, SchemaRecord] = {}
        self._drift_events: List[SchemaDriftEvent] = []

        # Row buffer
        self._row_buffer: OrderedDict[str, List[Dict[str, Any]]] = OrderedDict()
        self._total_rows = 0

        # Redis connection (None if unavailable)
        self._redis = _get_redis()

        # Rehydrate from Redis on startup
        if self._redis:
            self._load_from_redis()

    # ------------------------------------------------------------------
    # Redis persistence
    # ------------------------------------------------------------------

    def _load_from_redis(self) -> None:
        """Rehydrate in-memory state from Redis."""
        try:
            r = self._redis

            # Load receipt order (keys are composite: "run_id:pipe_id")
            order = r.lrange(f"{_REDIS_PREFIX}receipt_order", 0, -1)
            loaded_receipts = 0
            loaded_rows = 0

            for storage_key in order:
                raw = r.hget(f"{_REDIS_PREFIX}receipts", storage_key)
                if not raw:
                    continue
                d = json.loads(raw)
                receipt = RunReceipt(**d)

                # Migrate old keys: if storage_key lacks ":" it's pre-fix data
                if ":" not in storage_key:
                    storage_key = _make_key(receipt.run_id, receipt.pipe_id)

                self._receipts[storage_key] = receipt
                loaded_receipts += 1

                # Load rows (try composite key first, fall back to old key)
                rows_raw = r.get(f"{_REDIS_PREFIX}rows:{storage_key}")
                if not rows_raw:
                    rows_raw = r.get(f"{_REDIS_PREFIX}rows:{receipt.run_id}")
                if rows_raw:
                    rows = json.loads(rows_raw)
                    self._row_buffer[storage_key] = rows
                    self._total_rows += len(rows)
                    loaded_rows += len(rows)

            # Load schema registry
            schemas = r.hgetall(f"{_REDIS_PREFIX}schemas")
            for pipe_id, raw in schemas.items():
                d = json.loads(raw)
                self._schema_registry[pipe_id] = SchemaRecord(**d)

            # Load drift events
            drift_raw = r.get(f"{_REDIS_PREFIX}drift_events")
            if drift_raw:
                for d in json.loads(drift_raw):
                    self._drift_events.append(SchemaDriftEvent(**d))

            # Backfill dispatch_id for legacy receipts (pre-dispatch era)
            backfilled = 0
            for receipt in self._receipts.values():
                if not receipt.dispatch_id:
                    receipt.dispatch_id = _derive_dispatch_id(
                        receipt.run_timestamp, receipt.tenant_id, receipt.snapshot_name
                    )
                    backfilled += 1

            if loaded_receipts > 0:
                logger.info(
                    f"[IngestStore] Rehydrated from Redis: "
                    f"{loaded_receipts} receipts, {loaded_rows:,} rows, "
                    f"{len(self._schema_registry)} schemas"
                    + (f", backfilled {backfilled} dispatch_ids" if backfilled else "")
                )

        except Exception as e:
            logger.warning(f"[IngestStore] Redis rehydration failed: {e}")

    def _persist_receipt(self, storage_key: str, receipt: RunReceipt) -> None:
        """Write a receipt to Redis."""
        if not self._redis:
            return
        try:
            r = self._redis
            r.hset(f"{_REDIS_PREFIX}receipts", storage_key, json.dumps(asdict(receipt)))
            r.rpush(f"{_REDIS_PREFIX}receipt_order", storage_key)
            r.expire(f"{_REDIS_PREFIX}receipts", _REDIS_TTL)
            r.expire(f"{_REDIS_PREFIX}receipt_order", _REDIS_TTL)
        except Exception as e:
            logger.warning(f"[IngestStore] Redis persist receipt failed: {e}")

    def _persist_rows(self, storage_key: str, rows: List[Dict[str, Any]]) -> None:
        """Write rows to Redis with TTL."""
        if not self._redis:
            return
        try:
            key = f"{_REDIS_PREFIX}rows:{storage_key}"
            self._redis.set(key, json.dumps(rows, default=str))
            self._redis.expire(key, _REDIS_TTL)
        except Exception as e:
            logger.warning(f"[IngestStore] Redis persist rows failed: {e}")

    def _persist_schema(self, pipe_id: str, record: SchemaRecord) -> None:
        """Write a schema record to Redis."""
        if not self._redis:
            return
        try:
            self._redis.hset(
                f"{_REDIS_PREFIX}schemas", pipe_id, json.dumps(asdict(record))
            )
            self._redis.expire(f"{_REDIS_PREFIX}schemas", _REDIS_TTL)
        except Exception as e:
            logger.warning(f"[IngestStore] Redis persist schema failed: {e}")

    def _persist_drift_events(self) -> None:
        """Write drift events to Redis."""
        if not self._redis:
            return
        try:
            self._redis.set(
                f"{_REDIS_PREFIX}drift_events",
                json.dumps([asdict(e) for e in self._drift_events]),
            )
            self._redis.expire(f"{_REDIS_PREFIX}drift_events", _REDIS_TTL)
        except Exception as e:
            logger.warning(f"[IngestStore] Redis persist drift failed: {e}")

    def _evict_from_redis(self, storage_key: str) -> None:
        """Remove evicted entry from Redis."""
        if not self._redis:
            return
        try:
            self._redis.hdel(f"{_REDIS_PREFIX}receipts", storage_key)
            self._redis.lrem(f"{_REDIS_PREFIX}receipt_order", 1, storage_key)
            self._redis.delete(f"{_REDIS_PREFIX}rows:{storage_key}")
        except Exception as e:
            logger.warning(f"[IngestStore] Redis evict failed: {e}")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def ingest(
        self,
        run_id: str,
        pipe_id: str,
        schema_hash: str,
        request: IngestRequest,
        dispatch_id: str = "",
    ) -> RunReceipt:
        """
        Accept a Runner push.  Returns the RunReceipt.

        Concurrency-safe: all heavy computation (field extraction, row
        tagging, receipt construction) runs OUTSIDE the lock.  The lock
        protects only the dict mutations (~microseconds).
        """
        now = datetime.now(timezone.utc).isoformat()
        canonical_id = normalize_source_id(request.source_system)

        actual = len(request.rows)
        if actual != request.row_count:
            logger.warning(
                f"[Ingest] Row count mismatch for {pipe_id}: "
                f"declared={request.row_count}, actual={actual}"
            )

        field_names = _extract_field_names(request.rows)

        tagged = [
            {
                **row,
                "_run_id": run_id,
                "_dispatch_id": dispatch_id,
                "_pipe_id": pipe_id,
                "_source_system": canonical_id,
                "_inserted_at": now,
            }
            for row in request.rows
        ]

        schema_record = SchemaRecord(
            pipe_id=pipe_id,
            schema_hash=schema_hash,
            field_names=field_names,
            last_seen=now,
            run_id=run_id,
        )

        curr_field_set = set(field_names)

        evicted_ids: List[str] = []

        with self._lock:
            drift = False
            drift_fields: List[str] = []

            prev = self._schema_registry.get(pipe_id)
            if prev and prev.schema_hash != schema_hash:
                added = sorted(curr_field_set - set(prev.field_names))
                removed = sorted(set(prev.field_names) - curr_field_set)
                drift = True
                drift_fields = added + removed
                self._drift_events.append(SchemaDriftEvent(
                    pipe_id=pipe_id,
                    run_id=run_id,
                    previous_hash=prev.schema_hash,
                    incoming_hash=schema_hash,
                    added_fields=added,
                    removed_fields=removed,
                    detected_at=now,
                ))
                if len(self._drift_events) > _MAX_DRIFT_EVENTS:
                    self._drift_events = self._drift_events[-_MAX_DRIFT_EVENTS:]

            self._schema_registry[pipe_id] = schema_record

            receipt = RunReceipt(
                run_id=run_id,
                dispatch_id=dispatch_id,
                pipe_id=pipe_id,
                source_system=request.source_system,
                canonical_source_id=canonical_id,
                tenant_id=request.tenant_id,
                snapshot_name=request.snapshot_name,
                run_timestamp=request.run_timestamp,
                received_at=now,
                schema_version=request.schema_version,
                schema_hash=schema_hash,
                row_count=actual,
                schema_drift=drift,
                drift_fields=drift_fields,
                runner_id=request.runner_id,
            )
            key = _make_key(run_id, pipe_id)
            self._receipts[key] = receipt

            while len(self._receipts) > _MAX_RUNS:
                evicted_key, _ = self._receipts.popitem(last=False)
                self._row_buffer.pop(evicted_key, None)
                evicted_ids.append(evicted_key)

            self._row_buffer[key] = tagged
            self._total_rows += actual

            while self._total_rows > _MAX_BUFFERED_ROWS and self._row_buffer:
                evicted_id, evicted_rows = self._row_buffer.popitem(last=False)
                self._total_rows -= len(evicted_rows)
                evicted_ids.append(evicted_id)

        # Write-through to Redis (outside lock)
        self._persist_receipt(key, receipt)
        self._persist_rows(key, tagged)
        self._persist_schema(pipe_id, schema_record)
        if drift:
            self._persist_drift_events()
        for eid in evicted_ids:
            self._evict_from_redis(eid)

        return receipt

    # --- Query helpers ---

    def get_receipt(self, run_id: str, pipe_id: str = None) -> Optional[RunReceipt]:
        with self._lock:
            if pipe_id:
                return self._receipts.get(_make_key(run_id, pipe_id))
            # Search by run_id (returns first match)
            for receipt in self._receipts.values():
                if receipt.run_id == run_id:
                    return receipt
            return None

    def get_receipts_by_run(self, run_id: str) -> List[RunReceipt]:
        """Return all receipts for a given Farm run_id."""
        with self._lock:
            return [r for r in self._receipts.values() if r.run_id == run_id]

    def get_all_receipts(self) -> List[RunReceipt]:
        with self._lock:
            return list(self._receipts.values())

    def get_rows(self, run_id: str, pipe_id: str = None) -> List[Dict[str, Any]]:
        with self._lock:
            if pipe_id:
                return list(self._row_buffer.get(_make_key(run_id, pipe_id), []))
            # Search by run_id (returns first match)
            for key, rows in self._row_buffer.items():
                if key.startswith(f"{run_id}:") or key == run_id:
                    return list(rows)
            return []

    def get_rows_by_source(self, source_system: str) -> List[Dict[str, Any]]:
        canonical = normalize_source_id(source_system)
        with self._lock:
            rows = []
            for run_rows in self._row_buffer.values():
                for row in run_rows:
                    if row.get("_source_system") == canonical:
                        rows.append(row)
            return rows

    def get_drift_events(self, pipe_id: Optional[str] = None) -> List[SchemaDriftEvent]:
        with self._lock:
            if pipe_id:
                return [e for e in self._drift_events if e.pipe_id == pipe_id]
            return list(self._drift_events)

    def get_schema_registry(self) -> Dict[str, SchemaRecord]:
        with self._lock:
            return dict(self._schema_registry)

    def get_dispatches(self, snapshot_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """Group all receipts by dispatch_id and return summary per dispatch.

        If snapshot_name is provided, only return dispatches matching that
        Farm generation (e.g. 'cloudedge-a1b2').
        """
        with self._lock:
            groups: Dict[str, List[RunReceipt]] = {}
            for r in self._receipts.values():
                if snapshot_name and r.snapshot_name != snapshot_name:
                    continue
                groups.setdefault(r.dispatch_id, []).append(r)

            result: List[Dict[str, Any]] = []
            for dispatch_id, receipts in groups.items():
                sorted_receipts = sorted(receipts, key=lambda r: r.received_at)
                sources = sorted(set(r.source_system for r in receipts))
                run_ids = sorted(set(r.run_id for r in receipts))
                snapshots = sorted(set(r.snapshot_name for r in receipts))
                tenants = sorted(set(r.tenant_id for r in receipts))
                pipe_ids = sorted(set(r.pipe_id for r in receipts))
                result.append({
                    "dispatch_id": dispatch_id,
                    "snapshot_name": snapshots[0] if len(snapshots) == 1 else snapshots,
                    "tenant_id": tenants[0] if len(tenants) == 1 else tenants,
                    "pipe_count": len(receipts),
                    "total_rows": sum(r.row_count for r in receipts),
                    "unique_sources": sources,
                    "pipe_ids": pipe_ids,
                    "first_received_at": sorted_receipts[0].received_at,
                    "latest_received_at": sorted_receipts[-1].received_at,
                    "drift_count": sum(1 for r in receipts if r.schema_drift),
                    "run_ids": run_ids,
                })
            result.sort(key=lambda d: d["latest_received_at"], reverse=True)
            return result

    def get_receipts_by_dispatch(self, dispatch_id: str) -> List[RunReceipt]:
        """Return all receipts for a given dispatch_id."""
        with self._lock:
            return [r for r in self._receipts.values() if r.dispatch_id == dispatch_id]

    def get_rows_by_dispatch(self, dispatch_id: str) -> List[Dict[str, Any]]:
        """Return all rows tagged with the given dispatch_id."""
        with self._lock:
            rows: List[Dict[str, Any]] = []
            for run_rows in self._row_buffer.values():
                for row in run_rows:
                    if row.get("_dispatch_id") == dispatch_id:
                        rows.append(row)
            return rows

    def get_dispatch_summary(self, dispatch_id: str) -> Optional[Dict[str, Any]]:
        """Detailed summary for one dispatch, including per-source breakdown."""
        with self._lock:
            receipts = [r for r in self._receipts.values() if r.dispatch_id == dispatch_id]
            if not receipts:
                return None

            sorted_receipts = sorted(receipts, key=lambda r: r.received_at)
            sources_breakdown: Dict[str, Dict[str, Any]] = {}
            pipes_detail: List[Dict[str, Any]] = []
            for r in receipts:
                if r.source_system not in sources_breakdown:
                    sources_breakdown[r.source_system] = {"pipe_count": 0, "row_count": 0, "pipe_ids": []}
                sources_breakdown[r.source_system]["pipe_count"] += 1
                sources_breakdown[r.source_system]["row_count"] += r.row_count
                sources_breakdown[r.source_system]["pipe_ids"].append(r.pipe_id)
                pipes_detail.append({
                    "pipe_id": r.pipe_id,
                    "source_system": r.source_system,
                    "row_count": r.row_count,
                    "schema_drift": r.schema_drift,
                    "received_at": r.received_at,
                    "run_id": r.run_id,
                })

            snapshots = sorted(set(r.snapshot_name for r in receipts))
            tenants = sorted(set(r.tenant_id for r in receipts))

            return {
                "dispatch_id": dispatch_id,
                "snapshot_name": snapshots[0] if len(snapshots) == 1 else snapshots,
                "tenant_id": tenants[0] if len(tenants) == 1 else tenants,
                "pipe_count": len(receipts),
                "total_rows": sum(r.row_count for r in receipts),
                "unique_sources": sorted(set(r.source_system for r in receipts)),
                "pipe_ids": sorted(set(r.pipe_id for r in receipts)),
                "first_received_at": sorted_receipts[0].received_at,
                "latest_received_at": sorted_receipts[-1].received_at,
                "drift_count": sum(1 for r in receipts if r.schema_drift),
                "run_ids": sorted(set(r.run_id for r in receipts)),
                "sources_breakdown": sources_breakdown,
                "pipes": pipes_detail,
            }

    def record_aam_pull(self, run_id: str, source_names: list, source_ids: list, kpis: dict) -> int:
        """Record AAM pull event as a single summary receipt for Ingest panel.

        Creates ONE summary RunReceipt per AAM run (not per-source) to avoid
        flooding the 500-receipt store when AAM has 300-700 connections.
        The source list is stored in the receipt's source_system field as a
        comma-separated summary for the expanded view.

        Args:
            run_id: DCL run ID
            source_names: List of source display names from AAM
            source_ids: List of canonical source IDs from AAM
            kpis: AAM KPI dict with fabrics, pipes, sources counts

        Returns:
            1 if receipt created, 0 on failure
        """
        now = datetime.now(timezone.utc).isoformat()
        dispatch_id = f"aam_{run_id[:20]}"

        unique_sources = sorted(set(source_names))
        pipe_count = kpis.get("pipes", len(source_names))
        fabric_count = kpis.get("fabrics", 0)

        source_summary = f"{len(unique_sources)} sources across {fabric_count} fabrics"

        receipt = RunReceipt(
            run_id=run_id,
            pipe_id=f"aam-pull-{run_id[:8]}",
            source_system=source_summary,
            canonical_source_id="aam",
            tenant_id="aam",
            snapshot_name="aam-export",
            run_timestamp=now,
            received_at=now,
            schema_version="aam-live",
            schema_hash="",
            row_count=pipe_count,
            schema_drift=False,
            drift_fields=[],
            dispatch_id=dispatch_id,
            runner_id=None,
        )

        key = _make_key(run_id, f"aam-pull-{run_id[:8]}")
        evicted_ids: List[str] = []

        with self._lock:
            self._receipts[key] = receipt

            while len(self._receipts) > _MAX_RUNS:
                evicted_key, _ = self._receipts.popitem(last=False)
                self._row_buffer.pop(evicted_key, None)
                evicted_ids.append(evicted_key)

        self._persist_receipt(key, receipt)
        for eid in evicted_ids:
            self._evict_from_redis(eid)

        logger.info(
            f"[IngestStore] Recorded AAM pull: {pipe_count} pipes, "
            f"{len(unique_sources)} sources from run {run_id}"
        )
        return 1

    def get_batches(self) -> List[Dict[str, Any]]:
        _BATCH_GAP_SECONDS = 60

        with self._lock:
            snap_groups: Dict[str, List[RunReceipt]] = {}
            for r in self._receipts.values():
                snap_groups.setdefault(r.snapshot_name, []).append(r)

            batches: List[Dict[str, Any]] = []
            batch_seq = 0

            for snap_name, receipts in snap_groups.items():
                sorted_by_time = sorted(receipts, key=lambda r: r.received_at)

                current_window: List[RunReceipt] = [sorted_by_time[0]]
                for i in range(1, len(sorted_by_time)):
                    prev_ts = datetime.fromisoformat(sorted_by_time[i - 1].received_at)
                    curr_ts = datetime.fromisoformat(sorted_by_time[i].received_at)
                    gap = (curr_ts - prev_ts).total_seconds()

                    if gap > _BATCH_GAP_SECONDS:
                        batches.append(self._build_batch_summary(snap_name, current_window, batch_seq))
                        batch_seq += 1
                        current_window = [sorted_by_time[i]]
                    else:
                        current_window.append(sorted_by_time[i])

                batches.append(self._build_batch_summary(snap_name, current_window, batch_seq))
                batch_seq += 1

            batches.sort(key=lambda b: b["latest_received_at"], reverse=True)
            return batches

    @staticmethod
    def _build_batch_summary(
        snap_name: str,
        receipts: List["RunReceipt"],
        seq: int,
    ) -> Dict[str, Any]:
        sources = sorted(set(r.source_system for r in receipts))
        return {
            "batch_id": f"{snap_name}#{seq}",
            "snapshot_name": snap_name,
            "tenant_id": receipts[0].tenant_id,
            "run_count": len(receipts),
            "total_rows": sum(r.row_count for r in receipts),
            "unique_sources": len(sources),
            "source_list": sources,
            "first_run_id": receipts[0].run_id,
            "latest_run_id": receipts[-1].run_id,
            "first_received_at": receipts[0].received_at,
            "latest_received_at": receipts[-1].received_at,
            "drift_count": sum(1 for r in receipts if r.schema_drift),
        }

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            receipts = list(self._receipts.values())
            unique_sources = set(r.source_system for r in receipts)
            unique_tenants = set(r.tenant_id for r in receipts)
            latest = max(receipts, key=lambda r: r.received_at) if receipts else None
            first = min(receipts, key=lambda r: r.received_at) if receipts else None
            return {
                "total_runs": len(self._receipts),
                "total_rows_buffered": self._total_rows,
                "total_drift_events": len(self._drift_events),
                "pipes_tracked": len(self._schema_registry),
                "unique_sources": len(unique_sources),
                "unique_tenants": len(unique_tenants),
                "tenant_names": sorted(unique_tenants),
                "latest_run_id": latest.run_id if latest else None,
                "latest_run_at": latest.received_at if latest else None,
                "first_run_at": first.received_at if first else None,
                "max_runs": _MAX_RUNS,
                "max_rows": _MAX_BUFFERED_ROWS,
                "redis_connected": self._redis is not None,
            }


# ---------------------------------------------------------------------------
# Singleton
# ---------------------------------------------------------------------------

_store: Optional[IngestStore] = None


def get_ingest_store() -> IngestStore:
    global _store
    if _store is None:
        _store = IngestStore()
    return _store


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _derive_dispatch_id(run_timestamp: str, tenant_id: str, snapshot_name: str) -> str:
    """Deterministic dispatch_id from shared fields across a Farm manifest push.

    All pipes in the same dispatch share the same run_timestamp (to the second),
    tenant_id, and snapshot_name — so this produces a stable grouping key.
    """
    try:
        from datetime import datetime as _dt
        parsed = _dt.fromisoformat(run_timestamp.replace("Z", "+00:00"))
        ts_second = parsed.strftime("%Y-%m-%dT%H:%M:%S")
        ts_prefix = parsed.strftime("%Y%m%d_%H%M%S")
    except (ValueError, AttributeError):
        ts_second = run_timestamp[:19]
        ts_prefix = ts_second.replace("-", "").replace(":", "").replace("T", "_")
    short_hash = hashlib.sha256(
        f"{tenant_id}:{snapshot_name}:{ts_second}".encode()
    ).hexdigest()[:8]
    return f"dispatch_{ts_prefix}_{short_hash}"


def compute_schema_hash(rows: List[Dict[str, Any]]) -> str:
    """SHA-256 of the sorted union of all field names across rows."""
    names = _extract_field_names(rows)
    return hashlib.sha256(json.dumps(names).encode()).hexdigest()


def _extract_field_names(rows: List[Dict[str, Any]]) -> List[str]:
    """Sorted union of all keys across all rows."""
    keys: set = set()
    for row in rows:
        keys.update(row.keys())
    return sorted(keys)
