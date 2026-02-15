"""
DCL Ingestion Endpoint — accepts data pushes from AAM Runners.

Architecture:
  AAM dispatches a Job Manifest → Runner extracts + transforms →
  Runner POSTs to this endpoint → DCL stores metadata + buffers rows.

Zero-Trust compliance:
  - Row data is buffered IN-MEMORY ONLY (never written to disk).
  - Metadata (run receipts, schema hashes, drift events) is the durable
    record of what was ingested.
  - The PayloadSecurityGuard is NOT invoked on ingested rows because
    those rows live in a bounded in-memory buffer, not on disk.
"""

import hashlib
import json
import time
from collections import OrderedDict
from dataclasses import dataclass, field as dc_field
from datetime import datetime, timezone
from threading import Lock
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from backend.aam.ingress import normalize_source_id
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)


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
    run_id: str
    pipe_id: str
    rows_accepted: int
    schema_drift: bool = False
    drift_fields: List[str] = Field(default_factory=list)
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


# ---------------------------------------------------------------------------
# In-Memory Ingest Store  (Zero-Trust: never persisted to disk)
# ---------------------------------------------------------------------------

_MAX_RUNS = 500          # keep last N run receipts
_MAX_BUFFERED_ROWS = 200_000   # total rows across all runs
_MAX_DRIFT_EVENTS = 1000


class IngestStore:
    """
    In-memory store for ingested data and metadata.

    Bounded by _MAX_RUNS and _MAX_BUFFERED_ROWS to prevent OOM.
    Oldest runs are evicted first (FIFO).
    """

    def __init__(self) -> None:
        self._lock = Lock()

        # Metadata (what DCL durably "owns")
        self._receipts: OrderedDict[str, RunReceipt] = OrderedDict()
        self._schema_registry: Dict[str, SchemaRecord] = {}   # pipe_id → last schema
        self._drift_events: List[SchemaDriftEvent] = []

        # Row buffer (in-memory only, queryable)
        self._row_buffer: OrderedDict[str, List[Dict[str, Any]]] = OrderedDict()
        self._total_rows = 0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def ingest(
        self,
        run_id: str,
        pipe_id: str,
        schema_hash: str,
        request: IngestRequest,
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
            self._receipts[run_id] = receipt

            while len(self._receipts) > _MAX_RUNS:
                evicted_id, _ = self._receipts.popitem(last=False)
                self._row_buffer.pop(evicted_id, None)

            self._row_buffer[run_id] = tagged
            self._total_rows += actual

            while self._total_rows > _MAX_BUFFERED_ROWS and self._row_buffer:
                evicted_id, evicted_rows = self._row_buffer.popitem(last=False)
                self._total_rows -= len(evicted_rows)

        return receipt

    # --- Query helpers ---

    def get_receipt(self, run_id: str) -> Optional[RunReceipt]:
        with self._lock:
            return self._receipts.get(run_id)

    def get_all_receipts(self) -> List[RunReceipt]:
        with self._lock:
            return list(self._receipts.values())

    def get_rows(self, run_id: str) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._row_buffer.get(run_id, []))

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
