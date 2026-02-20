"""
DCL Pipe Definition Store — stores pipe schemas received from AAM's /export-pipes.

Architecture:
  AAM pushes pipe definitions (structure) → DCL stores them here.
  Farm pushes data rows (content) → DCL's /ingest checks this store
  before accepting data, performing the schema-on-write validation.

  The JOIN key is pipe_id. If content arrives without a matching
  pipe definition, the ingest endpoint rejects it with HTTP 422.

Persistence:
  - In-memory primary: all reads from memory (fast).
  - Redis write-through: definitions survive backend restarts.
  - Redis TTL: 24 hours (matches IngestStore).
  - If Redis is unavailable, in-memory only (logs warning).
"""

import json
import os
import tempfile
import time
from dataclasses import dataclass, field as dc_field, asdict
from datetime import datetime, timezone
from threading import Lock
from typing import Any, Dict, List, Optional

from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

_REDIS_PREFIX = "dcl:pipes:"
_REDIS_TTL = 86400  # 24 hours

_CACHE_DIR = os.path.join("backend", "cache")
_CACHE_FILE = os.path.join(_CACHE_DIR, "pipe_cache.json")
os.makedirs(_CACHE_DIR, exist_ok=True)


# ---------------------------------------------------------------------------
# Pipe Definition (what AAM's /export-pipes sends us)
# ---------------------------------------------------------------------------

@dataclass
class PipeDefinition:
    """A single pipe schema from AAM's export-pipes payload.

    This is the structure side of the late-binding join.
    The pipe_id is the JOIN key that links this definition
    to data rows pushed by Farm via /ingest.
    """
    pipe_id: str                          # THE JOIN KEY
    candidate_id: str = ""                # provenance only
    source_name: str = ""
    vendor: str = ""
    category: str = ""
    governance_status: Optional[str] = None
    fields: List[str] = dc_field(default_factory=list)
    entity_scope: Optional[str] = None
    identity_keys: List[str] = dc_field(default_factory=list)
    transport_kind: Optional[str] = None
    modality: Optional[str] = None
    change_semantics: Optional[str] = None
    health: str = "unknown"
    last_sync: Optional[str] = None
    asset_key: str = ""
    aod_asset_id: Optional[str] = None
    fabric_plane: str = ""
    received_at: str = ""                 # when DCL stored it


# ---------------------------------------------------------------------------
# Export Receipt (metadata for an export-pipes call)
# ---------------------------------------------------------------------------

@dataclass
class ExportReceipt:
    """Tracks a single export-pipes ingestion event."""
    aod_run_id: Optional[str]
    source: str                           # "aam"
    total_connections: int
    pipe_ids: List[str]
    received_at: str
    snapshot_name: Optional[str] = None


# ---------------------------------------------------------------------------
# Redis helpers (same pattern as IngestStore)
# ---------------------------------------------------------------------------

def _get_redis():
    """Try to connect to Redis. Returns client or None."""
    try:
        import redis
        r = redis.Redis(host="localhost", port=6379, decode_responses=True)
        r.ping()
        return r
    except Exception as e:
        logger.warning(f"[PipeStore] Redis unavailable: {e}")
        return None


# ---------------------------------------------------------------------------
# Pipe Definition Store
# ---------------------------------------------------------------------------

class PipeDefinitionStore:
    """
    In-memory store for pipe definitions with Redis write-through.

    Mirrors IngestStore's pattern:
    - Primary reads from memory
    - Writes go to memory + Redis
    - Rehydrates from Redis on startup

    Provides the lookup used by the ingest guard to validate
    that a pipe_id has a matching schema before accepting data.
    """

    def __init__(self) -> None:
        self._lock = Lock()
        self._definitions: Dict[str, PipeDefinition] = {}
        self._export_receipts: List[ExportReceipt] = []
        self._redis = _get_redis()

        if self._redis:
            self._load_from_redis()

        self._load_from_disk()

    # ------------------------------------------------------------------
    # Disk persistence (JSON file fallback)
    # ------------------------------------------------------------------

    def _save_to_disk(self) -> None:
        try:
            data = {
                "definitions": {k: asdict(v) for k, v in self._definitions.items()},
                "export_receipts": [asdict(r) for r in self._export_receipts],
            }
            fd, tmp_path = tempfile.mkstemp(dir=_CACHE_DIR, suffix=".tmp")
            try:
                with os.fdopen(fd, "w") as f:
                    json.dump(data, f, default=str)
                os.replace(tmp_path, _CACHE_FILE)
            except Exception:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                raise
        except Exception as e:
            logger.warning(f"[PipeStore] Failed to save to disk: {e}")

    def _load_from_disk(self) -> None:
        if not os.path.exists(_CACHE_FILE):
            return
        try:
            with open(_CACHE_FILE, "r") as f:
                data = json.load(f)

            if self._definitions:
                return

            for k, v in data.get("definitions", {}).items():
                self._definitions[k] = PipeDefinition(**v)

            self._export_receipts = [ExportReceipt(**d) for d in data.get("export_receipts", [])]

            logger.info(
                f"[PipeStore] Restored from disk: "
                f"{len(self._definitions)} definitions, "
                f"{len(self._export_receipts)} export receipts"
            )
        except Exception as e:
            logger.warning(f"[PipeStore] Failed to load from disk: {e}")

    def reset(self) -> None:
        with self._lock:
            self._definitions.clear()
            self._export_receipts.clear()
        try:
            if os.path.exists(_CACHE_FILE):
                os.remove(_CACHE_FILE)
        except Exception as e:
            logger.warning(f"[PipeStore] Failed to delete cache file: {e}")
        logger.info("[PipeStore] All state reset")

    # ------------------------------------------------------------------
    # Redis persistence
    # ------------------------------------------------------------------

    def _load_from_redis(self) -> None:
        """Rehydrate pipe definitions from Redis."""
        try:
            r = self._redis

            raw_defs = r.hgetall(f"{_REDIS_PREFIX}definitions")
            loaded = 0
            for pipe_id, raw in raw_defs.items():
                d = json.loads(raw)
                self._definitions[pipe_id] = PipeDefinition(**d)
                loaded += 1

            raw_receipts = r.get(f"{_REDIS_PREFIX}export_receipts")
            if raw_receipts:
                for d in json.loads(raw_receipts):
                    self._export_receipts.append(ExportReceipt(**d))

            if loaded > 0:
                logger.info(
                    f"[PipeStore] Rehydrated from Redis: "
                    f"{loaded} pipe definitions, "
                    f"{len(self._export_receipts)} export receipts"
                )
        except Exception as e:
            logger.warning(f"[PipeStore] Redis rehydration failed: {e}")

    def _persist_definition(self, pipe_id: str, defn: PipeDefinition) -> None:
        """Write a single pipe definition to Redis."""
        if not self._redis:
            return
        try:
            self._redis.hset(
                f"{_REDIS_PREFIX}definitions",
                pipe_id,
                json.dumps(asdict(defn)),
            )
            self._redis.expire(f"{_REDIS_PREFIX}definitions", _REDIS_TTL)
        except Exception as e:
            logger.warning(f"[PipeStore] Redis persist definition failed: {e}")

    def _persist_export_receipts(self) -> None:
        """Write export receipts to Redis."""
        if not self._redis:
            return
        try:
            self._redis.set(
                f"{_REDIS_PREFIX}export_receipts",
                json.dumps([asdict(r) for r in self._export_receipts]),
            )
            self._redis.expire(f"{_REDIS_PREFIX}export_receipts", _REDIS_TTL)
        except Exception as e:
            logger.warning(f"[PipeStore] Redis persist receipts failed: {e}")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def register(self, defn: PipeDefinition) -> None:
        """Register or update a pipe definition."""
        with self._lock:
            self._definitions[defn.pipe_id] = defn
        self._persist_definition(defn.pipe_id, defn)

    def register_batch(
        self,
        definitions: List[PipeDefinition],
        aod_run_id: Optional[str] = None,
        source: str = "aam",
        snapshot_name: Optional[str] = None,
    ) -> ExportReceipt:
        """Register multiple pipe definitions from an export-pipes call."""
        now = datetime.now(timezone.utc).isoformat()
        pipe_ids = []

        with self._lock:
            for defn in definitions:
                defn.received_at = now
                self._definitions[defn.pipe_id] = defn
                pipe_ids.append(defn.pipe_id)

            receipt = ExportReceipt(
                aod_run_id=aod_run_id,
                source=source,
                total_connections=len(definitions),
                pipe_ids=pipe_ids,
                received_at=now,
                snapshot_name=snapshot_name,
            )
            self._export_receipts.append(receipt)

        # Write-through to Redis (outside lock)
        for defn in definitions:
            self._persist_definition(defn.pipe_id, defn)
        self._persist_export_receipts()

        self._save_to_disk()
        logger.info(
            f"[PipeStore] Registered {len(definitions)} pipe definitions "
            f"(aod_run_id={aod_run_id})"
        )
        return receipt

    def lookup(self, pipe_id: str) -> Optional[PipeDefinition]:
        """Look up a pipe definition by pipe_id (the JOIN key)."""
        with self._lock:
            return self._definitions.get(pipe_id)

    def list_pipe_ids(self) -> List[str]:
        """Return all known pipe_ids."""
        with self._lock:
            return sorted(self._definitions.keys())

    def get_all_definitions(self) -> List[PipeDefinition]:
        """Return all pipe definitions."""
        with self._lock:
            return list(self._definitions.values())

    def count(self) -> int:
        """Return the number of registered pipe definitions."""
        with self._lock:
            return len(self._definitions)

    def get_export_receipts(self) -> List[ExportReceipt]:
        """Return all export receipt history."""
        with self._lock:
            return list(self._export_receipts)

    def clear(self) -> None:
        """Clear all definitions (for testing)."""
        with self._lock:
            self._definitions.clear()
            self._export_receipts.clear()
        if self._redis:
            try:
                self._redis.delete(f"{_REDIS_PREFIX}definitions")
                self._redis.delete(f"{_REDIS_PREFIX}export_receipts")
            except Exception:
                pass

    def get_stats(self) -> Dict[str, Any]:
        """Return store statistics."""
        with self._lock:
            defns = list(self._definitions.values())
            planes = set(d.fabric_plane for d in defns if d.fabric_plane)
            vendors = set(d.vendor for d in defns if d.vendor)
            latest_receipt = (
                max(self._export_receipts, key=lambda r: r.received_at)
                if self._export_receipts else None
            )
            return {
                "total_definitions": len(self._definitions),
                "total_exports": len(self._export_receipts),
                "fabric_planes": sorted(planes),
                "vendors": sorted(vendors),
                "latest_export_at": (
                    latest_receipt.received_at if latest_receipt else None
                ),
                "redis_connected": self._redis is not None,
            }


# ---------------------------------------------------------------------------
# Singleton
# ---------------------------------------------------------------------------

_store: Optional[PipeDefinitionStore] = None


def get_pipe_store() -> PipeDefinitionStore:
    global _store
    if _store is None:
        _store = PipeDefinitionStore()
    return _store
