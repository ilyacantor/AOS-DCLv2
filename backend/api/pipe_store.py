"""
DCL Pipe Definition Store — stores pipe schemas received from AAM's /export-pipes.

Architecture:
  AAM pushes pipe definitions (structure) → DCL stores them here.
  Farm pushes data rows (content) → DCL's /ingest checks this store
  before accepting data, performing the schema-on-write validation.

  The JOIN key is pipe_id. If content arrives without a matching
  pipe definition, the ingest endpoint rejects it with HTTP 422.

Persistence (ordered by durability):
  1. Postgres (source of truth) — survives redeploys, no TTL.
  2. Redis (write-through cache) — fast rehydration between restarts.
  3. Disk (fallback) — backup if both PG and Redis are unavailable.
  4. In-memory (read cache) — all reads served from memory.
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
from backend.core.db import get_connection as _pg_conn, close_pool as _close_shared_pool

logger = get_logger(__name__)

_REDIS_PREFIX = "dcl:pipes:"
_REDIS_TTL = 86400  # 24 hours

_CACHE_DIR = os.path.join("backend", "cache")
_CACHE_FILE = os.path.join(_CACHE_DIR, "pipe_cache.json")
os.makedirs(_CACHE_DIR, exist_ok=True)

_MAX_EXPORT_RECEIPTS = 100  # keep last N export receipts in memory


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
    trust_score: int = 0                  # 0 = AOD chain didn't run
    data_quality_score: int = 0           # 0 = AOD chain didn't run
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
    """Return the shared Redis client (or None if unavailable)."""
    from backend.core.redis_client import get_redis
    return get_redis()


# ---------------------------------------------------------------------------
# Postgres helpers — uses shared pool from backend.core.db
# ---------------------------------------------------------------------------


_CREATE_DEFINITIONS_TABLE = """
CREATE TABLE IF NOT EXISTS pipe_definitions (
    pipe_id       VARCHAR(256) PRIMARY KEY,
    candidate_id  VARCHAR(256) DEFAULT '',
    source_name   VARCHAR(256) DEFAULT '',
    vendor        VARCHAR(256) DEFAULT '',
    category      VARCHAR(256) DEFAULT '',
    governance_status VARCHAR(64),
    fields        JSONB DEFAULT '[]'::jsonb,
    entity_scope  VARCHAR(128),
    identity_keys JSONB DEFAULT '[]'::jsonb,
    transport_kind VARCHAR(128),
    modality      VARCHAR(128),
    change_semantics VARCHAR(128),
    health        VARCHAR(64) DEFAULT 'unknown',
    last_sync     VARCHAR(64),
    asset_key     VARCHAR(256) DEFAULT '',
    aod_asset_id  VARCHAR(128),
    fabric_plane  VARCHAR(128) DEFAULT '',
    trust_score   INTEGER DEFAULT 0,
    data_quality_score INTEGER DEFAULT 0,
    received_at   TIMESTAMPTZ DEFAULT NOW()
);
"""

_CREATE_RECEIPTS_TABLE = """
CREATE TABLE IF NOT EXISTS pipe_export_receipts (
    id                SERIAL PRIMARY KEY,
    aod_run_id        VARCHAR(128),
    source            VARCHAR(64) DEFAULT 'aam',
    total_connections INTEGER DEFAULT 0,
    pipe_ids          JSONB DEFAULT '[]'::jsonb,
    received_at       TIMESTAMPTZ DEFAULT NOW(),
    snapshot_name     VARCHAR(256)
);
"""

_UPSERT_DEFINITION = """
INSERT INTO pipe_definitions (
    pipe_id, candidate_id, source_name, vendor, category,
    governance_status, fields, entity_scope, identity_keys,
    transport_kind, modality, change_semantics, health,
    last_sync, asset_key, aod_asset_id, fabric_plane,
    trust_score, data_quality_score, received_at
) VALUES (
    %s, %s, %s, %s, %s,
    %s, %s, %s, %s,
    %s, %s, %s, %s,
    %s, %s, %s, %s,
    %s, %s, %s
)
ON CONFLICT (pipe_id) DO UPDATE SET
    candidate_id     = EXCLUDED.candidate_id,
    source_name      = EXCLUDED.source_name,
    vendor           = EXCLUDED.vendor,
    category         = EXCLUDED.category,
    governance_status = EXCLUDED.governance_status,
    fields           = EXCLUDED.fields,
    entity_scope     = EXCLUDED.entity_scope,
    identity_keys    = EXCLUDED.identity_keys,
    transport_kind   = EXCLUDED.transport_kind,
    modality         = EXCLUDED.modality,
    change_semantics = EXCLUDED.change_semantics,
    health           = EXCLUDED.health,
    last_sync        = EXCLUDED.last_sync,
    asset_key        = EXCLUDED.asset_key,
    aod_asset_id     = EXCLUDED.aod_asset_id,
    fabric_plane     = EXCLUDED.fabric_plane,
    trust_score      = EXCLUDED.trust_score,
    data_quality_score = EXCLUDED.data_quality_score,
    received_at      = EXCLUDED.received_at;
"""

_INSERT_RECEIPT = """
INSERT INTO pipe_export_receipts
    (aod_run_id, source, total_connections, pipe_ids, received_at, snapshot_name)
VALUES (%s, %s, %s, %s, %s, %s);
"""


# ---------------------------------------------------------------------------
# Pipe Definition Store
# ---------------------------------------------------------------------------

class PipeDefinitionStore:
    """
    In-memory store for pipe definitions backed by Postgres.

    Persistence hierarchy (most durable first):
    1. Postgres — source of truth, survives Render redeploys.
    2. Redis — write-through cache, fast rehydration (24h TTL).
    3. Disk — JSON fallback if both PG and Redis are unavailable.
    4. In-memory — read cache, always populated on startup.

    Provides the lookup used by the ingest guard to validate
    that a pipe_id has a matching schema before accepting data.
    """

    def __init__(self) -> None:
        self._lock = Lock()
        self._definitions: Dict[str, PipeDefinition] = {}
        self._export_receipts: List[ExportReceipt] = []
        self._redis = _get_redis()
        self._pg_available = False
        self._last_sync_time: float = 0.0

        # Ensure Postgres tables exist, then load from PG first
        self._ensure_pg_tables()
        self._load_from_postgres()

        # Fill gaps from Redis if PG didn't provide data
        if self._redis and not self._definitions:
            self._load_from_redis()

        # Last resort: disk cache
        if not self._definitions:
            self._load_from_disk()

        source = (
            "Postgres" if self._pg_available and self._definitions
            else "Redis" if self._redis and self._definitions
            else "disk" if self._definitions
            else "empty"
        )
        logger.info(
            f"[PipeStore] Initialized with {len(self._definitions)} definitions "
            f"from {source}"
        )

    # ------------------------------------------------------------------
    # Postgres persistence
    # ------------------------------------------------------------------

    def _ensure_pg_tables(self) -> None:
        """Create pipe_definitions and pipe_export_receipts tables if they don't exist."""
        with _pg_conn() as conn:
            if conn is None:
                return
            try:
                with conn.cursor() as cur:
                    cur.execute(_CREATE_DEFINITIONS_TABLE)
                    cur.execute(_CREATE_RECEIPTS_TABLE)
                    # Migrate: add trust_score / data_quality_score if missing
                    for col, default in [
                        ("trust_score", 0),
                        ("data_quality_score", 0),
                    ]:
                        cur.execute(f"""
                            DO $$
                            BEGIN
                                ALTER TABLE pipe_definitions
                                    ADD COLUMN {col} INTEGER DEFAULT {default};
                            EXCEPTION WHEN duplicate_column THEN
                                NULL;
                            END $$;
                        """)
                conn.commit()
                self._pg_available = True
                logger.info("[PipeStore] Postgres tables ensured")
            except Exception as e:
                logger.warning(f"[PipeStore] Failed to create PG tables: {e}")
                try:
                    conn.rollback()
                except Exception:
                    pass

    def _load_from_postgres(self) -> None:
        """Load all pipe definitions and export receipts from Postgres."""
        with _pg_conn() as conn:
            if conn is None:
                return
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT pipe_id, candidate_id, source_name, vendor, category, "
                        "governance_status, fields, entity_scope, identity_keys, "
                        "transport_kind, modality, change_semantics, health, "
                        "last_sync, asset_key, aod_asset_id, fabric_plane, "
                        "trust_score, data_quality_score, received_at "
                        "FROM pipe_definitions"
                    )
                    rows = cur.fetchall()
                    for row in rows:
                        defn = PipeDefinition(
                            pipe_id=row[0],
                            candidate_id=row[1] or "",
                            source_name=row[2] or "",
                            vendor=row[3] or "",
                            category=row[4] or "",
                            governance_status=row[5],
                            fields=row[6] if isinstance(row[6], list) else [],
                            entity_scope=row[7],
                            identity_keys=row[8] if isinstance(row[8], list) else [],
                            transport_kind=row[9],
                            modality=row[10],
                            change_semantics=row[11],
                            health=row[12] or "unknown",
                            last_sync=row[13],
                            asset_key=row[14] or "",
                            aod_asset_id=row[15],
                            fabric_plane=(row[16] or "").lower(),
                            trust_score=row[17] or 0,
                            data_quality_score=row[18] or 0,
                            received_at=str(row[19]) if row[19] else "",
                        )
                        self._definitions[defn.pipe_id] = defn

                    # Load export receipts
                    cur.execute(
                        "SELECT aod_run_id, source, total_connections, pipe_ids, "
                        "received_at, snapshot_name "
                        "FROM pipe_export_receipts ORDER BY id"
                    )
                    for row in cur.fetchall():
                        self._export_receipts.append(ExportReceipt(
                            aod_run_id=row[0],
                            source=row[1] or "aam",
                            total_connections=row[2] or 0,
                            pipe_ids=row[3] if isinstance(row[3], list) else [],
                            received_at=str(row[4]) if row[4] else "",
                            snapshot_name=row[5],
                        ))

                self._pg_available = True
                if self._definitions:
                    logger.info(
                        f"[PipeStore] Loaded from Postgres: "
                        f"{len(self._definitions)} definitions, "
                        f"{len(self._export_receipts)} receipts"
                    )
            except Exception as e:
                logger.warning(f"[PipeStore] Postgres load failed: {e}")

    def _persist_batch_to_postgres(
        self,
        definitions: List[PipeDefinition],
        receipt: ExportReceipt,
    ) -> None:
        """Upsert definitions and insert receipt into Postgres.

        Uses psycopg2.extras.execute_values for batch upsert (single round-trip)
        instead of per-row execute.
        """
        from psycopg2.extras import execute_values

        with _pg_conn() as conn:
            if conn is None:
                return
            try:
                with conn.cursor() as cur:
                    # Batch upsert all definitions via execute_values
                    if definitions:
                        values = [
                            (
                                defn.pipe_id,
                                defn.candidate_id,
                                defn.source_name,
                                defn.vendor,
                                defn.category,
                                defn.governance_status,
                                json.dumps(defn.fields),
                                defn.entity_scope,
                                json.dumps(defn.identity_keys),
                                defn.transport_kind,
                                defn.modality,
                                defn.change_semantics,
                                defn.health,
                                defn.last_sync,
                                defn.asset_key,
                                defn.aod_asset_id,
                                defn.fabric_plane,
                                defn.trust_score,
                                defn.data_quality_score,
                                defn.received_at,
                            )
                            for defn in definitions
                        ]
                        execute_values(
                            cur,
                            """
                            INSERT INTO pipe_definitions (
                                pipe_id, candidate_id, source_name, vendor, category,
                                governance_status, fields, entity_scope, identity_keys,
                                transport_kind, modality, change_semantics, health,
                                last_sync, asset_key, aod_asset_id, fabric_plane,
                                trust_score, data_quality_score, received_at
                            ) VALUES %s
                            ON CONFLICT (pipe_id) DO UPDATE SET
                                candidate_id     = EXCLUDED.candidate_id,
                                source_name      = EXCLUDED.source_name,
                                vendor           = EXCLUDED.vendor,
                                category         = EXCLUDED.category,
                                governance_status = EXCLUDED.governance_status,
                                fields           = EXCLUDED.fields,
                                entity_scope     = EXCLUDED.entity_scope,
                                identity_keys    = EXCLUDED.identity_keys,
                                transport_kind   = EXCLUDED.transport_kind,
                                modality         = EXCLUDED.modality,
                                change_semantics = EXCLUDED.change_semantics,
                                health           = EXCLUDED.health,
                                last_sync        = EXCLUDED.last_sync,
                                asset_key        = EXCLUDED.asset_key,
                                aod_asset_id     = EXCLUDED.aod_asset_id,
                                fabric_plane     = EXCLUDED.fabric_plane,
                                trust_score      = EXCLUDED.trust_score,
                                data_quality_score = EXCLUDED.data_quality_score,
                                received_at      = EXCLUDED.received_at
                            """,
                            values,
                            template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                        )

                    cur.execute(_INSERT_RECEIPT, (
                        receipt.aod_run_id,
                        receipt.source,
                        receipt.total_connections,
                        json.dumps(receipt.pipe_ids),
                        receipt.received_at,
                        receipt.snapshot_name,
                    ))
                conn.commit()
                self._pg_available = True
                logger.info(
                    f"[PipeStore] Persisted {len(definitions)} definitions to Postgres (batch)"
                )
            except Exception as e:
                logger.error(f"[PipeStore] Postgres batch persist failed: {e}")
                try:
                    conn.rollback()
                except Exception:
                    pass
                raise

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
                defn = PipeDefinition(**d)
                if defn.fabric_plane:
                    defn.fabric_plane = defn.fabric_plane.lower()
                self._definitions[pipe_id] = defn
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

    def _sync_from_redis(self) -> None:
        """Reload definitions and export receipts from Redis written by another worker.

        With --workers 2, Worker A may register definitions via /export-pipes
        while Worker B handles /ingest. Without this sync, Worker B's lookup()
        misses valid pipe_ids and creates false NO_MATCHING_PIPE drops.

        Also syncs export receipts so _resolve_export_identity() in the
        ingest route gets the correct snapshot_name cross-worker.

        Throttled to at most once per second to avoid Redis spam during
        burst ingest.
        """
        if not self._redis:
            return
        now = time.monotonic()
        if now - self._last_sync_time < 1.0:
            return
        self._last_sync_time = now
        try:
            # Sync definitions
            raw_defs = self._redis.hgetall(f"{_REDIS_PREFIX}definitions")
            new_count = 0
            if raw_defs:
                with self._lock:
                    for pipe_id, raw in raw_defs.items():
                        if pipe_id not in self._definitions:
                            defn = PipeDefinition(**json.loads(raw))
                            if defn.fabric_plane:
                                defn.fabric_plane = defn.fabric_plane.lower()
                            self._definitions[pipe_id] = defn
                            new_count += 1
            if new_count > 0:
                logger.info(
                    f"[PipeStore] Cross-worker sync: loaded {new_count} new "
                    f"definitions from Redis (total={len(self._definitions)})"
                )

            # Sync export receipts — only replace if Redis has more entries
            raw_receipts = self._redis.get(f"{_REDIS_PREFIX}export_receipts")
            if raw_receipts:
                redis_receipts = [ExportReceipt(**d) for d in json.loads(raw_receipts)]
                with self._lock:
                    if len(redis_receipts) > len(self._export_receipts):
                        self._export_receipts = redis_receipts
                        logger.info(
                            f"[PipeStore] Cross-worker sync: updated export receipts "
                            f"({len(redis_receipts)} from Redis)"
                        )
        except Exception as e:
            logger.warning(f"[PipeStore] Redis sync failed: {e}")

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
                # Normalize fabric_plane to lowercase for consistent counting
                if defn.fabric_plane:
                    defn.fabric_plane = defn.fabric_plane.lower()
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
            if len(self._export_receipts) > _MAX_EXPORT_RECEIPTS:
                self._export_receipts = self._export_receipts[-_MAX_EXPORT_RECEIPTS:]

        # Write-through to Postgres (source of truth)
        self._persist_batch_to_postgres(definitions, receipt)

        # Write-through to Redis (cache)
        for defn in definitions:
            self._persist_definition(defn.pipe_id, defn)
        self._persist_export_receipts()

        # Disk fallback
        self._save_to_disk()

        logger.info(
            f"[PipeStore] Registered {len(definitions)} pipe definitions "
            f"(aod_run_id={aod_run_id})"
        )
        return receipt

    def lookup(self, pipe_id: str) -> Optional[PipeDefinition]:
        """Look up a pipe definition by pipe_id (the JOIN key).

        On cache miss, syncs from Redis to pick up definitions
        registered by another worker (multi-worker support).
        """
        with self._lock:
            defn = self._definitions.get(pipe_id)
        if defn is not None:
            return defn
        # Cache miss — try syncing from Redis (another worker may have it)
        self._sync_from_redis()
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
        """Return all export receipt history.

        Syncs from Redis first so cross-worker reads always see the
        latest receipts (e.g. Worker B reading receipts written by
        Worker A via /export-pipes).
        """
        self._sync_from_redis()
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

    @staticmethod
    def close_pool() -> None:
        """Close the shared Postgres connection pool."""
        _close_shared_pool()

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
                "postgres_connected": self._pg_available,
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
