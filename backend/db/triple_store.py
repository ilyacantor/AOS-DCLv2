"""
TripleStore — data access for the semantic_triples table.

Sync psycopg2, parameterized queries, no business logic.
"""

import io
import json
from backend.core.db import get_connection
from backend.core.constants import INGEST_STATEMENT_TIMEOUT_MS
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

# Per-tenant LIFO cap on tenant_runs. Enforced atomically in swap_and_delete
# and applied one-time via scripts/prune_tenant_runs_cap.py. Per-tenant (not
# global) so the test tenant's entities cannot evict production entities.
TENANT_RUNS_CAP = 10


class TripleStore:

    _COPY_COLS = [
        "tenant_id", "entity_id", "concept", "property", "value",
        "period", "currency", "unit",
        "source_system", "source_table", "source_field",
        "pipe_id", "run_id", "source_run_tag",
        "confidence_score", "confidence_tier",
        "canonical_id", "resolution_method", "resolution_confidence",
        "fabric_plane", "fabric_product",
    ]
    _COPY_SQL = (
        f"COPY semantic_triples ({', '.join(_COPY_COLS)}) "
        f"FROM STDIN WITH (FORMAT text)"
    )

    @staticmethod
    def _copy_escape(val) -> str:
        """Escape a value for PostgreSQL COPY TEXT format."""
        if val is None:
            return "\\N"
        s = str(val)
        s = s.replace("\\", "\\\\")
        s = s.replace("\t", "\\t")
        s = s.replace("\n", "\\n")
        s = s.replace("\r", "\\r")
        return s

    @classmethod
    def _build_copy_buffer(cls, triples: list[dict]) -> io.StringIO:
        """Build a COPY TEXT buffer from a list of triple dicts."""
        escape = cls._copy_escape
        cols = cls._COPY_COLS
        buf = io.StringIO()
        for t in triples:
            row_vals = []
            for c in cols:
                if c == "value":
                    row_vals.append(escape(json.dumps(t["value"])))
                else:
                    row_vals.append(escape(t.get(c)))
            buf.write("\t".join(row_vals))
            buf.write("\n")
        buf.seek(0)
        return buf

    @classmethod
    def _copy_triples_into(cls, cur, triples: list[dict]) -> None:
        """COPY triples into semantic_triples using a caller-owned cursor.

        Does not commit. Used inside swap_and_delete and append_rows_for_entity
        so the insert and the pointer swap share a single transaction.
        """
        if not triples:
            return
        buf = cls._build_copy_buffer(triples)
        cur.copy_expert(cls._COPY_SQL, buf)

    def insert_triples(self, triples: list[dict]) -> int:
        """Batch insert triples using COPY. Owns its own transaction.

        Also UPSERTs into current_triples so the flat live mirror stays in
        sync. Primarily a test-facing primitive post–store-rebuild; the real
        ingest path goes through swap_and_delete, which maintains both tables
        under per-entity pointer semantics.
        """
        if not triples:
            return 0
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET LOCAL statement_timeout = {int(INGEST_STATEMENT_TIMEOUT_MS)}")
                self._copy_triples_into(cur, triples)
                self._sync_current_triples(cur, triples)
                conn.commit()
                return len(triples)

    @staticmethod
    def _sync_current_triples(cur, triples: list[dict]) -> None:
        """Mirror the given semantic_triples rows into current_triples.

        Looks up each row in semantic_triples by (tenant, entity, run, concept,
        property, period) and INSERTs into current_triples ON CONFLICT DO NOTHING.
        Safe to call after a COPY in the same transaction.
        """
        if not triples:
            return
        keys = set()
        for t in triples:
            keys.add((
                t.get("tenant_id"),
                t.get("entity_id"),
                t.get("run_id"),
            ))
        for tenant_id, entity_id, run_id in keys:
            cur.execute(
                """
                INSERT INTO current_triples (
                    id, tenant_id, entity_id, concept, property, value, period,
                    currency, unit, source_system, source_table, source_field,
                    pipe_id, source_run_tag,
                    confidence_score, confidence_tier,
                    canonical_id, resolution_method, resolution_confidence,
                    fabric_plane, fabric_product, created_at
                )
                SELECT
                    id, tenant_id, entity_id, concept, property, value, period,
                    currency, unit, source_system, source_table, source_field,
                    pipe_id, source_run_tag,
                    confidence_score, confidence_tier,
                    canonical_id, resolution_method, resolution_confidence,
                    fabric_plane, fabric_product, created_at
                FROM semantic_triples
                WHERE tenant_id = %s AND entity_id = %s AND run_id = %s
                ON CONFLICT (id) DO NOTHING
                """,
                (tenant_id, entity_id, run_id),
            )

    def get_triples(
        self,
        tenant_id: str,
        concept: str,
        entity_id: str | None = None,
        period: str | None = None,
    ) -> list[dict]:
        """Query by concept against the flat current_triples mirror."""
        clauses = ["tenant_id = %s", "concept = %s"]
        params: list = [tenant_id, concept]

        if entity_id is not None:
            clauses.append("entity_id = %s")
            params.append(entity_id)
        if period is not None:
            clauses.append("period = %s")
            params.append(period)

        where = " AND ".join(clauses)
        sql = f"SELECT * FROM current_triples WHERE {where} ORDER BY created_at"

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                columns = [desc[0] for desc in cur.description]
                return [dict(zip(columns, row)) for row in cur.fetchall()]

    def get_triples_by_run(self, run_id: str) -> list[dict]:
        """All triples from a run."""
        sql = "SELECT * FROM semantic_triples WHERE run_id = %s ORDER BY created_at"
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (run_id,))
                columns = [desc[0] for desc in cur.description]
                return [dict(zip(columns, row)) for row in cur.fetchall()]

    def upsert_tenant_run(
        self, tenant_id: str, new_run_id: str,
        entity_id: str,
        snapshot_name: str | None = None,
    ) -> str | None:
        """Atomically set current_run_id for a (tenant, entity). Returns the previous run_id.

        Entity-scoped: each (tenant_id, entity_id) pair has its own pointer.
        Single-row UPSERT — no table scan, no lock contention.
        """
        sql = """
            INSERT INTO tenant_runs (tenant_id, entity_id, current_run_id, previous_run_id,
                                     current_snapshot_name, previous_snapshot_name, updated_at)
            VALUES (%s, %s, %s, NULL, %s, NULL, now())
            ON CONFLICT (tenant_id, entity_id) DO UPDATE
              SET previous_run_id          = tenant_runs.current_run_id,
                  current_run_id           = EXCLUDED.current_run_id,
                  previous_snapshot_name   = tenant_runs.current_snapshot_name,
                  current_snapshot_name    = EXCLUDED.current_snapshot_name,
                  updated_at              = now()
            RETURNING previous_run_id
        """
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (tenant_id, entity_id, new_run_id, snapshot_name))
                row = cur.fetchone()
                conn.commit()
                return str(row[0]) if row and row[0] else None

    @staticmethod
    def _validate_rows_identity(
        rows: list[dict], tenant_id: str, entity_id: str, run_id: str
    ) -> None:
        """Fail loud if any row's identity triple doesn't match the expected values."""
        for r in rows:
            if r.get("tenant_id") != tenant_id:
                raise ValueError(
                    f"identity mismatch: expected tenant_id={tenant_id}, "
                    f"got {r.get('tenant_id')}"
                )
            if r.get("entity_id") != entity_id:
                raise ValueError(
                    f"identity mismatch: expected entity_id={entity_id}, "
                    f"got {r.get('entity_id')}"
                )
            if r.get("run_id") != run_id:
                raise ValueError(
                    f"identity mismatch: expected run_id={run_id}, "
                    f"got {r.get('run_id')}"
                )

    def append_rows_for_entity(
        self,
        tenant_id: str,
        entity_id: str,
        new_run_id: str,
        new_rows: list[dict],
        last_farm_run_id: str | None = None,
    ) -> int:
        """Append rows for an in-progress entity run (hot path).

        Per-batch work: COPY into semantic_triples, mirror new rows into
        current_triples, merge enrichment aggregates into tenant_runs. The
        merge is incremental — source_systems and fabric_pairs are aggregated
        from the in-memory batch and UNIONed with the existing arrays in SQL
        (no re-scan of current_triples). unique_pipes is maintained via a
        single narrow COUNT(DISTINCT pipe_id) over the entity slice, which
        uses idx_current_triples_entity and is far cheaper than the pre-fix
        ARRAY_AGG DISTINCT that ran ~300–500ms per batch.

        Transitions (a different current_run_id already exists for the entity)
        must go through swap_and_delete so the prior run is archived; this
        method raises ValueError in that case instead of silently creating an
        untracked run.

        The per-tenant LIFO cap is NOT enforced here — appending to an
        already-tracked (tenant, entity) row cannot introduce a new entity,
        so the cap is owned exclusively by swap_and_delete.
        """
        if not tenant_id or not entity_id or not new_run_id:
            raise ValueError(
                "append_rows_for_entity requires tenant_id, entity_id, new_run_id"
            )
        if not new_rows:
            return 0
        self._validate_rows_identity(new_rows, tenant_id, entity_id, new_run_id)

        # Aggregate enrichment deltas from the in-memory batch so the UPSERT
        # can merge without re-scanning current_triples.
        batch_sources: set[str] = set()
        batch_fabric_pairs: set[str] = set()
        for r in new_rows:
            src = r.get("source_system")
            if src:
                batch_sources.add(src)
            plane = r.get("fabric_plane")
            if plane:
                pair = f"{plane}|{r.get('fabric_product') or ''}|{src or ''}"
                batch_fabric_pairs.add(pair)

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SET LOCAL statement_timeout = {int(INGEST_STATEMENT_TIMEOUT_MS)}"
                )

                # Reject transitions — archive lives in swap_and_delete.
                # NOTE: we do NOT pre-read run_row_count here. Concurrent
                # append batches for the same entity used to read the same
                # prev value, each compute prev + batch_size in Python, then
                # each overwrite with EXCLUDED.run_row_count, losing one
                # batch's contribution. The UPSERT below now increments
                # run_row_count atomically under the row lock (see step 5).
                cur.execute(
                    "SELECT current_run_id FROM tenant_runs "
                    "WHERE tenant_id = %s AND entity_id = %s",
                    (tenant_id, entity_id),
                )
                existing = cur.fetchone()
                if existing and str(existing[0]) != str(new_run_id):
                    raise ValueError(
                        f"append_rows_for_entity: cannot transition from "
                        f"run_id={existing[0]} to {new_run_id} on entity "
                        f"{entity_id}. Use swap_and_delete for transitions."
                    )

                # 1. COPY new rows into semantic_triples
                self._copy_triples_into(cur, new_rows)

                # 2. Mirror the newly-COPYed rows into current_triples.
                #    NOT EXISTS filters out rows already mirrored by prior batches
                #    in this run so the re-ingest-safe invariant holds even if a
                #    caller replays the same batch.
                cur.execute(
                    """
                    INSERT INTO current_triples (
                        id, tenant_id, entity_id, concept, property, value, period,
                        currency, unit, source_system, source_table, source_field,
                        pipe_id, source_run_tag,
                        confidence_score, confidence_tier,
                        canonical_id, resolution_method, resolution_confidence,
                        fabric_plane, fabric_product, created_at
                    )
                    SELECT
                        st.id, st.tenant_id, st.entity_id, st.concept, st.property,
                        st.value, st.period, st.currency, st.unit,
                        st.source_system, st.source_table, st.source_field,
                        st.pipe_id, st.source_run_tag,
                        st.confidence_score, st.confidence_tier,
                        st.canonical_id, st.resolution_method, st.resolution_confidence,
                        st.fabric_plane, st.fabric_product, st.created_at
                    FROM semantic_triples st
                    WHERE st.tenant_id = %s AND st.entity_id = %s AND st.run_id = %s
                      AND NOT EXISTS (
                          SELECT 1 FROM current_triples ct WHERE ct.id = st.id
                      )
                    """,
                    (tenant_id, entity_id, new_run_id),
                )

                # 3. Narrow unique_pipes recount — the only aggregate we cannot
                #    derive from the in-memory batch (we do not store the full
                #    pipe_id set on tenant_runs). Scoped to one entity slice
                #    via idx_current_triples_entity.
                cur.execute(
                    "SELECT COUNT(DISTINCT pipe_id) FROM current_triples "
                    "WHERE tenant_id = %s AND entity_id = %s",
                    (tenant_id, entity_id),
                )
                pipe_count_row = cur.fetchone()
                unique_pipes_total = int(pipe_count_row[0] or 0) if pipe_count_row else 0

                # 4. UPSERT tenant_runs with the incremental enrichment merge.
                #    CRITICAL: run_row_count is an ATOMIC INCREMENT on the
                #    conflict path — concurrent append batches under the same
                #    (tenant, entity) serialize on the row lock acquired by
                #    the UPSERT, so each batch's contribution accrues.
                #    batch_delta = len(new_rows) (passed via EXCLUDED).
                #    On INSERT (fresh row), EXCLUDED.run_row_count IS the
                #    initial count which is equivalent.
                cur.execute(
                    """
                    INSERT INTO tenant_runs (
                        tenant_id, entity_id, current_run_id, previous_run_id,
                        current_snapshot_name, previous_snapshot_name,
                        run_row_count, previous_run_row_count, updated_at,
                        source_systems, fabric_pairs, unique_pipes,
                        first_received_at, latest_received_at,
                        last_farm_run_id
                    )
                    VALUES (%s, %s, %s, NULL, NULL, NULL, %s, NULL, now(),
                            %s::text[], %s::text[], %s, now(), now(), %s)
                    ON CONFLICT (tenant_id, entity_id) DO UPDATE
                      SET run_row_count = tenant_runs.run_row_count + EXCLUDED.run_row_count,
                          source_systems = (
                              SELECT COALESCE(array_agg(DISTINCT s ORDER BY s), ARRAY[]::text[])
                              FROM unnest(
                                  COALESCE(tenant_runs.source_systems, ARRAY[]::text[])
                                  || EXCLUDED.source_systems
                              ) s
                              WHERE s IS NOT NULL
                          ),
                          fabric_pairs = (
                              SELECT COALESCE(array_agg(DISTINCT p ORDER BY p), ARRAY[]::text[])
                              FROM unnest(
                                  COALESCE(tenant_runs.fabric_pairs, ARRAY[]::text[])
                                  || EXCLUDED.fabric_pairs
                              ) p
                              WHERE p IS NOT NULL
                          ),
                          unique_pipes = EXCLUDED.unique_pipes,
                          first_received_at = COALESCE(tenant_runs.first_received_at, now()),
                          latest_received_at = now(),
                          updated_at = now(),
                          last_farm_run_id = COALESCE(EXCLUDED.last_farm_run_id, tenant_runs.last_farm_run_id)
                    """,
                    (
                        tenant_id, entity_id, new_run_id, len(new_rows),
                        sorted(batch_sources),
                        sorted(batch_fabric_pairs),
                        unique_pipes_total,
                        last_farm_run_id,
                    ),
                )

                conn.commit()
                return len(new_rows)

    @staticmethod
    def _compute_entity_enrichment(cur, tenant_id: str, entity_id: str) -> dict:
        """Compute denormalized aggregates for a single (tenant, entity) slice.

        Scoped to one entity so the query hits idx_current_triples_entity
        (tenant_id, entity_id) and touches only that slice's rows — cheap even
        when the tenant has hundreds of thousands of rows total. Results are
        written into tenant_runs so /api/dcl/snapshots can read them without
        re-aggregating at request time (B18 budget).
        """
        cur.execute(
            """
            SELECT
                COALESCE(
                    ARRAY_AGG(DISTINCT source_system) FILTER (WHERE source_system IS NOT NULL),
                    ARRAY[]::text[]
                ),
                COALESCE(
                    ARRAY_AGG(DISTINCT fabric_plane || '|' || COALESCE(fabric_product, '') || '|' || COALESCE(source_system, ''))
                        FILTER (WHERE fabric_plane IS NOT NULL),
                    ARRAY[]::text[]
                ),
                COUNT(DISTINCT pipe_id),
                MIN(created_at),
                MAX(created_at)
            FROM current_triples
            WHERE tenant_id = %s AND entity_id = %s
            """,
            (tenant_id, entity_id),
        )
        row = cur.fetchone()
        if row is None:
            return {
                "source_systems": [],
                "fabric_pairs": [],
                "unique_pipes": 0,
                "first_received_at": None,
                "latest_received_at": None,
            }
        source_systems, fabric_pairs, unique_pipes, first_at, last_at = row
        return {
            "source_systems": list(source_systems or []),
            "fabric_pairs": list(fabric_pairs or []),
            "unique_pipes": int(unique_pipes or 0),
            "first_received_at": first_at,
            "latest_received_at": last_at,
        }

    @staticmethod
    def _enforce_tenant_cap(
        cur, tenant_id: str, cap: int = TENANT_RUNS_CAP
    ) -> list[dict]:
        """Evict tenant_runs rows beyond the per-tenant cap, LIFO by updated_at.

        Keeps the `cap` most-recently-updated entries for `tenant_id` and hard-
        deletes older ones from tenant_runs, semantic_triples, and current_triples.
        Must run inside the same transaction as the swap so the cap is atomic.
        Returns a list of eviction descriptors for logging.
        """
        cur.execute(
            """
            SELECT tenant_id, entity_id, run_row_count, updated_at
            FROM tenant_runs
            WHERE tenant_id = %s
            ORDER BY updated_at DESC
            OFFSET %s
            """,
            (tenant_id, cap),
        )
        evicted_rows = cur.fetchall()
        if not evicted_rows:
            return []

        evictions: list[dict] = []
        for (ev_tenant, ev_entity, ev_rowcount, ev_updated) in evicted_rows:
            cur.execute(
                "DELETE FROM semantic_triples WHERE tenant_id = %s AND entity_id = %s",
                (ev_tenant, ev_entity),
            )
            semantic_deleted = cur.rowcount
            cur.execute(
                "DELETE FROM current_triples WHERE tenant_id = %s AND entity_id = %s",
                (ev_tenant, ev_entity),
            )
            current_deleted = cur.rowcount
            cur.execute(
                "DELETE FROM tenant_runs WHERE tenant_id = %s AND entity_id = %s",
                (ev_tenant, ev_entity),
            )
            evictions.append({
                "tenant_id": str(ev_tenant),
                "entity_id": ev_entity,
                "run_row_count": ev_rowcount,
                "updated_at": ev_updated.isoformat() if ev_updated else None,
                "semantic_deleted": semantic_deleted,
                "current_deleted": current_deleted,
            })
        return evictions

    def swap_and_delete(
        self,
        tenant_id: str,
        entity_id: str,
        new_run_id: str,
        snapshot_name: str | None,
        new_rows: list[dict],
        replace: bool = False,
        last_farm_run_id: str | None = None,
    ) -> tuple[str | None, int, int]:
        """Atomic per-entity run swap — insert new rows, hard-delete prior, rebuild current_triples.

        Single transaction per (tenant_id, entity_id):
          0. If replace=True, DELETE existing rows for (tenant, entity, new_run_id)
             so a re-ingest of the same run_id fully replaces instead of appending.
             Does NOT touch rows under other run_ids — those flow through Step 4.
          1. COPY new_rows into semantic_triples.
          2. Count total rows for (tenant, entity, new_run_id) — covers prior
             append_rows_for_entity batches as well as the final batch.
          3. UPSERT tenant_runs (atomic pointer swap). Captures previous_run_id
             and previous_run_row_count.
          4. If previous_run_id exists and differs from new_run_id, ensure
             archive partitions, copy prior rows into semantic_triples_archive,
             delete them from semantic_triples. archived_count must equal
             deleted_count or the whole transaction aborts.
          5. Rebuild the (tenant, entity) slice of current_triples from
             semantic_triples for new_run_id.

        Returns (previous_run_id, archived_count, new_row_count).
        """
        if not tenant_id or not entity_id or not new_run_id:
            raise ValueError(
                "swap_and_delete requires tenant_id, entity_id, new_run_id"
            )
        if new_rows:
            self._validate_rows_identity(new_rows, tenant_id, entity_id, new_run_id)

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SET LOCAL statement_timeout = {int(INGEST_STATEMENT_TIMEOUT_MS)}"
                )

                # Step 0: replace semantics — clear prior rows under the same run_id
                if replace:
                    cur.execute(
                        "DELETE FROM semantic_triples "
                        "WHERE tenant_id = %s AND entity_id = %s AND run_id = %s",
                        (tenant_id, entity_id, new_run_id),
                    )

                # Step 1: COPY new rows (append to any already present from prior batches)
                if new_rows:
                    self._copy_triples_into(cur, new_rows)

                # Step 2: total row count for this (tenant, entity, run_id)
                cur.execute(
                    "SELECT COUNT(*) FROM semantic_triples "
                    "WHERE tenant_id = %s AND entity_id = %s AND run_id = %s",
                    (tenant_id, entity_id, new_run_id),
                )
                new_row_count = cur.fetchone()[0]
                if new_row_count == 0:
                    raise ValueError(
                        f"swap_and_delete: no rows found for "
                        f"tenant={tenant_id} entity={entity_id} run={new_run_id}"
                    )

                # Step 3: UPSERT tenant_runs — atomic pointer swap. The
                #    enrichment aggregates (source_systems, fabric_pairs, etc.)
                #    are computed AFTER current_triples is rebuilt in Step 5,
                #    so we placeholder them here and re-UPDATE at the end.
                cur.execute(
                    """
                    INSERT INTO tenant_runs (
                        tenant_id, entity_id, current_run_id, previous_run_id,
                        current_snapshot_name, previous_snapshot_name,
                        run_row_count, previous_run_row_count, updated_at,
                        last_farm_run_id
                    )
                    VALUES (%s, %s, %s, NULL, %s, NULL, %s, NULL, now(), %s)
                    ON CONFLICT (tenant_id, entity_id) DO UPDATE
                      SET previous_run_id        = tenant_runs.current_run_id,
                          previous_run_row_count = tenant_runs.run_row_count,
                          previous_snapshot_name = tenant_runs.current_snapshot_name,
                          current_run_id         = EXCLUDED.current_run_id,
                          run_row_count          = EXCLUDED.run_row_count,
                          current_snapshot_name  = EXCLUDED.current_snapshot_name,
                          updated_at             = now(),
                          last_farm_run_id       = COALESCE(EXCLUDED.last_farm_run_id, tenant_runs.last_farm_run_id)
                    RETURNING previous_run_id
                    """,
                    (
                        tenant_id, entity_id, new_run_id, snapshot_name,
                        new_row_count, last_farm_run_id,
                    ),
                )
                row = cur.fetchone()
                previous_run_id = str(row[0]) if row and row[0] else None

                # Step 4: archive + hard-delete prior run
                archived_count = 0
                if previous_run_id and previous_run_id != new_run_id:
                    cur.execute(
                        """
                        SELECT DISTINCT date_trunc('month', created_at)
                        FROM semantic_triples
                        WHERE tenant_id = %s AND entity_id = %s AND run_id = %s
                        """,
                        (tenant_id, entity_id, previous_run_id),
                    )
                    for (month,) in cur.fetchall():
                        cur.execute("SELECT ensure_archive_partition(%s)", (month,))

                    cur.execute(
                        """
                        INSERT INTO semantic_triples_archive (
                            id, tenant_id, entity_id, concept, property, value, period,
                            currency, unit, source_system, source_table, source_field,
                            pipe_id, run_id, source_run_tag,
                            confidence_score, confidence_tier,
                            canonical_id, resolution_method, resolution_confidence,
                            fabric_plane, fabric_product, created_at, updated_at
                        )
                        SELECT
                            id, tenant_id, entity_id, concept, property, value, period,
                            currency, unit, source_system, source_table, source_field,
                            pipe_id, run_id, source_run_tag,
                            confidence_score, confidence_tier,
                            canonical_id, resolution_method, resolution_confidence,
                            fabric_plane, fabric_product, created_at, updated_at
                        FROM semantic_triples
                        WHERE tenant_id = %s AND entity_id = %s AND run_id = %s
                        """,
                        (tenant_id, entity_id, previous_run_id),
                    )
                    archived_count = cur.rowcount

                    cur.execute(
                        "DELETE FROM semantic_triples "
                        "WHERE tenant_id = %s AND entity_id = %s AND run_id = %s",
                        (tenant_id, entity_id, previous_run_id),
                    )
                    deleted_count = cur.rowcount
                    if deleted_count != archived_count:
                        raise RuntimeError(
                            f"swap_and_delete: archive/delete mismatch — "
                            f"archived={archived_count} deleted={deleted_count} "
                            f"tenant={tenant_id} entity={entity_id} prev={previous_run_id}"
                        )

                # Step 5: rebuild current_triples slice for (tenant, entity)
                cur.execute(
                    "DELETE FROM current_triples "
                    "WHERE tenant_id = %s AND entity_id = %s",
                    (tenant_id, entity_id),
                )
                cur.execute(
                    """
                    INSERT INTO current_triples (
                        id, tenant_id, entity_id, concept, property, value, period,
                        currency, unit, source_system, source_table, source_field,
                        pipe_id, source_run_tag,
                        confidence_score, confidence_tier,
                        canonical_id, resolution_method, resolution_confidence,
                        fabric_plane, fabric_product, created_at
                    )
                    SELECT
                        id, tenant_id, entity_id, concept, property, value, period,
                        currency, unit, source_system, source_table, source_field,
                        pipe_id, source_run_tag,
                        confidence_score, confidence_tier,
                        canonical_id, resolution_method, resolution_confidence,
                        fabric_plane, fabric_product, created_at
                    FROM semantic_triples
                    WHERE tenant_id = %s AND entity_id = %s AND run_id = %s
                    ON CONFLICT (id) DO NOTHING
                    """,
                    (tenant_id, entity_id, new_run_id),
                )

                # Step 5b: refresh denormalized enrichment aggregates now that
                # current_triples reflects the new slice. Kept in tenant_runs
                # so /api/dcl/snapshots answers via a trivial 10-row read (B18).
                enrichment = self._compute_entity_enrichment(
                    cur, tenant_id, entity_id
                )
                cur.execute(
                    """
                    UPDATE tenant_runs
                       SET source_systems     = %s,
                           fabric_pairs       = %s,
                           unique_pipes       = %s,
                           first_received_at  = %s,
                           latest_received_at = %s
                     WHERE tenant_id = %s AND entity_id = %s
                    """,
                    (
                        enrichment["source_systems"],
                        enrichment["fabric_pairs"],
                        enrichment["unique_pipes"],
                        enrichment["first_received_at"],
                        enrichment["latest_received_at"],
                        tenant_id,
                        entity_id,
                    ),
                )

                # Step 6: enforce per-tenant LIFO cap on tenant_runs (atomic
                # with the swap). Only evicts rows for the inserting tenant
                # so concurrent tenants never collide.
                evictions = self._enforce_tenant_cap(cur, tenant_id)
                if evictions:
                    logger.info(
                        "tenant_runs cap=%d enforced for tenant=%s evicted=%d",
                        TENANT_RUNS_CAP, tenant_id, len(evictions),
                    )

                conn.commit()

        return previous_run_id, archived_count, new_row_count

    def resolve_single_tenant(self) -> str:
        """Return tenant_id if exactly one tenant exists in tenant_runs.

        Raises ValueError if zero or multiple tenants exist — no guessing.
        Used by /api/dcl/run when caller omits tenant_id.
        """
        sql = "SELECT tenant_id FROM tenant_runs"
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
        if len(rows) == 0:
            raise ValueError(
                "No tenants in tenant_runs. Run the ingest pipeline first."
            )
        if len(rows) > 1:
            tenant_ids = [str(r[0]) for r in rows]
            raise ValueError(
                f"Multiple tenants in tenant_runs: {tenant_ids}. "
                f"Specify tenant_id explicitly."
            )
        return str(rows[0][0])

    def resolve_tenant_for_entity(self, entity_id: str) -> str:
        """Return tenant_id for a given entity_id via tenant_runs.

        Raises ValueError if zero or multiple tenants match.
        """
        sql = (
            "SELECT DISTINCT tenant_id FROM tenant_runs "
            "WHERE entity_id = %s LIMIT 2"
        )
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (entity_id,))
                rows = cur.fetchall()
        if not rows:
            raise ValueError(
                f"No active tenant found for entity_id={entity_id}. "
                f"Ensure triples have been ingested for this entity."
            )
        if len(rows) > 1:
            raise ValueError(
                f"Multiple tenants found for entity_id={entity_id}: "
                f"{[str(r[0]) for r in rows]}. Specify tenant_id explicitly."
            )
        return str(rows[0][0])

    def get_entity_run_meta(
        self, tenant_id: str, entity_id: str,
    ) -> dict:
        """Return run metadata for a (tenant, entity) pair from tenant_runs.

        Raises ValueError if no row exists — no silent empty returns.
        """
        sql = (
            "SELECT current_run_id::text, current_snapshot_name, updated_at "
            "FROM tenant_runs "
            "WHERE tenant_id::text = %s AND entity_id = %s"
        )
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (tenant_id, entity_id))
                row = cur.fetchone()
        if not row:
            raise ValueError(
                f"No tenant_runs entry for tenant_id={tenant_id}, "
                f"entity_id={entity_id}. "
                f"Run the ingest pipeline first to populate tenant_runs."
            )
        return {
            "current_run_id": row[0],
            "current_snapshot_name": row[1],
            "updated_at": row[2].isoformat() + "Z" if row[2] else None,
        }

    def get_current_run_id(
        self, tenant_id: str, entity_id: str | None = None,
    ) -> str:
        """Return current_run_id for a (tenant, entity) pair.

        When entity_id is given, returns the exact pointer for that entity.
        When omitted, returns the most recently updated pointer for the tenant.

        Raises ValueError if no entry exists — no silent empty returns.
        Callers that need a best-effort fallback should catch ValueError.
        """
        if entity_id:
            sql = "SELECT current_run_id FROM tenant_runs WHERE tenant_id = %s AND entity_id = %s"
            params: tuple = (tenant_id, entity_id)
        else:
            sql = "SELECT current_run_id FROM tenant_runs WHERE tenant_id = %s ORDER BY updated_at DESC LIMIT 1"
            params = (tenant_id,)
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                row = cur.fetchone()
        if row is None:
            raise ValueError(
                f"No current_run_id registered for tenant {tenant_id}"
                f"{f', entity {entity_id}' if entity_id else ''}. "
                f"Run the ingest pipeline first to populate tenant_runs."
            )
        return str(row[0])

    def count_by_domain(self, tenant_id: str | None, run_id: str | None = None, entity_id: str | None = None) -> dict:
        """Count triples grouped by root concept domain (first segment before dot).

        When run_id is provided, counts from semantic_triples (the write-ahead
        log with historical run_id). Otherwise counts from current_triples
        (the flat live mirror, run-agnostic).
        """
        clauses: list[str] = []
        params: list = []
        if tenant_id is not None:
            clauses.append("tenant_id = %s")
            params.append(tenant_id)
        if entity_id is not None:
            clauses.append("entity_id = %s")
            params.append(entity_id)

        if run_id is not None:
            clauses.append("run_id = %s")
            params.append(run_id)
            where = " WHERE " + " AND ".join(clauses)
            sql = (
                f"SELECT split_part(concept, '.', 1) AS domain, COUNT(*) AS cnt "
                f"FROM semantic_triples{where} "
                f"GROUP BY domain ORDER BY domain"
            )
        else:
            where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
            sql = (
                f"SELECT split_part(concept, '.', 1) AS domain, COUNT(*) AS cnt "
                f"FROM current_triples{where} "
                f"GROUP BY domain ORDER BY domain"
            )

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return {row[0]: row[1] for row in cur.fetchall()}

    def count_by_run(self, run_id: str) -> int:
        """Count triples for a given run_id."""
        sql = "SELECT COUNT(*) FROM semantic_triples WHERE run_id = %s"
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (run_id,))
                return cur.fetchone()[0]

    def run_exists(self, run_id: str) -> bool:
        """Check if any triples exist for a run_id."""
        sql = "SELECT EXISTS(SELECT 1 FROM semantic_triples WHERE run_id = %s)"
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (run_id,))
                return cur.fetchone()[0]

    def get_run_info(self, run_id: str) -> dict | None:
        """Get summary info for a run."""
        sql = (
            "SELECT run_id, COUNT(*) as triple_count, "
            "MIN(created_at) as created_at "
            "FROM semantic_triples WHERE run_id = %s "
            "GROUP BY run_id"
        )
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (run_id,))
                row = cur.fetchone()
                if row is None:
                    return None
                columns = [desc[0] for desc in cur.description]
                return dict(zip(columns, row))

    def list_runs(self, tenant_id: str | None = None) -> list[dict]:
        """List all runs, most recent first."""
        if tenant_id:
            sql = (
                "SELECT run_id, tenant_id, COUNT(*) as triple_count, "
                "MIN(created_at) as created_at "
                "FROM semantic_triples WHERE tenant_id = %s "
                "GROUP BY run_id, tenant_id ORDER BY MIN(created_at) DESC"
            )
            params = (tenant_id,)
        else:
            sql = (
                "SELECT run_id, tenant_id, COUNT(*) as triple_count, "
                "MIN(created_at) as created_at "
                "FROM semantic_triples "
                "GROUP BY run_id, tenant_id ORDER BY MIN(created_at) DESC"
            )
            params = ()

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                columns = [desc[0] for desc in cur.description]
                return [dict(zip(columns, row)) for row in cur.fetchall()]

    def count_active(self, tenant_id: str, entity_id: str | None = None) -> int:
        """Count triples for a tenant, optionally scoped to one entity."""
        sql = "SELECT COUNT(*) FROM current_triples WHERE tenant_id = %s"
        params: list = [tenant_id]
        if entity_id:
            sql += " AND entity_id = %s"
            params.append(entity_id)
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchone()[0]

    def count_total_rows(self) -> int:
        """Count ALL rows in current_triples (all tenants)."""
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM current_triples")
                return cur.fetchone()[0]

    def get_source_run_ids(self, tenant_id: str) -> list[dict]:
        """Return current_run_ids for a specific tenant, most recent first.

        Each row: {run_id: str, created_at: datetime, triple_count: int}
        Returns all active entity runs for the tenant from tenant_runs.
        """
        sql = (
            "SELECT current_run_id AS run_id, updated_at AS created_at, "
            "run_row_count AS triple_count "
            "FROM tenant_runs WHERE tenant_id = %s "
            "ORDER BY updated_at DESC"
        )
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (tenant_id,))
                columns = [desc[0] for desc in cur.description]
                return [dict(zip(columns, row)) for row in cur.fetchall()]

    def get_run_entities(self, run_id: str) -> list[str]:
        """Return entity_ids whose current_run_id matches this run_id."""
        sql = (
            "SELECT entity_id FROM tenant_runs "
            "WHERE current_run_id = %s AND entity_id IS NOT NULL "
            "ORDER BY entity_id"
        )
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (run_id,))
                return [row[0] for row in cur.fetchall()]

    def get_persona_domain_stats(self, persona_domains: dict[str, list[str]]) -> dict:
        """Compute per-persona stats from active triples by domain mapping.

        Args:
            persona_domains: mapping of persona key to list of triple domains.
                e.g. {"CFO": ["revenue", "cogs", ...], "CRO": [...]}

        Returns:
            dict keyed by persona, each with data_sources, domains, triple_count, domain_list.
        """
        # Single query: get per-domain stats from the flat live mirror.
        sql = (
            "SELECT split_part(concept, '.', 1) AS domain, "
            "COUNT(DISTINCT source_system) AS source_count, "
            "COUNT(*) AS triple_count "
            "FROM current_triples "
            "GROUP BY domain"
        )
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                domain_stats: dict[str, dict] = {}
                for row in cur.fetchall():
                    domain_stats[row[0]] = {
                        "source_count": row[1],
                        "triple_count": row[2],
                    }

        result = {}
        for persona, domains in persona_domains.items():
            matched_domains = []
            total_sources: set[str] = set()
            total_triples = 0

            for d in domains:
                if d in domain_stats:
                    matched_domains.append(d)

            # Need distinct source_system across all matched domains.
            if matched_domains:
                placeholders = ", ".join(["%s"] * len(matched_domains))
                src_sql = (
                    f"SELECT COUNT(DISTINCT source_system) "
                    f"FROM current_triples "
                    f"WHERE split_part(concept, '.', 1) IN ({placeholders})"
                )
                with get_connection() as conn2:
                    with conn2.cursor() as cur2:
                        cur2.execute(src_sql, matched_domains)
                        data_sources = cur2.fetchone()[0]
            else:
                data_sources = 0

            for d in matched_domains:
                total_triples += domain_stats[d]["triple_count"]

            result[persona] = {
                "data_sources": data_sources,
                "domains": len(matched_domains),
                "triple_count": total_triples,
                "domain_list": matched_domains,
            }

        return result

    def get_sankey_aggregation(self, tenant_id: str, entity_id: str | None = None) -> list[dict]:
        """Aggregate triples for Sankey visualization, scoped to a tenant.

        Returns rows of {fabric_plane, fabric_product, source_system, domain,
        entity_id, triple_count} grouped by fabric × source × domain × entity.
        When entity_id is provided, only that entity's triples are aggregated.
        """
        sql = (
            "SELECT COALESCE(fabric_plane, 'unattributed') AS fabric_plane, "
            "COALESCE(fabric_product, 'unknown') AS fabric_product, "
            "source_system, split_part(concept, '.', 1) AS domain, "
            "entity_id, COUNT(*) AS triple_count "
            "FROM current_triples "
            "WHERE tenant_id = %s "
        )
        params: list = [tenant_id]
        if entity_id:
            sql += "AND entity_id = %s "
            params.append(entity_id)
        sql += (
            "GROUP BY COALESCE(fabric_plane, 'unattributed'), "
            "COALESCE(fabric_product, 'unknown'), "
            "source_system, split_part(concept, '.', 1), entity_id "
            "ORDER BY triple_count DESC"
        )
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                columns = [desc[0] for desc in cur.description]
                return [dict(zip(columns, row)) for row in cur.fetchall()]

    def get_concept_collisions(self, tenant_id: str, entity_id: str | None = None) -> list[dict]:
        """Detect concepts written by multiple source_systems in the current run.

        Returns rows of {entity_id, concept, property, period, sources} where
        sources is a comma-separated list of all source_systems that wrote that
        (entity_id, concept, property, period) combination. Only rows with
        2+ distinct sources are returned.

        The caller uses concept_authority.pick_primary() to rank these.
        """
        sql = (
            "SELECT entity_id, concept, property, period, "
            "string_agg(DISTINCT source_system, ',' ORDER BY source_system) AS sources, "
            "COUNT(DISTINCT source_system) AS source_count "
            "FROM current_triples "
            "WHERE tenant_id = %s "
        )
        params: list = [tenant_id]
        if entity_id:
            sql += "AND entity_id = %s "
            params.append(entity_id)
        sql += (
            "GROUP BY entity_id, concept, property, period "
            "HAVING COUNT(DISTINCT source_system) > 1 "
            "ORDER BY concept, entity_id, period"
        )
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                columns = [desc[0] for desc in cur.description]
                return [dict(zip(columns, row)) for row in cur.fetchall()]

    def get_fabric_planes(self, tenant_id: str) -> list[str]:
        """Return distinct fabric_plane:fabric_product pairs for the live mirror.

        Excludes rows where fabric_plane is NULL or 'none'. Returns sorted list
        of 'plane:product' strings matching sourceFabricPlanes in GraphSnapshot.meta.
        """
        sql = (
            "SELECT DISTINCT fabric_plane, fabric_product "
            "FROM current_triples "
            "WHERE tenant_id = %s "
            "AND fabric_plane IS NOT NULL AND fabric_plane != 'none' "
            "ORDER BY fabric_plane, fabric_product"
        )
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, [tenant_id])
                return sorted(
                    f"{row[0]}:{row[1]}" for row in cur.fetchall()
                    if row[0] and row[1]
                )

    def delete_by_run(self, run_id: str) -> int:
        """Hard-delete all triples for a run (test cleanup only).

        Clears both semantic_triples and the current_triples mirror so tests
        that call this between methods start with a clean slate.
        """
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT DISTINCT tenant_id, entity_id FROM semantic_triples "
                    "WHERE run_id = %s",
                    (run_id,),
                )
                affected = cur.fetchall()
                cur.execute(
                    "DELETE FROM semantic_triples WHERE run_id = %s",
                    (run_id,),
                )
                deleted = cur.rowcount
                for tenant_id, entity_id in affected:
                    cur.execute(
                        "DELETE FROM current_triples "
                        "WHERE tenant_id = %s AND entity_id = %s",
                        (tenant_id, entity_id),
                    )
                conn.commit()
                return deleted
