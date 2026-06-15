"""
TripleStore — data access for the semantic_triples table.

Sync psycopg2, parameterized queries, no business logic.

Bi-temporal store (Gate 0, ContextOS_Blueprint_v1 §6/§15): every fact carries
valid_from/valid_to (world time) and ingested_at/superseded_at (knowledge
time). Lifecycle writes CLOSE a fact's knowledge window (SET superseded_at)
— they never delete. is_active is a STORED generated column defined as
(superseded_at IS NULL); any code that still writes is_active fails loudly
at the database. Hard DELETEs survive only in the explicit retention tools
(delete_inactive / purge_old_runs / delete_by_run — B19 operator scope) and
in the same-run redelivery scrub inside replace_tenant_triples.
"""

import io
import json
from backend.core.db import get_connection
from backend.core.constants import INGEST_STATEMENT_TIMEOUT_MS
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)


class TripleStore:

    _COPY_COLS = [
        "tenant_id", "entity_id", "concept", "property", "value",
        "period", "currency", "unit",
        "source_system", "source_table", "source_field",
        "pipe_id", "run_id", "source_run_tag",
        "confidence_score", "confidence_tier",
        "canonical_id", "resolution_method", "resolution_confidence",
        "fabric_plane", "fabric_product",
        # JSONB — write-time normalization provenance (mig028). NULL when the
        # value was already canonical (base unit, canonical currency, canonical
        # period). Serialized as JSON for COPY exactly like `value`.
        "normalization_metadata",
    ]
    # JSONB columns: COPY needs their Python value json.dumps'd, not str()'d.
    _JSON_COPY_COLS = frozenset({"value", "normalization_metadata"})
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

    def insert_triples(self, triples: list[dict]) -> int:
        """Batch insert triples using COPY for maximum throughput."""
        if not triples:
            return 0

        escape = self._copy_escape
        cols = self._COPY_COLS
        json_cols = self._JSON_COPY_COLS
        buf = io.StringIO()
        for t in triples:
            row_vals = []
            for c in cols:
                if c in json_cols:
                    v = t.get(c)
                    # NULL stays NULL (\N); a present value is JSON-serialized.
                    row_vals.append(escape(json.dumps(v) if v is not None else None))
                else:
                    row_vals.append(escape(t.get(c)))
            buf.write("\t".join(row_vals))
            buf.write("\n")
        buf.seek(0)

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET LOCAL statement_timeout = {int(INGEST_STATEMENT_TIMEOUT_MS)}")
                cur.copy_expert(self._COPY_SQL, buf)
                conn.commit()
                return len(triples)

    def replace_tenant_triples(self, tenant_id: str, triples: list[dict]) -> int:
        """Atomically supersede prior live triples, then COPY-insert new batch.

        Bi-temporal semantics: prior runs' live rows get superseded_at=now()
        — history is preserved, nothing from earlier runs is deleted. The one
        DELETE that remains is the same-run redelivery scrub: rows already
        carrying THIS batch's run_id are the same ingest event re-delivered
        (?replace=true idempotent replay) and re-inserting without scrubbing
        would duplicate the event's own history.

        Scopes to entity_ids present in the incoming triples so replacing one
        entity's data does not supersede another entity's triples within the
        same tenant. All statements share one transaction — if COPY fails the
        scrub and supersession roll back.
        """
        if not tenant_id:
            raise ValueError("replace_tenant_triples requires tenant_id")
        if not triples:
            return 0

        run_ids = {str(t["run_id"]) for t in triples if t.get("run_id")}
        if len(run_ids) != 1:
            raise ValueError(
                f"replace_tenant_triples requires exactly one run_id across "
                f"the batch; got {sorted(run_ids) or '(none)'}"
            )
        run_id = run_ids.pop()

        entity_ids = sorted({t["entity_id"] for t in triples if t.get("entity_id")})

        escape = self._copy_escape
        cols = self._COPY_COLS
        json_cols = self._JSON_COPY_COLS
        buf = io.StringIO()
        for t in triples:
            row_vals = []
            for c in cols:
                if c in json_cols:
                    v = t.get(c)
                    row_vals.append(escape(json.dumps(v) if v is not None else None))
                else:
                    row_vals.append(escape(t.get(c)))
            buf.write("\t".join(row_vals))
            buf.write("\n")
        buf.seek(0)

        ent_clause = ""
        ent_params: list = []
        if entity_ids:
            placeholders = ", ".join(["%s"] * len(entity_ids))
            ent_clause = f" AND entity_id IN ({placeholders})"
            ent_params = entity_ids

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SET LOCAL statement_timeout = {int(INGEST_STATEMENT_TIMEOUT_MS)}"
                )
                cur.execute(
                    f"DELETE FROM semantic_triples "
                    f"WHERE tenant_id = %s AND run_id = %s{ent_clause}",
                    [tenant_id, run_id] + ent_params,
                )
                scrubbed = cur.rowcount
                cur.execute(
                    f"UPDATE semantic_triples "
                    f"SET superseded_at = now(), updated_at = now() "
                    f"WHERE tenant_id = %s AND is_active = true{ent_clause}",
                    [tenant_id] + ent_params,
                )
                superseded = cur.rowcount
                logger.info(
                    "[replace_tenant_triples] Superseded %d live triples "
                    "(+%d same-run redelivery rows scrubbed) for "
                    "tenant_id=%s, entity_ids=%s",
                    superseded, scrubbed, tenant_id, entity_ids or "(all)",
                )
                cur.copy_expert(self._COPY_SQL, buf)
                conn.commit()
                return len(triples)

    def get_triples(
        self,
        tenant_id: str,
        concept: str,
        entity_id: str | None = None,
        period: str | None = None,
        active_only: bool = True,
    ) -> list[dict]:
        """Query by concept with optional filters."""
        clauses = ["tenant_id = %s", "concept = %s"]
        params: list = [tenant_id, concept]

        if entity_id is not None:
            clauses.append("entity_id = %s")
            params.append(entity_id)
        if period is not None:
            clauses.append("period = %s")
            params.append(period)
        if active_only:
            if tenant_id is not None and entity_id is not None:
                # Per-entity pointer — exact match. Reading through the
                # current-state view (below) also applies is_active, so a
                # current-run row that was superseded never surfaces even if
                # the pointer is momentarily stale.
                clauses.append(
                    "run_id = (SELECT current_run_id FROM tenant_runs "
                    "WHERE tenant_id = %s AND entity_id = %s)"
                )
                params.extend([tenant_id, entity_id])
            elif tenant_id is not None:
                # Cross-entity — all active runs for tenant
                clauses.append(
                    "run_id IN (SELECT current_run_id FROM tenant_runs WHERE tenant_id = %s)"
                )
                params.append(tenant_id)
            # else: no extra predicate — the current-state view already filters
            # is_active (Stage 2: semantic_triples_current).

        # active_only surfaces the canonical current-state view; active_only=False
        # reads the base table (includes superseded history for diff/as-of callers).
        table = "semantic_triples_current" if active_only else "semantic_triples"
        where = " AND ".join(clauses)
        sql = f"SELECT * FROM {table} WHERE {where} ORDER BY created_at"

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

    def deactivate_entity_triples(self, entity_ids: list[str], tenant_id: str = "") -> int:
        """Deactivate all active triples for given entity_ids within a tenant.

        Used when a new Farm generation replaces all prior data for those entities.
        This prevents triple compounding across runs.

        Args:
            entity_ids: Entity IDs to deactivate triples for.
            tenant_id: Required tenant scope — prevents cross-tenant data corruption.
        """
        if not entity_ids:
            return 0
        if not tenant_id:
            raise ValueError(
                "deactivate_entity_triples requires tenant_id to prevent "
                "cross-tenant data corruption."
            )
        placeholders = ", ".join(["%s"] * len(entity_ids))
        sql = (
            "UPDATE semantic_triples SET superseded_at = now(), updated_at = now() "
            f"WHERE is_active = true AND tenant_id = %s AND entity_id IN ({placeholders})"
        )
        params = [tenant_id] + entity_ids
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET LOCAL statement_timeout = {int(INGEST_STATEMENT_TIMEOUT_MS)}")
                cur.execute(sql, params)
                conn.commit()
                return cur.rowcount

    def deactivate_tenant_triples(self, tenant_id: str) -> int:
        """Deactivate all active triples for a tenant.

        Used on full replacement ingest — kills financials, HR, etc. so the
        new run is the sole active dataset.
        """
        if not tenant_id:
            raise ValueError("deactivate_tenant_triples requires tenant_id.")
        sql = (
            "UPDATE semantic_triples SET superseded_at = now(), updated_at = now() "
            "WHERE is_active = true AND tenant_id = %s"
        )
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET LOCAL statement_timeout = {int(INGEST_STATEMENT_TIMEOUT_MS)}")
                cur.execute(sql, (tenant_id,))
                conn.commit()
                return cur.rowcount

    def delete_inactive(self) -> int:
        """Hard-delete all superseded triples across all tenants.

        RETENTION tool (B19 operator scope) — this is the one sanctioned way
        history leaves the store. Default lifecycle never deletes; an
        operator runs this deliberately to reclaim space, destroying as-of
        history older than the live set.
        """
        sql = "DELETE FROM semantic_triples WHERE is_active = false"
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                conn.commit()
                return cur.rowcount

    def deactivate_run(self, run_id: str) -> int:
        """Supersede all live triples in a run (closes their knowledge window).
        Returns count affected. Rows remain queryable via as-of reads."""
        sql = (
            "UPDATE semantic_triples SET superseded_at = now(), updated_at = now() "
            "WHERE run_id = %s AND is_active = true"
        )
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET LOCAL statement_timeout = {int(INGEST_STATEMENT_TIMEOUT_MS)}")
                cur.execute(sql, (run_id,))
                conn.commit()
                return cur.rowcount

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
              SET previous_run_id = CASE
                      WHEN tenant_runs.current_run_id IS DISTINCT FROM EXCLUDED.current_run_id
                      THEN tenant_runs.current_run_id
                      ELSE tenant_runs.previous_run_id
                  END,
                  previous_snapshot_name = CASE
                      WHEN tenant_runs.current_run_id IS DISTINCT FROM EXCLUDED.current_run_id
                      THEN tenant_runs.current_snapshot_name
                      ELSE tenant_runs.previous_snapshot_name
                  END,
                  current_run_id        = EXCLUDED.current_run_id,
                  current_snapshot_name = EXCLUDED.current_snapshot_name,
                  updated_at            = now()
            RETURNING previous_run_id
        """
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (tenant_id, entity_id, new_run_id, snapshot_name))
                row = cur.fetchone()
                conn.commit()
                return str(row[0]) if row and row[0] else None

    def swap_and_deactivate(
        self, tenant_id: str, new_run_id: str,
        entity_id: str,
        snapshot_name: str | None = None,
    ) -> tuple[str | None, int]:
        """Atomically swap tenant_runs pointer AND deactivate the displaced run.

        Entity-scoped: each (tenant_id, entity_id) pair has its own pointer.
        Pushing entity B does not deactivate entity A's triples.

        Single transaction: if either statement fails, both roll back.
        Returns (previous_run_id, deactivated_count).
        """
        upsert_sql = """
            INSERT INTO tenant_runs (tenant_id, entity_id, current_run_id, previous_run_id,
                                     current_snapshot_name, previous_snapshot_name, updated_at)
            VALUES (%s, %s, %s, NULL, %s, NULL, now())
            ON CONFLICT (tenant_id, entity_id) DO UPDATE
              SET previous_run_id = CASE
                      WHEN tenant_runs.current_run_id IS DISTINCT FROM EXCLUDED.current_run_id
                      THEN tenant_runs.current_run_id
                      ELSE tenant_runs.previous_run_id
                  END,
                  previous_snapshot_name = CASE
                      WHEN tenant_runs.current_run_id IS DISTINCT FROM EXCLUDED.current_run_id
                      THEN tenant_runs.current_snapshot_name
                      ELSE tenant_runs.previous_snapshot_name
                  END,
                  current_run_id        = EXCLUDED.current_run_id,
                  current_snapshot_name = EXCLUDED.current_snapshot_name,
                  updated_at            = now()
            RETURNING previous_run_id
        """
        deactivate_sql = (
            "UPDATE semantic_triples SET superseded_at = now(), updated_at = now() "
            "WHERE run_id = %s AND is_active = true"
        )
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET LOCAL statement_timeout = {int(INGEST_STATEMENT_TIMEOUT_MS)}")
                cur.execute(upsert_sql, (tenant_id, entity_id, new_run_id, snapshot_name))
                row = cur.fetchone()
                previous_run_id = str(row[0]) if row and row[0] else None
                deactivated = 0
                if previous_run_id and previous_run_id != new_run_id:
                    cur.execute(deactivate_sql, (previous_run_id,))
                    deactivated = cur.rowcount
                conn.commit()
        return previous_run_id, deactivated

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
        """Return tenant_id for a given entity_id via tenant_runs join.

        Uses the authoritative tenant_runs.current_run_id pointer —
        not is_active — to scope to the live run.

        Raises ValueError if zero or multiple tenants match.
        """
        sql = (
            "SELECT DISTINCT t.tenant_id "
            "FROM tenant_runs t "
            "JOIN semantic_triples s ON s.run_id = t.current_run_id "
            "WHERE s.entity_id = %s "
            "LIMIT 2"
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

    def get_tenant_snapshots(
        self, tenant_id: str, limit: int = 10,
    ) -> list[dict]:
        """Get the N most-recent snapshots for a tenant, newest-first.

        Single pool borrow + one SQL pass. Replaces the prior 3-query
        sequence (tenant_runs SELECT + recursive CTE skip-scan + counts_sql
        across ALL the tenant's runs) that exceeded the 15s statement
        timeout once the tenant accumulated hundreds of inactive runs (see
        dcl_deferred_work.md#27 BLOAT REGRESSION 2026-05-27).

        Mirrors get_all_snapshots()'s pattern: recursive CTE skip-scan to
        enumerate distinct run_ids for this tenant cheaply, ROW_NUMBER over
        created_at DESC, filter rn <= limit so counts_sql touches only N
        runs instead of all. Cost becomes O(N) at the read boundary
        regardless of bloat.

        snapshot_name preserves the existing per-tenant fetchone() behavior
        intentionally (one arbitrary tenant_runs row supplies current/
        previous names). Per-entity name correctness tracked in
        dcl_deferred_work.md#36/#39.

        Args:
            limit: Top-N runs by created_at. Clamped [1, 50].
        """
        n = max(1, min(int(limit), 50))
        sql = """
            WITH RECURSIVE all_runs AS (
                (SELECT run_id, entity_id, created_at
                 FROM semantic_triples
                 WHERE tenant_id = %s
                 ORDER BY run_id LIMIT 1)
                UNION ALL
                (SELECT s.run_id, s.entity_id, s.created_at
                 FROM all_runs r, LATERAL (
                     SELECT run_id, entity_id, created_at
                     FROM semantic_triples
                     WHERE tenant_id = %s AND run_id > r.run_id
                     ORDER BY run_id LIMIT 1
                 ) s)
            ),
            ranked AS (
                SELECT run_id, entity_id, created_at,
                       ROW_NUMBER() OVER (ORDER BY created_at DESC) AS rn
                FROM all_runs
            ),
            top_n AS (
                SELECT run_id, entity_id, created_at
                FROM ranked WHERE rn <= %s
            ),
            tenant_name_pick AS (
                SELECT current_run_id, previous_run_id,
                       current_snapshot_name, previous_snapshot_name
                FROM tenant_runs
                WHERE tenant_id = %s
                ORDER BY entity_id
                LIMIT 1
            ),
            run_counts AS (
                SELECT run_id, COUNT(*) AS total_rows
                FROM semantic_triples
                WHERE run_id IN (SELECT run_id FROM top_n)
                GROUP BY run_id
            )
            SELECT
                t.run_id, t.entity_id, t.created_at,
                tn.current_run_id, tn.previous_run_id,
                tn.current_snapshot_name, tn.previous_snapshot_name,
                COALESCE(c.total_rows, 0) AS total_rows
            FROM top_n t
            LEFT JOIN tenant_name_pick tn ON true
            LEFT JOIN run_counts c ON c.run_id = t.run_id
            ORDER BY t.created_at DESC
        """
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (tenant_id, tenant_id, n, tenant_id))
                rows = cur.fetchall()

        snapshots = []
        for (run_id, entity_id, created_at,
             current_run_id, previous_run_id,
             cur_snap_name, prev_snap_name, total_rows) in rows:
            rid = str(run_id)
            current = str(current_run_id) if current_run_id else None
            previous = str(previous_run_id) if previous_run_id else None

            if rid == current and cur_snap_name:
                snap_name = cur_snap_name
            elif rid == previous and prev_snap_name:
                snap_name = prev_snap_name
            else:
                snap_name = f"{entity_id}-{rid[:4]}" if entity_id else None

            snapshots.append({
                "dcl_ingest_id": rid,
                "snapshot_name": snap_name,
                "entity_id": entity_id,
                "run_timestamp": created_at.isoformat() if created_at else None,
                "total_rows": total_rows,
                "is_current": rid == current,
            })
        return snapshots

    def get_all_snapshots(self, limit_per_tenant: int = 10) -> list[dict]:
        """Get N most-recent snapshots per tenant, sorted newest-first.

        Single pool borrow + one SQL pass. Replaces the prior O(T) per-tenant
        loop that opened a fresh connection per tenant (see
        dcl_deferred_work.md#35 + aam_deferred_work.md#37).

        snapshot_name preserves the existing get_tenant_snapshots() per-tenant
        resolution: an arbitrary tenant_runs row supplies the current/previous
        names for the whole tenant, derive {entity_id}-{rid[:4]} otherwise.
        Per-entity name correctness is a separate write-site concern tracked
        in dcl_deferred_work.md#36/#39 — do not turn this into a JOIN.

        Args:
            limit_per_tenant: Top-N runs per tenant by created_at. Clamped [1, 50].
        """
        n = max(1, min(int(limit_per_tenant), 50))
        sql = """
            WITH RECURSIVE all_runs AS (
                (SELECT tenant_id, run_id, entity_id, created_at
                 FROM semantic_triples
                 ORDER BY tenant_id, run_id LIMIT 1)
                UNION ALL
                (SELECT s.tenant_id, s.run_id, s.entity_id, s.created_at
                 FROM all_runs r, LATERAL (
                     SELECT tenant_id, run_id, entity_id, created_at
                     FROM semantic_triples
                     WHERE (tenant_id, run_id) > (r.tenant_id, r.run_id)
                     ORDER BY tenant_id, run_id LIMIT 1
                 ) s)
            ),
            ranked AS (
                SELECT tenant_id, run_id, entity_id, created_at,
                       ROW_NUMBER() OVER (PARTITION BY tenant_id
                                          ORDER BY created_at DESC) AS rn
                FROM all_runs
            ),
            top_n AS (
                SELECT tenant_id, run_id, entity_id, created_at
                FROM ranked WHERE rn <= %s
            ),
            tenant_name_pick AS (
                SELECT DISTINCT ON (tenant_id) tenant_id,
                       current_run_id, previous_run_id,
                       current_snapshot_name, previous_snapshot_name
                FROM tenant_runs
                ORDER BY tenant_id, entity_id
            ),
            run_counts AS (
                SELECT run_id, COUNT(*) AS total_rows
                FROM semantic_triples
                WHERE run_id IN (SELECT run_id FROM top_n)
                GROUP BY run_id
            )
            SELECT
                t.run_id, t.entity_id, t.created_at,
                tn.current_run_id, tn.previous_run_id,
                tn.current_snapshot_name, tn.previous_snapshot_name,
                COALESCE(c.total_rows, 0) AS total_rows
            FROM top_n t
            LEFT JOIN tenant_name_pick tn ON tn.tenant_id = t.tenant_id
            LEFT JOIN run_counts c ON c.run_id = t.run_id
            ORDER BY t.created_at DESC
        """
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (n,))
                rows = cur.fetchall()

        snapshots = []
        for (run_id, entity_id, created_at,
             current_run_id, previous_run_id,
             cur_snap_name, prev_snap_name, total_rows) in rows:
            rid = str(run_id)
            current = str(current_run_id) if current_run_id else None
            previous = str(previous_run_id) if previous_run_id else None

            if rid == current and cur_snap_name:
                snap_name = cur_snap_name
            elif rid == previous and prev_snap_name:
                snap_name = prev_snap_name
            else:
                snap_name = f"{entity_id}-{rid[:4]}" if entity_id else None

            snapshots.append({
                "dcl_ingest_id": rid,
                "snapshot_name": snap_name,
                "entity_id": entity_id,
                "run_timestamp": created_at.isoformat() if created_at else None,
                "total_rows": total_rows,
                "is_current": rid == current,
            })
        return snapshots

    def purge_old_runs(self, tenant_id: str, keep_runs: int = 2) -> int:
        """Hard-delete triples from old runs, keeping the N most recent run_ids.

        Finds runs ordered by first-triple created_at DESC, skips the first
        keep_runs, deletes the rest. Current run is always among the kept runs
        (it's the most recent by definition).
        """
        if keep_runs < 1:
            raise ValueError("keep_runs must be >= 1")
        sql_find = """
            SELECT run_id FROM semantic_triples
            WHERE tenant_id = %s
            GROUP BY run_id
            ORDER BY MIN(created_at) DESC
            OFFSET %s
        """
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql_find, (tenant_id, keep_runs))
                old_run_ids = [row[0] for row in cur.fetchall()]
                if not old_run_ids:
                    return 0
                placeholders = ", ".join(["%s"] * len(old_run_ids))
                sql_delete = (
                    f"DELETE FROM semantic_triples "
                    f"WHERE tenant_id = %s AND run_id IN ({placeholders})"
                )
                cur.execute(sql_delete, [tenant_id] + old_run_ids)
                conn.commit()
                return cur.rowcount

    def count_by_domain(self, tenant_id: str | None, run_id: str | None = None, entity_id: str | None = None) -> dict:
        """Count triples grouped by root concept domain (first segment before dot)."""
        clauses = []
        params: list = []
        if run_id is not None:
            # Explicit run_id (e.g. ingest confirmation summary) — use directly
            clauses.append("run_id = %s")
            params.append(run_id)
            if tenant_id is not None:
                clauses.append("tenant_id = %s")
                params.append(tenant_id)
        elif tenant_id is not None:
            # Tenant-scoped: all active entity runs for tenant
            clauses.append("tenant_id = %s")
            clauses.append(
                "run_id IN (SELECT current_run_id FROM tenant_runs WHERE tenant_id = %s)"
            )
            params.extend([tenant_id, tenant_id])
        else:
            # Global aggregation — no tenant context, fall back to is_active
            clauses.append("is_active = true")
        if entity_id is not None:
            clauses.append("entity_id = %s")
            params.append(entity_id)

        where = " AND ".join(clauses)
        sql = (
            f"SELECT split_part(concept, '.', 1) AS domain, COUNT(*) AS cnt "
            f"FROM semantic_triples WHERE {where} "
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
            "MIN(created_at) as created_at, "
            "bool_and(is_active) as is_active "
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
                "MIN(created_at) as created_at, "
                "bool_and(is_active) as is_active "
                "FROM semantic_triples WHERE tenant_id = %s "
                "GROUP BY run_id, tenant_id ORDER BY MIN(created_at) DESC"
            )
            params = (tenant_id,)
        else:
            sql = (
                "SELECT run_id, tenant_id, COUNT(*) as triple_count, "
                "MIN(created_at) as created_at, "
                "bool_and(is_active) as is_active "
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
        sub = "SELECT current_run_id FROM tenant_runs WHERE tenant_id = %s"
        params: list = [tenant_id, tenant_id]
        if entity_id:
            sub += " AND entity_id = %s"
            params.append(entity_id)
        sql = (
            "SELECT COUNT(*) FROM semantic_triples "
            f"WHERE tenant_id = %s AND run_id IN ({sub})"
        )
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchone()[0]

    def count_total_rows(self) -> int:
        """Count ALL rows in semantic_triples (all tenants, active + inactive)."""
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM semantic_triples")
                return cur.fetchone()[0]

    def get_source_run_ids(self, tenant_id: str) -> list[dict]:
        """Return run_ids that are current for a specific tenant, most recent first.

        Each row: {run_id: str, created_at: datetime, triple_count: int}
        Returns all active entity runs for the tenant.
        """
        sql = (
            "SELECT st.run_id, MIN(st.created_at) AS created_at, COUNT(*) AS triple_count "
            "FROM semantic_triples st "
            "WHERE st.tenant_id = %s "
            "AND st.run_id IN (SELECT current_run_id FROM tenant_runs WHERE tenant_id = %s) "
            "GROUP BY st.run_id ORDER BY MIN(st.created_at) DESC"
        )
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (tenant_id, tenant_id))
                columns = [desc[0] for desc in cur.description]
                return [dict(zip(columns, row)) for row in cur.fetchall()]

    def get_run_entities(self, run_id: str) -> list[str]:
        """Return distinct entity_ids for a specific run_id."""
        sql = (
            "SELECT DISTINCT entity_id FROM semantic_triples "
            "WHERE run_id = %s AND entity_id IS NOT NULL "
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
        # One pass over the live triple set: per-domain triple_count plus the
        # distinct set of source systems. Joining tenant_runs keeps it to
        # current runs; is_active = true is the liveness invariant — it drives
        # the query off idx_triples_active and excludes deactivated runs.
        # Returning the per-domain source SET (not a count) lets per-persona
        # data_sources be unioned in Python, so this stays one query no matter
        # how many personas the config defines — previously it was one extra
        # full COUNT(DISTINCT) scan per persona (the N+1 that made this ~19s
        # on the prod-scale triple store).
        sql = (
            "SELECT split_part(st.concept, '.', 1) AS domain, "
            "array_agg(DISTINCT st.source_system) "
            "  FILTER (WHERE st.source_system IS NOT NULL) AS sources, "
            "COUNT(*) AS triple_count "
            "FROM semantic_triples st "
            "JOIN tenant_runs tr "
            "  ON tr.tenant_id = st.tenant_id AND tr.current_run_id = st.run_id "
            "WHERE st.is_active = true "
            "GROUP BY domain"
        )
        domain_stats: dict[str, dict] = {}
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                for row in cur.fetchall():
                    domain_stats[row[0]] = {
                        "sources": set(row[1] or []),
                        "triple_count": row[2],
                    }

        result = {}
        for persona, domains in persona_domains.items():
            matched_domains = [d for d in domains if d in domain_stats]
            sources: set[str] = set()
            total_triples = 0
            for d in matched_domains:
                sources |= domain_stats[d]["sources"]
                total_triples += domain_stats[d]["triple_count"]

            result[persona] = {
                "data_sources": len(sources),
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
        sub = "SELECT current_run_id FROM tenant_runs WHERE tenant_id = %s"
        params: list = [tenant_id, tenant_id]
        if entity_id:
            sub += " AND entity_id = %s"
            params.append(entity_id)
        sql = (
            "SELECT COALESCE(fabric_plane, 'unattributed') AS fabric_plane, "
            "COALESCE(fabric_product, 'unknown') AS fabric_product, "
            "source_system, split_part(concept, '.', 1) AS domain, "
            "entity_id, COUNT(*) AS triple_count "
            "FROM semantic_triples "
            f"WHERE tenant_id = %s AND run_id IN ({sub}) "
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

    def get_distinct_field_mappings(
        self, tenant_id: str, entity_id: str | None = None,
    ) -> list[dict]:
        """Return distinct (source_system, source_table, source_field, concept,
        confidence) tuples from the active triple set, scoped to a tenant
        and optional entity. Used by Prod-mode AI validation: the validator
        re-evaluates field-to-concept mappings, so each distinct field is the
        unit of work — multiple triples sharing a field produce one row.

        DISTINCT ON keeps the lowest-confidence representative per field so
        the validator focuses on the worst case.
        """
        sub = "SELECT current_run_id FROM tenant_runs WHERE tenant_id = %s"
        params: list = [tenant_id, tenant_id]
        if entity_id:
            sub += " AND entity_id = %s"
            params.append(entity_id)
        sql = (
            "SELECT DISTINCT ON (source_system, source_table, source_field) "
            "source_system, source_table, source_field, concept, confidence_score "
            "FROM semantic_triples "
            f"WHERE tenant_id = %s AND run_id IN ({sub}) "
            "AND source_field IS NOT NULL "
            "ORDER BY source_system, source_table, source_field, confidence_score ASC"
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
        sub = "SELECT current_run_id FROM tenant_runs WHERE tenant_id = %s"
        params: list = [tenant_id, tenant_id]
        if entity_id:
            sub += " AND entity_id = %s"
            params.append(entity_id)
        sql = (
            "SELECT entity_id, concept, property, period, "
            "string_agg(DISTINCT source_system, ',' ORDER BY source_system) AS sources, "
            "COUNT(DISTINCT source_system) AS source_count "
            "FROM semantic_triples "
            f"WHERE tenant_id = %s AND run_id IN ({sub}) "
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
        """Return distinct fabric_plane:fabric_product pairs for the current run.

        Excludes rows where fabric_plane is NULL or 'none'.
        Returns sorted list of 'plane:product' strings matching the
        sourceFabricPlanes format used in GraphSnapshot.meta.
        """
        sql = (
            "SELECT DISTINCT fabric_plane, fabric_product "
            "FROM semantic_triples "
            "WHERE tenant_id = %s "
            "AND run_id IN (SELECT current_run_id FROM tenant_runs WHERE tenant_id = %s) "
            "AND fabric_plane IS NOT NULL AND fabric_plane != 'none' "
            "ORDER BY fabric_plane, fabric_product"
        )
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, [tenant_id, tenant_id])
                return sorted(
                    f"{row[0]}:{row[1]}" for row in cur.fetchall()
                    if row[0] and row[1]
                )

    def diff_runs(
        self,
        tenant_id: str,
        entity_id: str,
        base_run_id: str,
        compare_run_id: str,
        limit: int = 200,
    ) -> dict:
        """Run-over-run diff for one entity between two ingest runs.

        Unit of comparison is the assertion group (concept, property, period,
        source_system) — coordinates are NOT row-unique inside a run (multi-
        source collisions; per-record ledger rows), so each side aggregates to
        row count + an order-independent digest of the value multiset, and the
        join can never fan out. Categories: added / removed / changed
        (count or value-set differs). Single bounded SQL — both runs reach
        their rows via idx_triples_run; per-category samples capped at
        `limit`, totals always exact (dcl_deferred_work.md#56 discipline).

        Returns {counts: {added, removed, changed, unchanged}, samples:
        {added: [...], removed: [...], changed: [...]}, truncated: {...}}.
        """
        safe_limit = max(1, min(int(limit), 1000))
        sql = """
            WITH base AS (
                SELECT concept, property, COALESCE(period, '') AS period,
                       source_system,
                       COUNT(*) AS cnt,
                       md5(string_agg(value::text, '|' ORDER BY value::text)) AS digest,
                       MIN(value::text) AS single_value
                FROM semantic_triples
                WHERE tenant_id = %s AND entity_id = %s AND run_id = %s
                GROUP BY concept, property, COALESCE(period, ''), source_system
            ),
            cmp AS (
                SELECT concept, property, COALESCE(period, '') AS period,
                       source_system,
                       COUNT(*) AS cnt,
                       md5(string_agg(value::text, '|' ORDER BY value::text)) AS digest,
                       MIN(value::text) AS single_value
                FROM semantic_triples
                WHERE tenant_id = %s AND entity_id = %s AND run_id = %s
                GROUP BY concept, property, COALESCE(period, ''), source_system
            ),
            joined AS (
                SELECT
                    COALESCE(b.concept, c.concept)             AS concept,
                    COALESCE(b.property, c.property)           AS property,
                    COALESCE(b.period, c.period)               AS period,
                    COALESCE(b.source_system, c.source_system) AS source_system,
                    b.cnt AS base_count, c.cnt AS compare_count,
                    b.single_value AS base_value, c.single_value AS compare_value,
                    CASE
                        WHEN b.concept IS NULL THEN 'added'
                        WHEN c.concept IS NULL THEN 'removed'
                        WHEN b.cnt != c.cnt OR b.digest != c.digest THEN 'changed'
                        ELSE 'unchanged'
                    END AS category
                FROM base b
                FULL OUTER JOIN cmp c
                  USING (concept, property, period, source_system)
            ),
            ranked AS (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY category
                                          ORDER BY concept, property, period,
                                                   source_system) AS rn,
                       COUNT(*) OVER (PARTITION BY category) AS total
                FROM joined
            )
            SELECT category, total, concept, property,
                   NULLIF(period, '') AS period, source_system,
                   base_count, compare_count, base_value, compare_value
            FROM ranked
            WHERE (category != 'unchanged' AND rn <= %s)
               OR (category = 'unchanged' AND rn = 1)
            ORDER BY category, rn
        """
        params = [tenant_id, entity_id, base_run_id,
                  tenant_id, entity_id, compare_run_id, safe_limit]
        counts = {"added": 0, "removed": 0, "changed": 0, "unchanged": 0}
        samples: dict[str, list[dict]] = {"added": [], "removed": [], "changed": []}
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                for (category, total, concept, prop, period, source_system,
                     base_count, compare_count, base_value, compare_value) in cur.fetchall():
                    counts[category] = total
                    if category == "unchanged":
                        continue
                    entry = {
                        "concept": concept,
                        "property": prop,
                        "period": period,
                        "source_system": source_system,
                    }
                    if category == "changed":
                        entry["base_count"] = base_count
                        entry["compare_count"] = compare_count
                        # Scalar values shown only when unambiguous (one row
                        # per side); multi-row groups differ by digest/count.
                        if base_count == 1 and compare_count == 1:
                            entry["base_value"] = base_value
                            entry["compare_value"] = compare_value
                    elif category == "added":
                        entry["count"] = compare_count
                        if compare_count == 1:
                            entry["value"] = compare_value
                    else:
                        entry["count"] = base_count
                        if base_count == 1:
                            entry["value"] = base_value
                    samples[category].append(entry)
        truncated = {
            cat: counts[cat] > len(samples[cat]) for cat in samples
        }
        return {"counts": counts, "samples": samples, "truncated": truncated,
                "sample_limit": safe_limit}

    def delete_by_run(self, run_id: str) -> int:
        """Hard-delete all triples for a run (retention/test cleanup only —
        B19 scope; default lifecycle supersedes, never deletes)."""
        sql = "DELETE FROM semantic_triples WHERE run_id = %s"
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (run_id,))
                conn.commit()
                return cur.rowcount

    # =========================================================================
    # MCP wire-protocol queries (Plan B WP5 §11.4)
    # =========================================================================

    def mcp_query_triples(
        self,
        tenant_id: str,
        *,
        domain: str | None = None,
        concept: str | None = None,
        entity_id: str | None = None,
        period: str | None = None,
        limit: int = 100,
        active_only: bool = True,
        as_of: str | None = None,
        domains: list[str] | None = None,
    ) -> list[dict]:
        """Query triples for a tenant. Used by the external MCP server.

        Returns dicts with JSON-safe values (UUIDs as strings, datetimes
        as ISO strings). Filters by domain (concept root) or full concept.

        as_of (ISO timestamp): knowledge-time travel — returns the rows that
        were live at that instant (ingested on or before, not yet superseded).
        Overrides active_only; the as-of predicate IS the liveness filter at
        time T.

        domains (Gate 2B persona scoping): restrict results to concepts whose
        root is in this list — one OR-group of (concept = d OR concept LIKE
        'd.%') per domain, ANDed with every other filter. Pushed into SQL so
        scoping is never a Python post-filter.
        """
        clauses = ["tenant_id = %s"]
        params: list = [tenant_id]
        if domains is not None:
            if not domains:
                raise ValueError(
                    "mcp_query_triples: domains scoping list must be "
                    "non-empty when provided — an empty scope is a "
                    "caller bug, not an empty result."
                )
            clauses.append("(concept = ANY(%s) OR concept LIKE ANY(%s))")
            params.append(list(domains))
            params.append([f"{d}.%" for d in domains])
        if concept is not None:
            if "." in concept:
                clauses.append("concept = %s")
                params.append(concept)
            else:
                # Unqualified catalog ids (what concept_lookup returns) must
                # compose with query_triples. Stored concepts are dotted
                # paths and a catalog id appears in two shapes:
                #   leaf under a domain  — 'net_income' -> 'pnl.net_income'
                #   root with submetrics — 'revenue'    -> 'revenue.total'
                # So an unqualified id matches the exact id, its namespace
                # (id.*, same semantics as the domain filter), and any
                # domain-qualified instance (*.id). Additive — more rows,
                # never fewer; dotted paths keep exact-match semantics.
                clauses.append("(concept = %s OR concept LIKE %s OR concept LIKE %s)")
                params.extend([concept, f"{concept}.%", f"%.{concept}"])
        if domain is not None:
            clauses.append("(concept = %s OR concept LIKE %s)")
            params.extend([domain, f"{domain}.%"])
        if entity_id is not None:
            clauses.append("entity_id = %s")
            params.append(entity_id)
        if period is not None:
            clauses.append("period = %s")
            params.append(period)
        if as_of is not None:
            clauses.append(
                "ingested_at <= %s AND (superseded_at IS NULL OR superseded_at > %s)"
            )
            params.extend([as_of, as_of])
        elif active_only:
            clauses.append("is_active = true")

        safe_limit = max(1, min(int(limit), 1000))
        where = " AND ".join(clauses)
        sql = (
            "SELECT id, tenant_id, entity_id, concept, property, value, period, "
            "       currency, unit, source_system, source_field, "
            "       fabric_plane, fabric_product, pipe_id, "
            "       run_id, confidence_score, confidence_tier, is_active, "
            "       created_at, ingested_at, superseded_at, valid_from, valid_to "
            f"FROM semantic_triples WHERE {where} "
            # id tiebreaker: batch inserts share created_at, and ties broke
            # arbitrarily per call — two identical queries could return
            # different row orders (consumers compare reads; B14).
            f"ORDER BY created_at DESC, id DESC LIMIT {safe_limit}"
        )
        rows: list[dict] = []
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                cols = [d[0] for d in cur.description]
                for r in cur.fetchall():
                    d = dict(zip(cols, r))
                    for k in ("id", "tenant_id", "pipe_id", "run_id"):
                        if d.get(k) is not None:
                            d[k] = str(d[k])
                    for k in ("created_at", "ingested_at", "superseded_at",
                              "valid_from", "valid_to"):
                        if d.get(k) is not None:
                            d[k] = d[k].isoformat()
                    if d.get("confidence_score") is not None:
                        d["confidence_score"] = float(d["confidence_score"])
                    rows.append(d)
        return rows

    def mcp_query_triples_expanded(
        self,
        tenant_id: str,
        *,
        exacts: list[str],
        prefixes: list[str],
        entity_id: str | None = None,
        period: str | None = None,
        limit: int = 100,
        active_only: bool = True,
        as_of: str | None = None,
        domains: list[str] | None = None,
    ) -> list[dict]:
        """Query triples whose concept matches a hierarchy expansion — exact
        names OR dotted subtrees (concept LIKE '<prefix>.%'). One SQL pass, so
        a parent-with-descendants read (Gate 1B concept hierarchy) stays a
        single deterministic query. Same row shape, ordering, and temporal
        semantics as mcp_query_triples — including the Gate 2B persona
        `domains` group, ANDed in SQL: hierarchy expansion can cross domain
        roots (tenant-defined links), so the persona scope must bound the
        expanded read too.
        """
        if not exacts and not prefixes:
            return []
        clauses = ["tenant_id = %s"]
        params: list = [tenant_id]
        if domains is not None:
            if not domains:
                raise ValueError(
                    "mcp_query_triples_expanded: domains scoping list must "
                    "be non-empty when provided — an empty scope is a "
                    "caller bug, not an empty result."
                )
            clauses.append("(concept = ANY(%s) OR concept LIKE ANY(%s))")
            params.append(list(domains))
            params.append([f"{d}.%" for d in domains])
        concept_terms: list[str] = []
        if exacts:
            concept_terms.append("concept = ANY(%s)")
            params.append(list(exacts))
        if prefixes:
            concept_terms.append("concept LIKE ANY(%s)")
            params.append([f"{p}.%" for p in prefixes])
        clauses.append("(" + " OR ".join(concept_terms) + ")")
        if entity_id is not None:
            clauses.append("entity_id = %s")
            params.append(entity_id)
        if period is not None:
            clauses.append("period = %s")
            params.append(period)
        if as_of is not None:
            clauses.append(
                "ingested_at <= %s AND (superseded_at IS NULL OR superseded_at > %s)"
            )
            params.extend([as_of, as_of])
        elif active_only:
            clauses.append("is_active = true")

        safe_limit = max(1, min(int(limit), 1000))
        sql = (
            "SELECT id, tenant_id, entity_id, concept, property, value, period, "
            "       currency, unit, source_system, source_field, "
            "       fabric_plane, fabric_product, pipe_id, "
            "       run_id, confidence_score, confidence_tier, is_active, "
            "       created_at, ingested_at, superseded_at, valid_from, valid_to "
            f"FROM semantic_triples WHERE {' AND '.join(clauses)} "
            f"ORDER BY created_at DESC, id DESC LIMIT {safe_limit}"
        )
        rows: list[dict] = []
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                cols = [d[0] for d in cur.description]
                for r in cur.fetchall():
                    d = dict(zip(cols, r))
                    for k in ("id", "tenant_id", "pipe_id", "run_id"):
                        if d.get(k) is not None:
                            d[k] = str(d[k])
                    for k in ("created_at", "ingested_at", "superseded_at",
                              "valid_from", "valid_to"):
                        if d.get(k) is not None:
                            d[k] = d[k].isoformat()
                    if d.get("confidence_score") is not None:
                        d["confidence_score"] = float(d["confidence_score"])
                    rows.append(d)
        return rows

    def mcp_list_domains(
        self, tenant_id: str, entity_id: str | None = None
    ) -> list[dict]:
        """Distinct concept-root domains visible to the tenant, with counts.

        entity_id scopes the inventory to a single run's entity — the Fabric Lab
        passes the selected run's entity so the domain inventory reflects ONE run,
        the way NLQ scopes to the selected snapshot. Omitted = the whole tenant.
        """
        clauses = ["tenant_id = %s"]
        params: list = [tenant_id]
        if entity_id is not None:
            clauses.append("entity_id = %s")
            params.append(entity_id)
        sql = (
            "SELECT split_part(concept, '.', 1) AS domain, COUNT(*) AS cnt "
            "FROM semantic_triples_current "
            f"WHERE {' AND '.join(clauses)} "
            "GROUP BY domain ORDER BY cnt DESC"
        )
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params))
                return [
                    {"domain": row[0], "triple_count": int(row[1])}
                    for row in cur.fetchall()
                ]

    def mcp_list_runs(self, tenant_id: str) -> list[dict]:
        """List the current runs (snapshots) for a tenant — one row per (entity,
        active run), newest first. The NLQ-snapshot equivalent for MCP consumers:
        a consumer builds a follow-latest run selector from this. Each row carries
        the run id (renamed run_id → dcl_ingest_id at the tool boundary, I1),
        entity_id, triple_count, and the latest created_at.
        """
        sql = (
            "SELECT entity_id, run_id, COUNT(*) AS cnt, MAX(created_at) AS latest "
            "FROM semantic_triples_current "
            "WHERE tenant_id = %s "
            "GROUP BY entity_id, run_id "
            "ORDER BY MAX(created_at) DESC"
        )
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (tenant_id,))
                out: list[dict] = []
                for row in cur.fetchall():
                    latest = row[3]
                    out.append({
                        "entity_id": row[0],
                        "run_id": str(row[1]) if row[1] is not None else None,
                        "triple_count": int(row[2]),
                        "created_at": latest.isoformat()
                        if hasattr(latest, "isoformat") else str(latest),
                    })
                return out

    def mcp_provenance_lookup(
        self,
        tenant_id: str,
        *,
        triple_id: str | None = None,
        concept: str | None = None,
        property: str | None = None,
        entity_id: str | None = None,
        period: str | None = None,
    ) -> dict | None:
        """Return one triple's provenance fields. Tenant-scoped lookup
        either by triple_id (exact) or by coordinates. Triple identity is
        (entity, concept, property, period) — property narrows the composite
        path so 'status' can't return 'amount''s provenance."""
        clauses = ["tenant_id = %s"]
        params: list = [tenant_id]
        if triple_id is not None:
            clauses.append("id = %s")
            params.append(triple_id)
        else:
            clauses.append("concept = %s")
            params.append(concept)
            if property is not None:
                clauses.append("property = %s")
                params.append(property)
            if entity_id is not None:
                clauses.append("entity_id = %s")
                params.append(entity_id)
            if period is not None:
                clauses.append("period = %s")
                params.append(period)
            clauses.append("is_active = true")

        where = " AND ".join(clauses)
        sql = (
            "SELECT id, concept, entity_id, period, value, "
            "       source_system, source_field, source_table, "
            "       pipe_id, run_id, confidence_score, confidence_tier "
            f"FROM semantic_triples WHERE {where} "
            # id tiebreaker: batch rows share created_at (B14 determinism).
            "ORDER BY created_at DESC, id DESC LIMIT 1"
        )
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                row = cur.fetchone()
                if row is None:
                    return None
                cols = [d[0] for d in cur.description]
                r = dict(zip(cols, row))
                return {
                    "triple_id": str(r["id"]),
                    "concept": r["concept"],
                    "entity_id": r["entity_id"],
                    "period": r.get("period"),
                    "source_system": r.get("source_system"),
                    "source_field": r.get("source_field"),
                    "source_table": r.get("source_table"),
                    "pipe_id": str(r["pipe_id"]) if r.get("pipe_id") else None,
                    # Per I1: run_id is banned in response payloads. We
                    # expose it under a namespaced key (dcl_ingest_id).
                    "dcl_ingest_id": str(r["run_id"]) if r.get("run_id") else None,
                    "confidence_score": float(r["confidence_score"]) if r.get("confidence_score") is not None else None,
                    "confidence_tier": r.get("confidence_tier"),
                }
