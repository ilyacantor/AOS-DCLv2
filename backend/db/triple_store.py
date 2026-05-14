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

    def insert_triples(self, triples: list[dict]) -> int:
        """Batch insert triples using COPY for maximum throughput."""
        if not triples:
            return 0

        escape = self._copy_escape
        cols = self._COPY_COLS
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

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET LOCAL statement_timeout = {int(INGEST_STATEMENT_TIMEOUT_MS)}")
                cur.copy_expert(self._COPY_SQL, buf)
                conn.commit()
                return len(triples)

    def replace_tenant_triples(self, tenant_id: str, triples: list[dict]) -> int:
        """Atomically DELETE old triples, then COPY-insert new batch.

        Scopes the DELETE to entity_ids present in the incoming triples so
        that replacing one entity's data does not nuke another entity's
        triples within the same tenant.  Both operations share one
        transaction — if COPY fails the DELETE rolls back.
        """
        if not tenant_id:
            raise ValueError("replace_tenant_triples requires tenant_id")
        if not triples:
            return 0

        entity_ids = sorted({t["entity_id"] for t in triples if t.get("entity_id")})

        escape = self._copy_escape
        cols = self._COPY_COLS
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

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SET LOCAL statement_timeout = {int(INGEST_STATEMENT_TIMEOUT_MS)}"
                )
                if entity_ids:
                    placeholders = ", ".join(["%s"] * len(entity_ids))
                    cur.execute(
                        f"DELETE FROM semantic_triples "
                        f"WHERE tenant_id = %s AND entity_id IN ({placeholders})",
                        [tenant_id] + entity_ids,
                    )
                else:
                    cur.execute(
                        "DELETE FROM semantic_triples WHERE tenant_id = %s",
                        (tenant_id,),
                    )
                deleted = cur.rowcount
                logger.info(
                    "[replace_tenant_triples] Deleted %d old triples for "
                    "tenant_id=%s, entity_ids=%s",
                    deleted, tenant_id, entity_ids or "(all)",
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
                # Per-entity pointer — exact match
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
            else:
                clauses.append("is_active = true")

        where = " AND ".join(clauses)
        sql = f"SELECT * FROM semantic_triples WHERE {where} ORDER BY created_at"

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
            "UPDATE semantic_triples SET is_active = false, updated_at = now() "
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
            "UPDATE semantic_triples SET is_active = false, updated_at = now() "
            "WHERE is_active = true AND tenant_id = %s"
        )
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET LOCAL statement_timeout = {int(INGEST_STATEMENT_TIMEOUT_MS)}")
                cur.execute(sql, (tenant_id,))
                conn.commit()
                return cur.rowcount

    def delete_inactive(self) -> int:
        """Hard-delete all inactive triples across all tenants.

        Maintenance operation to purge deactivated runs and reclaim space.
        """
        sql = "DELETE FROM semantic_triples WHERE is_active = false"
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                conn.commit()
                return cur.rowcount

    def deactivate_run(self, run_id: str) -> int:
        """Set is_active=false for all triples in a run. Returns count affected."""
        sql = (
            "UPDATE semantic_triples SET is_active = false, updated_at = now() "
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
              SET previous_run_id          = tenant_runs.current_run_id,
                  current_run_id           = EXCLUDED.current_run_id,
                  previous_snapshot_name   = tenant_runs.current_snapshot_name,
                  current_snapshot_name    = EXCLUDED.current_snapshot_name,
                  updated_at              = now()
            RETURNING previous_run_id
        """
        deactivate_sql = (
            "UPDATE semantic_triples SET is_active = false, updated_at = now() "
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

    def get_tenant_snapshots(self, tenant_id: str) -> list[dict]:
        """Get all available snapshots for a tenant from semantic_triples.

        Uses a recursive CTE skip-scan on idx_triples_tenant_run to find
        distinct run_ids in ~60ms, then batch-counts in ~600ms.  Replaces
        the old tenant_runs-only approach that capped at 2 snapshots.

        snapshot_name uses the stored name from tenant_runs for current/
        previous runs, and derives {entity_id}-{run_id_prefix} for older
        runs (I5 convention).
        """
        tr_sql = """
            SELECT current_run_id, previous_run_id,
                   current_snapshot_name, previous_snapshot_name
            FROM tenant_runs WHERE tenant_id = %s
        """
        # Recursive skip-scan: jumps between distinct run_ids via the
        # (tenant_id, run_id) index — touches ~1 row per run, not 20k.
        skip_scan_sql = """
            WITH RECURSIVE runs AS (
                (SELECT run_id, entity_id, created_at
                 FROM semantic_triples
                 WHERE tenant_id = %s
                 ORDER BY run_id LIMIT 1)
                UNION ALL
                (SELECT s.run_id, s.entity_id, s.created_at
                 FROM runs r, LATERAL (
                     SELECT run_id, entity_id, created_at
                     FROM semantic_triples
                     WHERE tenant_id = %s AND run_id > r.run_id
                     ORDER BY run_id LIMIT 1
                 ) s)
            )
            SELECT run_id, entity_id, created_at
            FROM runs ORDER BY created_at DESC
        """
        counts_sql = """
            SELECT run_id, COUNT(*) FROM semantic_triples
            WHERE run_id = ANY(%s::uuid[])
            GROUP BY run_id
        """
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(tr_sql, (tenant_id,))
                tr_row = cur.fetchone()

                cur.execute(skip_scan_sql, (tenant_id, tenant_id))
                run_rows = cur.fetchall()

                if not run_rows:
                    return []

                run_ids = [r[0] for r in run_rows]
                cur.execute(counts_sql, (run_ids,))
                counts = {str(r[0]): r[1] for r in cur.fetchall()}

        current_run_id = str(tr_row[0]) if tr_row and tr_row[0] else None
        previous_run_id = str(tr_row[1]) if tr_row and tr_row[1] else None
        cur_snap_name = tr_row[2] if tr_row else None
        prev_snap_name = tr_row[3] if tr_row else None

        snapshots = []
        for run_id_val, entity_id, created_at in run_rows:
            rid = str(run_id_val)
            if rid == current_run_id and cur_snap_name:
                snap_name = cur_snap_name
            elif rid == previous_run_id and prev_snap_name:
                snap_name = prev_snap_name
            else:
                snap_name = f"{entity_id}-{rid[:4]}" if entity_id else None

            snapshots.append({
                "dcl_ingest_id": rid,
                "snapshot_name": snap_name,
                "run_timestamp": created_at.isoformat() if created_at else None,
                "total_rows": counts.get(rid, 0),
                "is_current": rid == current_run_id,
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
        # Single query: get per-domain stats from current runs only
        # Join with tenant_runs so we see only live data, not stale historical runs.
        sql = (
            "SELECT split_part(st.concept, '.', 1) AS domain, "
            "COUNT(DISTINCT st.source_system) AS source_count, "
            "COUNT(*) AS triple_count "
            "FROM semantic_triples st "
            "JOIN tenant_runs tr "
            "  ON tr.tenant_id = st.tenant_id AND tr.current_run_id = st.run_id "
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

            # Need distinct source_system across all matched domains (current runs only)
            if matched_domains:
                placeholders = ", ".join(["%s"] * len(matched_domains))
                src_sql = (
                    f"SELECT COUNT(DISTINCT st.source_system) "
                    f"FROM semantic_triples st "
                    f"JOIN tenant_runs tr "
                    f"  ON tr.tenant_id = st.tenant_id AND tr.current_run_id = st.run_id "
                    f"WHERE split_part(st.concept, '.', 1) IN ({placeholders})"
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

    def delete_by_run(self, run_id: str) -> int:
        """Hard-delete all triples for a run (test cleanup only)."""
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
    ) -> list[dict]:
        """Query triples for a tenant. Used by the external MCP server.

        Returns dicts with JSON-safe values (UUIDs as strings, datetimes
        as ISO strings). Filters by domain (concept root) or full concept.
        """
        clauses = ["tenant_id = %s"]
        params: list = [tenant_id]
        if concept is not None:
            clauses.append("concept = %s")
            params.append(concept)
        if domain is not None:
            clauses.append("(concept = %s OR concept LIKE %s)")
            params.extend([domain, f"{domain}.%"])
        if entity_id is not None:
            clauses.append("entity_id = %s")
            params.append(entity_id)
        if period is not None:
            clauses.append("period = %s")
            params.append(period)
        if active_only:
            clauses.append("is_active = true")

        safe_limit = max(1, min(int(limit), 1000))
        where = " AND ".join(clauses)
        sql = (
            "SELECT id, tenant_id, entity_id, concept, property, value, period, "
            "       currency, unit, source_system, source_field, pipe_id, "
            "       run_id, confidence_score, confidence_tier, is_active, "
            "       created_at "
            f"FROM semantic_triples WHERE {where} "
            f"ORDER BY created_at DESC LIMIT {safe_limit}"
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
                    if d.get("created_at") is not None:
                        d["created_at"] = d["created_at"].isoformat()
                    if d.get("confidence_score") is not None:
                        d["confidence_score"] = float(d["confidence_score"])
                    rows.append(d)
        return rows

    def mcp_list_domains(self, tenant_id: str) -> list[dict]:
        """Distinct concept-root domains visible to the tenant, with counts."""
        sql = (
            "SELECT split_part(concept, '.', 1) AS domain, COUNT(*) AS cnt "
            "FROM semantic_triples "
            "WHERE tenant_id = %s AND is_active = true "
            "GROUP BY domain ORDER BY cnt DESC"
        )
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (tenant_id,))
                return [
                    {"domain": row[0], "triple_count": int(row[1])}
                    for row in cur.fetchall()
                ]

    def mcp_provenance_lookup(
        self,
        tenant_id: str,
        *,
        triple_id: str | None = None,
        concept: str | None = None,
        entity_id: str | None = None,
        period: str | None = None,
    ) -> dict | None:
        """Return one triple's provenance fields. Tenant-scoped lookup
        either by triple_id or by (concept, entity_id, period)."""
        clauses = ["tenant_id = %s"]
        params: list = [tenant_id]
        if triple_id is not None:
            clauses.append("id = %s")
            params.append(triple_id)
        else:
            clauses.append("concept = %s")
            params.append(concept)
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
            "ORDER BY created_at DESC LIMIT 1"
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
