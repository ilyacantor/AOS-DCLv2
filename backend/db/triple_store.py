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
                # Exclude COFA-process triples (cofa_conflict, cofa_mapping,
                # cofa_unified) from the DELETE.  These are owned by the COFA
                # mapping writer and have their own deactivation lifecycle via
                # deactivate_cofa_triples().  Without this exclusion, every
                # Farm re-ingest nukes conflict triples that Maestra wrote.
                cofa_exclusion = (
                    " AND split_part(concept, '.', 1) NOT IN "
                    "('cofa', 'cofa_conflict', 'cofa_mapping', 'cofa_unified')"
                )
                if entity_ids:
                    placeholders = ", ".join(["%s"] * len(entity_ids))
                    cur.execute(
                        f"DELETE FROM semantic_triples "
                        f"WHERE tenant_id = %s AND entity_id IN ({placeholders})"
                        f"{cofa_exclusion}",
                        [tenant_id] + entity_ids,
                    )
                else:
                    cur.execute(
                        "DELETE FROM semantic_triples WHERE tenant_id = %s"
                        f"{cofa_exclusion}",
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
            if tenant_id is not None:
                # Use current_run_id pointer — tenant_id already in clauses above
                clauses.append(
                    "run_id = (SELECT current_run_id FROM tenant_runs WHERE tenant_id = %s)"
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

    def deactivate_cofa_triples(self, entity_ids: list[str]) -> int:
        """Deactivate all active COFA triples for the given entity_ids.

        This ensures a new COFA unification run replaces — not accumulates on —
        prior runs' data.  Matches on concept prefix (cofa., cofa_mapping.,
        cofa_conflict., cofa_unified.) rather than source_field, because
        triples may originate from different writers with varying source_field
        values (including NULL).
        """
        all_ids = list(set(entity_ids + ["combined"]))
        placeholders = ", ".join(["%s"] * len(all_ids))
        sql = (
            "UPDATE semantic_triples SET is_active = false, updated_at = now() "
            "WHERE is_active = true "
            "  AND (   split_part(concept, '.', 1) = 'cofa' "
            "       OR split_part(concept, '.', 1) = 'cofa_mapping' "
            "       OR split_part(concept, '.', 1) = 'cofa_conflict' "
            "       OR split_part(concept, '.', 1) = 'cofa_unified') "
            f"  AND entity_id IN ({placeholders})"
        )
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, all_ids)
                conn.commit()
                return cur.rowcount

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
        # Exclude COFA-process triples — their lifecycle is managed by
        # deactivate_cofa_triples(), not by Farm ingest cycles.
        sql = (
            "UPDATE semantic_triples SET is_active = false, updated_at = now() "
            f"WHERE is_active = true AND tenant_id = %s AND entity_id IN ({placeholders}) "
            "AND split_part(concept, '.', 1) NOT IN "
            "('cofa', 'cofa_conflict', 'cofa_mapping', 'cofa_unified')"
        )
        params = [tenant_id] + entity_ids
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SET LOCAL statement_timeout = {int(INGEST_STATEMENT_TIMEOUT_MS)}")
                cur.execute(sql, params)
                conn.commit()
                return cur.rowcount

    def deactivate_tenant_triples(self, tenant_id: str) -> int:
        """Deactivate all active non-COFA triples for a tenant.

        Used on full replacement ingest — kills financials, HR, etc. so the
        new run is the sole active dataset.  Excludes COFA-process triples
        whose lifecycle is managed by deactivate_cofa_triples().
        """
        if not tenant_id:
            raise ValueError("deactivate_tenant_triples requires tenant_id.")
        sql = (
            "UPDATE semantic_triples SET is_active = false, updated_at = now() "
            "WHERE is_active = true AND tenant_id = %s "
            "AND split_part(concept, '.', 1) NOT IN "
            "('cofa', 'cofa_conflict', 'cofa_mapping', 'cofa_unified')"
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

    def upsert_tenant_run(self, tenant_id: str, new_run_id: str) -> None:
        """Atomically set current_run_id for a tenant. Saves previous run for rollback.

        This is the O(1) replacement for deactivate_tenant_triples on the ingest
        hot path. Single-row UPSERT — no table scan, no lock contention.
        """
        sql = """
            INSERT INTO tenant_runs (tenant_id, current_run_id, previous_run_id, updated_at)
            VALUES (%s, %s, NULL, now())
            ON CONFLICT (tenant_id) DO UPDATE
              SET previous_run_id = tenant_runs.current_run_id,
                  current_run_id  = EXCLUDED.current_run_id,
                  updated_at      = now()
        """
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (tenant_id, new_run_id))
                conn.commit()

    def get_current_run_id(self, tenant_id: str) -> str:
        """Return current_run_id for tenant.

        Raises ValueError if no entry exists — no silent empty returns.
        Callers that need a best-effort fallback should catch ValueError.
        """
        sql = "SELECT current_run_id FROM tenant_runs WHERE tenant_id = %s"
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (tenant_id,))
                row = cur.fetchone()
        if row is None:
            raise ValueError(
                f"No current_run_id registered for tenant {tenant_id}. "
                f"Run the ingest pipeline first to populate tenant_runs."
            )
        return str(row[0])

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
            # Tenant-scoped: use current_run_id pointer (avoids counting stale runs)
            clauses.append("tenant_id = %s")
            clauses.append(
                "run_id = (SELECT current_run_id FROM tenant_runs WHERE tenant_id = %s)"
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

    def count_active(self, tenant_id: str | None = None) -> int:
        """Count triples in the current run. With tenant_id uses current_run_id pointer."""
        if tenant_id:
            sql = (
                "SELECT COUNT(*) FROM semantic_triples "
                "WHERE tenant_id = %s "
                "AND run_id = (SELECT current_run_id FROM tenant_runs WHERE tenant_id = %s)"
            )
            params: tuple = (tenant_id, tenant_id)
        else:
            sql = "SELECT COUNT(*) FROM semantic_triples WHERE is_active = true"
            params = ()

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

    def get_source_run_ids(self) -> list[dict]:
        """Return run_ids that are current for at least one tenant, most recent first.

        Each row: {run_id: str, created_at: datetime, triple_count: int}
        Uses tenant_runs join to return only live runs, not all historical runs.
        """
        sql = (
            "SELECT st.run_id, MIN(st.created_at) AS created_at, COUNT(*) AS triple_count "
            "FROM semantic_triples st "
            "JOIN tenant_runs tr "
            "  ON tr.tenant_id = st.tenant_id AND tr.current_run_id = st.run_id "
            "GROUP BY st.run_id ORDER BY MIN(st.created_at) DESC"
        )
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
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

    def get_sankey_aggregation(self, tenant_id: str | None = None) -> list[dict]:
        """Aggregate triples for Sankey visualization.

        Returns rows of {source_system, domain, entity_id, triple_count}
        grouped by source_system × root concept domain × entity_id.
        """
        if tenant_id:
            sql = (
                "SELECT source_system, split_part(concept, '.', 1) AS domain, "
                "entity_id, COUNT(*) AS triple_count "
                "FROM semantic_triples "
                "WHERE tenant_id = %s "
                "AND run_id = (SELECT current_run_id FROM tenant_runs WHERE tenant_id = %s) "
                "GROUP BY source_system, split_part(concept, '.', 1), entity_id "
                "ORDER BY triple_count DESC"
            )
            params: list = [tenant_id, tenant_id]
        else:
            sql = (
                "SELECT st.source_system, split_part(st.concept, '.', 1) AS domain, "
                "st.entity_id, COUNT(*) AS triple_count "
                "FROM semantic_triples st "
                "JOIN tenant_runs tr "
                "  ON tr.tenant_id = st.tenant_id AND tr.current_run_id = st.run_id "
                "GROUP BY st.source_system, split_part(st.concept, '.', 1), st.entity_id "
                "ORDER BY triple_count DESC"
            )
            params = []

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                columns = [desc[0] for desc in cur.description]
                return [dict(zip(columns, row)) for row in cur.fetchall()]

    def delete_by_run(self, run_id: str) -> int:
        """Hard-delete all triples for a run (test cleanup only)."""
        sql = "DELETE FROM semantic_triples WHERE run_id = %s"
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (run_id,))
                conn.commit()
                return cur.rowcount
