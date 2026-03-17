"""
TripleStore — data access for the semantic_triples table.

Sync psycopg2, parameterized queries, no business logic.
"""

import json
from psycopg2.extras import execute_values
from backend.core.db import get_connection
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)


class TripleStore:

    def insert_triples(self, triples: list[dict]) -> int:
        """Batch insert triples. Returns count inserted."""
        if not triples:
            return 0

        cols = [
            "tenant_id", "entity_id", "concept", "property", "value",
            "period", "currency", "unit",
            "source_system", "source_table", "source_field",
            "pipe_id", "run_id",
            "confidence_score", "confidence_tier",
            "canonical_id", "resolution_method", "resolution_confidence",
        ]
        placeholders = ", ".join(["%s"] * len(cols))
        col_names = ", ".join(cols)
        sql = f"INSERT INTO semantic_triples ({col_names}) VALUES ({placeholders})"

        with get_connection() as conn:
            if conn is None:
                raise RuntimeError(
                    "TripleStore.insert_triples failed: database connection unavailable. "
                    "Check DATABASE_URL and Supabase connectivity."
                )
            with conn.cursor() as cur:
                rows = []
                for t in triples:
                    val = json.dumps(t["value"])
                    rows.append(tuple(
                        val if c == "value" else t.get(c)
                        for c in cols
                    ))
                # execute_values sends a single multi-row INSERT instead of
                # N individual INSERTs — typically 10-50x faster over network.
                template = "(" + ", ".join(["%s"] * len(cols)) + ")"
                execute_values(cur, sql.replace(
                    f"VALUES ({placeholders})", "VALUES %s"
                ), rows, template=template, page_size=1000)
                conn.commit()
                return len(rows)

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
            clauses.append("is_active = true")

        where = " AND ".join(clauses)
        sql = f"SELECT * FROM semantic_triples WHERE {where} ORDER BY created_at"

        with get_connection() as conn:
            if conn is None:
                raise RuntimeError(
                    "TripleStore.get_triples failed: database connection unavailable."
                )
            with conn.cursor() as cur:
                cur.execute(sql, params)
                columns = [desc[0] for desc in cur.description]
                return [dict(zip(columns, row)) for row in cur.fetchall()]

    def get_triples_by_run(self, run_id: str) -> list[dict]:
        """All triples from a run."""
        sql = "SELECT * FROM semantic_triples WHERE run_id = %s ORDER BY created_at"
        with get_connection() as conn:
            if conn is None:
                raise RuntimeError(
                    "TripleStore.get_triples_by_run failed: database connection unavailable."
                )
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
            if conn is None:
                raise RuntimeError(
                    "TripleStore.deactivate_cofa_triples failed: database connection unavailable."
                )
            with conn.cursor() as cur:
                cur.execute(sql, all_ids)
                conn.commit()
                return cur.rowcount

    def deactivate_run(self, run_id: str) -> int:
        """Set is_active=false for all triples in a run. Returns count affected."""
        sql = (
            "UPDATE semantic_triples SET is_active = false, updated_at = now() "
            "WHERE run_id = %s AND is_active = true"
        )
        with get_connection() as conn:
            if conn is None:
                raise RuntimeError(
                    "TripleStore.deactivate_run failed: database connection unavailable."
                )
            with conn.cursor() as cur:
                cur.execute(sql, (run_id,))
                conn.commit()
                return cur.rowcount

    def count_by_domain(self, tenant_id: str | None, run_id: str | None = None) -> dict:
        """Count triples grouped by root concept domain (first segment before dot)."""
        clauses = ["is_active = true"]
        params: list = []
        if tenant_id is not None:
            clauses.append("tenant_id = %s")
            params.append(tenant_id)
        if run_id is not None:
            clauses.append("run_id = %s")
            params.append(run_id)

        where = " AND ".join(clauses)
        sql = (
            f"SELECT split_part(concept, '.', 1) AS domain, COUNT(*) AS cnt "
            f"FROM semantic_triples WHERE {where} "
            f"GROUP BY domain ORDER BY domain"
        )

        with get_connection() as conn:
            if conn is None:
                raise RuntimeError(
                    "TripleStore.count_by_domain failed: database connection unavailable."
                )
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return {row[0]: row[1] for row in cur.fetchall()}

    def count_by_run(self, run_id: str) -> int:
        """Count triples for a given run_id (active only)."""
        sql = "SELECT COUNT(*) FROM semantic_triples WHERE run_id = %s AND is_active = true"
        with get_connection() as conn:
            if conn is None:
                raise RuntimeError(
                    "TripleStore.count_by_run failed: database connection unavailable."
                )
            with conn.cursor() as cur:
                cur.execute(sql, (run_id,))
                return cur.fetchone()[0]

    def run_exists(self, run_id: str) -> bool:
        """Check if any triples exist for a run_id."""
        sql = "SELECT EXISTS(SELECT 1 FROM semantic_triples WHERE run_id = %s)"
        with get_connection() as conn:
            if conn is None:
                raise RuntimeError(
                    "TripleStore.run_exists failed: database connection unavailable."
                )
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
            if conn is None:
                raise RuntimeError(
                    "TripleStore.get_run_info failed: database connection unavailable."
                )
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
            if conn is None:
                raise RuntimeError(
                    "TripleStore.list_runs failed: database connection unavailable."
                )
            with conn.cursor() as cur:
                cur.execute(sql, params)
                columns = [desc[0] for desc in cur.description]
                return [dict(zip(columns, row)) for row in cur.fetchall()]

    def count_active(self, tenant_id: str | None = None) -> int:
        """Count all active triples, optionally filtered by tenant."""
        if tenant_id:
            sql = "SELECT COUNT(*) FROM semantic_triples WHERE is_active = true AND tenant_id = %s"
            params: tuple = (tenant_id,)
        else:
            sql = "SELECT COUNT(*) FROM semantic_triples WHERE is_active = true"
            params = ()

        with get_connection() as conn:
            if conn is None:
                raise RuntimeError(
                    "TripleStore.count_active failed: database connection unavailable."
                )
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchone()[0]

    def get_source_run_ids(self) -> list[dict]:
        """Return distinct run_ids from active triples, most recent first.

        Each row: {run_id: str, created_at: datetime, triple_count: int}
        """
        sql = (
            "SELECT run_id, MIN(created_at) AS created_at, COUNT(*) AS triple_count "
            "FROM semantic_triples WHERE is_active = true "
            "GROUP BY run_id ORDER BY MIN(created_at) DESC"
        )
        with get_connection() as conn:
            if conn is None:
                raise RuntimeError(
                    "TripleStore.get_source_run_ids failed: database connection unavailable. "
                    "Check DATABASE_URL and Supabase connectivity."
                )
            with conn.cursor() as cur:
                cur.execute(sql)
                columns = [desc[0] for desc in cur.description]
                return [dict(zip(columns, row)) for row in cur.fetchall()]

    def get_persona_domain_stats(self, persona_domains: dict[str, list[str]]) -> dict:
        """Compute per-persona stats from active triples by domain mapping.

        Args:
            persona_domains: mapping of persona key to list of triple domains.
                e.g. {"CFO": ["revenue", "cogs", ...], "CRO": [...]}

        Returns:
            dict keyed by persona, each with data_sources, domains, triple_count, domain_list.
        """
        # Single query: get per-domain stats (source count + triple count)
        sql = (
            "SELECT split_part(concept, '.', 1) AS domain, "
            "COUNT(DISTINCT source_system) AS source_count, "
            "COUNT(*) AS triple_count "
            "FROM semantic_triples WHERE is_active = true "
            "GROUP BY domain"
        )
        with get_connection() as conn:
            if conn is None:
                raise RuntimeError(
                    "TripleStore.get_persona_domain_stats failed: database connection unavailable. "
                    "Check DATABASE_URL and Supabase connectivity."
                )
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

            # Need distinct source_system across all matched domains
            if matched_domains:
                placeholders = ", ".join(["%s"] * len(matched_domains))
                src_sql = (
                    f"SELECT COUNT(DISTINCT source_system) "
                    f"FROM semantic_triples WHERE is_active = true "
                    f"AND split_part(concept, '.', 1) IN ({placeholders})"
                )
                with get_connection() as conn2:
                    if conn2 is None:
                        raise RuntimeError(
                            "TripleStore.get_persona_domain_stats failed: "
                            "database connection unavailable on source count query."
                        )
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
        clauses = ["is_active = true"]
        params: list = []
        if tenant_id:
            clauses.append("tenant_id = %s")
            params.append(tenant_id)

        where = " AND ".join(clauses)
        sql = (
            f"SELECT source_system, split_part(concept, '.', 1) AS domain, "
            f"entity_id, COUNT(*) AS triple_count "
            f"FROM semantic_triples WHERE {where} "
            f"GROUP BY source_system, split_part(concept, '.', 1), entity_id "
            f"ORDER BY triple_count DESC"
        )

        with get_connection() as conn:
            if conn is None:
                raise RuntimeError(
                    "TripleStore.get_sankey_aggregation failed: database connection unavailable. "
                    "Check DATABASE_URL and Supabase connectivity."
                )
            with conn.cursor() as cur:
                cur.execute(sql, params)
                columns = [desc[0] for desc in cur.description]
                return [dict(zip(columns, row)) for row in cur.fetchall()]

    def delete_by_run(self, run_id: str) -> int:
        """Hard-delete all triples for a run (test cleanup only)."""
        sql = "DELETE FROM semantic_triples WHERE run_id = %s"
        with get_connection() as conn:
            if conn is None:
                raise RuntimeError(
                    "TripleStore.delete_by_run failed: database connection unavailable."
                )
            with conn.cursor() as cur:
                cur.execute(sql, (run_id,))
                conn.commit()
                return cur.rowcount
