"""
MaterializedViews — pre-computed aggregations over semantic_triples for performance.

Provides summary queries that would otherwise require scanning large portions
of the triple store. All queries are scoped to tenant_id.
"""

from backend.core.db import get_connection
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)


def _to_float(value) -> float:
    """Convert a JSONB value (returned as str by psycopg2) to float."""
    return float(value)


class MaterializedViews:
    """Pre-computed aggregations over semantic_triples for performance."""

    def __init__(self, tenant_id: str, run_id: str):
        self.tenant_id = tenant_id
        self.run_id = run_id

    def _query(self, sql: str, params: list) -> list[dict]:
        """Execute a parameterized query and return rows as dicts."""
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                columns = [desc[0] for desc in cur.description]
                return [dict(zip(columns, row)) for row in cur.fetchall()]

    def get_entity_summary(self, entity_id: str) -> dict:
        """
        Returns: total triples, period range, domain counts, concept list.
        """
        sql = """
            SELECT
                COUNT(*) AS total_triples,
                MIN(period) AS period_min,
                MAX(period) AS period_max,
                COUNT(DISTINCT concept) AS concept_count,
                COUNT(DISTINCT split_part(concept, '.', 1)) AS domain_count
            FROM semantic_triples
            WHERE tenant_id = %s
              AND run_id = %s
              AND entity_id = %s
        """
        rows = self._query(sql, [self.tenant_id, self.run_id, entity_id])
        if not rows or rows[0]["total_triples"] == 0:
            raise ValueError(
                f"Entity summary not found: entity_id='{entity_id}' — "
                f"no triples in semantic_triples for "
                f"tenant_id='{self.tenant_id}'"
            )
        row = rows[0]

        domain_sql = """
            SELECT split_part(concept, '.', 1) AS domain, COUNT(*) AS cnt
            FROM semantic_triples
            WHERE tenant_id = %s
              AND run_id = %s
              AND entity_id = %s
            GROUP BY domain ORDER BY domain
        """
        domain_rows = self._query(domain_sql, [self.tenant_id, self.run_id, entity_id])
        domain_counts = {r["domain"]: r["cnt"] for r in domain_rows}

        concept_sql = """
            SELECT DISTINCT concept
            FROM semantic_triples
            WHERE tenant_id = %s
              AND run_id = %s
              AND entity_id = %s
            ORDER BY concept
        """
        concept_rows = self._query(concept_sql, [self.tenant_id, self.run_id, entity_id])
        concepts = [r["concept"] for r in concept_rows]

        return {
            "entity_id": entity_id,
            "total_triples": row["total_triples"],
            "period_min": row["period_min"],
            "period_max": row["period_max"],
            "concept_count": row["concept_count"],
            "domain_count": row["domain_count"],
            "domain_counts": domain_counts,
            "concepts": concepts,
        }

    def get_period_summary(self, entity_id: str, period: str) -> dict:
        """
        Returns: all financial totals (revenue, cogs, opex, ebitda, assets, liabilities, equity).
        """
        totals_sql = """
            SELECT DISTINCT ON (entity_id, concept, period)
                   concept, value
            FROM semantic_triples
            WHERE tenant_id = %s
              AND run_id = %s
              AND entity_id = %s AND period = %s AND property = 'amount'
              AND concept IN (
                  'revenue.total', 'cogs.total', 'opex.total', 'pnl.ebitda',
                  'asset.total', 'liability.total', 'equity.total'
              )
            ORDER BY entity_id, concept, period, created_at DESC
        """
        rows = self._query(totals_sql, [self.tenant_id, self.run_id, entity_id, period])
        if not rows:
            raise ValueError(
                f"Period summary not found: entity_id='{entity_id}', period='{period}' — "
                f"no financial totals in semantic_triples for "
                f"tenant_id='{self.tenant_id}'"
            )

        result = {"entity_id": entity_id, "period": period}
        concept_map = {
            "revenue.total": "revenue",
            "cogs.total": "cogs",
            "opex.total": "opex",
            "pnl.ebitda": "ebitda",
            "asset.total": "assets",
            "liability.total": "liabilities",
            "equity.total": "equity",
        }
        for row in rows:
            key = concept_map.get(row["concept"])
            if key:
                result[key] = _to_float(row["value"])
        return result

    def get_all_periods(self) -> list[str]:
        """Returns sorted list of all distinct quarterly periods (YYYY-QN format)."""
        sql = """
            SELECT DISTINCT period
            FROM semantic_triples
            WHERE tenant_id = %s
              AND run_id = %s
              AND period IS NOT NULL
              AND period ~ '^[0-9]{4}-Q[1-4]$'
            ORDER BY period
        """
        rows = self._query(sql, [self.tenant_id, self.run_id])
        return [r["period"] for r in rows]

    def get_all_entities(self) -> list[str]:
        """Returns list of all distinct entity_ids."""
        sql = """
            SELECT DISTINCT entity_id
            FROM semantic_triples
            WHERE tenant_id = %s
              AND run_id = %s
              AND entity_id != 'combined'
            ORDER BY entity_id
        """
        rows = self._query(sql, [self.tenant_id, self.run_id])
        return [r["entity_id"] for r in rows]
