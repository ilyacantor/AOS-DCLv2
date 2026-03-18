"""
TripleQueryResolver v2 — resolves financial queries against semantic_triples in PG.

Unlike v1 (query_resolver.py) which resolves against the in-memory semantic graph,
v2 resolves directly against the semantic_triples fact store.
"""

from backend.core.db import get_connection
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

_IDENTITY_TOLERANCE = 0.05


def _to_float(value) -> float:
    """Convert a JSONB value (returned as str by psycopg2) to float."""
    return float(value)


class TripleQueryResolver:
    """Resolves financial queries against semantic_triples in PG."""

    def __init__(self, tenant_id: str, run_id: str):
        """Store tenant/run context. All queries scoped to these."""
        self.tenant_id = tenant_id
        self.run_id = run_id

    def _query(self, sql: str, params: list) -> list[dict]:
        """Execute a parameterized query and return rows as dicts."""
        with get_connection() as conn:
            if conn is None:
                raise RuntimeError(
                    "TripleQueryResolver: database connection unavailable. "
                    "Check DATABASE_URL and Supabase connectivity."
                )
            with conn.cursor() as cur:
                cur.execute(sql, params)
                columns = [desc[0] for desc in cur.description]
                return [dict(zip(columns, row)) for row in cur.fetchall()]

    def _query_scalar(self, sql: str, params: list):
        """Execute a query and return a single scalar value."""
        with get_connection() as conn:
            if conn is None:
                raise RuntimeError(
                    "TripleQueryResolver: database connection unavailable. "
                    "Check DATABASE_URL and Supabase connectivity."
                )
            with conn.cursor() as cur:
                cur.execute(sql, params)
                row = cur.fetchone()
                return row[0] if row else None

    # ------------------------------------------------------------------
    # Single metric
    # ------------------------------------------------------------------

    def get_metric(self, concept: str, entity_id: str, period: str) -> dict:
        """
        Get a single metric value.
        Returns: {"concept": str, "entity_id": str, "period": str, "value": float,
                  "currency": str, "unit": str, "source_system": str, "confidence_score": float}
        Raises ValueError if not found (NO silent fallback to None/0).
        """
        sql = """
            SELECT DISTINCT ON (entity_id, concept, property, period)
                   concept, entity_id, period, value, currency, unit,
                   source_system, confidence_score
            FROM semantic_triples
            WHERE tenant_id = %s AND is_active = true
              AND concept = %s AND entity_id = %s AND period = %s
              AND property = 'amount'
            ORDER BY entity_id, concept, property, period, created_at DESC
        """
        rows = self._query(sql, [self.tenant_id, concept, entity_id, period])
        if not rows:
            raise ValueError(
                f"Metric not found: concept='{concept}', entity_id='{entity_id}', "
                f"period='{period}' — no matching triple in semantic_triples for "
                f"tenant_id='{self.tenant_id}', run_id='{self.run_id}'"
            )
        row = rows[0]
        return {
            "concept": row["concept"],
            "entity_id": row["entity_id"],
            "period": row["period"],
            "value": _to_float(row["value"]),
            "currency": row["currency"],
            "unit": row["unit"],
            "source_system": row["source_system"],
            "confidence_score": float(row["confidence_score"]) if row["confidence_score"] is not None else 0.0,
        }

    # ------------------------------------------------------------------
    # Timeseries
    # ------------------------------------------------------------------

    def get_metric_timeseries(self, concept: str, entity_id: str,
                               periods: list[str] | None = None) -> list[dict]:
        """
        Get a metric across all periods (or specified periods).
        Returns list of dicts, ordered by period.
        Raises ValueError if concept/entity not found at all.
        """
        if periods:
            placeholders = ", ".join(["%s"] * len(periods))
            sql = f"""
                SELECT DISTINCT ON (entity_id, concept, property, period)
                       concept, entity_id, period, value, currency, unit,
                       source_system, confidence_score
                FROM semantic_triples
                WHERE tenant_id = %s AND is_active = true
                  AND concept = %s AND entity_id = %s AND property = 'amount'
                  AND period IN ({placeholders})
                ORDER BY entity_id, concept, property, period, created_at DESC
            """
            params = [self.tenant_id, concept, entity_id] + periods
        else:
            sql = """
                SELECT DISTINCT ON (entity_id, concept, property, period)
                       concept, entity_id, period, value, currency, unit,
                       source_system, confidence_score
                FROM semantic_triples
                WHERE tenant_id = %s AND is_active = true
                  AND concept = %s AND entity_id = %s AND property = 'amount'
                  AND period IS NOT NULL
                ORDER BY entity_id, concept, property, period, created_at DESC
            """
            params = [self.tenant_id, concept, entity_id]

        rows = self._query(sql, params)
        if not rows:
            raise ValueError(
                f"Timeseries not found: concept='{concept}', entity_id='{entity_id}' — "
                f"no matching triples in semantic_triples for "
                f"tenant_id='{self.tenant_id}', run_id='{self.run_id}'"
            )
        return [
            {
                "concept": r["concept"],
                "entity_id": r["entity_id"],
                "period": r["period"],
                "value": _to_float(r["value"]),
                "currency": r["currency"],
                "unit": r["unit"],
                "source_system": r["source_system"],
                "confidence_score": float(r["confidence_score"]) if r["confidence_score"] is not None else 0.0,
            }
            for r in rows
        ]

    # ------------------------------------------------------------------
    # Domain retrieval
    # ------------------------------------------------------------------

    def get_domain(self, domain: str, entity_id: str, period: str) -> list[dict]:
        """
        Get all concepts in a domain (e.g., 'revenue' returns revenue.total,
        revenue.consulting, etc.) for an entity/period.
        Domain = first segment of concept (split on '.').
        Returns list of {"concept": str, "value": float, ...}.
        """
        sql = """
            SELECT DISTINCT ON (entity_id, concept, property, period)
                   concept, entity_id, period, value, currency, unit,
                   source_system, confidence_score
            FROM semantic_triples
            WHERE tenant_id = %s AND is_active = true
              AND concept LIKE %s
              AND entity_id = %s AND period = %s AND property = 'amount'
            ORDER BY entity_id, concept, property, period, created_at DESC
        """
        rows = self._query(sql, [self.tenant_id, f"{domain}.%", entity_id, period])
        return [
            {
                "concept": r["concept"],
                "value": _to_float(r["value"]),
                "currency": r["currency"],
                "unit": r["unit"],
                "source_system": r["source_system"],
                "confidence_score": float(r["confidence_score"]) if r["confidence_score"] is not None else 0.0,
            }
            for r in rows
        ]

    # ------------------------------------------------------------------
    # Internal helpers for financial statements
    # ------------------------------------------------------------------

    def _domain_to_dict(self, domain: str, entity_id: str, period: str) -> dict[str, float]:
        """Fetch all concepts in a domain and return as {sub_key: value}.

        Strips the domain prefix: 'revenue.total' -> 'total'.
        """
        items = self.get_domain(domain, entity_id, period)
        result: dict[str, float] = {}
        prefix_len = len(domain) + 1  # "domain."
        for item in items:
            suffix = item["concept"][prefix_len:]
            result[suffix] = item["value"]
        return result

    def _cf_to_dict(self, entity_id: str, period: str) -> dict:
        """Fetch all cash_flow concepts and structure into nested dict.

        cash_flow.operating.total -> {"operating": {"total": ...}}
        cash_flow.net_change -> {"net_change": ...}
        """
        items = self.get_domain("cash_flow", entity_id, period)
        result: dict = {}
        prefix_len = len("cash_flow.")
        for item in items:
            suffix = item["concept"][prefix_len:]
            parts = suffix.split(".", 1)
            if len(parts) == 2:
                category, sub_key = parts
                if category not in result:
                    result[category] = {}
                result[category][sub_key] = item["value"]
            else:
                result[parts[0]] = item["value"]
        return result

    @staticmethod
    def _add_statement_dicts(a: dict, b: dict) -> dict:
        """Recursively add two statement dicts (for combining statements)."""
        result: dict = {}
        all_keys = set(a.keys()) | set(b.keys())
        for key in sorted(all_keys):
            av = a.get(key)
            bv = b.get(key)
            if isinstance(av, dict) and isinstance(bv, dict):
                result[key] = TripleQueryResolver._add_statement_dicts(av, bv)
            elif isinstance(av, dict):
                result[key] = av.copy()
            elif isinstance(bv, dict):
                result[key] = bv.copy()
            else:
                result[key] = (av or 0.0) + (bv or 0.0)
        return result

    # ------------------------------------------------------------------
    # Income statement
    # ------------------------------------------------------------------

    def get_income_statement(self, entity_id: str, period: str) -> dict:
        """
        Assemble P&L from triples: revenue.*, cogs.*, opex.*, pnl.*.
        Returns structured dict with line items and totals.
        Validates P&L identity: revenue.total - cogs.total - opex.total == pnl.ebitda.
        Raises ValueError if identity fails.
        """
        revenue = self._domain_to_dict("revenue", entity_id, period)
        cogs = self._domain_to_dict("cogs", entity_id, period)
        opex = self._domain_to_dict("opex", entity_id, period)
        pnl = self._domain_to_dict("pnl", entity_id, period)

        rev_total = revenue.get("total")
        cogs_total = cogs.get("total")
        opex_total = opex.get("total")
        ebitda = pnl.get("ebitda")

        missing = []
        if rev_total is None:
            missing.append("revenue.total")
        if cogs_total is None:
            missing.append("cogs.total")
        if opex_total is None:
            missing.append("opex.total")
        if ebitda is None:
            missing.append("pnl.ebitda")
        if missing:
            raise ValueError(
                f"Income statement incomplete for entity_id='{entity_id}', period='{period}': "
                f"missing {', '.join(missing)} in semantic_triples for "
                f"tenant_id='{self.tenant_id}', run_id='{self.run_id}'"
            )

        computed_ebitda = rev_total - cogs_total - opex_total
        if abs(computed_ebitda - ebitda) > _IDENTITY_TOLERANCE:
            raise ValueError(
                f"P&L identity failed for entity_id='{entity_id}', period='{period}': "
                f"revenue.total({rev_total}) - cogs.total({cogs_total}) - opex.total({opex_total}) = "
                f"{computed_ebitda} != pnl.ebitda({ebitda})"
            )

        stmt: dict = {
            "revenue": revenue,
            "cogs": cogs,
            "opex": opex,
        }
        for key, value in pnl.items():
            stmt[key] = value
        return stmt

    # ------------------------------------------------------------------
    # Balance sheet
    # ------------------------------------------------------------------

    def get_balance_sheet(self, entity_id: str, period: str) -> dict:
        """
        Assemble BS from triples: asset.*, liability.*, equity.*.
        Validates BS identity: asset.total == liability.total + equity.total.
        Raises ValueError if identity fails.
        """
        assets = self._domain_to_dict("asset", entity_id, period)
        liabilities = self._domain_to_dict("liability", entity_id, period)
        equity = self._domain_to_dict("equity", entity_id, period)

        a_total = assets.get("total")
        l_total = liabilities.get("total")
        e_total = equity.get("total")

        missing = []
        if a_total is None:
            missing.append("asset.total")
        if l_total is None:
            missing.append("liability.total")
        if e_total is None:
            missing.append("equity.total")
        if missing:
            raise ValueError(
                f"Balance sheet incomplete for entity_id='{entity_id}', period='{period}': "
                f"missing {', '.join(missing)} in semantic_triples for "
                f"tenant_id='{self.tenant_id}', run_id='{self.run_id}'"
            )

        rhs = l_total + e_total
        if abs(a_total - rhs) > _IDENTITY_TOLERANCE:
            raise ValueError(
                f"BS identity failed for entity_id='{entity_id}', period='{period}': "
                f"asset.total({a_total}) != liability.total({l_total}) + equity.total({e_total}) = {rhs}"
            )

        return {
            "assets": assets,
            "liabilities": liabilities,
            "equity": equity,
        }

    # ------------------------------------------------------------------
    # Cash flow
    # ------------------------------------------------------------------

    def get_cash_flow(self, entity_id: str, period: str) -> dict:
        """
        Assemble CF from triples: cash_flow.*.
        Validates CF identity: operating.total + investing.total + financing.total == net_change.
        Raises ValueError if identity fails.
        """
        cf = self._cf_to_dict(entity_id, period)

        op_total = cf.get("operating", {}).get("total")
        inv_total = cf.get("investing", {}).get("total")
        fin_total = cf.get("financing", {}).get("total")
        net_change = cf.get("net_change")

        missing = []
        if op_total is None:
            missing.append("cash_flow.operating.total")
        if inv_total is None:
            missing.append("cash_flow.investing.total")
        if fin_total is None:
            missing.append("cash_flow.financing.total")
        if net_change is None:
            missing.append("cash_flow.net_change")
        if missing:
            raise ValueError(
                f"Cash flow incomplete for entity_id='{entity_id}', period='{period}': "
                f"missing {', '.join(missing)} in semantic_triples for "
                f"tenant_id='{self.tenant_id}', run_id='{self.run_id}'"
            )

        computed = op_total + inv_total + fin_total
        if abs(computed - net_change) > _IDENTITY_TOLERANCE:
            raise ValueError(
                f"CF identity failed for entity_id='{entity_id}', period='{period}': "
                f"operating.total({op_total}) + investing.total({inv_total}) + "
                f"financing.total({fin_total}) = {computed} != net_change({net_change})"
            )

        return cf

    # ------------------------------------------------------------------
    # Combining statement
    # ------------------------------------------------------------------

    def get_combining_statement(self, statement_type: str, period: str) -> dict:
        """
        Get a combining statement (entity_a + entity_b + combined).
        statement_type: "income_statement" | "balance_sheet" | "cash_flow"
        Returns {"entity_a": {...}, "entity_b": {...}, "combined": {...}}.
        Combined = simple sum (no COFA adjustments).
        """
        method_map = {
            "income_statement": self.get_income_statement,
            "balance_sheet": self.get_balance_sheet,
            "cash_flow": self.get_cash_flow,
        }
        if statement_type not in method_map:
            raise ValueError(
                f"Invalid statement_type='{statement_type}'. "
                f"Must be one of: {', '.join(method_map.keys())}"
            )

        entities = self._get_entities()
        if len(entities) < 2:
            raise ValueError(
                f"Combining statement requires at least 2 entities, "
                f"found {len(entities)}: {entities} for "
                f"tenant_id='{self.tenant_id}', run_id='{self.run_id}'"
            )

        entity_a = entities[0]
        entity_b = entities[1]
        method = method_map[statement_type]

        stmt_a = method(entity_a, period)
        stmt_b = method(entity_b, period)
        combined = self._add_statement_dicts(stmt_a, stmt_b)

        return {
            "entity_a": stmt_a,
            "entity_b": stmt_b,
            "combined": combined,
        }

    def _get_entities(self) -> list[str]:
        """Get distinct entity_ids ordered descending (larger entity first).

        Excludes synthetic aggregate entities ('combined') — those are NLQ
        query-time constructs, not real entities with full triple data.
        """
        sql = """
            SELECT DISTINCT entity_id
            FROM semantic_triples
            WHERE tenant_id = %s AND is_active = true
              AND entity_id != 'combined'
            ORDER BY entity_id DESC
        """
        rows = self._query(sql, [self.tenant_id])
        return [r["entity_id"] for r in rows]

    # ------------------------------------------------------------------
    # Overlapping concepts
    # ------------------------------------------------------------------

    def get_overlapping_concepts(self, domain: str) -> list[str]:
        """
        Find concepts that appear under both entity_ids.
        Domain: 'customer', 'vendor', 'employee'.
        Returns list of concept names that have rows for both entities.
        """
        sql = """
            SELECT concept
            FROM semantic_triples
            WHERE tenant_id = %s AND is_active = true
              AND concept LIKE %s
            GROUP BY concept
            HAVING COUNT(DISTINCT entity_id) > 1
            ORDER BY concept
        """
        rows = self._query(sql, [self.tenant_id, f"{domain}.%"])
        return [r["concept"] for r in rows]

    # ------------------------------------------------------------------
    # Provenance
    # ------------------------------------------------------------------

    def get_provenance(self, concept: str, entity_id: str, period: str) -> dict:
        """
        Get full provenance for a value: source_system, source_table, source_field,
        pipe_id, confidence_score, confidence_tier, run_id.
        """
        sql = """
            SELECT DISTINCT ON (entity_id, concept, property, period)
                   source_system, source_table, source_field,
                   pipe_id, confidence_score, confidence_tier, run_id
            FROM semantic_triples
            WHERE tenant_id = %s AND is_active = true
              AND concept = %s AND entity_id = %s AND period = %s
              AND property = 'amount'
            ORDER BY entity_id, concept, property, period, created_at DESC
        """
        rows = self._query(sql, [self.tenant_id, concept, entity_id, period])
        if not rows:
            raise ValueError(
                f"Provenance not found: concept='{concept}', entity_id='{entity_id}', "
                f"period='{period}' — no matching triple in semantic_triples for "
                f"tenant_id='{self.tenant_id}', run_id='{self.run_id}'"
            )
        row = rows[0]
        return {
            "source_system": row["source_system"],
            "source_table": row["source_table"],
            "source_field": row["source_field"],
            "pipe_id": str(row["pipe_id"]) if row["pipe_id"] else None,
            "confidence_score": float(row["confidence_score"]) if row["confidence_score"] is not None else 0.0,
            "confidence_tier": row["confidence_tier"],
            "run_id": str(row["run_id"]),
        }
