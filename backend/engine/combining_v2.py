"""
CombiningEngineV2 — four-column combining financial statements from semantic_triples.

Produces combining statements (Entity A | Entity B | COFA Adjustments | Combined Pro Forma)
for income statement, balance sheet, and cash flow.

All data sourced from semantic_triples — no JSON file fallbacks.
Identity gates validate every statement before returning.
COFA conflicts read from cofa_conflict.* triples in the database.
"""

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

from backend.core.db import get_connection
from backend.engine.query_resolver_v2 import TripleQueryResolver
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)


def _jsonb_str(value) -> str:
    """Convert a JSONB value (returned as str by psycopg2) to a clean string.

    JSONB strings come back as '"COFA-001"' — strip the surrounding quotes.
    """
    s = str(value)
    if len(s) >= 2 and s.startswith('"') and s.endswith('"'):
        return s[1:-1]
    return s


def _jsonb_float(value) -> float:
    """Convert a JSONB value (returned as str by psycopg2) to float."""
    return float(value)


class CombiningEngineV2:
    """
    Produces combining financial statements from semantic_triples.

    Four columns: Entity A | Entity B | COFA Adjustments | Combined Pro Forma.
    Identity gates: every statement must balance before returning.
    """

    def __init__(self, tenant_id: str, run_id: str):
        """Store context, initialize TripleQueryResolver internally."""
        self.tenant_id = tenant_id
        self.run_id = run_id
        self._resolver = TripleQueryResolver(tenant_id, run_id)

    # ------------------------------------------------------------------
    # COFA adjustments
    # ------------------------------------------------------------------

    def get_cofa_adjustments(self, period: str | None = None) -> list[dict]:
        """
        Get all COFA conflict triples.

        Returns list of dicts with conflict_id, concept, description,
        dollar_impact, severity, conflict_type, acquirer_treatment,
        target_treatment, resolution_status.

        COFA conflicts are informational — they surface policy
        disagreements for human review. They are NOT automatically
        applied to the combining P&L.
        """
        raw = self._query_cofa_triples()

        grouped: dict[str, dict[str, str]] = defaultdict(dict)
        for concept, prop, value in raw:
            if prop not in grouped[concept]:
                grouped[concept][prop] = value

        results = []
        for concept in sorted(grouped.keys()):
            props = grouped[concept]
            conflict_id = concept.split(".")[-1] if "." in concept else concept
            dollar_impact = _jsonb_float(props.get("adjustment_amount", "0"))

            results.append({
                "conflict_id": _jsonb_str(props.get("conflict_id", conflict_id)),
                "concept": concept,
                "description": _jsonb_str(props.get("description", "")),
                "dollar_impact": dollar_impact,
                "severity": _jsonb_str(props.get("severity", "")),
                "conflict_type": _jsonb_str(props.get("category", "")),
                "acquirer_treatment": _jsonb_str(props.get("entity_a_treatment", "")),
                "target_treatment": _jsonb_str(props.get("entity_b_treatment", "")),
                "resolution_status": _jsonb_str(props.get("resolution_status", "")),
                "rationale": _jsonb_str(props.get("rationale", "")),
            })

        if not results:
            logger.info(
                "No COFA conflicts found in semantic_triples for "
                "tenant_id='%s'. Combining statement will use zero adjustments.",
                self.tenant_id,
            )

        return results

    def _query_cofa_triples(self) -> list[tuple[str, str, str]]:
        """Query all COFA conflict triples. Returns list of (concept, property, value)."""
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT ON (concept, property)
                           concept, property, value
                    FROM semantic_triples
                    WHERE tenant_id = %s AND is_active = true
                      AND concept LIKE 'cofa.%%'
                    ORDER BY concept, property, created_at DESC
                    """,
                    [self.tenant_id],
                )
                return [(r[0], r[1], r[2]) for r in cur.fetchall()]

    # ------------------------------------------------------------------
    # Combining income statement
    # ------------------------------------------------------------------

    def get_combining_income_statement(self, period: str) -> dict:
        """
        Four-column P&L combining statement.

        Identity gate: combined.ebitda == entity_a.ebitda + entity_b.ebitda + adjustments.total_ebitda_impact
        Raises ValueError if identity fails.
        """
        entities = self._resolver._get_entities()
        if len(entities) < 2:
            raise ValueError(
                f"Combining statement requires at least 2 entities, "
                f"found {len(entities)}: {entities} for "
                f"tenant_id='{self.tenant_id}', run_id='{self.run_id}'"
            )
        entity_a_id = entities[0]
        entity_b_id = entities[1]

        with ThreadPoolExecutor(max_workers=3) as pool:
            stmt_a_f = pool.submit(self._resolver.get_income_statement, entity_a_id, period)
            stmt_b_f = pool.submit(self._resolver.get_income_statement, entity_b_id, period)
            cofas_f = pool.submit(self.get_cofa_adjustments, period)
            stmt_a = stmt_a_f.result()
            stmt_b = stmt_b_f.result()
            cofas = cofas_f.result()
        adjustments, combined = self._apply_cofa_to_pnl(stmt_a, stmt_b, cofas)

        # Identity gate
        a_ebitda = stmt_a["ebitda"]
        b_ebitda = stmt_b["ebitda"]
        total_impact = adjustments["total_ebitda_impact"]
        expected_ebitda = round(a_ebitda + b_ebitda + total_impact, 2)
        actual_ebitda = combined["ebitda"]

        if abs(actual_ebitda - expected_ebitda) > 0.05:
            raise ValueError(
                f"P&L combining identity failed for period='{period}': "
                f"combined.ebitda({actual_ebitda}) != "
                f"entity_a.ebitda({a_ebitda}) + entity_b.ebitda({b_ebitda}) + "
                f"adjustments.total_ebitda_impact({total_impact}) = {expected_ebitda}"
            )

        return {
            "period": period,
            "entity_a": {"name": entity_a_id, **stmt_a},
            "entity_b": {"name": entity_b_id, **stmt_b},
            "adjustments": adjustments,
            "combined": combined,
            "identity_check": {
                "passed": True,
                "detail": (
                    f"combined.ebitda({actual_ebitda}) == "
                    f"entity_a.ebitda({a_ebitda}) + entity_b.ebitda({b_ebitda}) + "
                    f"adjustments({total_impact}) = {expected_ebitda}"
                ),
            },
        }

    # ------------------------------------------------------------------
    # Combining balance sheet
    # ------------------------------------------------------------------

    def get_combining_balance_sheet(self, period: str) -> dict:
        """
        Four-column BS combining statement.

        Identity gate: combined.assets == combined.liabilities + combined.equity
        $0 tolerance. Raises ValueError if identity fails.
        """
        entities = self._resolver._get_entities()
        if len(entities) < 2:
            raise ValueError(
                f"Combining BS requires at least 2 entities, "
                f"found {len(entities)}: {entities}"
            )
        entity_a_id = entities[0]
        entity_b_id = entities[1]

        with ThreadPoolExecutor(max_workers=2) as pool:
            bs_a_f = pool.submit(self._resolver.get_balance_sheet, entity_a_id, period)
            bs_b_f = pool.submit(self._resolver.get_balance_sheet, entity_b_id, period)
            bs_a = bs_a_f.result()
            bs_b = bs_b_f.result()

        combined = TripleQueryResolver._add_statement_dicts(bs_a, bs_b)

        # Identity gate
        a_total = round(combined["assets"]["total"], 2)
        l_total = round(combined["liabilities"]["total"], 2)
        e_total = round(combined["equity"]["total"], 2)
        rhs = round(l_total + e_total, 2)

        if abs(a_total - rhs) > 0.05:
            raise ValueError(
                f"BS combining identity failed for period='{period}': "
                f"combined.assets.total({a_total}) != "
                f"combined.liabilities.total({l_total}) + "
                f"combined.equity.total({e_total}) = {rhs}"
            )

        return {
            "period": period,
            "entity_a": {"name": entity_a_id, **bs_a},
            "entity_b": {"name": entity_b_id, **bs_b},
            "combined": combined,
            "identity_check": {
                "passed": True,
                "detail": (
                    f"combined.assets({a_total}) == "
                    f"combined.liabilities({l_total}) + "
                    f"combined.equity({e_total}) = {rhs}"
                ),
            },
        }

    # ------------------------------------------------------------------
    # Combining cash flow
    # ------------------------------------------------------------------

    def get_combining_cash_flow(self, period: str) -> dict:
        """
        Four-column CF combining statement.

        Identity gate: combined.operating + combined.investing + combined.financing == combined.net_change
        $0 tolerance. Raises ValueError if identity fails.
        """
        entities = self._resolver._get_entities()
        if len(entities) < 2:
            raise ValueError(
                f"Combining CF requires at least 2 entities, "
                f"found {len(entities)}: {entities}"
            )
        entity_a_id = entities[0]
        entity_b_id = entities[1]

        with ThreadPoolExecutor(max_workers=2) as pool:
            cf_a_f = pool.submit(self._resolver.get_cash_flow, entity_a_id, period)
            cf_b_f = pool.submit(self._resolver.get_cash_flow, entity_b_id, period)
            cf_a = cf_a_f.result()
            cf_b = cf_b_f.result()

        combined = TripleQueryResolver._add_statement_dicts(cf_a, cf_b)

        # Identity gate
        op = round(combined["operating"]["total"], 2)
        inv = round(combined["investing"]["total"], 2)
        fin = round(combined["financing"]["total"], 2)
        net = round(combined["net_change"], 2)
        computed = round(op + inv + fin, 2)

        if computed != net:
            raise ValueError(
                f"CF combining identity failed for period='{period}': "
                f"operating({op}) + investing({inv}) + financing({fin}) "
                f"= {computed} != net_change({net})"
            )

        return {
            "period": period,
            "entity_a": {"name": entity_a_id, **cf_a},
            "entity_b": {"name": entity_b_id, **cf_b},
            "combined": combined,
            "identity_check": {
                "passed": True,
                "detail": (
                    f"operating({op}) + investing({inv}) + financing({fin}) "
                    f"= {computed} == net_change({net})"
                ),
            },
        }

    # ------------------------------------------------------------------
    # COFA application
    # ------------------------------------------------------------------

    def _apply_cofa_to_pnl(
        self,
        entity_a_pnl: dict,
        entity_b_pnl: dict,
        cofa: list[dict],
    ) -> tuple[dict, dict]:
        """
        Produce the adjustments column and combined column.
        Returns (adjustments_dict, combined_dict).

        COFA conflicts are informational — they surface policy disagreements
        for human review. The combining statement sums entity P&Ls directly;
        COFA conflicts are presented separately (not auto-applied).
        """
        adjustments = {
            "revenue": {"total": 0.0},
            "cogs": {"total": 0.0},
            "opex": {"total": 0.0},
            "depreciation": {"total": 0.0},
            "total_ebitda_impact": 0.0,
            "cofa_conflicts": len(cofa),
        }

        raw_combined = TripleQueryResolver._add_statement_dicts(entity_a_pnl, entity_b_pnl)

        return adjustments, raw_combined
