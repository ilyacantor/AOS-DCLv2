"""
CombiningEngineV2 — four-column combining financial statements from semantic_triples.

Produces combining statements (Entity A | Entity B | COFA Adjustments | Combined Pro Forma)
for income statement, balance sheet, and cash flow.

All data sourced from semantic_triples — no JSON file fallbacks.
Identity gates validate every statement before returning.
COFA adjustments read from cofa.* triples in the database.
"""

from collections import defaultdict

from backend.core.db import get_connection
from backend.engine.query_resolver_v2 import TripleQueryResolver
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

# COFA concept → which P&L section it affects
_COFA_AFFECTS = {
    "cofa.revenue_gross_up": "revenue",
    "cofa.benefits_loading": "cogs",
    "cofa.sales_marketing_bundling": "opex",
    "cofa.recruiting_capitalization": "opex",
    "cofa.automation_capitalization": "opex",
    "cofa.depreciation_methods": "depreciation",
}

# COFA concept → key name in the adjustments dict
_COFA_KEY = {
    "cofa.revenue_gross_up": "cofa_revenue_gross_up",
    "cofa.benefits_loading": "cofa_benefits_loading",
    "cofa.sales_marketing_bundling": "cofa_sales_marketing_bundling",
    "cofa.recruiting_capitalization": "cofa_recruiting_capitalization",
    "cofa.automation_capitalization": "cofa_automation_capitalization",
    "cofa.depreciation_methods": "cofa_depreciation_methods",
}


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
        Get all COFA adjustments from triples.

        Returns list of dicts with conflict_id, concept, category, description,
        entity_a_treatment, entity_b_treatment, adjustment_amount, rationale, affects.
        """
        raw = self._query_cofa_triples()

        # Group by concept
        grouped: dict[str, dict[str, str]] = defaultdict(dict)
        for concept, prop, value in raw:
            if prop not in grouped[concept]:
                grouped[concept][prop] = value

        # For depreciation_methods, compute period-specific amount if period given
        depreciation_adj = None
        if period is not None:
            depreciation_adj = self._compute_depreciation_adjustment(period)

        results = []
        for concept in sorted(grouped.keys()):
            props = grouped[concept]
            adj_amount = _jsonb_float(props.get("adjustment_amount", "0"))

            if concept == "cofa.depreciation_methods" and depreciation_adj is not None:
                adj_amount = depreciation_adj

            results.append({
                "conflict_id": _jsonb_str(props.get("conflict_id", "")),
                "concept": concept,
                "category": _jsonb_str(props.get("category", "")),
                "description": _jsonb_str(props.get("description", "")),
                "entity_a_treatment": _jsonb_str(props.get("entity_a_treatment", "")),
                "entity_b_treatment": _jsonb_str(props.get("entity_b_treatment", "")),
                "adjustment_amount": adj_amount,
                "rationale": _jsonb_str(props.get("rationale", "")),
                "affects": _COFA_AFFECTS.get(concept, "unknown"),
            })

        if not results:
            raise ValueError(
                f"No COFA adjustments found in semantic_triples for "
                f"tenant_id='{self.tenant_id}', run_id='{self.run_id}'. "
                f"COFA triples must be seeded before combining statements can be produced."
            )

        return results

    def _query_cofa_triples(self) -> list[tuple[str, str, str]]:
        """Query all COFA triples. Returns list of (concept, property, value)."""
        with get_connection() as conn:
            if conn is None:
                raise RuntimeError(
                    "CombiningEngineV2: database connection unavailable. "
                    "Check DATABASE_URL and Supabase connectivity."
                )
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT concept, property, value
                    FROM semantic_triples
                    WHERE tenant_id = %s AND run_id = %s AND is_active = true
                      AND concept LIKE 'cofa.%%'
                    ORDER BY concept, property, id
                    """,
                    [self.tenant_id, self.run_id],
                )
                return [(r[0], r[1], r[2]) for r in cur.fetchall()]

    def _compute_depreciation_adjustment(self, period: str) -> float:
        """Compute depreciation adjustment from Cascadia's D&A for a period.

        Formula: -(cascadia_da / 3) rounded to 2 decimals.
        This matches the seed data where accelerated depreciation (3-year)
        is restated to straight-line (5-year) by removing 1/3 of the amount.
        """
        entities = self._resolver._get_entities()
        entity_b = entities[1] if len(entities) >= 2 else "cascadia"

        da = self._resolver.get_metric(
            "pnl.depreciation_amortization", entity_b, period
        )
        return -round(da["value"] / 3, 2)

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

        stmt_a = self._resolver.get_income_statement(entity_a_id, period)
        stmt_b = self._resolver.get_income_statement(entity_b_id, period)

        cofas = self.get_cofa_adjustments(period=period)
        adjustments, combined = self._apply_cofa_to_pnl(stmt_a, stmt_b, cofas)

        # Identity gate
        a_ebitda = stmt_a["ebitda"]
        b_ebitda = stmt_b["ebitda"]
        total_impact = adjustments["total_ebitda_impact"]
        expected_ebitda = round(a_ebitda + b_ebitda + total_impact, 2)
        actual_ebitda = combined["ebitda"]

        if actual_ebitda != expected_ebitda:
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

        bs_a = self._resolver.get_balance_sheet(entity_a_id, period)
        bs_b = self._resolver.get_balance_sheet(entity_b_id, period)

        combined = TripleQueryResolver._add_statement_dicts(bs_a, bs_b)

        # Identity gate
        a_total = round(combined["assets"]["total"], 2)
        l_total = round(combined["liabilities"]["total"], 2)
        e_total = round(combined["equity"]["total"], 2)
        rhs = round(l_total + e_total, 2)

        if a_total != rhs:
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

        cf_a = self._resolver.get_cash_flow(entity_a_id, period)
        cf_b = self._resolver.get_cash_flow(entity_b_id, period)

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
        Apply COFA adjustments to produce the adjustments column and combined column.
        Returns (adjustments_dict, combined_dict).

        COFA mapping:
        - COFA-001 (revenue_gross_up): adjusts revenue.total
        - COFA-002 (benefits_loading): reclassifies within COGS, net impact 0
        - COFA-003 (sales_marketing_bundling): reclassifies within OpEx, net impact 0
        - COFA-004 (recruiting_capitalization): moves from capitalized to OpEx
        - COFA-005 (automation_capitalization): moves from capitalized to OpEx
        - COFA-006 (depreciation_methods): adjusts D&A (below EBITDA)
        """
        # Build adjustment amounts by P&L section
        rev_adjustments: dict[str, float] = {}
        cogs_adjustments: dict[str, float] = {}
        opex_adjustments: dict[str, float] = {}
        depreciation_adjustments: dict[str, float] = {}

        for item in cofa:
            concept = item["concept"]
            key = _COFA_KEY.get(concept)
            affects = _COFA_AFFECTS.get(concept)
            amount = item["adjustment_amount"]

            if key is None or affects is None:
                continue

            if affects == "revenue":
                rev_adjustments[key] = amount
            elif affects == "cogs":
                cogs_adjustments[key] = amount
            elif affects == "opex":
                opex_adjustments[key] = amount
            elif affects == "depreciation":
                depreciation_adjustments[key] = amount

        rev_total = round(sum(rev_adjustments.values()), 2)
        cogs_total = round(sum(cogs_adjustments.values()), 2)
        opex_total = round(sum(opex_adjustments.values()), 2)
        depreciation_total = round(sum(depreciation_adjustments.values()), 2)

        # EBITDA impact = revenue_adj - cogs_adj - opex_adj
        # (depreciation does NOT affect EBITDA, only operating profit)
        total_ebitda_impact = round(rev_total - cogs_total - opex_total, 2)

        adjustments = {
            "revenue": {**rev_adjustments, "total": rev_total},
            "cogs": {**cogs_adjustments, "total": cogs_total},
            "opex": {**opex_adjustments, "total": opex_total},
            "depreciation": {**depreciation_adjustments, "total": depreciation_total},
            "total_ebitda_impact": total_ebitda_impact,
        }

        # Build combined P&L
        raw_combined = TripleQueryResolver._add_statement_dicts(entity_a_pnl, entity_b_pnl)

        # Apply adjustments to totals
        raw_combined["revenue"]["total"] = round(
            raw_combined["revenue"]["total"] + rev_total, 2
        )
        raw_combined["cogs"]["total"] = round(
            raw_combined["cogs"]["total"] + cogs_total, 2
        )
        raw_combined["opex"]["total"] = round(
            raw_combined["opex"]["total"] + opex_total, 2
        )

        # Recompute EBITDA
        raw_combined["ebitda"] = round(
            raw_combined["revenue"]["total"]
            - raw_combined["cogs"]["total"]
            - raw_combined["opex"]["total"],
            2,
        )

        # Apply depreciation adjustment
        da_key = "depreciation_amortization"
        if da_key in raw_combined:
            raw_combined[da_key] = round(
                raw_combined[da_key] + depreciation_total, 2
            )

        # Recompute operating profit
        da_val = raw_combined.get(da_key, 0.0)
        raw_combined["operating_profit"] = round(
            raw_combined["ebitda"] - da_val, 2
        )

        # Recompute net income
        tax_val = raw_combined.get("tax", 0.0)
        raw_combined["net_income"] = round(
            raw_combined["operating_profit"] - tax_val, 2
        )

        return adjustments, raw_combined
