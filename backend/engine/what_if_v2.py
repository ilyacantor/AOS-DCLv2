"""
WhatIfEngineV2 — what-if scenario engine with baselines from semantic_triples.

Baselines are read from PG via TripleQueryResolver. Scenarios apply percentage
or absolute adjustments to baselines and compute downstream impacts through
the P&L chain (revenue → EBITDA → net income).

Scenario persistence: saved to PG table `whatif_scenarios`.
"""

import uuid
from datetime import datetime, timezone

from backend.core.db import get_connection
from backend.engine.query_resolver_v2 import TripleQueryResolver
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

# Concepts that cascade from revenue changes into EBITDA and net income.
# When revenue changes, COGS and OPEX stay fixed, so the entire delta
# flows through to EBITDA, and then to net income (after tax/D&A).
_PNL_CASCADE_CONCEPTS = [
    "pnl.ebitda",
    "pnl.net_income",
]


def _ensure_scenarios_table() -> None:
    """Create the whatif_scenarios table if it does not exist."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS whatif_scenarios (
                    id TEXT PRIMARY KEY,
                    tenant_id TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    period TEXT NOT NULL,
                    adjustments JSONB NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            conn.commit()


class WhatIfEngineV2:
    """
    What-if scenario engine with baselines from semantic_triples.

    Baselines are read from PG. Scenarios apply percentage or absolute
    adjustments to baselines and compute downstream impacts through
    the P&L, BS, and CF chains.
    """

    def __init__(self, tenant_id: str, run_id: str):
        self.tenant_id = tenant_id
        self.run_id = run_id
        self._resolver = TripleQueryResolver(tenant_id, run_id)

    def get_baseline(self, entity_id: str, period: str) -> dict:
        """
        Get baseline financial metrics from triples.
        Returns P&L + BS + CF baselines.
        These are the starting point for any scenario.
        """
        pnl = self._resolver.get_income_statement(entity_id, period)
        bs = self._resolver.get_balance_sheet(entity_id, period)
        cf = self._resolver.get_cash_flow(entity_id, period)

        return {
            "revenue": pnl["revenue"],
            "cogs": pnl["cogs"],
            "opex": pnl["opex"],
            "ebitda": pnl["ebitda"],
            "net_income": pnl["net_income"],
            "balance_sheet": bs,
            "cash_flow": cf,
        }

    def apply_scenario(self, entity_id: str, period: str,
                       adjustments: list[dict]) -> dict:
        """
        Apply adjustments to baseline and compute impacts.

        adjustments: [{"concept": "revenue.total", "type": "pct", "value": -10.0}]
        type: "pct" (percentage change) or "abs" (absolute change)

        Returns:
        {
            "baseline": {...},
            "adjusted": {...},
            "impacts": [{"concept": str, "baseline": float, "adjusted": float,
                         "delta": float, "pct_change": float}]
        }

        Cascading impacts: revenue change flows through to EBITDA, net income.
        """
        baseline = self.get_baseline(entity_id, period)
        adjusted = _deep_copy_dict(baseline)
        impacts: list[dict] = []

        # Track total revenue delta for cascading
        revenue_delta = 0.0

        for adj in adjustments:
            concept = adj["concept"]
            adj_type = adj["type"]
            adj_value = adj["value"]

            # Resolve the baseline value for this concept
            baseline_value = _resolve_concept_value(baseline, concept)
            if baseline_value is None:
                raise ValueError(
                    f"Cannot apply adjustment: concept '{concept}' not found in baseline "
                    f"for entity_id='{entity_id}', period='{period}'"
                )

            if adj_type == "pct":
                delta = round(baseline_value * adj_value / 100.0, 4)
            elif adj_type == "abs":
                delta = adj_value
            else:
                raise ValueError(
                    f"Invalid adjustment type '{adj_type}'. Must be 'pct' or 'abs'."
                )

            new_value = round(baseline_value + delta, 2)
            _set_concept_value(adjusted, concept, new_value)

            impacts.append({
                "concept": concept,
                "baseline": baseline_value,
                "adjusted": new_value,
                "delta": round(delta, 2),
                "pct_change": round(delta / baseline_value * 100, 2) if baseline_value != 0 else 0.0,
            })

            # Track revenue-level changes for cascading
            if concept.startswith("revenue."):
                revenue_delta += delta

        # Cascade revenue changes to EBITDA and net income
        # Revenue change flows straight through because COGS and OPEX are fixed
        if revenue_delta != 0.0:
            for cascade_concept in _PNL_CASCADE_CONCEPTS:
                base_val = _resolve_concept_value(baseline, cascade_concept)
                if base_val is not None:
                    new_val = round(base_val + revenue_delta, 2)
                    _set_concept_value(adjusted, cascade_concept, new_val)
                    # Only add impact if not already directly adjusted
                    already_adjusted = any(i["concept"] == cascade_concept for i in impacts)
                    if not already_adjusted:
                        impacts.append({
                            "concept": cascade_concept,
                            "baseline": base_val,
                            "adjusted": new_val,
                            "delta": round(revenue_delta, 2),
                            "pct_change": round(revenue_delta / base_val * 100, 2) if base_val != 0 else 0.0,
                        })

        return {
            "baseline": baseline,
            "adjusted": adjusted,
            "impacts": impacts,
        }

    def compare_scenarios(self, entity_id: str, period: str,
                          scenarios: dict[str, list[dict]]) -> dict:
        """
        Compare multiple named scenarios side by side.
        scenarios: {"bear": [adjustments], "base": [adjustments], "bull": [adjustments]}
        Returns: {"scenarios": {name: result_dict}, "comparison_table": [...]}
        """
        results: dict[str, dict] = {}
        for name, adjustments in scenarios.items():
            results[name] = self.apply_scenario(entity_id, period, adjustments)

        # Build comparison table across all scenarios
        comparison_table: list[dict] = []
        all_concepts: set[str] = set()
        for result in results.values():
            for impact in result["impacts"]:
                all_concepts.add(impact["concept"])

        for concept in sorted(all_concepts):
            row: dict = {"concept": concept}
            for name, result in results.items():
                matching = [i for i in result["impacts"] if i["concept"] == concept]
                if matching:
                    row[name] = matching[0]["adjusted"]
                else:
                    row[name] = _resolve_concept_value(result["baseline"], concept)
            comparison_table.append(row)

        return {
            "scenarios": results,
            "comparison_table": comparison_table,
        }

    def sensitivity_analysis(self, entity_id: str, period: str,
                             concept: str, range_pct: float = 20.0,
                             steps: int = 5) -> list[dict]:
        """
        Vary a single concept from -range_pct to +range_pct in steps.
        Returns list of impact dicts showing how EBITDA/net_income change.
        """
        baseline = self.get_baseline(entity_id, period)
        base_value = _resolve_concept_value(baseline, concept)
        if base_value is None:
            raise ValueError(
                f"Concept '{concept}' not found in baseline for "
                f"entity_id='{entity_id}', period='{period}'"
            )

        results: list[dict] = []
        step_size = (2 * range_pct) / (steps - 1) if steps > 1 else 0
        for i in range(steps):
            pct = -range_pct + i * step_size
            scenario = self.apply_scenario(entity_id, period, [
                {"concept": concept, "type": "pct", "value": pct}
            ])
            ebitda = _resolve_concept_value(scenario["adjusted"], "pnl.ebitda")
            net_income = _resolve_concept_value(scenario["adjusted"], "pnl.net_income")
            results.append({
                "pct_change": round(pct, 2),
                "concept_value": _resolve_concept_value(scenario["adjusted"], concept),
                "ebitda": ebitda,
                "net_income": net_income,
            })

        return results

    def save_scenario(self, name: str, entity_id: str, period: str,
                      adjustments: list[dict]) -> str:
        """
        Persist a scenario to PG. Returns scenario_id.
        """
        import json

        _ensure_scenarios_table()
        scenario_id = str(uuid.uuid4())

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO whatif_scenarios (id, tenant_id, run_id, name, entity_id, period, adjustments, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    [
                        scenario_id, self.tenant_id, self.run_id, name,
                        entity_id, period, json.dumps(adjustments),
                        datetime.now(timezone.utc).isoformat(),
                    ],
                )
                conn.commit()

        logger.info(
            "[what_if_v2] Saved scenario '%s' (id=%s) for entity=%s period=%s",
            name, scenario_id, entity_id, period,
        )
        return scenario_id

    def list_scenarios(self) -> list[dict]:
        """List all saved scenarios for this tenant/run."""
        _ensure_scenarios_table()

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, name, entity_id, period, adjustments, created_at
                    FROM whatif_scenarios
                    WHERE tenant_id = %s AND run_id = %s
                    ORDER BY created_at DESC
                    """,
                    [self.tenant_id, self.run_id],
                )
                columns = [desc[0] for desc in cur.description]
                rows = [dict(zip(columns, row)) for row in cur.fetchall()]

        return [
            {
                "id": r["id"],
                "name": r["name"],
                "entity_id": r["entity_id"],
                "period": r["period"],
                "adjustments": r["adjustments"],
                "created_at": str(r["created_at"]),
            }
            for r in rows
        ]

    def load_scenario(self, scenario_id: str) -> dict:
        """Load a saved scenario and re-apply it against current baselines."""
        _ensure_scenarios_table()

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT name, entity_id, period, adjustments
                    FROM whatif_scenarios
                    WHERE id = %s AND tenant_id = %s AND run_id = %s
                    """,
                    [scenario_id, self.tenant_id, self.run_id],
                )
                row = cur.fetchone()

        if row is None:
            raise ValueError(
                f"Scenario not found: id='{scenario_id}' for "
                f"tenant_id='{self.tenant_id}', run_id='{self.run_id}'"
            )

        name, entity_id, period, adjustments = row

        # Re-apply against current baselines
        result = self.apply_scenario(entity_id, period, adjustments)
        result["scenario_id"] = scenario_id
        result["name"] = name
        result["entity_id"] = entity_id
        result["period"] = period
        result["adjustments_applied"] = adjustments
        return result


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _deep_copy_dict(d: dict) -> dict:
    """Deep copy a dict of dicts/floats."""
    result: dict = {}
    for k, v in d.items():
        if isinstance(v, dict):
            result[k] = _deep_copy_dict(v)
        else:
            result[k] = v
    return result


def _resolve_concept_value(data: dict, concept: str):
    """
    Resolve a dotted concept path to its value in the baseline dict.

    Mapping:
      'revenue.total' -> data['revenue']['total']
      'revenue.consulting' -> data['revenue']['consulting']
      'pnl.ebitda' -> data['ebitda']
      'pnl.net_income' -> data['net_income']
    """
    parts = concept.split(".")
    if len(parts) == 2:
        domain, key = parts
        # P&L top-level keys (ebitda, net_income) are stored flat
        if domain == "pnl":
            return data.get(key)
        # Nested dicts (revenue, cogs, opex, balance_sheet, cash_flow)
        section = data.get(domain)
        if isinstance(section, dict):
            return section.get(key)
    return None


def _set_concept_value(data: dict, concept: str, value: float) -> None:
    """Set a value at a dotted concept path in the data dict."""
    parts = concept.split(".")
    if len(parts) == 2:
        domain, key = parts
        if domain == "pnl":
            data[key] = value
            return
        section = data.get(domain)
        if isinstance(section, dict):
            section[key] = value
