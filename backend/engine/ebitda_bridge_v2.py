"""
EBITDABridgeV2 — EBITDA bridge from reported to adjusted using ebitda_adjustment.* triples.

Bridge flow:
Reported EBITDA (Entity A + Entity B)
+ Normalizations (non_recurring_legal, non_recurring_professional_fees, related_party_transactions)
+ Cost Reductions (owner_compensation)
+ Synergies (facility_consolidation, headcount_synergies, run_rate_cost_savings, technology_consolidation)
= Adjusted Pro Forma EBITDA

All data sourced from semantic_triples in PG — no JSON files.
"""

from backend.core.db import get_connection
from backend.engine.engagement import get_active_engagement
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

# Lever classification for each adjustment concept
_LEVER_MAP = {
    "ebitda_adjustment.non_recurring_legal": "normalization",
    "ebitda_adjustment.non_recurring_professional_fees": "normalization",
    "ebitda_adjustment.related_party_transactions": "normalization",
    "ebitda_adjustment.owner_compensation": "cost_reduction",
    "ebitda_adjustment.facility_consolidation": "synergy",
    "ebitda_adjustment.headcount_synergies": "synergy",
    "ebitda_adjustment.run_rate_cost_savings": "synergy",
    "ebitda_adjustment.technology_consolidation": "synergy",
}

# All 2025 quarters for annual EBITDA
_ANNUAL_PERIODS = ["2025-Q1", "2025-Q2", "2025-Q3", "2025-Q4"]


def _to_float(value) -> float:
    """Convert a JSONB value to float."""
    return float(value)


def _to_str(value) -> str:
    """Convert a JSONB value to a clean string (strip surrounding quotes)."""
    s = str(value)
    if len(s) >= 2 and s.startswith('"') and s.endswith('"'):
        return s[1:-1]
    return s


class EBITDABridgeV2:
    """
    Produces EBITDA bridge from reported to adjusted using ebitda_adjustment.* triples.

    Bridge flow:
    Reported EBITDA (Entity A + Entity B)
    + Normalizations (non_recurring_legal, non_recurring_professional_fees, related_party_transactions)
    + Cost Reductions (owner_compensation)
    + Synergies (facility_consolidation, headcount_synergies, run_rate_cost_savings, technology_consolidation)
    = Adjusted Pro Forma EBITDA
    """

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

    def _get_entities(self) -> tuple[str, str]:
        """Get entity IDs from the active engagement config."""
        eng = get_active_engagement()
        return eng.entity_ids()

    def _get_reported_ebitda(self, entity_id: str) -> float:
        """Sum pnl.ebitda across all 2025 quarters for an entity."""
        placeholders = ", ".join(["%s"] * len(_ANNUAL_PERIODS))
        sql = f"""
            SELECT DISTINCT ON (entity_id, concept, period)
                   period, value
            FROM semantic_triples
            WHERE tenant_id = %s AND is_active = true
              AND concept = 'pnl.ebitda' AND entity_id = %s
              AND property = 'amount'
              AND period IN ({placeholders})
            ORDER BY entity_id, concept, period, created_at DESC
        """
        params = [self.tenant_id, entity_id] + _ANNUAL_PERIODS
        rows = self._query(sql, params)

        if not rows:
            raise ValueError(
                f"No pnl.ebitda triples found for entity_id='{entity_id}' "
                f"in periods {_ANNUAL_PERIODS} — "
                f"tenant_id='{self.tenant_id}', run_id='{self.run_id}'"
            )

        if len(rows) != len(_ANNUAL_PERIODS):
            found_periods = [r["period"] for r in rows]
            missing = set(_ANNUAL_PERIODS) - set(found_periods)
            raise ValueError(
                f"Incomplete pnl.ebitda data for entity_id='{entity_id}': "
                f"found {len(rows)}/{len(_ANNUAL_PERIODS)} quarters, "
                f"missing {missing}"
            )

        return sum(_to_float(r["value"]) for r in rows)

    def _get_adjustment_triples(self, entity_id: str) -> list[dict]:
        """Fetch all ebitda_adjustment.* triples for an entity, grouped by concept.

        Returns list of dicts, one per adjustment concept, with all properties.
        Raises ValueError if no triples found.
        """
        sql = """
            SELECT DISTINCT ON (entity_id, concept, property)
                   concept, property, value
            FROM semantic_triples
            WHERE tenant_id = %s AND is_active = true
              AND concept LIKE 'ebitda_adjustment.%%'
              AND entity_id = %s
            ORDER BY entity_id, concept, property, created_at DESC
        """
        rows = self._query(sql, [self.tenant_id, entity_id])

        if not rows:
            raise ValueError(
                f"No ebitda_adjustment triples found for entity_id='{entity_id}' — "
                f"tenant_id='{self.tenant_id}', run_id='{self.run_id}'. "
                f"EBITDA adjustment triples must be seeded before bridge can be produced."
            )

        # Group by concept
        grouped: dict[str, dict[str, str]] = {}
        for row in rows:
            concept = row["concept"]
            if concept not in grouped:
                grouped[concept] = {}
            grouped[concept][row["property"]] = row["value"]

        # Build adjustment list
        adjustments = []
        for concept in sorted(grouped.keys()):
            props = grouped[concept]
            lever = _LEVER_MAP.get(concept)
            if lever is None:
                raise ValueError(
                    f"Unknown ebitda_adjustment concept '{concept}' — "
                    f"not in lever classification map"
                )

            # Extract the sub-name from the concept (e.g., "facility_consolidation")
            name = concept.split(".", 1)[1].replace("_", " ").title()

            adjustments.append({
                "name": name,
                "concept": concept,
                "lever": lever,
                "amount": round(_to_float(props["amount_current"]), 2),
                "amount_low": round(_to_float(props["amount_low"]), 2),
                "amount_high": round(_to_float(props["amount_high"]), 2),
                "confidence": round(_to_float(props["confidence"]), 2),
                "rationale": _to_str(props.get("rationale", "")),
                "support_reference": _to_str(props.get("support_reference", "")),
            })

        return adjustments

    def get_bridge(self, entity_id: str | None = None) -> dict:
        """
        Get EBITDA bridge for one entity or combined.
        If entity_id is None, produces combined bridge.

        Returns:
        {
            "reported_ebitda": float,
            "adjustments": [
                {"name": str, "concept": str, "lever": str, "amount": float,
                 "confidence": float, "rationale": str}
            ],
            "total_adjustments": float,
            "adjusted_ebitda": float,
            "by_lever": {
                "normalization": float,
                "cost_reduction": float,
                "synergy": float
            }
        }
        """
        entity_a, entity_b = self._get_entities()

        if entity_id is not None:
            # Single entity bridge
            reported = self._get_reported_ebitda(entity_id)
            adjustments = self._get_adjustment_triples(entity_id)
        else:
            # Combined bridge — sum both entities
            reported_a = self._get_reported_ebitda(entity_a)
            reported_b = self._get_reported_ebitda(entity_b)
            reported = round(reported_a + reported_b, 2)

            adj_a = self._get_adjustment_triples(entity_a)
            adj_b = self._get_adjustment_triples(entity_b)

            # Merge adjustments by concept (sum amounts)
            adj_map: dict[str, dict] = {}
            for adj in adj_a + adj_b:
                concept = adj["concept"]
                if concept not in adj_map:
                    adj_map[concept] = {
                        "name": adj["name"],
                        "concept": concept,
                        "lever": adj["lever"],
                        "amount": 0.0,
                        "amount_low": 0.0,
                        "amount_high": 0.0,
                        "confidence": adj["confidence"],
                        "rationale": adj["rationale"],
                        "support_reference": adj["support_reference"],
                    }
                adj_map[concept]["amount"] = round(
                    adj_map[concept]["amount"] + adj["amount"], 2
                )
                adj_map[concept]["amount_low"] = round(
                    adj_map[concept]["amount_low"] + adj["amount_low"], 2
                )
                adj_map[concept]["amount_high"] = round(
                    adj_map[concept]["amount_high"] + adj["amount_high"], 2
                )

            adjustments = [adj_map[c] for c in sorted(adj_map.keys())]

        total_adjustments = round(sum(a["amount"] for a in adjustments), 2)
        adjusted_ebitda = round(reported + total_adjustments, 2)

        by_lever: dict[str, float] = {
            "normalization": 0.0,
            "cost_reduction": 0.0,
            "synergy": 0.0,
        }
        for adj in adjustments:
            by_lever[adj["lever"]] = round(
                by_lever[adj["lever"]] + adj["amount"], 2
            )

        return {
            "reported_ebitda": round(reported, 2),
            "adjustments": adjustments,
            "total_adjustments": total_adjustments,
            "adjusted_ebitda": adjusted_ebitda,
            "by_lever": by_lever,
        }

    def get_bridge_comparison(self) -> dict:
        """
        Side-by-side bridge for both entities.
        Returns {"entity_a": bridge_dict, "entity_b": bridge_dict, "combined": bridge_dict}
        """
        entity_a, entity_b = self._get_entities()
        return {
            "entity_a": self.get_bridge(entity_a),
            "entity_b": self.get_bridge(entity_b),
            "combined": self.get_bridge(None),
        }

    def get_adjustment_detail(self, adjustment_concept: str) -> dict:
        """
        Detailed view of one adjustment: amount_current, amount_low, amount_high,
        confidence, rationale, support_reference.

        Returns combined view across both entities.
        """
        entity_a, entity_b = self._get_entities()

        sql = """
            SELECT DISTINCT ON (entity_id, concept, property)
                   entity_id, property, value
            FROM semantic_triples
            WHERE tenant_id = %s AND is_active = true
              AND concept = %s
              AND entity_id != 'combined'
            ORDER BY entity_id, concept, property, created_at DESC
        """
        rows = self._query(sql, [self.tenant_id, adjustment_concept])

        if not rows:
            raise ValueError(
                f"No triples found for adjustment concept '{adjustment_concept}' — "
                f"tenant_id='{self.tenant_id}', run_id='{self.run_id}'"
            )

        # Group by entity
        by_entity: dict[str, dict[str, str]] = {}
        for row in rows:
            eid = row["entity_id"]
            if eid not in by_entity:
                by_entity[eid] = {}
            by_entity[eid][row["property"]] = row["value"]

        result: dict = {"concept": adjustment_concept, "entities": {}}
        for eid in sorted(by_entity.keys()):
            props = by_entity[eid]
            result["entities"][eid] = {
                "amount_current": round(_to_float(props.get("amount_current", "0")), 2),
                "amount_low": round(_to_float(props.get("amount_low", "0")), 2),
                "amount_high": round(_to_float(props.get("amount_high", "0")), 2),
                "confidence": round(_to_float(props.get("confidence", "0")), 2),
                "rationale": _to_str(props.get("rationale", "")),
                "support_reference": _to_str(props.get("support_reference", "")),
            }

        return result

    def get_sensitivity_matrix(self) -> list[dict]:
        """
        Shows adjusted EBITDA under different confidence-weighted scenarios.
        Base case = amount_current, Low case = amount_low, High case = amount_high.
        """
        entity_a, entity_b = self._get_entities()

        # Get reported EBITDA
        reported_a = self._get_reported_ebitda(entity_a)
        reported_b = self._get_reported_ebitda(entity_b)
        reported_combined = round(reported_a + reported_b, 2)

        # Get adjustments for both entities
        adj_a = self._get_adjustment_triples(entity_a)
        adj_b = self._get_adjustment_triples(entity_b)

        # Merge by concept
        adj_map: dict[str, dict] = {}
        for adj in adj_a + adj_b:
            concept = adj["concept"]
            if concept not in adj_map:
                adj_map[concept] = {
                    "concept": concept,
                    "name": adj["name"],
                    "lever": adj["lever"],
                    "confidence": adj["confidence"],
                    "base": 0.0,
                    "low": 0.0,
                    "high": 0.0,
                }
            adj_map[concept]["base"] = round(
                adj_map[concept]["base"] + adj["amount"], 2
            )
            adj_map[concept]["low"] = round(
                adj_map[concept]["low"] + adj["amount_low"], 2
            )
            adj_map[concept]["high"] = round(
                adj_map[concept]["high"] + adj["amount_high"], 2
            )

        matrix = []
        for concept in sorted(adj_map.keys()):
            entry = adj_map[concept]
            matrix.append({
                "concept": concept,
                "name": entry["name"],
                "lever": entry["lever"],
                "confidence": entry["confidence"],
                "base": entry["base"],
                "low": entry["low"],
                "high": entry["high"],
                "adjusted_ebitda_base": round(reported_combined + entry["base"], 2),
                "adjusted_ebitda_low": round(reported_combined + entry["low"], 2),
                "adjusted_ebitda_high": round(reported_combined + entry["high"], 2),
            })

        return matrix
