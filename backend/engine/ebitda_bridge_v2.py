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

# Lever classification for each adjustment concept (keyed by base 2-segment concept)
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

# Lifecycle stage ordering — higher number = later in diligence process
STAGE_ORDER = {
    "management": 0,
    "initial_diligence": 1,
    "confirmatory": 2,
    "agreed": 3,
    "post_close": 4,
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


def _parse_concept(concept: str) -> tuple[str, str]:
    """Parse a concept into (base_concept, lifecycle_stage).

    3-segment concepts: ebitda_adjustment.category.stage -> (ebitda_adjustment.category, stage)
    2-segment concepts: ebitda_adjustment.category -> (ebitda_adjustment.category, initial_diligence)
    """
    parts = concept.split(".")
    if len(parts) >= 3:
        base = ".".join(parts[:2])
        stage = parts[2]
        return base, stage
    # Legacy 2-segment: treat as initial_diligence (locked decision)
    return concept, "initial_diligence"


def _derive_trend(current: float | None, prior: float | None) -> str:
    """Derive trend from current and prior amounts."""
    if prior is None:
        return "neutral"
    if current is None:
        return "neutral"
    if current > prior:
        return "increasing"
    if current < prior:
        return "decreasing"
    return "stable"


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
        """Fetch all ebitda_adjustment.* triples for an entity, lifecycle-aware.

        Groups triples by base concept (2-segment), collects all lifecycle
        stages per concept, and pivots into the output format with
        diligence_amount, prior_amount, trend, and lifecycle_history.

        Returns list of dicts, one per base adjustment concept.
        Raises ValueError if no triples found.
        """
        # DISTINCT ON deduplicates across runs per (entity, full-concept, property).
        # The full 3-segment concept distinguishes lifecycle stages, so each
        # stage's properties are returned separately.
        sql = """
            SELECT DISTINCT ON (entity_id, concept, property)
                   concept, property, value, period
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

        # Group by (base_concept, stage) -> {property: value}
        stage_data: dict[str, dict[str, dict[str, object]]] = {}
        period_map: dict[str, str | None] = {}
        for row in rows:
            base_concept, stage = _parse_concept(row["concept"])
            if base_concept not in stage_data:
                stage_data[base_concept] = {}
            if stage not in stage_data[base_concept]:
                stage_data[base_concept][stage] = {}
            stage_data[base_concept][stage][row["property"]] = row["value"]
            if row.get("period"):
                period_map[base_concept] = row["period"]

        # Build adjustment list — one entry per base concept
        adjustments = []
        for base_concept in sorted(stage_data.keys()):
            stages = stage_data[base_concept]

            lever = _LEVER_MAP.get(base_concept)
            if lever is None:
                raise ValueError(
                    f"Unknown ebitda_adjustment concept '{base_concept}' — "
                    f"not in lever classification map"
                )

            # Build lifecycle_history ordered by stage progression
            lifecycle_history = []
            for stage_name in sorted(stages.keys(), key=lambda s: STAGE_ORDER.get(s, 99)):
                props = stages[stage_name]
                lifecycle_history.append({
                    "stage": stage_name,
                    "amount": round(_to_float(props.get("amount_current", 0)), 2),
                    "amount_low": round(_to_float(props.get("amount_low", 0)), 2),
                    "amount_high": round(_to_float(props.get("amount_high", 0)), 2),
                    "confidence": round(_to_float(props.get("confidence", 0)), 2),
                })

            # Latest = highest STAGE_ORDER present
            latest_entry = lifecycle_history[-1]
            latest_stage = latest_entry["stage"]
            latest_props = stages[latest_stage]

            # Prior = one step before latest (if more than one stage)
            prior_entry = lifecycle_history[-2] if len(lifecycle_history) >= 2 else None

            # Diligence amount = management stage (LOI number)
            management_entry = next(
                (e for e in lifecycle_history if e["stage"] == "management"),
                None,
            )

            # Get display name from the triple's name property, falling back to concept
            name_from_triple = _to_str(latest_props.get("name", ""))
            if name_from_triple:
                display_name = name_from_triple.replace("_", " ").title()
            else:
                display_name = base_concept.split(".", 1)[1].replace("_", " ").title()

            current_amount = latest_entry["amount"]
            prior_amount = prior_entry["amount"] if prior_entry else None
            diligence_amount = management_entry["amount"] if management_entry else None

            adjustments.append({
                "name": display_name,
                "concept": base_concept,
                "lever": lever,
                "amount": current_amount,
                "diligence_amount": diligence_amount,
                "prior_amount": prior_amount,
                "trend": _derive_trend(current_amount, prior_amount),
                "amount_low": latest_entry["amount_low"],
                "amount_high": latest_entry["amount_high"],
                "confidence": latest_entry["confidence"],
                "lifecycle_stage": latest_stage,
                "period": period_map.get(base_concept),
                "period_type": _to_str(latest_props.get("period_type", "")),
                "rationale": _to_str(latest_props.get("rationale", "")),
                "support_reference": _to_str(latest_props.get("support_reference", "")),
                "lifecycle_history": lifecycle_history,
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
                 "diligence_amount": float|null, "prior_amount": float|null,
                 "trend": str, "confidence": float, "lifecycle_stage": str,
                 "lifecycle_history": [...], "rationale": str}
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

            # Merge adjustments by base concept (sum amounts)
            adj_map: dict[str, dict] = {}
            for adj in adj_a + adj_b:
                concept = adj["concept"]
                if concept not in adj_map:
                    adj_map[concept] = {
                        "name": adj["name"],
                        "concept": concept,
                        "lever": adj["lever"],
                        "amount": 0.0,
                        "diligence_amount": 0.0,
                        "prior_amount": 0.0,
                        "amount_low": 0.0,
                        "amount_high": 0.0,
                        "confidence": adj["confidence"],
                        "lifecycle_stage": adj["lifecycle_stage"],
                        "period": adj["period"],
                        "period_type": adj["period_type"],
                        "rationale": adj["rationale"],
                        "support_reference": adj["support_reference"],
                        "lifecycle_history": [],
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
                # Sum diligence/prior amounts across entities
                if adj["diligence_amount"] is not None:
                    if adj_map[concept]["diligence_amount"] is None:
                        adj_map[concept]["diligence_amount"] = 0.0
                    adj_map[concept]["diligence_amount"] = round(
                        adj_map[concept]["diligence_amount"] + adj["diligence_amount"], 2
                    )
                if adj["prior_amount"] is not None:
                    if adj_map[concept]["prior_amount"] is None:
                        adj_map[concept]["prior_amount"] = 0.0
                    adj_map[concept]["prior_amount"] = round(
                        adj_map[concept]["prior_amount"] + adj["prior_amount"], 2
                    )

                # Merge lifecycle histories by summing amounts per matching stage
                for entry in adj["lifecycle_history"]:
                    existing = next(
                        (h for h in adj_map[concept]["lifecycle_history"]
                         if h["stage"] == entry["stage"]),
                        None,
                    )
                    if existing:
                        existing["amount"] = round(existing["amount"] + entry["amount"], 2)
                        existing["amount_low"] = round(existing["amount_low"] + entry["amount_low"], 2)
                        existing["amount_high"] = round(existing["amount_high"] + entry["amount_high"], 2)
                    else:
                        adj_map[concept]["lifecycle_history"].append({
                            "stage": entry["stage"],
                            "amount": entry["amount"],
                            "amount_low": entry["amount_low"],
                            "amount_high": entry["amount_high"],
                            "confidence": entry["confidence"],
                        })

            # Sort lifecycle history and derive combined trend
            for entry in adj_map.values():
                entry["lifecycle_history"].sort(
                    key=lambda h: STAGE_ORDER.get(h["stage"], 99)
                )
                # Set combined diligence/prior to None if they stayed at 0.0
                # from initialization (i.e., no entity had that value)
                entry["trend"] = _derive_trend(
                    entry["amount"], entry["prior_amount"]
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
        Detailed view of one adjustment across both entities with lifecycle history.

        Accepts base concept (2-segment, e.g. 'ebitda_adjustment.facility_consolidation')
        and queries all matching lifecycle stages.

        Returns combined view with lifecycle history per entity.
        """
        entity_a, entity_b = self._get_entities()

        # Query all stages for this base concept (match 3-segment concepts)
        sql = """
            SELECT DISTINCT ON (entity_id, concept, property)
                   entity_id, concept, property, value
            FROM semantic_triples
            WHERE tenant_id = %s AND is_active = true
              AND (concept = %s OR concept LIKE %s)
              AND entity_id != 'combined'
            ORDER BY entity_id, concept, property, created_at DESC
        """
        like_pattern = adjustment_concept + ".%"
        rows = self._query(sql, [self.tenant_id, adjustment_concept, like_pattern])

        if not rows:
            raise ValueError(
                f"No triples found for adjustment concept '{adjustment_concept}' — "
                f"tenant_id='{self.tenant_id}', run_id='{self.run_id}'"
            )

        # Group by entity -> stage -> {property: value}
        by_entity: dict[str, dict[str, dict[str, object]]] = {}
        for row in rows:
            eid = row["entity_id"]
            _, stage = _parse_concept(row["concept"])
            if eid not in by_entity:
                by_entity[eid] = {}
            if stage not in by_entity[eid]:
                by_entity[eid][stage] = {}
            by_entity[eid][stage][row["property"]] = row["value"]

        result: dict = {"concept": adjustment_concept, "entities": {}}
        for eid in sorted(by_entity.keys()):
            stages = by_entity[eid]

            # Build lifecycle history ordered by stage
            lifecycle_history = []
            for stage_name in sorted(stages.keys(), key=lambda s: STAGE_ORDER.get(s, 99)):
                props = stages[stage_name]
                lifecycle_history.append({
                    "stage": stage_name,
                    "amount_current": round(_to_float(props.get("amount_current", "0")), 2),
                    "amount_low": round(_to_float(props.get("amount_low", "0")), 2),
                    "amount_high": round(_to_float(props.get("amount_high", "0")), 2),
                    "confidence": round(_to_float(props.get("confidence", "0")), 2),
                    "rationale": _to_str(props.get("rationale", "")),
                    "support_reference": _to_str(props.get("support_reference", "")),
                })

            # Latest stage summary
            latest = lifecycle_history[-1] if lifecycle_history else {}
            result["entities"][eid] = {
                "amount_current": latest.get("amount_current", 0.0),
                "amount_low": latest.get("amount_low", 0.0),
                "amount_high": latest.get("amount_high", 0.0),
                "confidence": latest.get("confidence", 0.0),
                "rationale": latest.get("rationale", ""),
                "support_reference": latest.get("support_reference", ""),
                "lifecycle_stage": lifecycle_history[-1]["stage"] if lifecycle_history else None,
                "lifecycle_history": lifecycle_history,
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
