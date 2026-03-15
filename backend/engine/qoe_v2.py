"""
QualityOfEarningsV2 — QoE analysis derived from EBITDA bridge and financial triples.

QoE assesses how reliable the reported earnings are by analyzing:
1. Adjustment magnitude relative to EBITDA
2. Confidence distribution of adjustments
3. Revenue quality (recurring vs non-recurring mix)
4. Margin trends over time

All data sourced from semantic_triples in PG — no JSON files.
"""

from backend.core.db import get_connection
from backend.engine.ebitda_bridge_v2 import EBITDABridgeV2
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

# All periods for trend analysis (12 quarters: 2023-Q1 through 2025-Q4)
_ALL_PERIODS = [
    f"{year}-Q{q}" for year in (2023, 2024, 2025) for q in (1, 2, 3, 4)
]

# 2025 quarters for annual revenue
_ANNUAL_PERIODS = ["2025-Q1", "2025-Q2", "2025-Q3", "2025-Q4"]


def _to_float(value) -> float:
    """Convert a JSONB value to float."""
    return float(value)


class QualityOfEarningsV2:
    """
    Quality of Earnings analysis derived from EBITDA bridge and financial triples.

    QoE assesses how reliable the reported earnings are by analyzing:
    1. Adjustment magnitude relative to EBITDA
    2. Confidence distribution of adjustments
    3. Revenue quality (recurring vs non-recurring mix)
    4. Margin trends over time
    """

    def __init__(self, tenant_id: str, run_id: str):
        self.tenant_id = tenant_id
        self.run_id = run_id
        self._bridge_engine = EBITDABridgeV2(tenant_id, run_id)

    def _query(self, sql: str, params: list) -> list[dict]:
        """Execute a parameterized query and return rows as dicts."""
        with get_connection() as conn:
            if conn is None:
                raise RuntimeError(
                    "QualityOfEarningsV2: database connection unavailable. "
                    "Check DATABASE_URL and Supabase connectivity."
                )
            with conn.cursor() as cur:
                cur.execute(sql, params)
                columns = [desc[0] for desc in cur.description]
                return [dict(zip(columns, row)) for row in cur.fetchall()]

    def _get_metric(self, concept: str, entity_id: str, period: str) -> float | None:
        """Get a single metric value, returning None if not found."""
        sql = """
            SELECT value
            FROM semantic_triples
            WHERE tenant_id = %s AND run_id = %s AND is_active = true
              AND concept = %s AND entity_id = %s AND period = %s
              AND property = 'amount'
        """
        rows = self._query(sql, [self.tenant_id, self.run_id, concept, entity_id, period])
        if not rows:
            return None
        return _to_float(rows[0]["value"])

    def _get_revenue_streams(self, entity_id: str) -> list[dict]:
        """Get all revenue.* concepts for an entity across 2025 quarters."""
        placeholders = ", ".join(["%s"] * len(_ANNUAL_PERIODS))
        sql = f"""
            SELECT concept, SUM((value #>> '{{}}')::float) as total_value
            FROM semantic_triples
            WHERE tenant_id = %s AND run_id = %s AND is_active = true
              AND concept LIKE 'revenue.%%'
              AND entity_id = %s
              AND property = 'amount'
              AND period IN ({placeholders})
            GROUP BY concept
            ORDER BY SUM((value #>> '{{}}')::float) DESC
        """
        params = [self.tenant_id, self.run_id, entity_id] + _ANNUAL_PERIODS
        return self._query(sql, params)

    def _get_margin_trend(self, entity_id: str) -> list[dict]:
        """Get EBITDA margin trend across all available periods."""
        # Get revenue.total and pnl.ebitda for each period
        sql = """
            SELECT period, concept, value
            FROM semantic_triples
            WHERE tenant_id = %s AND run_id = %s AND is_active = true
              AND concept IN ('revenue.total', 'pnl.ebitda')
              AND entity_id = %s
              AND property = 'amount'
            ORDER BY period, concept
        """
        rows = self._query(sql, [self.tenant_id, self.run_id, entity_id])

        # Group by period
        by_period: dict[str, dict[str, float]] = {}
        for row in rows:
            period = row["period"]
            if period not in by_period:
                by_period[period] = {}
            by_period[period][row["concept"]] = _to_float(row["value"])

        # Calculate margins
        trend = []
        for period in sorted(by_period.keys()):
            data = by_period[period]
            rev = data.get("revenue.total")
            ebitda = data.get("pnl.ebitda")
            if rev is not None and ebitda is not None and rev != 0:
                margin = round(ebitda / rev * 100, 2)
                trend.append({"period": period, "ebitda_margin": margin})

        return trend

    def get_qoe_summary(self, entity_id: str) -> dict:
        """
        Returns:
        {
            "entity_id": str,
            "reported_ebitda": float,
            "adjusted_ebitda": float,
            "adjustment_pct": float,  # total_adjustments / reported_ebitda
            "confidence_weighted_ebitda": float,
            "revenue_quality": {
                "total_revenue": float,
                "by_stream": [{"concept": str, "value": float, "pct": float}]
            },
            "margin_trend": [{"period": str, "ebitda_margin": float}],
            "risk_factors": [str]
        }
        """
        bridge = self._bridge_engine.get_bridge(entity_id)

        reported = bridge["reported_ebitda"]
        adjusted = bridge["adjusted_ebitda"]
        total_adj = bridge["total_adjustments"]

        if reported == 0:
            raise ValueError(
                f"Reported EBITDA is zero for entity_id='{entity_id}' — "
                f"cannot compute QoE adjustment percentage"
            )

        adjustment_pct = round(total_adj / reported * 100, 2)

        # Confidence-weighted EBITDA: sum(amount * confidence) for each adjustment
        conf_weighted_adj = sum(
            a["amount"] * a["confidence"] for a in bridge["adjustments"]
        )
        confidence_weighted_ebitda = round(reported + conf_weighted_adj, 2)

        # Revenue quality
        streams = self._get_revenue_streams(entity_id)
        total_revenue = 0.0
        by_stream = []
        for s in streams:
            val = round(float(s["total_value"]), 2)
            if s["concept"] == "revenue.total":
                total_revenue = val
            else:
                by_stream.append({
                    "concept": s["concept"],
                    "value": val,
                })

        # Calculate percentages
        for item in by_stream:
            item["pct"] = round(item["value"] / total_revenue * 100, 2) if total_revenue != 0 else 0.0

        # Margin trend
        margin_trend = self._get_margin_trend(entity_id)

        # Risk factors
        risk_factors = self._compute_risk_factors(bridge, adjustment_pct, margin_trend)

        return {
            "entity_id": entity_id,
            "reported_ebitda": reported,
            "adjusted_ebitda": adjusted,
            "adjustment_pct": adjustment_pct,
            "confidence_weighted_ebitda": confidence_weighted_ebitda,
            "revenue_quality": {
                "total_revenue": total_revenue,
                "by_stream": by_stream,
            },
            "margin_trend": margin_trend,
            "risk_factors": risk_factors,
        }

    def get_combined_qoe(self) -> dict:
        """Combined QoE for both entities."""
        entity_a, entity_b = self._bridge_engine._get_entities()
        return {
            "entity_a": self.get_qoe_summary(entity_a),
            "entity_b": self.get_qoe_summary(entity_b),
            "combined": self._get_combined_summary(entity_a, entity_b),
        }

    def _get_combined_summary(self, entity_a: str, entity_b: str) -> dict:
        """Produce combined QoE summary."""
        bridge = self._bridge_engine.get_bridge(None)  # combined

        reported = bridge["reported_ebitda"]
        adjusted = bridge["adjusted_ebitda"]
        total_adj = bridge["total_adjustments"]

        if reported == 0:
            raise ValueError(
                "Combined reported EBITDA is zero — cannot compute QoE"
            )

        adjustment_pct = round(total_adj / reported * 100, 2)

        conf_weighted_adj = sum(
            a["amount"] * a["confidence"] for a in bridge["adjustments"]
        )
        confidence_weighted_ebitda = round(reported + conf_weighted_adj, 2)

        # Combined margin trend
        margin_trend_a = self._get_margin_trend(entity_a)
        margin_trend_b = self._get_margin_trend(entity_b)

        # Merge by period
        margin_map_a = {m["period"]: m["ebitda_margin"] for m in margin_trend_a}
        margin_map_b = {m["period"]: m["ebitda_margin"] for m in margin_trend_b}
        all_periods = sorted(set(margin_map_a.keys()) | set(margin_map_b.keys()))

        # For combined margin we need raw revenue and EBITDA, not just percentages
        # Use a simplified weighted average approach
        combined_trend = []
        for period in all_periods:
            rev_a = self._get_metric("revenue.total", entity_a, period)
            rev_b = self._get_metric("revenue.total", entity_b, period)
            ebitda_a = self._get_metric("pnl.ebitda", entity_a, period)
            ebitda_b = self._get_metric("pnl.ebitda", entity_b, period)

            if all(v is not None for v in (rev_a, rev_b, ebitda_a, ebitda_b)):
                total_rev = rev_a + rev_b
                total_ebitda = ebitda_a + ebitda_b
                if total_rev != 0:
                    combined_trend.append({
                        "period": period,
                        "ebitda_margin": round(total_ebitda / total_rev * 100, 2),
                    })

        risk_factors = self._compute_risk_factors(bridge, adjustment_pct, combined_trend)

        return {
            "entity_id": "combined",
            "reported_ebitda": reported,
            "adjusted_ebitda": adjusted,
            "adjustment_pct": adjustment_pct,
            "confidence_weighted_ebitda": confidence_weighted_ebitda,
            "margin_trend": combined_trend,
            "risk_factors": risk_factors,
        }

    @staticmethod
    def _compute_risk_factors(
        bridge: dict,
        adjustment_pct: float,
        margin_trend: list[dict],
    ) -> list[str]:
        """Identify risk factors from the bridge and margin data."""
        risks = []

        # Large adjustment magnitude
        if abs(adjustment_pct) > 20:
            risks.append(
                f"Total adjustments represent {adjustment_pct:.1f}% of reported EBITDA — "
                f"high adjustment magnitude raises reliability concerns"
            )

        # Low confidence adjustments
        low_conf = [a for a in bridge["adjustments"] if a["confidence"] < 0.70]
        if low_conf:
            names = ", ".join(a["name"] for a in low_conf)
            risks.append(
                f"{len(low_conf)} adjustment(s) have confidence below 0.70: {names}"
            )

        # Declining margins
        if len(margin_trend) >= 2:
            recent = margin_trend[-1]["ebitda_margin"]
            prior = margin_trend[-2]["ebitda_margin"]
            if recent < prior - 1.0:
                risks.append(
                    f"EBITDA margin declining: {prior:.1f}% → {recent:.1f}% "
                    f"({margin_trend[-2]['period']} → {margin_trend[-1]['period']})"
                )

        # Synergy dominance
        by_lever = bridge.get("by_lever", {})
        total_adj = bridge.get("total_adjustments", 0)
        synergy = by_lever.get("synergy", 0)
        if total_adj > 0 and synergy / total_adj > 0.60:
            risks.append(
                f"Synergies represent {synergy / total_adj * 100:.0f}% of total adjustments — "
                f"high dependency on forward-looking estimates"
            )

        return risks
