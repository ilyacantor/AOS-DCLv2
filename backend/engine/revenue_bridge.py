"""
RevenueBridgeV2 — revenue bridge analysis from semantic_triples.

Period-over-period and entity comparison revenue walks.
All data sourced from PG via TripleQueryResolver.
"""

from backend.engine.query_resolver_v2 import TripleQueryResolver
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)


class RevenueBridgeV2:
    """
    Revenue bridge analysis: period-over-period and entity comparison.
    All data from semantic_triples.
    """

    def __init__(self, tenant_id: str, run_id: str):
        self.tenant_id = tenant_id
        self.run_id = run_id
        self._resolver = TripleQueryResolver(tenant_id, run_id)

    def get_revenue_bridge(self, entity_id: str,
                           period_from: str, period_to: str) -> dict:
        """
        Revenue walk from period_from to period_to.
        Breaks down by revenue sub-component changes.

        Returns:
        {
            "entity_id": str,
            "from_period": str, "to_period": str,
            "from_total": float, "to_total": float,
            "total_change": float, "pct_change": float,
            "by_stream": [
                {"concept": str, "from": float, "to": float, "delta": float, "pct": float}
            ]
        }
        """
        from_items = self._resolver.get_domain("revenue", entity_id, period_from)
        to_items = self._resolver.get_domain("revenue", entity_id, period_to)

        from_dict = {item["concept"]: item["value"] for item in from_items}
        to_dict = {item["concept"]: item["value"] for item in to_items}

        from_total = from_dict.get("revenue.total")
        to_total = to_dict.get("revenue.total")

        if from_total is None:
            raise ValueError(
                f"Revenue bridge: revenue.total not found for entity_id='{entity_id}', "
                f"period='{period_from}' in semantic_triples for "
                f"tenant_id='{self.tenant_id}', run_id='{self.run_id}'"
            )
        if to_total is None:
            raise ValueError(
                f"Revenue bridge: revenue.total not found for entity_id='{entity_id}', "
                f"period='{period_to}' in semantic_triples for "
                f"tenant_id='{self.tenant_id}', run_id='{self.run_id}'"
            )

        total_change = round(to_total - from_total, 2)
        pct_change = round(total_change / from_total * 100, 2) if from_total != 0 else 0.0

        # Build by-stream breakdown (all revenue sub-concepts except total)
        all_concepts = sorted(set(from_dict.keys()) | set(to_dict.keys()))
        by_stream: list[dict] = []
        for concept in all_concepts:
            if concept == "revenue.total":
                continue
            f_val = from_dict.get(concept, 0.0)
            t_val = to_dict.get(concept, 0.0)
            delta = round(t_val - f_val, 2)
            pct = round(delta / f_val * 100, 2) if f_val != 0 else 0.0
            by_stream.append({
                "concept": concept,
                "from": f_val,
                "to": t_val,
                "delta": delta,
                "pct": pct,
            })

        return {
            "entity_id": entity_id,
            "from_period": period_from,
            "to_period": period_to,
            "from_total": from_total,
            "to_total": to_total,
            "total_change": total_change,
            "pct_change": pct_change,
            "by_stream": by_stream,
        }

    def get_yoy_bridge(self, entity_id: str, period: str) -> dict:
        """
        Year-over-year revenue bridge (e.g., 2025-Q1 vs 2024-Q1).
        """
        prior_period = _prior_year_period(period)
        return self.get_revenue_bridge(entity_id, prior_period, period)

    def get_combined_revenue_bridge(self, period_from: str, period_to: str) -> dict:
        """
        Combined (Entity A + B) revenue bridge.
        """
        entities = self._resolver._get_entities()
        if len(entities) < 2:
            raise ValueError(
                f"Combined revenue bridge requires at least 2 entities, "
                f"found {len(entities)}: {entities} for "
                f"tenant_id='{self.tenant_id}', run_id='{self.run_id}'"
            )

        bridge_a = self.get_revenue_bridge(entities[0], period_from, period_to)
        bridge_b = self.get_revenue_bridge(entities[1], period_from, period_to)

        combined_from = round(bridge_a["from_total"] + bridge_b["from_total"], 2)
        combined_to = round(bridge_a["to_total"] + bridge_b["to_total"], 2)
        combined_change = round(combined_to - combined_from, 2)
        combined_pct = round(combined_change / combined_from * 100, 2) if combined_from != 0 else 0.0

        # Merge by_stream across entities
        stream_map: dict[str, dict] = {}
        for stream in bridge_a["by_stream"] + bridge_b["by_stream"]:
            concept = stream["concept"]
            if concept not in stream_map:
                stream_map[concept] = {"concept": concept, "from": 0.0, "to": 0.0, "delta": 0.0}
            stream_map[concept]["from"] = round(stream_map[concept]["from"] + stream["from"], 2)
            stream_map[concept]["to"] = round(stream_map[concept]["to"] + stream["to"], 2)
            stream_map[concept]["delta"] = round(stream_map[concept]["delta"] + stream["delta"], 2)

        by_stream: list[dict] = []
        for concept in sorted(stream_map.keys()):
            entry = stream_map[concept]
            entry["pct"] = round(entry["delta"] / entry["from"] * 100, 2) if entry["from"] != 0 else 0.0
            by_stream.append(entry)

        return {
            "entity_ids": entities,
            "from_period": period_from,
            "to_period": period_to,
            "from_total": combined_from,
            "to_total": combined_to,
            "total_change": combined_change,
            "pct_change": combined_pct,
            "by_stream": by_stream,
            "entity_bridges": {
                entities[0]: bridge_a,
                entities[1]: bridge_b,
            },
        }


def _prior_year_period(period: str) -> str:
    """Convert a period like '2025-Q1' to its prior year equivalent '2024-Q1'."""
    parts = period.split("-", 1)
    if len(parts) != 2:
        raise ValueError(
            f"Cannot compute prior year for period '{period}'. "
            f"Expected format 'YYYY-Q#' (e.g. '2025-Q1')."
        )
    year = int(parts[0])
    return f"{year - 1}-{parts[1]}"
