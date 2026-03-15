"""
CrossSellEngineV2 — cross-sell opportunity scoring from semantic_triples.

Identifies services that Entity A offers to shared customers
that Entity B could also offer (and vice versa).

All data sourced from PG semantic_triples — no JSON files.
"""

from backend.core.db import get_connection
from backend.engine.overlap_v2 import OverlapEngineV2
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)


def _to_float(value, context: str = "") -> float:
    """Convert a JSONB value to float. Raises on failure — financial data must not silently become zero."""
    if value is None:
        raise ValueError(f"Null numeric value{' in ' + context if context else ''}")
    try:
        return float(value)
    except (TypeError, ValueError) as e:
        raise ValueError(f"Cannot convert {value!r} to numeric{' in ' + context if context else ''}: {e}")


class CrossSellEngineV2:
    """
    Cross-sell opportunity scoring from overlap analysis.

    Identifies services that Entity A offers to shared customers
    that Entity B could also offer (and vice versa).
    """

    def __init__(self, tenant_id: str, run_id: str):
        self.tenant_id = tenant_id
        self.run_id = run_id
        self._overlap_engine = OverlapEngineV2(tenant_id, run_id)

    def _query(self, sql: str, params: list) -> list[dict]:
        """Execute a parameterized query and return rows as dicts."""
        with get_connection() as conn:
            if conn is None:
                raise RuntimeError(
                    "CrossSellEngineV2: database connection unavailable. "
                    "Check DATABASE_URL and Supabase connectivity."
                )
            with conn.cursor() as cur:
                cur.execute(sql, params)
                columns = [desc[0] for desc in cur.description]
                return [dict(zip(columns, row)) for row in cur.fetchall()]

    def _get_entities(self) -> tuple[str, str]:
        """Get the two entity_ids, ordered descending (entity_a first)."""
        return self._overlap_engine._get_entities()

    def _get_service_portfolio(self, entity_id: str) -> list[dict]:
        """
        Get service portfolio for an entity from service.* triples.
        Returns list of {"concept": str, "typical_acv": float, "description": str}.
        """
        sql = """
            SELECT concept, property, value
            FROM semantic_triples
            WHERE tenant_id = %s AND run_id = %s AND is_active = true
              AND split_part(concept, '.', 1) = 'service'
              AND entity_id = %s
            ORDER BY concept, property
        """
        rows = self._query(sql, [self.tenant_id, self.run_id, entity_id])

        # Group by concept
        services: dict[str, dict] = {}
        for row in rows:
            concept = row["concept"]
            if concept not in services:
                services[concept] = {"concept": concept}
            services[concept][row["property"]] = row["value"]

        result = []
        for concept, props in sorted(services.items()):
            result.append({
                "concept": concept,
                "typical_acv": _to_float(props.get("typical_acv", 0), context=f"service {concept} typical_acv"),
                "description": str(props.get("description", "")),
            })
        return result

    def get_cross_sell_opportunities(self) -> list[dict]:
        """
        For each overlapping customer, identify services from one entity
        that could be offered by the other.

        Uses service.* triples to determine entity service portfolios.

        Returns list of:
        {
            "customer": str,
            "current_entity": str,
            "opportunity_entity": str,
            "service": str,
            "typical_acv": float,
            "rationale": str
        }
        """
        entity_a, entity_b = self._get_entities()

        # Get service portfolios
        a_services = self._get_service_portfolio(entity_a)
        b_services = self._get_service_portfolio(entity_b)

        if not a_services:
            raise ValueError(
                f"CrossSellEngineV2: no service.* triples found for entity_id='{entity_a}' "
                f"in tenant_id='{self.tenant_id}', run_id='{self.run_id}'"
            )
        if not b_services:
            raise ValueError(
                f"CrossSellEngineV2: no service.* triples found for entity_id='{entity_b}' "
                f"in tenant_id='{self.tenant_id}', run_id='{self.run_id}'"
            )

        # Determine which services are unique to each entity
        a_concepts = {s["concept"] for s in a_services}
        b_concepts = {s["concept"] for s in b_services}
        a_only = a_concepts - b_concepts
        b_only = b_concepts - a_concepts

        # Get overlapping customers
        shared_customers = self._overlap_engine._find_overlapping_concepts("customer")
        if not shared_customers:
            return []

        opportunities = []

        # Direction a_to_b: Entity A's unique services → shared customers via entity B
        for svc in a_services:
            if svc["concept"] not in a_only:
                continue
            service_name = svc["concept"].split(".", 1)[1] if "." in svc["concept"] else svc["concept"]
            for customer_concept in shared_customers:
                customer_name = customer_concept.split(".", 1)[1] if "." in customer_concept else customer_concept
                opportunities.append({
                    "customer": customer_name,
                    "current_entity": entity_a,
                    "opportunity_entity": entity_b,
                    "service": service_name,
                    "typical_acv": svc["typical_acv"],
                    "rationale": (
                        f"{entity_a} offers {service_name} "
                        f"(typical ACV ${svc['typical_acv']}M). "
                        f"{customer_name} is a shared customer — "
                        f"{entity_b} could cross-sell this service."
                    ),
                })

        # Direction b_to_a: Entity B's unique services → shared customers via entity A
        for svc in b_services:
            if svc["concept"] not in b_only:
                continue
            service_name = svc["concept"].split(".", 1)[1] if "." in svc["concept"] else svc["concept"]
            for customer_concept in shared_customers:
                customer_name = customer_concept.split(".", 1)[1] if "." in customer_concept else customer_concept
                opportunities.append({
                    "customer": customer_name,
                    "current_entity": entity_b,
                    "opportunity_entity": entity_a,
                    "service": service_name,
                    "typical_acv": svc["typical_acv"],
                    "rationale": (
                        f"{entity_b} offers {service_name} "
                        f"(typical ACV ${svc['typical_acv']}M). "
                        f"{customer_name} is a shared customer — "
                        f"{entity_a} could cross-sell this service."
                    ),
                })

        logger.info(
            "CrossSellEngineV2: %d opportunities (%d a_to_b, %d b_to_a) "
            "for tenant=%s, run=%s",
            len(opportunities),
            sum(1 for o in opportunities if o["opportunity_entity"] == entity_b),
            sum(1 for o in opportunities if o["opportunity_entity"] == entity_a),
            self.tenant_id, self.run_id,
        )

        return opportunities

    def get_cross_sell_summary(self) -> dict:
        """
        Returns:
        {
            "total_opportunities": int,
            "total_potential_acv": float,
            "by_service": [{"service": str, "count": int, "total_acv": float}],
            "by_direction": {"a_to_b": int, "b_to_a": int}
        }
        """
        entity_a, entity_b = self._get_entities()
        opportunities = self.get_cross_sell_opportunities()

        total_acv = sum(o["typical_acv"] for o in opportunities)

        # Group by service
        service_map: dict[str, dict] = {}
        for o in opportunities:
            svc = o["service"]
            if svc not in service_map:
                service_map[svc] = {"service": svc, "count": 0, "total_acv": 0.0}
            service_map[svc]["count"] += 1
            service_map[svc]["total_acv"] += o["typical_acv"]

        by_service = sorted(service_map.values(), key=lambda x: x["total_acv"], reverse=True)

        # Round ACVs
        for entry in by_service:
            entry["total_acv"] = round(entry["total_acv"], 2)

        # Count by direction
        a_to_b = sum(1 for o in opportunities if o["opportunity_entity"] == entity_b)
        b_to_a = sum(1 for o in opportunities if o["opportunity_entity"] == entity_a)

        return {
            "total_opportunities": len(opportunities),
            "total_potential_acv": round(total_acv, 2),
            "by_service": by_service,
            "by_direction": {"a_to_b": a_to_b, "b_to_a": b_to_a},
        }
