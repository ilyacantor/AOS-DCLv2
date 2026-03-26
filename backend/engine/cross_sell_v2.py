"""
CrossSellEngineV2 — cross-sell opportunity scoring from semantic_triples.

Identifies services that Entity A offers to shared customers
that Entity B could also offer (and vice versa).

Computes propensity scores from customer triple data:
  - industry_match (0-25): industry alignment between customer and target entity
  - size_match (0-20): customer size relative to target entity's typical client
  - behavioral_score (0-30): customer revenue as proxy for engagement depth
  - engagement_fit (0-15): service/delivery model fit
  - relationship_strength (0-10): overlap match confidence

All data sourced from PG semantic_triples — no JSON files.
"""

from collections import defaultdict

from backend.core.db import get_connection
from backend.engine.overlap_v2 import OverlapEngineV2
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)


def _safe_float(value, default: float = 0.0) -> float:
    """Convert a JSONB value to float. Returns default for None/unconvertible."""
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


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

    Computes propensity scores from customer and service triples.
    """

    def __init__(self, tenant_id: str, run_id: str):
        self.tenant_id = tenant_id
        self.run_id = run_id
        self._overlap_engine = OverlapEngineV2(tenant_id, run_id)

    def _query(self, sql: str, params: list) -> list[dict]:
        """Execute a parameterized query and return rows as dicts."""
        with get_connection() as conn:
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
        Returns list of {"concept": str, "typical_acv": float, "description": str,
                         "delivery_model": str}.
        """
        sql = """
            SELECT DISTINCT ON (concept, property)
                   concept, property, value
            FROM semantic_triples
            WHERE tenant_id = %s AND is_active = true
              AND concept LIKE 'service.%%'
              AND entity_id = %s
            ORDER BY concept, property, created_at DESC
        """
        rows = self._query(sql, [self.tenant_id, entity_id])

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
                "delivery_model": str(props.get("delivery_model", "")),
            })
        return result

    def _get_exclusive_customer_data(self, entity_id: str, other_entity_id: str) -> dict[str, dict]:
        """
        Get customer data for customers exclusive to entity_id (not in other_entity_id).

        Pushes exclusivity filter into SQL via NOT EXISTS and only fetches the 5
        properties used by propensity scoring. Excludes subcategory concepts
        (e.g. customer.pipeline.closed_won) in both outer and subquery.

        Returns dict keyed by top-level customer concept:
            {"customer.accenture": {"revenue": "12.0", "industry": "...", ...}}
        """
        sql = """
            SELECT DISTINCT ON (st.concept, st.property)
                   st.concept, st.property, st.value
            FROM semantic_triples st
            WHERE st.tenant_id = %s AND st.is_active = true
              AND st.concept LIKE 'customer.%%'
              AND st.concept NOT LIKE 'customer.%%.%%'
              AND st.entity_id = %s
              AND st.property IN ('revenue', 'industry', 'segment', 'size', 'match_confidence')
              AND NOT EXISTS (
                  SELECT 1
                  FROM semantic_triples other
                  WHERE other.tenant_id = %s AND other.is_active = true
                    AND other.concept = st.concept
                    AND other.concept LIKE 'customer.%%'
                    AND other.concept NOT LIKE 'customer.%%.%%'
                    AND other.entity_id = %s
              )
            ORDER BY st.concept, st.property, st.created_at DESC
        """
        rows = self._query(sql, [self.tenant_id, entity_id, self.tenant_id, other_entity_id])

        customers: dict[str, dict] = {}
        for row in rows:
            concept = row["concept"]
            if concept not in customers:
                customers[concept] = {}
            customers[concept][row["property"]] = row["value"]

        return customers

    def _compute_propensity_score(
        self,
        customer_props: dict,
        service_acv: float,
        delivery_model: str,
    ) -> dict:
        """
        Compute propensity sub-scores from customer triple properties.

        Scoring rubric:
          industry_match (0-25): Enterprise customers in services-aligned industries score higher
          size_match (0-20): Larger customers (by employee count) are better targets
          behavioral_score (0-30): Higher current engagement revenue = stronger signal
          engagement_fit (0-15): Service ACV relative to customer engagement
          relationship_strength (0-10): Based on overlap match_confidence
        """
        revenue = _safe_float(customer_props.get("revenue"), 0.0)
        size = _safe_float(customer_props.get("size"), 0.0)
        match_conf = _safe_float(customer_props.get("match_confidence"), 0.5)
        segment = str(customer_props.get("segment", "")).lower()
        industry = str(customer_props.get("industry", ""))

        # industry_match (0-25): known service industries score higher
        services_industries = {
            "professional services", "technology", "financial services",
            "healthcare", "manufacturing", "retail", "energy",
            "telecommunications", "media", "insurance",
        }
        ind_lower = industry.lower()
        if any(si in ind_lower for si in services_industries):
            industry_match = 20
        elif industry:
            industry_match = 12
        else:
            industry_match = 5
        # Enterprise segment bonus
        if segment == "enterprise":
            industry_match = min(25, industry_match + 5)
        elif segment == "mid-market":
            industry_match = min(25, industry_match + 2)

        # size_match (0-20): scale by employee count
        if size >= 10000:
            size_match = 20
        elif size >= 5000:
            size_match = 16
        elif size >= 1000:
            size_match = 12
        elif size > 0:
            size_match = 8
        else:
            size_match = 10  # unknown size = moderate

        # behavioral_score (0-30): engagement depth by revenue
        if revenue >= 10.0:
            behavioral_score = 28
        elif revenue >= 5.0:
            behavioral_score = 24
        elif revenue >= 2.0:
            behavioral_score = 18
        elif revenue >= 0.5:
            behavioral_score = 12
        elif revenue > 0:
            behavioral_score = 8
        else:
            behavioral_score = 4

        # engagement_fit (0-15): service ACV fit
        if service_acv >= 5.0:
            engagement_fit = 13
        elif service_acv >= 3.0:
            engagement_fit = 10
        else:
            engagement_fit = 7
        # Delivery model bonus for team-based (lower implementation risk)
        if delivery_model in ("team_based", "hybrid_onshore_nearshore"):
            engagement_fit = min(15, engagement_fit + 2)

        # relationship_strength (0-10): from overlap match_confidence
        relationship_strength = round(match_conf * 10)

        total = industry_match + size_match + behavioral_score + engagement_fit + relationship_strength

        return {
            "propensity_score": total,
            "industry_match": industry_match,
            "size_match": size_match,
            "behavioral_score": behavioral_score,
            "engagement_fit": engagement_fit,
            "relationship_strength": relationship_strength,
        }

    def get_cross_sell_opportunities(self) -> list[dict]:
        """
        For each overlapping customer, identify services from one entity
        that could be offered by the other, with propensity scoring.

        Returns list of:
        {
            "customer": str,
            "customer_id": str,
            "customer_name": str,
            "current_entity": str,
            "opportunity_entity": str,
            "service": str,
            "recommended_service": str,
            "typical_acv": float,
            "estimated_acv": float,
            "propensity_score": int,
            "industry_match": int,
            "size_match": int,
            "behavioral_score": int,
            "engagement_fit": int,
            "relationship_strength": int,
            "customer_engagement_M": float,
            "industry": str,
            "segment": str,
            "buyer_persona": str,
            "years_as_client": int,
            "comparable_customers": list[str],
            "rationale": str,
        }
        """
        entity_a, entity_b = self._get_entities()

        # Get service portfolios
        a_services = self._get_service_portfolio(entity_a)
        b_services = self._get_service_portfolio(entity_b)

        if not a_services:
            raise ValueError(
                f"CrossSellEngineV2: no service.* triples found for entity_id='{entity_a}' "
                f"in tenant_id='{self.tenant_id}'"
            )
        if not b_services:
            raise ValueError(
                f"CrossSellEngineV2: no service.* triples found for entity_id='{entity_b}' "
                f"in tenant_id='{self.tenant_id}'"
            )

        # Determine which services are unique to each entity
        a_concepts = {s["concept"] for s in a_services}
        b_concepts = {s["concept"] for s in b_services}
        a_only = a_concepts - b_concepts
        b_only = b_concepts - a_concepts

        # Get customer data for entity-exclusive customers only (SQL-filtered).
        b_customers = self._get_exclusive_customer_data(entity_b, entity_a)
        a_customers = self._get_exclusive_customer_data(entity_a, entity_b)

        b_exclusive = set(b_customers.keys())
        a_exclusive = set(a_customers.keys())

        if not b_exclusive and not a_exclusive:
            logger.info(
                "CrossSellEngineV2: no entity-exclusive customers found for tenant=%s "
                "(a_customers=%d, b_customers=%d, all shared) — no cross-sell opportunities",
                self.tenant_id, len(a_customer_concepts), len(b_customer_concepts),
            )
            return []

        # Service lookup dicts
        a_svc_map = {s["concept"]: s for s in a_services}
        b_svc_map = {s["concept"]: s for s in b_services}

        # Buyer persona mapping by delivery model
        persona_map = {
            "senior_partner_led": "CEO / Managing Partner",
            "specialist_led": "CRO / General Counsel",
            "team_based": "COO / VP Operations",
            "hybrid_onshore_nearshore": "CTO / VP Engineering",
            "hybrid_onshore_offshore": "CHRO / VP People",
            "offshore_delivery_center": "CFO / VP Finance",
            "multi_geo_delivery": "COO / SVP Customer Success",
            "nearshore_delivery_center": "COO / VP Supply Chain",
        }

        opportunities = []

        def _build_opportunity(
            customer_concept: str,
            current_entity: str,
            opportunity_entity: str,
            svc: dict,
            customer_props: dict,
        ) -> dict:
            customer_name = customer_concept.split(".", 1)[1] if "." in customer_concept else customer_concept
            service_name = svc["concept"].split(".", 1)[1] if "." in svc["concept"] else svc["concept"]
            revenue = _safe_float(customer_props.get("revenue"), 0.0)
            industry = str(customer_props.get("industry", ""))
            segment = str(customer_props.get("segment", ""))

            scores = self._compute_propensity_score(
                customer_props, svc["typical_acv"], svc.get("delivery_model", ""),
            )

            return {
                "customer": customer_name,
                "customer_id": customer_name,
                "customer_name": customer_name.replace("_", " ").title(),
                "current_entity": current_entity,
                "opportunity_entity": opportunity_entity,
                "service": service_name,
                "recommended_service": service_name.replace("_", " ").title(),
                "typical_acv": svc["typical_acv"],
                "estimated_acv": svc["typical_acv"],
                **scores,
                "customer_engagement_M": round(revenue, 2),
                "industry": industry,
                "segment": segment,
                "buyer_persona": persona_map.get(svc.get("delivery_model", ""), "CFO"),
                "years_as_client": 3,  # Not in triples — use reasonable default
                "comparable_customers": [],
                "rationale": (
                    f"{current_entity} offers {service_name} "
                    f"(typical ACV ${svc['typical_acv']}M). "
                    f"{customer_name.replace('_', ' ').title()} is an exclusive "
                    f"{opportunity_entity} client with ${revenue}M engagement — "
                    f"cross-sell opportunity via {current_entity} service capabilities."
                ),
            }

        # Direction a_to_b: Entity A's unique services → entity B's exclusive clients
        for svc in a_services:
            if svc["concept"] not in a_only:
                continue
            for customer_concept in sorted(b_exclusive):
                customer_props = b_customers.get(customer_concept, {})
                opportunities.append(_build_opportunity(
                    customer_concept, entity_a, entity_b, svc, customer_props,
                ))

        # Direction b_to_a: Entity B's unique services → entity A's exclusive clients
        for svc in b_services:
            if svc["concept"] not in b_only:
                continue
            for customer_concept in sorted(a_exclusive):
                customer_props = a_customers.get(customer_concept, {})
                opportunities.append(_build_opportunity(
                    customer_concept, entity_b, entity_a, svc, customer_props,
                ))

        # Sort by propensity score descending
        opportunities.sort(key=lambda x: x["propensity_score"], reverse=True)

        # Add comparable customers: top 2 customers with highest propensity in same direction.
        # Pre-index by (service, opportunity_entity) to avoid O(n²) scan.
        comp_index: dict[tuple[str, str], list[str]] = defaultdict(list)
        for opp in opportunities:
            if opp["propensity_score"] >= 70:
                key = (opp["service"], opp["opportunity_entity"])
                comp_index[key].append(opp["customer_name"])

        for opp in opportunities:
            key = (opp["service"], opp["opportunity_entity"])
            candidates = comp_index.get(key, [])
            opp["comparable_customers"] = [c for c in candidates if c != opp["customer_name"]][:2]

        logger.info(
            "CrossSellEngineV2: %d opportunities (%d a_to_b, %d b_to_a) "
            "from %d/%d exclusive customers for tenant=%s",
            len(opportunities),
            sum(1 for o in opportunities if o["opportunity_entity"] == entity_b),
            sum(1 for o in opportunities if o["opportunity_entity"] == entity_a),
            len(b_exclusive),
            len(a_exclusive),
            self.tenant_id,
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
