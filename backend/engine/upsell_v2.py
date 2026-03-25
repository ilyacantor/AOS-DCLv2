"""
UpsellEngineV2 — upsell opportunity scoring from semantic_triples.

Identifies shared customers (served by both entities), maps their
service portfolios from customer_service.* triples, finds service gaps,
and scores upsell opportunities.

Scoring model (100-point, 4 components):
  - relationship_strength (0-30): satisfaction_score from current engagement
  - service_adjacency (0-25): how many services the customer already buys from target
  - revenue_potential (0-25): typical_acv from service.* triples
  - contract_recency (0-20): engagement_start_year freshness

All data sourced from PG semantic_triples — no JSON files.
"""

from datetime import datetime

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


class UpsellEngineV2:
    """
    Upsell opportunity scoring from shared customer analysis.

    Identifies services that Entity A provides to a shared customer
    that Entity B does not (and vice versa), then scores the expansion
    opportunity based on engagement quality and service fit.
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

    def _get_service_portfolio(self, entity_id: str) -> dict[str, dict]:
        """
        Get service portfolio for an entity from service.* triples.
        Returns dict keyed by service key (e.g. "strategy"):
            {"typical_acv": float, "description": str, "delivery_model": str}
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

        services: dict[str, dict] = {}
        for row in rows:
            concept = row["concept"]
            if concept not in services:
                services[concept] = {"concept": concept}
            services[concept][row["property"]] = row["value"]

        # Re-key by service name (strip "service." prefix)
        result = {}
        for concept, props in services.items():
            svc_key = concept.split(".", 1)[1] if "." in concept else concept
            result[svc_key] = {
                "concept": concept,
                "typical_acv": _to_float(
                    props.get("typical_acv", 0),
                    context=f"service {concept} typical_acv",
                ),
                "description": str(props.get("description", "")),
                "delivery_model": str(props.get("delivery_model", "")),
            }
        return result

    def _get_customer_service_engagements(self, entity_id: str) -> dict[str, dict[str, dict]]:
        """
        Get customer-service engagement data from customer_service.* triples.

        Returns nested dict: {customer_norm: {service_key: {properties}}}
        where concept format is customer_service.{customer}.{service}.

        Properties per engagement: engagement_revenue, engagement_start_year,
        contract_type, satisfaction_score.
        """
        sql = """
            SELECT DISTINCT ON (concept, property)
                   concept, property, value
            FROM semantic_triples
            WHERE tenant_id = %s AND is_active = true
              AND concept LIKE 'customer_service.%%'
              AND entity_id = %s
            ORDER BY concept, property, created_at DESC
        """
        rows = self._query(sql, [self.tenant_id, entity_id])

        # Group by concept then split into customer/service
        raw: dict[str, dict] = {}
        for row in rows:
            concept = row["concept"]
            if concept not in raw:
                raw[concept] = {}
            raw[concept][row["property"]] = row["value"]

        # Parse concept format: customer_service.{customer}.{service}
        result: dict[str, dict[str, dict]] = {}
        for concept, props in raw.items():
            parts = concept.split(".", 2)  # ["customer_service", customer, service]
            if len(parts) != 3:
                logger.warning(
                    "UpsellEngineV2: skipping malformed concept '%s' "
                    "(expected customer_service.{customer}.{service})",
                    concept,
                )
                continue
            customer_norm = parts[1]
            service_key = parts[2]
            if customer_norm not in result:
                result[customer_norm] = {}
            result[customer_norm][service_key] = props

        return result

    def _find_shared_customers(self) -> list[str]:
        """
        Find customers that appear under both entities.
        Returns list of normalized customer names (e.g. "accenture").

        Delegates to OverlapEngineV2._find_overlapping_concepts("customer")
        which returns concepts like "customer.accenture" — we strip the prefix.
        """
        overlapping = self._overlap_engine._find_overlapping_concepts("customer")
        return [c.split(".", 1)[1] for c in overlapping if "." in c]

    def _compute_upsell_score(
        self,
        engagement_props: dict,
        service_data: dict,
        n_existing_services: int,
        current_year: int,
    ) -> dict:
        """
        Compute upsell sub-scores for a gap service opportunity.

        Scoring rubric:
          relationship_strength (0-30): satisfaction from current engagement
          service_adjacency (0-25): number of services customer already buys from target
          revenue_potential (0-25): typical_acv of the gap service
          contract_recency (0-20): how recently the customer started with source entity
        """
        satisfaction = _safe_float(engagement_props.get("satisfaction_score"), 60)
        start_year = int(_safe_float(engagement_props.get("engagement_start_year"), 2020))
        typical_acv = service_data.get("typical_acv", 0.0)
        years_since_start = current_year - start_year

        # relationship_strength (0-30): from satisfaction_score
        if satisfaction >= 85:
            relationship_strength = 28
        elif satisfaction >= 75:
            relationship_strength = 22
        elif satisfaction >= 65:
            relationship_strength = 16
        elif satisfaction >= 55:
            relationship_strength = 10
        else:
            relationship_strength = 5

        # service_adjacency (0-25): more existing services = higher adjacency
        if n_existing_services >= 4:
            service_adjacency = 23
        elif n_existing_services >= 3:
            service_adjacency = 18
        elif n_existing_services >= 2:
            service_adjacency = 13
        elif n_existing_services >= 1:
            service_adjacency = 8
        else:
            service_adjacency = 3

        # revenue_potential (0-25): typical_acv from service.* triples
        if typical_acv >= 5.0:
            revenue_potential = 23
        elif typical_acv >= 3.0:
            revenue_potential = 18
        elif typical_acv >= 1.0:
            revenue_potential = 13
        elif typical_acv > 0:
            revenue_potential = 8
        else:
            revenue_potential = 3

        # contract_recency (0-20): engagement_start_year freshness
        if years_since_start <= 1:
            contract_recency = 18
        elif years_since_start <= 3:
            contract_recency = 14
        elif years_since_start <= 5:
            contract_recency = 10
        else:
            contract_recency = 6

        total = relationship_strength + service_adjacency + revenue_potential + contract_recency

        return {
            "upsell_score": total,
            "relationship_strength": relationship_strength,
            "service_adjacency": service_adjacency,
            "revenue_potential": revenue_potential,
            "contract_recency": contract_recency,
        }

    def get_upsell_opportunities(self) -> list[dict]:
        """
        For each shared customer, identify services one entity provides
        that the other does not, with upsell scoring.

        Returns list of opportunity dicts sorted by upsell_score descending.
        """
        entity_a, entity_b = self._get_entities()

        # Get service portfolios for ACV lookup
        a_svc_portfolio = self._get_service_portfolio(entity_a)
        b_svc_portfolio = self._get_service_portfolio(entity_b)

        if not a_svc_portfolio:
            raise ValueError(
                f"UpsellEngineV2: no service.* triples found for entity_id='{entity_a}' "
                f"in tenant_id='{self.tenant_id}'"
            )
        if not b_svc_portfolio:
            raise ValueError(
                f"UpsellEngineV2: no service.* triples found for entity_id='{entity_b}' "
                f"in tenant_id='{self.tenant_id}'"
            )

        # Get customer-service engagement data
        a_engagements = self._get_customer_service_engagements(entity_a)
        b_engagements = self._get_customer_service_engagements(entity_b)

        if not a_engagements and not b_engagements:
            raise ValueError(
                f"UpsellEngineV2: no customer_service.* triples found for either entity "
                f"in tenant_id='{self.tenant_id}' — run the Farm pipeline to generate "
                f"customer-service engagement triples before using the upsell report"
            )

        # Find shared customers
        shared_customers = self._find_shared_customers()
        if not shared_customers:
            logger.info(
                "UpsellEngineV2: no shared customers found for tenant=%s — "
                "all customers are entity-exclusive, no upsell opportunities",
                self.tenant_id,
            )
            return []

        # Get customer.* properties for match_type and display name
        a_customer_props = self._get_customer_properties(entity_a)
        b_customer_props = self._get_customer_properties(entity_b)

        current_year = datetime.now().year
        opportunities = []

        for customer_norm in sorted(shared_customers):
            a_svcs = a_engagements.get(customer_norm, {})
            b_svcs = b_engagements.get(customer_norm, {})

            # Skip customers with no engagement data in either entity
            if not a_svcs and not b_svcs:
                continue

            a_service_keys = set(a_svcs.keys())
            b_service_keys = set(b_svcs.keys())

            # Gaps: services A provides that B does not → upsell B's customers
            a_gaps_for_b = a_service_keys - b_service_keys
            # Gaps: services B provides that A does not → upsell A's customers
            b_gaps_for_a = b_service_keys - a_service_keys

            customer_display = customer_norm.replace("_", " ").title()

            # Get match_type from customer.* triple properties
            cust_concept = f"customer.{customer_norm}"
            a_cust_props = a_customer_props.get(cust_concept, {})
            b_cust_props = b_customer_props.get(cust_concept, {})
            match_type = str(
                a_cust_props.get("match_type", b_cust_props.get("match_type", "exact"))
            )

            # Direction: entity_a has service, entity_b does not → source=a, target=b
            for gap_svc in sorted(a_gaps_for_b):
                # Source entity (A) engagement props for this customer
                source_eng = a_svcs.get(gap_svc, {})
                # Use best available engagement for scoring reference
                best_eng = _best_engagement(a_svcs)

                svc_data = a_svc_portfolio.get(gap_svc, b_svc_portfolio.get(gap_svc, {}))
                scores = self._compute_upsell_score(
                    best_eng, svc_data, len(b_service_keys), current_year,
                )

                eng_revenue = _safe_float(source_eng.get("engagement_revenue"), 0.0)
                svc_name = gap_svc.replace("_", " ").title()
                typical_acv = svc_data.get("typical_acv", 0.0)

                opportunities.append({
                    "customer": customer_norm,
                    "customer_id": customer_norm,
                    "customer_name": customer_display,
                    "source_entity": entity_a,
                    "target_entity": entity_b,
                    "gap_service": gap_svc,
                    "gap_service_name": svc_name,
                    "typical_acv": typical_acv,
                    **scores,
                    "current_services": sorted(b_service_keys),
                    "current_engagement_revenue_M": round(eng_revenue, 2),
                    "satisfaction_score": int(_safe_float(best_eng.get("satisfaction_score"), 0)),
                    "contract_type": str(best_eng.get("contract_type", "")),
                    "engagement_start_year": int(_safe_float(best_eng.get("engagement_start_year"), 0)),
                    "match_type": match_type,
                    "rationale": (
                        f"{entity_a} provides {svc_name} (ACV ${typical_acv}M) but "
                        f"{entity_b} does not serve {customer_display} with this service. "
                        f"Customer already buys {len(b_service_keys)} service(s) from {entity_b} "
                        f"— strong expansion opportunity."
                    ),
                })

            # Direction: entity_b has service, entity_a does not → source=b, target=a
            for gap_svc in sorted(b_gaps_for_a):
                source_eng = b_svcs.get(gap_svc, {})
                best_eng = _best_engagement(b_svcs)

                svc_data = b_svc_portfolio.get(gap_svc, a_svc_portfolio.get(gap_svc, {}))
                scores = self._compute_upsell_score(
                    best_eng, svc_data, len(a_service_keys), current_year,
                )

                eng_revenue = _safe_float(source_eng.get("engagement_revenue"), 0.0)
                svc_name = gap_svc.replace("_", " ").title()
                typical_acv = svc_data.get("typical_acv", 0.0)

                opportunities.append({
                    "customer": customer_norm,
                    "customer_id": customer_norm,
                    "customer_name": customer_display,
                    "source_entity": entity_b,
                    "target_entity": entity_a,
                    "gap_service": gap_svc,
                    "gap_service_name": svc_name,
                    "typical_acv": typical_acv,
                    **scores,
                    "current_services": sorted(a_service_keys),
                    "current_engagement_revenue_M": round(eng_revenue, 2),
                    "satisfaction_score": int(_safe_float(best_eng.get("satisfaction_score"), 0)),
                    "contract_type": str(best_eng.get("contract_type", "")),
                    "engagement_start_year": int(_safe_float(best_eng.get("engagement_start_year"), 0)),
                    "match_type": match_type,
                    "rationale": (
                        f"{entity_b} provides {svc_name} (ACV ${typical_acv}M) but "
                        f"{entity_a} does not serve {customer_display} with this service. "
                        f"Customer already buys {len(a_service_keys)} service(s) from {entity_a} "
                        f"— strong expansion opportunity."
                    ),
                })

        # Sort by score descending
        opportunities.sort(key=lambda x: x["upsell_score"], reverse=True)

        logger.info(
            "UpsellEngineV2: %d opportunities from %d shared customers "
            "(%d a_to_b, %d b_to_a) for tenant=%s",
            len(opportunities),
            len(shared_customers),
            sum(1 for o in opportunities if o["target_entity"] == entity_b),
            sum(1 for o in opportunities if o["target_entity"] == entity_a),
            self.tenant_id,
        )

        return opportunities

    def _get_customer_properties(self, entity_id: str) -> dict[str, dict]:
        """Get customer.* triple properties for match_type and metadata."""
        sql = """
            SELECT DISTINCT ON (concept, property)
                   concept, property, value
            FROM semantic_triples
            WHERE tenant_id = %s AND is_active = true
              AND concept LIKE 'customer.%%'
              AND concept NOT LIKE 'customer.%%.%%'
              AND entity_id = %s
            ORDER BY concept, property, created_at DESC
        """
        rows = self._query(sql, [self.tenant_id, entity_id])

        customers: dict[str, dict] = {}
        for row in rows:
            concept = row["concept"]
            if concept not in customers:
                customers[concept] = {}
            customers[concept][row["property"]] = row["value"]
        return customers

    def get_upsell_summary(self) -> dict:
        """
        Returns:
        {
            "total_shared_customers": int,
            "total_opportunities": int,
            "total_expansion_acv": float,
            "avg_score": float,
            "by_direction": {"a_to_b": {"count": int, "acv": float},
                             "b_to_a": {"count": int, "acv": float}},
            "by_service": [{"service": str, "count": int, "total_acv": float}]
        }
        """
        entity_a, entity_b = self._get_entities()
        opportunities = self.get_upsell_opportunities()

        total_acv = sum(o["typical_acv"] for o in opportunities)
        avg_score = round(
            sum(o["upsell_score"] for o in opportunities) / len(opportunities), 1
        ) if opportunities else 0.0

        # Unique shared customers in results
        shared_in_results = len({o["customer"] for o in opportunities})

        # Group by service
        service_map: dict[str, dict] = {}
        for o in opportunities:
            svc = o["gap_service"]
            if svc not in service_map:
                service_map[svc] = {"service": svc, "count": 0, "total_acv": 0.0}
            service_map[svc]["count"] += 1
            service_map[svc]["total_acv"] += o["typical_acv"]

        by_service = sorted(service_map.values(), key=lambda x: x["total_acv"], reverse=True)
        for entry in by_service:
            entry["total_acv"] = round(entry["total_acv"], 2)

        # Count by direction
        a_to_b = [o for o in opportunities if o["target_entity"] == entity_b]
        b_to_a = [o for o in opportunities if o["target_entity"] == entity_a]

        return {
            "total_shared_customers": shared_in_results,
            "total_opportunities": len(opportunities),
            "total_expansion_acv": round(total_acv, 2),
            "avg_score": avg_score,
            "by_direction": {
                "a_to_b": {
                    "count": len(a_to_b),
                    "acv": round(sum(o["typical_acv"] for o in a_to_b), 2),
                },
                "b_to_a": {
                    "count": len(b_to_a),
                    "acv": round(sum(o["typical_acv"] for o in b_to_a), 2),
                },
            },
            "by_service": by_service,
        }


def _best_engagement(engagements: dict[str, dict]) -> dict:
    """Pick the engagement with the highest satisfaction_score for scoring reference."""
    if not engagements:
        return {}
    best = max(
        engagements.values(),
        key=lambda e: _safe_float(e.get("satisfaction_score"), 0),
    )
    return best
