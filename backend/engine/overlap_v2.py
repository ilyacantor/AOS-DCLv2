"""
OverlapEngineV2 — PG-backed overlap analysis from semantic_triples.

Analyzes entity overlap: concepts that appear under both entity_ids
within a domain (customer, vendor, employee).

All data sourced from semantic_triples in PG — no JSON files.
"""

from backend.core.db import get_connection
from backend.engine.engagement import get_active_engagement
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

_ALLOWED_DOMAINS = ("customer", "vendor", "employee")


import re as _re

_NUMERIC_RE = _re.compile(r"^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$")


def _safe_sort_float(val) -> float:
    """Convert a value to float for sorting. Non-numeric values sort as 0."""
    if val is None:
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip().strip('"')
    if _NUMERIC_RE.match(s):
        return float(s)
    return 0.0

# Entity ordering: entity_a sorts first descending (meridian > cascadia)
_ENTITY_A_LABEL = "entity_a"
_ENTITY_B_LABEL = "entity_b"


class OverlapEngineV2:
    """
    Analyzes entity overlap from semantic_triples.

    Overlap = concepts that appear under both entity_ids within a domain.
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

    def _count_concepts_for_entity(self, domain: str, entity_id: str) -> int:
        """Count distinct concepts in a domain for a specific entity."""
        sql = """
            SELECT COUNT(DISTINCT concept) as cnt
            FROM semantic_triples
            WHERE tenant_id = %s AND is_active = true
              AND concept LIKE %s
              AND entity_id = %s
        """
        rows = self._query(sql, [self.tenant_id, f"{domain}.%", entity_id])
        return rows[0]["cnt"] if rows else 0

    def _find_overlapping_concepts(self, domain: str) -> list[str]:
        """Find concepts in a domain that appear under both entity_ids."""
        sql = """
            SELECT concept
            FROM semantic_triples
            WHERE tenant_id = %s AND is_active = true
              AND concept LIKE %s
            GROUP BY concept
            HAVING COUNT(DISTINCT entity_id) > 1
            ORDER BY concept
        """
        rows = self._query(sql, [self.tenant_id, f"{domain}.%"])
        return [r["concept"] for r in rows]

    def get_overlap_summary(self) -> dict:
        """
        Returns overlap summary per domain.

        Example:
        {
            "customer": {"overlap_count": 34, "entity_a_total": 1218, "entity_b_total": 220,
                         "overlap_pct_a": 2.79, "overlap_pct_b": 15.45},
            ...
        }
        """
        entity_a, entity_b = self._get_entities()
        summary = {}

        for domain in _ALLOWED_DOMAINS:
            overlapping = self._find_overlapping_concepts(domain)
            overlap_count = len(overlapping)
            a_total = self._count_concepts_for_entity(domain, entity_a)
            b_total = self._count_concepts_for_entity(domain, entity_b)

            overlap_pct_a = round(overlap_count / a_total * 100, 2) if a_total > 0 else 0.0
            overlap_pct_b = round(overlap_count / b_total * 100, 2) if b_total > 0 else 0.0

            summary[domain] = {
                "overlap_count": overlap_count,
                "entity_a_total": a_total,
                "entity_b_total": b_total,
                "overlap_pct_a": overlap_pct_a,
                "overlap_pct_b": overlap_pct_b,
            }

        logger.info(
            "OverlapEngineV2.get_overlap_summary: %s for tenant=%s, run=%s",
            {d: s["overlap_count"] for d, s in summary.items()},
            self.tenant_id, self.run_id,
        )
        return summary

    def get_overlapping_concepts(self, domain: str) -> list[dict]:
        """
        Returns list of overlapping concepts with properties from both entities.

        [{"concept": "customer.accenture",
          "entity_a_properties": {"revenue": 12.0, "industry": "Professional Services", ...},
          "entity_b_properties": {"revenue": 4.0, "industry": "Professional Services", ...}}]
        """
        if domain not in _ALLOWED_DOMAINS:
            raise ValueError(
                f"Invalid domain '{domain}'. Must be one of: {', '.join(_ALLOWED_DOMAINS)}"
            )

        entity_a, entity_b = self._get_entities()
        overlapping = self._find_overlapping_concepts(domain)

        if not overlapping:
            return []

        # Fetch all properties for overlapping concepts in one query
        placeholders = ", ".join(["%s"] * len(overlapping))
        sql = f"""
            SELECT concept, entity_id, property, value
            FROM semantic_triples
            WHERE tenant_id = %s AND is_active = true
              AND concept IN ({placeholders})
            ORDER BY concept, entity_id, property
        """
        params = [self.tenant_id] + overlapping
        rows = self._query(sql, params)

        # Organize by concept → entity → properties
        concept_data: dict[str, dict[str, dict]] = {}
        for row in rows:
            concept = row["concept"]
            eid = row["entity_id"]
            prop = row["property"]
            val = row["value"]

            if concept not in concept_data:
                concept_data[concept] = {}
            if eid not in concept_data[concept]:
                concept_data[concept][eid] = {}
            concept_data[concept][eid][prop] = val

        result = []
        for concept in overlapping:
            props = concept_data.get(concept, {})
            result.append({
                "concept": concept,
                "entity_a_properties": props.get(entity_a, {}),
                "entity_b_properties": props.get(entity_b, {}),
            })

        return result

    def get_entity_only_concepts(self, domain: str, entity_id: str) -> list[str]:
        """
        Concepts in domain that appear ONLY under the given entity.
        E.g., Meridian-only customers = 1218 - 34 = 1184.
        """
        if domain not in _ALLOWED_DOMAINS:
            raise ValueError(
                f"Invalid domain '{domain}'. Must be one of: {', '.join(_ALLOWED_DOMAINS)}"
            )

        overlapping_set = set(self._find_overlapping_concepts(domain))

        sql = """
            SELECT DISTINCT concept
            FROM semantic_triples
            WHERE tenant_id = %s AND is_active = true
              AND concept LIKE %s
              AND entity_id = %s
            ORDER BY concept
        """
        rows = self._query(sql, [self.tenant_id, f"{domain}.%", entity_id])
        all_concepts = [r["concept"] for r in rows]

        return [c for c in all_concepts if c not in overlapping_set]

    def get_overlap_by_property(self, domain: str, property_name: str) -> list[dict]:
        """
        Compare a specific property across overlapping concepts.
        E.g., compare 'revenue' for shared customers.
        Returns sorted list with both entity values for comparison.
        """
        if domain not in _ALLOWED_DOMAINS:
            raise ValueError(
                f"Invalid domain '{domain}'. Must be one of: {', '.join(_ALLOWED_DOMAINS)}"
            )

        entity_a, entity_b = self._get_entities()
        overlapping = self._find_overlapping_concepts(domain)

        if not overlapping:
            return []

        placeholders = ", ".join(["%s"] * len(overlapping))
        sql = f"""
            SELECT concept, entity_id, value
            FROM semantic_triples
            WHERE tenant_id = %s AND is_active = true
              AND concept IN ({placeholders})
              AND property = %s
            ORDER BY concept, entity_id
        """
        params = [self.tenant_id] + overlapping + [property_name]
        rows = self._query(sql, params)

        # Group by concept
        concept_values: dict[str, dict[str, object]] = {}
        for row in rows:
            concept = row["concept"]
            if concept not in concept_values:
                concept_values[concept] = {}
            concept_values[concept][row["entity_id"]] = row["value"]

        result = []
        for concept in overlapping:
            vals = concept_values.get(concept, {})
            result.append({
                "concept": concept,
                "entity_a_value": vals.get(entity_a),
                "entity_b_value": vals.get(entity_b),
            })

        # Sort by entity_a value descending
        def sort_key(item):
            val = item.get("entity_a_value")
            return _safe_sort_float(val)

        result.sort(key=sort_key, reverse=True)
        return result
