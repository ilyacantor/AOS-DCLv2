"""
MCP query_triples must carry fabric attribution (fabric_plane, fabric_product).

§9.2: the two fabric-attribution columns were dropped from the MCP query
SELECT, so agents on the MCP server could not see which fabric plane/product
a triple came from. These columns exist on semantic_triples and are written by
the ingest path; the MCP tool surface must expose them.

Acceptance (driven through the registered MCP tool function the wire server
dispatches to — backend/engine/mcp_tools.py:tool_query_triples):
  - Every returned triple carries the keys `fabric_plane` and `fabric_product`.
  - For a triple known (from the DB at test time) to have non-null fabric
    attribution, the MCP-returned values equal the stored ground truth.

Ground truth is read from semantic_triples at test runtime — never hardcoded
(B8/B10). Runs against the aos-dev DB loaded by the root conftest.
"""

from __future__ import annotations

import pytest

from backend.core.db import get_connection
from backend.engine.mcp_tools import tool_query_triples

# query_triples is paginated. The ground-truth triple must live in a
# (tenant, concept) group small enough to be returned in full within this
# limit — otherwise a truncated page can legitimately omit it (the dev DB
# holds >5k active rows for some demo concepts, e.g. gl.2200). Coupling the
# two keeps this acceptance about fabric attribution, not pagination luck.
QUERY_LIMIT = 1000


def _ground_truth_fabric_triple() -> dict | None:
    """One active triple that carries real fabric attribution, straight from
    the store. Restricted to a (tenant, concept) group that fits within
    QUERY_LIMIT so the verifying query returns the whole group, and ordered so
    the same triple is picked every run (deterministic, truncation-proof).
    Returns None only if no fabric-attributed data exists."""
    sql = (
        "WITH grp AS ("
        "  SELECT tenant_id, concept FROM semantic_triples "
        "  WHERE is_active = true "
        "    AND fabric_plane IS NOT NULL AND fabric_plane NOT IN ('none', '') "
        "  GROUP BY tenant_id, concept HAVING count(*) <= %s"
        ") "
        "SELECT t.tenant_id, t.entity_id, t.concept, t.id, t.fabric_plane, t.fabric_product "
        "FROM semantic_triples t "
        "JOIN grp ON grp.tenant_id = t.tenant_id AND grp.concept = t.concept "
        "WHERE t.is_active = true "
        "  AND t.fabric_plane IS NOT NULL AND t.fabric_plane NOT IN ('none', '') "
        "ORDER BY t.tenant_id, t.concept, t.id "
        "LIMIT 1"
    )
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = 15000")
            cur.execute(sql, (QUERY_LIMIT,))
            row = cur.fetchone()
            if row is None:
                return None
            cols = [d[0] for d in cur.description]
            d = dict(zip(cols, row))
            d["tenant_id"] = str(d["tenant_id"])
            d["id"] = str(d["id"])
            return d


def test_query_triples_carries_fabric_attribution():
    gt = _ground_truth_fabric_triple()
    assert gt is not None, (
        "No active triple with fabric attribution exists in the DB — cannot "
        "verify §9.2. The demo's fabric-attributed data (e.g. cloud/iPaaS "
        "sources) must be present for this acceptance."
    )

    rows = tool_query_triples(
        gt["tenant_id"], concept=gt["concept"], limit=QUERY_LIMIT, active_only=True
    )
    assert rows, (
        f"query_triples(tenant={gt['tenant_id']}, concept={gt['concept']}) "
        f"returned 0 triples, but ground-truth triple {gt['id']} is active."
    )

    # Contract: the MCP result must carry both fabric keys on EVERY row —
    # a dropped column is the exact §9.2 regression. Present even when the
    # value is null (financial rows without attribution still carry the keys).
    for r in rows:
        assert "fabric_plane" in r, (
            f"fabric_plane missing from MCP query_triples row {r.get('id')} — "
            f"agents cannot see fabric attribution (§9.2 regression)."
        )
        assert "fabric_product" in r, (
            f"fabric_product missing from MCP query_triples row {r.get('id')} — "
            f"agents cannot see fabric attribution (§9.2 regression)."
        )

    # Positive, ground-truth-compared: the specific triple's attribution
    # surfaces with the stored values, not None.
    match = next((r for r in rows if str(r.get("id")) == gt["id"]), None)
    assert match is not None, (
        f"Ground-truth triple {gt['id']} (concept={gt['concept']}) not present "
        f"in query_triples output."
    )
    assert match["fabric_plane"] == gt["fabric_plane"], (
        f"User (MCP agent) queried concept={gt['concept']}. Expected "
        f"fabric_plane={gt['fabric_plane']!r} from DCL. Got "
        f"{match['fabric_plane']!r}."
    )
    assert match["fabric_product"] == gt["fabric_product"], (
        f"User (MCP agent) queried concept={gt['concept']}. Expected "
        f"fabric_product={gt['fabric_product']!r} from DCL. Got "
        f"{match['fabric_product']!r}."
    )
