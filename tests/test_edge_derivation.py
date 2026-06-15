"""Stage-3 edge derivation gate — the stitched graph (ContextOS).

The governing gate: prove each derived edge carries information NO single source
record holds. The hero edge is the cross-source comp-gap (BELOW_MARKET) — it is
synthesized by JOINING workday_hr comp_band 165000 with radford_comp market
190000 across the engineering->software_engineering resolution. Neither source
record holds the gap (gap_usd 25000 / gap_pct 13.16); it exists only in the join.

Runs in-process (TestClient) against the SHARED aos-dev store, reading the
ALREADY-INGESTED ContextOSDemo entity (NOT re-ingested). The derivation is
exercised through the real POST /api/dcl/graph/derive endpoint; the assertions
read entity_edges and semantic_triples directly to prove the synthesis.

Strong, specific assertions (B4): exact gap_pct/median/below_market values,
exact share/rank, and a negative proof that NO single triple holds the
synthesized number — the join is what creates it.
"""

import os

import pytest
from dotenv import load_dotenv

# The DCL test convention: load the dev env explicitly so the in-process app and
# these direct DB reads both resolve aos-dev (the run command also sources it,
# but loading here makes the file self-contained).
load_dotenv(".env.development")

from fastapi.testclient import TestClient

from backend.api.main import app
from backend.core.db import get_connection

client = TestClient(app)

# The already-ingested demo entity (do NOT re-ingest — read it).
TENANT_ID = "51aee6ec-15c3-4fb0-833a-a19bb4511296"
ENTITY_ID = "ContextOSDemo"
DCL_INGEST_ID = "e4ec6c50-2104-46e4-b173-74d579b1a136"

# Prod project ref — these tests must NEVER run against it.
_PROD_PROJECT_REF = "gdbmdrouocxjxiohpixr"


@pytest.fixture(scope="module", autouse=True)
def _guard_dev_db_and_derive():
    """Hard-assert the store is aos-dev (NOT prod), then run the derivation once
    through the real endpoint so every test reads its persisted output."""
    db_url = (
        os.environ.get("DATABASE_URL")
        or os.environ.get("SUPABASE_DB_URL")
        or ""
    )
    assert _PROD_PROJECT_REF not in db_url, (
        f"REFUSING TO RUN: the active DATABASE_URL points at the PROD project "
        f"{_PROD_PROJECT_REF!r}. Edge-derivation tests run ONLY against aos-dev."
    )

    resp = client.post(
        "/api/dcl/graph/derive",
        json={
            "tenant_id": TENANT_ID,
            "entity_id": ENTITY_ID,
            "dcl_ingest_id": DCL_INGEST_ID,
        },
    )
    assert resp.status_code == 201, (
        f"derive endpoint failed: {resp.status_code} {resp.text}"
    )
    yield resp.json()


def _fetch_edge(src_type, src_key, edge_type, dst_type, dst_key):
    """One live edge_edges row by full coordinate, as a dict."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT properties, derivation, source_system, confidence_tier "
                "FROM entity_edges "
                "WHERE tenant_id = %s AND entity_id = %s AND is_active = true "
                "AND src_type = %s AND src_key = %s AND edge_type = %s "
                "AND dst_type = %s AND dst_key = %s",
                [TENANT_ID, ENTITY_ID, src_type, src_key, edge_type, dst_type, dst_key],
            )
            rows = cur.fetchall()
    assert len(rows) == 1, (
        f"expected exactly one live {edge_type} edge "
        f"{src_type}:{src_key}->{dst_type}:{dst_key}, found {len(rows)}"
    )
    props, derivation, source_system, tier = rows[0]
    return {
        "properties": props, "derivation": derivation,
        "source_system": source_system, "confidence_tier": tier,
    }


def test_comp_gap_edge_derived():
    """The hero edge persists with the synthesized gap. User-visible outcome:
    engineering BELOW_MARKET software_engineering with internal 165000, market
    190000, gap_pct 13.16, below_market true, derivation 'derived'."""
    edge = _fetch_edge("department", "engineering", "BELOW_MARKET",
                       "job_family", "software_engineering")
    p = edge["properties"]
    assert p["internal_median"] == 165000, f"internal_median expected 165000, got {p['internal_median']}"
    assert p["market_median"] == 190000, f"market_median expected 190000, got {p['market_median']}"
    assert p["gap_usd"] == 25000, f"gap_usd expected 25000, got {p['gap_usd']}"
    assert p["gap_pct"] == 13.16, f"gap_pct expected 13.16, got {p['gap_pct']}"
    assert p["below_market"] is True, f"below_market expected True, got {p['below_market']}"
    assert edge["derivation"] == "derived", f"derivation expected 'derived', got {edge['derivation']}"
    assert edge["source_system"] == "dcl_derived", (
        f"source_system expected 'dcl_derived', got {edge['source_system']}"
    )


def test_comp_gap_carries_info_no_single_record_holds():
    """THE GATE. Prove gap_pct 13.16 / gap_usd 25000 are held by NO single
    triple — they are synthesized by joining the two source records. And prove
    the two source records DO exist separately, on DIFFERENT source_systems."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            # No single active triple holds the synthesized gap (25000 or 13.16),
            # in any concept/property/period — the gap lives only in the join.
            cur.execute(
                "SELECT concept, property, value FROM semantic_triples "
                "WHERE tenant_id = %s AND entity_id = %s AND is_active = true "
                "AND (value::numeric = 25000 OR value::numeric = 13.16)",
                [TENANT_ID, ENTITY_ID],
            )
            gap_holders = cur.fetchall()
            assert gap_holders == [], (
                "gap_pct 13.16 is synthesized by joining workday_hr comp_band 165000 "
                "with radford_comp market 190000 across the engineering->software_engineering "
                "resolution; no single record holds it. But a triple was found carrying "
                f"the synthesized value: {gap_holders}"
            )

            # The internal side: comp_band engineering 165000 from workday_hr.
            cur.execute(
                "SELECT value, source_system FROM semantic_triples "
                "WHERE tenant_id = %s AND entity_id = %s AND is_active = true "
                "AND concept = %s AND property = %s AND period = %s",
                [TENANT_ID, ENTITY_ID, "comp_band.median.by_department",
                 "engineering", "2026-03"],
            )
            internal_rows = cur.fetchall()
            assert len(internal_rows) == 1, f"expected 1 internal comp_band row, got {len(internal_rows)}"
            internal_value, internal_source = internal_rows[0]
            assert float(internal_value) == 165000, f"internal comp_band expected 165000, got {internal_value}"
            assert internal_source == "workday_hr", f"internal source expected 'workday_hr', got {internal_source}"

            # The market side: market_benchmark software_engineering 190000 from radford_comp.
            cur.execute(
                "SELECT value, source_system FROM semantic_triples "
                "WHERE tenant_id = %s AND entity_id = %s AND is_active = true "
                "AND concept = %s AND property = %s AND period = %s",
                [TENANT_ID, ENTITY_ID, "market_benchmark.median.by_job_family",
                 "software_engineering", "2026-03"],
            )
            market_rows = cur.fetchall()
            assert len(market_rows) == 1, f"expected 1 market_benchmark row, got {len(market_rows)}"
            market_value, market_source = market_rows[0]
            assert float(market_value) == 190000, f"market expected 190000, got {market_value}"
            assert market_source == "radford_comp", f"market source expected 'radford_comp', got {market_source}"

            # The two source records exist on DIFFERENT source_systems — the gap
            # is genuinely cross-source.
            assert internal_source != market_source, (
                f"the comp-gap must be cross-source: internal {internal_source!r} and "
                f"market {market_source!r} are the SAME system — the gap would mirror "
                f"a single source, failing the gate"
            )


def test_exit_driver_dominant_synthesized():
    """DRIVEN_BY engineering->compensation with share 0.667, rank 1, count 8,
    total 12 — and NO single triple asserts 'compensation is dominant' (each
    exit_theme triple is just a count; the ranking is synthesized)."""
    edge = _fetch_edge("department", "engineering", "DRIVEN_BY",
                       "exit_theme", "compensation")
    p = edge["properties"]
    assert p["count"] == 8, f"count expected 8, got {p['count']}"
    assert p["total"] == 12, f"total expected 12, got {p['total']}"
    assert p["share"] == 0.667, f"share expected 0.667, got {p['share']}"
    assert p["rank"] == 1, f"rank expected 1, got {p['rank']}"
    assert edge["derivation"] == "derived", f"derivation expected 'derived', got {edge['derivation']}"

    # No single exit_theme triple asserts dominance — they are bare counts. The
    # four counts (8/2/1/1) each live in their own triple; "compensation
    # dominates" is the synthesized rank, present in no source record.
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT concept, value FROM semantic_triples "
                "WHERE tenant_id = %s AND entity_id = %s AND is_active = true "
                "AND concept LIKE %s AND property = %s AND period = %s "
                "ORDER BY concept",
                [TENANT_ID, ENTITY_ID, "workforce.exit_theme.%", "engineering", "2026-03"],
            )
            rows = cur.fetchall()
    counts = {c.split(".")[2]: float(v) for c, v in rows}
    assert counts == {"compensation": 8.0, "growth": 2.0, "management": 1.0, "work_life": 1.0}, (
        f"the four engineering exit-theme counts (each a bare count, no dominance "
        f"asserted) expected 8/2/1/1, got {counts}"
    )
    # Each source triple is a single count; none carries 'rank' or 'share' or any
    # dominance claim. The ranking that names compensation the driver is
    # synthesized across all four — no single record holds it.
    assert max(counts, key=counts.get) == "compensation", (
        "ranking the four bare counts yields compensation as dominant — this "
        "ranking is what DRIVEN_BY synthesizes; no single count triple asserts it"
    )


def test_g_and_a_above_market():
    """The derivation computes real direction, not a constant: g&a's BELOW_MARKET
    edge has below_market false because g&a internal 130000 > general_admin
    market 128000 (a department that is ABOVE its market)."""
    edge = _fetch_edge("department", "g&a", "BELOW_MARKET",
                       "job_family", "general_admin")
    p = edge["properties"]
    assert p["internal_median"] == 130000, f"g&a internal expected 130000, got {p['internal_median']}"
    assert p["market_median"] == 128000, f"general_admin market expected 128000, got {p['market_median']}"
    assert p["below_market"] is False, (
        f"g&a is ABOVE market (130000 > 128000) so below_market must be False, "
        f"got {p['below_market']} — the derivation must compute direction, not a constant"
    )
    assert p["gap_usd"] == -2000, f"g&a gap_usd expected -2000 (above market), got {p['gap_usd']}"
