"""ContextOS Stage 4 — graph-grounded retrieval gate.

The governing gate (Stage 4): the grounded agent answers the demo comp-gap
question BY TRAVERSING the entity graph — and the traversed answer DIFFERS from
the flat one. traverse_graph HANDS the agent the synthesized cross-source
comp-gap (engineering 13.16% below market, gap_usd 25000); the same flat
query_triples can only hand it two raw medians (internal 165000 from workday_hr,
market 190000 from radford_comp) as SEPARATE rows — no row holds the gap, so a
flat-only agent would have to compute it locally and ungrounded.

Runs in-process against the SHARED aos-dev store, reading the ALREADY-INGESTED
ContextOSDemo entity (NOT re-ingested). Calls the real shared MCP tool
implementations (backend.engine.mcp_tools) — the same functions both MCP
servers dispatch to.

Strong, specific assertions (B4): exact gap_pct / below_market / share on the
traversal; exact raw medians + their distinct sources on the flat query; and a
negative proof that NO flat row holds the synthesized gap (13.16 / 25000) — the
join is what creates it.
"""

import os

import pytest
from dotenv import load_dotenv

# The DCL test convention: load the dev env explicitly so these direct tool/DB
# reads resolve aos-dev (the run command also sources it, but loading here makes
# the file self-contained).
load_dotenv(".env.development")

from backend.engine.mcp_tools import tool_query_triples, tool_traverse_graph

import demo.panel_b as panel_b

# The already-ingested demo entity (do NOT re-ingest — read it).
TENANT_ID = "51aee6ec-15c3-4fb0-833a-a19bb4511296"
ENTITY_ID = "ContextOSDemo"

# Prod project ref — these tests must NEVER run against it.
_PROD_PROJECT_REF = "gdbmdrouocxjxiohpixr"

# Stage-3/4 spec values (ContextOS edge-derivation gate). The cross-source
# comp-gap synthesized from workday_hr 165000 (internal) and radford_comp 190000
# (market) across the engineering -> software_engineering resolution.
EXPECTED_INTERNAL_MEDIAN = 165000.0
EXPECTED_MARKET_MEDIAN = 190000.0
EXPECTED_GAP_PCT = 13.16
EXPECTED_GAP_USD = 25000
EXPECTED_DRIVEN_BY_SHARE = 0.667  # compensation is the dominant exit driver
INTERNAL_SOURCE = "workday_hr"
MARKET_SOURCE = "radford_comp"


@pytest.fixture(scope="module", autouse=True)
def _guard_dev_db():
    """Hard-assert the active store is aos-dev (NOT prod) before any read."""
    db_url = (
        os.environ.get("DATABASE_URL")
        or os.environ.get("SUPABASE_DB_URL")
        or ""
    )
    assert _PROD_PROJECT_REF not in db_url, (
        f"REFUSING TO RUN: the active DATABASE_URL points at the PROD project "
        f"{_PROD_PROJECT_REF!r}. Graph-grounded tests run ONLY against aos-dev."
    )


def _edges_to_node(result: dict, edge_type: str, dst_key: str) -> list[dict]:
    """The result's edges of one type that touch dst_key (src or dst)."""
    out = []
    for e in result["edges"]:
        if e["edge_type"] != edge_type:
            continue
        if dst_key in (e["src_key"], e["dst_key"]):
            out.append(e)
    return out


def test_traverse_returns_synthesized_gap():
    """Traversing the engineering node HANDS the agent the synthesized gap:
    a BELOW_MARKET edge to software_engineering whose properties state
    gap_pct 13.16 / below_market True, AND a DRIVEN_BY edge to compensation
    with share 0.667. The agent reads the gap off the graph — it does not
    compute it."""
    result = tool_traverse_graph(
        TENANT_ID,
        entity_id=ENTITY_ID,
        node_type="department",
        node_key="engineering",
    )

    below = _edges_to_node(result, "BELOW_MARKET", "software_engineering")
    assert len(below) == 1, (
        f"User traversed department/engineering. Expected exactly one "
        f"BELOW_MARKET edge to software_engineering. Got {len(below)}: {below}"
    )
    props = below[0]["properties"]
    assert props["gap_pct"] == EXPECTED_GAP_PCT, (
        f"BELOW_MARKET edge must carry the synthesized gap_pct "
        f"{EXPECTED_GAP_PCT}. Got {props.get('gap_pct')} — properties {props}"
    )
    assert props["below_market"] is True, (
        f"BELOW_MARKET edge must flag below_market True. Got "
        f"{props.get('below_market')!r}"
    )
    assert props["internal_median"] == EXPECTED_INTERNAL_MEDIAN
    assert props["market_median"] == EXPECTED_MARKET_MEDIAN
    assert props["gap_usd"] == EXPECTED_GAP_USD
    assert props["internal_source"] == INTERNAL_SOURCE
    assert props["market_source"] == MARKET_SOURCE

    driven = _edges_to_node(result, "DRIVEN_BY", "compensation")
    assert len(driven) == 1, (
        f"Expected one DRIVEN_BY edge from engineering to the compensation "
        f"exit_theme. Got {len(driven)}: {driven}"
    )
    share = round(float(driven[0]["properties"]["share"]), 3)
    assert share == EXPECTED_DRIVEN_BY_SHARE, (
        f"DRIVEN_BY(compensation) must carry the dominant-driver share "
        f"{EXPECTED_DRIVEN_BY_SHARE}. Got {share} — "
        f"properties {driven[0]['properties']}"
    )


def test_flat_query_cannot_synthesize_the_gap():
    """THE CONTRAST: flat query_triples returns the two raw medians as SEPARATE
    rows from their distinct sources — and NO row holds the synthesized gap
    (13.16 / 25000). A flat-only agent would have to compute the gap locally,
    ungrounded. So the traversed answer (gap from the graph, auditable) DIFFERS
    from the flat one (two raw numbers, no gap)."""
    comp_rows = tool_query_triples(
        TENANT_ID,
        concept="comp_band.median.by_department",
        entity_id=ENTITY_ID,
        limit=500,
    )
    market_rows = tool_query_triples(
        TENANT_ID,
        concept="market_benchmark.median.by_job_family",
        entity_id=ENTITY_ID,
        limit=500,
    )

    # The internal median: engineering's comp_band, from workday_hr.
    eng_internal = [
        r for r in comp_rows
        if r.get("property") == "engineering"
        and float(r["value"]) == EXPECTED_INTERNAL_MEDIAN
        and r.get("source_system") == INTERNAL_SOURCE
    ]
    assert eng_internal, (
        f"Flat query must return engineering's internal median "
        f"{EXPECTED_INTERNAL_MEDIAN} from {INTERNAL_SOURCE}. "
        f"comp_band rows for engineering: "
        f"{[(r.get('value'), r.get('source_system')) for r in comp_rows if r.get('property') == 'engineering']}"
    )

    # The market median: software_engineering's benchmark, from radford_comp.
    swe_market = [
        r for r in market_rows
        if r.get("property") == "software_engineering"
        and float(r["value"]) == EXPECTED_MARKET_MEDIAN
        and r.get("source_system") == MARKET_SOURCE
    ]
    assert swe_market, (
        f"Flat query must return software_engineering's market median "
        f"{EXPECTED_MARKET_MEDIAN} from {MARKET_SOURCE}. "
        f"market_benchmark rows for software_engineering: "
        f"{[(r.get('value'), r.get('source_system')) for r in market_rows if r.get('property') == 'software_engineering']}"
    )

    # They are SEPARATE rows from DIFFERENT sources — not one fused fact.
    assert eng_internal[0]["source_system"] != swe_market[0]["source_system"], (
        "The internal and market medians come from the same source — they must "
        "be cross-source (workday_hr vs radford_comp) for the join to be the "
        "thing that synthesizes the gap."
    )

    # NEGATIVE PROOF: no flat row anywhere carries the synthesized gap.
    def carries(rows, target):
        hits = []
        for r in rows:
            for k, v in r.items():
                if isinstance(v, bool):
                    continue
                if isinstance(v, (int, float)) and abs(float(v) - target) < 0.01:
                    hits.append((k, v, r.get("concept"), r.get("property")))
        return hits

    all_rows = comp_rows + market_rows
    gap_pct_hits = carries(all_rows, EXPECTED_GAP_PCT)
    gap_usd_hits = carries(all_rows, EXPECTED_GAP_USD)
    assert gap_pct_hits == [], (
        f"Flat retrieval cannot state the gap, yet a row carried "
        f"{EXPECTED_GAP_PCT}: {gap_pct_hits}. The gap must exist ONLY in the "
        f"traversed edge, never in a fact row."
    )
    assert gap_usd_hits == [], (
        f"Flat retrieval cannot state the gap, yet a row carried "
        f"{EXPECTED_GAP_USD}: {gap_usd_hits}. The gap_usd must exist ONLY in "
        f"the traversed edge."
    )


def test_traverse_graph_in_demo_scope():
    """The wiring: the demo agent's minted MCP token requests traverse_graph,
    so the grounded agent CAN call it (without this, Stage 4 cannot happen —
    the agent is locked out of the graph)."""
    assert "traverse_graph" in panel_b.TOKEN_SCOPE, (
        "demo.panel_b.TOKEN_SCOPE must include 'traverse_graph' — otherwise the "
        f"grounded demo agent cannot traverse. Scope: {panel_b.TOKEN_SCOPE}"
    )
