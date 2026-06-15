"""Edge-provenance reveal — drill from a synthesized edge to its source records
(ContextOS Blueprint §13: "drill to source record IDs, confidence, ingest time").

The hero edge is the cross-source comp-gap (engineering BELOW_MARKET
software_engineering). Its synthesized gap_pct 13.16 / gap_usd 25000 exist only
in the join — no single record holds them. The reveal IS the audit trail: it
returns the two REAL source records the gap was synthesized from —
comp_band engineering 165000 (workday_hr) and market_benchmark
software_engineering 190000 (radford_comp) — each with full provenance
(triple_id, source_field, confidence, ingested_at).

Runs in-process (TestClient) against the SHARED aos-dev store, reading the
ALREADY-INGESTED + already-derived ContextOSDemo entity (NOT re-ingested). The
reveal is exercised through the real GET /api/dcl/graph/edge-provenance endpoint.

Strong, specific assertions (B4): exact source values 165000/190000, exact
source_systems, two DIFFERENT triple_ids, non-null triple_id + ingested_at on
each. Plus the negative proof — no returned source record itself carries the
synthesized gap — and the fail-loud proof: a non-existent coordinate is a
404/422 with a readable message, never an empty 200.
"""

import os

import pytest
from dotenv import load_dotenv

# DCL test convention: load the dev env explicitly so the in-process app and any
# direct reads both resolve aos-dev (the run command also sources it; loading
# here makes the file self-contained).
load_dotenv(".env.development")

from fastapi.testclient import TestClient

from backend.api.main import app

client = TestClient(app)

# The already-ingested + already-derived demo entity (do NOT re-ingest; read it).
TENANT_ID = "51aee6ec-15c3-4fb0-833a-a19bb4511296"
ENTITY_ID = "ContextOSDemo"

# Prod project ref — these tests must NEVER run against it.
_PROD_PROJECT_REF = "gdbmdrouocxjxiohpixr"

# The hero edge coordinate.
_HERO = {
    "entity_id": ENTITY_ID,
    "src_type": "department", "src_key": "engineering",
    "edge_type": "BELOW_MARKET",
    "dst_type": "job_family", "dst_key": "software_engineering",
}


@pytest.fixture(scope="module", autouse=True)
def _guard_dev_db():
    """Hard-assert the active store is aos-dev (NOT prod) before any read.
    The edges are already derived (Stage 3) by the demo ingest — these tests
    only READ provenance, they do not re-ingest or re-derive."""
    db_url = (
        os.environ.get("DATABASE_URL")
        or os.environ.get("SUPABASE_DB_URL")
        or ""
    )
    assert _PROD_PROJECT_REF not in db_url, (
        f"REFUSING TO RUN: the active DATABASE_URL points at the PROD project "
        f"{_PROD_PROJECT_REF!r}. Edge-provenance tests run ONLY against aos-dev."
    )


def _get_provenance(coord: dict):
    return client.get("/api/dcl/graph/edge-provenance", params=coord)


def test_edge_provenance_reveals_both_sources():
    """The reveal returns the TWO real source records the engineering
    BELOW_MARKET gap was synthesized from. User-visible outcome: a comp_band
    record value 165000 from workday_hr AND a market_benchmark record value
    190000 from radford_comp — two DIFFERENT source_systems, two DIFFERENT
    triple_ids, each with a non-null triple_id and ingested_at. The synthesized
    gap_pct 13.16 traces to exactly these two real records."""
    resp = _get_provenance(_HERO)
    assert resp.status_code == 200, f"edge-provenance failed: {resp.status_code} {resp.text}"
    body = resp.json()

    sources = body["sources"]
    by_concept = {s["concept"]: s for s in sources}

    # The internal side: comp_band engineering 165000 from workday_hr.
    internal = by_concept.get("comp_band.median.by_department")
    assert internal is not None, (
        f"expected a comp_band.median.by_department source record; got concepts "
        f"{sorted(by_concept)}"
    )
    assert internal["value"] == 165000, f"internal value expected 165000, got {internal['value']}"
    assert internal["source_system"] == "workday_hr", (
        f"internal source_system expected 'workday_hr', got {internal['source_system']}"
    )
    assert internal["property"] == "engineering", (
        f"internal record must key on the department src_key 'engineering', got "
        f"{internal['property']}"
    )
    assert internal["triple_id"], "internal record must carry a non-null triple_id (the source row id)"
    assert internal["ingested_at"], "internal record must carry a non-null ingested_at (ingest time)"

    # The market side: market_benchmark software_engineering 190000 from radford_comp.
    market = by_concept.get("market_benchmark.median.by_job_family")
    assert market is not None, (
        f"expected a market_benchmark.median.by_job_family source record; got concepts "
        f"{sorted(by_concept)}"
    )
    assert market["value"] == 190000, f"market value expected 190000, got {market['value']}"
    assert market["source_system"] == "radford_comp", (
        f"market source_system expected 'radford_comp', got {market['source_system']}"
    )
    assert market["property"] == "software_engineering", (
        f"market record must key on the job_family dst_key 'software_engineering', got "
        f"{market['property']}"
    )
    assert market["triple_id"], "market record must carry a non-null triple_id (the source row id)"
    assert market["ingested_at"], "market record must carry a non-null ingested_at (ingest time)"

    # The reveal IS the audit trail: two genuinely cross-source records.
    assert internal["source_system"] != market["source_system"], (
        f"the comp-gap must be cross-source: both records are {internal['source_system']!r} "
        f"— the gap would mirror a single source"
    )
    assert internal["triple_id"] != market["triple_id"], (
        f"the two source records must be two DIFFERENT rows; both triple_ids are "
        f"{internal['triple_id']!r}"
    )

    # The synthesized gap the operator drilled FROM, for the audit line.
    assert body["synthesized"]["gap_pct"] == 13.16, (
        f"synthesized gap_pct expected 13.16, got {body['synthesized'].get('gap_pct')}"
    )


def test_edge_provenance_is_real_not_synthetic():
    """Provenance shows the INPUTS, not the derived value. NO returned source
    record itself carries gap_pct 13.16 or gap_usd 25000 — those live only in
    the edge's synthesized block. The records are the raw 165000 / 190000
    inputs; the gap is what the join produced from them."""
    resp = _get_provenance(_HERO)
    assert resp.status_code == 200, f"edge-provenance failed: {resp.status_code} {resp.text}"
    body = resp.json()

    # The synthesized block (what the operator drilled from) DOES hold the gap.
    assert body["synthesized"]["gap_pct"] == 13.16
    assert body["synthesized"]["gap_usd"] == 25000

    # NO source record carries the synthesized gap — not as a value, not as a
    # field. Each is a raw input (165000 or 190000), the gap is absent.
    for s in body["sources"]:
        assert s["value"] not in (13.16, 25000), (
            f"source record {s['concept']} value {s['value']} equals a SYNTHESIZED "
            f"value (gap_pct 13.16 / gap_usd 25000) — provenance must show inputs, "
            f"not the derived gap"
        )
        assert "gap_pct" not in s, (
            f"source record {s['concept']} carries a gap_pct field — the gap lives "
            f"only on the edge, never on the raw input records"
        )
        assert "gap_usd" not in s, (
            f"source record {s['concept']} carries a gap_usd field — the gap lives "
            f"only on the edge, never on the raw input records"
        )

    # The two raw inputs are exactly 165000 and 190000 (and ONLY those).
    values = sorted(s["value"] for s in body["sources"])
    assert values == [165000, 190000], (
        f"the two source inputs must be exactly 165000 and 190000; got {values}"
    )


def test_provenance_gap_fails_loud():
    """A non-existent edge coordinate is a 404 with a readable message — never an
    empty 200 that would read as 'this edge has no sources' (A1). The operator
    sees WHY: the edge does not exist."""
    bad = dict(_HERO, src_key="no_such_department")
    resp = _get_provenance(bad)
    assert resp.status_code in (404, 422), (
        f"a non-existent edge coordinate must fail loud (404/422), got "
        f"{resp.status_code}: {resp.text}"
    )

    body = resp.json()
    detail = body.get("detail")
    message = detail.get("message") if isinstance(detail, dict) else str(detail)
    assert message, f"the failure must carry a readable message, got: {body}"
    # The message names the missing edge — not just a status code.
    assert "no_such_department" in message, (
        f"the readable error must name the missing coordinate; got: {message}"
    )
    assert "BELOW_MARKET" in message, (
        f"the readable error must name the edge type that was not found; got: {message}"
    )

    # And it is NOT an empty-200 success shape masquerading as a result.
    assert "sources" not in body, (
        f"a missing edge must not return a sources list — that would present an "
        f"empty audit trail as a real answer; got: {body}"
    )
