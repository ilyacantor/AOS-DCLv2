"""
Regression test: graph v2 must have zero L3 orphans.

An L3 orphan is a concept domain node (ontology_X) with no outgoing
consumption link to any L4 persona node. This test exists because the
same bug class — Farm emits a new domain prefix, DCL silently skips it
— has regressed three times. Fail-loud checks in dcl_engine.py and
ingest_triples.py are the primary defense. This test is the
belt-and-suspenders that catches them regressing together.

Hits /api/dcl/run over HTTP against a running DCL service (B1/B2/B6).
Requires DCL up on localhost:8004 with a fresh Farm pipeline run (B15).
"""
import os
import pytest
import requests

DCL_URL = os.getenv("DCL_URL", "http://localhost:8004")
ALL_PERSONAS = ["CFO", "CRO", "COO", "CTO", "CHRO"]
REQUEST_TIMEOUT = 130  # above the 120s engine timeout


def _list_entities():
    """Fetch every entity with active triples in DCL.

    Skips the whole module if DCL is unreachable or empty (B15: pipeline
    must be live). Retries briefly on 5xx to tolerate auto-reload races.
    """
    import time
    last_err = None
    for _ in range(3):
        try:
            r = requests.get(f"{DCL_URL}/api/dcl/entities", timeout=10)
        except requests.ConnectionError as e:
            last_err = e
            time.sleep(1)
            continue
        if r.status_code == 200:
            entities = [e["entity_id"] for e in r.json().get("entities", [])]
            if not entities:
                pytest.skip("No entities in DCL — run a fresh Farm pipeline (B15)")
            return entities
        last_err = f"{r.status_code}: {r.text[:200]}"
        time.sleep(1)
    pytest.skip(f"DCL /api/dcl/entities unreachable after 3 tries: {last_err}")


def _run_graph_v2(entity_id: str):
    """POST /api/dcl/run for one entity and return parsed JSON.

    Retries on 503 warming responses — DCL returns 503 with phase=warming
    for a few seconds after startup or a code reload while the graph
    cache rebuilds. This is a legitimate transient state, not a bug.
    """
    import time
    for attempt in range(5):
        r = requests.post(
            f"{DCL_URL}/api/dcl/run",
            json={
                "mode": "Farm",
                "run_mode": "Dev",
                "personas": ALL_PERSONAS,
                "entity_id": entity_id,
            },
            timeout=REQUEST_TIMEOUT,
        )
        if r.status_code == 503 and "warming" in r.text:
            time.sleep(2)
            continue
        break
    assert r.status_code == 200, (
        f"POST /api/dcl/run entity_id={entity_id} returned "
        f"{r.status_code}: {r.text[:500]}"
    )
    return r.json()


def pytest_generate_tests(metafunc):
    """Parametrize any test that takes `entity_id` over all live DCL entities."""
    if "entity_id" in metafunc.fixturenames:
        metafunc.parametrize("entity_id", _list_entities())


def _partition_nodes_and_links(payload):
    graph = payload["graph"]
    nodes = graph.get("nodes", [])
    links = graph.get("links", [])
    l3_ids = {n["id"] for n in nodes if n.get("level") == "L3"}
    l4_ids = {n["id"] for n in nodes if n.get("level") == "L4"}
    consumption_sources = {
        link["source"]
        for link in links
        if link.get("flowType") == "consumption"
        and link.get("target") in l4_ids
    }
    return nodes, links, l3_ids, l4_ids, consumption_sources


def test_graph_v2_has_no_l3_orphans(entity_id):
    """Every L3 (domain) node must consume into at least one L4 persona.

    What the user sees: a domain node in graph v2 Sankey with no link
    flowing into any persona column — the exact bug this test guards.
    """
    payload = _run_graph_v2(entity_id)
    _, _, l3_ids, _, consumption_sources = _partition_nodes_and_links(payload)

    assert l3_ids, (
        f"graph for {entity_id} has no L3 nodes — pipeline empty?"
    )

    orphans = sorted(l3_ids - consumption_sources)
    assert not orphans, (
        f"L3 orphans in {entity_id} (domains with no persona consumer): "
        f"{orphans}. Add the missing domain prefix to the correct persona "
        f"in config/persona_domains.yaml."
    )


def test_graph_v2_has_all_five_personas(entity_id):
    """All 5 personas must render as L4 nodes when all 5 are requested."""
    payload = _run_graph_v2(entity_id)
    _, _, _, l4_ids, _ = _partition_nodes_and_links(payload)
    expected = {f"bll_{p.lower()}" for p in ALL_PERSONAS}
    missing = expected - l4_ids
    assert not missing, (
        f"L4 persona nodes missing for {entity_id}: {sorted(missing)}"
    )


def test_graph_v2_core_financial_domains_reach_cfo(entity_id):
    """Core Farm-emitted financial prefixes must flow to CFO.

    Guards against the specific regression the user reported: "pure
    financial metrics orphaned in L3". If these aren't reaching CFO,
    the persona_domains.yaml mapping has drifted again.
    """
    payload = _run_graph_v2(entity_id)
    _, links, l3_ids, _, _ = _partition_nodes_and_links(payload)

    cfo_id = "bll_cfo"
    cfo_l3_sources = {
        link["source"]
        for link in links
        if link.get("target") == cfo_id and link.get("flowType") == "consumption"
    }

    required_financial_domains = {
        "ontology_revenue", "ontology_arr", "ontology_gl",
        "ontology_cogs", "ontology_opex", "ontology_pnl",
    }
    present_in_l3 = required_financial_domains & l3_ids
    missing_cfo = present_in_l3 - cfo_l3_sources
    assert not missing_cfo, (
        f"Financial L3 domains present in {entity_id} but NOT routed to "
        f"CFO: {sorted(missing_cfo)}. Update CFO in persona_domains.yaml."
    )
