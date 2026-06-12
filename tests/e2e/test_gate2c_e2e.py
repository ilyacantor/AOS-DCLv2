"""Gate 2C acceptance (dev stack, :8104): the exported Turtle is a working
graph in a real external triple store.

Operator-visible outcome under test: after this run's per-run-unique tenant
seeds two entities, a two-edge-type chain (engineering BELONGS_TO
Gate2CE2E-<tag>-A, which HAS auth-api) plus a second entity's BELONGS_TO
edge, the operator downloads /api/dcl/export/graph.ttl from the live dev
backend, loads it into Oxigraph (pyoxigraph.Store — THE named external
store), and a SPARQL traversal crossing BOTH seeded edge types
(?x BELONGS_TO ?y . ?y HAS ?z) returns exactly the chain DCL's own
/api/dcl/graph/subgraph API reports at test time — one row:
(engineering, Gate2CE2E-<tag>-A, auth-api) as urn:dcl:entity IRIs. The
loaded store's total triple count equals an independent pyoxigraph parse of
the same body (loadability proven, not asserted).

Acceptance grain: live dcl-dev backend (DCL_BACKEND_URL, default :8104,
aos-dev DB) over HTTP. Per-run-unique fixtures via the real ingest surfaces;
direct DB access only for per-run test-tenant cleanup.
"""

import os
import sys
import uuid
from pathlib import Path
from urllib.parse import quote

import httpx
import pytest

_repo = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_repo))

from dotenv import load_dotenv
load_dotenv(_repo / ".env.development")

import pyoxigraph as ox

DCL_BACKEND = os.environ.get("DCL_BACKEND_URL", "http://localhost:8104")

TENANT = str(uuid.uuid4())
TAG = uuid.uuid4().hex[:6]
ENTITY_A = f"Gate2CE2E-{TAG}-A"
ENTITY_B = f"Gate2CE2E-{TAG}-B"
SRC_SYS = "workday"


def _iri(key: str) -> str:
    """Same IRI rule the export pins: urn:dcl:entity:<percent-encoded key>."""
    return f"urn:dcl:entity:{quote(str(key), safe='-._~')}"


def _scan_no_run_id_keys(obj) -> None:
    if isinstance(obj, dict):
        for k, v in obj.items():
            assert k != "run_id", f"bare run_id key in JSON response near: {list(obj)}"
            _scan_no_run_id_keys(v)
    elif isinstance(obj, list):
        for v in obj:
            _scan_no_run_id_keys(v)


@pytest.fixture(scope="module")
def http():
    with httpx.Client(base_url=DCL_BACKEND, timeout=60) as c:
        r = c.get("/api/health")
        assert r.status_code == 200, f"dev backend not healthy at {DCL_BACKEND}: {r.status_code}"
        yield c


@pytest.fixture(scope="module", autouse=True)
def seeded_tenant(http):
    """Seed through the live ingest surfaces (B5): facts, a 2-edge-type
    chain for ENTITY_A, one edge for ENTITY_B, one tenant hierarchy link."""
    for ent in (ENTITY_A, ENTITY_B):
        run_id = str(uuid.uuid4())
        r = http.post("/api/dcl/ingest-triples", json={
            "tenant_id": TENANT, "dcl_ingest_id": run_id, "entity_id": ent,
            "snapshot_name": f"{ent}-{run_id.replace('-', '')[:4]}",
            "triples": [{
                "entity_id": ent, "concept": "revenue.total", "property": "amount",
                "value": 500.0, "period": "2026-Q1", "source_system": SRC_SYS,
                "source_table": "gate2c_e2e", "source_field": "amount",
                "pipe_id": str(uuid.uuid4()), "confidence_score": 0.95,
                "confidence_tier": "exact", "fabric_plane": "ipaas",
            }],
        })
        assert r.status_code == 201, f"triple ingest failed: {r.status_code} {r.text}"

    r = http.post("/api/dcl/ingest-edges", json={
        "tenant_id": TENANT, "dcl_ingest_id": str(uuid.uuid4()),
        "entity_id": ENTITY_A, "source_system": SRC_SYS,
        "edges": [
            {"src_type": "department", "src_key": "engineering",
             "edge_type": "BELONGS_TO", "dst_type": "org_unit", "dst_key": ENTITY_A},
            {"src_type": "org_unit", "src_key": ENTITY_A,
             "edge_type": "HAS", "dst_type": "service", "dst_key": "auth-api"},
        ],
    })
    assert r.status_code == 201, r.text
    assert r.json()["edges_written"] == 2 and r.json()["violations"] == []
    _scan_no_run_id_keys(r.json())

    r = http.post("/api/dcl/ingest-edges", json={
        "tenant_id": TENANT, "dcl_ingest_id": str(uuid.uuid4()),
        "entity_id": ENTITY_B, "source_system": SRC_SYS,
        "edges": [
            {"src_type": "department", "src_key": "ops",
             "edge_type": "BELONGS_TO", "dst_type": "org_unit", "dst_key": ENTITY_B},
        ],
    })
    assert r.status_code == 201, r.text

    r = http.put("/api/dcl/concepts/hierarchy", json={
        "tenant_id": TENANT, "concept": "workforce",
        "parent_concept": f"people_ops_{TAG}",
    })
    assert r.status_code == 201, r.text

    yield

    # Per-run test-tenant cleanup (direct DB — the sanctioned exception).
    from backend.core.db import get_connection
    with get_connection() as conn:
        with conn.cursor() as cur:
            for table in ("semantic_triples", "entity_edges", "conflict_register",
                          "edge_types", "concept_hierarchy", "resolver_hitl_queue",
                          "tenant_runs"):
                cur.execute(
                    f"DELETE FROM {table} WHERE tenant_id::text = %s", [TENANT],
                )
            conn.commit()


def test_exported_turtle_traverses_in_oxigraph(http):
    """The acceptance story: real endpoint -> Oxigraph -> SPARQL traversal
    crossing both seeded edge types == DCL's own graph API answer."""
    # 1. Export over the live endpoint.
    r = http.get("/api/dcl/export/graph.ttl", params={"tenant_id": TENANT})
    assert r.status_code == 200, f"export failed: {r.status_code} {r.text[:300]}"
    assert r.headers["content-type"].startswith("text/turtle")
    assert "attachment" in r.headers["content-disposition"]
    body = r.text
    assert "run_id" not in body and TENANT not in body  # I1/I2 on the body

    # 2. Load into THE named external store. Loadability is proven by the
    #    loaded total equalling an independent parse of the same bytes.
    store = ox.Store()
    store.load(body.encode(), format=ox.RdfFormat.TURTLE)
    parsed_count = sum(1 for _ in ox.parse(body.encode(), format=ox.RdfFormat.TURTLE))
    assert len(store) == parsed_count, (
        f"store load lost triples: store={len(store)} parse={parsed_count}"
    )
    assert parsed_count > 0

    # 3. Ground truth from DCL's own graph API at test time (B10): every
    #    BELONGS_TO -> HAS chain across the tenant's entities.
    belongs, has = [], []
    for ent in (ENTITY_A, ENTITY_B):
        sub = http.get("/api/dcl/graph/subgraph", params={
            "tenant_id": TENANT, "entity_id": ent, "include_values": "false",
        })
        assert sub.status_code == 200, sub.text
        assert sub.headers["content-type"].startswith("application/json")
        d = sub.json()
        _scan_no_run_id_keys(d)
        for e in d["edges"]:
            if e["edge_type"] == "BELONGS_TO":
                belongs.append((e["src_key"], e["dst_key"]))
            elif e["edge_type"] == "HAS":
                has.append((e["src_key"], e["dst_key"]))
    expected_chains = {
        (_iri(a), _iri(b), _iri(c))
        for (a, b) in belongs
        for (b2, c) in has
        if b2 == b
    }
    assert expected_chains == {(_iri("engineering"), _iri(ENTITY_A), _iri("auth-api"))}, (
        "seed self-check: the API ground truth must contain exactly the seeded chain"
    )

    # 4. SPARQL traversal in Oxigraph crossing BOTH seeded edge types.
    rows = store.query(
        "SELECT ?x ?y ?z WHERE { "
        "?x <urn:dcl:edge:BELONGS_TO> ?y . ?y <urn:dcl:edge:HAS> ?z . }"
    )
    got_chains = {(str(s["x"].value), str(s["y"].value), str(s["z"].value)) for s in rows}
    assert got_chains == expected_chains, (
        f"Oxigraph traversal {sorted(got_chains)} != DCL graph API ground truth "
        f"{sorted(expected_chains)}"
    )
