"""Gate 2C acceptance (API grain): standards-track exports.

Operator-visible outcome under test: after this run's per-run-unique tenant
ingests facts for two entities (Gate2C-<tag>-A / Gate2C-<tag>-B), three
declared typed edges across two edge types (engineering BELONGS_TO A,
A HAS auth-api, ops BELONGS_TO B) and one tenant hierarchy link
(workforce -> people_ops_<tag>), the operator downloads:

  GET /api/dcl/export/graph.ttl     — valid Turtle (text/turtle) that
      pyoxigraph (independent store) parses to EXACTLY the triple count
      computed at test time from the ontology YAML + the tenant's own
      hierarchy / edge-type / subgraph APIs; the seeded BELONGS_TO edge
      carries an OWL axiom annotation with sourceSystem=workday,
      derivation=declared and dclIngestId equal to the ingest id the
      subgraph API reports; the literal string "run_id" and the tenant
      UUID appear nowhere in the body (I1/I2).
  GET /api/dcl/export/graph.jsonld  — the same graph as JSON-LD
      (application/ld+json): pyld expands it, and pyoxigraph loads both
      serializations to identical per-predicate triple counts.
  GET /api/dcl/export/metrics.yaml  — MetricFlow-spec YAML
      (application/x-yaml) the real dbt-semantic-interfaces parser+validator
      accept, whose metric name set equals the DCL catalog ids served by
      /api/dcl/semantic-export at test time.

Each endpoint is deterministic across two calls (B14 grain: parsed-set
identity, never raw bytes), 422-loud on missing/malformed tenant_id, and
404-loud (naming the tenant) when the tenant has no graph.

Live-service integration tests: TestClient drives the real FastAPI app
against the aos-dev database. All fixture values are per-run-unique; direct
DB access is used only for end-of-session test-tenant cleanup.
"""

import json
import os
import sys
import uuid
from collections import Counter
from pathlib import Path

import pytest
import yaml

_repo = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_repo))

from dotenv import load_dotenv
load_dotenv(_repo / ".env.development")

import pyoxigraph as ox
from pyld import jsonld as pyld_jsonld
from rdflib import Graph as RdflibGraph
from rdflib.compare import isomorphic
from dbt_semantic_interfaces.parsing.dir_to_model import (
    parse_yaml_files_to_validation_ready_semantic_manifest,
)
from dbt_semantic_interfaces.parsing.objects import YamlConfigFile
from dbt_semantic_interfaces.validations.semantic_manifest_validator import (
    SemanticManifestValidator,
)

from fastapi.testclient import TestClient
from backend.api.main import app
from backend.core.db import get_connection

client = TestClient(app, raise_server_exceptions=False)

# Per-run-unique identity (B14): fresh tenant every run, no fixed-tenant races.
TENANT = str(uuid.uuid4())
TAG = uuid.uuid4().hex[:6]
ENTITY_A = f"Gate2C-{TAG}-A"
ENTITY_B = f"Gate2C-{TAG}-B"
SRC_SYS = "workday"
HIER_PARENT = f"people_ops_{TAG}"

DEPTH_FIELDS = (
    "recognition_basis", "timing_semantics", "scope_boundaries",
    "calculation_methodology", "comparability_rules",
)

OWL = "http://www.w3.org/2002/07/owl#"


# ---------------------------------------------------------------------------
# Seeding (real APIs) + cleanup (direct DB, test tenants only)
# ---------------------------------------------------------------------------

def _push_triples(entity_id: str) -> str:
    run_id = str(uuid.uuid4())
    r = client.post(
        "/api/dcl/ingest-triples",
        json={
            "tenant_id": TENANT, "dcl_ingest_id": run_id, "entity_id": entity_id,
            "snapshot_name": f"{entity_id}-{run_id.replace('-', '')[:4]}",
            "triples": [{
                "entity_id": entity_id, "concept": "revenue.total",
                "property": "amount", "value": 1000.0, "period": "2026-Q1",
                "source_system": SRC_SYS, "source_table": "gate2c_probe",
                "source_field": "amount", "pipe_id": str(uuid.uuid4()),
                "confidence_score": 0.95, "confidence_tier": "exact",
                "fabric_plane": "ipaas",
            }],
        },
    )
    assert r.status_code == 201, f"triple ingest failed: {r.status_code} {r.text}"
    return run_id


def _push_edges(entity_id: str, edges: list[dict]) -> str:
    run_id = str(uuid.uuid4())
    r = client.post(
        "/api/dcl/ingest-edges",
        json={
            "tenant_id": TENANT, "dcl_ingest_id": run_id, "entity_id": entity_id,
            "source_system": SRC_SYS, "edges": edges,
        },
    )
    assert r.status_code == 201, f"edge ingest failed: {r.status_code} {r.text}"
    body = r.json()
    assert body["edges_written"] == len(edges), f"edge writes rejected: {body}"
    assert body["violations"] == []
    return run_id


@pytest.fixture(scope="module", autouse=True)
def seeded_tenant():
    """Seed through the real pipeline surfaces (B5): facts via ingest-triples,
    a 2-edge-type graph via ingest-edges, one tenant hierarchy link."""
    _push_triples(ENTITY_A)
    _push_triples(ENTITY_B)
    # ENTITY_A: a chain crossing BOTH edge types (BELONGS_TO then HAS).
    _push_edges(ENTITY_A, [
        {"src_type": "department", "src_key": "engineering",
         "edge_type": "BELONGS_TO", "dst_type": "org_unit", "dst_key": ENTITY_A},
        {"src_type": "org_unit", "src_key": ENTITY_A,
         "edge_type": "HAS", "dst_type": "service", "dst_key": "auth-api"},
    ])
    _push_edges(ENTITY_B, [
        {"src_type": "department", "src_key": "ops",
         "edge_type": "BELONGS_TO", "dst_type": "org_unit", "dst_key": ENTITY_B},
    ])
    r = client.put("/api/dcl/concepts/hierarchy", json={
        "tenant_id": TENANT, "concept": "workforce", "parent_concept": HIER_PARENT,
    })
    assert r.status_code == 201, r.text
    yield
    # Per-run test-tenant cleanup (direct DB — the sanctioned exception),
    # same scrub as the Gate 1B suite so per-run tenants never pollute the
    # shared dev stack's cross-tenant snapshot selectors.
    with get_connection() as conn:
        with conn.cursor() as cur:
            for table in ("semantic_triples", "entity_edges", "conflict_register",
                          "edge_types", "concept_hierarchy", "resolver_hitl_queue",
                          "tenant_runs"):
                cur.execute(
                    f"DELETE FROM {table} WHERE tenant_id::text = %s", [TENANT],
                )
            conn.commit()


# ---------------------------------------------------------------------------
# Ground truth computed at test time (B8/B10 — never hardcoded totals)
# ---------------------------------------------------------------------------

def _subgraphs() -> list[dict]:
    out = []
    for ent in (ENTITY_A, ENTITY_B):
        r = client.get("/api/dcl/graph/subgraph", params={
            "tenant_id": TENANT, "entity_id": ent, "include_values": "false",
        })
        assert r.status_code == 200, r.text
        out.append(r.json())
    return out


def _expected_export_triple_count() -> int:
    """Mirror of the documented rdf_export content contract, computed from
    the ontology YAML (the spec source) + this tenant's own hierarchy,
    edge-type and subgraph APIs at test time."""
    concepts = yaml.safe_load(
        (_repo / "config" / "ontology_concepts.yaml").read_text()
    )["concepts"]
    hier = client.get("/api/dcl/concepts/hierarchy", params={"tenant_id": TENANT})
    assert hier.status_code == 200, hier.text
    links = hier.json()["tenant_links"]

    n = 0
    ontology_ids = set()
    for c in concepts:
        ontology_ids.add(c["id"])
        n += 1  # owl:Class
        if c.get("name"):
            n += 1
        if c.get("description"):
            n += 1
        n += len(set(c.get("aliases") or []))
        n += sum(1 for f in DEPTH_FIELDS if c.get(f))
        if links.get(c["id"]) or c.get("domain"):
            n += 1  # rdfs:subClassOf
    n += sum(1 for k, p in links.items() if k not in ontology_ids and p)

    et = client.get("/api/dcl/graph/edge-types", params={"tenant_id": TENANT})
    assert et.status_code == 200, et.text
    for spec in et.json()["edge_types"].values():
        n += 1  # owl:ObjectProperty
        if spec.get("description"):
            n += 1
        if spec.get("cardinality"):
            n += 1
        if spec.get("allowed_pairs") is not None:
            n += 1

    node_keys, node_typings, base_edges, edge_rows = set(), set(), set(), 0
    for sub in _subgraphs():
        for nd in sub["nodes"]:
            node_keys.add(nd["node_key"])
            node_typings.add((nd["node_key"], nd["node_type"]))
        for e in sub["edges"]:
            base_edges.add((e["src_key"], e["edge_type"], e["dst_key"]))
            edge_rows += 1
    n += len(node_keys) + len(node_typings) + len(base_edges) + 10 * edge_rows
    return n


def _scan_no_run_id_keys(obj) -> None:
    """I1: no bare run_id key anywhere in a JSON payload."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            assert k != "run_id", f"bare run_id key in JSON response near: {list(obj)}"
            _scan_no_run_id_keys(v)
    elif isinstance(obj, list):
        for v in obj:
            _scan_no_run_id_keys(v)


def _load_turtle(body: str) -> ox.Store:
    store = ox.Store()
    store.load(body.encode(), format=ox.RdfFormat.TURTLE)
    return store


# ---------------------------------------------------------------------------
# 1. graph.ttl
# ---------------------------------------------------------------------------

class TestGraphTurtle:

    def test_missing_tenant_422(self):
        r = client.get("/api/dcl/export/graph.ttl")
        assert r.status_code == 422
        assert r.headers["content-type"].startswith("application/json")
        _scan_no_run_id_keys(r.json())

    def test_turtle_parses_to_ground_truth_count(self):
        r = client.get("/api/dcl/export/graph.ttl", params={"tenant_id": TENANT})
        assert r.status_code == 200, r.text
        assert r.headers["content-type"].startswith("text/turtle")
        assert "attachment" in r.headers["content-disposition"]
        body = r.text
        # I1/I2 on the export body itself
        assert "run_id" not in body, "literal 'run_id' leaked into the Turtle export"
        assert TENANT not in body, "tenant UUID leaked into the Turtle export (I2)"
        store = _load_turtle(body)  # independent validator: pyoxigraph
        expected = _expected_export_triple_count()
        assert len(store) == expected, (
            f"Turtle export triple count {len(store)} != ground-truth "
            f"expectation {expected} computed from the tenant's own APIs"
        )

    def test_seeded_edge_axiom_provenance(self):
        # Ground truth: the BELONGS_TO edge row as the subgraph API serves it.
        sub = client.get("/api/dcl/graph/subgraph", params={
            "tenant_id": TENANT, "entity_id": ENTITY_A, "include_values": "false",
        }).json()
        row = [e for e in sub["edges"]
               if e["edge_type"] == "BELONGS_TO" and e["src_key"] == "engineering"]
        assert len(row) == 1, sub["edges"]
        row = row[0]

        body = client.get(
            "/api/dcl/export/graph.ttl", params={"tenant_id": TENANT}
        ).text
        store = _load_turtle(body)
        q = f"""
        SELECT ?src ?deriv ?ingId WHERE {{
          ?ax a <{OWL}Axiom> ;
              <{OWL}annotatedSource> <urn:dcl:entity:engineering> ;
              <{OWL}annotatedProperty> <urn:dcl:edge:BELONGS_TO> ;
              <{OWL}annotatedTarget> <urn:dcl:entity:{ENTITY_A}> ;
              <urn:dcl:meta:sourceSystem> ?src ;
              <urn:dcl:meta:derivation> ?deriv ;
              <urn:dcl:meta:dclIngestId> ?ingId .
        }}"""
        rows = list(store.query(q))
        assert len(rows) == 1, f"expected exactly one axiom annotation, got {len(rows)}"
        got = {
            "sourceSystem": rows[0]["src"].value,
            "derivation": rows[0]["deriv"].value,
            "dclIngestId": rows[0]["ingId"].value,
        }
        assert got == {
            "sourceSystem": row["source_system"],
            "derivation": row["derivation"],
            "dclIngestId": row["dcl_ingest_id"],
        }, f"axiom provenance {got} != subgraph API ground truth"


# ---------------------------------------------------------------------------
# 2. graph.jsonld
# ---------------------------------------------------------------------------

class TestGraphJsonLd:

    def test_jsonld_expands_and_matches_turtle(self):
        r = client.get("/api/dcl/export/graph.jsonld", params={"tenant_id": TENANT})
        assert r.status_code == 200, r.text
        assert r.headers["content-type"].startswith("application/ld+json")
        assert "attachment" in r.headers["content-disposition"]
        assert "run_id" not in r.text
        assert TENANT not in r.text, "tenant UUID leaked into the JSON-LD export (I2)"
        doc = json.loads(r.text)
        _scan_no_run_id_keys(doc)

        expanded = pyld_jsonld.expand(doc)  # independent validator: pyld
        assert isinstance(expanded, list) and len(expanded) > 0

        # Same graph, two serializations: load BOTH into pyoxigraph and
        # compare totals, distinct-subject counts, and per-predicate counts.
        ttl_store = _load_turtle(
            client.get("/api/dcl/export/graph.ttl", params={"tenant_id": TENANT}).text
        )
        nquads = pyld_jsonld.normalize(
            doc, {"algorithm": "URDNA2015", "format": "application/n-quads"}
        )
        ld_store = ox.Store()
        ld_store.load(nquads.encode(), format=ox.RdfFormat.N_QUADS)

        assert len(ld_store) == len(ttl_store)
        ttl_subjects = {q.subject for q in ttl_store if not isinstance(q.subject, ox.BlankNode)}
        ld_subjects = {q.subject for q in ld_store if not isinstance(q.subject, ox.BlankNode)}
        assert ld_subjects == ttl_subjects, "named-subject sets differ between serializations"
        ttl_preds = Counter(str(q.predicate) for q in ttl_store)
        ld_preds = Counter(str(q.predicate) for q in ld_store)
        assert ld_preds == ttl_preds, (
            f"per-predicate counts differ: {set(ttl_preds.items()) ^ set(ld_preds.items())}"
        )


# ---------------------------------------------------------------------------
# 3. metrics.yaml
# ---------------------------------------------------------------------------

class TestMetricsYaml:

    def test_spec_parser_accepts_and_names_match_catalog(self):
        r = client.get("/api/dcl/export/metrics.yaml", params={"tenant_id": TENANT})
        assert r.status_code == 200, r.text
        assert r.headers["content-type"].startswith("application/x-yaml")
        assert "attachment" in r.headers["content-disposition"]
        body = r.text
        assert "run_id" not in body, "literal 'run_id' leaked into the metrics export"
        assert TENANT not in body, "tenant UUID leaked into the metrics export (I2)"

        # Independent validator: the REAL dbt-semantic-interfaces parser +
        # full semantic-manifest validation.
        result = parse_yaml_files_to_validation_ready_semantic_manifest(
            [YamlConfigFile(filepath="inline:gate2c-export", contents=body)],
            raise_issues_as_exceptions=True,
        )
        manifest = result.semantic_manifest
        SemanticManifestValidator().checked_validations(manifest)

        # Ground truth at test time: the retained JSON export's catalog ids,
        # read from the LIVE dev backend (read-only GET for expected values —
        # in-proc TestClient never runs the lifespan, so the warmup gate holds
        # /api/dcl/semantic-export at 503 in-proc; the live app serves it).
        import httpx
        backend_url = os.environ.get("DCL_BACKEND_URL", "http://localhost:8104")
        cat = httpx.get(f"{backend_url}/api/dcl/semantic-export", timeout=30)
        assert cat.status_code == 200, cat.text
        catalog_ids = {m["id"] for m in cat.json()["metrics"]}
        exported_names = {m.name for m in manifest.metrics}
        assert exported_names == catalog_ids, (
            f"exported metric names != DCL catalog ids; "
            f"missing={sorted(catalog_ids - exported_names)[:5]} "
            f"extra={sorted(exported_names - catalog_ids)[:5]}"
        )
        assert all(m.type.value == "simple" for m in manifest.metrics)


# ---------------------------------------------------------------------------
# 4. Determinism (B14 grain: parsed sets, never raw bytes)
# ---------------------------------------------------------------------------

class TestDeterminism:

    def test_turtle_twice_isomorphic(self):
        b1 = client.get("/api/dcl/export/graph.ttl", params={"tenant_id": TENANT}).text
        b2 = client.get("/api/dcl/export/graph.ttl", params={"tenant_id": TENANT}).text
        g1 = RdflibGraph().parse(data=b1, format="turtle")
        g2 = RdflibGraph().parse(data=b2, format="turtle")
        assert isomorphic(g1, g2), "two Turtle exports are not graph-isomorphic"

    def test_jsonld_twice_isomorphic(self):
        b1 = client.get("/api/dcl/export/graph.jsonld", params={"tenant_id": TENANT}).text
        b2 = client.get("/api/dcl/export/graph.jsonld", params={"tenant_id": TENANT}).text
        n1 = pyld_jsonld.normalize(
            json.loads(b1), {"algorithm": "URDNA2015", "format": "application/n-quads"})
        n2 = pyld_jsonld.normalize(
            json.loads(b2), {"algorithm": "URDNA2015", "format": "application/n-quads"})
        assert n1 == n2, "two JSON-LD exports normalize to different canonical graphs"

    def test_metrics_twice_identical_parsed(self):
        b1 = client.get("/api/dcl/export/metrics.yaml", params={"tenant_id": TENANT}).text
        b2 = client.get("/api/dcl/export/metrics.yaml", params={"tenant_id": TENANT}).text
        assert list(yaml.safe_load_all(b1)) == list(yaml.safe_load_all(b2))


# ---------------------------------------------------------------------------
# 5. Negative paths (loud, readable)
# ---------------------------------------------------------------------------

class TestNegative:

    @pytest.mark.parametrize("path", [
        "/api/dcl/export/graph.ttl",
        "/api/dcl/export/graph.jsonld",
        "/api/dcl/export/metrics.yaml",
    ])
    def test_unknown_tenant_404_names_tenant(self, path):
        ghost = str(uuid.uuid4())  # valid UUID, no data anywhere
        r = client.get(path, params={"tenant_id": ghost})
        assert r.status_code == 404, f"{path}: {r.status_code} {r.text}"
        assert r.headers["content-type"].startswith("application/json")
        detail = r.json()["detail"]
        assert detail["error"] == "NO_GRAPH_FOR_TENANT"
        assert ghost in detail["message"], "404 message must name the tenant"
        assert "no live edges" in detail["message"]
        _scan_no_run_id_keys(r.json())

    @pytest.mark.parametrize("path", [
        "/api/dcl/export/graph.ttl",
        "/api/dcl/export/graph.jsonld",
        "/api/dcl/export/metrics.yaml",
    ])
    def test_malformed_tenant_422(self, path):
        r = client.get(path, params={"tenant_id": "not-a-uuid"})
        assert r.status_code == 422, f"{path}: {r.status_code} {r.text}"
        assert r.headers["content-type"].startswith("application/json")
        detail = r.json()["detail"]
        assert detail["error"] == "TENANT_ID_INVALID"
        assert "not-a-uuid" in detail["message"]
        _scan_no_run_id_keys(r.json())

    def test_unknown_entity_filter_404(self):
        r = client.get("/api/dcl/export/graph.ttl", params={
            "tenant_id": TENANT, "entity_id": f"NoSuchEntity-{TAG}",
        })
        assert r.status_code == 404
        msg = r.json()["detail"]["message"]
        assert f"NoSuchEntity-{TAG}" in msg and TENANT in msg
