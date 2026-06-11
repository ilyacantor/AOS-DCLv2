"""Gate 1B — entity graph: typed edges, constraints, hierarchy, traversal.

Store-level + API-level tests against the live dev store (same convention as
test_bitemporal_store.py — real Postgres, no mocks). Each test run uses its
own fresh tenant UUID so runs are isolated and re-runnable (B14); rows stay
behind as bi-temporal history, which is the model's contract, not residue.

Covers the Gate 1B verification spec:
  - records ingest producing a DERIVED edge (org membership from workforce shape)
  - a DECLARED edge through POST /api/dcl/ingest-edges
  - typed traversal returning both (REST + MCP tool)
  - constraint violation flagged into conflict_register (structural class)
  - as-of traversal showing pre-supersession topology
  - identity enforcement (422 / loud errors) at every boundary
  - concept hierarchy participating in reads (include_descendants)
"""

import time
import uuid

import pytest
from fastapi.testclient import TestClient

from backend.api.main import app
from backend.core.db import get_connection
from backend.db.edge_store import (
    EdgeIdentityError,
    get_edge_store,
)
from backend.engine.mcp_tools import dispatch

client = TestClient(app)

ENTITY = "Gate1BTest-0001"
SRC_SYS = "workday"

# Tenants this module created — scrubbed at session end so the module's
# fresh-tenant snapshots don't pollute the SHARED dev stack's cross-tenant
# snapshot selectors (the #12/#35 family: follow-latest UIs pick the newest
# snapshot across tenants; a pile of 2-triple Gate1BTest runs displaces the
# rich Farm runs the monitoring e2e specs select). Same established pattern
# as Farm's conftest test-tenant cleanup. Hard DELETE here is test-data
# retention (the store's operator scope), not a lifecycle write.
_CREATED_TENANTS: list[str] = []


@pytest.fixture(scope="session", autouse=True)
def _scrub_module_tenants():
    yield
    if not _CREATED_TENANTS:
        return
    with get_connection() as conn:
        with conn.cursor() as cur:
            for table in ("semantic_triples", "entity_edges", "conflict_register",
                          "edge_types", "concept_hierarchy", "resolver_hitl_queue",
                          "tenant_runs"):
                # tenant_id is UUID on some tables, TEXT on others — the ::text
                # cast compares uniformly.
                cur.execute(
                    f"DELETE FROM {table} WHERE tenant_id::text = ANY(%s)",
                    [_CREATED_TENANTS],
                )
            conn.commit()


def _edge(src_t, src_k, et, dst_t, dst_k, run_id, **over):
    e = {
        "src_type": src_t, "src_key": src_k, "edge_type": et,
        "dst_type": dst_t, "dst_key": dst_k,
        "properties": None,
        "source_system": SRC_SYS, "source_table": None, "source_field": "test",
        "pipe_id": None, "dcl_ingest_id": run_id, "source_run_tag": None,
        "confidence_score": 1.0, "confidence_tier": "exact",
        "fabric_plane": None, "fabric_product": None,
        "derivation": "declared",
    }
    e.update(over)
    return e


@pytest.fixture()
def tenant_id():
    t = str(uuid.uuid4())
    _CREATED_TENANTS.append(t)
    return t


class TestEdgeStoreLifecycle:

    def test_01_insert_and_read_typed(self, tenant_id):
        run = str(uuid.uuid4())
        res = get_edge_store().assert_edges(tenant_id, ENTITY, [
            _edge("department", "engineering", "BELONGS_TO", "org_unit", ENTITY, run),
            _edge("org_unit", ENTITY, "HAS", "service", "auth-api", run),
        ])
        assert res.written == 2
        assert res.violations == []
        out = get_edge_store().get_neighbors(
            tenant_id, ENTITY, "department", "engineering")
        assert len(out) == 1
        assert out[0]["edge_type"] == "BELONGS_TO"
        assert out[0]["dst_key"] == ENTITY
        assert out[0]["is_active"] is True
        assert out[0]["dcl_ingest_id"] == run          # namespaced, I1
        assert "run_id" not in out[0]

    def test_02_reassert_supersedes_not_deletes(self, tenant_id):
        run1, run2 = str(uuid.uuid4()), str(uuid.uuid4())
        store = get_edge_store()
        store.assert_edges(tenant_id, ENTITY, [
            _edge("person", "alice", "REPORTS_TO", "person", "bob", run1),
        ])
        t_between = None
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT now()")
                t_between = cur.fetchone()[0].isoformat()
        time.sleep(0.05)
        # correction: alice now reports to carol — same coordinates? No:
        # REPORTS_TO is many_to_one so re-pointing means superseding the old
        # row. Re-assert with the NEW dst; the constraint engine must treat
        # the in-batch re-assertion correctly only when coordinates match —
        # a CHANGED dst is a different coordinate, so first supersede via
        # replace of the specific coordinate: simulate the real correction
        # path = re-assert same coordinates with new properties, then check
        # changing topology via replace.
        res = store.assert_edges(tenant_id, ENTITY, [
            _edge("person", "alice", "REPORTS_TO", "person", "bob", run2,
                  properties={"weight": 1.0}),
        ])
        assert res.written == 1
        assert res.superseded == 1                      # old row closed, not deleted
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT count(*) FROM entity_edges WHERE tenant_id = %s AND entity_id = %s",
                    [tenant_id, ENTITY],
                )
                total = cur.fetchone()[0]
        assert total == 2                               # history grows, nothing deleted
        # as-of BEFORE the correction sees the original row as live
        asof = store.get_neighbors(
            tenant_id, ENTITY, "person", "alice", as_of=t_between)
        assert len(asof) == 1
        assert asof[0]["properties"] is None            # the pre-correction row

    def test_03_identity_required(self, tenant_id):
        store = get_edge_store()
        with pytest.raises(EdgeIdentityError, match="tenant_id"):
            store.assert_edges("", ENTITY, [_edge("a", "1", "HAS", "b", "2", str(uuid.uuid4()))])
        with pytest.raises(EdgeIdentityError, match="not a valid UUID"):
            store.assert_edges("not-a-uuid", ENTITY, [_edge("a", "1", "HAS", "b", "2", str(uuid.uuid4()))])
        with pytest.raises(EdgeIdentityError, match="entity_id"):
            store.get_neighbors(tenant_id, "", "a", "1")


class TestConstraintViolations:

    def test_04_many_to_one_violation_flags_register(self, tenant_id):
        run = str(uuid.uuid4())
        store = get_edge_store()
        res = store.assert_edges(tenant_id, ENTITY, [
            _edge("department", "sales", "BELONGS_TO", "org_unit", ENTITY, run),
            # second BELONGS_TO from the SAME src to a DIFFERENT dst — violates many_to_one
            _edge("department", "sales", "BELONGS_TO", "org_unit", "OtherOrg-9999", run),
        ])
        assert res.written == 1
        assert len(res.violations) == 1
        v = res.violations[0]
        assert v["conflict_class"] == "edge_cardinality"
        assert v["edge"]["dst_key"] == "OtherOrg-9999"
        # the violation is IN THE REGISTER (structural class), not silently dropped
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT conflict_type, conflict_class, concept, property, status, claims "
                    "FROM conflict_register WHERE tenant_id = %s AND entity_id = %s "
                    "AND dcl_ingest_id = %s",
                    [tenant_id, ENTITY, run],
                )
                rows = cur.fetchall()
        assert len(rows) == 1
        ctype, cclass, concept, prop, status, claims = rows[0]
        assert ctype == "structural"
        assert cclass == "edge_cardinality"
        assert concept == "edge.BELONGS_TO"
        assert prop == "department:sales->org_unit:OtherOrg-9999"
        assert status == "open"
        assert claims["edge"]["src_key"] == "sales"
        assert claims["conflicting_with"]["dst_key"] == ENTITY
        # and the graph holds ONLY the admissible edge
        live = store.get_neighbors(tenant_id, ENTITY, "department", "sales")
        assert len(live) == 1
        assert live[0]["dst_key"] == ENTITY

    def test_05_unregistered_type_flags_register(self, tenant_id):
        run = str(uuid.uuid4())
        res = get_edge_store().assert_edges(tenant_id, ENTITY, [
            _edge("a", "1", "MADE_UP_TYPE", "b", "2", run),
        ])
        assert res.written == 0
        assert res.violations[0]["conflict_class"] == "edge_type_unregistered"

    def test_06_tenant_defined_type_with_pair_rule(self, tenant_id):
        run = str(uuid.uuid4())
        # define a custom type allowing only (service -> metric)
        r = client.put("/api/dcl/graph/edge-types", json={
            "tenant_id": tenant_id, "edge_type": "EMITS",
            "description": "service emits a metric stream",
            "cardinality": "many_to_many",
            "allowed_pairs": [["service", "metric"]],
        })
        assert r.status_code == 201, r.text
        res = get_edge_store().assert_edges(tenant_id, ENTITY, [
            _edge("service", "auth-api", "EMITS", "metric", "latency_p99", run),
            _edge("department", "sales", "EMITS", "metric", "quota", run),  # pair disallowed
        ])
        assert res.written == 1
        assert res.violations[0]["conflict_class"] == "edge_pair_disallowed"


class TestIngestSurfacesAndTraversal:

    def test_07_records_ingest_derives_membership_edges(self, tenant_id):
        """The canonical records path produces DERIVED edges: workforce-shaped
        records (headcount_by_department) → department BELONGS_TO org."""
        run = str(uuid.uuid4())
        body = {
            "tenant_id": tenant_id, "run_id": run, "entity_id": ENTITY,
            "pipes": [{
                "pipe_id": str(uuid.uuid4()),
                "source_system": "snowflake",
                "fabric_plane": "bi",
                "domain": "operations",
                "records": [
                    {"period": "2026-Q1",
                     "headcount_total": 235,
                     "headcount_by_department": {"engineering": 80, "sales": 60},
                     "uptime_pct_by_service": {"auth-api": 99.95}},
                ],
            }],
        }
        r = client.post(f"/api/dcl/ingest-records?replace=true", json=body)
        assert r.status_code == 201, r.text
        d = r.json()
        assert d["edges_derived"] == 3        # 2 departments BELONGS_TO + 1 service HAS
        assert d["edges_written"] == 3
        assert d["edge_violations"] == []
        assert d["triples_written"] > 0

        # traversal returns them TYPED
        r = client.get("/api/dcl/graph/neighbors", params={
            "tenant_id": tenant_id, "entity_id": ENTITY,
            "node_type": "org_unit", "node_key": ENTITY,
        })
        assert r.status_code == 200
        d = r.json()
        types = sorted({e["edge_type"] for e in d["edges"]})
        assert types == ["BELONGS_TO", "HAS"]
        assert {n["node_key"] for n in d["neighbors"]} == {"engineering", "sales", "auth-api"}
        derivs = {e["derivation"] for e in d["edges"]}
        assert derivs == {"derived"}
        # provenance present on every edge
        for e in d["edges"]:
            assert e["source_system"] == "snowflake"
            assert e["source_field"] in ("headcount_by_department", "uptime_pct_by_service")

    def test_08_declared_edges_and_typed_filter(self, tenant_id):
        run = str(uuid.uuid4())
        r = client.post("/api/dcl/ingest-edges", json={
            "tenant_id": tenant_id, "run_id": run, "entity_id": ENTITY,
            "source_system": "workday",
            "edges": [
                {"src_type": "person", "src_key": "dana", "edge_type": "REPORTS_TO",
                 "dst_type": "person", "dst_key": "erin"},
                {"src_type": "department", "src_key": "engineering", "edge_type": "GENERATES",
                 "dst_type": "artifact", "dst_key": "release-train",
                 "properties": {"cadence": "quarterly"}},
            ],
        })
        assert r.status_code == 201, r.text
        d = r.json()
        assert d["edges_written"] == 2
        assert d["violations"] == []

        # type filter returns only the requested type
        r = client.get("/api/dcl/graph/neighbors", params={
            "tenant_id": tenant_id, "entity_id": ENTITY,
            "node_type": "department", "node_key": "engineering",
            "edge_type": "GENERATES",
        })
        d = r.json()
        assert d["edge_count"] == 1
        assert d["edges"][0]["edge_type"] == "GENERATES"
        assert d["edges"][0]["properties"] == {"cadence": "quarterly"}
        assert d["edges"][0]["derivation"] == "declared"

    def test_09_ingest_edges_identity_422(self, tenant_id):
        r = client.post("/api/dcl/ingest-edges", json={
            "tenant_id": tenant_id, "run_id": str(uuid.uuid4()), "entity_id": "  ",
            "source_system": "workday",
            "edges": [{"src_type": "a", "src_key": "1", "edge_type": "HAS",
                       "dst_type": "b", "dst_key": "2"}],
        })
        assert r.status_code == 422
        assert r.json()["detail"]["error"] == "ENTITY_ID_REQUIRED"

        r = client.post("/api/dcl/ingest-edges", json={
            "tenant_id": "not-a-uuid", "run_id": str(uuid.uuid4()), "entity_id": ENTITY,
            "source_system": "workday",
            "edges": [{"src_type": "a", "src_key": "1", "edge_type": "HAS",
                       "dst_type": "b", "dst_key": "2"}],
        })
        assert r.status_code in (400, 422)

    def test_10_asof_traversal_pre_supersession(self, tenant_id):
        """Re-running the records path with a reorg shows the OLD topology
        under as_of and the NEW topology live."""
        run1, run2 = str(uuid.uuid4()), str(uuid.uuid4())
        pipe = {
            "pipe_id": str(uuid.uuid4()), "source_system": "snowflake",
            "fabric_plane": "bi", "domain": "operations",
        }
        r = client.post("/api/dcl/ingest-records?replace=true", json={
            "tenant_id": tenant_id, "run_id": run1, "entity_id": ENTITY,
            "pipes": [{**pipe, "records": [
                {"period": "2026-Q1", "headcount_total": 100,
                 "headcount_by_department": {"engineering": 50, "sales": 50}},
            ]}],
        })
        assert r.status_code == 201, r.text
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT now()")
                t_before_reorg = cur.fetchone()[0].isoformat()
        time.sleep(0.05)
        # reorg: sales dissolved into 'revenue_org'
        r = client.post("/api/dcl/ingest-records?replace=true", json={
            "tenant_id": tenant_id, "run_id": run2, "entity_id": ENTITY,
            "pipes": [{**pipe, "pipe_id": str(uuid.uuid4()), "records": [
                {"period": "2026-Q2", "headcount_total": 100,
                 "headcount_by_department": {"engineering": 50, "revenue_org": 50}},
            ]}],
        })
        assert r.status_code == 201, r.text

        live = client.get("/api/dcl/graph/subgraph", params={
            "tenant_id": tenant_id, "entity_id": ENTITY, "include_values": False,
        }).json()
        live_depts = {n["node_key"] for n in live["nodes"] if n["node_type"] == "department"}
        assert live_depts == {"engineering", "revenue_org"}

        old = client.get("/api/dcl/graph/subgraph", params={
            "tenant_id": tenant_id, "entity_id": ENTITY, "include_values": False,
            "as_of": t_before_reorg,
        }).json()
        old_depts = {n["node_key"] for n in old["nodes"] if n["node_type"] == "department"}
        assert old_depts == {"engineering", "sales"}   # pre-supersession topology

    def test_11_mcp_traverse_graph(self, tenant_id):
        run = str(uuid.uuid4())
        get_edge_store().assert_edges(tenant_id, ENTITY, [
            _edge("department", "engineering", "BELONGS_TO", "org_unit", ENTITY, run),
        ])
        out = dispatch(tenant_id, "traverse_graph", {
            "entity_id": ENTITY, "node_type": "org_unit", "node_key": ENTITY,
        })
        assert out["edges"][0]["edge_type"] == "BELONGS_TO"
        assert out["neighbors"] == [{"node_type": "department", "node_key": "engineering"}]
        # whole-subgraph form
        out = dispatch(tenant_id, "traverse_graph", {"entity_id": ENTITY})
        assert {n["node_type"] for n in out["nodes"]} == {"department", "org_unit"}

    def test_12_inspector_shape(self, tenant_id):
        """Hero inspector: type, domain, values, relationships."""
        run = str(uuid.uuid4())
        r = client.post("/api/dcl/ingest-records?replace=true", json={
            "tenant_id": tenant_id, "run_id": run, "entity_id": ENTITY,
            "pipes": [{
                "pipe_id": str(uuid.uuid4()), "source_system": "snowflake",
                "fabric_plane": "bi", "domain": "operations",
                "records": [
                    {"period": "2026-Q1",
                     "headcount_by_department": {"engineering": 80}},
                ],
            }],
        })
        assert r.status_code == 201, r.text
        d = client.get("/api/dcl/graph/inspector", params={
            "tenant_id": tenant_id, "entity_id": ENTITY,
            "node_type": "department", "node_key": "engineering",
        }).json()
        assert d["node"]["node_type"] == "department"
        assert d["relationship_count"] == 1
        assert d["relationships"][0]["edge_type"] == "BELONGS_TO"
        assert d["relationships"][0]["direction"] == "out"
        assert d["relationships"][0]["other"] == {"node_type": "org_unit", "node_key": ENTITY}
        # values joined from the records-path triple shape (headcount.by_department)
        assert d["values"]["headcount.by_department"]["value"] == 80
        assert d["values"]["headcount.by_department"]["period"] == "2026-Q1"
        assert d["domains"] == ["headcount"]


class TestConceptHierarchyReads:

    def test_13_hierarchy_view_and_descendants(self, tenant_id):
        d = client.get("/api/dcl/concepts/hierarchy", params={
            "tenant_id": tenant_id, "concept": "workforce",
        }).json()
        assert d["parent"] == "hr"                     # ontology-derived default
        d = client.get("/api/dcl/concepts/hierarchy", params={
            "tenant_id": tenant_id, "concept": "hr", "include_descendants": True,
        }).json()
        assert "workforce" in d["expansion"]["exact"]
        assert "workforce" in d["expansion"]["prefixes"]

    def test_14_tenant_link_overrides_and_cycle_rejected(self, tenant_id):
        r = client.put("/api/dcl/concepts/hierarchy", json={
            "tenant_id": tenant_id, "concept": "workforce", "parent_concept": "people_ops",
        })
        assert r.status_code == 201
        d = client.get("/api/dcl/concepts/hierarchy", params={
            "tenant_id": tenant_id, "concept": "workforce",
        }).json()
        assert d["parent"] == "people_ops"
        r = client.put("/api/dcl/concepts/hierarchy", json={
            "tenant_id": tenant_id, "concept": "people_ops", "parent_concept": "workforce",
        })
        assert r.status_code == 422                     # cycle, loudly rejected

    def test_15_query_triples_include_descendants(self, tenant_id):
        """Hierarchy participates in reads: querying the parent domain returns
        the child root's triples."""
        run = str(uuid.uuid4())
        r = client.post("/api/dcl/ingest-records?replace=true", json={
            "tenant_id": tenant_id, "run_id": run, "entity_id": ENTITY,
            "pipes": [{
                "pipe_id": str(uuid.uuid4()), "source_system": "snowflake",
                "fabric_plane": "bi", "domain": "operations",
                "records": [{"period": "2026-Q1", "headcount_total": 235,
                             "attrition_rate": 0.12}],
            }],
        })
        assert r.status_code == 201, r.text
        rows = dispatch(tenant_id, "query_triples", {
            "concept": "hr", "entity_id": ENTITY, "include_descendants": True,
        })
        concepts = {t["concept"] for t in rows}
        assert "workforce.headcount.total" in concepts
        assert "workforce.attrition_rate" in concepts
        # without descendants the domain-as-concept read finds nothing — the
        # expansion is what makes the parent read work
        rows = dispatch(tenant_id, "query_triples", {
            "concept": "hr", "entity_id": ENTITY,
        })
        assert rows == []
