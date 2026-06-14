"""Gate 2B acceptance (API grain): persona-aware query execution.

Operator-visible outcome under test: this run's per-run-unique tenant seeds
one triple in a CFO-only domain, one in a CRO-only domain, and one in a
shared domain (domains derived from config/persona_domains.yaml AT RUNTIME,
never hardcoded). The same question asked as CFO vs CRO then returns exactly
the disjoint+shared concept sets on MCP query_triples, /api/dcl/triples/
browse, and browse-batch; /api/dcl/resolve scopes concept location the same
way (a CRO asking for a CFO-only concept gets the detailed persona-exclusion
reason). Personaless calls return the exact union with the pre-existing
response shape (byte-identical keys, rows still carrying run_id on browse).
Unknown persona is a readable 422/tool error naming the valid keys; an
explicit domain (or dotted concept) outside the persona's list is a loud
conflict error, never a silent narrowing. Every persona-scoped HTTP answer
appends exactly one decision_traces row whose payload carries the persona
and whose decision_type names the surface; personaless calls append zero.
The legacy HTTP shim (POST /api/mcp/tools/call) is held to the same bar:
a persona-scoped query_triples through it appends exactly one trace with
decision_type='query_triples'; a personaless shim call appends zero.

Live-service integration tests: TestClient drives the real FastAPI app
against the aos-dev database. All fixture values are per-run-unique;
direct DB access is used only for per-run test-tenant cleanup.
"""

import os
import sys
import uuid
from pathlib import Path

import pytest
import yaml

_repo = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_repo))

from dotenv import load_dotenv
load_dotenv(_repo / ".env.development")

# Same single-var secret resolution as the wp5/gate2a suites: prefer the env,
# else the live server's shim secret — ONE var from `.env`, never the whole
# file (its DATABASE_URL is prod).
if not os.environ.get("DCL_MCP_TOKEN_SECRET"):
    from dotenv import dotenv_values
    _live_secret = dotenv_values(_repo / ".env").get("DCL_MCP_TOKEN_SECRET")
    if _live_secret:
        os.environ["DCL_MCP_TOKEN_SECRET"] = _live_secret
os.environ.setdefault("DCL_MCP_TOKEN_SECRET", "gate2b-test-secret-do-not-use-in-prod")

from fastapi.testclient import TestClient
from backend.api.main import app
from backend.core.db import get_connection
from backend.engine.mcp_tools import MCPToolError, dispatch

client = TestClient(app, raise_server_exceptions=False)

TENANT = str(uuid.uuid4())
TAG = uuid.uuid4().hex[:6]
ENTITY = f"Gate2B-{TAG}"
PERIOD = "2026-Q1"
PIPE = str(uuid.uuid4())


def _derive_persona_map() -> dict:
    """Derive the test's domain expectations from config/persona_domains.yaml
    AT RUNTIME (B8/B10 — the YAML is the spec; never hardcode the lists).

    Picks one ingest-valid domain that is CFO-only, one CRO-only, one in
    both. Preference order makes the seeded concepts readable, but any
    registry-valid member works.
    """
    cfg = yaml.safe_load((_repo / "config" / "persona_domains.yaml").read_text())
    personas = cfg["personas"]
    cfo = set(personas["CFO"]["domains"])
    cro = set(personas["CRO"]["domains"])

    from backend.registry.concept_registry import ConceptRegistry
    reg = ConceptRegistry()

    def pick(candidates: set, preferred: list[str], label: str) -> str:
        for p in preferred:
            if p in candidates and reg.is_valid_concept(p):
                return p
        for p in sorted(candidates):
            if reg.is_valid_concept(p):
                return p
        raise AssertionError(
            f"persona_domains.yaml has no ingest-valid {label} domain — "
            f"candidates {sorted(candidates)}"
        )

    return {
        "cfo_only": pick(cfo - cro, ["cogs"], "CFO-only"),
        "cro_only": pick(cro - cfo, ["account"], "CRO-only"),
        "shared": pick(cfo & cro, ["revenue"], "CFO∩CRO"),
        "valid_keys": sorted(personas.keys()),
        "cfo_domains": sorted(cfo),
        "cro_domains": sorted(cro),
    }


PMAP = _derive_persona_map()
CONCEPT_CFO = f"{PMAP['cfo_only']}.total"
CONCEPT_CRO = f"{PMAP['cro_only']}.total"
CONCEPT_SHARED = f"{PMAP['shared']}.total"
ALL_CONCEPTS = {CONCEPT_CFO, CONCEPT_CRO, CONCEPT_SHARED}
EXPECTED = {
    "CFO": {CONCEPT_CFO, CONCEPT_SHARED},
    "CRO": {CONCEPT_CRO, CONCEPT_SHARED},
}


def _triple(concept, value):
    return {
        "entity_id": ENTITY, "concept": concept, "property": "amount",
        "value": value, "period": PERIOD, "source_system": "gate2b_src",
        "source_table": "gate2b_probe", "source_field": "amount",
        "pipe_id": PIPE, "confidence_score": 0.95, "confidence_tier": "exact",
        "fabric_plane": "ipaas",
    }


def _cleanup():
    with get_connection() as conn:
        with conn.cursor() as cur:
            for sql in (
                "DELETE FROM semantic_triples WHERE tenant_id = %s",
                "DELETE FROM tenant_runs WHERE tenant_id = %s",
            ):
                cur.execute(sql, (TENANT,))
            conn.commit()


def _traces(**params):
    params.setdefault("tenant_id", TENANT)
    resp = client.get("/api/dcl/traces", params=params)
    assert resp.status_code == 200, resp.text
    return resp.json()


def _keys_of(obj):
    """Recursive key iterator for the I1 no-run_id-key scan."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield str(k)
            yield from _keys_of(v)
    elif isinstance(obj, list):
        for item in obj:
            yield from _keys_of(item)


@pytest.fixture(scope="module", autouse=True)
def gate2b_seed():
    """Seed this run's three-domain triples through the real ingest path and
    build the in-proc semantic graph via the real startup builder (the
    module-level TestClient never enters the lifespan, so warmup's
    rebuild_graph() is invoked here exactly as startup does)."""
    from backend.engine.graph_store import rebuild_graph
    rebuild_graph()

    run_id = str(uuid.uuid4())
    resp = client.post(
        "/api/dcl/ingest-triples",
        json={"tenant_id": TENANT, "dcl_ingest_id": run_id, "entity_id": ENTITY,
              "snapshot_name": f"{ENTITY}-{run_id.replace('-', '')[:4]}",
              "triples": [
                  _triple(CONCEPT_CFO, 111.0),
                  _triple(CONCEPT_CRO, 222.0),
                  _triple(CONCEPT_SHARED, 333.0),
              ]},
    )
    assert resp.status_code == 201, f"seed ingest failed: {resp.status_code} {resp.text}"
    yield
    _cleanup()


class TestMCPQueryTriples:
    def test_01_cfo_vs_cro_exact_disjoint_plus_shared_sets(self):
        """The same question (persona-only scope, this entity+period) returns
        exactly the persona's domain-scoped triple set — disjoint singles
        plus the shared concept, per the runtime-derived YAML map."""
        for persona, expected in EXPECTED.items():
            rows = dispatch(TENANT, "query_triples",
                            {"persona": persona, "entity_id": ENTITY,
                             "period": PERIOD})
            got = {r["concept"] for r in rows}
            assert got == expected, (
                f"{persona} expected exactly {sorted(expected)}, got {sorted(got)}"
            )
            for r in rows:
                assert str(r["tenant_id"]) == TENANT
                assert "dcl_ingest_id" in r and "run_id" not in r, (
                    f"I1: persona-scoped row must carry dcl_ingest_id: {sorted(r)}"
                )

    def test_02_personaless_union_unchanged(self):
        """Personaless query_triples (unqualified concept 'total') returns
        the exact union of all three seeded concepts — behavior as today."""
        rows = dispatch(TENANT, "query_triples",
                        {"concept": "total", "entity_id": ENTITY,
                         "period": PERIOD})
        got = {r["concept"] for r in rows}
        assert got == ALL_CONCEPTS, (
            f"personaless union expected {sorted(ALL_CONCEPTS)}, got {sorted(got)}"
        )

    def test_03_unknown_persona_tool_error_names_valid_keys(self):
        with pytest.raises(MCPToolError) as exc:
            dispatch(TENANT, "query_triples",
                     {"persona": "cfo", "entity_id": ENTITY})
        msg = str(exc.value)
        assert "'cfo'" in msg
        for key in PMAP["valid_keys"]:
            assert key in msg, f"valid persona key {key} missing from: {msg}"

    def test_04_explicit_domain_conflict_is_loud(self):
        """Explicit domain (or dotted concept) outside the persona's list is
        a conflict error naming both sides — never a silent narrowing."""
        with pytest.raises(MCPToolError) as exc:
            dispatch(TENANT, "query_triples",
                     {"persona": "CFO", "domain": PMAP["cro_only"]})
        msg = str(exc.value)
        assert f"'{PMAP['cro_only']}'" in msg and "'CFO'" in msg
        assert "conflict" in msg.lower()

        with pytest.raises(MCPToolError) as exc:
            dispatch(TENANT, "query_triples",
                     {"persona": "CFO", "concept": CONCEPT_CRO})
        msg = str(exc.value)
        assert f"'{CONCEPT_CRO}'" in msg and "'CFO'" in msg

    def test_05_in_scope_domain_with_persona_is_allowed(self):
        rows = dispatch(TENANT, "query_triples",
                        {"persona": "CFO", "domain": PMAP["cfo_only"],
                         "entity_id": ENTITY, "period": PERIOD})
        assert {r["concept"] for r in rows} == {CONCEPT_CFO}


class TestBrowse:
    def test_06_cfo_vs_cro_exact_sets_with_identity(self):
        for persona, expected in EXPECTED.items():
            resp = client.get("/api/dcl/triples/browse",
                              params={"tenant_id": TENANT, "entity_id": ENTITY,
                                      "period": PERIOD, "persona": persona})
            assert resp.status_code == 200, resp.text
            body = resp.json()
            got = {t["concept"] for t in body["triples"]}
            assert got == expected, (
                f"{persona} browse expected exactly {sorted(expected)}, got {sorted(got)}"
            )
            assert body["tenant_id"] == TENANT          # I2
            assert body["persona"] == persona
            assert body["filters_applied"]["persona"] == persona
            assert body["total_count"] == len(expected)

    def test_07_personaless_browse_byte_identical_shape_and_union(self):
        resp = client.get("/api/dcl/triples/browse",
                          params={"tenant_id": TENANT, "entity_id": ENTITY,
                                  "period": PERIOD})
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert sorted(body.keys()) == ["filters_applied", "total_count", "triples"], (
            f"personaless browse response shape changed: {sorted(body.keys())}"
        )
        got = {t["concept"] for t in body["triples"]}
        assert got == ALL_CONCEPTS
        assert body["total_count"] == len(ALL_CONCEPTS)
        # Pre-existing row shape retained — the run_id column is still
        # present on the unscoped surface (NLQ consumes it as-is).
        assert all("run_id" in t for t in body["triples"])
        assert "persona" not in body["filters_applied"]

    def test_08_browse_conflict_and_unknown_persona_422(self):
        resp = client.get("/api/dcl/triples/browse",
                          params={"tenant_id": TENANT, "persona": "CFO",
                                  "domain": PMAP["cro_only"]})
        assert resp.status_code == 422, resp.text
        detail = resp.json()["detail"]
        assert PMAP["cro_only"] in detail and "'CFO'" in detail
        assert "conflict" in detail.lower()

        resp = client.get("/api/dcl/triples/browse",
                          params={"tenant_id": TENANT, "persona": "Cfo"})
        assert resp.status_code == 422, resp.text
        detail = resp.json()["detail"]
        assert "'Cfo'" in detail
        for key in PMAP["valid_keys"]:
            assert key in detail


class TestBrowseBatch:
    def test_09_batch_in_scope_domains_exact_sets(self):
        resp = client.post("/api/dcl/triples/browse-batch",
                           json={"tenant_id": TENANT, "persona": "CFO",
                                 "domains": [PMAP["cfo_only"], PMAP["shared"]],
                                 "entity_ids": [ENTITY], "period": PERIOD})
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["tenant_id"] == TENANT and body["persona"] == "CFO"
        got = {t["concept"]
               for rows in body["triples_by_domain"].values() for t in rows}
        assert got == EXPECTED["CFO"]
        assert body["total_count"] == len(EXPECTED["CFO"])

    def test_10_batch_out_of_scope_domain_conflict_422(self):
        resp = client.post("/api/dcl/triples/browse-batch",
                           json={"tenant_id": TENANT, "persona": "CFO",
                                 "domains": [PMAP["shared"], PMAP["cro_only"]]})
        assert resp.status_code == 422, resp.text
        detail = resp.json()["detail"]
        assert PMAP["cro_only"] in detail and "'CFO'" in detail

        resp = client.post("/api/dcl/triples/browse-batch",
                           json={"tenant_id": TENANT, "persona": "CcO",
                                 "domains": [PMAP["shared"]]})
        assert resp.status_code == 422, resp.text
        for key in PMAP["valid_keys"]:
            assert key in resp.json()["detail"]

    def test_11_personaless_batch_byte_identical_shape_and_union(self):
        resp = client.post("/api/dcl/triples/browse-batch",
                           json={"tenant_id": TENANT,
                                 "domains": sorted({PMAP["cfo_only"],
                                                    PMAP["cro_only"],
                                                    PMAP["shared"]}),
                                 "entity_ids": [ENTITY], "period": PERIOD})
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert sorted(body.keys()) == ["domains_requested", "domains_returned",
                                       "total_count", "triples_by_domain"], (
            f"personaless batch response shape changed: {sorted(body.keys())}"
        )
        got = {t["concept"]
               for rows in body["triples_by_domain"].values() for t in rows}
        assert got == ALL_CONCEPTS


class TestResolve:
    """Persona scoping on the /resolve surface. What this class uniquely
    protects is the resolve response SHAPE and the persona EXCLUSION-DISCLOSURE
    contract (which concepts a persona excludes, disclosed in warnings) — not
    whether any concept currently resolves. (#76's records-path mapping writer
    made /resolve able to return can_answer=True for mapped concepts, so the
    old "no normalizer-mapped sources → always No-sources" premise is gone;
    the affirmative can_answer=True path is covered deterministically by
    test_fabric_connect_ingest::test_records_ingest_makes_concept_resolvable_with_provenance.
    These tests therefore assert scoping + a data-independent can_answer/reason
    invariant, never ambient dev-graph contents.)"""

    @staticmethod
    def _resolve_concepts():
        """Runtime-derived graph-recognized concepts: one in both personas'
        domains, one CFO-only (the pairings YAML names the graph's concept
        nodes; intersecting it with persona_domains.yaml is the spec)."""
        pair = yaml.safe_load(
            (_repo / "config" / "concept_dimension_pairings.yaml").read_text()
        )
        graph_concepts = set(pair.get("pairings", {}).keys())
        cfo = set(PMAP["cfo_domains"])
        cro = set(PMAP["cro_domains"])
        shared = sorted((cfo & cro) & graph_concepts)
        cfo_only = sorted((cfo - cro) & graph_concepts)
        assert shared and cfo_only, (
            f"need graph-recognized persona domains: shared={shared}, cfo_only={cfo_only}"
        )
        return shared[0], cfo_only[0]

    def test_12_personaless_resolve_byte_identical_shape(self):
        shared_c, cfo_c = self._resolve_concepts()
        resp = client.post("/api/dcl/resolve",
                           json={"concepts": [shared_c, cfo_c]})
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert sorted(body.keys()) == [
            "can_answer", "concept_sources", "confidence", "data_query",
            "join_paths", "provenance", "reason", "resolved_filters",
            "warnings",
        ], f"personaless resolve response shape changed: {sorted(body.keys())}"
        # Personaless resolve excludes nothing — no persona-scoping warning,
        # regardless of whether the concepts resolve. (The old can_answer=False
        # assertion was the empty-graph premise #76 invalidated.)
        assert not any("scoping excluded" in w for w in body["warnings"]), (
            f"personaless resolve must not emit a persona-scoping warning: {body['warnings']}"
        )
        # Data-independent invariant on real resolve behavior: a resolvable
        # answer carries located sources and no failure reason; an
        # unanswerable one names its reason. This holds whatever the dev graph
        # currently contains — it is not a tautology over ambient data.
        if body["can_answer"]:
            assert body["concept_sources"], "can_answer=True must carry concept_sources"
            assert not body["reason"], f"can_answer=True must not carry a failure reason: {body['reason']}"
        else:
            assert body["reason"], "can_answer=False must carry a reason"

    def test_13_cfo_vs_cro_materially_different_scoping(self):
        shared_c, cfo_c = self._resolve_concepts()
        ask = {"concepts": [shared_c, cfo_c], "tenant_id": TENANT}

        cfo = client.post("/api/dcl/resolve", json={**ask, "persona": "CFO"})
        assert cfo.status_code == 200, cfo.text
        cfo_body = cfo.json()
        assert cfo_body["tenant_id"] == TENANT and cfo_body["persona"] == "CFO"
        # Both concepts are in CFO scope → CFO excludes neither → no
        # persona-scoping exclusion warning. (Asserted via the exclusion
        # warning, not `reason`, which is None once a concept resolves.)
        cfo_excl = [w for w in cfo_body["warnings"] if "scoping excluded" in w]
        assert cfo_excl == [], f"CFO must not exclude in-scope concepts: {cfo_excl}"

        cro = client.post("/api/dcl/resolve", json={**ask, "persona": "CRO"})
        assert cro.status_code == 200, cro.text
        cro_body = cro.json()
        assert cro_body["persona"] == "CRO"
        # The CFO-only concept is outside CRO scope → CRO excludes it and
        # discloses exactly one persona-scoping exclusion naming CRO and it.
        cro_excl = [w for w in cro_body["warnings"] if "scoping excluded" in w]
        assert len(cro_excl) == 1, (
            f"CRO must disclose exactly one persona exclusion: {cro_body['warnings']}"
        )
        assert "CRO" in cro_excl[0] and cfo_c in cro_excl[0], (
            f"CRO exclusion warning must name CRO and {cfo_c}: {cro_excl[0]}"
        )

        # Material difference on the SAME request: CRO discloses an exclusion
        # CFO does not. (Deterministic from persona_domains.yaml — independent
        # of whether the concepts have sources in the graph.)
        assert cfo_excl != cro_excl, (
            "CFO and CRO must scope the same request materially differently"
        )

    def test_14_all_concepts_outside_persona_detailed_reason(self):
        _, cfo_c = self._resolve_concepts()
        resp = client.post("/api/dcl/resolve",
                           json={"concepts": [cfo_c], "persona": "CRO",
                                 "tenant_id": TENANT})
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["can_answer"] is False
        reason = body["reason"]
        assert "Persona scoping excluded all requested concepts" in reason
        assert "CRO" in reason and cfo_c in reason
        # The reason names the persona's allowed domains in detail.
        for d in PMAP["cro_domains"]:
            assert d in reason, f"allowed domain {d} missing from reason: {reason}"

    def test_15_unknown_persona_and_missing_tenant_422(self):
        shared_c, _ = self._resolve_concepts()
        resp = client.post("/api/dcl/resolve",
                           json={"concepts": [shared_c], "persona": "cro",
                                 "tenant_id": TENANT})
        assert resp.status_code == 422, resp.text
        detail = resp.json()["detail"]
        assert "'cro'" in detail
        for key in PMAP["valid_keys"]:
            assert key in detail

        resp = client.post("/api/dcl/resolve",
                           json={"concepts": [shared_c], "persona": "CRO"})
        assert resp.status_code == 422, resp.text
        assert "tenant_id" in resp.json()["detail"]


class TestTraces:
    def test_16_every_scoped_answer_leaves_a_persona_trace(self):
        """Three persona-scoped HTTP answers append exactly three
        decision_traces rows — decision_type names the surface, payload
        carries the persona, transport rows are tenant-scoped (I2)."""
        shared_c, _ = self._concepts()
        before = _traces()["total_count"]

        r1 = client.get("/api/dcl/triples/browse",
                        params={"tenant_id": TENANT, "entity_id": ENTITY,
                                "period": PERIOD, "persona": "CFO"})
        r2 = client.post("/api/dcl/triples/browse-batch",
                         json={"tenant_id": TENANT, "persona": "CRO",
                               "domains": [PMAP["cro_only"]],
                               "entity_ids": [ENTITY], "period": PERIOD})
        r3 = client.post("/api/dcl/resolve",
                         json={"concepts": [shared_c], "persona": "CFO",
                               "tenant_id": TENANT})
        assert (r1.status_code, r2.status_code, r3.status_code) == (200, 200, 200)

        body = _traces()
        assert body["total_count"] == before + 3, (
            f"expected exactly 3 new traces, got {body['total_count'] - before}"
        )
        new = sorted(body["traces"], key=lambda t: t["occurred_at"])[-3:]
        by_surface = {t["decision_type"]: t for t in new}
        assert set(by_surface) == {"triples_browse", "triples_browse_batch",
                                   "resolve"}
        assert by_surface["triples_browse"]["payload"]["persona"] == "CFO"
        assert by_surface["triples_browse_batch"]["payload"]["persona"] == "CRO"
        assert by_surface["resolve"]["payload"]["persona"] == "CFO"
        for t in new:
            assert t["trace_type"] == "mcp_call"
            assert t["tenant_id"] == TENANT
            assert t["agent"] == f"http:{t['decision_type']}"
            assert t["outcome"] == "success"
        assert by_surface["triples_browse"]["entity_id"] == ENTITY
        assert by_surface["triples_browse"]["result_summary"]["rows"] == len(
            EXPECTED["CFO"]
        )
        assert by_surface["resolve"]["payload"]["concepts"] == [shared_c]

    def test_17_personaless_calls_add_zero_traces(self):
        shared_c, _ = self._concepts()
        before = _traces()["total_count"]
        r1 = client.get("/api/dcl/triples/browse",
                        params={"tenant_id": TENANT, "entity_id": ENTITY})
        r2 = client.post("/api/dcl/triples/browse-batch",
                         json={"tenant_id": TENANT,
                               "domains": [PMAP["shared"]]})
        r3 = client.post("/api/dcl/resolve", json={"concepts": [shared_c]})
        assert (r1.status_code, r2.status_code, r3.status_code) == (200, 200, 200)
        after = _traces()["total_count"]
        assert after == before, (
            f"personaless calls must add zero traces; delta={after - before}"
        )

    @staticmethod
    def _concepts():
        return TestResolve._resolve_concepts()


class TestIdentityRules:
    def test_18_i1_no_run_id_key_on_scoped_responses(self):
        shared_c, _ = TestResolve._resolve_concepts()
        payloads = [
            client.get("/api/dcl/triples/browse",
                       params={"tenant_id": TENANT, "entity_id": ENTITY,
                               "period": PERIOD, "persona": "CFO"}).json(),
            client.post("/api/dcl/triples/browse-batch",
                        json={"tenant_id": TENANT, "persona": "CRO",
                              "domains": [PMAP["cro_only"]],
                              "entity_ids": [ENTITY]}).json(),
            client.post("/api/dcl/resolve",
                        json={"concepts": [shared_c], "persona": "CFO",
                              "tenant_id": TENANT}).json(),
        ]
        for payload in payloads:
            offenders = [k for k in _keys_of(payload) if "run_id" in k]
            assert offenders == [], f"I1 violation — run_id-bearing keys: {offenders}"

    def test_19_i2_tenant_id_on_every_scoped_response(self):
        shared_c, _ = TestResolve._resolve_concepts()
        responses = {
            "browse": client.get(
                "/api/dcl/triples/browse",
                params={"tenant_id": TENANT, "persona": "CFO"}).json(),
            "browse_batch": client.post(
                "/api/dcl/triples/browse-batch",
                json={"tenant_id": TENANT, "persona": "CFO",
                      "domains": [PMAP["cfo_only"]]}).json(),
            "resolve": client.post(
                "/api/dcl/resolve",
                json={"concepts": [shared_c], "persona": "CFO",
                      "tenant_id": TENANT}).json(),
        }
        for name, body in responses.items():
            assert body.get("tenant_id") == TENANT, (
                f"{name} scoped response dropped tenant_id: {sorted(body.keys())}"
            )

    def test_20_scoped_reads_twice_identical(self):
        """B14: the persona-scoped answer is deterministic."""
        params = {"tenant_id": TENANT, "entity_id": ENTITY,
                  "period": PERIOD, "persona": "CRO"}
        a = client.get("/api/dcl/triples/browse", params=params).json()
        b = client.get("/api/dcl/triples/browse", params=params).json()
        assert a == b, "persona-scoped browse must be deterministic (B14)"


class TestLegacyShim:
    """POST /api/mcp/tools/call — the legacy HTTP shim (Mai's internal path).
    Gate 2B's bar applies here too: persona recorded in the trace for every
    scoped answer (minimal close of the ledger's shim-audit gap; wholesale
    shim auditing of ALL calls stays open). The shim resolves its tenant
    from AOS_TENANT_ID, so each test pins it to this run's tenant."""

    @staticmethod
    def _shim_call(arguments):
        return client.post(
            "/api/mcp/tools/call",
            json={"tool": "query_triples", "arguments": arguments,
                  "api_key": "dcl-mcp-test-key"},
        )

    def test_21_persona_scoped_shim_call_leaves_one_persona_trace(self, monkeypatch):
        """A persona-scoped query_triples through the legacy shim returns
        exactly the persona's concept set AND appends exactly one
        decision_traces row — decision_type='query_triples', payload
        carrying the persona, attributed to this run's tenant, real
        success outcome and compact result_summary."""
        monkeypatch.setenv("AOS_TENANT_ID", TENANT)
        before = _traces()["total_count"]

        resp = self._shim_call({"persona": "CFO", "entity_id": ENTITY,
                                "period": PERIOD})
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["success"] is True, body.get("error")
        got = {r["concept"] for r in body["result"]}
        assert got == EXPECTED["CFO"], (
            f"CFO via legacy shim expected exactly "
            f"{sorted(EXPECTED['CFO'])}, got {sorted(got)}"
        )

        traces = _traces()
        assert traces["total_count"] == before + 1, (
            f"expected exactly 1 new trace, got {traces['total_count'] - before}"
        )
        newest = sorted(traces["traces"], key=lambda t: t["occurred_at"])[-1]
        assert newest["decision_type"] == "query_triples"
        assert newest["trace_type"] == "mcp_call"
        assert newest["tenant_id"] == TENANT
        assert newest["agent"] == "http:legacy-tools-call"
        assert newest["payload"]["persona"] == "CFO"
        assert newest["entity_id"] == ENTITY
        assert newest["outcome"] == "success"
        assert newest["result_summary"]["rows"] == len(EXPECTED["CFO"])

    def test_22_personaless_shim_call_adds_zero_traces(self, monkeypatch):
        """A personaless legacy-shim query_triples answers the unscoped
        union exactly as before and appends zero decision_traces rows —
        the legacy path stays byte-identical when no persona is given."""
        monkeypatch.setenv("AOS_TENANT_ID", TENANT)
        before = _traces()["total_count"]
        resp = self._shim_call({"concept": "total", "entity_id": ENTITY,
                                "period": PERIOD})
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["success"] is True, body.get("error")
        assert {r["concept"] for r in body["result"]} == ALL_CONCEPTS
        after = _traces()["total_count"]
        assert after == before, (
            f"personaless shim call must add zero traces; delta={after - before}"
        )
