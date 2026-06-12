"""Gate 2B acceptance (dev stack, :8104): persona-aware query execution —
the campaign-brief story end to end.

Operator-visible outcome under test: ONE question expressed identically —
concept 'total' for this run's entity and period 2026-Q1 — asked as CFO and
as CRO returns materially different, persona-domain-scoped answers on both
the MCP path (real `mcp` SDK client over the stdio transport, wp5 mechanics,
.env.development backing) and the NLQ read path (GET /api/dcl/triples/browse
on the live dev backend): CFO sees exactly the CFO-only + shared concepts,
CRO exactly the CRO-only + shared concepts (domains derived from
config/persona_domains.yaml AT RUNTIME). The unscoped leg is unchanged — the
exact three-concept union in the pre-existing response shape, and it appends
zero new decision traces. GET /api/dcl/traces filtered to the per-run tenant
shows every persona-scoped answer as a trace whose payload carries the
persona and whose decision_type names the surface (query_triples for the MCP
leg, triples_browse for the browse leg).

Acceptance grain: live dcl-dev backend (:8104, aos-dev DB) over HTTP; MCP
over the real stdio transport (subprocess inherits this process's
.env.development DATABASE_URL and never loads .env). All fixture values are
per-run-unique; direct DB access only for per-run test-tenant cleanup.
"""

import asyncio
import os
import sys
import uuid
from pathlib import Path

import httpx
import pytest
import yaml

_repo = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_repo))

from dotenv import load_dotenv
load_dotenv(_repo / ".env.development")

# Single-var secret resolution (wp5 pattern): prefer the env, else the live
# server's shim secret — ONE var from `.env`, never the whole file (its
# DATABASE_URL is prod; the stdio subprocess inherits this env).
if not os.environ.get("DCL_MCP_TOKEN_SECRET"):
    from dotenv import dotenv_values
    _live_secret = dotenv_values(_repo / ".env").get("DCL_MCP_TOKEN_SECRET")
    if _live_secret:
        os.environ["DCL_MCP_TOKEN_SECRET"] = _live_secret
os.environ.setdefault("DCL_MCP_TOKEN_SECRET", "gate2b-e2e-secret-do-not-use-in-prod")

DCL_BACKEND = os.environ.get("DCL_BACKEND_URL", "http://localhost:8104")

TENANT = str(uuid.uuid4())
TAG = uuid.uuid4().hex[:6]
ENTITY = f"Gate2BE2E-{TAG}"
PERIOD = "2026-Q1"
PIPE = str(uuid.uuid4())


def _derive_persona_map() -> dict:
    """Domain expectations from config/persona_domains.yaml AT RUNTIME —
    the YAML is the spec (B8); the lists are never hardcoded here."""
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
        raise AssertionError(f"no ingest-valid {label} domain in {sorted(candidates)}")

    return {
        "cfo_only": pick(cfo - cro, ["cogs"], "CFO-only"),
        "cro_only": pick(cro - cfo, ["account"], "CRO-only"),
        "shared": pick(cfo & cro, ["revenue"], "CFO∩CRO"),
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
# The ONE question, expressed identically on every leg: the unqualified
# concept 'total' (matches *.total) + this entity + this period. Only the
# persona differs between the legs.
QUESTION = {"concept": "total", "entity_id": ENTITY, "period": PERIOD}


def _triple(concept, value):
    return {
        "entity_id": ENTITY, "concept": concept, "property": "amount",
        "value": value, "period": PERIOD, "source_system": "gate2b_e2e_src",
        "source_table": "gate2b_e2e_probe", "source_field": "amount",
        "pipe_id": PIPE, "confidence_score": 0.95, "confidence_tier": "exact",
        "fabric_plane": "ipaas",
    }


def _run_async(coro):
    """asyncio.run, immune to pytest-playwright's leftover running-loop
    registration when the e2e directory runs in one session."""
    saved = None
    try:
        saved = asyncio.events._get_running_loop()
        asyncio.events._set_running_loop(None)
    except Exception:
        pass
    try:
        return asyncio.run(coro)
    finally:
        if saved is not None:
            try:
                asyncio.events._set_running_loop(saved)
            except Exception:
                pass


async def _mcp_stdio_calls(token_str, calls):
    """One real stdio MCP session (wp5 transport mechanics), N tool calls.
    Returns the decoded JSON payload per call."""
    import json as _json
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client

    env = {**os.environ, "DCL_MCP_TOKEN": token_str, "PYTHONPATH": str(_repo)}
    params = StdioServerParameters(
        command=sys.executable, args=["-m", "backend.api.mcp_stdio"], env=env
    )
    out = []
    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            for name, args in calls:
                r = await session.call_tool(name, args)
                assert r.isError is False, f"{name}{args} failed: {r.content}"
                out.append(_json.loads(r.content[0].text))
    return out


def _cleanup():
    import psycopg2
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    try:
        cur = conn.cursor()
        for sql in (
            "DELETE FROM semantic_triples WHERE tenant_id = %s",
            "DELETE FROM tenant_runs WHERE tenant_id = %s",
        ):
            cur.execute(sql, (TENANT,))
        conn.commit()
    finally:
        conn.close()


def _get(client, path, **params):
    r = client.get(f"{DCL_BACKEND}{path}", params=params)
    assert r.status_code == 200, f"GET {path}: {r.status_code} {r.text}"
    return r.json()


def _keys_of(obj):
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield str(k)
            yield from _keys_of(v)
    elif isinstance(obj, list):
        for item in obj:
            yield from _keys_of(item)


@pytest.fixture(scope="module", autouse=True)
def story():
    """The Gate 2B story executed once, in order, through real paths:
    seed three-domain triples via the live ingest, ask the ONE question as
    CFO / CRO / unscoped over real stdio MCP, then over live browse (the
    surface NLQ calls), capturing trace counts around the unscoped legs.
    Teardown scrubs the per-run triples/runs; mai_mcp_audit is append-only
    (house posture)."""
    from backend.api.mcp_auth import mint_token

    state = {}
    with httpx.Client(timeout=120.0) as client:
        health = client.get(f"{DCL_BACKEND}/api/health")
        assert health.status_code == 200, f"DCL dev backend not healthy at {DCL_BACKEND}"

        run_id = str(uuid.uuid4())
        resp = client.post(
            f"{DCL_BACKEND}/api/dcl/ingest-triples",
            json={"tenant_id": TENANT, "dcl_ingest_id": run_id, "entity_id": ENTITY,
                  "snapshot_name": f"{ENTITY}-{run_id.replace('-', '')[:4]}",
                  "triples": [
                      _triple(CONCEPT_CFO, 111.0),
                      _triple(CONCEPT_CRO, 222.0),
                      _triple(CONCEPT_SHARED, 333.0),
                  ]},
        )
        assert resp.status_code == 201, f"seed ingest failed: {resp.status_code} {resp.text}"

        # MCP leg — real client, real stdio transport, ONE session: the same
        # question as CFO, as CRO, and unscoped.
        minted = mint_token(TENANT, scope=("query_triples",))
        state["token_id"] = minted["token_id"]
        cfo_rows, cro_rows, unscoped_rows = _run_async(_mcp_stdio_calls(
            minted["token"],
            [
                ("query_triples", {**QUESTION, "persona": "CFO"}),
                ("query_triples", {**QUESTION, "persona": "CRO"}),
                ("query_triples", QUESTION),
            ],
        ))
        state["mcp"] = {"CFO": cfo_rows, "CRO": cro_rows, "unscoped": unscoped_rows}

        # NLQ leg — browse on the live backend, same question coordinates.
        state["browse"] = {}
        for persona in ("CFO", "CRO"):
            state["browse"][persona] = _get(
                client, "/api/dcl/triples/browse",
                tenant_id=TENANT, entity_id=ENTITY, period=PERIOD,
                persona=persona,
            )
        # Unscoped leg with trace-count bracketing: zero new traces.
        before = _get(client, "/api/dcl/traces", tenant_id=TENANT)["total_count"]
        state["browse"]["unscoped"] = _get(
            client, "/api/dcl/triples/browse",
            tenant_id=TENANT, entity_id=ENTITY, period=PERIOD,
        )
        after = _get(client, "/api/dcl/traces", tenant_id=TENANT)["total_count"]
        state["unscoped_trace_delta"] = after - before

        state["traces"] = _get(client, "/api/dcl/traces", tenant_id=TENANT)

    yield state
    _cleanup()


class TestMCPLeg:
    def test_01_cfo_vs_cro_materially_different_exact_sets(self, story):
        got_cfo = {r["concept"] for r in story["mcp"]["CFO"]}
        got_cro = {r["concept"] for r in story["mcp"]["CRO"]}
        assert got_cfo == EXPECTED["CFO"], (
            f"CFO expected exactly {sorted(EXPECTED['CFO'])}, got {sorted(got_cfo)}"
        )
        assert got_cro == EXPECTED["CRO"], (
            f"CRO expected exactly {sorted(EXPECTED['CRO'])}, got {sorted(got_cro)}"
        )
        assert got_cfo != got_cro, "the two personas must answer differently"
        assert got_cfo & got_cro == {CONCEPT_SHARED}
        for rows in (story["mcp"]["CFO"], story["mcp"]["CRO"]):
            for r in rows:
                assert str(r["tenant_id"]) == TENANT
                assert "dcl_ingest_id" in r and "run_id" not in r

    def test_02_unscoped_mcp_leg_is_the_exact_union(self, story):
        got = {r["concept"] for r in story["mcp"]["unscoped"]}
        assert got == ALL_CONCEPTS, (
            f"unscoped expected exactly {sorted(ALL_CONCEPTS)}, got {sorted(got)}"
        )


class TestBrowseLeg:
    def test_03_cfo_vs_cro_exact_sets_with_identity(self, story):
        for persona in ("CFO", "CRO"):
            body = story["browse"][persona]
            got = {t["concept"] for t in body["triples"]}
            assert got == EXPECTED[persona], (
                f"{persona} browse expected exactly {sorted(EXPECTED[persona])}, "
                f"got {sorted(got)}"
            )
            assert body["tenant_id"] == TENANT       # I2
            assert body["persona"] == persona
            assert body["total_count"] == len(EXPECTED[persona])
            offenders = [k for k in _keys_of(body) if "run_id" in k]
            assert offenders == [], f"I1 violation in {persona} browse: {offenders}"

    def test_04_unscoped_browse_unchanged_shape_and_union(self, story):
        body = story["browse"]["unscoped"]
        assert sorted(body.keys()) == ["filters_applied", "total_count", "triples"], (
            f"unscoped browse response shape changed: {sorted(body.keys())}"
        )
        got = {t["concept"] for t in body["triples"]}
        assert got == ALL_CONCEPTS
        # Pre-existing row shape retained on the unscoped surface.
        assert all("run_id" in t for t in body["triples"])
        assert story["unscoped_trace_delta"] == 0, (
            "unscoped browse must append zero decision traces; "
            f"delta={story['unscoped_trace_delta']}"
        )


class TestTraces:
    def test_05_each_scoped_answer_carries_its_persona_in_a_trace(self, story):
        traces = story["traces"]["traces"]
        assert story["traces"]["tenant_id"] == TENANT
        mcp_traces = [t for t in traces
                      if t["decision_type"] == "query_triples"
                      and (t["payload"] or {}).get("persona")]
        browse_traces = [t for t in traces
                         if t["decision_type"] == "triples_browse"]
        assert {t["payload"]["persona"] for t in mcp_traces} == {"CFO", "CRO"}, (
            f"MCP-leg traces must carry both personas: "
            f"{[(t['decision_type'], t['payload']) for t in mcp_traces]}"
        )
        assert {t["payload"]["persona"] for t in browse_traces} == {"CFO", "CRO"}, (
            f"browse-leg traces must carry both personas: "
            f"{[(t['decision_type'], t['payload']) for t in browse_traces]}"
        )
        for t in mcp_traces:
            assert t["trace_type"] == "mcp_call"
            assert t["agent"] == story["token_id"]
            assert t["entity_id"] == ENTITY
            assert t["outcome"] == "success"
            # The trace payload is the exact question the agent asked.
            assert t["payload"]["concept"] == "total"
            assert t["payload"]["period"] == PERIOD
        for t in browse_traces:
            assert t["trace_type"] == "mcp_call"
            assert t["agent"] == "http:triples_browse"
            assert t["entity_id"] == ENTITY
            assert t["outcome"] == "success"
            assert t["result_summary"]["rows"] == len(
                EXPECTED[t["payload"]["persona"]]
            )

    def test_06_exactly_one_browse_trace_per_scoped_browse_answer(self, story):
        browse_traces = [t for t in story["traces"]["traces"]
                        if t["decision_type"] == "triples_browse"]
        assert len(browse_traces) == 2, (
            f"two persona-scoped browse answers must leave exactly two "
            f"traces, got {len(browse_traces)}"
        )
