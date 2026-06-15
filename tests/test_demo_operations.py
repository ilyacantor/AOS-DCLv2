"""
Demo operations + scoring + audit-read tests (grounded-agent demo, §13).

Deterministic by design: scorer tests run on fixed capture fragments (pure
functions, no model calls); the audit-read tests exercise the REAL write
path (backend.api.mcp_audit.write_audit — the same function the MCP server
calls per tool invocation) against the live dev store; the containment test
enforces that Panel A can never become an importable data path.
"""

from __future__ import annotations

import re
import uuid
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent

# ---------------------------------------------------------------------------
# Containment — backend/* and src/* never import the demo package
# ---------------------------------------------------------------------------

IMPORT_DEMO_RE = re.compile(r"^\s*(from\s+demo[.\s]|import\s+demo\b)", re.MULTILINE)


def test_backend_never_imports_demo():
    offenders = []
    for base in (REPO_ROOT / "backend", REPO_ROOT / "src"):
        for path in base.rglob("*.py"):
            if IMPORT_DEMO_RE.search(path.read_text(errors="ignore")):
                offenders.append(str(path))
    assert offenders == [], (
        f"platform code imports the demo package — Panel A must stay an "
        f"operator-gated tool, not a data path: {offenders}"
    )


# ---------------------------------------------------------------------------
# Question slots are valid data
# ---------------------------------------------------------------------------

def _load_slots():
    spec = yaml.safe_load((REPO_ROOT / "demo" / "questions.yaml").read_text())
    return spec, spec["slots"]


def test_questions_slot_count_in_eval_range():
    _, slots = _load_slots()
    assert 8 <= len(slots) <= 10, f"§13 eval harness is 8-10 slots; got {len(slots)}"


def test_questions_slots_well_formed():
    spec, slots = _load_slots()
    ids = [s["id"] for s in slots]
    assert len(ids) == len(set(ids)), f"duplicate slot ids: {ids}"
    assert spec["meta"]["entity_default"], "meta.entity_default required"
    for s in slots:
        assert s["status"] in ("live", "pending"), s["id"]
        assert s["kind"] in ("numeric", "no_data", "conflict"), s["id"]
        if s["status"] == "pending":
            assert len(s.get("pending_reason", "")) > 30, (
                f"{s['id']}: pending slots must say WHY they are pending"
            )
        if s["status"] == "live" and s["kind"] == "numeric":
            gt = s["ground_truth"]
            assert gt["feed"] in ("financial", "operational", "ledger"), s["id"]
            assert gt["field"] and gt["period"], s["id"]
            assert gt["scales"], s["id"]


def test_gate1a_headline_slots_present_and_live():
    """The two §13 headline beats exist verbatim as slots and are LIVE
    conflict slots — flipped pending→live when the Farm scenario months
    landed (ledger #66 RESOLVED, commit b3387e5). The never-simulate rule
    holds in its live form: conflict slots score against the entity's real
    Register (see the capture's conflict scores), never canned output."""
    _, slots = _load_slots()
    by_id = {s["id"]: s for s in slots}
    q1 = by_id["q1_attrition_headline"]
    assert q1["status"] == "live"
    assert q1["kind"] == "conflict"
    assert "attrition" in q1["question"].lower()
    q2 = by_id["q2_cloud_conflict"]
    assert q2["status"] == "live"
    assert q2["kind"] == "conflict"
    assert "compute spend" in q2["question"].lower()


# ---------------------------------------------------------------------------
# Scorer — pure-function determinism on fixed fragments
# ---------------------------------------------------------------------------

from demo import scoring  # noqa: E402  (after REPO_ROOT setup)


def test_score_numeric_exact_and_unit_scales():
    assert scoring.score_numeric("Net income was $99.99 million.", 99.99, [1, 1000], 0.02)["passed"]
    res_b = scoring.score_numeric("ARR ended at about $1.48B.", 1477.13, [1, 1000], 0.02)
    assert res_b["passed"] and res_b["scale"] == 1000
    assert not scoring.score_numeric("Revenue was $5M.", 361.06, [1, 1000], 0.02)["passed"]


def test_score_numeric_rate_scales_both_directions():
    assert scoring.score_numeric("Attrition is 5.8%.", 0.058, [1, 0.01, 100], 0.02)["passed"]
    assert scoring.score_numeric("Attrition is 0.058.", 5.8, [1, 0.01, 100], 0.02)["passed"]


def test_score_no_data_honest_vs_fabricated():
    honest = scoring.score_no_data("There is no data for employee NPS in the store.")
    assert honest["passed"] and not honest["fabricated_number"]
    fabricated = scoring.score_no_data("Employee NPS is 42 this quarter.")
    assert not fabricated["passed"] and fabricated["fabricated_number"]


def test_provenance_requires_evidence_from_tool_results():
    cap = {
        "answer_text": "Per netsuite (confidence 0.95), net income was $99.99M.",
        "tool_calls": [{"result_excerpt": '{"source_system": "netsuite", "value": 99.99}'}],
    }
    res = scoring.provenance_present(cap)
    assert res["present"] and res["cited_source_systems"] == ["netsuite"]

    uncited = scoring.provenance_present(
        {"answer_text": "Net income was $99.99M.",
         "tool_calls": [{"result_excerpt": '{"source_system": "netsuite"}'}]}
    )
    assert not uncited["present"]

    no_calls = scoring.provenance_present({"answer_text": "netsuite says 99.99", "tool_calls": []})
    assert not no_calls["present"]


def test_score_conflict_against_register():
    register = [{"claims": [{"source_system": "confluent"}, {"source_system": "workato"}]}]
    tool_calls = [{"name": "conflict_query"}]
    disclosed = scoring.score_conflict(
        "Yes — confluent and workato disagree on customer_name records.", register, tool_calls
    )
    assert disclosed["passed"] and disclosed["sources_named_in_answer"] == ["confluent", "workato"]

    silent = scoring.score_conflict("All customer data looks fine to me.", register, [])
    assert not silent["passed"]

    none_expected = scoring.score_conflict("No conflicts detected across sources.", [], tool_calls)
    assert none_expected["passed"] and none_expected["expected_conflicts"] == 0


# ---------------------------------------------------------------------------
# MCP audit read endpoint — real write path, real store
# ---------------------------------------------------------------------------

from fastapi.testclient import TestClient  # noqa: E402

from backend.api.main import app  # noqa: E402
from backend.api.mcp_audit import AuditRow, write_audit  # noqa: E402

client = TestClient(app)


def test_audit_read_requires_tenant_id():
    resp = client.get("/api/dcl/mcp/audit")
    assert resp.status_code == 422, resp.text


def test_audit_read_rejects_unknown_outcome():
    resp = client.get(
        "/api/dcl/mcp/audit",
        params={"tenant_id": str(uuid.uuid4()), "outcome": "sideways"},
    )
    assert resp.status_code == 422
    assert "outcome" in resp.text


def test_audit_read_rejects_malformed_since():
    resp = client.get(
        "/api/dcl/mcp/audit",
        params={"tenant_id": str(uuid.uuid4()), "since": "yesterday-ish"},
    )
    assert resp.status_code == 422
    assert "ISO-8601" in resp.text


def test_audit_read_roundtrip_via_real_writer():
    """Write one row through the platform's own audit writer (the exact
    function the MCP server calls per tool invocation), then read it back
    through the endpoint, filtered by token id."""
    tenant_id = str(uuid.uuid4())
    token_id = uuid.uuid4().hex[:16]
    write_audit(AuditRow(
        tenant_id=tenant_id,
        tool_name="query_triples",
        caller_token_id=token_id,
        arguments_hash="t" * 64,
        latency_ms=12,
        outcome="success",
        transport="http+sse",
    ))
    resp = client.get(
        "/api/dcl/mcp/audit",
        params={"tenant_id": tenant_id, "caller_token_id": token_id},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["tenant_id"] == tenant_id
    assert body["total_count"] == 1
    entry = body["entries"][0]
    assert entry["tool_name"] == "query_triples"
    assert entry["caller_token_id"] == token_id
    assert entry["outcome"] == "success"
    assert entry["transport"] == "http+sse"
    assert entry["created_at"] is not None


def test_audit_read_tenant_scoping_isolates():
    """A row written for tenant X is invisible under tenant Y."""
    tenant_x, tenant_y = str(uuid.uuid4()), str(uuid.uuid4())
    token_id = uuid.uuid4().hex[:16]
    write_audit(AuditRow(
        tenant_id=tenant_x, tool_name="list_domains", caller_token_id=token_id,
        arguments_hash="", latency_ms=5, outcome="success", transport="stdio",
    ))
    resp = client.get(
        "/api/dcl/mcp/audit",
        params={"tenant_id": tenant_y, "caller_token_id": token_id},
    )
    assert resp.status_code == 200
    assert resp.json()["total_count"] == 0


# ---------------------------------------------------------------------------
# Malformed-ingest beat contract (the sequence's real-condition beat)
# ---------------------------------------------------------------------------

def test_ingest_records_empty_pipes_rejected_loudly():
    resp = client.post("/api/dcl/ingest-records", json={
        "tenant_id": str(uuid.uuid4()),
        "dcl_ingest_id": str(uuid.uuid4()),
        "entity_id": "DemoBeat-Reject",
        "snapshot_name": "DemoBeat-Reject-0000",
        "pipes": [],
    })
    # Domain validation contract: structurally-complete envelope with nothing
    # to ingest -> 400 VALIDATION_FAILED (422 is the I2/identity class).
    assert resp.status_code == 400, resp.text
    detail = resp.json()["detail"]
    assert detail["error"] == "VALIDATION_FAILED"
    assert "pipes" in detail["message"]


def test_mcp_concept_filter_composes_with_concept_lookup():
    """Regression for the lookup->query composition gap found by the demo's
    grounded panel: concept_lookup returns unqualified catalog ids
    ('net_income') while the store keys domain-qualified dotted paths
    ('pnl.net_income'). query_triples must match the unqualified id against
    its domain-qualified instances — otherwise a grounded consumer gets a
    false 'no data' for a populated metric."""
    import json as _json

    # Read tenant AND entity from the SAME manifest snapshot. _update_seed_manifest
    # rewrites this file on every ingest-triples, so mid-suite it churns; the
    # import-frozen seed_tenant_id fixture (conftest TENANT_ID, read once at import)
    # drifts from a fresh entities[0] read, leaving the entity on a DIFFERENT tenant
    # than the fixture — that is the "composition gap is back" flap (the entity's
    # net_income lives on its own tenant, not the stale fixture one). One read keeps
    # (tenant, entity) a consistent pair, since _update_seed_manifest writes both
    # atomically for the same run.
    manifest = _json.loads((REPO_ROOT / "data" / "seed_manifest.json").read_text())
    entity = manifest["entities"][0]
    tenant = manifest["tenant_id"]

    from backend.engine.mcp_tools import tool_query_triples

    rows = tool_query_triples(
        tenant, concept="net_income", entity_id=entity, limit=50
    )
    assert len(rows) > 0, (
        f"unqualified catalog id 'net_income' returned no triples for "
        f"{entity} — the lookup->query composition gap is back"
    )
    assert all(
        t["concept"] == "net_income" or t["concept"].endswith(".net_income")
        for t in rows
    ), [t["concept"] for t in rows[:5]]

    dotted_concepts = {t["concept"] for t in rows if "." in t["concept"]}
    assert dotted_concepts, "expected domain-qualified instances in the store"
    target = sorted(dotted_concepts)[0]
    exact = tool_query_triples(
        tenant, concept=target, entity_id=entity, limit=50
    )
    assert len(exact) > 0
    assert all(t["concept"] == target for t in exact)

    # Root-shaped catalog ids ('revenue' -> 'revenue.total', the other
    # composition shape) must surface their namespace too.
    root_rows = tool_query_triples(
        tenant, concept="revenue", entity_id=entity, limit=200
    )
    namespaced = {t["concept"] for t in root_rows if t["concept"].startswith("revenue.")}
    assert "revenue.total" in namespaced, (
        f"'revenue' must surface its namespace (revenue.total et al); "
        f"got concepts {sorted({t['concept'] for t in root_rows})[:8]}"
    )


def test_ingest_records_missing_ingest_id_rejected_loudly():
    """The other malformed shape: an envelope without its run identity is
    refused at the pydantic boundary, naming the missing field."""
    resp = client.post("/api/dcl/ingest-records", json={
        "tenant_id": str(uuid.uuid4()),
        "entity_id": "DemoBeat-Reject",
        "pipes": [],
    })
    assert resp.status_code == 422
    assert "run_id" in resp.text or "dcl_ingest_id" in resp.text
