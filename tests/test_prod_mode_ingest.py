"""
Prod-mode AI/RAG wiring tests for DCL write paths.

Asserts:
- run_mode='Prod' with no keys returns 503 with a readable body.
- Both write paths (POST /api/dcl/ingest-triples and POST /api/dcl/run AAM
  mode) route through the same _apply_prod_mode_ai helper — no drift.
- run_mode='Dev' or omitted leaves the persisted triple set bit-identical
  to pre-change behavior (no AI rewrite of concepts).
"""

import inspect
import sys
import uuid
from pathlib import Path
from unittest.mock import patch

import pytest

_repo = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_repo))

from dotenv import load_dotenv
load_dotenv(_repo / ".env.development")

from fastapi.testclient import TestClient
from backend.api.main import app
from backend.core.db import get_connection

client = TestClient(app, raise_server_exceptions=False)

TEST_TENANT_ID = str(uuid.uuid5(uuid.NAMESPACE_DNS, "prod-mode-ingest-test"))


def _make_triple(**overrides):
    base = {
        "entity_id": "test_entity",
        "concept": "revenue.total",
        "property": "amount",
        "value": 1000,
        "period": "2025-Q1",
        "currency": "USD",
        "source_system": "test_sys",
        "source_table": "invoices",
        "source_field": "amount",
        "confidence_score": 0.4,
        "confidence_tier": "low",
    }
    base.update(overrides)
    return base


def _cleanup():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM semantic_triples WHERE tenant_id = %s",
                (TEST_TENANT_ID,),
            )
            cur.execute(
                "DELETE FROM tenant_runs WHERE tenant_id = %s",
                (TEST_TENANT_ID,),
            )
            conn.commit()


@pytest.fixture(autouse=True)
def cleanup_around_each():
    _cleanup()
    yield
    _cleanup()


# ---------------------------------------------------------------------------
# Test (a): loud failure on missing keys
# ---------------------------------------------------------------------------

def test_503_on_missing_keys(monkeypatch):
    """run_mode='Prod' with OPENAI_API_KEY absent → 503 with readable message.

    The contract is: when an operator explicitly opts into Prod, missing
    config is a loud failure, not a silent skip.
    """
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("AI_INTEGRATIONS_OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("PINECONE_API_KEY", raising=False)

    run_id = str(uuid.uuid4())
    resp = client.post("/api/dcl/ingest-triples", json={
        "tenant_id": TEST_TENANT_ID,
        "run_id": run_id,
        "run_mode": "Prod",
        "triples": [_make_triple()],
    })

    assert resp.status_code == 503, (
        f"Expected 503 on missing keys; got {resp.status_code}: {resp.text}"
    )
    body = resp.json()
    detail = body.get("detail", {})
    assert detail.get("error") == "PROD_MODE_KEYS_MISSING", (
        f"Expected error=PROD_MODE_KEYS_MISSING; got {detail}"
    )
    assert "OPENAI_API_KEY" in detail.get("message", ""), (
        f"503 message must name the missing env var; got {detail.get('message')}"
    )


# ---------------------------------------------------------------------------
# Test (b): both write paths share the same helper (no drift)
# ---------------------------------------------------------------------------

def test_shared_helper_called_from_ingest_triples(monkeypatch):
    """Behavioral: POST /api/dcl/ingest-triples with run_mode=Prod calls
    _apply_prod_mode_ai exactly once."""
    monkeypatch.setenv("OPENAI_API_KEY", "stub-test-key")
    monkeypatch.setenv("PINECONE_API_KEY", "stub-test-key")

    call_count = {"n": 0}

    def fake_helper(mappings, ontology, narration, run_id):
        call_count["n"] += 1
        return list(mappings), 0, {"total_validated": 0}

    with patch(
        "backend.engine.dcl_engine._apply_prod_mode_ai",
        side_effect=fake_helper,
    ):
        run_id = str(uuid.uuid4())
        resp = client.post("/api/dcl/ingest-triples", json={
            "tenant_id": TEST_TENANT_ID,
            "run_id": run_id,
            "run_mode": "Prod",
            "triples": [_make_triple()],
        })

    assert resp.status_code == 201, resp.text
    assert call_count["n"] == 1, (
        f"Expected _apply_prod_mode_ai called exactly once from ingest-triples; "
        f"got {call_count['n']}"
    )


def test_shared_helper_referenced_by_both_write_paths():
    """Structural: both write paths reference _apply_prod_mode_ai by name in
    their source. This pins the call path so AI behavior cannot diverge
    between the ingest-triples route and the AAM-mode build_graph_snapshot.
    """
    from backend.engine import dcl_engine
    from backend.api.routes import ingest_triples as ingest_route

    assert hasattr(dcl_engine, "_apply_prod_mode_ai"), (
        "Shared helper _apply_prod_mode_ai must be defined in dcl_engine"
    )

    engine_src = inspect.getsource(dcl_engine.DCLEngine.build_graph_snapshot)
    assert "_apply_prod_mode_ai" in engine_src, (
        "build_graph_snapshot must call the shared helper, not inline the "
        "validator + RAG sequence"
    )

    ingest_src = inspect.getsource(ingest_route.ingest_triples)
    assert "_apply_prod_mode_ai" in ingest_src, (
        "ingest_triples must call the shared helper for Prod-mode behavior"
    )


# ---------------------------------------------------------------------------
# Test (c): Dev / omitted run_mode is bit-identical (no AI rewrite)
# ---------------------------------------------------------------------------

def _fetch_concepts_for_run(run_id: str) -> list[tuple]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT entity_id, concept, source_field, confidence_score
                   FROM semantic_triples
                   WHERE run_id = %s
                   ORDER BY entity_id, concept""",
                (run_id,),
            )
            return [tuple(r) for r in cur.fetchall()]


def test_dev_path_bit_identical():
    """Dev or omitted run_mode must persist the input concept verbatim — no
    AI rewrite. Two batches with the same triples (one Dev, one omitted)
    must produce the same persisted concept set.
    """
    triples = [
        _make_triple(entity_id="ent_a", concept="revenue.total", source_field="amt"),
        _make_triple(entity_id="ent_a", concept="opex.total", source_field="exp", period="2025-Q2"),
    ]

    run_id_omitted = str(uuid.uuid4())
    resp1 = client.post("/api/dcl/ingest-triples", json={
        "tenant_id": TEST_TENANT_ID,
        "run_id": run_id_omitted,
        "triples": triples,
    })
    assert resp1.status_code == 201, resp1.text
    persisted_omitted = _fetch_concepts_for_run(run_id_omitted)

    run_id_dev = str(uuid.uuid4())
    resp2 = client.post("/api/dcl/ingest-triples", json={
        "tenant_id": TEST_TENANT_ID,
        "run_id": run_id_dev,
        "run_mode": "Dev",
        "triples": triples,
    })
    assert resp2.status_code == 201, resp2.text
    persisted_dev = _fetch_concepts_for_run(run_id_dev)

    assert persisted_omitted == persisted_dev, (
        f"Dev and omitted paths must be bit-identical.\n"
        f"omitted: {persisted_omitted}\n"
        f"Dev:     {persisted_dev}"
    )

    input_concepts = sorted({(t["entity_id"], t["concept"]) for t in triples})
    persisted_concepts = sorted({(r[0], r[1]) for r in persisted_dev})
    assert input_concepts == persisted_concepts, (
        f"Concepts must persist verbatim in Dev mode (no AI rewrite).\n"
        f"input:     {input_concepts}\n"
        f"persisted: {persisted_concepts}"
    )
