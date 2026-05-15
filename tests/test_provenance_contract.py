"""Provenance contract enforcement at /api/dcl/ingest-triples.

Every triple must carry the full provenance chain:
  source_system, source_field, pipe_id, fabric_plane, confidence_score

Missing any of these is a contract violation (422 PROVENANCE_INCOMPLETE).
No silent acceptance. No per-row partial accept — the entire request is
rejected so the upstream producer surfaces the gap loudly.

Positive: a fully-provenance triple is accepted (201).
Negative: one test per required field, each removed independently → 422
with a readable error naming the missing field.
"""

import sys
import uuid
from pathlib import Path

import pytest

_repo = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_repo))

from dotenv import load_dotenv
load_dotenv(_repo / ".env.development")

from fastapi.testclient import TestClient

from backend.api.main import app
from backend.core.db import get_connection


client = TestClient(app, raise_server_exceptions=False)

TEST_TENANT_ID = str(uuid.uuid5(uuid.NAMESPACE_DNS, "provenance-contract-test"))


def _cleanup():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM semantic_triples WHERE tenant_id = %s", (TEST_TENANT_ID,))
            cur.execute("DELETE FROM tenant_runs WHERE tenant_id = %s", (TEST_TENANT_ID,))
            conn.commit()


@pytest.fixture(autouse=True)
def around_each():
    _cleanup()
    yield
    _cleanup()


def _full_triple(**overrides) -> dict:
    """One canonical fully-provenance triple. Override individual fields to
    test rejection of each missing one.
    """
    base = {
        "entity_id": "test_entity_contract",
        "concept": "revenue.total",
        "property": "amount",
        "value": 1000,
        "period": "2025-Q1",
        "currency": "USD",
        "source_system": "test_system",
        "source_field": "amount",
        "pipe_id": "00000000-0000-0000-0000-000000000099",
        "fabric_plane": "ipaas",
        "confidence_score": 0.95,
        "confidence_tier": "exact",
    }
    base.update(overrides)
    return base


def _post(triple: dict, run_id: str | None = None):
    return client.post(
        "/api/dcl/ingest-triples",
        json={
            "tenant_id": TEST_TENANT_ID,
            "run_id": run_id or str(uuid.uuid4()),
            "triples": [triple],
        },
    )


def test_positive_full_provenance_accepted():
    """A triple carrying all five provenance fields is accepted (201)."""
    resp = _post(_full_triple())
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["triple_count"] == 1
    assert body["triples_written"] == 1


def test_negative_missing_source_system_rejected_422():
    """source_system absent → 422 PROVENANCE_INCOMPLETE naming the field."""
    triple = _full_triple()
    triple["source_system"] = ""
    resp = _post(triple)
    assert resp.status_code == 422, resp.text
    body = resp.json()
    assert body["detail"]["error"] == "PROVENANCE_INCOMPLETE"
    assert body["detail"]["field"] == "source_system"
    assert "source_system" in body["detail"]["message"]


def test_negative_missing_source_field_rejected_422():
    """source_field absent → 422 PROVENANCE_INCOMPLETE naming the field."""
    triple = _full_triple()
    triple["source_field"] = None
    resp = _post(triple)
    assert resp.status_code == 422, resp.text
    body = resp.json()
    assert body["detail"]["error"] == "PROVENANCE_INCOMPLETE"
    assert body["detail"]["field"] == "source_field"
    assert "source_field" in body["detail"]["message"]


def test_negative_missing_pipe_id_rejected_422():
    """pipe_id absent → 422 PROVENANCE_INCOMPLETE naming the field."""
    triple = _full_triple()
    triple["pipe_id"] = None
    resp = _post(triple)
    assert resp.status_code == 422, resp.text
    body = resp.json()
    assert body["detail"]["error"] == "PROVENANCE_INCOMPLETE"
    assert body["detail"]["field"] == "pipe_id"
    assert "pipe_id" in body["detail"]["message"]


def test_negative_missing_fabric_plane_rejected_422():
    """fabric_plane absent → 422 PROVENANCE_INCOMPLETE naming the field."""
    triple = _full_triple()
    triple["fabric_plane"] = None
    resp = _post(triple)
    assert resp.status_code == 422, resp.text
    body = resp.json()
    assert body["detail"]["error"] == "PROVENANCE_INCOMPLETE"
    assert body["detail"]["field"] == "fabric_plane"
    assert "fabric_plane" in body["detail"]["message"]


def test_negative_confidence_score_out_of_range_rejected():
    """confidence_score outside [0,1] → 400 VALIDATION_FAILED.

    Pre-existing validator; included to document the full provenance
    contract surface even though the status code differs from the
    PROVENANCE_INCOMPLETE family.
    """
    triple = _full_triple(confidence_score=1.5)
    resp = _post(triple)
    assert resp.status_code == 400, resp.text
    assert "confidence_score" in resp.json()["detail"]["message"]


def test_negative_one_bad_triple_rejects_entire_batch():
    """Atomic batch: a bad triple in a 3-triple POST rejects the whole batch.

    No per-row partial accept (per the contract). The offending triple's
    index is named in the error so the producer can fix at source.
    """
    good_a = _full_triple(entity_id="batch_a")
    bad = _full_triple(entity_id="batch_b")
    bad["pipe_id"] = None  # provenance violation
    good_c = _full_triple(entity_id="batch_c")
    run_id = str(uuid.uuid4())
    resp = client.post(
        "/api/dcl/ingest-triples",
        json={
            "tenant_id": TEST_TENANT_ID,
            "run_id": run_id,
            "triples": [good_a, bad, good_c],
        },
    )
    assert resp.status_code == 422, resp.text
    body = resp.json()
    assert body["detail"]["triple_index"] == 1
    assert body["detail"]["field"] == "pipe_id"

    # Confirm nothing landed
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM semantic_triples WHERE tenant_id = %s AND run_id = %s",
                (TEST_TENANT_ID, run_id),
            )
            count = cur.fetchone()[0]
    assert count == 0, "Atomic batch should not have written any rows on rejection"
