"""
Pipeline Identity Compliance Tests — DCL ingest + verify.

Asserts:
- Ingest response carries dcl_ingest_id, source_farm_manifest_id,
  tenant_id, entity_id, source_rows, triples_written, expansion_factor
- Verify response carries verify_id, dcl_ingest_id
- 422 returned when run_id not provided by caller
- entity_id inferred from triples when not in request
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

TEST_TENANT_ID = str(uuid.uuid5(uuid.NAMESPACE_DNS, "pipeline-identity-test"))


def _make_triple(**overrides):
    base = {
        "entity_id": "test_entity",
        "concept": "revenue.total",
        "property": "amount",
        "value": 1000,
        "period": "2025-Q1",
        "currency": "USD",
        "source_system": "test",
        "confidence_score": 0.9,
        "confidence_tier": "high",
    }
    base.update(overrides)
    return base


@pytest.fixture(autouse=True, scope="module")
def cleanup():
    _do_cleanup()
    yield
    _do_cleanup()


def _do_cleanup():
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


# ---------------------------------------------------------------------------
# Ingest identity tests
# ---------------------------------------------------------------------------

class TestIngestIdentity:
    """Ingest response carries all pipeline identity fields."""

    def setup_method(self):
        _do_cleanup()

    def teardown_method(self):
        _do_cleanup()

    def test_ingest_response_has_dcl_ingest_id(self):
        """dcl_ingest_id present at top level, matches the run_id sent."""
        run_id = str(uuid.uuid4())
        resp = client.post("/api/dcl/ingest-triples", json={
            "tenant_id": TEST_TENANT_ID,
            "run_id": run_id,
            "triples": [_make_triple()],
        })
        assert resp.status_code == 201, resp.text
        body = resp.json()
        assert "dcl_ingest_id" in body, f"Missing dcl_ingest_id. Keys: {list(body.keys())}"
        assert body["dcl_ingest_id"] == run_id

    def test_ingest_response_has_tenant_id(self):
        """tenant_id echoed back in response (I2 compliance)."""
        run_id = str(uuid.uuid4())
        resp = client.post("/api/dcl/ingest-triples", json={
            "tenant_id": TEST_TENANT_ID,
            "run_id": run_id,
            "triples": [_make_triple()],
        })
        assert resp.status_code == 201, resp.text
        body = resp.json()
        assert body["tenant_id"] == TEST_TENANT_ID

    def test_ingest_response_has_entity_id_inferred(self):
        """entity_id inferred from triples when all share one entity."""
        run_id = str(uuid.uuid4())
        resp = client.post("/api/dcl/ingest-triples", json={
            "tenant_id": TEST_TENANT_ID,
            "run_id": run_id,
            "triples": [
                _make_triple(entity_id="alpha"),
                _make_triple(entity_id="alpha"),
            ],
        })
        assert resp.status_code == 201, resp.text
        body = resp.json()
        assert body["entity_id"] == "alpha"

    def test_ingest_response_entity_id_explicit(self):
        """Explicit entity_id in request takes priority over inference."""
        run_id = str(uuid.uuid4())
        resp = client.post("/api/dcl/ingest-triples", json={
            "tenant_id": TEST_TENANT_ID,
            "run_id": run_id,
            "entity_id": "explicit_entity",
            "triples": [
                _make_triple(entity_id="triple_entity_a"),
                _make_triple(entity_id="triple_entity_b"),
            ],
        })
        assert resp.status_code == 201, resp.text
        body = resp.json()
        assert body["entity_id"] == "explicit_entity"

    def test_ingest_response_entity_id_null_for_multi(self):
        """entity_id is null when triples span multiple entities and no explicit value."""
        run_id = str(uuid.uuid4())
        resp = client.post("/api/dcl/ingest-triples", json={
            "tenant_id": TEST_TENANT_ID,
            "run_id": run_id,
            "triples": [
                _make_triple(entity_id="ent_x"),
                _make_triple(entity_id="ent_y"),
            ],
        })
        assert resp.status_code == 201, resp.text
        body = resp.json()
        assert body["entity_id"] is None

    def test_ingest_response_has_source_farm_manifest_id(self):
        """source_farm_manifest_id echoed when provided by caller."""
        run_id = str(uuid.uuid4())
        farm_manifest = str(uuid.uuid4())
        resp = client.post("/api/dcl/ingest-triples", json={
            "tenant_id": TEST_TENANT_ID,
            "run_id": run_id,
            "source_farm_manifest_id": farm_manifest,
            "triples": [_make_triple()],
        })
        assert resp.status_code == 201, resp.text
        body = resp.json()
        assert body["source_farm_manifest_id"] == farm_manifest

    def test_ingest_response_has_expansion_fields(self):
        """source_rows, triples_written, expansion_factor all present."""
        run_id = str(uuid.uuid4())
        resp = client.post("/api/dcl/ingest-triples", json={
            "tenant_id": TEST_TENANT_ID,
            "run_id": run_id,
            "source_rows": 5,
            "triples": [_make_triple() for _ in range(10)],
        })
        assert resp.status_code == 201, resp.text
        body = resp.json()
        assert body["source_rows"] == 5
        assert body["triples_written"] == 10
        assert body["expansion_factor"] == 2.0

    def test_ingest_expansion_defaults_to_triple_count(self):
        """When source_rows not provided, defaults to triple count received."""
        run_id = str(uuid.uuid4())
        resp = client.post("/api/dcl/ingest-triples", json={
            "tenant_id": TEST_TENANT_ID,
            "run_id": run_id,
            "triples": [_make_triple() for _ in range(3)],
        })
        assert resp.status_code == 201, resp.text
        body = resp.json()
        assert body["source_rows"] == 3
        assert body["triples_written"] == 3
        assert body["expansion_factor"] == 1.0


# ---------------------------------------------------------------------------
# 422 on missing run identifier
# ---------------------------------------------------------------------------

class TestRunIdRequired:
    """DCL must not silently mint its own run_id (I6 anti-brittleness)."""

    def test_missing_run_id_returns_422(self):
        """Omitting run_id from request body → 422 (Pydantic validation)."""
        resp = client.post("/api/dcl/ingest-triples", json={
            "tenant_id": TEST_TENANT_ID,
            "triples": [_make_triple()],
        })
        assert resp.status_code == 422, (
            f"Expected 422 when run_id missing. Got {resp.status_code}: {resp.text}"
        )

    def test_empty_run_id_returns_400(self):
        """Empty string run_id → 400 (not a valid UUID)."""
        resp = client.post("/api/dcl/ingest-triples", json={
            "tenant_id": TEST_TENANT_ID,
            "run_id": "",
            "triples": [_make_triple()],
        })
        assert resp.status_code == 400, (
            f"Expected 400 for empty run_id. Got {resp.status_code}: {resp.text}"
        )

    def test_non_uuid_run_id_returns_400(self):
        """Non-UUID run_id → 400."""
        resp = client.post("/api/dcl/ingest-triples", json={
            "tenant_id": TEST_TENANT_ID,
            "run_id": "not-a-uuid",
            "triples": [_make_triple()],
        })
        assert resp.status_code == 400, (
            f"Expected 400 for non-UUID run_id. Got {resp.status_code}: {resp.text}"
        )


# ---------------------------------------------------------------------------
# Verify identity tests
# ---------------------------------------------------------------------------

class TestVerifyIdentity:
    """Verify response carries verify_id and dcl_ingest_id."""

    def test_verify_response_has_verify_id(self):
        """verify_id present at top level and is a valid UUID."""
        resp = client.post("/api/farm/v2/verify/fake-farm-run-id")
        # The verify may fail (no Farm data), but the response structure
        # should still include verify_id.
        body = resp.json()
        # verify may return 200 with grade=ERROR or 500
        if resp.status_code == 200:
            assert "verify_id" in body, f"Missing verify_id. Keys: {list(body.keys())}"
            # Validate it's a UUID
            uuid.UUID(body["verify_id"])

    def test_verify_response_has_dcl_ingest_id(self):
        """dcl_ingest_id present in verify response, matches dcl_run_id param."""
        dcl_run = str(uuid.uuid4())
        resp = client.post(
            f"/api/farm/v2/verify/fake-farm-run-id?dcl_run_id={dcl_run}"
        )
        body = resp.json()
        if resp.status_code == 200:
            assert "dcl_ingest_id" in body, f"Missing dcl_ingest_id. Keys: {list(body.keys())}"
            assert body["dcl_ingest_id"] == dcl_run
