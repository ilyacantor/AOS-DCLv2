"""
S1-DCL Harness — 20 tests for semantic triple store.

Tests schema, data access, ingest endpoint, and concept registry
using synthetic triples. No Farm dependency.
"""

import json
import os
import sys
import uuid
import pytest
from pathlib import Path

# Ensure repo root on path
_repo = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_repo))

# Load .env for DATABASE_URL
from dotenv import load_dotenv
load_dotenv(_repo / ".env")

from backend.core.db import get_connection
from backend.db.triple_store import TripleStore
from backend.db.run_ledger_store import RunLedgerStore
from backend.registry.concept_registry import ConceptRegistry

# FastAPI test client
from fastapi.testclient import TestClient
from backend.api.main import app

client = TestClient(app, raise_server_exceptions=False)

# ---------------------------------------------------------------------------
# Fixtures & helpers
# ---------------------------------------------------------------------------

TEST_TENANT_ID = str(uuid.uuid5(uuid.NAMESPACE_DNS, "s1-dcl-test-tenant"))
TEST_RUN_ID_A = str(uuid.uuid5(uuid.NAMESPACE_DNS, "s1-dcl-test-run-a"))
TEST_RUN_ID_B = str(uuid.uuid5(uuid.NAMESPACE_DNS, "s1-dcl-test-run-b"))
TEST_ENGAGEMENT_ID = "s1-dcl-test-engagement"
TEST_IDEM_PREFIX = "s1-dcl-test-idem"


def make_test_triple(
    entity_id="test_entity",
    concept="revenue.total",
    property="amount",
    value=1000000,
    period="2025-Q1",
    **overrides,
) -> dict:
    """Factory for test triples."""
    base = {
        "entity_id": entity_id,
        "concept": concept,
        "property": property,
        "value": value,
        "period": period,
        "currency": "USD",
        "unit": "dollars",
        "source_system": "test",
        "confidence_score": 0.95,
        "confidence_tier": "high",
    }
    base.update(overrides)
    return base


@pytest.fixture(autouse=True, scope="module")
def cleanup():
    """Clean up all test data before and after the test module."""
    _cleanup_all()
    yield
    _cleanup_all()


def _cleanup_all():
    """Remove all test data from all tables."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Triples
            cur.execute(
                "DELETE FROM semantic_triples WHERE tenant_id = %s",
                (TEST_TENANT_ID,),
            )
            # Tenant run pointer
            cur.execute(
                "DELETE FROM tenant_runs WHERE tenant_id = %s",
                (TEST_TENANT_ID,),
            )
            # Resolution workspaces
            cur.execute(
                "DELETE FROM resolution_workspaces WHERE tenant_id = %s",
                (TEST_TENANT_ID,),
            )
            # Run ledger
            cur.execute(
                "DELETE FROM run_ledger WHERE tenant_id = %s",
                (TEST_TENANT_ID,),
            )
            conn.commit()


# ---------------------------------------------------------------------------
# Schema tests
# ---------------------------------------------------------------------------

class TestSchema:
    """Tests 1-2: Tables and indexes exist."""

    def test_01_tables_exist(self):
        """All 5 tables present in PG."""
        expected_tables = [
            "semantic_triples",
            "dimension_values_v2",
            "resolution_workspaces",
            "engagement_state",
            "run_ledger",
        ]
        with get_connection() as conn:
            assert conn is not None, "Database connection unavailable"
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = 'public' AND table_name = ANY(%s)",
                    (expected_tables,),
                )
                found = {row[0] for row in cur.fetchall()}
        for t in expected_tables:
            assert t in found, f"Table '{t}' not found in PG. Found: {found}"

    def test_02_indexes_exist(self):
        """All indexes from migration present."""
        expected_indexes = [
            "idx_triples_entity_concept",
            "idx_triples_concept_period",
            "idx_triples_run",
            "idx_triples_canonical",
            "idx_triples_entity_period",
            "idx_triples_active",
            "idx_dimval_v2_tenant_dim",
            "idx_dimval_v2_parent",
            "idx_resws_tenant_status",
            "idx_resws_type",
            "idx_engagement_tenant",
            "idx_engagement_eid",
            "idx_runledger_engagement",
            "idx_runledger_idem",
            "idx_runledger_status",
        ]
        with get_connection() as conn:
            assert conn is not None, "Database connection unavailable"
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT indexname FROM pg_indexes "
                    "WHERE schemaname = 'public' AND indexname = ANY(%s)",
                    (expected_indexes,),
                )
                found = {row[0] for row in cur.fetchall()}
        for idx in expected_indexes:
            assert idx in found, f"Index '{idx}' not found in PG. Found: {found}"


# ---------------------------------------------------------------------------
# Data access tests
# ---------------------------------------------------------------------------

class TestTripleStore:
    """Tests 3-7: Triple CRUD."""

    def setup_method(self):
        self.store = TripleStore()
        # Clean test data before each test method
        self.store.delete_by_run(TEST_RUN_ID_A)
        self.store.delete_by_run(TEST_RUN_ID_B)
        # Register run A as current so get_triples(active_only=True) works via current_run_id pointer
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM tenant_runs WHERE tenant_id = %s",
                    (TEST_TENANT_ID,),
                )
                conn.commit()
        self.store.upsert_tenant_run(TEST_TENANT_ID, TEST_RUN_ID_A, "test_entity")

    def test_03_triple_round_trip(self):
        """Insert via TripleStore → read back → all fields match. JSONB numeric values survive."""
        t = make_test_triple(value=1234567.89)
        t["tenant_id"] = TEST_TENANT_ID
        t["run_id"] = TEST_RUN_ID_A
        t["source_table"] = "opportunities"
        t["source_field"] = "total_revenue"

        self.store.insert_triples([t])
        rows = self.store.get_triples(TEST_TENANT_ID, "revenue.total")
        assert len(rows) >= 1, "Expected at least 1 triple back"

        r = rows[0]
        assert r["entity_id"] == "test_entity"
        assert r["concept"] == "revenue.total"
        assert r["property"] == "amount"
        # JSONB numeric — parse it back
        val = r["value"]
        if isinstance(val, str):
            val = json.loads(val)
        assert val == 1234567.89, f"JSONB numeric corruption: expected 1234567.89, got {val}"
        assert r["period"] == "2025-Q1"
        assert r["currency"] == "USD"
        assert r["source_system"] == "test"
        assert float(r["confidence_score"]) == 0.95
        assert r["confidence_tier"] == "high"
        assert r["is_active"] is True

    def test_04_batch_insert(self):
        """Insert 100 triples → count returned is 100 → read back count is 100."""
        triples = []
        for i in range(100):
            t = make_test_triple(entity_id=f"batch_entity_{i}", value=i * 1000)
            t["tenant_id"] = TEST_TENANT_ID
            t["run_id"] = TEST_RUN_ID_A
            triples.append(t)

        count = self.store.insert_triples(triples)
        assert count == 100, f"insert_triples returned {count}, expected 100"

        read_count = self.store.count_by_run(TEST_RUN_ID_A)
        assert read_count == 100, f"count_by_run returned {read_count}, expected 100"

    def test_05_constraint_enforcement(self):
        """Check constraints: bad confidence_score, bad tier, bad resolution_method, null concept."""
        base = make_test_triple()
        base["tenant_id"] = TEST_TENANT_ID
        base["run_id"] = TEST_RUN_ID_A

        # confidence_score = 1.5 → fails
        bad = {**base, "confidence_score": 1.5}
        with pytest.raises(Exception):
            self.store.insert_triples([bad])

        # confidence_score = -0.1 → fails
        bad2 = {**base, "confidence_score": -0.1}
        with pytest.raises(Exception):
            self.store.insert_triples([bad2])

        # confidence_tier = 'unknown' → fails
        bad3 = {**base, "confidence_tier": "unknown"}
        with pytest.raises(Exception):
            self.store.insert_triples([bad3])

        # resolution_method = 'magic' → fails
        bad4 = {**base, "resolution_method": "magic"}
        with pytest.raises(Exception):
            self.store.insert_triples([bad4])

        # concept = NULL → fails
        bad5 = {**base, "concept": None}
        with pytest.raises(Exception):
            self.store.insert_triples([bad5])

    def test_06_query_filtering(self):
        """Two entities, two concepts. Query by entity/concept/period."""
        triples = [
            {**make_test_triple(entity_id="ent_a", concept="revenue.total", period="2025-Q1"),
             "tenant_id": TEST_TENANT_ID, "run_id": TEST_RUN_ID_A},
            {**make_test_triple(entity_id="ent_a", concept="cost.direct", period="2025-Q1"),
             "tenant_id": TEST_TENANT_ID, "run_id": TEST_RUN_ID_A},
            {**make_test_triple(entity_id="ent_b", concept="revenue.total", period="2025-Q2"),
             "tenant_id": TEST_TENANT_ID, "run_id": TEST_RUN_ID_A},
            {**make_test_triple(entity_id="ent_b", concept="cost.direct", period="2025-Q2"),
             "tenant_id": TEST_TENANT_ID, "run_id": TEST_RUN_ID_A},
        ]
        self.store.insert_triples(triples)

        # By entity
        by_ent_a = self.store.get_triples(TEST_TENANT_ID, "revenue.total", entity_id="ent_a")
        assert len(by_ent_a) == 1
        assert by_ent_a[0]["entity_id"] == "ent_a"

        # By concept
        by_rev = self.store.get_triples(TEST_TENANT_ID, "revenue.total")
        assert len(by_rev) == 2

        # By entity + concept
        by_both = self.store.get_triples(TEST_TENANT_ID, "cost.direct", entity_id="ent_b")
        assert len(by_both) == 1
        assert by_both[0]["entity_id"] == "ent_b"

        # By period
        by_period = self.store.get_triples(TEST_TENANT_ID, "revenue.total", period="2025-Q2")
        assert len(by_period) == 1
        assert by_period[0]["entity_id"] == "ent_b"

    def test_07_run_deactivation(self):
        """Per-entity pointers: each entity gets its own current_run_id."""
        t_a = {**make_test_triple(entity_id="deact_a"), "tenant_id": TEST_TENANT_ID, "run_id": TEST_RUN_ID_A}
        t_b = {**make_test_triple(entity_id="deact_b"), "tenant_id": TEST_TENANT_ID, "run_id": TEST_RUN_ID_B}
        self.store.insert_triples([t_a])
        self.store.insert_triples([t_b])

        # Register both entities — each gets its own pointer row
        self.store.upsert_tenant_run(TEST_TENANT_ID, TEST_RUN_ID_A, "deact_a")
        self.store.upsert_tenant_run(TEST_TENANT_ID, TEST_RUN_ID_B, "deact_b")

        # Each entity points to its own run
        current_a = self.store.get_current_run_id(TEST_TENANT_ID, "deact_a")
        current_b = self.store.get_current_run_id(TEST_TENANT_ID, "deact_b")
        assert current_a == TEST_RUN_ID_A, "Entity deact_a should point to run A"
        assert current_b == TEST_RUN_ID_B, "Entity deact_b should point to run B"


class TestRunLedgerStore:
    """Tests 10-11: Run ledger CRUD and array query."""

    def setup_method(self):
        self.store = RunLedgerStore()

    def teardown_method(self):
        self.store.delete_by_engagement(TEST_ENGAGEMENT_ID)
        for i in range(5):
            self.store.delete_by_idempotency_key(f"{TEST_IDEM_PREFIX}-{i}")

    def test_10_run_ledger_round_trip(self):
        """Create → read → verify. Duplicate idempotency_key → fails."""
        step = self.store.create_step({
            "tenant_id": TEST_TENANT_ID,
            "engagement_id": TEST_ENGAGEMENT_ID,
            "step_name": "extract",
            "status": "pending",
            "idempotency_key": f"{TEST_IDEM_PREFIX}-0",
            "upstream_deps": ["init"],
        })
        assert step["step_name"] == "extract"
        assert step["status"] == "pending"
        assert step["idempotency_key"] == f"{TEST_IDEM_PREFIX}-0"

        fetched = self.store.get_step(str(step["id"]))
        assert fetched is not None
        assert fetched["step_name"] == "extract"

        # Duplicate idempotency_key → fails
        with pytest.raises(Exception):
            self.store.create_step({
                "tenant_id": TEST_TENANT_ID,
                "engagement_id": TEST_ENGAGEMENT_ID,
                "step_name": "extract_dup",
                "status": "pending",
                "idempotency_key": f"{TEST_IDEM_PREFIX}-0",
            })

    def test_11_upstream_deps_query(self):
        """Three steps with upstream_deps. Query downstream entries."""
        self.store.create_step({
            "tenant_id": TEST_TENANT_ID,
            "engagement_id": TEST_ENGAGEMENT_ID,
            "step_name": "step_a",
            "status": "complete",
            "idempotency_key": f"{TEST_IDEM_PREFIX}-1",
            "upstream_deps": None,
        })
        self.store.create_step({
            "tenant_id": TEST_TENANT_ID,
            "engagement_id": TEST_ENGAGEMENT_ID,
            "step_name": "step_b",
            "status": "pending",
            "idempotency_key": f"{TEST_IDEM_PREFIX}-2",
            "upstream_deps": ["step_a"],
        })
        self.store.create_step({
            "tenant_id": TEST_TENANT_ID,
            "engagement_id": TEST_ENGAGEMENT_ID,
            "step_name": "step_c",
            "status": "pending",
            "idempotency_key": f"{TEST_IDEM_PREFIX}-3",
            "upstream_deps": ["step_a", "step_b"],
        })

        downstream_of_a = self.store.find_downstream("step_a")
        names = [s["step_name"] for s in downstream_of_a]
        assert "step_b" in names, f"step_b should depend on step_a. Got: {names}"
        assert "step_c" in names, f"step_c should depend on step_a. Got: {names}"

        downstream_of_b = self.store.find_downstream("step_b")
        names_b = [s["step_name"] for s in downstream_of_b]
        assert "step_c" in names_b, f"step_c should depend on step_b. Got: {names_b}"
        assert "step_b" not in names_b


# ---------------------------------------------------------------------------
# Ingest endpoint tests
# ---------------------------------------------------------------------------

class TestIngestEndpoint:
    """Tests 12-19: REST endpoint validation, atomicity, idempotency."""

    def setup_method(self):
        _cleanup_all()

    def teardown_method(self):
        _cleanup_all()

    def _post_triples(self, triples, tenant_id=None, run_id=None, replace=False):
        tid = tenant_id or TEST_TENANT_ID
        rid = run_id or str(uuid.uuid4())
        payload = {
            "tenant_id": tid,
            "run_id": rid,
            "triples": triples,
        }
        url = "/api/dcl/ingest-triples"
        if replace:
            url += "?replace=true"
        return client.post(url, json=payload), rid

    def test_12_ingest_valid_triples(self):
        """POST 10 triples → 201 with correct count and concept summary."""
        triples = [make_test_triple(entity_id=f"e_{i}") for i in range(10)]
        resp, rid = self._post_triples(triples)
        assert resp.status_code == 201, f"Expected 201, got {resp.status_code}: {resp.text}"
        body = resp.json()
        assert body["triple_count"] == 10
        assert "revenue" in body["concept_summary"]
        assert body["concept_summary"]["revenue"] == 10

    def test_13_reject_invalid_concept(self):
        """POST triple with concept 'zzz_fake.something' → 400."""
        triples = [make_test_triple(concept="zzz_fake.something")]
        resp, _ = self._post_triples(triples)
        assert resp.status_code == 400, f"Expected 400, got {resp.status_code}: {resp.text}"
        assert "INVALID_CONCEPT" in resp.text

    def test_14_reject_missing_field(self):
        """POST triple with null concept → 400."""
        triples = [make_test_triple()]
        triples[0]["concept"] = None
        resp, _ = self._post_triples(triples)
        assert resp.status_code == 422 or resp.status_code == 400, (
            f"Expected 400 or 422, got {resp.status_code}: {resp.text}"
        )

    def test_15_reject_bad_confidence(self):
        """POST triple with confidence_score 1.5 → 400."""
        triples = [make_test_triple(confidence_score=1.5)]
        resp, _ = self._post_triples(triples)
        assert resp.status_code == 400, f"Expected 400, got {resp.status_code}: {resp.text}"
        assert "confidence_score" in resp.text

    def test_16_batch_atomicity(self):
        """POST 5 triples where #3 has invalid concept → entire batch rejected → 0 triples."""
        run_id = str(uuid.uuid4())
        triples = [make_test_triple(entity_id=f"atom_{i}") for i in range(5)]
        triples[2]["concept"] = "zzz_invalid.nope"
        resp, _ = self._post_triples(triples, run_id=run_id)
        assert resp.status_code == 400

        # Verify 0 triples in PG for that run
        store = TripleStore()
        rows = store.get_triples_by_run(run_id)
        assert len(rows) == 0, f"Expected 0 triples after batch reject, got {len(rows)}"

    def test_17_idempotency_reject(self):
        """Ingest run A → re-ingest run A → 409."""
        run_id = str(uuid.uuid4())
        triples = [make_test_triple()]
        resp1, _ = self._post_triples(triples, run_id=run_id)
        assert resp1.status_code == 201

        resp2, _ = self._post_triples(triples, run_id=run_id)
        assert resp2.status_code == 409, f"Expected 409, got {resp2.status_code}: {resp2.text}"

    def test_18_idempotency_replace(self):
        """Ingest run -> re-ingest same run_id with ?replace=true -> 201.
        replace=true deletes triples for the ENTITIES in the new batch only,
        preserving other entities' data within the same tenant."""
        run_id = str(uuid.uuid4())
        triples_v1 = [make_test_triple(entity_id="v1_entity", value=100)]
        resp1, _ = self._post_triples(triples_v1, run_id=run_id)
        assert resp1.status_code == 201

        triples_v2 = [make_test_triple(entity_id="v2_entity", value=200)]
        resp2, _ = self._post_triples(triples_v2, run_id=run_id, replace=True)
        assert resp2.status_code == 201

        store = TripleStore()
        all_rows = store.get_triples_by_run(run_id)
        entity_ids = {r["entity_id"] for r in all_rows}
        # v1_entity survives — entity-scoped replace only deletes matching entities
        assert "v1_entity" in entity_ids, "v1_entity should survive (different entity)"
        assert "v2_entity" in entity_ids, "v2_entity should be present after replace"
        assert len(all_rows) == 2, f"Expected 2 triples after replace, got {len(all_rows)}"

    def test_18b_replace_different_runs(self):
        """Ingest run_A -> ingest run_B with replace=true -> run_A entities survive
        because replace is scoped to entities in the new batch."""
        run_id_1 = str(uuid.uuid4())
        run_id_2 = str(uuid.uuid4())

        triples_1 = [
            make_test_triple(entity_id="alpha", concept="revenue.total", value=111),
            make_test_triple(entity_id="beta", concept="cost.direct", value=222),
        ]
        resp1, _ = self._post_triples(triples_1, run_id=run_id_1)
        assert resp1.status_code == 201

        triples_2 = [make_test_triple(entity_id="gamma", concept="revenue.consulting", value=333)]
        resp2, _ = self._post_triples(triples_2, run_id=run_id_2, replace=True)
        assert resp2.status_code == 201

        store = TripleStore()
        # alpha and beta survive — different entities from gamma
        old_rows = store.get_triples_by_run(run_id_1)
        assert len(old_rows) == 2, f"Expected 2 triples for run_1 (preserved), got {len(old_rows)}"

        new_rows = store.get_triples_by_run(run_id_2)
        assert len(new_rows) == 1, f"Expected 1 triple for run_2, got {len(new_rows)}"
        assert new_rows[0]["entity_id"] == "gamma"

    def test_18c_replace_same_entity(self):
        """Replace=true with same entity_id deletes old triples for that entity."""
        run_id = str(uuid.uuid4())
        triples_v1 = [make_test_triple(entity_id="entity_alpha", value=100)]
        resp1, _ = self._post_triples(triples_v1, run_id=run_id)
        assert resp1.status_code == 201

        triples_v2 = [make_test_triple(entity_id="entity_alpha", value=999)]
        resp2, _ = self._post_triples(triples_v2, run_id=run_id, replace=True)
        assert resp2.status_code == 201

        store = TripleStore()
        rows = store.get_triples_by_run(run_id)
        assert len(rows) == 1, f"Expected 1 triple after same-entity replace, got {len(rows)}"
        assert rows[0]["value"] == 999, "Should have the replaced value"

    def test_18d_replace_preserves_other_entity(self):
        """Multi-entity scenario: replacing one entity's triples preserves the other."""
        run_id = str(uuid.uuid4())
        triples = [
            make_test_triple(entity_id="entity_alpha", concept="revenue.total", value=5_000),
            make_test_triple(entity_id="entity_beta", concept="revenue.total", value=1_000),
        ]
        resp1, _ = self._post_triples(triples, run_id=run_id)
        assert resp1.status_code == 201

        # Replace only entity_alpha
        new_alpha = [make_test_triple(entity_id="entity_alpha", concept="revenue.total", value=5_500)]
        resp2, _ = self._post_triples(new_alpha, run_id=run_id, replace=True)
        assert resp2.status_code == 201

        store = TripleStore()
        rows = store.get_triples_by_run(run_id)
        by_entity = {r["entity_id"]: r["value"] for r in rows}
        assert by_entity["entity_alpha"] == 5_500, "Alpha should have new value"
        assert by_entity["entity_beta"] == 1_000, "Beta should be untouched"

    def test_19_run_status_endpoint(self):
        """Ingest → GET status → correct count and summary."""
        run_id = str(uuid.uuid4())
        triples = [
            make_test_triple(concept="revenue.total"),
            make_test_triple(concept="revenue.consulting"),
            make_test_triple(concept="cost.direct"),
        ]
        resp, _ = self._post_triples(triples, run_id=run_id)
        assert resp.status_code == 201

        status_resp = client.get(f"/api/dcl/ingest-status/{run_id}")
        assert status_resp.status_code == 200, f"Expected 200, got {status_resp.status_code}: {status_resp.text}"
        body = status_resp.json()
        assert body["triple_count"] == 3
        assert body["concept_summary"]["revenue"] == 2
        assert body["concept_summary"]["cost"] == 1
        assert body["is_active"] is True


# ---------------------------------------------------------------------------
# Concept registry tests
# ---------------------------------------------------------------------------

class TestConceptRegistry:
    """Test 20: Concept registry validation."""

    def test_20_concept_registry(self):
        """Registry loads from YAML. Hierarchical validation. Invalid concept rejected."""
        registry = ConceptRegistry()

        concepts = registry.list_concepts()
        assert len(concepts) >= 107, f"Expected ≥107 concepts, got {len(concepts)}"

        # Known concepts must be valid
        assert registry.is_valid_concept("revenue"), "'revenue' should be valid"
        assert registry.is_valid_concept("revenue.total"), "'revenue.total' should be valid"
        assert registry.is_valid_concept("revenue.total.consulting"), "'revenue.total.consulting' should be valid"
        assert registry.is_valid_concept("cost"), "'cost' should be valid"
        assert registry.is_valid_concept("customer"), "'customer' should be valid"
        assert registry.is_valid_concept("vendor"), "'vendor' should be valid"
        assert registry.is_valid_concept("employee"), "'employee' should be valid"

        # New concepts added in S1
        assert registry.is_valid_concept("ebitda_adjustment"), "'ebitda_adjustment' should be valid"
        assert registry.is_valid_concept("service"), "'service' should be valid"
        assert registry.is_valid_concept("cash_flow"), "'cash_flow' should be valid"
        assert registry.is_valid_concept("asset"), "'asset' should be valid"
        assert registry.is_valid_concept("liability"), "'liability' should be valid"
        assert registry.is_valid_concept("equity"), "'equity' should be valid"
        assert registry.is_valid_concept("bench"), "'bench' should be valid"

        # Invalid
        assert not registry.is_valid_concept("zzz_not_real"), "'zzz_not_real' should be invalid"
        assert not registry.is_valid_concept(""), "empty string should be invalid"

        # Domain lookup
        assert registry.get_domain("revenue.total") == "finance"
        assert registry.get_domain("customer") == "sales"
        assert registry.get_domain("zzz") is None
