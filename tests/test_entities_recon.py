"""
Integration tests for GET /api/dcl/entities and entity_id scoping on /api/dcl/recon.

Requires: DCL running on port 8004 with at least one active ingest run.
"""

import pytest
import httpx

DCL_BASE = "http://localhost:8004"


@pytest.fixture(scope="module")
def client():
    return httpx.Client(base_url=DCL_BASE, timeout=30.0)


@pytest.fixture(scope="module")
def entities_response(client):
    """Fetch entities once for the module."""
    resp = client.get("/api/dcl/entities")
    assert resp.status_code == 200, f"GET /api/dcl/entities returned {resp.status_code}: {resp.text}"
    return resp.json()


# ---------------------------------------------------------------------------
# GET /api/dcl/entities
# ---------------------------------------------------------------------------

class TestEntitiesEndpoint:
    def test_returns_200_with_entities_key(self, entities_response):
        assert "entities" in entities_response
        assert isinstance(entities_response["entities"], list)

    def test_entities_have_required_fields(self, entities_response):
        required = {"entity_id", "display_name", "triple_count", "latest_ingest", "is_most_recent"}
        for entity in entities_response["entities"]:
            missing = required - set(entity.keys())
            assert not missing, (
                f"Entity '{entity.get('entity_id', '?')}' missing fields: {missing}"
            )

    def test_triple_count_positive(self, entities_response):
        for entity in entities_response["entities"]:
            assert entity["triple_count"] > 0, (
                f"Entity '{entity['entity_id']}' has triple_count={entity['triple_count']} — "
                f"only active triples should appear"
            )

    def test_exactly_one_most_recent(self, entities_response):
        entities = entities_response["entities"]
        if not entities:
            pytest.skip("No entities in the store — cannot verify is_most_recent")
        most_recent = [e for e in entities if e["is_most_recent"]]
        assert len(most_recent) == 1, (
            f"Expected exactly 1 entity with is_most_recent=true, got {len(most_recent)}: "
            f"{[e['entity_id'] for e in most_recent]}"
        )

    def test_most_recent_is_first(self, entities_response):
        entities = entities_response["entities"]
        if not entities:
            pytest.skip("No entities in the store")
        assert entities[0]["is_most_recent"] is True, (
            f"First entity '{entities[0]['entity_id']}' should be is_most_recent=true "
            f"(ordered by latest_ingest DESC)"
        )

    def test_ordered_by_latest_ingest_desc(self, entities_response):
        entities = entities_response["entities"]
        if len(entities) < 2:
            pytest.skip("Need >= 2 entities to verify ordering")
        timestamps = [e["latest_ingest"] for e in entities]
        assert timestamps == sorted(timestamps, reverse=True), (
            f"Entities not ordered by latest_ingest DESC: {timestamps}"
        )

    def test_display_name_not_empty(self, entities_response):
        for entity in entities_response["entities"]:
            assert entity["display_name"], (
                f"Entity '{entity['entity_id']}' has empty display_name"
            )


# ---------------------------------------------------------------------------
# GET /api/dcl/recon — entity_id scoping
# ---------------------------------------------------------------------------

class TestReconEntityScoping:
    def test_recon_no_params_unchanged(self, client):
        """Existing behavior: no params → latest run, 5 checks."""
        resp = client.get("/api/dcl/recon")
        assert resp.status_code == 200
        data = resp.json()
        assert "checks" in data
        assert len(data["checks"]) == 5, f"Expected 5 checks, got {len(data['checks'])}"
        assert "run_id" in data

    def test_recon_with_entity_id(self, client, entities_response):
        """Recon scoped by entity_id resolves a run and returns 5 checks."""
        entities = entities_response["entities"]
        if not entities:
            pytest.skip("No entities available")
        entity_id = entities[0]["entity_id"]

        resp = client.get("/api/dcl/recon", params={"entity_id": entity_id})
        assert resp.status_code == 200
        data = resp.json()
        assert data["entity_id"] == entity_id, (
            f"Response entity_id should be '{entity_id}', got '{data.get('entity_id')}'"
        )
        assert len(data["checks"]) == 5
        assert data["run_id"] is not None, "entity_id should resolve to a run_id"

    def test_recon_entity_id_in_response(self, client, entities_response):
        """Response includes entity_id field even when None."""
        resp = client.get("/api/dcl/recon")
        data = resp.json()
        assert "entity_id" in data, "Response must include entity_id field"

    def test_recon_entity_with_run_id(self, client, entities_response):
        """Both entity_id and run_id work together."""
        entities = entities_response["entities"]
        if not entities:
            pytest.skip("No entities available")
        entity_id = entities[0]["entity_id"]

        # First get a valid run_id for this entity
        resp1 = client.get("/api/dcl/recon", params={"entity_id": entity_id})
        run_id = resp1.json()["run_id"]

        # Now pass both
        resp2 = client.get("/api/dcl/recon", params={"entity_id": entity_id, "run_id": run_id})
        assert resp2.status_code == 200
        data = resp2.json()
        assert data["entity_id"] == entity_id
        assert data["run_id"] == run_id
        assert len(data["checks"]) == 5

    def test_recon_nonexistent_entity_returns_fail(self, client):
        """Nonexistent entity_id → fail with empty checks, not fallback to unscoped."""
        resp = client.get("/api/dcl/recon", params={"entity_id": "nonexistent_entity_xyz"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["overall"] == "fail", (
            f"Nonexistent entity should fail, got overall='{data['overall']}'"
        )
        assert data["checks"] == [], (
            "Nonexistent entity should return empty checks, not fallback to unscoped data"
        )
        assert data["entity_id"] == "nonexistent_entity_xyz"
