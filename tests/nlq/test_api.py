"""
Integration tests for NLQ API endpoints.

Tests the full request/response cycle for answerability endpoints.
"""

import pytest
from fastapi.testclient import TestClient


class TestAnswerabilityRankEndpoint:
    """Integration tests for POST /api/nlq/answerability_rank."""

    @pytest.fixture
    def client(self):
        """Create test client."""
        from backend.api.main import app
        return TestClient(app)

    def test_rank_services_revenue_question(self, client):
        """Should return ranked circles for services revenue question."""
        response = client.post(
            "/api/nlq/answerability_rank",
            json={
                "question": "Services revenue (25% of total) is down 50% QoQ — what's happening?",
                "tenant_id": "t_123",
                "context": {
                    "timeWindow": "QoQ",
                    "metricHint": "services_revenue"
                }
            }
        )

        assert response.status_code == 200
        data = response.json()

        # Should have question echoed back
        assert data["question"] == "Services revenue (25% of total) is down 50% QoQ — what's happening?"

        # Should have 2-3 circles
        assert 2 <= len(data["circles"]) <= 3

        # Circles should be ranked
        for i, circle in enumerate(data["circles"]):
            assert circle["rank"] == i + 1
            assert 0 <= circle["probabilityOfAnswer"] <= 1
            assert 0 <= circle["confidence"] <= 1
            assert circle["color"] in ["hot", "warm", "cool"]
            assert circle["id"]
            assert circle["label"]
            assert circle["planId"]

    def test_rank_without_context(self, client):
        """Should work without explicit context hints."""
        response = client.post(
            "/api/nlq/answerability_rank",
            json={
                "question": "Revenue is down QoQ",
                "tenantId": "default"
            }
        )

        assert response.status_code == 200
        data = response.json()

        assert len(data["circles"]) > 0

    def test_rank_circles_ordered_by_probability(self, client):
        """Circles should be ordered by probability descending."""
        response = client.post(
            "/api/nlq/answerability_rank",
            json={
                "question": "Services revenue dropped significantly",
                "tenantId": "default"
            }
        )

        assert response.status_code == 200
        data = response.json()

        circles = data["circles"]
        for i in range(len(circles) - 1):
            assert circles[i]["probabilityOfAnswer"] >= circles[i + 1]["probabilityOfAnswer"]

    def test_rank_includes_requirements(self, client):
        """Each circle should include requirements."""
        response = client.post(
            "/api/nlq/answerability_rank",
            json={
                "question": "Services revenue is down",
                "tenantId": "default"
            }
        )

        assert response.status_code == 200
        data = response.json()

        for circle in data["circles"]:
            assert "requires" in circle
            requires = circle["requires"]
            assert "definitions" in requires
            assert "events" in requires
            assert "dims" in requires

    def test_rank_includes_why_ranked(self, client):
        """Each circle should include ranking reasons."""
        response = client.post(
            "/api/nlq/answerability_rank",
            json={
                "question": "Services revenue is down",
                "tenantId": "default",
                "context": {"metricHint": "services_revenue"}
            }
        )

        assert response.status_code == 200
        data = response.json()

        for circle in data["circles"]:
            assert "whyRanked" in circle
            assert isinstance(circle["whyRanked"], list)


class TestExplainEndpoint:
    """Integration tests for POST /api/nlq/explain."""

    @pytest.fixture
    def client(self):
        """Create test client."""
        from backend.api.main import app
        return TestClient(app)

    def test_explain_volume_hypothesis(self, client):
        """Should return explanation for volume hypothesis."""
        response = client.post(
            "/api/nlq/explain",
            json={
                "question": "Services revenue is down 50% QoQ",
                "tenantId": "t_123",
                "hypothesisId": "h_volume",
                "planId": "plan_services_rev_bridge"
            }
        )

        assert response.status_code == 200
        data = response.json()

        assert data["headline"]
        assert len(data["why"]) > 0
        assert "goDeeper" in data
        assert len(data["proof"]) > 0
        assert len(data["next"]) > 0

    def test_explain_includes_facts_with_confidence(self, client):
        """Facts should include confidence scores."""
        response = client.post(
            "/api/nlq/explain",
            json={
                "question": "Services revenue is down",
                "tenantId": "default",
                "hypothesisId": "h_volume",
                "planId": "plan_services_rev_bridge"
            }
        )

        assert response.status_code == 200
        data = response.json()

        for fact in data["why"]:
            assert "fact" in fact
            assert "confidence" in fact
            assert 0 <= fact["confidence"] <= 1

    def test_explain_includes_bridge_analysis(self, client):
        """Should include bridge decomposition."""
        response = client.post(
            "/api/nlq/explain",
            json={
                "question": "Revenue dropped",
                "tenantId": "default",
                "hypothesisId": "h_volume",
                "planId": "plan_test"
            }
        )

        assert response.status_code == 200
        data = response.json()

        go_deeper = data["goDeeper"]
        assert "bridge" in go_deeper
        assert len(go_deeper["bridge"]) > 0

        for component in go_deeper["bridge"]:
            assert "component" in component
            assert "share" in component

    def test_explain_includes_drilldowns(self, client):
        """Should include drilldown options."""
        response = client.post(
            "/api/nlq/explain",
            json={
                "question": "Revenue issue",
                "tenantId": "default",
                "hypothesisId": "h_volume",
                "planId": "plan_test"
            }
        )

        assert response.status_code == 200
        data = response.json()

        go_deeper = data["goDeeper"]
        assert "drilldowns" in go_deeper
        assert len(go_deeper["drilldowns"]) > 0

    def test_explain_includes_proof_pointers(self, client):
        """Should include proof pointers."""
        response = client.post(
            "/api/nlq/explain",
            json={
                "question": "Services revenue down",
                "tenantId": "default",
                "hypothesisId": "h_volume",
                "planId": "plan_services_rev_bridge"
            }
        )

        assert response.status_code == 200
        data = response.json()

        proof = data["proof"]
        assert len(proof) > 0

        # Should have at least a query hash
        types = [p["type"] for p in proof]
        assert "query_hash" in types

    def test_explain_includes_next_actions(self, client):
        """Should include suggested next actions."""
        response = client.post(
            "/api/nlq/explain",
            json={
                "question": "Revenue down",
                "tenantId": "default",
                "hypothesisId": "h_volume",
                "planId": "plan_test"
            }
        )

        assert response.status_code == 200
        data = response.json()

        next_actions = data["next"]
        assert len(next_actions) > 0

        for action in next_actions:
            assert "action" in action
            assert "label" in action


class TestEndpointErrors:
    """Tests for error handling in NLQ endpoints."""

    @pytest.fixture
    def client(self):
        """Create test client."""
        from backend.api.main import app
        return TestClient(app)

    def test_rank_missing_question(self, client):
        """Should return 422 for missing question."""
        response = client.post(
            "/api/nlq/answerability_rank",
            json={
                "tenantId": "default"
            }
        )

        assert response.status_code == 422

    def test_explain_missing_hypothesis_id(self, client):
        """Should return 422 for missing hypothesis_id."""
        response = client.post(
            "/api/nlq/explain",
            json={
                "question": "Test question",
                "tenantId": "default",
                "planId": "plan_test"
            }
        )

        assert response.status_code == 422

    def test_explain_missing_plan_id(self, client):
        """Should return 422 for missing plan_id."""
        response = client.post(
            "/api/nlq/explain",
            json={
                "question": "Test question",
                "tenantId": "default",
                "hypothesisId": "h_volume"
            }
        )

        assert response.status_code == 422
