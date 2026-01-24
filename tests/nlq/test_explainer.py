"""
Unit tests for NLQ Hypothesis Explainer.

Tests explanation generation and proof pointers.
"""

import pytest
from backend.nlq.explainer import HypothesisExplainer, HYPOTHESIS_EXPLANATIONS
from backend.nlq.models import ExplainRequest


class TestHypothesisExplainer:
    """Tests for HypothesisExplainer."""

    def setup_method(self):
        """Set up explainer with default persistence."""
        self.explainer = HypothesisExplainer()

    def test_explain_volume_hypothesis(self):
        """Should generate explanation for volume hypothesis."""
        request = ExplainRequest(
            question="Services revenue is down 50% QoQ",
            tenant_id="default",
            hypothesis_id="h_volume",
            plan_id="plan_services_rev_bridge",
        )

        response = self.explainer.explain(request)

        assert response.headline
        assert "Services revenue" in response.headline
        assert len(response.why) > 0
        assert all(0 <= f.confidence <= 1 for f in response.why)

    def test_explain_timing_hypothesis(self):
        """Should generate explanation for timing hypothesis."""
        request = ExplainRequest(
            question="Revenue timing shifted",
            tenant_id="default",
            hypothesis_id="h_timing",
            plan_id="plan_timing_slip_check",
        )

        response = self.explainer.explain(request)

        assert response.headline
        assert "timing" in response.headline.lower()
        assert len(response.why) > 0

    def test_explain_reclass_hypothesis(self):
        """Should generate explanation for reclass hypothesis."""
        request = ExplainRequest(
            question="Revenue classification changed",
            tenant_id="default",
            hypothesis_id="h_reclass",
            plan_id="plan_mapping_drift",
        )

        response = self.explainer.explain(request)

        assert response.headline
        assert "classification" in response.headline.lower() or "reclass" in response.headline.lower()

    def test_explain_includes_bridge(self):
        """Explanation should include bridge analysis."""
        request = ExplainRequest(
            question="Services revenue is down",
            tenant_id="default",
            hypothesis_id="h_volume",
            plan_id="plan_services_rev_bridge",
        )

        response = self.explainer.explain(request)

        assert response.go_deeper is not None
        assert len(response.go_deeper.bridge) > 0

        # Bridge shares should sum to ~1.0
        total_share = sum(b.share for b in response.go_deeper.bridge)
        assert 0.95 <= total_share <= 1.05

    def test_explain_includes_drilldowns(self):
        """Explanation should include drilldown options."""
        request = ExplainRequest(
            question="Services revenue is down",
            tenant_id="default",
            hypothesis_id="h_volume",
            plan_id="plan_services_rev_bridge",
        )

        response = self.explainer.explain(request)

        assert response.go_deeper is not None
        assert len(response.go_deeper.drilldowns) > 0

    def test_explain_includes_proof_pointers(self):
        """Explanation should include proof pointers."""
        request = ExplainRequest(
            question="Services revenue is down",
            tenant_id="default",
            hypothesis_id="h_volume",
            plan_id="plan_services_rev_bridge",
        )

        response = self.explainer.explain(request)

        assert len(response.proof) > 0

        # Should have query hash
        query_hashes = [p for p in response.proof if p.type == "query_hash"]
        assert len(query_hashes) > 0
        assert query_hashes[0].value.startswith("sha256:")

    def test_explain_includes_source_pointers(self):
        """Explanation should include source system pointers from proof hooks."""
        request = ExplainRequest(
            question="Services revenue is down",
            tenant_id="default",
            hypothesis_id="h_volume",
            plan_id="plan_services_rev_bridge",
        )

        response = self.explainer.explain(request)

        source_pointers = [p for p in response.proof if p.type == "source_pointer"]
        assert len(source_pointers) > 0
        assert source_pointers[0].system is not None

    def test_explain_includes_next_actions(self):
        """Explanation should include next action suggestions."""
        request = ExplainRequest(
            question="Services revenue is down",
            tenant_id="default",
            hypothesis_id="h_volume",
            plan_id="plan_services_rev_bridge",
        )

        response = self.explainer.explain(request)

        assert len(response.next) > 0

        actions = [n.action for n in response.next]
        assert "open_sources" in actions
        assert "deeper" in actions

    def test_explain_unknown_hypothesis(self):
        """Should handle unknown hypothesis gracefully."""
        request = ExplainRequest(
            question="Something happened",
            tenant_id="default",
            hypothesis_id="h_unknown",
            plan_id="plan_unknown",
        )

        response = self.explainer.explain(request)

        assert "Unable to generate" in response.headline
        assert len(response.why) > 0


class TestExplanationTemplates:
    """Tests for explanation templates."""

    def test_templates_exist_for_hypotheses(self):
        """Should have templates for all standard hypotheses."""
        expected_ids = ["h_volume", "h_timing", "h_reclass"]

        for h_id in expected_ids:
            assert h_id in HYPOTHESIS_EXPLANATIONS
            assert "headline_template" in HYPOTHESIS_EXPLANATIONS[h_id]
            assert "facts" in HYPOTHESIS_EXPLANATIONS[h_id]
            assert "bridge" in HYPOTHESIS_EXPLANATIONS[h_id]

    def test_template_facts_have_confidence(self):
        """Template facts should have confidence scores."""
        for h_id, template in HYPOTHESIS_EXPLANATIONS.items():
            for fact in template["facts"]:
                assert "fact" in fact
                assert "confidence" in fact
                assert 0 <= fact["confidence"] <= 1

    def test_template_bridge_shares_valid(self):
        """Template bridge component shares should sum to ~1.0."""
        for h_id, template in HYPOTHESIS_EXPLANATIONS.items():
            total_share = sum(b["share"] for b in template["bridge"])
            assert 0.95 <= total_share <= 1.05, f"Bridge shares for {h_id} sum to {total_share}"
