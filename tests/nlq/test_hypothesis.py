"""
Unit tests for Dynamic Hypothesis Generator.

Tests hypothesis generation based on definition structure.
"""

import pytest
from backend.nlq.hypothesis import (
    HypothesisGenerator,
    HypothesisTemplate,
    BASE_HYPOTHESES,
    METRIC_CHANGE_HYPOTHESES,
)
from backend.nlq.persistence import NLQPersistence


class TestHypothesisGenerator:
    """Tests for HypothesisGenerator."""

    def setup_method(self):
        """Set up generator with default fixtures."""
        self.persistence = NLQPersistence()
        self.generator = HypothesisGenerator(persistence=self.persistence)

    def test_generate_for_services_revenue(self):
        """Should generate hypotheses for services_revenue definition."""
        hypotheses = self.generator.generate(
            definition_id="services_revenue",
            question_type="change",
            metric_name="Services Revenue",
        )

        assert len(hypotheses) > 0
        # Should include volume hypothesis
        ids = [h.id for h in hypotheses]
        assert "h_volume" in ids

    def test_generate_includes_rate_for_sum_measure(self):
        """Should include rate hypothesis for sum aggregation."""
        hypotheses = self.generator.generate(
            definition_id="services_revenue",
            question_type="change",
            metric_name="Services Revenue",
        )

        ids = [h.id for h in hypotheses]
        assert "h_rate" in ids

    def test_generate_includes_mix_for_dims(self):
        """Should include mix hypothesis for each allowed dimension."""
        hypotheses = self.generator.generate(
            definition_id="services_revenue",
            question_type="change",
            metric_name="Services Revenue",
        )

        # Should have mix hypotheses for top 2 dims
        ids = [h.id for h in hypotheses]
        mix_ids = [id for id in ids if id.startswith("h_mix_")]
        assert len(mix_ids) > 0

    def test_generate_includes_timing(self):
        """Should include timing hypothesis when time_field exists."""
        hypotheses = self.generator.generate(
            definition_id="services_revenue",
            question_type="change",
            metric_name="Services Revenue",
        )

        ids = [h.id for h in hypotheses]
        assert "h_timing" in ids

    def test_generate_trend_hypotheses(self):
        """Should add trend hypotheses for trend questions."""
        hypotheses = self.generator.generate(
            definition_id="services_revenue",
            question_type="trend",
            metric_name="Services Revenue",
        )

        ids = [h.id for h in hypotheses]
        assert "h_seasonal" in ids

    def test_generate_anomaly_hypotheses(self):
        """Should add anomaly hypotheses for anomaly questions."""
        hypotheses = self.generator.generate(
            definition_id="services_revenue",
            question_type="anomaly",
            metric_name="Services Revenue",
        )

        ids = [h.id for h in hypotheses]
        assert "h_outlier" in ids

    def test_generate_fallback_for_missing_definition(self):
        """Should return generic hypotheses for missing definition."""
        hypotheses = self.generator.generate(
            definition_id="nonexistent_definition",
            question_type="change",
            metric_name="Unknown Metric",
        )

        assert len(hypotheses) > 0
        # Should use generic hypotheses
        assert hypotheses[0].required_events == ["revenue_recognized"]

    def test_hypothesis_has_plan_id(self):
        """Each hypothesis should have a plan_id."""
        hypotheses = self.generator.generate(
            definition_id="services_revenue",
            question_type="change",
            metric_name="Services Revenue",
        )

        for h in hypotheses:
            assert h.plan_id
            assert "services_revenue" in h.plan_id.lower().replace(" ", "_")

    def test_hypothesis_has_required_events(self):
        """Each hypothesis should specify required events."""
        hypotheses = self.generator.generate(
            definition_id="services_revenue",
            question_type="change",
            metric_name="Services Revenue",
        )

        for h in hypotheses:
            assert isinstance(h.required_events, list)


class TestBaseHypotheses:
    """Tests for base hypothesis templates."""

    def test_change_hypotheses_exist(self):
        """Should have change hypotheses."""
        assert "change" in BASE_HYPOTHESES
        assert len(BASE_HYPOTHESES["change"]) > 0

    def test_trend_hypotheses_exist(self):
        """Should have trend hypotheses."""
        assert "trend" in BASE_HYPOTHESES
        assert len(BASE_HYPOTHESES["trend"]) > 0

    def test_anomaly_hypotheses_exist(self):
        """Should have anomaly hypotheses."""
        assert "anomaly" in BASE_HYPOTHESES
        assert len(BASE_HYPOTHESES["anomaly"]) > 0


class TestMetricChangeHypotheses:
    """Tests for legacy METRIC_CHANGE_HYPOTHESES."""

    def test_legacy_templates_exist(self):
        """Should have legacy templates for backwards compatibility."""
        assert len(METRIC_CHANGE_HYPOTHESES) == 3

    def test_legacy_templates_have_required_fields(self):
        """Legacy templates should have all required fields."""
        for h in METRIC_CHANGE_HYPOTHESES:
            assert h.id
            assert h.label_template
            assert h.required_events
            assert h.plan_id
            assert 0 <= h.base_probability <= 1
