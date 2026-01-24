"""
Unit tests for NLQ Answerability Scorer.

Tests scoring logic, ranking, and color mapping.
"""

import pytest
from unittest.mock import MagicMock, patch
from backend.nlq.scorer import (
    AnswerabilityScorer,
    QuestionParser,
    HypothesisTemplate,
    METRIC_CHANGE_HYPOTHESES,
)
from backend.nlq.models import ContextHints, Definition


class TestQuestionParser:
    """Tests for QuestionParser."""

    def setup_method(self):
        self.parser = QuestionParser()

    def test_parse_time_window_qoq(self):
        """Should extract QoQ time window from question."""
        result = self.parser.parse("Revenue is down 50% QoQ")
        assert result["time_window"] == "QOQ"

    def test_parse_time_window_yoy(self):
        """Should extract YoY time window from question."""
        result = self.parser.parse("Revenue is up year-over-year")
        assert result["time_window"] == "YOY"

    def test_parse_metric_services_revenue(self):
        """Should extract services revenue metric hint."""
        result = self.parser.parse("Services revenue is declining")
        assert result["metric_hint"] == "services_revenue"

    def test_parse_metric_total_revenue(self):
        """Should extract total revenue metric hint."""
        result = self.parser.parse("Total revenue is down")
        assert result["metric_hint"] == "total_revenue"

    def test_parse_with_context_override(self):
        """Context hints should override parsed values."""
        context = ContextHints(time_window="MTD", metric_hint="arr")
        result = self.parser.parse("Revenue is down QoQ", context)
        assert result["time_window"] == "MTD"
        assert result["metric_hint"] == "arr"

    def test_parse_change_question_type(self):
        """Should detect change question type."""
        result = self.parser.parse("Revenue dropped significantly")
        assert result["question_type"] == "change"

    def test_parse_no_hints(self):
        """Should handle question with no recognizable hints."""
        result = self.parser.parse("What is the status?")
        assert result["metric_hint"] is None
        assert result["time_window"] is None


class TestAnswerabilityScorer:
    """Tests for AnswerabilityScorer."""

    def setup_method(self):
        # Create a mock persistence layer
        self.mock_persistence = MagicMock()
        self.scorer = AnswerabilityScorer(persistence=self.mock_persistence)

    def test_score_hypothesis_with_definition(self):
        """Score should be higher when definition exists."""
        definition = Definition(
            id="services_revenue",
            version=1,
            quality_score=0.85,
        )

        # Mock persistence methods
        self.mock_persistence.check_event_binding.return_value = {
            "revenue_recognized": True
        }
        self.mock_persistence.get_binding_quality.return_value = 0.9
        self.mock_persistence.check_dims_available.return_value = {
            "customer": True,
            "service_line": True,
        }
        self.mock_persistence.get_proof_availability.return_value = 0.8

        hypothesis = METRIC_CHANGE_HYPOTHESES[0]  # h_volume
        score, confidence, why_ranked = self.scorer.score_hypothesis(
            hypothesis, definition
        )

        # Score should reflect definition exists + events bound + dims available + proof
        assert score > 0.7
        assert confidence > 0.5
        assert "definition services_revenue exists" in why_ranked

    def test_score_hypothesis_without_definition(self):
        """Score should be lower when definition doesn't exist."""
        self.mock_persistence.check_event_binding.return_value = {
            "revenue_recognized": True
        }
        self.mock_persistence.get_binding_quality.return_value = 0.9
        self.mock_persistence.check_dims_available.return_value = {
            "customer": True,
            "service_line": True,
        }
        self.mock_persistence.get_proof_availability.return_value = 0.0

        hypothesis = METRIC_CHANGE_HYPOTHESES[0]  # h_volume
        score, confidence, why_ranked = self.scorer.score_hypothesis(
            hypothesis, None
        )

        # Score should be lower without definition (50% weight)
        assert score < 0.5
        assert "no matching definition found" in why_ranked

    def test_score_hypothesis_missing_events(self):
        """Score should drop when required events not bound."""
        definition = Definition(
            id="services_revenue",
            version=1,
            quality_score=0.85,
        )

        # Events not bound
        self.mock_persistence.check_event_binding.return_value = {
            "revenue_recognized": False,
            "invoice_posted": False,
        }
        self.mock_persistence.get_binding_quality.return_value = 0.0
        self.mock_persistence.check_dims_available.return_value = {
            "service_line": False,
        }
        self.mock_persistence.get_proof_availability.return_value = 0.0

        hypothesis = METRIC_CHANGE_HYPOTHESES[1]  # h_timing
        score, confidence, why_ranked = self.scorer.score_hypothesis(
            hypothesis, definition
        )

        # Score should only have definition component
        assert score <= 0.5
        # No "events bound" reason since none are bound
        assert not any("events" in r and "bound" in r for r in why_ranked)

    def test_score_hypothesis_missing_dims(self):
        """Score should drop when required dims not available."""
        definition = Definition(
            id="services_revenue",
            version=1,
            quality_score=0.85,
        )

        self.mock_persistence.check_event_binding.return_value = {
            "revenue_recognized": True
        }
        self.mock_persistence.get_binding_quality.return_value = 0.9
        # Dims not available
        self.mock_persistence.check_dims_available.return_value = {
            "customer": False,
            "service_line": False,
        }
        self.mock_persistence.get_proof_availability.return_value = 0.8

        hypothesis = METRIC_CHANGE_HYPOTHESES[0]  # h_volume
        score, confidence, why_ranked = self.scorer.score_hypothesis(
            hypothesis, definition
        )

        # Score should be lower without dims (15% weight)
        # But definition (50%) + events (25%) + proof (10%) should still contribute
        assert score > 0.5
        # No "dims available" reason since none are available
        assert not any("dims" in r and "available" in r for r in why_ranked)

    def test_get_color_hot(self):
        """Hot color when score >= 0.70 and confidence >= 0.70."""
        color = self.scorer.get_color(0.75, 0.75)
        assert color == "hot"

    def test_get_color_warm(self):
        """Warm color when score >= 0.40 but not hot."""
        color = self.scorer.get_color(0.55, 0.50)
        assert color == "warm"

    def test_get_color_cool(self):
        """Cool color when score < 0.40."""
        color = self.scorer.get_color(0.30, 0.80)
        assert color == "cool"

    def test_get_color_edge_hot(self):
        """Edge case: exactly at hot threshold."""
        color = self.scorer.get_color(0.70, 0.70)
        assert color == "hot"

    def test_get_color_edge_warm(self):
        """Edge case: exactly at warm threshold."""
        color = self.scorer.get_color(0.40, 0.30)
        assert color == "warm"

    def test_rank_hypotheses_order(self):
        """Hypotheses should be ranked by probability descending."""
        self.mock_persistence.resolve_definition.return_value = Definition(
            id="services_revenue",
            version=1,
            quality_score=0.85,
        )
        self.mock_persistence.check_event_binding.return_value = {
            "revenue_recognized": True,
            "invoice_posted": True,
            "mapping_changed": False,
        }
        self.mock_persistence.get_binding_quality.return_value = 0.8
        self.mock_persistence.check_dims_available.return_value = {
            "customer": True,
            "service_line": True,
        }
        self.mock_persistence.get_proof_availability.return_value = 0.7

        circles = self.scorer.rank_hypotheses(
            "Services revenue is down 50% QoQ"
        )

        # Should have 3 circles
        assert len(circles) == 3

        # Should be ranked 1, 2, 3
        assert circles[0].rank == 1
        assert circles[1].rank == 2
        assert circles[2].rank == 3

        # Probabilities should be descending
        assert circles[0].probability_of_answer >= circles[1].probability_of_answer
        assert circles[1].probability_of_answer >= circles[2].probability_of_answer

    def test_needs_context_low_probability(self):
        """Should return clarifiers when top probability is low."""
        circles = self.scorer.rank_hypotheses.__self__

        # Create mock circles with low probability
        from backend.nlq.models import Circle, CircleRequirements
        low_prob_circles = [
            Circle(
                id="h_volume",
                rank=1,
                label="Test",
                probability_of_answer=0.30,
                confidence=0.20,
                color="cool",
                why_ranked=["no matching definition found"],
                requires=CircleRequirements(definitions=[], events=["revenue_recognized"], dims=[]),
                plan_id="plan_test",
            )
        ]

        clarifiers = self.scorer.get_needs_context(low_prob_circles)
        assert len(clarifiers) > 0
        assert len(clarifiers) <= 2

    def test_needs_context_high_probability(self):
        """Should return no clarifiers when top probability is high."""
        from backend.nlq.models import Circle, CircleRequirements
        high_prob_circles = [
            Circle(
                id="h_volume",
                rank=1,
                label="Test",
                probability_of_answer=0.75,
                confidence=0.80,
                color="hot",
                why_ranked=["definition services_revenue exists"],
                requires=CircleRequirements(
                    definitions=["services_revenue"],
                    events=["revenue_recognized"],
                    dims=["customer"],
                ),
                plan_id="plan_test",
            )
        ]

        clarifiers = self.scorer.get_needs_context(high_prob_circles)
        assert len(clarifiers) == 0


class TestHypothesisTemplate:
    """Tests for HypothesisTemplate dataclass."""

    def test_hypothesis_templates_exist(self):
        """Should have predefined hypothesis templates."""
        assert len(METRIC_CHANGE_HYPOTHESES) == 3

    def test_hypothesis_ids_unique(self):
        """Hypothesis IDs should be unique."""
        ids = [h.id for h in METRIC_CHANGE_HYPOTHESES]
        assert len(ids) == len(set(ids))

    def test_hypothesis_has_required_fields(self):
        """Each hypothesis should have all required fields."""
        for h in METRIC_CHANGE_HYPOTHESES:
            assert h.id
            assert h.label_template
            assert h.required_events
            assert h.plan_id
            assert 0 <= h.base_probability <= 1
