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
from backend.nlq.models import ContextHints, Definition, ValidationResult, WeakBinding
from backend.nlq.persistence import NLQPersistence


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
    """Tests for AnswerabilityScorer with real fixtures."""

    def setup_method(self):
        """Set up scorer with real persistence."""
        self.persistence = NLQPersistence()
        self.scorer = AnswerabilityScorer(persistence=self.persistence)

    def test_score_hypothesis_with_definition(self):
        """Score should use validator and return proper values."""
        definition = self.persistence.get_definition("services_revenue")
        hypothesis = METRIC_CHANGE_HYPOTHESES[0]  # h_volume

        prob, confidence, why_ranked, validation = self.scorer.score_hypothesis(
            hypothesis, definition
        )

        # Should have scores from validator
        assert 0 <= prob <= 1
        assert 0 <= confidence <= 1
        assert "definition services_revenue exists" in why_ranked
        assert validation is not None

    def test_score_hypothesis_without_definition(self):
        """Score should be zero when definition doesn't exist."""
        hypothesis = METRIC_CHANGE_HYPOTHESES[0]  # h_volume

        prob, confidence, why_ranked, validation = self.scorer.score_hypothesis(
            hypothesis, None
        )

        # Score should be zero without definition
        assert prob == 0.0
        assert confidence == 0.0
        assert "no matching definition found" in why_ranked

    def test_score_uses_new_formula(self):
        """Score should use the spec formula: 0.55*coverage + 0.25*freshness + 0.20*proof."""
        definition = self.persistence.get_definition("services_revenue")
        hypothesis = METRIC_CHANGE_HYPOTHESES[0]  # h_volume

        prob, confidence, why_ranked, validation = self.scorer.score_hypothesis(
            hypothesis, definition
        )

        # Verify formula is applied (approximately)
        expected_prob = (
            0.55 * validation.coverage_score +
            0.25 * validation.freshness_score +
            0.20 * validation.proof_score
        )
        assert abs(prob - expected_prob) < 0.01

    def test_get_color_hot(self):
        """Hot color when prob >= 0.70 and confidence >= 0.70."""
        color = self.scorer.get_color(0.75, 0.75)
        assert color == "hot"

    def test_get_color_warm(self):
        """Warm color when prob >= 0.40 but not hot."""
        color = self.scorer.get_color(0.55, 0.50)
        assert color == "warm"

    def test_get_color_cool(self):
        """Cool color when prob < 0.40."""
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

    def test_rank_hypotheses_with_context(self):
        """Should use context hints for ranking."""
        context = ContextHints(metric_hint="services_revenue", time_window="QoQ")
        circles = self.scorer.rank_hypotheses(
            "What's happening?",
            context=context,
        )

        # Should have circles with the right definition
        assert len(circles) > 0
        assert "services_revenue" in circles[0].requires.definitions

    def test_needs_context_low_probability(self):
        """Should return clarifiers when top probability is low."""
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


class TestAnswerabilityFormula:
    """Tests for the scoring formula from the spec."""

    def setup_method(self):
        self.persistence = NLQPersistence()
        self.scorer = AnswerabilityScorer(persistence=self.persistence)

    def test_probability_weights_sum_to_one(self):
        """Probability weights should sum to 1.0."""
        total = (
            self.scorer.WEIGHT_COVERAGE +
            self.scorer.WEIGHT_FRESHNESS +
            self.scorer.WEIGHT_PROOF
        )
        assert abs(total - 1.0) < 0.01

    def test_confidence_weights_sum_to_one(self):
        """Confidence weights should sum to 1.0."""
        total = (
            self.scorer.CONF_COVERAGE +
            self.scorer.CONF_PROOF
        )
        assert abs(total - 1.0) < 0.01

    def test_probability_formula_matches_spec(self):
        """Formula should be: 0.55*coverage + 0.25*freshness + 0.20*proof."""
        assert self.scorer.WEIGHT_COVERAGE == 0.55
        assert self.scorer.WEIGHT_FRESHNESS == 0.25
        assert self.scorer.WEIGHT_PROOF == 0.20

    def test_confidence_formula_matches_spec(self):
        """Formula should be: 0.70*coverage + 0.30*proof."""
        assert self.scorer.CONF_COVERAGE == 0.70
        assert self.scorer.CONF_PROOF == 0.30


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

    def test_volume_hypothesis_config(self):
        """Volume hypothesis should require revenue_recognized event."""
        volume_h = METRIC_CHANGE_HYPOTHESES[0]
        assert volume_h.id == "h_volume"
        assert "revenue_recognized" in volume_h.required_events

    def test_timing_hypothesis_config(self):
        """Timing hypothesis should require both revenue and invoice events."""
        timing_h = METRIC_CHANGE_HYPOTHESES[1]
        assert timing_h.id == "h_timing"
        assert "revenue_recognized" in timing_h.required_events
        assert "invoice_posted" in timing_h.required_events

    def test_reclass_hypothesis_config(self):
        """Reclass hypothesis should require mapping_changed event."""
        reclass_h = METRIC_CHANGE_HYPOTHESES[2]
        assert reclass_h.id == "h_reclass"
        assert "mapping_changed" in reclass_h.required_events
