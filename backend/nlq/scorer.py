"""
Answerability Scorer for NLQ Circles.

Implements deterministic scoring for hypothesis ranking using the DefinitionValidator.

Scoring formula (from spec):
- probability_of_answer = 0.55*coverage_score + 0.25*freshness_score + 0.20*proof_score
- confidence = 0.70*coverage_score + 0.30*proof_score

Color mapping:
- hot: prob >= 0.70 AND confidence >= 0.70
- warm: prob >= 0.40
- cool: otherwise
"""

import re
from typing import List, Dict, Optional, Tuple, Any
from dataclasses import dataclass
from backend.utils.log_utils import get_logger
from backend.nlq.models import (
    Circle,
    CircleRequirements,
    ContextHints,
    Definition,
    ValidationResult,
)
from backend.nlq.persistence import NLQPersistence
from backend.nlq.validator import DefinitionValidator

logger = get_logger(__name__)


# =============================================================================
# Hypothesis Templates
# =============================================================================

@dataclass
class HypothesisTemplate:
    """
    Template for a hypothesis type.

    Each hypothesis has required events, dimensions, and a plan_id.
    """
    id: str
    label_template: str  # Use {metric} as placeholder
    required_events: List[str]
    required_dims: List[str]
    plan_id: str
    base_probability: float = 0.5  # Base probability before adjustments


# Standard hypotheses for "metric down/up" questions
METRIC_CHANGE_HYPOTHESES = [
    HypothesisTemplate(
        id="h_volume",
        label_template="Volume drop (fewer recognized {metric} events)",
        required_events=["revenue_recognized"],
        required_dims=["customer", "service_line"],
        plan_id="plan_{metric}_bridge",
        base_probability=0.6,
    ),
    HypothesisTemplate(
        id="h_timing",
        label_template="Timing slip (recognition shifted across quarter boundary)",
        required_events=["revenue_recognized", "invoice_posted"],
        required_dims=["service_line"],
        plan_id="plan_timing_slip_check",
        base_probability=0.5,
    ),
    HypothesisTemplate(
        id="h_reclass",
        label_template="Reclass / mapping drift ({metric} tagged differently)",
        required_events=["mapping_changed"],
        required_dims=["service_line"],
        plan_id="plan_mapping_drift",
        base_probability=0.4,
    ),
]


# =============================================================================
# Question Parser (Minimal MVP)
# =============================================================================

class QuestionParser:
    """
    Minimal question parser for MVP.

    Extracts:
    - metric hint (from context or keyword matching)
    - time window (QoQ, YoY, etc.)
    - question type (change, trend, comparison, etc.)
    """

    TIME_WINDOW_PATTERNS = {
        "qoq": r"\bqoq\b|quarter[- ]over[- ]quarter",
        "yoy": r"\byoy\b|year[- ]over[- ]year",
        "mom": r"\bmom\b|month[- ]over[- ]month",
        "mtd": r"\bmtd\b|month[- ]to[- ]date",
        "qtd": r"\bqtd\b|quarter[- ]to[- ]date",
        "ytd": r"\bytd\b|year[- ]to[- ]date",
    }

    METRIC_KEYWORDS = {
        "services_revenue": ["services revenue", "services", "professional services"],
        "total_revenue": ["total revenue", "revenue", "sales"],
        "subscription_revenue": ["subscription", "recurring", "saas", "arr"],
        "arr": ["arr", "annual recurring", "annualized"],
        "dso": ["dso", "days sales outstanding", "collection"],
    }

    CHANGE_KEYWORDS = [
        "down", "up", "dropped", "increased", "declined", "grew",
        "falling", "rising", "decreased", "change", "changed"
    ]

    def parse(
        self, question: str, context: Optional[ContextHints] = None
    ) -> Dict[str, Any]:
        """
        Parse a question and extract relevant hints.

        Returns:
            Dict with keys: metric_hint, time_window, question_type, keywords
        """
        result = {
            "metric_hint": None,
            "time_window": None,
            "question_type": "unknown",
            "keywords": [],
        }

        question_lower = question.lower()

        # Use context hints if provided
        if context:
            if context.metric_hint:
                result["metric_hint"] = context.metric_hint
            if context.time_window:
                result["time_window"] = context.time_window

        # Extract time window from question if not in context
        if not result["time_window"]:
            for window, pattern in self.TIME_WINDOW_PATTERNS.items():
                if re.search(pattern, question_lower, re.IGNORECASE):
                    result["time_window"] = window.upper()
                    break

        # Extract metric hint from question if not in context
        if not result["metric_hint"]:
            for metric, keywords in self.METRIC_KEYWORDS.items():
                for kw in keywords:
                    if kw in question_lower:
                        result["metric_hint"] = metric
                        break
                if result["metric_hint"]:
                    break

        # Detect question type
        for kw in self.CHANGE_KEYWORDS:
            if kw in question_lower:
                result["question_type"] = "change"
                break

        # Extract keywords
        result["keywords"] = [
            word for word in question_lower.split()
            if len(word) > 3 and word.isalpha()
        ]

        return result


# =============================================================================
# Answerability Scorer
# =============================================================================

class AnswerabilityScorer:
    """
    Calculates answerability scores for hypotheses using DefinitionValidator.

    Scoring formula (from spec):
    - probability_of_answer = 0.55*coverage_score + 0.25*freshness_score + 0.20*proof_score
    - confidence = 0.70*coverage_score + 0.30*proof_score

    Color mapping:
    - hot: prob >= 0.70 AND confidence >= 0.70
    - warm: prob >= 0.40
    - cool: otherwise
    """

    # Probability weights (from spec)
    WEIGHT_COVERAGE = 0.55
    WEIGHT_FRESHNESS = 0.25
    WEIGHT_PROOF = 0.20

    # Confidence weights (from spec)
    CONF_COVERAGE = 0.70
    CONF_PROOF = 0.30

    # Color thresholds
    HOT_SCORE_THRESHOLD = 0.70
    HOT_CONFIDENCE_THRESHOLD = 0.70
    WARM_SCORE_THRESHOLD = 0.40

    def __init__(self, persistence: Optional[NLQPersistence] = None):
        """
        Initialize the scorer.

        Args:
            persistence: NLQPersistence instance. Creates default if not provided.
        """
        self.persistence = persistence or NLQPersistence()
        self.validator = DefinitionValidator(persistence=self.persistence)
        self.parser = QuestionParser()

    def score_hypothesis(
        self,
        hypothesis: HypothesisTemplate,
        definition: Optional[Definition],
        tenant_id: str = "default",
    ) -> Tuple[float, float, List[str], ValidationResult]:
        """
        Score a single hypothesis using the DefinitionValidator.

        Args:
            hypothesis: The hypothesis template to score
            definition: The matched definition (if any)
            tenant_id: Tenant ID

        Returns:
            Tuple of (probability, confidence, why_ranked_reasons, validation_result)
        """
        why_ranked = []

        # If no definition, return low scores
        if not definition:
            why_ranked.append("no matching definition found")
            return 0.0, 0.0, why_ranked, ValidationResult(ok=False)

        why_ranked.append(f"definition {definition.id} exists")

        # Use the validator to get detailed validation result
        validation = self.validator.validate(
            definition_id=definition.id,
            version="v1",
            requested_dims=hypothesis.required_dims,
            tenant_id=tenant_id,
        )

        # Build why_ranked from validation result
        if validation.missing_events:
            why_ranked.append(f"missing events: {', '.join(validation.missing_events)}")
        else:
            # Get bound events
            event_bindings = self.persistence.check_event_binding(
                hypothesis.required_events, tenant_id
            )
            bound_names = [e for e, b in event_bindings.items() if b]
            if bound_names:
                why_ranked.append(f"events {', '.join(bound_names)} bound")

        if validation.missing_dims:
            why_ranked.append(f"missing dims: {', '.join(validation.missing_dims)}")
        else:
            # Get available dims
            dims_available = self.persistence.check_dims_available(
                hypothesis.required_dims, hypothesis.required_events, tenant_id
            )
            avail_names = [d for d, a in dims_available.items() if a]
            if avail_names:
                why_ranked.append(f"dims {', '.join(avail_names)} available")

        if validation.weak_bindings:
            weak_systems = [wb.source_system for wb in validation.weak_bindings[:2]]
            why_ranked.append(f"weak bindings: {', '.join(weak_systems)}")

        if validation.proof_score > 0:
            why_ranked.append(f"proof hooks available (score: {validation.proof_score:.2f})")

        # Calculate probability using spec formula
        probability = (
            self.WEIGHT_COVERAGE * validation.coverage_score +
            self.WEIGHT_FRESHNESS * validation.freshness_score +
            self.WEIGHT_PROOF * validation.proof_score
        )

        # Calculate confidence using spec formula
        confidence = (
            self.CONF_COVERAGE * validation.coverage_score +
            self.CONF_PROOF * validation.proof_score
        )

        # Clamp to [0, 1]
        probability = max(0.0, min(1.0, probability))
        confidence = max(0.0, min(1.0, confidence))

        return probability, confidence, why_ranked, validation

    def get_color(self, score: float, confidence: float) -> str:
        """
        Determine color based on score and confidence.

        Returns:
            "hot", "warm", or "cool"
        """
        if score >= self.HOT_SCORE_THRESHOLD and confidence >= self.HOT_CONFIDENCE_THRESHOLD:
            return "hot"
        elif score >= self.WARM_SCORE_THRESHOLD:
            return "warm"
        else:
            return "cool"

    def rank_hypotheses(
        self,
        question: str,
        tenant_id: str = "default",
        context: Optional[ContextHints] = None,
    ) -> List[Circle]:
        """
        Rank hypotheses for a question and return circles.

        Args:
            question: The user's question
            tenant_id: Tenant ID
            context: Optional context hints

        Returns:
            List of Circle objects, ranked by probability_of_answer
        """
        # Parse the question
        parsed = self.parser.parse(question, context)
        logger.info(f"Parsed question: {parsed}")

        # Resolve definition
        definition = self.persistence.resolve_definition(
            metric_hint=parsed["metric_hint"],
            keywords=parsed.get("keywords"),
            tenant_id=tenant_id,
        )

        if definition:
            logger.info(f"Resolved definition: {definition.id}")
        else:
            logger.warning("No definition resolved for question")

        # Determine hypothesis set based on question type
        if parsed["question_type"] == "change":
            hypotheses = METRIC_CHANGE_HYPOTHESES
        else:
            # Default to change hypotheses for MVP
            hypotheses = METRIC_CHANGE_HYPOTHESES

        # Score each hypothesis
        scored_hypotheses = []
        metric_name = parsed.get("metric_hint", "metric").replace("_", " ")

        for h in hypotheses:
            probability, confidence, why_ranked, validation = self.score_hypothesis(
                h, definition, tenant_id
            )

            # Apply base probability as a weight factor
            # Higher base_probability hypotheses get a slight boost
            weighted_probability = probability * (0.7 + 0.3 * h.base_probability)
            weighted_probability = max(0.0, min(1.0, weighted_probability))

            # Format label
            label = h.label_template.format(metric=metric_name)

            # Format plan_id
            plan_id = h.plan_id.format(
                metric=parsed.get("metric_hint", "unknown").replace("_", "_")
            )

            scored_hypotheses.append({
                "id": h.id,
                "label": label,
                "probability": weighted_probability,
                "confidence": confidence,
                "why_ranked": why_ranked,
                "required_events": h.required_events,
                "required_dims": h.required_dims,
                "plan_id": plan_id,
                "validation": validation,
            })

        # Sort by probability descending
        scored_hypotheses.sort(key=lambda x: x["probability"], reverse=True)

        # Build circles
        circles = []
        for rank, h in enumerate(scored_hypotheses, 1):
            color = self.get_color(h["probability"], h["confidence"])

            circle = Circle(
                id=h["id"],
                rank=rank,
                label=h["label"],
                probability_of_answer=round(h["probability"], 2),
                confidence=round(h["confidence"], 2),
                color=color,
                why_ranked=h["why_ranked"],
                requires=CircleRequirements(
                    definitions=[definition.id] if definition else [],
                    events=h["required_events"],
                    dims=h["required_dims"],
                ),
                plan_id=h["plan_id"],
            )
            circles.append(circle)

        return circles

    def get_needs_context(
        self,
        circles: List[Circle],
        threshold: float = 0.40,
    ) -> List[str]:
        """
        Generate clarifying questions if top hypothesis probability is too low.

        Args:
            circles: Ranked circles
            threshold: Probability threshold below which to request clarification

        Returns:
            List of clarifying questions
        """
        if not circles:
            return ["Could you specify which metric you're asking about?"]

        top_circle = circles[0]
        clarifiers = []

        if top_circle.probability_of_answer < threshold:
            # Check what's missing
            if not top_circle.requires.definitions:
                clarifiers.append(
                    "Which specific metric are you analyzing? (e.g., services revenue, ARR, total revenue)"
                )

            # Check for missing events
            for event in top_circle.requires.events:
                if f"events {event} bound" not in " ".join(top_circle.why_ranked):
                    clarifiers.append(
                        f"We may need access to '{event}' events. Can you confirm which source system tracks this?"
                    )

            if not clarifiers:
                clarifiers.append(
                    "Could you provide more context about the time period or dimensions you want to analyze?"
                )

        return clarifiers[:2]  # Return max 2 clarifiers
