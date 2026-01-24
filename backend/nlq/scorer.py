"""
Answerability Scorer for NLQ Circles.

Implements deterministic scoring for hypothesis ranking:
- score = 0.50*(definition_exists) + 0.25*(required_events_bound) + 0.15*(required_dims_available) + 0.10*(proof_hook_available)
- confidence = 0.60*(binding_quality) + 0.40*(definition_quality)
- color mapping: hot (score>=0.70, confidence>=0.70), warm (score>=0.40), cool (otherwise)
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
)
from backend.nlq.persistence import NLQPersistence

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
    Calculates answerability scores for hypotheses.

    Scoring formula:
    - score = 0.50*(definition_exists) + 0.25*(required_events_bound) + 0.15*(required_dims_available) + 0.10*(proof_hook_available)
    - confidence = 0.60*(binding_quality) + 0.40*(definition_quality)

    Color mapping:
    - hot: score >= 0.70 AND confidence >= 0.70
    - warm: score >= 0.40
    - cool: otherwise
    """

    # Scoring weights
    WEIGHT_DEFINITION = 0.50
    WEIGHT_EVENTS = 0.25
    WEIGHT_DIMS = 0.15
    WEIGHT_PROOF = 0.10

    # Confidence weights
    CONF_BINDING = 0.60
    CONF_DEFINITION = 0.40

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
        self.parser = QuestionParser()

    def score_hypothesis(
        self,
        hypothesis: HypothesisTemplate,
        definition: Optional[Definition],
        tenant_id: str = "default",
    ) -> Tuple[float, float, List[str]]:
        """
        Score a single hypothesis.

        Args:
            hypothesis: The hypothesis template to score
            definition: The matched definition (if any)
            tenant_id: Tenant ID

        Returns:
            Tuple of (score, confidence, why_ranked_reasons)
        """
        why_ranked = []

        # Component scores
        definition_score = 0.0
        events_score = 0.0
        dims_score = 0.0
        proof_score = 0.0
        binding_quality = 0.0
        definition_quality = 0.0

        # Definition exists
        if definition:
            definition_score = 1.0
            definition_quality = definition.quality_score
            why_ranked.append(f"definition {definition.id} exists")
        else:
            why_ranked.append("no matching definition found")

        # Check required events
        event_bindings = self.persistence.check_event_binding(
            hypothesis.required_events, tenant_id
        )
        bound_events = sum(1 for bound in event_bindings.values() if bound)
        total_events = len(hypothesis.required_events)
        if total_events > 0:
            events_score = bound_events / total_events

        if bound_events > 0:
            bound_names = [e for e, b in event_bindings.items() if b]
            why_ranked.append(f"events {', '.join(bound_names)} bound")

            # Calculate binding quality from bound events
            qualities = []
            for event_id in bound_names:
                q = self.persistence.get_binding_quality(event_id, tenant_id)
                if q > 0:
                    qualities.append(q)
            if qualities:
                binding_quality = sum(qualities) / len(qualities)

        # Check required dimensions
        dims_available = self.persistence.check_dims_available(
            hypothesis.required_dims, hypothesis.required_events, tenant_id
        )
        available_dims = sum(1 for avail in dims_available.values() if avail)
        total_dims = len(hypothesis.required_dims)
        if total_dims > 0:
            dims_score = available_dims / total_dims

        if available_dims > 0:
            avail_names = [d for d, a in dims_available.items() if a]
            why_ranked.append(f"dims {', '.join(avail_names)} available")

        # Check proof hooks
        if definition:
            proof_score = self.persistence.get_proof_availability(definition.id, tenant_id)
            if proof_score > 0:
                why_ranked.append(f"proof hooks available (score: {proof_score:.2f})")

        # Calculate final scores
        score = (
            self.WEIGHT_DEFINITION * definition_score +
            self.WEIGHT_EVENTS * events_score +
            self.WEIGHT_DIMS * dims_score +
            self.WEIGHT_PROOF * proof_score
        )

        confidence = (
            self.CONF_BINDING * binding_quality +
            self.CONF_DEFINITION * definition_quality
        )

        # Clamp to [0, 1]
        score = max(0.0, min(1.0, score))
        confidence = max(0.0, min(1.0, confidence))

        return score, confidence, why_ranked

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
            score, confidence, why_ranked = self.score_hypothesis(
                h, definition, tenant_id
            )

            # Apply base probability as a multiplier
            probability = score * h.base_probability + (1 - h.base_probability) * score
            probability = max(0.0, min(1.0, probability))

            # Format label
            label = h.label_template.format(metric=metric_name)

            # Format plan_id
            plan_id = h.plan_id.format(
                metric=parsed.get("metric_hint", "unknown").replace("_", "_")
            )

            scored_hypotheses.append({
                "id": h.id,
                "label": label,
                "probability": probability,
                "confidence": confidence,
                "why_ranked": why_ranked,
                "required_events": h.required_events,
                "required_dims": h.required_dims,
                "plan_id": plan_id,
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
