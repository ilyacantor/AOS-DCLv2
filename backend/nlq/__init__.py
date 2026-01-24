"""
NLQ (Natural Language Query) module for Answerability Circles.

This module provides deterministic hypothesis ranking for user questions
without using LLM calls in the hot path.
"""

from backend.nlq.models import (
    AnswerabilityRequest,
    AnswerabilityResponse,
    Circle,
    ExplainRequest,
    ExplainResponse,
)
from backend.nlq.scorer import AnswerabilityScorer
from backend.nlq.persistence import NLQPersistence

__all__ = [
    "AnswerabilityRequest",
    "AnswerabilityResponse",
    "Circle",
    "ExplainRequest",
    "ExplainResponse",
    "AnswerabilityScorer",
    "NLQPersistence",
]
