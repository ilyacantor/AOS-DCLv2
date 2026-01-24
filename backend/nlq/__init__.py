"""
NLQ (Natural Language Query) module for Answerability Circles.

This module provides deterministic hypothesis ranking for user questions
without using LLM calls in the hot path.

Key components:
- AnswerabilityScorer: Ranks hypotheses based on definition validation
- DefinitionValidator: Validates definition answerability from metadata
- DefinitionCompiler: Compiles definitions to query plan templates
- NLQPersistence: JSON fixture-based persistence for semantic metadata
"""

from backend.nlq.models import (
    AnswerabilityRequest,
    AnswerabilityResponse,
    Circle,
    ExplainRequest,
    ExplainResponse,
    # Data model types
    CanonicalEvent,
    Entity,
    Binding,
    Definition,
    DefinitionVersion,
    DefinitionVersionSpec,
    ProofHook,
    # Validator output types
    ValidationResult,
    WeakBinding,
    CompiledPlan,
)
from backend.nlq.scorer import AnswerabilityScorer
from backend.nlq.persistence import NLQPersistence
from backend.nlq.validator import DefinitionValidator, DefinitionCompiler

__all__ = [
    # API models
    "AnswerabilityRequest",
    "AnswerabilityResponse",
    "Circle",
    "ExplainRequest",
    "ExplainResponse",
    # Data model types
    "CanonicalEvent",
    "Entity",
    "Binding",
    "Definition",
    "DefinitionVersion",
    "DefinitionVersionSpec",
    "ProofHook",
    # Validator output types
    "ValidationResult",
    "WeakBinding",
    "CompiledPlan",
    # Services
    "AnswerabilityScorer",
    "NLQPersistence",
    "DefinitionValidator",
    "DefinitionCompiler",
]
