"""
NLQ (Natural Language Query) module for Answerability Circles.

This module provides deterministic hypothesis ranking for user questions
without using LLM calls in the hot path.

Key components:
- AnswerabilityScorer: Ranks hypotheses based on definition validation
- DefinitionValidator: Validates definition answerability from metadata
- DefinitionCompiler: Compiles definitions to query plan templates
- NLQPersistence: JSON fixture-based persistence for semantic metadata
- DefinitionRegistry: Admin API for managing definitions
- ConsistencyValidator: Validates semantic layer consistency
- LineageService: Tracks dependencies and impact analysis
- SchemaEnforcer: Validates schema correctness
- QueryExecutor: Executes queries against data warehouses
- ProofResolver: Resolves proof hooks to source system URLs
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

# New services
from backend.nlq.registry import DefinitionRegistry
from backend.nlq.consistency import ConsistencyValidator
from backend.nlq.lineage import LineageService
from backend.nlq.schema_enforcer import SchemaEnforcer
from backend.nlq.executor import QueryExecutor
from backend.nlq.proof import ProofResolver

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
    # New services
    "DefinitionRegistry",
    "ConsistencyValidator",
    "LineageService",
    "SchemaEnforcer",
    "QueryExecutor",
    "ProofResolver",
]
