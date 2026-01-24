"""
Domain models for the Answerability Circles NLQ feature.

Defines request/response shapes for:
- POST /api/nlq/answerability_rank
- POST /api/nlq/explain
"""

from typing import List, Optional, Dict, Any, Literal
from pydantic import BaseModel, Field
from backend.domain.base import CamelCaseModel


# =============================================================================
# Data Model Types (for persistence layer) - DCL Semantic Layer
# =============================================================================

class CanonicalEvent(BaseModel):
    """
    A canonical event type in the semantic model.

    Example: revenue_recognized, invoice_posted, mapping_changed

    Table: canonical_events
    - id (string, PK): e.g. revenue_recognized, invoice_posted
    - tenant_id (string): Tenant identifier for isolation
    - schema_json (jsonb): fields/types
    - time_semantics_json (jsonb): which timestamp means what
    - created_at, updated_at
    """
    id: str
    tenant_id: str = "default"
    schema_json: Dict[str, Any] = Field(default_factory=dict)
    time_semantics_json: Dict[str, Any] = Field(default_factory=dict)
    description: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class Entity(BaseModel):
    """
    A business entity (dimension) in the semantic model.

    Example: customer, service_line, region

    Table: entities
    - id (string, PK): customer, service_line, project, contract
    - tenant_id (string): Tenant identifier for isolation
    - identifiers_json (jsonb): cross-system identifier expectations
    """
    id: str
    tenant_id: str = "default"
    identifiers_json: Dict[str, Any] = Field(default_factory=dict)
    description: Optional[str] = None


class Binding(BaseModel):
    """
    Maps a source system to canonical events/entities.

    Table: bindings
    - id (pk)
    - tenant_id (string): Tenant identifier for isolation
    - source_system (string): netsuite, salesforce, etc.
    - canonical_event_id (fk)
    - mapping_json (jsonb): source_field → canonical_field
    - dims_coverage_json (jsonb): {customer:true, service_line:true, region:false}
    - quality_score (float 0..1)
    - freshness_score (float 0..1)
    - updated_at
    """
    id: str
    tenant_id: str = "default"
    source_system: str
    canonical_event_id: str  # Maps to canonical event
    mapping_json: Dict[str, str] = Field(default_factory=dict)  # source_field → canonical_field
    dims_coverage_json: Dict[str, bool] = Field(default_factory=dict)  # {dim: covered}
    quality_score: float = Field(ge=0.0, le=1.0, default=0.5)
    freshness_score: float = Field(ge=0.0, le=1.0, default=0.5)
    updated_at: Optional[str] = None


class Definition(BaseModel):
    """
    A semantic definition (metric or view).

    Table: definitions
    - id (string, PK): services_revenue, services_bookings, etc.
    - tenant_id (string): Tenant identifier for isolation
    - kind: metric|view
    - description
    - default_time_semantics_json (jsonb)
    - created_at, updated_at
    """
    id: str
    tenant_id: str = "default"
    kind: Literal["metric", "view"] = "metric"
    description: Optional[str] = None
    default_time_semantics_json: Dict[str, Any] = Field(default_factory=dict)
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class DefinitionVersionSpec(BaseModel):
    """
    The specification for a definition version.

    Embedded in definition_versions.spec_json
    """
    required_events: List[str] = Field(default_factory=list)
    measure: Dict[str, Any] = Field(default_factory=dict)  # {op: "sum", field: "amount"}
    filters: Dict[str, Any] = Field(default_factory=dict)  # DSL over canonical fields
    allowed_grains: List[str] = Field(default_factory=list)  # ["month", "quarter"]
    allowed_dims: List[str] = Field(default_factory=list)  # ["customer", "service_line"]
    joins: Dict[str, str] = Field(default_factory=dict)  # event field → entity
    time_field: Optional[str] = None  # e.g., "recognized_at"


class DefinitionVersion(BaseModel):
    """
    A versioned definition specification.

    Table: definition_versions
    - id (pk)
    - tenant_id (string): Tenant identifier for isolation
    - definition_id (fk)
    - version (string): v1, v2...
    - status (draft|published|deprecated)
    - spec_json (jsonb): the full specification
    - published_at (nullable)
    """
    id: str
    tenant_id: str = "default"
    definition_id: str
    version: str = "v1"
    status: Literal["draft", "published", "deprecated"] = "published"
    spec: DefinitionVersionSpec = Field(default_factory=DefinitionVersionSpec)
    published_at: Optional[str] = None


class ProofHook(BaseModel):
    """
    A proof hook provides source system pointers for a definition.

    Table: proof_hooks
    - id (pk)
    - tenant_id (string): Tenant identifier for isolation
    - definition_id (fk)
    - pointer_template_json (jsonb): how to point to source record IDs
    - availability_score (0..1)
    """
    id: str
    tenant_id: str = "default"
    definition_id: str
    pointer_template_json: Dict[str, Any] = Field(default_factory=dict)
    availability_score: float = Field(ge=0.0, le=1.0, default=0.5)


# =============================================================================
# Validator Output Types
# =============================================================================

class WeakBinding(BaseModel):
    """A binding with quality issues."""
    source_system: str
    canonical_event_id: str
    dims_missing: List[str] = Field(default_factory=list)
    quality_score: float
    freshness_score: float


class ValidationResult(BaseModel):
    """
    Result of DefinitionValidator.validate()

    Returns detailed information about definition answerability.
    """
    ok: bool
    missing_events: List[str] = Field(default_factory=list)
    missing_dims: List[str] = Field(default_factory=list)
    weak_bindings: List[WeakBinding] = Field(default_factory=list)
    coverage_score: float = Field(ge=0.0, le=1.0, default=0.0)
    freshness_score: float = Field(ge=0.0, le=1.0, default=0.0)
    proof_score: float = Field(ge=0.0, le=1.0, default=0.0)


# =============================================================================
# Compiler Output Types
# =============================================================================

class CompiledPlan(BaseModel):
    """
    Result of DefinitionCompiler.compile()

    Returns a deterministic plan template (does NOT execute SQL).
    """
    sql_template: str = ""
    params_schema: Dict[str, Any] = Field(default_factory=dict)
    required_events: List[str] = Field(default_factory=list)
    required_dims: List[str] = Field(default_factory=list)
    time_semantics: Dict[str, Any] = Field(default_factory=dict)
    proof_hook: Optional[Dict[str, Any]] = None


# =============================================================================
# API Request/Response Models
# =============================================================================

class ContextHints(CamelCaseModel):
    """
    Optional context hints provided with the question.
    """
    time_window: Optional[str] = None  # e.g., "QoQ", "YoY", "MTD"
    metric_hint: Optional[str] = None  # e.g., "services_revenue"


class AnswerabilityRequest(CamelCaseModel):
    """
    Request body for POST /api/nlq/answerability_rank

    Example:
    {
        "question": "Services revenue (25% of total) is down 50% QoQ — what's happening?",
        "tenant_id": "t_123",
        "context": {
            "time_window": "QoQ",
            "metric_hint": "services_revenue"
        }
    }
    """
    question: str
    tenant_id: str = "default"
    context: Optional[ContextHints] = None


class CircleRequirements(CamelCaseModel):
    """
    What a hypothesis requires to be answerable.
    """
    definitions: List[str] = Field(default_factory=list)
    events: List[str] = Field(default_factory=list)
    dims: List[str] = Field(default_factory=list)


class Circle(CamelCaseModel):
    """
    An answer circle (hypothesis) with probability, confidence, and ranking.

    - size = probability_of_answer (how likely this hypothesis answers the question)
    - rank = left→right order (most likely answerable first)
    - color = confidence (evidence quality: hot/warm/cool)
    """
    id: str
    rank: int
    label: str
    probability_of_answer: float = Field(ge=0.0, le=1.0)
    confidence: float = Field(ge=0.0, le=1.0)
    color: Literal["hot", "warm", "cool"]
    why_ranked: List[str] = Field(default_factory=list)
    requires: CircleRequirements
    plan_id: str


class AnswerabilityResponse(CamelCaseModel):
    """
    Response body for POST /api/nlq/answerability_rank

    Returns 2-3 ranked circles (hypotheses) ordered by answerability.
    """
    question: str
    circles: List[Circle] = Field(default_factory=list)
    needs_context: List[str] = Field(default_factory=list)  # Clarifying questions if probability too low


class ExplainRequest(CamelCaseModel):
    """
    Request body for POST /api/nlq/explain

    Example:
    {
        "question": "Services revenue is down 50% QoQ — what's happening?",
        "tenant_id": "t_123",
        "hypothesis_id": "h_volume",
        "plan_id": "plan_services_rev_bridge"
    }
    """
    question: str
    tenant_id: str = "default"
    hypothesis_id: str
    plan_id: str


class Fact(CamelCaseModel):
    """
    A fact in an explanation with confidence level.
    """
    fact: str
    confidence: float = Field(ge=0.0, le=1.0)


class BridgeComponent(CamelCaseModel):
    """
    A component in a variance bridge analysis.
    """
    component: str
    share: float = Field(ge=0.0, le=1.0)


class GoDeeper(CamelCaseModel):
    """
    Additional analysis options for drilling down.
    """
    bridge: List[BridgeComponent] = Field(default_factory=list)
    drilldowns: List[str] = Field(default_factory=list)


class ProofPointer(CamelCaseModel):
    """
    A proof pointer linking to source system evidence.
    """
    type: Literal["query_hash", "source_pointer", "event_trace"]
    value: Optional[str] = None
    system: Optional[str] = None
    ref: Optional[str] = None


class NextAction(CamelCaseModel):
    """
    A suggested next action for the user.
    """
    action: str
    label: str


class ExplainResponse(CamelCaseModel):
    """
    Response body for POST /api/nlq/explain

    Returns a deterministic explanation with proof pointers.
    """
    headline: str
    why: List[Fact] = Field(default_factory=list)
    go_deeper: Optional[GoDeeper] = None
    proof: List[ProofPointer] = Field(default_factory=list)
    next: List[NextAction] = Field(default_factory=list)
