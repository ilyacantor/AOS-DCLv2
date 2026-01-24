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
# Data Model Types (for persistence layer)
# =============================================================================

class CanonicalEvent(BaseModel):
    """
    A canonical event type in the semantic model.

    Example: revenue_recognized, invoice_posted, mapping_changed
    """
    id: str
    schema_json: Dict[str, Any] = Field(default_factory=dict)
    time_semantics: Optional[str] = None  # e.g., "event_time", "processing_time"
    description: Optional[str] = None


class Entity(BaseModel):
    """
    A business entity (dimension) in the semantic model.

    Example: customer, service_line, region
    """
    id: str
    identifiers_json: Dict[str, Any] = Field(default_factory=dict)
    description: Optional[str] = None


class Binding(BaseModel):
    """
    Maps a source system to semantic model elements.

    Tracks which events and dimensions are available from each source.
    """
    id: str
    source_system: str
    maps_to: str  # event_id or entity_id
    binding_type: Literal["event", "entity"] = "event"
    quality_score: float = Field(ge=0.0, le=1.0, default=0.5)
    dims_available_json: List[str] = Field(default_factory=list)


class Definition(BaseModel):
    """
    A metric/KPI definition in the semantic model.

    Example: services_revenue, total_arr, gross_margin
    """
    id: str
    version: int = 1
    inputs_json: Dict[str, Any] = Field(default_factory=dict)  # Required events/entities
    grain_json: Dict[str, Any] = Field(default_factory=dict)   # Time grain, entity grain
    allowed_dims_json: List[str] = Field(default_factory=list) # Allowed drill-down dimensions
    quality_score: float = Field(ge=0.0, le=1.0, default=0.5)
    description: Optional[str] = None


class ProofHook(BaseModel):
    """
    A proof hook provides source system pointers for a definition.

    Used to generate proof links in explanations.
    """
    id: str
    definition_id: str
    pointer_template_json: Dict[str, Any] = Field(default_factory=dict)
    availability_score: float = Field(ge=0.0, le=1.0, default=0.5)


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
