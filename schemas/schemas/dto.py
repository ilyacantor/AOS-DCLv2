"""
DCL Engine Data Transfer Objects (DTOs)

This module defines Pydantic V2 models for DCL Engine API contracts,
providing type safety, validation, and OpenAPI documentation.

Phase 3 Priority 3: Clean API contracts with proper validation
- Uses Pydantic V2 with ConfigDict (not old Config class)
- Includes field validators for tenant_id consistency
- Uses constrained types (UUID, HttpUrl, conint)
- Supports backward compatibility with optional legacy fields
"""

from typing import Optional, List, Dict, Any, Literal, Union
from datetime import datetime
from enum import Enum
from uuid import UUID
from pydantic import BaseModel, Field, ConfigDict, field_validator, conint, constr, HttpUrl
from pydantic.functional_validators import AfterValidator


# ================================================================================
# ENUMS
# ================================================================================

class FeatureFlag(str, Enum):
    """Feature flags for toggling DCL capabilities"""
    DEV_MODE = "dev_mode"
    AUTO_INGEST = "auto_ingest"
    AAM_MODE = "aam_mode"
    RAG_ENABLED = "rag_enabled"
    AGENT_EXECUTION = "agent_execution"
    DISTRIBUTED_LOCKING = "distributed_locking"


class ConnectorType(str, Enum):
    """Supported data source connector types"""
    SALESFORCE = "salesforce"
    HUBSPOT = "hubspot"
    MONGODB = "mongodb"
    SUPABASE = "supabase"
    SNOWFLAKE = "snowflake"
    SAP = "sap"
    NETSUITE = "netsuite"
    DYNAMICS = "dynamics"
    LEGACY_SQL = "legacy_sql"
    FILESOURCE = "filesource"


class DevMode(str, Enum):
    """Development mode states"""
    ENABLED = "enabled"
    DISABLED = "disabled"
    AUTO = "auto"


class ConnectionStatus(str, Enum):
    """Connection status states"""
    ACTIVE = "active"
    PENDING = "pending"
    FAILED = "failed"
    HEALING = "healing"
    DISCONNECTED = "disconnected"
    DRIFTED = "drifted"


class AgentType(str, Enum):
    """Available agent types for execution"""
    REVOPS_PILOT = "revops_pilot"
    FINOPS_ANALYST = "finops_analyst"
    DATA_QUALITY = "data_quality"
    DRIFT_DETECTOR = "drift_detector"
    SCHEMA_OBSERVER = "schema_observer"


# ================================================================================
# BASE DTO
# ================================================================================

class BaseDTO(BaseModel):
    """
    Base DTO with common fields for all requests/responses.
    
    Provides:
    - Idempotency support for distributed systems
    - Tenant isolation for multi-tenancy
    - Request tracing for debugging
    """
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "idempotency_key": "req_550e8400-e29b-41d4-a716-446655440000",
                "tenant_id": "default",
                "trace_id": "trace_123456789"
            }
        }
    )
    
    idempotency_key: Optional[str] = Field(
        None,
        description="Idempotency key for request deduplication",
        min_length=1,
        max_length=255
    )
    tenant_id: Optional[str] = Field(
        "default",
        description="Tenant identifier for multi-tenant isolation",
        max_length=100
    )
    trace_id: Optional[str] = Field(
        None,
        description="Request trace ID for debugging and correlation",
        min_length=1,
        max_length=255
    )
    
    @field_validator('tenant_id')
    @classmethod
    def validate_tenant_id(cls, v: Optional[str]) -> str:
        """Ensure tenant_id is always set to a valid value"""
        if not v or v.strip() == "":
            return "default"
        return v.strip()


# ================================================================================
# PHASE 1: DCL ENGINE DTOs
# ================================================================================

# --- /dcl/connect DTOs ---

class ConnectRequest(BaseDTO):
    """Request to connect data sources and execute agents"""
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "sources": ["salesforce", "hubspot"],
                "agents": ["revops_pilot"],
                "llm_model": "gemini-2.0-flash-exp",
                "force_refresh": False,
                "tenant_id": "default"
            }
        }
    )
    
    sources: List[constr(min_length=1, max_length=100)] = Field(  # type: ignore
        ...,
        description="List of data sources to connect",
        min_length=1,
        max_length=100
    )
    agents: Optional[List[constr(min_length=1, max_length=100)]] = Field(  # type: ignore
        [],
        description="List of agents to execute after connection",
        max_length=50
    )
    llm_model: Optional[str] = Field(
        "gemini-2.0-flash-exp",
        description="LLM model to use for processing",
        max_length=100
    )
    force_refresh: Optional[bool] = Field(
        False,
        description="Force refresh of cached data"
    )
    
    @field_validator('sources')
    @classmethod
    def validate_sources(cls, v: List[str]) -> List[str]:
        """Validate source list is not empty and within limits"""
        if not v:
            raise ValueError("At least one source must be specified")
        if len(v) > 100:
            raise ValueError("Maximum 100 sources allowed")
        return v


class ConnectResponse(BaseDTO):
    """Response from connecting data sources"""
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "status": "success",
                "connected_sources": ["salesforce", "hubspot"],
                "graph_nodes": 25,
                "graph_edges": 30,
                "confidence": 0.85,
                "message": "Successfully connected 2 sources",
                "tenant_id": "default"
            }
        }
    )
    
    status: Literal["success", "partial", "failed"] = Field(
        ...,
        description="Overall connection status"
    )
    connected_sources: List[str] = Field(
        ...,
        description="Successfully connected sources"
    )
    failed_sources: Optional[List[Dict[str, str]]] = Field(
        None,
        description="Sources that failed to connect with error details"
    )
    graph_nodes: conint(ge=0) = Field(  # type: ignore
        0,
        description="Number of nodes in the generated graph"
    )
    graph_edges: conint(ge=0) = Field(  # type: ignore
        0,
        description="Number of edges in the generated graph"
    )
    confidence: Optional[float] = Field(
        None,
        ge=0.0,
        le=1.0,
        description="Overall confidence score of the graph"
    )
    message: str = Field(
        ...,
        description="Human-readable status message"
    )
    execution_time_ms: Optional[float] = Field(
        None,
        description="Execution time in milliseconds"
    )


# --- /dcl/state DTOs ---

class GraphNode(BaseModel):
    """Represents a node in the DCL graph"""
    id: str = Field(..., description="Unique node identifier")
    label: str = Field(..., description="Node display label")
    type: str = Field(..., description="Node type (entity, source, agent)")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional node metadata")


class GraphEdge(BaseModel):
    """Represents an edge in the DCL graph"""
    source: str = Field(..., description="Source node ID")
    target: str = Field(..., description="Target node ID")
    label: Optional[str] = Field(None, description="Edge label")
    weight: Optional[float] = Field(None, description="Edge weight/confidence")


class GraphState(BaseModel):
    """Represents the complete graph state"""
    nodes: List[GraphNode] = Field([], description="Graph nodes")
    edges: List[GraphEdge] = Field([], description="Graph edges")
    confidence: Optional[float] = Field(None, ge=0.0, le=1.0, description="Overall graph confidence")
    last_updated: Optional[datetime] = Field(None, description="Last update timestamp")


class StateResponse(BaseDTO):
    """Response containing current DCL state"""
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "nodes": [
                    {"id": "account_1", "label": "Acme Corp", "type": "entity"},
                    {"id": "salesforce", "label": "Salesforce", "type": "source"}
                ],
                "edges": [
                    {"source": "salesforce", "target": "account_1", "label": "provides"}
                ],
                "confidence": 0.85,
                "last_updated": "2024-01-15T10:30:00Z",
                "sources_added": ["salesforce", "hubspot"],
                "entity_sources": {
                    "account": ["salesforce", "hubspot"],
                    "opportunity": ["salesforce"]
                },
                "metadata": {
                    "tenant_id": "default",
                    "llm_calls": 10,
                    "rag_retrievals": 5
                }
            }
        }
    )
    
    nodes: List[GraphNode] = Field([], description="Graph nodes")
    edges: List[GraphEdge] = Field([], description="Graph edges")
    confidence: Optional[float] = Field(None, ge=0.0, le=1.0, description="Overall graph confidence")
    last_updated: Optional[datetime] = Field(None, description="Last update timestamp")
    sources_added: List[str] = Field([], description="List of connected sources")
    entity_sources: Dict[str, List[str]] = Field({}, description="Mapping of entities to their sources")
    selected_agents: Optional[List[str]] = Field([], description="Currently selected agents")
    metadata: Optional[Dict[str, Any]] = Field(
        None,
        description="Additional state metadata including tenant_id"
    )


# --- /dcl/reset DTOs ---

class ResetRequest(BaseDTO):
    """Request to reset DCL state"""
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "confirm": True,
                "reset_scope": "all",
                "tenant_id": "default"
            }
        }
    )
    
    confirm: bool = Field(
        ...,
        description="Confirmation flag to prevent accidental resets"
    )
    reset_scope: Literal["all", "graph", "sources", "agents"] = Field(
        "all",
        description="Scope of reset operation"
    )
    preserve_config: Optional[bool] = Field(
        False,
        description="Preserve configuration settings during reset"
    )


class ResetResponse(BaseDTO):
    """Response from reset operation"""
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "status": "success",
                "reset_scope": "all",
                "message": "Successfully reset DCL state",
                "tenant_id": "default"
            }
        }
    )
    
    status: Literal["success", "failed"] = Field(..., description="Reset operation status")
    reset_scope: str = Field(..., description="Scope of completed reset")
    message: str = Field(..., description="Human-readable status message")
    error: Optional[str] = Field(None, description="Error details if reset failed")


# --- /dcl/toggle DTOs ---

class ToggleRequest(BaseDTO):
    """Request to toggle a feature flag"""
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "feature": "dev_mode",
                "value": "enabled",
                "tenant_id": "default"
            }
        }
    )
    
    feature: FeatureFlag = Field(..., description="Feature flag to toggle")
    value: Optional[Union[bool, str]] = Field(None, description="New value (if not toggling)")


class ToggleResponse(BaseDTO):
    """Response from toggle operation"""
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "feature": "dev_mode",
                "previous_value": False,
                "current_value": True,
                "message": "Dev mode enabled",
                "tenant_id": "default"
            }
        }
    )
    
    feature: str = Field(..., description="Feature flag that was toggled")
    previous_value: Any = Field(..., description="Previous value")
    current_value: Any = Field(..., description="New current value")
    message: str = Field(..., description="Human-readable status message")


# --- /dcl/source-schemas DTOs ---

class SourceSchema(BaseModel):
    """Schema information for a data source"""
    source: str = Field(..., description="Source name")
    tables: Dict[str, List[str]] = Field(..., description="Tables and their columns")
    row_counts: Optional[Dict[str, int]] = Field(None, description="Row counts per table")
    last_synced: Optional[datetime] = Field(None, description="Last sync timestamp")


class SourceSchemasResponse(BaseDTO):
    """Response containing source schema information"""
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "schemas": [
                    {
                        "source": "salesforce",
                        "tables": {
                            "Account": ["Id", "Name", "Type", "Industry"],
                            "Opportunity": ["Id", "Name", "Amount", "Stage"]
                        },
                        "row_counts": {"Account": 1000, "Opportunity": 500}
                    }
                ],
                "total_sources": 2,
                "tenant_id": "default"
            }
        }
    )
    
    schemas: List[SourceSchema] = Field(..., description="Schema information for each source")
    total_sources: conint(ge=0) = Field(0, description="Total number of sources")  # type: ignore
    total_tables: Optional[conint(ge=0)] = Field(None, description="Total number of tables across all sources")  # type: ignore


# ================================================================================
# PHASE 2: DCL REST API DTOs
# ================================================================================

# --- /api/v1/dcl/views DTOs ---

class ViewsRequest(BaseDTO):
    """Request for DCL unified views"""
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "entity_types": ["account", "opportunity", "contact"],
                "include_metadata": True,
                "limit": 100,
                "tenant_id": "default"
            }
        }
    )
    
    entity_types: Optional[List[str]] = Field(
        None,
        description="Specific entity types to retrieve",
        max_length=20
    )
    include_metadata: Optional[bool] = Field(
        True,
        description="Include metadata in response"
    )
    limit: Optional[conint(ge=1, le=10000)] = Field(  # type: ignore
        100,
        description="Maximum records per entity type"
    )
    offset: Optional[conint(ge=0)] = Field(  # type: ignore
        0,
        description="Pagination offset"
    )


class EntityView(BaseModel):
    """Unified view of an entity across sources"""
    entity_type: str = Field(..., description="Type of entity")
    unified_id: str = Field(..., description="Unified entity ID")
    source_ids: Dict[str, str] = Field(..., description="Source-specific IDs")
    attributes: Dict[str, Any] = Field(..., description="Unified attributes")
    confidence: Optional[float] = Field(None, ge=0.0, le=1.0, description="Unification confidence")
    last_updated: Optional[datetime] = Field(None, description="Last update timestamp")


class ViewsResponse(BaseDTO):
    """Response containing DCL unified views"""
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "views": [
                    {
                        "entity_type": "account",
                        "unified_id": "acc_unified_123",
                        "source_ids": {
                            "salesforce": "001D000000AbcDe",
                            "hubspot": "12345"
                        },
                        "attributes": {
                            "name": "Acme Corp",
                            "industry": "Technology",
                            "revenue": 1000000
                        },
                        "confidence": 0.95
                    }
                ],
                "total_entities": 100,
                "has_more": True,
                "tenant_id": "default"
            }
        }
    )
    
    views: List[EntityView] = Field(..., description="Unified entity views")
    total_entities: conint(ge=0) = Field(0, description="Total entities available")  # type: ignore
    has_more: bool = Field(False, description="More results available for pagination")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional response metadata")


# --- /api/v1/dcl/unify DTOs ---

class UnifyRequest(BaseDTO):
    """Request to unify entities across sources"""
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "entity_type": "contact",
                "matching_strategy": "email",
                "sources": ["salesforce", "hubspot"],
                "confidence_threshold": 0.7,
                "tenant_id": "default"
            }
        }
    )
    
    entity_type: str = Field(
        ...,
        description="Type of entity to unify",
        min_length=1,
        max_length=100
    )
    matching_strategy: Literal["email", "name", "id", "fuzzy", "ml"] = Field(
        "email",
        description="Strategy for matching entities"
    )
    sources: Optional[List[str]] = Field(
        None,
        description="Specific sources to unify (None = all)",
        max_length=50
    )
    confidence_threshold: Optional[float] = Field(
        0.7,
        ge=0.0,
        le=1.0,
        description="Minimum confidence for matches"
    )
    dry_run: Optional[bool] = Field(
        False,
        description="Preview unification without persisting"
    )


class UnificationMatch(BaseModel):
    """Represents a unification match between entities"""
    unified_id: str = Field(..., description="Unified entity ID")
    source_entities: List[Dict[str, Any]] = Field(..., description="Matched source entities")
    match_confidence: float = Field(..., ge=0.0, le=1.0, description="Match confidence score")
    match_reason: str = Field(..., description="Reason for match")


class UnifyResponse(BaseDTO):
    """Response from entity unification"""
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "status": "success",
                "entity_type": "contact",
                "total_processed": 500,
                "total_unified": 450,
                "matches": [
                    {
                        "unified_id": "contact_unified_123",
                        "source_entities": [
                            {"source": "salesforce", "id": "003XX000004TMM2"},
                            {"source": "hubspot", "id": "contact-456"}
                        ],
                        "match_confidence": 0.95,
                        "match_reason": "Email match: john@example.com"
                    }
                ],
                "execution_time_ms": 1250.5,
                "tenant_id": "default"
            }
        }
    )
    
    status: Literal["success", "partial", "failed"] = Field(..., description="Unification status")
    entity_type: str = Field(..., description="Type of entities unified")
    total_processed: conint(ge=0) = Field(0, description="Total entities processed")  # type: ignore
    total_unified: conint(ge=0) = Field(0, description="Total unified entities created")  # type: ignore
    total_duplicates: Optional[conint(ge=0)] = Field(None, description="Duplicates found")  # type: ignore
    matches: Optional[List[UnificationMatch]] = Field(
        None,
        description="Sample of unification matches"
    )
    execution_time_ms: Optional[float] = Field(None, description="Execution time in milliseconds")
    error: Optional[str] = Field(None, description="Error details if unification failed")


# ================================================================================
# AGENT EXECUTION DTOs
# ================================================================================

class AgentExecutionRequest(BaseDTO):
    """Request to execute agents on DCL data"""
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "agents": ["revops_pilot", "data_quality"],
                "input_data": {"focus_entity": "account"},
                "async_execution": True,
                "tenant_id": "default"
            }
        }
    )
    
    agents: List[str] = Field(
        ...,
        description="Agents to execute",
        min_length=1,
        max_length=10
    )
    input_data: Optional[Dict[str, Any]] = Field(
        None,
        description="Input data for agents"
    )
    async_execution: Optional[bool] = Field(
        False,
        description="Execute agents asynchronously"
    )


class AgentResult(BaseModel):
    """Result from agent execution"""
    agent_id: str = Field(..., description="Agent identifier")
    status: Literal["success", "failed", "running"] = Field(..., description="Execution status")
    output: Optional[Dict[str, Any]] = Field(None, description="Agent output data")
    error: Optional[str] = Field(None, description="Error details if failed")
    execution_time_ms: Optional[float] = Field(None, description="Execution time")


class AgentExecutionResponse(BaseDTO):
    """Response from agent execution"""
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "status": "success",
                "results": [
                    {
                        "agent_id": "revops_pilot",
                        "status": "success",
                        "output": {"insights": ["High-value accounts identified"]},
                        "execution_time_ms": 500
                    }
                ],
                "total_execution_time_ms": 750,
                "tenant_id": "default"
            }
        }
    )
    
    status: Literal["success", "partial", "failed"] = Field(..., description="Overall execution status")
    results: List[AgentResult] = Field(..., description="Individual agent results")
    total_execution_time_ms: Optional[float] = Field(None, description="Total execution time")


# ================================================================================
# DATA QUALITY & MONITORING DTOs
# ================================================================================

class DataQualityMetrics(BaseModel):
    """Data quality metrics for DCL"""
    completeness: float = Field(..., ge=0.0, le=1.0, description="Data completeness score")
    accuracy: float = Field(..., ge=0.0, le=1.0, description="Data accuracy score")
    consistency: float = Field(..., ge=0.0, le=1.0, description="Data consistency score")
    timeliness: float = Field(..., ge=0.0, le=1.0, description="Data timeliness score")
    overall_score: float = Field(..., ge=0.0, le=1.0, description="Overall quality score")


class DriftAlert(BaseModel):
    """Schema drift alert"""
    alert_id: str = Field(..., description="Alert identifier")
    source: str = Field(..., description="Affected source")
    entity_type: str = Field(..., description="Affected entity type")
    drift_type: Literal["schema", "data", "volume"] = Field(..., description="Type of drift")
    severity: Literal["low", "medium", "high", "critical"] = Field(..., description="Alert severity")
    detected_at: datetime = Field(..., description="Detection timestamp")
    details: Dict[str, Any] = Field(..., description="Drift details")


class HITLReview(BaseModel):
    """Human-in-the-loop review item"""
    review_id: str = Field(..., description="Review identifier")
    entity_type: str = Field(..., description="Entity type requiring review")
    field_name: str = Field(..., description="Field requiring review")
    current_value: Any = Field(..., description="Current value")
    suggested_value: Any = Field(..., description="Suggested value")
    confidence: float = Field(..., ge=0.0, le=1.0, description="Suggestion confidence")
    reason: str = Field(..., description="Reason for review")