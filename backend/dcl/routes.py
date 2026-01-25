"""
DCL Routes - Unified API endpoints for NLQ and structured execution.

Endpoints:
- POST /api/execute - Unified structured execution
- GET /api/datasets/current - Get current Farm dataset info
- GET /api/history - List query history
- GET /api/history/{id} - Get specific history entry
- DELETE /api/history - Clear history
"""
import os
import time
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from backend.dcl.executor.executor import (
    execute_query as dcl_execute,
    ExecuteRequest as DCLExecuteRequest,
)
from backend.dcl.history.persistence import get_history_store, HistoryEntry
from backend.dcl.definitions.registry import DefinitionRegistry

router = APIRouter(prefix="/api", tags=["DCL"])
logger = logging.getLogger(__name__)


# =============================================================================
# Request/Response Models
# =============================================================================


class UnifiedExecuteRequest(BaseModel):
    """Unified execute request for structured execution."""
    definition_id: str = Field(..., description="Definition ID to execute")
    dataset_id: Optional[str] = Field(None, description="Dataset ID (auto-resolved if not provided)")
    limit: Optional[int] = Field(None, description="Limit results (uses definition default if not specified)")
    offset: int = Field(0, description="Offset for pagination")
    filters: Optional[Dict[str, Any]] = Field(None, description="Filters to apply")
    time_range: Optional[Dict[str, str]] = Field(None, description="Time range filter")


class WarningResponse(BaseModel):
    """Warning in response."""
    type: str
    message: str


class UnifiedExecuteResponse(BaseModel):
    """Unified execute response."""
    rows: List[Dict[str, Any]]
    aggregations: Dict[str, Any]
    warnings: List[WarningResponse]
    data_summary: str
    narrative_answer: Optional[str] = None
    debug: Optional[Dict[str, Any]] = None


class DatasetInfo(BaseModel):
    """Current dataset information."""
    dataset_id: str
    snapshot_ts: Optional[str] = None
    source: str  # "demo" | "farm" | "env"
    description: Optional[str] = None


class HistoryEntryResponse(BaseModel):
    """History entry response."""
    id: str
    timestamp: str
    question: str
    dataset_id: str
    definition_id: str
    extracted_params: Dict[str, Any]
    response: Dict[str, Any]
    latency_ms: int
    status: str


class HistoryListResponse(BaseModel):
    """History list response."""
    entries: List[HistoryEntryResponse]
    total: int


class DebugCall(BaseModel):
    """Debug call information."""
    timestamp: str
    endpoint: str
    method: str
    request_payload: Dict[str, Any]
    response_payload: Dict[str, Any]
    latency_ms: int
    definition_id: Optional[str] = None
    warnings: List[str] = []


# =============================================================================
# Dataset Resolution
# =============================================================================


def resolve_dataset_id(requested_id: Optional[str] = None) -> DatasetInfo:
    """
    Resolve the current dataset ID.

    Priority:
    1. Explicitly requested dataset_id
    2. Environment variable DCL_DATASET_ID
    3. Farm API current snapshot (if available)
    4. Default to "demo9"
    """
    # Check explicit request
    if requested_id:
        source = "farm" if requested_id.startswith("farm:") else "demo"
        return DatasetInfo(
            dataset_id=requested_id,
            source=source,
            description=f"Explicitly requested dataset"
        )

    # Check environment variable
    env_dataset = os.environ.get("DCL_DATASET_ID")
    if env_dataset:
        return DatasetInfo(
            dataset_id=env_dataset,
            source="env",
            description=f"From DCL_DATASET_ID environment variable"
        )

    # Check for Farm current snapshot
    farm_api_url = os.environ.get("FARM_API_URL", "https://autonomos.farm")
    try:
        # Try to get current Farm snapshot
        # For now, we default to demo mode if Farm is not configured
        pass
    except Exception:
        pass

    # Default to demo9
    return DatasetInfo(
        dataset_id="demo9",
        source="demo",
        snapshot_ts=datetime.utcnow().isoformat() + "Z",
        description="Default demo dataset"
    )


# =============================================================================
# Endpoints
# =============================================================================


@router.get("/datasets/current", response_model=DatasetInfo)
def get_current_dataset():
    """
    Get the current dataset ID and source information.

    Returns:
    - dataset_id: The resolved dataset ID
    - snapshot_ts: Timestamp of the snapshot (if available)
    - source: Where the dataset comes from ("demo", "farm", "env")
    - description: Human-readable description
    """
    return resolve_dataset_id()


@router.post("/execute", response_model=UnifiedExecuteResponse)
async def execute(request: UnifiedExecuteRequest):
    """
    Execute a definition query with unified response format.

    This is the primary structured execution endpoint for DCL.
    Features:
    - Automatic dataset resolution
    - Proper aggregation semantics (population_total, topn_total, share_of_total_pct)
    - Typed warnings (MISSING_LIMIT, etc.)
    - Mechanical data_summary generation

    Example:
    ```json
    {
        "definition_id": "crm.top_customers",
        "limit": 5
    }
    ```

    Returns:
    - rows: Query result rows
    - aggregations: Computed aggregations
    - warnings: Any warnings (e.g., MISSING_LIMIT)
    - data_summary: Human-readable mechanical summary
    """
    start_time = time.time()

    try:
        # Resolve dataset
        dataset_info = resolve_dataset_id(request.dataset_id)

        # Build DCL request
        dcl_request = DCLExecuteRequest(
            definition_id=request.definition_id,
            dataset_id=dataset_info.dataset_id,
            limit=request.limit,
            offset=request.offset,
            filters=request.filters,
            time_range=request.time_range,
        )

        # Execute
        result = dcl_execute(dcl_request)

        # Store in history (for structured queries too)
        latency_ms = int((time.time() - start_time) * 1000)
        history_store = get_history_store()
        history_store.add(
            question=f"[Structured] {request.definition_id}",
            dataset_id=dataset_info.dataset_id,
            definition_id=request.definition_id,
            extracted_params={
                "limit": request.limit,
                "offset": request.offset,
                "filters": request.filters,
            },
            response=result.to_dict(),
            latency_ms=latency_ms,
            status="success",
        )

        return UnifiedExecuteResponse(
            rows=result.rows,
            aggregations=result.aggregations,
            warnings=[
                WarningResponse(type=w.type.value, message=w.message)
                for w in result.warnings
            ],
            data_summary=result.data_summary,
            narrative_answer=result.narrative_answer,
            debug=result.debug,
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Execute failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Execution failed: {str(e)}")


@router.get("/history", response_model=HistoryListResponse)
def list_history(
    tenant_id: str = Query("default", description="Tenant ID"),
    limit: int = Query(50, description="Maximum entries to return"),
    offset: int = Query(0, description="Offset for pagination"),
):
    """
    List query history entries.

    Returns history entries with:
    - question: The original question or structured query
    - dataset_id: Dataset used
    - definition_id: Definition that was matched/executed
    - response: The full response payload
    - latency_ms: Execution time
    """
    history_store = get_history_store()
    entries = history_store.list(tenant_id=tenant_id, limit=limit, offset=offset)

    return HistoryListResponse(
        entries=[
            HistoryEntryResponse(
                id=e.id,
                timestamp=e.timestamp,
                question=e.question,
                dataset_id=e.dataset_id,
                definition_id=e.definition_id,
                extracted_params=e.extracted_params,
                response=e.response,
                latency_ms=e.latency_ms,
                status=e.status,
            )
            for e in entries
        ],
        total=len(entries),
    )


@router.get("/history/{entry_id}", response_model=HistoryEntryResponse)
def get_history_entry(entry_id: str):
    """
    Get a specific history entry by ID.

    Use this to replay a previous query result without re-executing.
    """
    history_store = get_history_store()
    entry = history_store.get(entry_id)

    if not entry:
        raise HTTPException(status_code=404, detail=f"History entry not found: {entry_id}")

    return HistoryEntryResponse(
        id=entry.id,
        timestamp=entry.timestamp,
        question=entry.question,
        dataset_id=entry.dataset_id,
        definition_id=entry.definition_id,
        extracted_params=entry.extracted_params,
        response=entry.response,
        latency_ms=entry.latency_ms,
        status=entry.status,
    )


@router.delete("/history")
def clear_history(tenant_id: Optional[str] = Query(None, description="Tenant ID to clear (all if not specified)")):
    """Clear query history."""
    history_store = get_history_store()
    history_store.clear(tenant_id)
    return {"status": "cleared", "tenant_id": tenant_id or "all"}


@router.get("/definitions")
def list_definitions():
    """List all available definitions with metadata."""
    from backend.bll.definitions import list_definitions as bll_list

    definitions = bll_list()
    result = []

    for defn in definitions:
        meta = DefinitionRegistry.get_metadata(defn.definition_id)
        result.append({
            "definition_id": defn.definition_id,
            "name": defn.name,
            "description": defn.description,
            "category": defn.category.value,
            "keywords": defn.keywords,
            "metadata": {
                "kind": meta.kind.value if meta else "ranked_list",
                "supports_limit": meta.supports_limit if meta else True,
                "default_limit": meta.default_limit if meta else None,
                "ranked_list": meta.ranked_list if meta else True,
                "primary_metric": meta.primary_metric if meta else None,
                "entity_type": meta.entity_type if meta else None,
            },
        })

    return {"definitions": result}


# =============================================================================
# Presets for UI
# =============================================================================

PRESETS = [
    {
        "id": "top_5_customers",
        "label": "Top 5 customers",
        "question": "Show me the top 5 customers by revenue",
        "category": "CRM",
    },
    {
        "id": "top_10_customers",
        "label": "Top 10 customers",
        "question": "Show me the top 10 customers by revenue",
        "category": "CRM",
    },
    {
        "id": "pipeline",
        "label": "Sales pipeline",
        "question": "What does our sales pipeline look like?",
        "category": "CRM",
    },
    {
        "id": "arr",
        "label": "Current ARR",
        "question": "What is our current ARR?",
        "category": "FinOps",
    },
    {
        "id": "burn_rate",
        "label": "Burn rate",
        "question": "What is our burn rate?",
        "category": "FinOps",
    },
    {
        "id": "idle_resources",
        "label": "Idle resources",
        "question": "Show me zombie or idle resources",
        "category": "AOD",
    },
    {
        "id": "unallocated_spend",
        "label": "Unallocated spend",
        "question": "What is our unallocated cloud spend?",
        "category": "FinOps",
    },
    {
        "id": "slo_status",
        "label": "SLO status",
        "question": "How are our SLOs trending?",
        "category": "Infra",
    },
    {
        "id": "mttr",
        "label": "MTTR metrics",
        "question": "What is our mean time to recovery?",
        "category": "Infra",
    },
]


@router.get("/presets")
def get_presets():
    """Get preset query suggestions for the UI."""
    return {"presets": PRESETS}
