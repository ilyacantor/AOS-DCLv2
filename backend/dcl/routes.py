"""
DCL Routes - Metadata-only API endpoints.

DCL is a semantic mapping layer. Query execution and NLQ have been
moved to AOS-NLQ repository.

Endpoints:
- GET /api/datasets/current - Get current dataset info
- GET /api/presets - Get UI preset suggestions
"""
import os
import logging
from typing import Optional
from datetime import datetime

from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter(prefix="/api", tags=["DCL"])
logger = logging.getLogger(__name__)


class DatasetInfo(BaseModel):
    """Current dataset information."""
    dataset_id: str
    snapshot_ts: Optional[str] = None
    source: str
    description: Optional[str] = None


def resolve_dataset_id(requested_id: Optional[str] = None) -> DatasetInfo:
    """
    Resolve the current dataset ID.

    Priority:
    1. Explicitly requested dataset_id
    2. Environment variable DCL_DATASET_ID
    3. Default to "nlq_test"
    """
    if requested_id:
        source = "farm" if requested_id.startswith("farm:") else "demo"
        return DatasetInfo(
            dataset_id=requested_id,
            source=source,
            description="Explicitly requested dataset"
        )

    env_dataset = os.environ.get("DCL_DATASET_ID")
    if env_dataset:
        return DatasetInfo(
            dataset_id=env_dataset,
            source="env",
            description="From DCL_DATASET_ID environment variable"
        )

    return DatasetInfo(
        dataset_id="nlq_test",
        source="demo",
        snapshot_ts=datetime.utcnow().isoformat() + "Z",
        description="Default dataset"
    )


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
