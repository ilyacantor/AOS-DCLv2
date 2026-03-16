"""
COFA Mapping Route
==================
POST /api/dcl/cofa-mapping

Accepts Maestra's structured COFA mapping output and writes semantic triples.
"""

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import Optional

from backend.engine.cofa_mapping_writer import write_cofa_mapping
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/api/dcl/cofa-mapping", tags=["COFA Mapping"])


class MappingEntry(BaseModel):
    unified_account: str
    acquirer_account: Optional[str] = None
    target_account: Optional[str] = None
    confidence: float = Field(..., ge=0.0, le=1.0)
    mapping_basis: str = "unknown"


class ConflictEntry(BaseModel):
    conflict_id: str
    conflict_type: str
    severity: str
    dollar_impact: Optional[float] = None
    description: Optional[str] = None
    acquirer_treatment: Optional[str] = None
    target_treatment: Optional[str] = None
    resolution_status: str = "pending"


class UnifiedAccountEntry(BaseModel):
    account_name: str
    account_type: Optional[str] = None
    hierarchy_parent: Optional[str] = None
    source_entities: list[str] = Field(default_factory=list)


class COFAMappingRequest(BaseModel):
    engagement_id: str
    acquirer_entity_id: str
    target_entity_id: str
    tenant_id: str
    run_id: str
    mappings: list[MappingEntry]
    conflicts: list[ConflictEntry] = Field(default_factory=list)
    unified_accounts: list[UnifiedAccountEntry] = Field(default_factory=list)


@router.post(
    "",
    summary="Write COFA mapping triples",
    description=(
        "Converts Maestra's structured COFA mapping output into semantic triples "
        "and writes them to the semantic_triples table. Idempotent per run_id."
    ),
)
async def create_cofa_mapping(req: COFAMappingRequest):
    data = req.model_dump()
    result = write_cofa_mapping(data)

    if result["status"] == "error":
        raise HTTPException(status_code=422, detail=result)

    return JSONResponse(status_code=201, content=result)
