"""
COFA Validation Route
=====================
POST /api/dcl/cofa/validate-completeness

Validates that a COFA mapping covers every source account.
Returns structured rejection with orphaned account details.
"""

from fastapi import APIRouter
from pydantic import BaseModel, Field
from typing import Any

from backend.engine.cofa_validation import COFACompletionGate

router = APIRouter(prefix="/api/dcl/cofa", tags=["COFA Validation"])

_gate = COFACompletionGate()


class COFAValidationRequest(BaseModel):
    source_coa: list[dict[str, Any]] = Field(
        ...,
        description="Source chart of accounts. Each item needs at least "
        "'account_number' and 'account_name'.",
    )
    mapping_entries: list[dict[str, Any]] = Field(
        ...,
        description="COFA mapping entries produced by Maestra. "
        "Each entry should reference source accounts via "
        "'entity_a_account_number' and/or 'entity_b_account_number'.",
    )
    source_key: str = Field(
        default="account_number",
        description="Field name for the account identifier in source_coa.",
    )


class COFAValidationResponse(BaseModel):
    complete: bool
    source_count: int
    mapped_count: int
    orphaned_accounts: list[dict[str, Any]]
    message: str
    rejection_message: str | None = None


@router.post(
    "/validate-completeness",
    response_model=COFAValidationResponse,
    summary="Validate COFA mapping completeness",
    description=(
        "Checks that every account in the source CoA appears in the mapping. "
        "Returns orphaned accounts if incomplete."
    ),
)
async def validate_cofa_completeness(
    req: COFAValidationRequest,
) -> COFAValidationResponse:
    result = _gate.validate_and_reject(
        source_coa=req.source_coa,
        mapping_entries=req.mapping_entries,
        source_key=req.source_key,
    )
    return COFAValidationResponse(**result)
