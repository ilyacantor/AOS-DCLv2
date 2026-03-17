"""
COFA Mapping Route
==================
POST /api/dcl/cofa-mapping

Accepts Maestra's structured COFA mapping output and writes semantic triples.
Enforces COFACompletionGate — rejects with 422 if any CoA account is unmapped.
"""

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import Optional

from backend.core.db import get_connection
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


def _load_coa_accounts(cur, entity_id: str) -> dict[str, str]:
    """Load CoA account_name → account_number mapping for an entity.

    Returns {account_name: account_number} from the coa.* triples.
    """
    cur.execute(
        "SELECT split_part(concept, '.', 2) AS acct_num, "
        "       value #>> '{}' AS acct_name "
        "FROM semantic_triples "
        "WHERE concept LIKE 'coa.%%' AND entity_id = %s "
        "  AND property = 'account_name' AND is_active = true",
        (entity_id,),
    )
    return {row[1]: row[0] for row in cur.fetchall() if row[1]}


def _check_completeness(
    acquirer_coa: dict[str, str],
    target_coa: dict[str, str],
    mappings: list[dict],
) -> dict:
    """Check that every CoA account_name appears in the mapping.

    Returns {"complete": bool, "orphans": {...}, "message": str}.
    """
    # Collect all account names referenced in mappings
    mapped_acquirer_names: set[str] = set()
    mapped_target_names: set[str] = set()
    for m in mappings:
        if m.get("acquirer_account"):
            mapped_acquirer_names.add(m["acquirer_account"])
        if m.get("target_account"):
            mapped_target_names.add(m["target_account"])

    acq_orphans = {
        name: num for name, num in acquirer_coa.items()
        if name not in mapped_acquirer_names
    }
    tgt_orphans = {
        name: num for name, num in target_coa.items()
        if name not in mapped_target_names
    }

    complete = len(acq_orphans) == 0 and len(tgt_orphans) == 0

    return {
        "complete": complete,
        "acquirer_orphans": [
            {"account_number": num, "account_name": name}
            for name, num in sorted(acq_orphans.items(), key=lambda x: x[1])
        ],
        "target_orphans": [
            {"account_number": num, "account_name": name}
            for name, num in sorted(tgt_orphans.items(), key=lambda x: x[1])
        ],
        "acquirer_total": len(acquirer_coa),
        "target_total": len(target_coa),
        "acquirer_mapped": len(acquirer_coa) - len(acq_orphans),
        "target_mapped": len(target_coa) - len(tgt_orphans),
    }


@router.post(
    "",
    summary="Write COFA mapping triples",
    description=(
        "Converts Maestra's structured COFA mapping output into semantic triples "
        "and writes them to the semantic_triples table. Idempotent per run_id. "
        "Enforces COFACompletionGate — rejects with 422 if any CoA account is unmapped."
    ),
)
async def create_cofa_mapping(req: COFAMappingRequest):
    data = req.model_dump()

    # --- COFACompletionGate: load CoA accounts from DB and validate ---
    with get_connection() as conn:
        with conn.cursor() as cur:
            acquirer_coa = _load_coa_accounts(cur, req.acquirer_entity_id)
            target_coa = _load_coa_accounts(cur, req.target_entity_id)

    # Only enforce the gate if CoA data exists (it should after Farm ingest)
    if acquirer_coa or target_coa:
        gate_result = _check_completeness(acquirer_coa, target_coa, data["mappings"])

        if not gate_result["complete"]:
            acq_orphan_list = [
                f"{o['account_number']} ({o['account_name']})"
                for o in gate_result["acquirer_orphans"]
            ]
            tgt_orphan_list = [
                f"{o['account_number']} ({o['account_name']})"
                for o in gate_result["target_orphans"]
            ]
            detail_parts = [
                f"COFACompletionGate REJECTED: mapping is incomplete.",
                f"Acquirer ({req.acquirer_entity_id}): "
                f"{gate_result['acquirer_mapped']}/{gate_result['acquirer_total']} mapped.",
                f"Target ({req.target_entity_id}): "
                f"{gate_result['target_mapped']}/{gate_result['target_total']} mapped.",
            ]
            if acq_orphan_list:
                detail_parts.append(
                    f"Missing acquirer accounts: {', '.join(acq_orphan_list)}"
                )
            if tgt_orphan_list:
                detail_parts.append(
                    f"Missing target accounts: {', '.join(tgt_orphan_list)}"
                )
            detail_parts.append(
                "Re-submit with ALL accounts mapped. Every CoA account_name "
                "must appear as either acquirer_account or target_account "
                "in at least one mapping entry."
            )

            logger.warning(
                "[COFA] Completeness gate rejected mapping: "
                "acquirer=%d/%d, target=%d/%d orphans",
                len(gate_result["acquirer_orphans"]), gate_result["acquirer_total"],
                len(gate_result["target_orphans"]), gate_result["target_total"],
            )

            raise HTTPException(status_code=422, detail=" ".join(detail_parts))

        logger.info(
            "[COFA] Completeness gate PASSED: acquirer=%d/%d, target=%d/%d",
            gate_result["acquirer_mapped"], gate_result["acquirer_total"],
            gate_result["target_mapped"], gate_result["target_total"],
        )

    result = write_cofa_mapping(data)

    if result["status"] == "error":
        raise HTTPException(status_code=422, detail=result)

    return JSONResponse(status_code=201, content=result)
