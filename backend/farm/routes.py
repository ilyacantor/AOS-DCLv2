"""
Farm Integration Routes for DCL.

These endpoints expose Farm's DCL integration APIs through DCL, allowing:
1. Source lookup - fetch pristine records for repair
2. Verification - validate repaired records against ground truth
3. Toxic stream - test Ingest Sidecar with chaos injection

Integration Pattern:
DCL receives drifted data → calls Farm's source endpoint → repairs data 
→ calls verify endpoint to confirm fix matches ground truth
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Dict, Any, List, Optional
from backend.farm.client import get_farm_client
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/api/farm", tags=["Farm Integration"])


class VerifyInvoiceRequest(BaseModel):
    """Request to verify a repaired invoice against ground truth."""
    invoice_id: str
    customer_id: Optional[str] = None
    amount: Optional[float] = None
    currency: str = "USD"
    status: Optional[str] = None
    issue_date: Optional[str] = None
    due_date: Optional[str] = None
    line_items: Optional[List[Dict[str, Any]]] = None
    metadata: Optional[Dict[str, Any]] = None


class RepairRequest(BaseModel):
    """Request to repair a drifted record using Farm's source of truth."""
    invoice_id: str
    drifted_record: Dict[str, Any]


class RepairResponse(BaseModel):
    """Response with repaired record and verification result."""
    invoice_id: str
    original: Dict[str, Any]
    source_of_truth: Dict[str, Any]
    repaired: Dict[str, Any]
    verification: Dict[str, Any]
    drift_detected: List[str]


@router.get("/health")
def farm_health():
    """Check Farm API connectivity."""
    client = get_farm_client()
    result = client.health_check()
    return result


@router.get("/source/salesforce/invoice/{invoice_id}")
def get_source_invoice(invoice_id: str):
    """
    Fetch pristine invoice from Farm's source of truth.
    
    Use this to get the canonical record when repairing drifted data.
    
    Returns:
        Pristine invoice record from Salesforce master
    """
    client = get_farm_client()
    try:
        record = client.get_source_invoice(invoice_id)
        return {
            "status": "ok",
            "source": "salesforce_master",
            "invoice": record
        }
    except Exception as e:
        logger.error(f"Source lookup failed for {invoice_id}: {e}")
        raise HTTPException(status_code=502, detail=f"Farm source lookup failed: {str(e)}")


@router.post("/verify/salesforce/invoice")
def verify_invoice(request: VerifyInvoiceRequest):
    """
    Verify a repaired invoice against Farm's ground truth.
    
    Submit a repaired record to compare against the source of truth.
    Returns detailed mismatch report with field-by-field comparison.
    
    Returns:
        - match: bool - whether record matches ground truth
        - quality_score: float - repair accuracy (0.0-1.0)
        - mismatches: list - fields that don't match
        - field_comparison: dict - detailed field-by-field comparison
    """
    client = get_farm_client()
    try:
        result = client.verify_invoice(request.model_dump())
        return {
            "status": "ok",
            "verification": result
        }
    except Exception as e:
        logger.error(f"Verification failed for {request.invoice_id}: {e}")
        raise HTTPException(status_code=502, detail=f"Farm verification failed: {str(e)}")


@router.get("/stream/synthetic/mulesoft")
def get_toxic_stream(
    limit: int = Query(100, ge=1, le=1000, description="Number of records to fetch"),
    chaos: bool = Query(False, description="Enable chaos injection for testing"),
    offset: Optional[int] = Query(None, ge=0, description="Offset for pagination")
):
    """
    Fetch records from Farm's toxic MuleSoft stream.
    
    Use this to test DCL's Ingest Sidecar with realistic "dirty" data.
    With chaos=true, Farm injects data quality issues:
    - Null values in required fields
    - Type mismatches
    - Invalid references
    - Duplicate records
    
    Returns:
        - data: list of stream records
        - metadata: stream info including chaos_events_so_far
    """
    client = get_farm_client()
    try:
        result = client.get_toxic_stream(limit=limit, chaos=chaos, offset=offset)
        return result
    except Exception as e:
        logger.error(f"Toxic stream fetch failed: {e}")
        raise HTTPException(status_code=502, detail=f"Farm toxic stream failed: {str(e)}")


@router.get("/stream/synthetic/mulesoft/sample")
def get_toxic_stream_sample():
    """
    Get a sample record from the toxic stream.
    
    Useful for schema inspection without fetching the full stream.
    """
    client = get_farm_client()
    try:
        return client.get_toxic_stream_sample()
    except Exception as e:
        logger.error(f"Sample fetch failed: {e}")
        raise HTTPException(status_code=502, detail=f"Farm sample fetch failed: {str(e)}")


@router.post("/repair")
def repair_drifted_record(request: RepairRequest):
    """
    Full repair workflow: fetch source → compare → repair → verify.
    
    This is the complete DCL-Farm integration pattern:
    1. Receive drifted record from Ingest Sidecar
    2. Fetch pristine record from Farm's source of truth
    3. Identify drift fields
    4. Apply repairs (replace drifted values with source values)
    5. Verify repaired record against ground truth
    
    Returns:
        - original: the drifted record
        - source_of_truth: pristine record from Farm
        - repaired: record with drift fields corrected
        - verification: Farm's verification result
        - drift_detected: list of fields that were corrected
    """
    client = get_farm_client()
    try:
        source_record = client.get_source_invoice(request.invoice_id)
        
        drifted = request.drifted_record
        repaired = drifted.copy()
        drift_fields = []
        
        if isinstance(source_record, dict):
            source_data = source_record.get("invoice", source_record)
            for key, source_value in source_data.items():
                if key in drifted and drifted[key] != source_value:
                    drift_fields.append(key)
                    repaired[key] = source_value
        
        verification = client.verify_invoice(repaired)
        
        return RepairResponse(
            invoice_id=request.invoice_id,
            original=drifted,
            source_of_truth=source_record,
            repaired=repaired,
            verification=verification,
            drift_detected=drift_fields
        )
    except Exception as e:
        logger.error(f"Repair workflow failed for {request.invoice_id}: {e}")
        raise HTTPException(status_code=502, detail=f"Repair workflow failed: {str(e)}")
