"""
Farm Integration Routes for DCL.

These endpoints expose Farm's DCL integration APIs through DCL, allowing:

v1 (legacy):
1. Source lookup - fetch pristine records for repair
2. Verification - validate repaired records against ground truth
3. Toxic stream - test Ingest Sidecar with chaos injection

v2 (business data):
4. Generate - trigger Farm data generation + push to DCL
5. Verify - run ground truth verification loop
6. Inspect - view ingested data, profiles, payloads

Integration Pattern (v2):
DCL triggers Farm generation → Farm pushes 20 pipes to DCL ingest →
DCL unifies + maps → DCL verifies against Farm ground truth
"""

import os
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional
from backend.farm.client import get_farm_client
from backend.farm.ingest_bridge import get_ingest_summary
from backend.farm.verification import verify_against_ground_truth
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


# =============================================================================
# v2 Business Data Endpoints
# =============================================================================


class GenerateRequest(BaseModel):
    """Request to trigger Farm v2 business data generation."""
    push_to_dcl: bool = Field(
        True,
        description="If true, Farm pushes all 20 pipes to DCL's ingest endpoint"
    )
    dcl_ingest_url: Optional[str] = Field(
        None,
        description="Override DCL ingest URL. Defaults to DCL_INGEST_URL env var."
    )
    seed: Optional[int] = Field(
        None,
        description="Deterministic seed for reproducible generation"
    )


@router.post("/v2/generate")
def generate_business_data(request: GenerateRequest):
    """
    Trigger Farm v2 business data generation.

    POST /api/farm/v2/generate

    This calls Farm's POST /api/business-data/generate endpoint with
    push_to_dcl=true, which causes Farm to push 20 pipe payloads
    (120k+ records, 8 source systems) to DCL's POST /api/dcl/ingest.

    Returns:
        - run_id: Farm generation run ID
        - record_counts: per-pipe record counts
        - pipes_pushed: number of pipes sent to DCL
    """
    client = get_farm_client()

    dcl_url = request.dcl_ingest_url or os.getenv("DCL_INGEST_URL")

    try:
        result = client.generate_business_data(
            push_to_dcl=request.push_to_dcl,
            dcl_ingest_url=dcl_url,
            seed=request.seed,
        )
        return {
            "status": "ok",
            "farm_run_id": result.get("run_id"),
            "generation": result,
        }
    except Exception as e:
        logger.error(f"Farm generation failed: {e}")
        raise HTTPException(
            status_code=502,
            detail=f"Farm generation failed: {str(e)}"
        )


@router.post("/v2/verify/{farm_run_id}")
def verify_farm_run(farm_run_id: str, dcl_run_id: Optional[str] = None):
    """
    Run the full ground truth verification loop for a Farm generation run.

    POST /api/farm/v2/verify/{farm_run_id}

    DCL's verification loop:
    1. Check ingestion completeness (20 pipes received?)
    2. Fetch ground truth manifest from Farm (89 metrics/quarter)
    3. Compare DCL's unified data against each metric
    4. Verify DCL detected the 3 intentional cross-system conflicts
    5. Check 13 dimensional breakdowns

    Returns:
        - overall_grade: A/B/C/D/F
        - ingestion: completeness stats
        - metrics: accuracy per metric
        - conflicts: detection results
        - dimensional: breakdown accuracy
    """
    try:
        report = verify_against_ground_truth(farm_run_id, dcl_run_id)
        return report.to_dict()
    except Exception as e:
        logger.error(f"Verification failed for {farm_run_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Verification failed: {str(e)}"
        )


@router.get("/v2/ingest-status")
def get_farm_ingest_status():
    """
    Get the current state of Farm data ingested into DCL.

    GET /api/farm/v2/ingest-status

    Shows how many pipes, sources, and records have been received.
    """
    summary = get_ingest_summary()
    return {
        "status": "ok",
        **summary,
    }


@router.get("/v2/runs")
def list_farm_runs():
    """
    List all Farm business data generation runs.

    GET /api/farm/v2/runs

    Proxies to Farm's GET /api/business-data/runs endpoint.
    """
    client = get_farm_client()
    try:
        return client.list_business_data_runs()
    except Exception as e:
        logger.error(f"Failed to list Farm runs: {e}")
        raise HTTPException(
            status_code=502,
            detail=f"Failed to list Farm runs: {str(e)}"
        )


@router.get("/v2/ground-truth/{farm_run_id}")
def get_ground_truth(farm_run_id: str):
    """
    Fetch the full ground truth manifest from Farm.

    GET /api/farm/v2/ground-truth/{farm_run_id}

    Returns 89 metrics/quarter, 13 dimensional breakdowns,
    36 expected conflicts.
    """
    client = get_farm_client()
    try:
        return client.get_ground_truth(farm_run_id)
    except Exception as e:
        logger.error(f"Ground truth fetch failed: {e}")
        raise HTTPException(
            status_code=502,
            detail=f"Ground truth fetch failed: {str(e)}"
        )


@router.get("/v2/ground-truth/{farm_run_id}/metric/{metric}")
def get_ground_truth_metric(
    farm_run_id: str,
    metric: str,
    quarter: Optional[str] = Query(None, description="e.g., 2024-Q1"),
):
    """
    Look up a single metric from the ground truth.

    GET /api/farm/v2/ground-truth/{farm_run_id}/metric/{metric}?quarter=2024-Q1
    """
    client = get_farm_client()
    try:
        return client.get_ground_truth_metric(farm_run_id, metric, quarter)
    except Exception as e:
        raise HTTPException(
            status_code=502,
            detail=f"Metric lookup failed: {str(e)}"
        )


@router.get("/v2/ground-truth/{farm_run_id}/dimensional/{dimension}")
def get_ground_truth_dimensional(farm_run_id: str, dimension: str):
    """
    Fetch a dimensional breakdown from the ground truth.

    GET /api/farm/v2/ground-truth/{farm_run_id}/dimensional/{dimension}
    """
    client = get_farm_client()
    try:
        return client.get_ground_truth_dimensional(farm_run_id, dimension)
    except Exception as e:
        raise HTTPException(
            status_code=502,
            detail=f"Dimensional breakdown failed: {str(e)}"
        )


@router.get("/v2/ground-truth/{farm_run_id}/conflicts")
def get_ground_truth_conflicts(farm_run_id: str):
    """
    Fetch expected cross-system conflicts from the ground truth.

    GET /api/farm/v2/ground-truth/{farm_run_id}/conflicts

    Returns the 3 intentional conflicts DCL should detect.
    """
    client = get_farm_client()
    try:
        return client.get_ground_truth_conflicts(farm_run_id)
    except Exception as e:
        raise HTTPException(
            status_code=502,
            detail=f"Conflicts fetch failed: {str(e)}"
        )


@router.get("/v2/profile/{farm_run_id}")
def get_business_profile(farm_run_id: str):
    """
    Fetch the full financial model trajectory for a Farm run.

    GET /api/farm/v2/profile/{farm_run_id}

    Returns ARR waterfall, P&L, BS, CF, SaaS metrics per quarter.
    """
    client = get_farm_client()
    try:
        return client.get_business_profile(farm_run_id)
    except Exception as e:
        raise HTTPException(
            status_code=502,
            detail=f"Profile fetch failed: {str(e)}"
        )


@router.get("/v2/payload/{farm_run_id}/{pipe_id}")
def get_pipe_payload(farm_run_id: str, pipe_id: str):
    """
    Fetch raw DCL payload for a specific pipe (debugging).

    GET /api/farm/v2/payload/{farm_run_id}/{pipe_id}
    """
    client = get_farm_client()
    try:
        return client.get_pipe_payload(farm_run_id, pipe_id)
    except Exception as e:
        raise HTTPException(
            status_code=502,
            detail=f"Payload fetch failed: {str(e)}"
        )
