"""
BLL Contract API Routes - REST endpoints for BLL consumption contracts.
"""
import os
import logging
from fastapi import APIRouter, HTTPException, Request

from .models import (
    ExecuteRequest, ExecuteResponse, ProofResponse, 
    DefinitionListItem, Definition
)
from .definitions import get_definition, list_definitions
from .executor import execute_definition, generate_proof, DATASET_ID


router = APIRouter(prefix="/api/bll", tags=["BLL Contracts"])
logger = logging.getLogger(__name__)


@router.get("/dataset")
def get_dataset_info():
    """Get current dataset configuration."""
    return {
        "dataset_id": DATASET_ID,
        "env_var": "DCL_DATASET_ID",
        "default": "demo9"
    }


@router.get("/definitions", response_model=list[DefinitionListItem])
def list_all_definitions():
    """List all available BLL definitions."""
    definitions = list_definitions()
    return [
        DefinitionListItem(
            definition_id=d.definition_id,
            name=d.name,
            category=d.category,
            version=d.version,
            description=d.description,
            keywords=d.keywords
        )
        for d in definitions
    ]


@router.get("/definitions/{definition_id}", response_model=Definition)
def get_definition_by_id(definition_id: str):
    """Get a specific definition by ID."""
    definition = get_definition(definition_id)
    if not definition:
        raise HTTPException(status_code=404, detail=f"Definition not found: {definition_id}")
    return definition


@router.post("/execute/debug")
async def execute_debug(request: Request):
    """Debug endpoint to see raw request body."""
    body = await request.json()
    logger.info(f"Raw execute request body: {body}")
    return {"received": body, "expected_format": {"definition_id": "string", "dataset_id": "string (default: nlq_test)"}}


@router.post("/execute", response_model=ExecuteResponse)
async def execute(request: Request):
    """
    Execute a definition against a dataset.
    
    Returns data, metadata, quality metrics, and lineage information.
    Accepts both snake_case and camelCase field names.
    """
    try:
        body = await request.json()
        print(f"[BLL] Raw execute request body: {body}")
        
        normalized = {}
        normalized["definition_id"] = body.get("definition_id") or body.get("definitionId")
        normalized["dataset_id"] = body.get("dataset_id") or body.get("datasetId") or "nlq_test"
        normalized["version"] = body.get("version")
        normalized["limit"] = body.get("limit", 1000)
        normalized["offset"] = body.get("offset", 0)
        normalized["dimensions"] = body.get("dimensions")
        normalized["filters"] = body.get("filters")
        normalized["time_window"] = body.get("time_window") or body.get("timeWindow")
        
        if not normalized["definition_id"]:
            print(f"[BLL] Missing definition_id in request: {body}")
            raise HTTPException(
                status_code=422, 
                detail="Missing required field: definition_id (or definitionId)"
            )
        
        print(f"[BLL] Normalized request: {normalized}")
        exec_request = ExecuteRequest(**normalized)
        result = execute_definition(exec_request)
        print(f"[BLL] Execute success, returning {len(result.data)} rows")
        return result
    except HTTPException:
        raise
    except FileNotFoundError as e:
        print(f"[BLL] File not found: {e}")
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        print(f"[BLL] Value error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        import traceback
        print(f"[BLL] Execution failed: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Execution failed: {str(e)}")


@router.get("/proof/{definition_id}", response_model=ProofResponse)
def get_proof(definition_id: str):
    """
    Get execution proof/lineage for a definition.
    
    Returns source/join/filter breadcrumbs showing how data is derived.
    """
    try:
        return generate_proof(definition_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
