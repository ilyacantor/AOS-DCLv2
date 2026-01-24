"""
BLL Contract API Routes - REST endpoints for BLL consumption contracts.
"""
import os
from fastapi import APIRouter, HTTPException

from .models import (
    ExecuteRequest, ExecuteResponse, ProofResponse, 
    DefinitionListItem, Definition
)
from .definitions import get_definition, list_definitions
from .executor import execute_definition, generate_proof, DATASET_ID


router = APIRouter(prefix="/api/bll", tags=["BLL Contracts"])


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
            description=d.description
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


@router.post("/execute", response_model=ExecuteResponse)
def execute(request: ExecuteRequest):
    """
    Execute a definition against a dataset.
    
    Returns data, metadata, quality metrics, and lineage information.
    """
    try:
        return execute_definition(request)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
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
