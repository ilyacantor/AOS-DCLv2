"""
V2 combining financial statement routes — data from semantic_triples.

Mounts at /api/dcl/reports/v2:
  GET /api/dcl/reports/v2/combining/income-statement?period=2025-Q1
  GET /api/dcl/reports/v2/combining/balance-sheet?period=2025-Q1
  GET /api/dcl/reports/v2/combining/cash-flow?period=2025-Q1
  GET /api/dcl/reports/v2/cofa-adjustments?period=2025-Q1
"""

from typing import Optional

from fastapi import APIRouter, HTTPException

from backend.engine.combining_v2 import CombiningEngineV2
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/api/dcl/reports/v2", tags=["Reports V2"])

_TENANT_ID = "400aa910-a6b4-5d44-ab9f-e6aecde37721"
_RUN_ID = "6754a9d7-387a-553f-8c4c-978bfbbfca13"


def _get_engine() -> CombiningEngineV2:
    return CombiningEngineV2(_TENANT_ID, _RUN_ID)


@router.get("/combining/income-statement")
async def get_combining_income_statement_v2(period: str = "2025-Q1"):
    """Four-column combining income statement from semantic_triples."""
    try:
        engine = _get_engine()
        return engine.get_combining_income_statement(period)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))


@router.get("/combining/balance-sheet")
async def get_combining_balance_sheet_v2(period: str = "2025-Q1"):
    """Four-column combining balance sheet from semantic_triples."""
    try:
        engine = _get_engine()
        return engine.get_combining_balance_sheet(period)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))


@router.get("/combining/cash-flow")
async def get_combining_cash_flow_v2(period: str = "2025-Q1"):
    """Four-column combining cash flow from semantic_triples."""
    try:
        engine = _get_engine()
        return engine.get_combining_cash_flow(period)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))


@router.get("/cofa-adjustments")
async def get_cofa_adjustments_v2(period: Optional[str] = None):
    """Get all COFA adjustments from semantic_triples."""
    try:
        engine = _get_engine()
        return engine.get_cofa_adjustments(period=period)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
