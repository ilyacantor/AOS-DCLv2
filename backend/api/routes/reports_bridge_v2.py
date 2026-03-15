"""
V2 EBITDA bridge + QoE routes — data from semantic_triples.

Mounts at /api/dcl/reports/v2/bridge:
  GET /api/dcl/reports/v2/bridge?entity_id=meridian
  GET /api/dcl/reports/v2/bridge/comparison
  GET /api/dcl/reports/v2/bridge/adjustment/{concept}
  GET /api/dcl/reports/v2/bridge/sensitivity
  GET /api/dcl/reports/v2/qoe?entity_id=meridian
  GET /api/dcl/reports/v2/qoe/combined
"""

from typing import Optional

from fastapi import APIRouter, HTTPException

from backend.engine.ebitda_bridge_v2 import EBITDABridgeV2
from backend.engine.qoe_v2 import QualityOfEarningsV2
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/api/dcl/reports/v2", tags=["Reports V2 - Bridge & QoE"])

_TENANT_ID = "400aa910-a6b4-5d44-ab9f-e6aecde37721"
_RUN_ID = "6754a9d7-387a-553f-8c4c-978bfbbfca13"


def _get_bridge() -> EBITDABridgeV2:
    return EBITDABridgeV2(_TENANT_ID, _RUN_ID)


def _get_qoe() -> QualityOfEarningsV2:
    return QualityOfEarningsV2(_TENANT_ID, _RUN_ID)


@router.get("/bridge")
async def get_bridge(entity_id: Optional[str] = None):
    """EBITDA bridge for one entity or combined (entity_id=None)."""
    try:
        engine = _get_bridge()
        return engine.get_bridge(entity_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))


@router.get("/bridge/comparison")
async def get_bridge_comparison():
    """Side-by-side bridge for both entities + combined."""
    try:
        engine = _get_bridge()
        return engine.get_bridge_comparison()
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))


@router.get("/bridge/adjustment/{concept:path}")
async def get_adjustment_detail(concept: str):
    """Detailed view of one adjustment concept."""
    try:
        engine = _get_bridge()
        return engine.get_adjustment_detail(concept)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))


@router.get("/bridge/sensitivity")
async def get_sensitivity_matrix():
    """Sensitivity matrix showing base/low/high scenarios."""
    try:
        engine = _get_bridge()
        return engine.get_sensitivity_matrix()
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))


@router.get("/qoe")
async def get_qoe_summary(entity_id: str = "meridian"):
    """QoE summary for one entity."""
    try:
        engine = _get_qoe()
        return engine.get_qoe_summary(entity_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))


@router.get("/qoe/combined")
async def get_combined_qoe():
    """Combined QoE for both entities."""
    try:
        engine = _get_qoe()
        return engine.get_combined_qoe()
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
