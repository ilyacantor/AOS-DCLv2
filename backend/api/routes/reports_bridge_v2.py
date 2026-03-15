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

from fastapi import APIRouter, HTTPException, Query

from backend.api.routes.v2_helpers import resolve_tenant_and_run
from backend.engine.ebitda_bridge_v2 import EBITDABridgeV2
from backend.engine.qoe_v2 import QualityOfEarningsV2
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/api/dcl/reports/v2", tags=["Reports V2 - Bridge & QoE"])


@router.get("/bridge")
async def get_bridge(
    entity_id: Optional[str] = None,
    tenant_id: Optional[str] = Query(None),
    run_id: Optional[str] = Query(None),
):
    """EBITDA bridge for one entity or combined (entity_id=None)."""
    tid, rid = resolve_tenant_and_run(tenant_id, run_id)
    try:
        engine = EBITDABridgeV2(tid, rid)
        return engine.get_bridge(entity_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))


@router.get("/bridge/comparison")
async def get_bridge_comparison(
    tenant_id: Optional[str] = Query(None),
    run_id: Optional[str] = Query(None),
):
    """Side-by-side bridge for both entities + combined."""
    tid, rid = resolve_tenant_and_run(tenant_id, run_id)
    try:
        engine = EBITDABridgeV2(tid, rid)
        return engine.get_bridge_comparison()
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))


@router.get("/bridge/adjustment/{concept:path}")
async def get_adjustment_detail(
    concept: str,
    tenant_id: Optional[str] = Query(None),
    run_id: Optional[str] = Query(None),
):
    """Detailed view of one adjustment concept."""
    tid, rid = resolve_tenant_and_run(tenant_id, run_id)
    try:
        engine = EBITDABridgeV2(tid, rid)
        return engine.get_adjustment_detail(concept)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))


@router.get("/bridge/sensitivity")
async def get_sensitivity_matrix(
    tenant_id: Optional[str] = Query(None),
    run_id: Optional[str] = Query(None),
):
    """Sensitivity matrix showing base/low/high scenarios."""
    tid, rid = resolve_tenant_and_run(tenant_id, run_id)
    try:
        engine = EBITDABridgeV2(tid, rid)
        return engine.get_sensitivity_matrix()
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))


@router.get("/qoe")
async def get_qoe_summary(
    entity_id: str = "meridian",
    tenant_id: Optional[str] = Query(None),
    run_id: Optional[str] = Query(None),
):
    """QoE summary for one entity."""
    tid, rid = resolve_tenant_and_run(tenant_id, run_id)
    try:
        engine = QualityOfEarningsV2(tid, rid)
        return engine.get_qoe_summary(entity_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))


@router.get("/qoe/combined")
async def get_combined_qoe(
    tenant_id: Optional[str] = Query(None),
    run_id: Optional[str] = Query(None),
):
    """Combined QoE for both entities."""
    tid, rid = resolve_tenant_and_run(tenant_id, run_id)
    try:
        engine = QualityOfEarningsV2(tid, rid)
        return engine.get_combined_qoe()
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
