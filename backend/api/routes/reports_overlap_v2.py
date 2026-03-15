"""
V2 overlap and cross-sell routes — data from semantic_triples.

Mounts at /api/dcl/reports/v2/overlap:
  GET /api/dcl/reports/v2/overlap/summary
  GET /api/dcl/reports/v2/overlap/{domain}
  GET /api/dcl/reports/v2/overlap/{domain}/entity-only/{entity_id}
  GET /api/dcl/reports/v2/cross-sell
  GET /api/dcl/reports/v2/cross-sell/summary
"""

from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from backend.api.routes.v2_helpers import resolve_tenant_and_run
from backend.engine.cross_sell_v2 import CrossSellEngineV2
from backend.engine.overlap_v2 import OverlapEngineV2
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/api/dcl/reports/v2", tags=["Reports V2 Overlap"])


@router.get("/overlap/summary")
async def get_overlap_summary(
    tenant_id: Optional[str] = Query(None),
    run_id: Optional[str] = Query(None),
):
    """Overlap summary across customer/vendor/employee domains."""
    tid, rid = resolve_tenant_and_run(tenant_id, run_id)
    try:
        engine = OverlapEngineV2(tid, rid)
        return engine.get_overlap_summary()
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))


@router.get("/overlap/{domain}")
async def get_overlap_domain(
    domain: str,
    tenant_id: Optional[str] = Query(None),
    run_id: Optional[str] = Query(None),
):
    """Overlapping concepts with detail for a specific domain."""
    tid, rid = resolve_tenant_and_run(tenant_id, run_id)
    try:
        engine = OverlapEngineV2(tid, rid)
        concepts = engine.get_overlapping_concepts(domain)
        return {"domain": domain, "overlap_count": len(concepts), "concepts": concepts}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))


@router.get("/overlap/{domain}/entity-only/{entity_id}")
async def get_entity_only(
    domain: str,
    entity_id: str,
    tenant_id: Optional[str] = Query(None),
    run_id: Optional[str] = Query(None),
):
    """Concepts in a domain that appear ONLY under the given entity."""
    tid, rid = resolve_tenant_and_run(tenant_id, run_id)
    try:
        engine = OverlapEngineV2(tid, rid)
        only = engine.get_entity_only_concepts(domain, entity_id)
        return {"domain": domain, "entity_id": entity_id, "count": len(only), "concepts": only}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))


@router.get("/cross-sell")
async def get_cross_sell(
    tenant_id: Optional[str] = Query(None),
    run_id: Optional[str] = Query(None),
):
    """Cross-sell opportunities from overlapping customers and service portfolios."""
    tid, rid = resolve_tenant_and_run(tenant_id, run_id)
    try:
        engine = CrossSellEngineV2(tid, rid)
        opportunities = engine.get_cross_sell_opportunities()
        return {"total": len(opportunities), "opportunities": opportunities}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))


@router.get("/cross-sell/summary")
async def get_cross_sell_summary(
    tenant_id: Optional[str] = Query(None),
    run_id: Optional[str] = Query(None),
):
    """Summary of cross-sell opportunities with ACV totals and breakdowns."""
    tid, rid = resolve_tenant_and_run(tenant_id, run_id)
    try:
        engine = CrossSellEngineV2(tid, rid)
        return engine.get_cross_sell_summary()
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
