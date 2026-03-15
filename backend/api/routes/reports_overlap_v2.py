"""
V2 overlap and cross-sell routes — data from semantic_triples.

Mounts at /api/dcl/reports/v2/overlap:
  GET /api/dcl/reports/v2/overlap/summary
  GET /api/dcl/reports/v2/overlap/{domain}
  GET /api/dcl/reports/v2/overlap/{domain}/entity-only/{entity_id}
  GET /api/dcl/reports/v2/cross-sell
  GET /api/dcl/reports/v2/cross-sell/summary
"""

from fastapi import APIRouter, HTTPException

from backend.engine.cross_sell_v2 import CrossSellEngineV2
from backend.engine.overlap_v2 import OverlapEngineV2
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/api/dcl/reports/v2", tags=["Reports V2 Overlap"])

_TENANT_ID = "400aa910-a6b4-5d44-ab9f-e6aecde37721"
_RUN_ID = "6754a9d7-387a-553f-8c4c-978bfbbfca13"


def _get_overlap_engine() -> OverlapEngineV2:
    return OverlapEngineV2(_TENANT_ID, _RUN_ID)


def _get_cross_sell_engine() -> CrossSellEngineV2:
    return CrossSellEngineV2(_TENANT_ID, _RUN_ID)


@router.get("/overlap/summary")
async def get_overlap_summary():
    """Overlap summary across customer/vendor/employee domains."""
    try:
        engine = _get_overlap_engine()
        return engine.get_overlap_summary()
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))


@router.get("/overlap/{domain}")
async def get_overlap_domain(domain: str):
    """Overlapping concepts with detail for a specific domain."""
    try:
        engine = _get_overlap_engine()
        concepts = engine.get_overlapping_concepts(domain)
        return {"domain": domain, "overlap_count": len(concepts), "concepts": concepts}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))


@router.get("/overlap/{domain}/entity-only/{entity_id}")
async def get_entity_only(domain: str, entity_id: str):
    """Concepts in a domain that appear ONLY under the given entity."""
    try:
        engine = _get_overlap_engine()
        only = engine.get_entity_only_concepts(domain, entity_id)
        return {"domain": domain, "entity_id": entity_id, "count": len(only), "concepts": only}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))


@router.get("/cross-sell")
async def get_cross_sell():
    """Cross-sell opportunities from overlapping customers and service portfolios."""
    try:
        engine = _get_cross_sell_engine()
        opportunities = engine.get_cross_sell_opportunities()
        return {"total": len(opportunities), "opportunities": opportunities}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))


@router.get("/cross-sell/summary")
async def get_cross_sell_summary():
    """Summary of cross-sell opportunities with ACV totals and breakdowns."""
    try:
        engine = _get_cross_sell_engine()
        return engine.get_cross_sell_summary()
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
