"""
Compatibility routes — old /api/reports/* paths serving v2 engine data.

Maps legacy endpoint paths to v2 engine handlers so existing consumers
(NLQ, Platform, frontend) can transition without breaking.
Response shapes are v2 format — consumers should update their parsers.

Registered at /api/reports in main.py (replaces old reports router when
LEGACY_JSON_LOAD is not set).
"""

from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from backend.api.routes.v2_helpers import resolve_tenant_and_run
from backend.engine.combining_v2 import CombiningEngineV2
from backend.engine.overlap_v2 import OverlapEngineV2
from backend.engine.cross_sell_v2 import CrossSellEngineV2
from backend.engine.ebitda_bridge_v2 import EBITDABridgeV2
from backend.engine.qoe_v2 import QualityOfEarningsV2
from backend.engine.what_if_v2 import WhatIfEngineV2
from backend.engine.query_resolver_v2 import TripleQueryResolver
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/api/reports", tags=["Reports (Compat → V2)"])


# ---------------------------------------------------------------------------
# Combining IS  (old: GET /api/reports/combining-is)
# ---------------------------------------------------------------------------
@router.get("/combining-is")
def combining_income_statement(
    period: str = "2025-Q1",
    tenant_id: Optional[str] = Query(None),
    run_id: Optional[str] = Query(None),
):
    """Compat: combining income statement via v2 engine."""
    tid, rid = resolve_tenant_and_run(tenant_id, run_id, domain_hint="financial")
    try:
        engine = CombiningEngineV2(tid, rid)
        return engine.get_combining_income_statement(period)
    except ValueError as e:
        raise HTTPException(status_code=422, detail={"error": "data_incomplete", "detail": str(e)})
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))


# ---------------------------------------------------------------------------
# Entity Overlap  (old: GET /api/reports/entity-overlap)
# ---------------------------------------------------------------------------
@router.get("/entity-overlap")
def entity_overlap(
    tenant_id: Optional[str] = Query(None),
    run_id: Optional[str] = Query(None),
):
    """Compat: overlap summary via v2 engine."""
    tid, rid = resolve_tenant_and_run(tenant_id, run_id)
    try:
        engine = OverlapEngineV2(tid, rid)
        return engine.get_overlap_summary()
    except ValueError as e:
        raise HTTPException(status_code=422, detail={"error": "data_incomplete", "detail": str(e)})
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))


# ---------------------------------------------------------------------------
# Cross-sell  (old: GET /api/reports/cross-sell)
# ---------------------------------------------------------------------------
@router.get("/cross-sell")
def cross_sell(
    tenant_id: Optional[str] = Query(None),
    run_id: Optional[str] = Query(None),
):
    """Compat: cross-sell summary via v2 engine."""
    tid, rid = resolve_tenant_and_run(tenant_id, run_id)
    try:
        engine = CrossSellEngineV2(tid, rid)
        return engine.get_cross_sell_summary()
    except ValueError as e:
        raise HTTPException(status_code=422, detail={"error": "data_incomplete", "detail": str(e)})
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))


# ---------------------------------------------------------------------------
# EBITDA Bridge  (old: GET /api/reports/ebitda-bridge)
# ---------------------------------------------------------------------------
@router.get("/ebitda-bridge")
def ebitda_bridge(
    entity_id: Optional[str] = Query(None),
    tenant_id: Optional[str] = Query(None),
    run_id: Optional[str] = Query(None),
):
    """Compat: EBITDA bridge via v2 engine."""
    tid, rid = resolve_tenant_and_run(tenant_id, run_id, domain_hint="financial")
    try:
        engine = EBITDABridgeV2(tid, rid)
        return engine.get_bridge(entity_id=entity_id)
    except ValueError as e:
        raise HTTPException(status_code=422, detail={"error": "data_incomplete", "detail": str(e)})
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))


# ---------------------------------------------------------------------------
# QoE  (old: GET /api/reports/qoe)
# ---------------------------------------------------------------------------
@router.get("/qoe")
def quality_of_earnings(
    entity_id: Optional[str] = Query(None),
    tenant_id: Optional[str] = Query(None),
    run_id: Optional[str] = Query(None),
):
    """Compat: QoE summary via v2 engine."""
    tid, rid = resolve_tenant_and_run(tenant_id, run_id, domain_hint="financial")
    try:
        engine = QualityOfEarningsV2(tid, rid)
        return engine.get_qoe_summary(entity_id)
    except ValueError as e:
        raise HTTPException(status_code=422, detail={"error": "data_incomplete", "detail": str(e)})
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))


# ---------------------------------------------------------------------------
# What-if  (old: POST /api/reports/what-if)
# ---------------------------------------------------------------------------
class WhatIfCompatRequest(BaseModel):
    entity_id: Optional[str] = None
    period: str = "2025-Q1"
    levers: list[dict] | None = None
    adjustments: list[dict] | None = None


@router.post("/what-if")
def what_if(
    request: WhatIfCompatRequest,
    tenant_id: Optional[str] = Query(None),
    run_id: Optional[str] = Query(None),
):
    """Compat: what-if scenario via v2 engine. Accepts 'levers' or 'adjustments'."""
    tid, rid = resolve_tenant_and_run(tenant_id, run_id, domain_hint="financial")
    # Support both old 'levers' and new 'adjustments' field names
    adjustments = request.adjustments or request.levers or []
    try:
        engine = WhatIfEngineV2(tid, rid)
        return engine.apply_scenario(request.entity_id, request.period, adjustments)
    except ValueError as e:
        raise HTTPException(status_code=422, detail={"error": "data_incomplete", "detail": str(e)})
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))


# ---------------------------------------------------------------------------
# Dashboard  (old: GET /api/reports/dashboard/{persona})
# ---------------------------------------------------------------------------
@router.get("/dashboard/{persona}")
def dashboard(
    persona: str,
    period: str = "2025-Q1",
    tenant_id: Optional[str] = Query(None),
    run_id: Optional[str] = Query(None),
):
    """Compat: persona dashboard via v2 engine resolver."""
    tid, rid = resolve_tenant_and_run(tenant_id, run_id, domain_hint="financial")
    try:
        resolver = TripleQueryResolver(tid, rid)
        entities = resolver._get_entities()
        if not entities:
            raise HTTPException(
                status_code=422,
                detail={"error": "data_incomplete", "detail": "No entities found in triples"},
            )

        entity_id = entities[0]  # primary entity

        # Build dashboard from resolver data
        pnl = resolver.get_income_statement(entity_id, period)
        bs = resolver.get_balance_sheet(entity_id, period)

        return {
            "persona": persona,
            "entity_id": entity_id,
            "period": period,
            "pnl": pnl,
            "balance_sheet": bs,
            "source": "v2_engine",
        }
    except ValueError as e:
        raise HTTPException(status_code=422, detail={"error": "data_incomplete", "detail": str(e)})
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
