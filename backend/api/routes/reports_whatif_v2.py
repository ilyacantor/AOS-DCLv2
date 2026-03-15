"""
V2 What-If + Revenue Bridge routes — data from semantic_triples.

Mounts at /api/dcl/reports/v2:
  POST /api/dcl/reports/v2/whatif/scenario
  POST /api/dcl/reports/v2/whatif/compare
  GET  /api/dcl/reports/v2/whatif/sensitivity
  POST /api/dcl/reports/v2/whatif/save
  GET  /api/dcl/reports/v2/whatif/scenarios
  GET  /api/dcl/reports/v2/whatif/scenarios/{scenario_id}
  GET  /api/dcl/reports/v2/revenue-bridge
  GET  /api/dcl/reports/v2/revenue-bridge/yoy
  GET  /api/dcl/reports/v2/revenue-bridge/combined
"""

from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from backend.api.routes.v2_helpers import resolve_tenant_and_run
from backend.engine.what_if_v2 import WhatIfEngineV2
from backend.engine.revenue_bridge import RevenueBridgeV2
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/api/dcl/reports/v2", tags=["Reports V2 - What-If & Revenue Bridge"])


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------


class AdjustmentItem(BaseModel):
    concept: str
    type: str  # "pct" or "abs"
    value: float


class ScenarioRequest(BaseModel):
    entity_id: str
    period: str
    adjustments: list[AdjustmentItem]


class CompareRequest(BaseModel):
    entity_id: str
    period: str
    scenarios: dict[str, list[AdjustmentItem]]


class SaveScenarioRequest(BaseModel):
    name: str
    entity_id: str
    period: str
    adjustments: list[AdjustmentItem]


# ---------------------------------------------------------------------------
# What-If endpoints
# ---------------------------------------------------------------------------


@router.post("/whatif/scenario")
async def apply_scenario(
    request: ScenarioRequest,
    tenant_id: Optional[str] = Query(None),
    run_id: Optional[str] = Query(None),
):
    """Apply what-if adjustments to a baseline and compute impacts."""
    tid, rid = resolve_tenant_and_run(tenant_id, run_id)
    try:
        engine = WhatIfEngineV2(tid, rid)
        adjustments = [a.model_dump() for a in request.adjustments]
        return engine.apply_scenario(request.entity_id, request.period, adjustments)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))


@router.post("/whatif/compare")
async def compare_scenarios(
    request: CompareRequest,
    tenant_id: Optional[str] = Query(None),
    run_id: Optional[str] = Query(None),
):
    """Compare multiple named scenarios side by side."""
    tid, rid = resolve_tenant_and_run(tenant_id, run_id)
    try:
        engine = WhatIfEngineV2(tid, rid)
        scenarios = {
            name: [a.model_dump() for a in adjs]
            for name, adjs in request.scenarios.items()
        }
        return engine.compare_scenarios(request.entity_id, request.period, scenarios)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))


@router.get("/whatif/sensitivity")
async def sensitivity_analysis(
    entity_id: str = "meridian",
    period: str = "2025-Q1",
    concept: str = "revenue.total",
    range_pct: float = 20.0,
    steps: int = 5,
    tenant_id: Optional[str] = Query(None),
    run_id: Optional[str] = Query(None),
):
    """Vary a single concept and show impact on EBITDA/net income."""
    tid, rid = resolve_tenant_and_run(tenant_id, run_id)
    try:
        engine = WhatIfEngineV2(tid, rid)
        return engine.sensitivity_analysis(entity_id, period, concept, range_pct, steps)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))


@router.post("/whatif/save")
async def save_scenario(
    request: SaveScenarioRequest,
    tenant_id: Optional[str] = Query(None),
    run_id: Optional[str] = Query(None),
):
    """Persist a scenario to the database."""
    tid, rid = resolve_tenant_and_run(tenant_id, run_id)
    try:
        engine = WhatIfEngineV2(tid, rid)
        adjustments = [a.model_dump() for a in request.adjustments]
        scenario_id = engine.save_scenario(
            request.name, request.entity_id, request.period, adjustments,
        )
        return {"scenario_id": scenario_id, "name": request.name}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))


@router.get("/whatif/scenarios")
async def list_scenarios(
    tenant_id: Optional[str] = Query(None),
    run_id: Optional[str] = Query(None),
):
    """List all saved scenarios."""
    tid, rid = resolve_tenant_and_run(tenant_id, run_id)
    try:
        engine = WhatIfEngineV2(tid, rid)
        return engine.list_scenarios()
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))


@router.get("/whatif/scenarios/{scenario_id}")
async def load_scenario(
    scenario_id: str,
    tenant_id: Optional[str] = Query(None),
    run_id: Optional[str] = Query(None),
):
    """Load a saved scenario and re-apply against current baselines."""
    tid, rid = resolve_tenant_and_run(tenant_id, run_id)
    try:
        engine = WhatIfEngineV2(tid, rid)
        return engine.load_scenario(scenario_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))


# ---------------------------------------------------------------------------
# Revenue Bridge endpoints
# ---------------------------------------------------------------------------


@router.get("/revenue-bridge")
async def get_revenue_bridge(
    entity_id: str = "meridian",
    period_from: Optional[str] = None,
    period_to: Optional[str] = None,
    tenant_id: Optional[str] = Query(None),
    run_id: Optional[str] = Query(None),
):
    """Revenue bridge between two periods."""
    tid, rid = resolve_tenant_and_run(tenant_id, run_id)
    try:
        bridge = RevenueBridgeV2(tid, rid)
        if period_from is None or period_to is None:
            raise ValueError(
                "Revenue bridge requires 'period_from' and 'period_to' query parameters. "
                "Example: ?entity_id=meridian&period_from=2024-Q1&period_to=2025-Q1"
            )
        return bridge.get_revenue_bridge(entity_id, period_from, period_to)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))


@router.get("/revenue-bridge/yoy")
async def get_yoy_bridge(
    entity_id: str = "meridian",
    period: str = "2025-Q1",
    tenant_id: Optional[str] = Query(None),
    run_id: Optional[str] = Query(None),
):
    """Year-over-year revenue bridge."""
    tid, rid = resolve_tenant_and_run(tenant_id, run_id)
    try:
        bridge = RevenueBridgeV2(tid, rid)
        return bridge.get_yoy_bridge(entity_id, period)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))


@router.get("/revenue-bridge/combined")
async def get_combined_revenue_bridge(
    period_from: Optional[str] = None,
    period_to: Optional[str] = None,
    tenant_id: Optional[str] = Query(None),
    run_id: Optional[str] = Query(None),
):
    """Combined (all entities) revenue bridge."""
    tid, rid = resolve_tenant_and_run(tenant_id, run_id)
    try:
        bridge = RevenueBridgeV2(tid, rid)
        if period_from is None or period_to is None:
            raise ValueError(
                "Combined revenue bridge requires 'period_from' and 'period_to' query parameters."
            )
        return bridge.get_combined_revenue_bridge(period_from, period_to)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
