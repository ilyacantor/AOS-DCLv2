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

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from backend.engine.what_if_v2 import WhatIfEngineV2
from backend.engine.revenue_bridge import RevenueBridgeV2
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/api/dcl/reports/v2", tags=["Reports V2 - What-If & Revenue Bridge"])

_TENANT_ID = "400aa910-a6b4-5d44-ab9f-e6aecde37721"
_RUN_ID = "6754a9d7-387a-553f-8c4c-978bfbbfca13"


def _get_whatif() -> WhatIfEngineV2:
    return WhatIfEngineV2(_TENANT_ID, _RUN_ID)


def _get_bridge() -> RevenueBridgeV2:
    return RevenueBridgeV2(_TENANT_ID, _RUN_ID)


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
async def apply_scenario(request: ScenarioRequest):
    """Apply what-if adjustments to a baseline and compute impacts."""
    try:
        engine = _get_whatif()
        adjustments = [a.model_dump() for a in request.adjustments]
        return engine.apply_scenario(request.entity_id, request.period, adjustments)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))


@router.post("/whatif/compare")
async def compare_scenarios(request: CompareRequest):
    """Compare multiple named scenarios side by side."""
    try:
        engine = _get_whatif()
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
):
    """Vary a single concept and show impact on EBITDA/net income."""
    try:
        engine = _get_whatif()
        return engine.sensitivity_analysis(entity_id, period, concept, range_pct, steps)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))


@router.post("/whatif/save")
async def save_scenario(request: SaveScenarioRequest):
    """Persist a scenario to the database."""
    try:
        engine = _get_whatif()
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
async def list_scenarios():
    """List all saved scenarios."""
    try:
        engine = _get_whatif()
        return engine.list_scenarios()
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))


@router.get("/whatif/scenarios/{scenario_id}")
async def load_scenario(scenario_id: str):
    """Load a saved scenario and re-apply against current baselines."""
    try:
        engine = _get_whatif()
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
):
    """Revenue bridge between two periods."""
    try:
        bridge = _get_bridge()
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
):
    """Year-over-year revenue bridge."""
    try:
        bridge = _get_bridge()
        return bridge.get_yoy_bridge(entity_id, period)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))


@router.get("/revenue-bridge/combined")
async def get_combined_revenue_bridge(
    period_from: Optional[str] = None,
    period_to: Optional[str] = None,
):
    """Combined (all entities) revenue bridge."""
    try:
        bridge = _get_bridge()
        if period_from is None or period_to is None:
            raise ValueError(
                "Combined revenue bridge requires 'period_from' and 'period_to' query parameters."
            )
        return bridge.get_combined_revenue_bridge(period_from, period_to)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
