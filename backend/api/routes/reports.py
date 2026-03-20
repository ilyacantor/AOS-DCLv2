"""
DCL Report endpoints — drill-down, snapshot, Maestra, and data push routes.

Primary report routes (combining-is, entity-overlap, cross-sell, ebitda-bridge,
what-if, qoe, dashboard) are served by compat.py via v2 engines.
This module retains drill-down and auxiliary routes:
  GET  /api/reports/combining-is/drill/{line_item}  — drill into combining IS line item
  GET  /api/reports/entity-overlap/drill/{m}/{n}    — drill into overlap match
  GET  /api/reports/cross-sell/drill/{customer_id}  — drill into cross-sell candidate
  GET  /api/reports/what-if/presets                 — what-if preset definitions
  POST /api/reports/qoe/snapshot                    — save QofE snapshot for temporal tracking
  POST /api/reports/maestra/engage                  — create Maestra engagement
  POST /api/reports/maestra/{id}/message            — send Maestra message
  GET  /api/reports/maestra/{id}/status             — engagement status
  POST /api/reports/data/push/customer_profiles     — push customer profiles & invalidate cache
"""

import json
import time
from datetime import date, datetime
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException

from backend.engine import _engine_cache
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/api/reports", tags=["Reports"])

_DATA_DIR = Path(__file__).resolve().parent.parent.parent.parent / "data"


def _load_combining_data() -> dict:
    """Load combining_statements.json or raise 404 if missing."""
    combining_path = _DATA_DIR / "combining_statements.json"
    if not combining_path.exists():
        raise HTTPException(
            status_code=404,
            detail=(
                "Combining statement data not generated. "
                "Run scripts/generate_combining_data.py first."
            ),
        )
    with open(combining_path) as f:
        return json.load(f)


def _resolve_period(data: dict, period: Optional[str]) -> tuple[str, list[str]]:
    """Resolve the target period from combining statement data.

    Returns (resolved_period, available_periods).  Raises 404 if no periods
    exist or the requested period is not found.
    """
    periods = data.get("_periods", [])
    if not periods:
        raise HTTPException(
            status_code=404,
            detail="No combining statement periods available in combining_statements.json.",
        )

    if period is None:
        today = date.today()
        q_end_month = {1: 3, 2: 6, 3: 9, 4: 12}
        for p in reversed(periods):
            year = int(p[:4])
            q = int(p[-1])
            end_month = q_end_month[q]
            if date(year, end_month, 28) < today:
                period = p
                break
        if period is None:
            period = periods[0]

    if period not in data:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Period '{period}' not found in combining statement data. "
                f"Available periods: {periods}"
            ),
        )

    return period, periods


@router.get("/combining-is/drill/{line_item}")
async def drill_combining_line_item(line_item: str, period: Optional[str] = None):
    """Drill into a combining IS line item to see COFA adjustment details.

    Returns the full line item with adjustment_details showing which COFA
    adjustments affect this line, their amounts, rationale, and entity treatments.
    """
    data = _load_combining_data()
    period, periods = _resolve_period(data, period)

    period_data = data[period]
    line_items = period_data.get("line_items", [])

    # Case-insensitive match on line_item name.
    match = None
    for li in line_items:
        if li["line_item"].lower() == line_item.lower():
            match = li
            break

    if match is None:
        available = [li["line_item"] for li in line_items]
        raise HTTPException(
            status_code=404,
            detail=(
                f"Line item '{line_item}' not found in period '{period}'. "
                f"Available line items: {available}"
            ),
        )

    return {
        "period": period,
        "available_periods": periods,
        **match,
    }


def _load_overlap_data() -> dict:
    """Load entity_overlap.json or raise 404 if missing."""
    overlap_path = _DATA_DIR / "entity_overlap.json"
    if not overlap_path.exists():
        raise HTTPException(
            status_code=404,
            detail=(
                "Entity overlap data not generated. "
                "Run scripts/generate_combining_data.py first."
            ),
        )
    with open(overlap_path) as f:
        return json.load(f)


@router.get("/entity-overlap/drill/{match_type}/{name}")
async def drill_entity_overlap(match_type: str, name: str):
    """Drill into an entity overlap match to see full detail.

    Args:
        match_type: "customer", "vendor", or "people"
        name: canonical_name for customer/vendor, function name for people
    """
    data = _load_overlap_data()

    if match_type == "customer":
        matches = data.get("customer_overlap", {}).get("matches", [])
        for m in matches:
            if m["canonical_name"].lower() == name.lower():
                return m
        available = [m["canonical_name"] for m in matches]
        raise HTTPException(
            status_code=404,
            detail=(
                f"Customer '{name}' not found in overlap data. "
                f"Available customers ({len(available)}): {available[:20]}"
                f"{'...' if len(available) > 20 else ''}"
            ),
        )

    elif match_type == "vendor":
        matches = data.get("vendor_overlap", {}).get("matches", [])
        for m in matches:
            if m["canonical_name"].lower() == name.lower():
                return m
        available = [m["canonical_name"] for m in matches]
        raise HTTPException(
            status_code=404,
            detail=(
                f"Vendor '{name}' not found in overlap data. "
                f"Available vendors ({len(available)}): {available[:20]}"
                f"{'...' if len(available) > 20 else ''}"
            ),
        )

    elif match_type == "people":
        functions = data.get("people_overlap", {}).get("functions", [])
        for f in functions:
            if f["function"].lower() == name.lower():
                return f
        available = [f["function"] for f in functions]
        raise HTTPException(
            status_code=404,
            detail=(
                f"Function '{name}' not found in people overlap data. "
                f"Available functions: {available}"
            ),
        )

    else:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Invalid match_type '{match_type}'. "
                f"Must be one of: customer, vendor, people"
            ),
        )


# ─── Cross-sell pipeline ────────────────────────────────────────────────


@router.get("/cross-sell/drill/{customer_id}")
async def drill_cross_sell(customer_id: str):
    """Drill into a specific cross-sell candidate by customer_id."""
    try:
        result = _engine_cache.get("cross_sell")
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

    for candidate in result["m_to_c"] + result["c_to_m"]:
        if candidate["customer_id"] == customer_id:
            return candidate

    all_ids = [c["customer_id"] for c in result["m_to_c"] + result["c_to_m"]]
    raise HTTPException(
        status_code=404,
        detail=(
            f"Customer '{customer_id}' not found in cross-sell pipeline. "
            f"Pipeline contains {len(all_ids)} candidates."
        ),
    )


# ─── What-if / sensitivity ─────────────────────────────────────────────


@router.get("/what-if/presets")
async def get_what_if_presets():
    """Return available what-if presets and lever definitions."""
    from backend.engine.what_if import PRESETS, LEVER_DEFINITIONS

    return {
        "presets": PRESETS,
        "lever_definitions": LEVER_DEFINITIONS,
    }


# ─── Maestra engagement lifecycle ───────────────────────────────────────

# In-memory engagement store (single-process; production would use DB)
_engagements: dict[str, dict] = {}


@router.post("/maestra/engage")
async def create_maestra_engagement():
    """Create a new Maestra engagement using the active engagement config."""
    from backend.engine.maestra import create_engagement

    state = create_engagement()
    _engagements[state["engagement_id"]] = state
    return {
        "engagement_id": state["engagement_id"],
        "phase": state["phase"],
        "deal_name": state["deal_name"],
        "workstreams": len(state["workstreams"]),
        "risks": len(state["risks"]),
    }


@router.post("/maestra/{engagement_id}/message")
async def send_maestra_message(engagement_id: str, body: dict):
    """Send a message to Maestra for an existing engagement.

    Body JSON: { "message": "your message here" }
    """
    if engagement_id not in _engagements:
        raise HTTPException(
            status_code=404,
            detail=f"Engagement '{engagement_id}' not found. Create one via POST /api/reports/maestra/engage.",
        )

    message = body.get("message", "").strip()
    if not message:
        raise HTTPException(status_code=400, detail="Message cannot be empty.")

    from backend.engine.maestra import process_message

    t0 = time.monotonic()
    try:
        result = process_message(engagement_id, message, _engagements[engagement_id])
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    _engagements[engagement_id] = result["state"]
    elapsed_ms = (time.monotonic() - t0) * 1000
    logger.info("[reports] maestra message processed in %.0fms", elapsed_ms)
    response = {
        "response": result["response"],
        "actions_taken": result["actions_taken"],
        "suggestions": result["suggestions"],
        "phase": result["state"]["phase"],
    }
    if result.get("navigation"):
        response["navigation"] = result["navigation"]
    return response


@router.get("/maestra/{engagement_id}/status")
async def get_maestra_status(engagement_id: str):
    """Get the current status of a Maestra engagement."""
    if engagement_id not in _engagements:
        raise HTTPException(
            status_code=404,
            detail=f"Engagement '{engagement_id}' not found.",
        )

    from backend.engine.maestra import get_engagement_status

    return get_engagement_status(_engagements[engagement_id])


# ── Quality of Earnings ──────────────────────────────────────────────────────


@router.post("/qoe/snapshot")
async def save_qoe_snapshot_endpoint():
    """Save the current QofE as the prior snapshot for the next quarter's comparison."""
    from backend.engine.qoe import save_qoe_snapshot

    try:
        result = _engine_cache.get("qoe")
    except (FileNotFoundError, ValueError) as e:
        raise HTTPException(status_code=404, detail=str(e))

    save_qoe_snapshot(result)
    return {"status": "saved", "period": result["period"], "sustainability_score": result["summary"]["sustainability_score"]}


# ─── Data push ─────────────────────────────────────────────────────────


@router.post("/data/push/customer_profiles")
async def push_customer_profiles(body: dict):
    """Push updated customer profiles and invalidate the engine cache.

    Body JSON must contain at least one key matching *_customers with a
    non-empty list value (e.g. {"meridian_customers": [...], "cascadia_customers": [...]}).

    Overwrites data/customer_profiles.json and invalidates the engine cache
    so the next request recomputes cross-sell scores with the new data.
    """
    # --- Validate body has at least one *_customers key with a non-empty list ---
    customer_keys = {
        k: v for k, v in body.items()
        if k.endswith("_customers") and isinstance(v, list) and len(v) > 0
    }
    if not customer_keys:
        raise HTTPException(
            status_code=400,
            detail=(
                "Request body must contain at least one '*_customers' key "
                "with a non-empty list. Received keys: "
                + str(list(body.keys()))
            ),
        )

    # --- Write to data/customer_profiles.json ---
    profiles_path = _DATA_DIR / "customer_profiles.json"
    try:
        with open(profiles_path, "w") as f:
            json.dump(body, f, indent=2)
    except OSError as e:
        raise HTTPException(
            status_code=500,
            detail=(
                f"Failed to write customer profiles to {profiles_path}: {e}"
            ),
        )

    # --- Invalidate engine cache ---
    _engine_cache.invalidate()
    logger.info(
        "[reports] customer_profiles pushed (%d entity keys) — engine cache invalidated",
        len(customer_keys),
    )

    # --- Return summary ---
    customer_counts = {k: len(v) for k, v in customer_keys.items()}
    return {
        "status": "accepted",
        "customer_counts": customer_counts,
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }
