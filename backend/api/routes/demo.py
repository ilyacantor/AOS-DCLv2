"""
Glass Box commercial demo — Server-Sent Events trace stream.

RAILS MODE (contextOS_blueprint_v1.5 §0): this endpoint REPLAYS a captured,
verified 3-hop trace from the lab. It is NOT the live engine. The values are a
replay artifact (demo/glassbox_trace.json), not computed here, and the stream
labels every frame with `"replay": true` so the UI never presents it as a live
computation to a buyer while contextOS is building.

LIVE-ENGINE SEAM: when contextOS is extracted, replace `_load_trace()` +
`_event_stream()` with a client that proxies contextOS's own SSE stream. The
event contract (stage names INTAKE/RETRIEVE/PRUNE/COMPUTE/DONE + identity pair
on every frame) is the integration boundary. There is NO silent fallback: if
the live stream later fails, raise — do not fall back to this replay.
"""

import asyncio
import json
from pathlib import Path
from typing import Any, Dict, Iterator

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/api/demo", tags=["demo"])

# routes -> api -> backend -> <repo root>
_TRACE_PATH = Path(__file__).resolve().parents[3] / "demo" / "glassbox_trace.json"

_SSE_HEADERS = {
    "Cache-Control": "no-cache, no-transform",
    "Connection": "keep-alive",
    # Defeat reverse-proxy buffering so frames arrive in real time.
    "X-Accel-Buffering": "no",
}


def _load_trace() -> Dict[str, Any]:
    """Load the captured replay trace. Fail loudly — no silent fallback (A1)."""
    if not _TRACE_PATH.exists():
        raise HTTPException(
            status_code=500,
            detail=(
                f"Glass Box demo trace fixture missing at {_TRACE_PATH}. "
                "This endpoint replays demo/glassbox_trace.json; it does not "
                "synthesize data. Restore the fixture or wire the live "
                "contextOS stream."
            ),
        )
    try:
        trace = json.loads(_TRACE_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Glass Box demo trace fixture is not valid JSON: {exc}",
        ) from exc

    missing = [k for k in ("tenant_id", "entity_id", "events") if not trace.get(k)]
    if missing:
        # Identity pair must be present on every frame (I2). A fixture without
        # it is broken — surface it, do not stream a degraded response.
        raise HTTPException(
            status_code=500,
            detail=(
                "Glass Box demo trace fixture missing required keys "
                f"{missing}; refusing to stream an identity-less trace (I2)."
            ),
        )
    return trace


def _frames(trace: Dict[str, Any]) -> Iterator[tuple[str, float]]:
    """Yield (sse_frame, delay_seconds) for each event, identity stamped."""
    tenant_id = trace["tenant_id"]
    entity_id = trace["entity_id"]
    demo_trace_id = trace.get("demo_trace_id")
    for ev in trace["events"]:
        delay = float(ev.get("delay_ms", 1200)) / 1000.0
        payload = {k: v for k, v in ev.items() if k != "delay_ms"}
        # I2: tenant_id + entity_id on every frame. `replay` keeps the UI honest.
        payload.update(
            tenant_id=tenant_id,
            entity_id=entity_id,
            demo_trace_id=demo_trace_id,
            replay=True,
        )
        frame = f"event: {payload['stage']}\ndata: {json.dumps(payload)}\n\n"
        yield frame, delay


@router.get("/stream-trace")
async def stream_trace() -> StreamingResponse:
    """Stream the captured 3-hop Glass Box trace as paced SSE events.

    Emits typed events INTAKE -> RETRIEVE -> PRUNE -> COMPUTE -> DONE, paced by
    each event's delay_ms so the X-Ray canvas can animate. Every frame carries
    the tenant_id + entity_id identity pair (I2).
    """
    trace = _load_trace()  # raises 500 loudly if the fixture is gone/broken

    async def event_stream():
        # Open-comment line nudges proxies to flush the connection immediately.
        yield ": glassbox replay stream open\n\n"
        for frame, delay in _frames(trace):
            yield frame
            await asyncio.sleep(delay)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers=_SSE_HEADERS,
    )
