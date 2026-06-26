"""
Glass Box commercial demo — Server-Sent Events trace stream (gallery).

RAILS MODE (contextOS_blueprint_v1.5 §0): replays preselected, verticalized
captured traces from demo/glassbox_gallery.json. NOT the live engine. Two
capabilities per question:
  - 'conflict'  — source-authority prune (two sources disagree; the
                  unauthorized one is dropped before compute).
  - 'traversal' — relationship discovery (the hero): assemble an answer no
                  single source holds by hopping non-obvious relationships;
                  the hard edge is flagged discovered:true.
Every frame carries the tenant_id+entity_id identity pair (I2) and replay:true.

LIVE-ENGINE SEAM: replace _load_gallery()/_event_stream() with a client that
proxies contextOS's own SSE stream. No silent fallback (A1): a live failure
raises — it never falls back to this replay.
"""

import asyncio
import json
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse

from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/api/demo", tags=["demo"])

# routes -> api -> backend -> <repo root>
_GALLERY_PATH = Path(__file__).resolve().parents[3] / "demo" / "glassbox_gallery.json"

_SSE_HEADERS = {
    "Cache-Control": "no-cache, no-transform",
    "Connection": "keep-alive",
    "X-Accel-Buffering": "no",  # defeat reverse-proxy buffering
}


def _load_gallery() -> Dict[str, Any]:
    """Load the preselected demo gallery. Fail loudly — no silent fallback (A1)."""
    if not _GALLERY_PATH.exists():
        raise HTTPException(
            status_code=500,
            detail=(
                f"Glass Box gallery fixture missing at {_GALLERY_PATH}. This "
                "endpoint replays demo/glassbox_gallery.json; it does not "
                "synthesize data. Restore it or wire the live contextOS stream."
            ),
        )
    try:
        gallery = json.loads(_GALLERY_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Glass Box gallery fixture is not valid JSON: {exc}",
        ) from exc
    if not gallery.get("questions"):
        raise HTTPException(
            status_code=500,
            detail="Glass Box gallery has no questions; refusing to serve an empty gallery.",
        )
    return gallery


def _find_question(gallery: Dict[str, Any], qid: Optional[str]) -> Dict[str, Any]:
    questions = gallery["questions"]
    if qid is None:
        return questions[0]  # default keeps the bare /stream-trace working
    for q in questions:
        if q.get("id") == qid:
            return q
    known = [q.get("id") for q in questions]
    raise HTTPException(status_code=404, detail=f"Glass Box question '{qid}' not found. Known ids: {known}")


@router.get("/questions")
def list_questions() -> Dict[str, Any]:
    """Return the preselected gallery for the picker (client groups by vertical)."""
    gallery = _load_gallery()
    items = [
        {
            "id": q["id"],
            "category": q.get("category", "Operate with Confidence"),
            "capability": q.get("capability", "traversal"),
            "question": q["question"],
            "entity_id": q.get("entity_id"),
        }
        for q in gallery["questions"]
    ]
    return {"questions": items}


def _frames(question: Dict[str, Any]):
    """Yield (sse_frame, delay_seconds) for each event, identity stamped (I2)."""
    tenant_id = question["tenant_id"]
    entity_id = question["entity_id"]
    qid = question["id"]
    capability = question.get("capability", "traversal")
    for ev in question["events"]:
        delay = float(ev.get("delay_ms", 1200)) / 1000.0
        payload = {k: v for k, v in ev.items() if k != "delay_ms"}
        payload.update(
            tenant_id=tenant_id,
            entity_id=entity_id,
            question_id=qid,
            capability=capability,
            replay=True,
        )
        yield f"event: {payload['stage']}\ndata: {json.dumps(payload)}\n\n", delay


@router.get("/stream-trace")
async def stream_trace(q: Optional[str] = Query(default=None)) -> StreamingResponse:
    """Stream a selected gallery question as paced SSE events.

    Conflict questions emit INTAKE->RETRIEVE->PRUNE->COMPUTE->DONE; traversal
    questions emit INTAKE->TRAVERSE(*)->COMPUTE->DONE. Every frame carries the
    tenant_id+entity_id identity pair (I2).
    """
    gallery = _load_gallery()
    question = _find_question(gallery, q)

    if not question.get("tenant_id") or not question.get("entity_id"):
        # Identity pair must be present (I2) — fail loud, never stream identity-less.
        raise HTTPException(
            status_code=500,
            detail=f"Glass Box question '{question.get('id')}' missing tenant_id/entity_id (I2).",
        )

    async def event_stream():
        yield ": glassbox replay stream open\n\n"
        for frame, delay in _frames(question):
            yield frame
            await asyncio.sleep(delay)

    return StreamingResponse(event_stream(), media_type="text/event-stream", headers=_SSE_HEADERS)
