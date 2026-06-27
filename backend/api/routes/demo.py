"""
Glass Box commercial demo — presenter-paced story API.

RAILS MODE (contextOS_blueprint_v1.5 §0): serves preselected, authored stories
from demo/glassbox_gallery.json. NOT the live engine. Each question is a STORY:
plain-English beats the UI reveals one click at a time (presenter controls the
pace), the 'aha' beat flagged link:true, ending in a plain answer. Beats may
carry a `record` (raw row) shown in the audit drawer. Every response carries the
tenant_id + entity_id identity pair (I2).

LIVE-ENGINE SEAM: replace _load_gallery() with a client that asks contextOS for
the same beat structure. No silent fallback (A1): a live failure raises — it
never falls back to this replay.
"""

import json
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, Query

from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/api/demo", tags=["demo"])

# routes -> api -> backend -> <repo root>
_GALLERY_PATH = Path(__file__).resolve().parents[3] / "demo" / "glassbox_gallery.json"


def _load_gallery() -> Dict[str, Any]:
    """Load the preselected demo gallery. Fail loudly — no silent fallback (A1)."""
    if not _GALLERY_PATH.exists():
        raise HTTPException(
            status_code=500,
            detail=(
                f"Glass Box gallery fixture missing at {_GALLERY_PATH}. This "
                "endpoint replays demo/glassbox_gallery.json; it does not "
                "synthesize data. Restore it or wire the live contextOS engine."
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
        return questions[0]
    for q in questions:
        if q.get("id") == qid:
            return q
    known = [q.get("id") for q in questions]
    raise HTTPException(status_code=404, detail=f"Glass Box question '{qid}' not found. Known ids: {known}")


@router.get("/questions")
def list_questions() -> Dict[str, Any]:
    """Return the preselected gallery for the picker (client groups by category)."""
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


@router.get("/trace")
def get_trace(q: Optional[str] = Query(default=None)) -> Dict[str, Any]:
    """Return one question's full story for the presenter-paced reveal.

    The UI fetches this once and advances through `story.beats` on click. The
    response carries the tenant_id + entity_id identity pair (I2).
    """
    gallery = _load_gallery()
    question = _find_question(gallery, q)

    if not question.get("tenant_id") or not question.get("entity_id"):
        raise HTTPException(
            status_code=500,
            detail=f"Glass Box question '{question.get('id')}' missing tenant_id/entity_id (I2).",
        )
    story = question.get("story")
    if not story or not story.get("beats"):
        raise HTTPException(
            status_code=500,
            detail=f"Glass Box question '{question.get('id')}' has no story beats; refusing to serve an empty trace.",
        )
    return question
