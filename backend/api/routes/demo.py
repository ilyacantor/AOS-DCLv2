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
_REPO_ROOT = Path(__file__).resolve().parents[3]
_GALLERY_PATH = _REPO_ROOT / "demo" / "glassbox_gallery.json"
# The agent-context arc writes its capture here. The headless arc runs the REAL
# ops (auth -> traverse -> act -> govern -> revoke) and writes the capture; this
# endpoint RENDERS that capture (replay-only, no live ops, no outcome logic).
_CAPTURES_DIR = _REPO_ROOT / "public" / "demo-captures"


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
            # Source of the request — who is asking (e.g. "FinOps Agent",
            # "Head of Engineering"). askerKind distinguishes agent vs human.
            "asker": q.get("asker"),
            "askerKind": q.get("askerKind"),
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


@router.get("/finops-arc")
def get_finops_arc() -> Dict[str, Any]:
    """Return the LATEST agent-context arc capture (headless finops_arc tool).

    The Agent Arc UI tab was removed — Glass Box is the only DCL demo surface —
    but the headless arc and its capture are retained as a regression / ground-
    truth tool (it runs the REAL auth -> traverse -> act -> govern -> revoke ops
    against DCL-MCP). This endpoint serves the latest capture for inspection
    (curl / tooling). `python -m demo.finops_arc` runs the ops and writes
    `public/demo-captures/finops_arc__<stamp>.json`; this endpoint parses and
    returns the most recent capture verbatim — RENDER-ONLY, synthesizes nothing.
    No silent fallback (A1): if no capture exists, 404 with the command to
    produce one. The identity pair (I2) is carried in the capture's `target`
    (tenant_id machine-only + entity_id business key).
    """
    if not _CAPTURES_DIR.exists():
        raise HTTPException(
            status_code=404,
            detail=(
                f"No demo-captures directory at {_CAPTURES_DIR}. This endpoint serves "
                "the headless arc capture; run `python -m demo.finops_arc` first to "
                "produce one."
            ),
        )
    captures = sorted(
        _CAPTURES_DIR.glob("finops_arc__*.json"), key=lambda p: p.stat().st_mtime
    )
    if not captures:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No finops_arc capture found in {_CAPTURES_DIR}. This endpoint serves "
                "the headless arc capture; run `python -m demo.finops_arc` first to "
                "produce one."
            ),
        )
    latest = captures[-1]
    try:
        capture = json.loads(latest.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=500,
            detail=f"finops_arc capture {latest.name} is not valid JSON: {exc}",
        ) from exc

    target = capture.get("target") or {}
    if not target.get("tenant_id") or not target.get("entity_id"):
        raise HTTPException(
            status_code=500,
            detail=(
                f"finops_arc capture {latest.name} is missing tenant_id/entity_id in "
                "its target — refusing to serve a capture without the identity pair (I2)."
            ),
        )
    if not capture.get("beats"):
        raise HTTPException(
            status_code=500,
            detail=f"finops_arc capture {latest.name} has no beats; refusing to serve an empty arc.",
        )

    # Provenance for the replay tag — which capture file is on screen.
    capture["_capture_file"] = latest.name
    return capture
