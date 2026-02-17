"""
Deprecated / moved endpoint stubs — return 410 GONE.

These endpoints existed in earlier versions but have been migrated
to AOS-NLQ or AOS-AAM.  The 410 responses give callers a clear
signal with a redirect hint.
"""

from fastapi import APIRouter, HTTPException

router = APIRouter(tags=["Deprecated"])


# ---------------------------------------------------------------------------
# Moved to AAM
# ---------------------------------------------------------------------------

@router.post("/api/ingest/provision")
@router.get("/api/ingest/provision")
def ingest_provision_gone():
    """Ingest pipeline moved to AAM."""
    raise HTTPException(status_code=410, detail={"error": "MOVED_TO_AAM"})


@router.get("/api/ingest/config")
def ingest_config_gone():
    """Ingest pipeline moved to AAM."""
    raise HTTPException(status_code=410, detail={"error": "MOVED_TO_AAM"})


@router.get("/api/ingest/telemetry")
def ingest_telemetry_gone():
    """Ingest pipeline moved to AAM."""
    raise HTTPException(status_code=410, detail={"error": "MOVED_TO_AAM"})


# ---------------------------------------------------------------------------
# Moved to AOS-NLQ
# ---------------------------------------------------------------------------

@router.get("/api/nlq/ask")
def nlq_ask_moved():
    """NLQ functionality moved to AOS-NLQ."""
    raise HTTPException(status_code=410, detail={"error": "MOVED_TO_AOS_NLQ", "note": "Use AOS-NLQ service for NLQ queries"})


@router.post("/api/nlq/ask")
def nlq_ask_post_moved():
    """NLQ functionality moved to AOS-NLQ."""
    raise HTTPException(status_code=410, detail={"error": "MOVED_TO_AOS_NLQ", "note": "Use AOS-NLQ service for NLQ queries"})


@router.get("/api/bll/{path:path}")
def bll_moved(path: str):
    """BLL functionality moved to AOS-NLQ."""
    raise HTTPException(status_code=410, detail={"error": "MOVED_TO_AOS_NLQ", "note": "Use AOS-NLQ service for BLL operations"})


@router.post("/api/bll/{path:path}")
def bll_post_moved(path: str):
    """BLL functionality moved to AOS-NLQ."""
    raise HTTPException(status_code=410, detail={"error": "MOVED_TO_AOS_NLQ", "note": "Use AOS-NLQ service for BLL operations"})


@router.get("/api/execute")
def execute_moved():
    """Execute functionality moved to AOS-NLQ."""
    raise HTTPException(status_code=410, detail={"error": "MOVED_TO_AOS_NLQ", "note": "Use AOS-NLQ service for query execution"})


@router.post("/api/execute")
def execute_post_moved():
    """Execute functionality moved to AOS-NLQ."""
    raise HTTPException(status_code=410, detail={"error": "MOVED_TO_AOS_NLQ", "note": "Use AOS-NLQ service for query execution"})
