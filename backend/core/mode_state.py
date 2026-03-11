"""
DCL Mode State - Tracks the current active data mode.

NLQ relies on this to know which semantic catalog to use without
needing to track mode separately.

Multi-worker safe: writes through to Redis so all Gunicorn workers
agree on the current mode.  Falls back to in-memory if Redis is
unavailable (same behavior as before this change).
"""

from typing import Literal, Optional
from datetime import datetime

from pydantic import BaseModel

from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

_REDIS_KEY = "dcl:mode_state"


class ModeState(BaseModel):
    """Current DCL mode state.

    data_mode values:
      "Demo"    — no live data; serving from static fact_base.json
      "Farm"    — triggered via POST /api/dcl/run with mode=Farm
      "AAM"     — triggered via POST /api/dcl/run with mode=AAM
      "Ingest"  — live data exists in the ingest buffer (auto-promoted from Demo)
    """
    data_mode: Literal["Demo", "Farm", "AAM", "Ingest"] = "Demo"
    run_mode: Literal["Dev", "Prod"] = "Dev"
    last_updated: Optional[str] = None
    last_run_id: Optional[str] = None


_current_state = ModeState()


def _get_redis():
    """Return the shared Redis client (or None if unavailable)."""
    from backend.core.redis_client import get_redis
    return get_redis()


def get_current_mode() -> ModeState:
    """Get the current DCL mode state.

    Reads from Redis first so all workers see the same mode.
    Falls back to in-memory if Redis is unavailable.
    """
    global _current_state
    r = _get_redis()
    if r:
        try:
            raw = r.get(_REDIS_KEY)
            if raw:
                _current_state = ModeState.model_validate_json(raw)
        except Exception as e:
            logger.warning(f"[ModeState] Redis read failed, using in-memory: {e}")
    return _current_state


def set_current_mode(
    data_mode: Literal["Demo", "Farm", "AAM", "Ingest"],
    run_mode: Literal["Dev", "Prod"] = "Dev",
    run_id: Optional[str] = None
) -> ModeState:
    """Update the current DCL mode state.

    Writes through to Redis so all workers see the change immediately.
    """
    global _current_state
    _current_state = ModeState(
        data_mode=data_mode,
        run_mode=run_mode,
        last_updated=datetime.utcnow().isoformat() + "Z",
        last_run_id=run_id
    )
    r = _get_redis()
    if r:
        try:
            r.set(_REDIS_KEY, _current_state.model_dump_json())
        except Exception as e:
            logger.warning(f"[ModeState] Redis persist failed: {e}")
    return _current_state


def get_data_mode() -> Literal["Demo", "Farm", "AAM", "Ingest"]:
    """Get just the current data mode."""
    return get_current_mode().data_mode
