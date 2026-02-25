"""
DCL Mode State - Tracks the current active data mode.

NLQ relies on this to know which semantic catalog to use without
needing to track mode separately.
"""

from typing import Literal, Optional
from datetime import datetime
from pydantic import BaseModel


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


def get_current_mode() -> ModeState:
    """Get the current DCL mode state."""
    return _current_state


def set_current_mode(
    data_mode: Literal["Demo", "Farm", "AAM", "Ingest"],
    run_mode: Literal["Dev", "Prod"] = "Dev",
    run_id: Optional[str] = None
) -> ModeState:
    """Update the current DCL mode state."""
    global _current_state
    _current_state = ModeState(
        data_mode=data_mode,
        run_mode=run_mode,
        last_updated=datetime.utcnow().isoformat() + "Z",
        last_run_id=run_id
    )
    return _current_state


def get_data_mode() -> Literal["Demo", "Farm", "AAM", "Ingest"]:
    """Get just the current data mode."""
    return _current_state.data_mode
