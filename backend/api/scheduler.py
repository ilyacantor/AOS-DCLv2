"""APScheduler singleton for the DCL monitor schedule (Gate 3B D1).

get_scheduler() / set_scheduler() decouple the lifespan init (main.py)
from the route handlers (monitor_schedule.py) without a circular import.
The scheduler is an AsyncIOScheduler — sync job functions run in the
event loop's thread executor (non-blocking).
"""

from __future__ import annotations
from typing import Optional

try:
    from apscheduler.schedulers.asyncio import AsyncIOScheduler as _Sched
except ImportError as _e:
    raise ImportError(
        "APScheduler is required for the monitor schedule (Gate 3B D1). "
        "Install with: pip install 'apscheduler>=3.10,<4.0'"
    ) from _e

_scheduler: Optional[_Sched] = None


def get_scheduler() -> Optional[_Sched]:
    return _scheduler


def set_scheduler(s: _Sched) -> None:
    global _scheduler
    _scheduler = s
