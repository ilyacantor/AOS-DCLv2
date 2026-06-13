"""Monitor schedule API (Gate 3B D1).

GET  /api/dcl/monitor/schedule                         — list all jobs
POST /api/dcl/monitor/schedule/{job_name}/pause        — pause a job (enabled=false)
POST /api/dcl/monitor/schedule/{job_name}/resume       — resume a job (enabled=true)
POST /api/dcl/monitor/schedule/{job_name}/run-now      — trigger one sweep synchronously

Identity: schedule is global (not tenant-scoped) — no tenant_id on these routes.
I1: no run_id in any response.
"""

from fastapi import APIRouter, HTTPException

from backend.db.monitor_store import MonitorStore
from backend.api.scheduler import get_scheduler
from backend.api.drift_monitor import (
    run_structural_drift_sweep, drift_job,
    run_value_drift_sweep, value_drift_job,
)
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

router = APIRouter(tags=["Monitor Schedule"])

_store = MonitorStore()

# Scheduler fires these wrappers (which call the sweep AND record outcomes).
_JOB_FUNCTIONS = {
    "structural_drift": drift_job,
    "value_drift": value_drift_job,
}

# Pure sweep functions — used by run-now to return a result dict.
_SWEEP_FUNCTIONS = {
    "structural_drift": run_structural_drift_sweep,
    "value_drift": run_value_drift_sweep,
}


def _get_job_or_404(job_name: str) -> dict:
    job = _store.get_job(job_name)
    if job is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Monitor job '{job_name}' not found in monitor_schedule. "
                f"Known jobs: {sorted(_JOB_FUNCTIONS)}"
            ),
        )
    return job


@router.get("/api/dcl/monitor/schedule")
def schedule_list():
    """List all jobs in monitor_schedule with their current state."""
    jobs = _store.list_jobs()
    return {"jobs": jobs, "count": len(jobs)}


@router.post("/api/dcl/monitor/schedule/{job_name}/pause")
def schedule_pause(job_name: str):
    """Pause a job: set enabled=false in monitor_schedule and remove it from the
    APScheduler instance. The job will not fire until resumed. Pause survives a
    backend restart because it is stored in the DB."""
    _get_job_or_404(job_name)
    updated = _store.set_enabled(job_name, False)

    sched = get_scheduler()
    if sched is not None:
        try:
            sched.remove_job(job_name)
            logger.info("[monitor-schedule] paused APScheduler job '%s'", job_name)
        except Exception:
            # Job might not be registered (already paused or never added). Fine.
            pass

    return {"job_name": job_name, "action": "paused", "job": updated}


@router.post("/api/dcl/monitor/schedule/{job_name}/resume")
def schedule_resume(job_name: str):
    """Resume a paused job: set enabled=true in monitor_schedule and add it back
    to the APScheduler instance at its configured interval."""
    job = _get_job_or_404(job_name)
    if job_name not in _JOB_FUNCTIONS:
        raise HTTPException(
            status_code=422,
            detail=f"No sweep function registered for job '{job_name}'.",
        )

    updated = _store.set_enabled(job_name, True)

    sched = get_scheduler()
    if sched is not None:
        fn = _JOB_FUNCTIONS[job_name]
        # Add the job if not already present; replace if it somehow exists.
        try:
            sched.remove_job(job_name)
        except Exception:
            pass
        sched.add_job(
            fn,
            trigger="interval",
            seconds=job["interval_seconds"],
            id=job_name,
            replace_existing=True,
            max_instances=1,
        )
        logger.info(
            "[monitor-schedule] resumed APScheduler job '%s' at %ds interval",
            job_name, job["interval_seconds"],
        )

    return {"job_name": job_name, "action": "resumed", "job": updated}


@router.post("/api/dcl/monitor/schedule/{job_name}/run-now")
def schedule_run_now(job_name: str):
    """Trigger one sweep synchronously. NOT a test-only backdoor: calls the
    exact same sweep function the scheduler fires (run_structural_drift_sweep),
    then records the outcome in monitor_schedule (same as drift_job does).

    Returns the sweep summary (entities_scanned, drift_findings,
    proposals_filed, proposals_deduped). On error: 500 with the informative
    message from the sweep (A1).
    """
    _get_job_or_404(job_name)
    sweep_fn = _SWEEP_FUNCTIONS.get(job_name)
    if sweep_fn is None:
        raise HTTPException(
            status_code=422,
            detail=f"No sweep function registered for job '{job_name}'.",
        )

    try:
        result = sweep_fn()
        detail = (
            f"scanned={result['entities_scanned']} "
            f"findings={result['drift_findings']} "
            f"filed={result['proposals_filed']} "
            f"deduped={result['proposals_deduped']}"
        )
        _store.record_run(job_name, "ok", detail)
    except RuntimeError as exc:
        logger.error("[monitor-schedule] run-now '%s' completed with errors: %s", job_name, exc)
        _store.record_run(job_name, "error", str(exc)[:2000])
        raise HTTPException(
            status_code=500,
            detail=(
                f"Sweep '{job_name}' completed with errors: {exc}. "
                f"Check DCL logs for per-entity details."
            ),
        )
    except Exception as exc:
        logger.error(
            "[monitor-schedule] run-now '%s' unexpected error: %s", job_name, exc, exc_info=True
        )
        _store.record_run(job_name, "error", f"{type(exc).__name__}: {exc}"[:2000])
        raise HTTPException(
            status_code=500,
            detail=f"Sweep '{job_name}' failed: {type(exc).__name__}: {exc}",
        )

    return {
        "job_name": job_name,
        "status": "ok",
        **result,
    }
