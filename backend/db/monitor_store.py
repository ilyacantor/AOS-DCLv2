"""MonitorStore — data access for the monitor_schedule table (Gate 3B D1).

Stores durable job state for the APScheduler-backed structural drift monitor.
I1: no field named run_id in any response shape.
"""

from typing import Optional

from backend.core.db import get_connection
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

_JOB_COLS = (
    "job_name, interval_seconds, enabled, last_run_at, last_status, last_detail, updated_at"
)


def _row_to_job(row: tuple) -> dict:
    job_name, interval_seconds, enabled, last_run_at, last_status, last_detail, updated_at = row
    return {
        "job_name": job_name,
        "interval_seconds": interval_seconds,
        "enabled": enabled,
        "last_run_at": last_run_at.isoformat() if last_run_at else None,
        "last_status": last_status,
        "last_detail": last_detail,
        "updated_at": updated_at.isoformat() if updated_at else None,
    }


class MonitorStore:
    def list_jobs(self) -> list[dict]:
        """Return all rows from monitor_schedule, ordered by job_name."""
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT {_JOB_COLS} FROM monitor_schedule ORDER BY job_name")
                return [_row_to_job(r) for r in cur.fetchall()]

    def get_job(self, job_name: str) -> Optional[dict]:
        """Return one job row by name, or None if not found."""
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT {_JOB_COLS} FROM monitor_schedule WHERE job_name = %s",
                    (job_name,),
                )
                row = cur.fetchone()
        return _row_to_job(row) if row else None

    def set_enabled(self, job_name: str, enabled: bool) -> Optional[dict]:
        """Flip enabled flag. Returns updated row, or None if job_name not found."""
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE monitor_schedule SET enabled = %s, updated_at = now() "
                    f"WHERE job_name = %s RETURNING {_JOB_COLS}",
                    (enabled, job_name),
                )
                row = cur.fetchone()
            conn.commit()
        return _row_to_job(row) if row else None

    def record_run(self, job_name: str, status: str, detail: str) -> None:
        """Record the outcome of a sweep into last_run_at / last_status / last_detail."""
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE monitor_schedule "
                    "SET last_run_at = now(), last_status = %s, last_detail = %s, updated_at = now() "
                    "WHERE job_name = %s",
                    (status, detail[:2000] if detail else detail, job_name),
                )
            conn.commit()
