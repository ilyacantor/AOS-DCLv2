"""Demo pipeline monitor — DCL metrics endpoint.

GET /api/dcl/monitor/metrics — read-only ingest stats for the AAM-served
pipeline monitor page (app/routers/monitor.py in the aam repo).

Additive and read-only: no existing route is modified, no DDL is run,
nothing is written.

Triple figures are read from the ingest_log table — one row per ingest
call, carrying triples_written and a source_systems array. ingest_log is
the right surface here for two reasons: (1) the store-rebuild guardrail
(migrations 014-016) reserves direct triple-store table access for the
whitelisted data layer, and a new route file is not on that whitelist;
(2) ingest_log is small — one row per ingest, not per triple — so this
endpoint stays fast even when the triple store itself is large. The
figures are therefore ingest-cumulative: triples DCL has received per
source, which is the pipeline-flow reading a monitor wants for the DCL node.

CORS: the response carries an explicit Access-Control-Allow-Origin for the
AAM-served origin (MONITOR_ALLOWED_ORIGIN env, default http://localhost:8002).
The app-wide CORSMiddleware is left untouched — this scopes the cross-origin
allowance to this one endpoint without affecting any other route.

p95 query latency is reported null (the page renders "n/a"): DCL records no
per-call query latency. See dcl_deferred_work.md #30.
"""

import os

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from backend.core.db import get_connection
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

router = APIRouter(tags=["Monitor"])

# The AAM-served monitor page's origin. The metrics endpoint echoes this as
# its Access-Control-Allow-Origin so the cross-origin poll succeeds —
# scoped to this endpoint alone, no other route touched.
_ALLOWED_ORIGIN = os.getenv("MONITOR_ALLOWED_ORIGIN", "http://localhost:8002")


@router.get("/api/dcl/monitor/metrics")
def dcl_monitor_metrics() -> JSONResponse:
    """DCL panel for the pipeline monitor: triples ingested + per-source
    split, ingest batches, push-success rate. Read-only, all from ingest_log.

    A DB failure propagates — 503 via the app-wide PoolExhausted handler,
    else 500. The monitor page treats either as unreachable: gray dot, last
    values kept. A half-populated panel would be a silent fallback (A1)."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Per-source triples written. unnest expands the source_systems
            # array; a single ingest row may name more than one source.
            cur.execute(
                "SELECT s AS source_system, COALESCE(SUM(triples_written), 0) "
                "FROM ingest_log, unnest(source_systems) AS s "
                "GROUP BY s"
            )
            by_source = {(r[0] or "unknown"): int(r[1]) for r in cur.fetchall()}
            # Total written — computed without unnest so a multi-source
            # ingest row is counted once, not once per source.
            cur.execute("SELECT COALESCE(SUM(triples_written), 0) FROM ingest_log")
            total = int((cur.fetchone() or [0])[0] or 0)
            # Ingest batches = distinct ingest runs.
            cur.execute("SELECT COUNT(DISTINCT run_id) FROM ingest_log")
            active_batches = int((cur.fetchone() or [0])[0] or 0)
            # Push success = triples written / received over recent ingests.
            cur.execute(
                "SELECT COALESCE(SUM(triples_written), 0), "
                "       COALESCE(SUM(triples_received), 0) "
                "FROM ingest_log WHERE created_at >= now() - interval '24 hours'"
            )
            written, received = cur.fetchone()

    def _source_total(needle: str) -> int:
        """Sum triples for every source whose name contains `needle`
        — robust to 'NetSuite' / 'netsuite' / 'sage_intacct' casing."""
        return sum(v for k, v in by_source.items() if needle in k.lower())

    push_pct = None
    if received:
        push_pct = round(int(written) / int(received) * 100, 1)

    body = {
        "service": "dcl",
        "triples": {
            "total": total,
            "netsuite": _source_total("netsuite"),
            "sage": _source_total("sage"),
            "by_source": by_source,
        },
        "active_batches": active_batches,
        "push_success_pct": push_pct,
        # DCL records no per-call query latency — see dcl_deferred_work.md #30.
        "p95_query_ms": None,
    }
    return JSONResponse(
        content=body,
        headers={"Access-Control-Allow-Origin": _ALLOWED_ORIGIN, "Vary": "Origin"},
    )
