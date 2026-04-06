"""
Shared helpers for v2 route files — tenant_id and run_id resolution.

Every v2 endpoint that needs a tenant_id or run_id must use these helpers.
No hardcoded UUIDs anywhere in route handlers.

Resolution order (DCL — SE only):
1. Explicit query parameter (?tenant_id=...)
2. Most recent active tenant from semantic_triples
3. HTTP 400 with actionable error message
"""

from fastapi import HTTPException

from backend.core.db import get_connection
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

# Concept prefixes that identify financial statement data.  Used by
# domain_hint='financial' to prefer runs containing P&L / BS / CF triples
# over non-financial runs (e.g. HR workforce data).
_FINANCIAL_PREFIXES = [
    "revenue.%", "cogs.%", "opex.%", "pnl.%",
    "asset.%", "liability.%", "equity.%",
]


def resolve_tenant_id(tenant_id: str | None) -> str:
    """Resolve tenant_id from explicit param or latest tenant in PG.

    Never returns a hardcoded default. Raises HTTP 400 if unresolvable.
    """
    if tenant_id:
        return tenant_id

    latest = _get_latest_tenant()
    if latest:
        return latest

    raise HTTPException(
        status_code=400,
        detail=(
            "No tenant_id provided and no tenants found "
            "in semantic_triples. Ingest data first or pass ?tenant_id= explicitly."
        ),
    )


def resolve_run_id(
    run_id: str | None, tenant_id: str, domain_hint: str | None = None,
) -> str:
    """Resolve run_id from explicit param or most recent active run.

    Never returns a hardcoded default. Raises HTTP 400 if unresolvable.
    """
    if run_id:
        return run_id

    latest = _get_latest_run(tenant_id, domain_hint=domain_hint)
    if latest:
        return latest

    raise HTTPException(
        status_code=400,
        detail=(
            f"No run_id provided and no active runs found for tenant_id='{tenant_id}' "
            f"in semantic_triples. Ingest data first or pass ?run_id= explicitly."
        ),
    )


def resolve_tenant_and_run(
    tenant_id: str | None,
    run_id: str | None,
    domain_hint: str | None = None,
) -> tuple[str, str]:
    """Resolve both tenant_id and run_id. Convenience wrapper.

    Pass domain_hint='financial' from financial report endpoints so run
    resolution prefers runs containing P&L/BS/CF triples.
    """
    tid = resolve_tenant_id(tenant_id)
    rid = resolve_run_id(run_id, tid, domain_hint=domain_hint)
    return tid, rid


def _get_latest_tenant() -> str | None:
    """Get the most recent tenant_id from semantic_triples."""
    sql = """
        SELECT tenant_id
        FROM semantic_triples
        WHERE is_active = true
        ORDER BY created_at DESC
        LIMIT 1
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            if row is None:
                return None
            return str(row[0])


def _get_latest_run(
    tenant_id: str, domain_hint: str | None = None,
) -> str | None:
    """Get the primary run_id for a tenant from semantic_triples.

    Picks the run_id with the most active triples, not just the newest
    created_at. This prevents small supplementary runs (e.g.
    HR imports) from shadowing the main financial ingest run.

    When domain_hint='financial', only counts triples whose concept
    matches financial domain prefixes (revenue.%, cogs.%, etc.).  This
    prevents non-financial runs (e.g. 124K HR triples) from being
    selected when a financial report endpoint needs the financial run.
    """
    if domain_hint == "financial":
        sql = """
            SELECT run_id
            FROM semantic_triples
            WHERE tenant_id = %s AND is_active = true
              AND run_id = (SELECT current_run_id FROM tenant_runs WHERE tenant_id = %s)
              AND concept LIKE ANY(%s)
            GROUP BY run_id
            ORDER BY COUNT(*) DESC
            LIMIT 1
        """
        params: list = [tenant_id, tenant_id, _FINANCIAL_PREFIXES]
    else:
        sql = """
            SELECT run_id
            FROM semantic_triples
            WHERE tenant_id = %s AND is_active = true
              AND run_id = (SELECT current_run_id FROM tenant_runs WHERE tenant_id = %s)
            GROUP BY run_id
            ORDER BY COUNT(*) DESC
            LIMIT 1
        """
        params = [tenant_id, tenant_id]

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            if row is None:
                return None
            return str(row[0])
