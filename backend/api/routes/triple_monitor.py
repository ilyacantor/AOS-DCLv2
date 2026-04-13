"""
Triple monitoring endpoints.

GET /api/dcl/triples/overview         — high-level summary
GET /api/dcl/triples/runs             — ingest run list
GET /api/dcl/triples/identity-checks  — accounting identity verification
GET /api/dcl/triples/browse           — paginated triple browser
POST /api/dcl/triples/browse-batch    — batch browse (multiple domains, one SQL)
GET /api/dcl/triples/engagement       — engagement state
GET /api/dcl/triples/resolution-summary — resolution workspace stats
GET /api/dcl/triples/persona-stats    — per-persona stats from triples
"""

import json
import os
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import yaml
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from typing import List, Optional

from backend.db.triple_store import TripleStore
from psycopg2 import sql as pgsql
from backend.core.db import get_connection, PoolExhausted
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)


def _entity_display_name(entity_id: str) -> str:
    """Human-readable display name from entity_id via title-case transform."""
    if not entity_id:
        return entity_id
    return entity_id.replace("_", " ").title()



router = APIRouter(tags=["Triple Monitor"])

_triple_store = TripleStore()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _serialize_value(val):
    """Make a value JSON-serializable."""
    if isinstance(val, Decimal):
        return float(val)
    if isinstance(val, datetime):
        return val.isoformat()
    if isinstance(val, bytes):
        return val.decode("utf-8", errors="replace")
    return val


def _serialize_row(row: dict) -> dict:
    """Serialize all values in a row dict for JSON response."""
    return {k: _serialize_value(v) for k, v in row.items()}


# ---------------------------------------------------------------------------
# GET /api/dcl/entities
# ---------------------------------------------------------------------------

@router.get("/api/dcl/entities")
def list_entities():
    """Return distinct live entities from tenant_runs with triple counts and recency.

    Post–store-rebuild, run_row_count on tenant_runs is the authoritative
    per-(tenant, entity) count — no semantic_triples scan needed.
    """
    sql = (
        "SELECT tenant_id, entity_id, run_row_count AS triple_count, updated_at AS latest_ingest "
        "FROM tenant_runs ORDER BY updated_at DESC"
    )

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
    except PoolExhausted as e:
        raise HTTPException(
            status_code=503,
            detail=f"DCL database pool exhausted — too many concurrent requests. {e}",
        )
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))

    entities = []
    for i, (tenant_id, entity_id, triple_count, latest_ingest) in enumerate(rows):
        entities.append({
            "tenant_id": str(tenant_id),
            "entity_id": entity_id,
            "display_name": _entity_display_name(entity_id),
            "triple_count": triple_count,
            "latest_ingest": latest_ingest.isoformat() if latest_ingest else None,
            "is_most_recent": i == 0,
        })

    return {"entities": entities}


# ---------------------------------------------------------------------------
# GET /api/dcl/triples/overview
# ---------------------------------------------------------------------------

@router.get("/api/dcl/triples/overview")
def triples_overview(
    source_run_tag: Optional[str] = Query(None, description="Filter by Farm-originated source_run_tag"),
    tenant_id: Optional[str] = Query(None, description="Filter by tenant_id (deal scope)"),
):
    """High-level summary of the triple store.

    Optional filters narrow results:
    - tenant_id: scope to a single deal/tenant (required for multi-tenant accuracy)
    - source_run_tag: scope to triples from a specific Farm run
    """
    clauses: list[str] = []
    params: list = []
    if tenant_id:
        clauses.append("tenant_id = %s")
        params.append(tenant_id)
    if source_run_tag:
        clauses.append("source_run_tag = %s")
        params.append(source_run_tag)

    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""

    sql_total = f"SELECT COUNT(*) FROM current_triples{where}"
    sql_entities = (
        f"SELECT entity_id, COUNT(*) AS triple_count "
        f"FROM current_triples{where} "
        f"GROUP BY entity_id ORDER BY triple_count DESC"
    )
    sql_domains = (
        f"SELECT split_part(concept, '.', 1) AS domain, entity_id, COUNT(*) AS cnt "
        f"FROM current_triples{where} "
        f"GROUP BY domain, entity_id ORDER BY domain, entity_id"
    )
    period_where = where + (" AND " if where else " WHERE ") + "period IS NOT NULL"
    sql_periods = f"SELECT DISTINCT period FROM current_triples{period_where} ORDER BY period"

    # Latest ingest comes from tenant_runs (current_triples has no run_id).
    if tenant_id:
        sql_latest = (
            "SELECT current_run_id, updated_at, run_row_count "
            "FROM tenant_runs WHERE tenant_id = %s "
            "ORDER BY updated_at DESC LIMIT 1"
        )
        latest_params = [tenant_id]
    else:
        sql_latest = (
            "SELECT current_run_id, updated_at, run_row_count "
            "FROM tenant_runs ORDER BY updated_at DESC LIMIT 1"
        )
        latest_params = []

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql_total, params)
                total_triples = cur.fetchone()[0]

                cur.execute(sql_entities, params)
                entities = []
                for row in cur.fetchall():
                    entity_id = row[0]
                    entities.append({
                        "entity_id": entity_id,
                        "triple_count": row[1],
                        "display_name": _entity_display_name(entity_id),
                    })

                cur.execute(sql_domains, params)
                # Pivot per-entity counts into {domain, count, by_entity}
                domain_map: dict[str, dict] = {}
                for r in cur.fetchall():
                    domain, entity_id, cnt = r[0], r[1], r[2]
                    if domain not in domain_map:
                        domain_map[domain] = {"domain": domain, "count": 0, "by_entity": {}}
                    domain_map[domain]["count"] += cnt
                    domain_map[domain]["by_entity"][entity_id] = cnt
                domains = sorted(domain_map.values(), key=lambda d: d["count"], reverse=True)

                cur.execute(sql_periods, params)
                periods = [r[0] for r in cur.fetchall()]

                cur.execute(sql_latest, latest_params)
                latest_row = cur.fetchone()
                last_ingest = None
                if latest_row:
                    last_ingest = {
                        "dcl_ingest_id": str(latest_row[0]) if latest_row[0] else None,
                        "timestamp": latest_row[1].isoformat() if latest_row[1] else None,
                        "triple_count": latest_row[2] or 0,
                    }
    except PoolExhausted as e:
        raise HTTPException(
            status_code=503,
            detail=f"DCL database pool exhausted — too many concurrent requests. {e}",
        )
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))

    return {
        "total_triples": total_triples,
        "active_triples": total_triples,
        "entities": entities,
        "domains": domains,
        "periods": periods,
        "last_ingest": last_ingest,
    }


# ---------------------------------------------------------------------------
# GET /api/dcl/triples/runs
# ---------------------------------------------------------------------------

@router.get("/api/dcl/triples/runs")
def triples_runs():
    """List all current entity runs with per-run summary.

    Each tenant_runs row is one (tenant, entity, current_run_id). Domain
    summaries are computed from current_triples grouped per entity.
    """
    runs_sql = (
        "SELECT current_run_id, tenant_id, entity_id, run_row_count, updated_at "
        "FROM tenant_runs ORDER BY updated_at DESC"
    )

    domain_sql = (
        "SELECT tenant_id, entity_id, split_part(concept, '.', 1) AS domain, "
        "COUNT(*) AS cnt "
        "FROM current_triples "
        "GROUP BY tenant_id, entity_id, domain "
        "ORDER BY tenant_id, entity_id, domain"
    )

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(runs_sql)
                columns = [desc[0] for desc in cur.description]
                raw_runs = [dict(zip(columns, row)) for row in cur.fetchall()]

                cur.execute(domain_sql)
                domain_rows = cur.fetchall()
    except PoolExhausted as e:
        raise HTTPException(
            status_code=503,
            detail=f"DCL database pool exhausted — too many concurrent requests. {e}",
        )
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))

    # Index domain summaries by (tenant_id, entity_id)
    domain_by_key: dict[tuple[str, str], dict[str, int]] = {}
    for tid, eid, domain, cnt in domain_rows:
        key = (str(tid), eid)
        domain_by_key.setdefault(key, {})[domain] = cnt

    runs = []
    for r in raw_runs:
        run_id_str = str(r["current_run_id"]) if r["current_run_id"] else None
        tenant_id_str = str(r["tenant_id"])
        entity_id = r["entity_id"]
        label = _entity_display_name(entity_id) if entity_id and entity_id != "combined" else tenant_id_str[:8]

        runs.append({
            "dcl_ingest_id": run_id_str,
            "tenant_id": tenant_id_str,
            "tenant_label": label,
            "timestamp": r["updated_at"].isoformat() if r["updated_at"] else None,
            "triple_count": r["run_row_count"],
            "domain_summary": domain_by_key.get((tenant_id_str, entity_id), {}),
            "entity_summary": {entity_id: r["run_row_count"]} if entity_id else {},
        })

    return {"runs": runs}


# ---------------------------------------------------------------------------
# GET /api/dcl/triples/identity-checks
# ---------------------------------------------------------------------------

# All concept prefixes needed for identity checks, grouped by check.
_CONCEPT_PREFIXES = [
    "asset", "liability", "equity",
    "cash_flow.operating", "cash_flow.investing",
    "cash_flow.financing", "cash_flow.net_change",
    "revenue.total", "cogs.total", "opex.total", "pnl.ebitda",
    "asset.current.cash",
]


def _coerce_to_float(raw) -> float | None:
    """Coerce a JSONB-stored value to float.

    Returns None for values that cannot be represented as a number
    (dicts, unparseable strings, etc.). This is not a silent fallback —
    None means "this triple value is not numeric" and callers skip
    the identity check for that entity/period combination.
    """
    if isinstance(raw, (int, float, Decimal)):
        return float(raw)
    if isinstance(raw, str):
        parsed = json.loads(raw) if raw else raw
        if isinstance(parsed, (int, float)):
            return float(parsed)
        return None  # non-numeric string (e.g. "N/A", label text)
    if isinstance(raw, dict):
        return None  # structured JSONB object, not a scalar
    if hasattr(raw, '__float__'):
        return float(raw)
    return None  # unknown type, not coercible


def _build_identity_lookup(cur) -> tuple[dict, list[str], list[str]]:
    """Fetch all identity-relevant triples in one query.

    Returns (lookup, entity_ids, periods) where lookup maps
    (entity_id, concept_prefix, period) -> float value.
    """
    like_clauses = " OR ".join(["concept LIKE %s"] * len(_CONCEPT_PREFIXES))
    like_params = [p + "%" for p in _CONCEPT_PREFIXES]

    sql = (
        "SELECT entity_id, concept, period, value "
        "FROM current_triples "
        "WHERE property = 'amount' "
        f"  AND ({like_clauses})"
    )
    cur.execute(sql, like_params)
    rows = cur.fetchall()

    # Build lookup keyed by (entity_id, concept_prefix, period).
    # For each row, find the longest matching prefix so that e.g.
    # "asset.current.cash" matches prefix "asset.current.cash" not "asset".
    # Sort prefixes longest-first for greedy matching.
    sorted_prefixes = sorted(_CONCEPT_PREFIXES, key=len, reverse=True)

    lookup: dict[tuple[str, str, str], float] = {}
    entity_set: set[str] = set()
    period_set: set[str] = set()

    for row in rows:
        eid, concept, period, raw_val = row[0], row[1], row[2], row[3]
        val = _coerce_to_float(raw_val)
        if val is None:
            continue
        entity_set.add(eid)
        if period is not None:
            period_set.add(period)

        for prefix in sorted_prefixes:
            if concept.startswith(prefix):
                key = (eid, prefix, period)
                if key not in lookup:
                    lookup[key] = val
                break

    return lookup, sorted(entity_set), sorted(period_set)


def _run_check(lookup, entity_ids, periods, name, description, lhs_prefixes, rhs_prefixes, lhs_signs=None, rhs_signs=None):
    """Run a single identity check: sum(lhs) == sum(rhs) within tolerance."""
    if lhs_signs is None:
        lhs_signs = [1.0] * len(lhs_prefixes)
    if rhs_signs is None:
        rhs_signs = [1.0] * len(rhs_prefixes)

    results = []
    pass_count = 0
    fail_count = 0

    for eid in entity_ids:
        for period in periods:
            lhs = 0.0
            rhs = 0.0
            skip = False
            for prefix, sign in zip(lhs_prefixes, lhs_signs):
                v = lookup.get((eid, prefix, period))
                if v is None:
                    skip = True
                    break
                lhs += sign * v
            if skip:
                continue
            for prefix, sign in zip(rhs_prefixes, rhs_signs):
                v = lookup.get((eid, prefix, period))
                if v is None:
                    skip = True
                    break
                rhs += sign * v
            if skip:
                continue

            status = "PASS" if abs(lhs - rhs) < 0.01 else "FAIL"
            if status == "PASS":
                pass_count += 1
            else:
                fail_count += 1
            results.append({
                "entity_id": eid,
                "period": period,
                "status": status,
                "lhs": round(lhs, 2),
                "rhs": round(rhs, 2),
            })

    return {
        "name": name,
        "description": description,
        "results": results,
        "overall": "FAIL" if fail_count > 0 else ("PASS" if pass_count > 0 else "N/A"),
        "pass_count": pass_count,
        "fail_count": fail_count,
    }


@router.get("/api/dcl/triples/identity-checks")
def triples_identity_checks():
    """Run accounting identity checks against the live triple store."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                lookup, entity_ids, periods = _build_identity_lookup(cur)
    except PoolExhausted as e:
        raise HTTPException(
            status_code=503,
            detail=f"DCL database pool exhausted — too many concurrent requests. {e}",
        )
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))

    checks = []

    # BS Identity: Assets = Liabilities + Equity
    checks.append(_run_check(
        lookup, entity_ids, periods,
        "BS Identity", "Assets = Liabilities + Equity",
        lhs_prefixes=["asset"],
        rhs_prefixes=["liability", "equity"],
    ))

    # CF Identity: Operating + Investing + Financing = Net Change
    checks.append(_run_check(
        lookup, entity_ids, periods,
        "CF Identity", "Operating + Investing + Financing = Net Change",
        lhs_prefixes=["cash_flow.operating", "cash_flow.investing", "cash_flow.financing"],
        rhs_prefixes=["cash_flow.net_change"],
    ))

    # P&L Identity: Revenue - COGS - OpEx = EBITDA
    checks.append(_run_check(
        lookup, entity_ids, periods,
        "P&L Identity", "Revenue - COGS - OpEx = EBITDA",
        lhs_prefixes=["revenue.total", "cogs.total", "opex.total"],
        lhs_signs=[1.0, -1.0, -1.0],
        rhs_prefixes=["pnl.ebitda"],
    ))

    # Cash Continuity: Cash[Q(n)] + Net Change[Q(n+1)] = Cash[Q(n+1)]
    cc_results = []
    cc_pass = 0
    cc_fail = 0
    sorted_periods = sorted(periods)
    for eid in entity_ids:
        for i in range(len(sorted_periods) - 1):
            p_curr = sorted_periods[i]
            p_next = sorted_periods[i + 1]
            cash_curr = lookup.get((eid, "asset.current.cash", p_curr))
            net_change_next = lookup.get((eid, "cash_flow.net_change", p_next))
            cash_next = lookup.get((eid, "asset.current.cash", p_next))
            if any(v is None for v in [cash_curr, net_change_next, cash_next]):
                continue
            lhs = cash_curr + net_change_next
            rhs = cash_next
            status = "PASS" if abs(lhs - rhs) < 0.01 else "FAIL"
            if status == "PASS":
                cc_pass += 1
            else:
                cc_fail += 1
            cc_results.append({
                "entity_id": eid,
                "period": p_next,
                "status": status,
                "lhs": round(lhs, 2),
                "rhs": round(rhs, 2),
            })

    checks.append({
        "name": "Cash Continuity",
        "description": "Cash[Q(n)] + Net Change[Q(n+1)] = Cash[Q(n+1)]",
        "results": cc_results,
        "overall": "FAIL" if cc_fail > 0 else ("PASS" if cc_pass > 0 else "N/A"),
        "pass_count": cc_pass,
        "fail_count": cc_fail,
    })

    all_pass = all(c["overall"] == "PASS" for c in checks)
    return {
        "checks": checks,
        "all_pass": all_pass,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# GET /api/dcl/triples/browse
# ---------------------------------------------------------------------------

@router.get("/api/dcl/triples/browse")
def triples_browse(
    domain: Optional[str] = None,
    entity_id: Optional[str] = None,
    period: Optional[str] = None,
    property: Optional[str] = Query(None, alias="property"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """Browse raw triples with filtering and pagination."""
    clauses: list[str] = []
    params: list = []

    if domain:
        # Use prefix LIKE instead of split_part for index-friendly filtering.
        # concept LIKE 'revenue.%' can use btree indexes on concept.
        clauses.append("concept LIKE %s")
        params.append(f"{domain}.%")
    if entity_id:
        clauses.append("entity_id = %s")
        params.append(entity_id)
    if period:
        clauses.append("period = %s")
        params.append(period)
    if property:
        clauses.append("property = %s")
        params.append(property)

    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""

    count_sql = f"SELECT COUNT(*) FROM current_triples{where}"
    data_sql = (
        f"SELECT * FROM current_triples{where} "
        f"ORDER BY entity_id, concept, property, period "
        f"LIMIT %s OFFSET %s"
    )

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(count_sql, params)
                total_count = cur.fetchone()[0]

                cur.execute(data_sql, params + [limit, offset])
                columns = [desc[0] for desc in cur.description]
                triples = [_serialize_row(dict(zip(columns, row))) for row in cur.fetchall()]
    except PoolExhausted as e:
        raise HTTPException(
            status_code=503,
            detail=f"DCL database pool exhausted — too many concurrent requests. {e}",
        )
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))

    filters_applied = {}
    if domain:
        filters_applied["domain"] = domain
    if entity_id:
        filters_applied["entity_id"] = entity_id
    if period:
        filters_applied["period"] = period
    if property:
        filters_applied["property"] = property

    return {
        "triples": triples,
        "total_count": total_count,
        "filters_applied": filters_applied,
    }


# ---------------------------------------------------------------------------
# POST /api/dcl/triples/browse-batch
# ---------------------------------------------------------------------------

class BrowseBatchRequest(BaseModel):
    """Request body for batch browse — fetches triples across multiple domains
    in a single SQL query instead of N individual browse calls."""
    domains: List[str] = Field(..., min_length=1, description="List of concept domains to fetch")
    entity_ids: Optional[List[str]] = Field(None, description="Filter by entity IDs")
    period: Optional[str] = Field(None, description="Filter by period")


@router.post("/api/dcl/triples/browse-batch")
def triples_browse_batch(req: BrowseBatchRequest):
    """Batch browse: fetch deduplicated triples for multiple domains in one call.

    Returns triples grouped by domain. Replaces N individual browse calls with
    one SQL query, eliminating HTTP round-trip overhead for reports.
    """
    clauses: list[str] = []
    params: list = []

    # Domain filter: concept LIKE 'domain1.%' OR concept LIKE 'domain2.%' ...
    domain_conditions = []
    for domain in req.domains:
        domain_conditions.append("concept LIKE %s")
        params.append(f"{domain}.%")
    clauses.append(f"({' OR '.join(domain_conditions)})")

    if req.entity_ids:
        placeholders = ", ".join(["%s"] * len(req.entity_ids))
        clauses.append(f"entity_id IN ({placeholders})")
        params.extend(req.entity_ids)

    if req.period:
        clauses.append("period = %s")
        params.append(req.period)

    where = " AND ".join(clauses)

    data_sql = (
        f"SELECT * FROM current_triples WHERE {where} "
        f"ORDER BY entity_id, concept, property, period"
    )

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(data_sql, params)
                columns = [desc[0] for desc in cur.description]
                all_triples = [_serialize_row(dict(zip(columns, row))) for row in cur.fetchall()]
    except PoolExhausted as e:
        raise HTTPException(
            status_code=503,
            detail=f"DCL database pool exhausted — too many concurrent requests. {e}",
        )
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))

    # Group by domain (first segment of concept)
    by_domain: dict = {}
    for t in all_triples:
        concept = t.get("concept", "")
        domain = concept.split(".")[0] if concept else ""
        by_domain.setdefault(domain, []).append(t)

    return {
        "triples_by_domain": by_domain,
        "total_count": len(all_triples),
        "domains_requested": req.domains,
        "domains_returned": list(by_domain.keys()),
    }


# ---------------------------------------------------------------------------
# GET /api/dcl/triples/engagement
# ---------------------------------------------------------------------------

@router.get("/api/dcl/triples/engagement")
def triples_engagement(tenant_id: Optional[str] = Query(None)):
    """Entity state from the SE triple store.

    DCL is SE-only. Returns distinct entity_ids from the current run's
    triples. Engagement lifecycle is not DCL's concern.

    When multiple tenants exist, caller must pass tenant_id explicitly.
    """
    from backend.db.triple_store import TripleStore

    store = TripleStore()
    try:
        if not tenant_id:
            tenant_id = store.resolve_single_tenant()
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

    # Read entity_ids directly from tenant_runs — one row per entity.
    # Not filtered through a single current_run_id (the old bug).
    sql = "SELECT entity_id FROM tenant_runs WHERE tenant_id = %s ORDER BY updated_at DESC"
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (tenant_id,))
            entity_ids = [row[0] for row in cur.fetchall()]

    return {
        "entities": [
            {"id": eid, "display_name": _entity_display_name(eid)}
            for eid in entity_ids
        ],
        "status": "active",
    }


# ---------------------------------------------------------------------------
# GET /api/dcl/triples/resolution-summary
# ---------------------------------------------------------------------------

@router.get("/api/dcl/triples/resolution-summary")
def triples_resolution_summary():
    """Resolution workspace aggregate stats."""
    # Try v2 table first, fall back to v1
    for table_name, type_col_name, decided_col_name in (
        ("resolution_workspaces_v2", "domain", "updated_at"),
        ("resolution_workspaces", "workspace_type", "decided_at"),
    ):
        tbl = pgsql.Identifier(table_name)
        type_col = pgsql.Identifier(type_col_name)
        decided_col = pgsql.Identifier(decided_col_name)
        try:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(pgsql.SQL("SELECT COUNT(*) FROM {}").format(tbl))
                    total = cur.fetchone()[0]
                    if total == 0:
                        continue

                    cur.execute(
                        pgsql.SQL("SELECT status, COUNT(*) FROM {} GROUP BY status").format(tbl)
                    )
                    by_status = {r[0]: r[1] for r in cur.fetchall()}

                    cur.execute(
                        pgsql.SQL("SELECT {}, COUNT(*) FROM {} GROUP BY {}").format(
                            type_col, tbl, type_col
                        )
                    )
                    by_type = {r[0]: r[1] for r in cur.fetchall()}

                    # Recent decisions
                    cur.execute(
                        pgsql.SQL(
                            "SELECT id, {type_col}, status, decided_by, {decided_col} "
                            "FROM {tbl} "
                            "WHERE status IN ('resolved', 'confirmed', 'rejected', 'escalated') "
                            "ORDER BY {decided_col} DESC NULLS LAST LIMIT 10"
                        ).format(type_col=type_col, decided_col=decided_col, tbl=tbl)
                    )
                    columns = [desc[0] for desc in cur.description]
                    recent = []
                    for row in cur.fetchall():
                        d = dict(zip(columns, row))
                        recent.append({
                            "workspace_id": str(d["id"]),
                            "type": d[type_col_name],
                            "decision": d["status"],
                            "decided_by": d.get("decided_by"),
                            "decided_at": d[decided_col_name].isoformat() if d.get(decided_col_name) else None,
                        })

                    return {
                        "total_workspaces": total,
                        "by_status": by_status,
                        "by_type": by_type,
                        "recent_decisions": recent,
                    }

        except PoolExhausted as e:
            raise HTTPException(
                status_code=503,
                detail=f"DCL database pool exhausted — too many concurrent requests. {e}",
            )
        except RuntimeError as e:
            raise HTTPException(status_code=503, detail=str(e))
        except Exception as e:
            logger.debug(f"[resolution-summary] Table {table_name} query failed: {e}")
            continue

    # No data in either table
    return {
        "total_workspaces": 0,
        "by_status": {},
        "by_type": {},
        "recent_decisions": [],
    }


# ---------------------------------------------------------------------------
# GET /api/dcl/triples/persona-stats
# ---------------------------------------------------------------------------

def _load_persona_domains() -> dict[str, list[str]]:
    """Load persona→domain mapping from config/persona_domains.yaml."""
    config_path = Path(__file__).resolve().parents[3] / "config" / "persona_domains.yaml"
    if not config_path.exists():
        raise RuntimeError(
            f"Persona domain config not found at {config_path}. "
            f"This file is required for persona-stats."
        )
    with open(config_path) as f:
        data = yaml.safe_load(f)
    personas = data.get("personas", {})
    return {key: entry["domains"] for key, entry in personas.items()}


@router.get("/api/dcl/triples/persona-stats")
def triples_persona_stats():
    """Per-persona statistics derived from the semantic triple store.

    Returns data_sources, domain count, triple count, and matched domain list
    for each persona based on persona→domain mapping in config/persona_domains.yaml.
    """
    try:
        persona_domains = _load_persona_domains()
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to load persona domain config: {e}",
        )

    try:
        stats = _triple_store.get_persona_domain_stats(persona_domains)
    except PoolExhausted as e:
        raise HTTPException(
            status_code=503,
            detail=f"DCL database pool exhausted — too many concurrent requests. {e}",
        )
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))

    return stats


# ---------------------------------------------------------------------------
# GET /api/dcl/contextualization-summary
# ---------------------------------------------------------------------------

from backend.registry.concept_registry import ConceptRegistry

_concept_registry = ConceptRegistry()


@router.get("/api/dcl/contextualization-summary")
def contextualization_summary(
    entity_id: Optional[str] = Query(None),
    tenant_id: Optional[str] = Query(None, description="Filter by tenant_id (deal scope)"),
):
    """Contextualization quality summary — reads the flat current_triples mirror.

    current_triples holds exactly one row per live logical triple across all
    (tenant, entity) pairs; no run_id filtering is needed.
    """
    clauses: list[str] = []
    params: list = []

    if tenant_id:
        clauses.append("tenant_id = %s")
        params.append(tenant_id)
    if entity_id:
        clauses.append("entity_id = %s")
        params.append(entity_id)

    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""

    # Query 1: per-domain aggregation
    domain_sql = (
        f"SELECT split_part(concept, '.', 1) AS domain, "
        f"COUNT(*) AS triple_count, "
        f"COUNT(DISTINCT concept) AS concepts_used, "
        f"COUNT(DISTINCT source_system) AS source_count, "
        f"AVG(confidence_score) AS avg_confidence, "
        f"COUNT(*) FILTER (WHERE confidence_tier = 'exact') AS tier_exact, "
        f"COUNT(*) FILTER (WHERE confidence_tier = 'high') AS tier_high, "
        f"COUNT(*) FILTER (WHERE confidence_tier = 'medium') AS tier_medium, "
        f"COUNT(*) FILTER (WHERE confidence_tier = 'low') AS tier_low "
        f"FROM current_triples{where} "
        f"GROUP BY domain ORDER BY triple_count DESC"
    )

    # Query 2: source system breakdown
    source_sql = (
        f"SELECT source_system, COUNT(*) AS triple_count, "
        f"AVG(confidence_score) AS avg_confidence "
        f"FROM current_triples{where} "
        f"GROUP BY source_system ORDER BY triple_count DESC"
    )

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(domain_sql, params)
                domain_rows = cur.fetchall()

                cur.execute(source_sql, params)
                source_rows = cur.fetchall()
    except PoolExhausted as e:
        raise HTTPException(
            status_code=503,
            detail=f"DCL database pool exhausted — too many concurrent requests. {e}",
        )
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))

    # Build ontology concept map for concepts_available per domain
    all_concepts = _concept_registry.list_concepts()
    concept_domain_map: dict[str, list[str]] = {}
    for cid in all_concepts:
        concept_domain_map.setdefault(cid, []).append(cid)

    # Populated domains from live data
    populated_domains = set()
    domain_data = []
    total_confidence = {"exact": 0, "high": 0, "medium": 0, "low": 0}

    for row in domain_rows:
        domain_name = row[0]
        populated_domains.add(domain_name)
        concepts_available = 1 if domain_name in concept_domain_map else 0
        domain_data.append({
            "domain": domain_name,
            "triple_count": row[1],
            "concepts_used": row[2],
            "concepts_available": max(row[2], concepts_available),
            "source_count": row[3],
            "avg_confidence": round(float(row[4]), 3) if row[4] else 0.0,
        })
        total_confidence["exact"] += row[5]
        total_confidence["high"] += row[6]
        total_confidence["medium"] += row[7]
        total_confidence["low"] += row[8]

    # Resolution activity from resolution_workspaces_v2
    resolution = {"workspaces_total": 0, "workspaces_pending": 0, "workspaces_resolved": 0, "conflicts_detected": 0}
    for table_name in ("resolution_workspaces_v2", "resolution_workspaces"):
        tbl = pgsql.Identifier(table_name)
        try:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(pgsql.SQL("SELECT COUNT(*) FROM {}").format(tbl))
                    total = cur.fetchone()[0]
                    if total == 0:
                        continue
                    cur.execute(pgsql.SQL("SELECT status, COUNT(*) FROM {} GROUP BY status").format(tbl))
                    by_status = {r[0]: r[1] for r in cur.fetchall()}
                    resolution["workspaces_total"] = total
                    resolution["workspaces_pending"] = by_status.get("pending", 0)
                    resolution["workspaces_resolved"] = (
                        by_status.get("resolved", 0) + by_status.get("confirmed", 0)
                    )
                    resolution["conflicts_detected"] = by_status.get("conflict", 0) + by_status.get("escalated", 0)
                    break
        except PoolExhausted as e:
            raise HTTPException(
                status_code=503,
                detail=f"DCL database pool exhausted — too many concurrent requests. {e}",
            )
        except RuntimeError as e:
            raise HTTPException(status_code=503, detail=str(e))
        except Exception as e:
            logger.warning(f"[persona-view] Failed to query resolution table {table_name}: {e}")
            continue

    source_data = []
    for row in source_rows:
        source_data.append({
            "system": row[0],
            "triple_count": row[1],
            "avg_confidence": round(float(row[2]), 3) if row[2] else 0.0,
        })

    return {
        "domain_coverage": {
            "domains_populated": len(populated_domains),
            "domains_total": len(all_concepts),
            "domains": domain_data,
        },
        "confidence_distribution": total_confidence,
        "resolution_activity": resolution,
        "source_system_breakdown": source_data,
    }


# ---------------------------------------------------------------------------
# GET /api/dcl/dashboard-data
# ---------------------------------------------------------------------------

@router.get("/api/dcl/dashboard-data")
def dashboard_data(
    tenant_id: Optional[str] = Query(None, description="Filter by tenant_id (deal scope)"),
    entity_id: Optional[str] = Query(None),
    domain: Optional[str] = Query(None),
    source_system: Optional[str] = Query(None),
    period: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
):
    """Paginated, filterable triple data for the Dashboard tab.

    Reads from current_triples — one row per live logical triple, no dedup
    needed, counts match ingest and context tabs by construction.
    """
    clauses: list[str] = []
    params: list = []

    if tenant_id:
        clauses.append("tenant_id = %s")
        params.append(tenant_id)
    if entity_id:
        clauses.append("entity_id = %s")
        params.append(entity_id)
    if domain:
        clauses.append("concept LIKE %s")
        params.append(f"{domain}.%")
    if source_system:
        clauses.append("source_system = %s")
        params.append(source_system)
    if period:
        clauses.append("period = %s")
        params.append(period)

    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    offset = (page - 1) * page_size

    count_sql = f"SELECT COUNT(*) FROM current_triples{where}"

    data_sql = (
        f"SELECT id, entity_id, concept, property, value, period, "
        f"source_system, confidence_score, confidence_tier, pipe_id "
        f"FROM current_triples{where} "
        f"ORDER BY entity_id, concept, property, period "
        f"LIMIT %s OFFSET %s"
    )

    agg_domain_sql = (
        f"SELECT split_part(concept, '.', 1) AS domain, COUNT(*) AS cnt "
        f"FROM current_triples{where} "
        f"GROUP BY domain ORDER BY cnt DESC"
    )
    agg_source_sql = (
        f"SELECT source_system, COUNT(*) AS cnt "
        f"FROM current_triples{where} "
        f"GROUP BY source_system ORDER BY cnt DESC"
    )
    period_where = where + (" AND " if where else " WHERE ") + "period IS NOT NULL"
    agg_period_sql = (
        f"SELECT period, COUNT(*) AS cnt "
        f"FROM current_triples{period_where} "
        f"GROUP BY period ORDER BY period"
    )

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(count_sql, params)
                total_count = cur.fetchone()[0]

                cur.execute(data_sql, params + [page_size, offset])
                columns = [desc[0] for desc in cur.description]
                rows = [_serialize_row(dict(zip(columns, row))) for row in cur.fetchall()]

                cur.execute(agg_domain_sql, params)
                by_domain = [{"domain": r[0], "count": r[1]} for r in cur.fetchall()]

                cur.execute(agg_source_sql, params)
                by_source = [{"system": r[0], "count": r[1]} for r in cur.fetchall()]

                cur.execute(agg_period_sql, params)
                by_period = [{"period": r[0], "count": r[1]} for r in cur.fetchall()]
    except PoolExhausted as e:
        raise HTTPException(
            status_code=503,
            detail=f"DCL database pool exhausted — too many concurrent requests. {e}",
        )
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))

    filters_applied = {}
    if entity_id:
        filters_applied["entity_id"] = entity_id
    if domain:
        filters_applied["domain"] = domain
    if source_system:
        filters_applied["source_system"] = source_system
    if period:
        filters_applied["period"] = period

    return {
        "rows": rows,
        "total_count": total_count,
        "page": page,
        "page_size": page_size,
        "filters_applied": filters_applied,
        "aggregations": {
            "by_domain": by_domain,
            "by_source": by_source,
            "by_period": by_period,
        },
    }
