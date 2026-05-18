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
POST /api/dcl/triples/deactivate-run  — deactivate a run
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
    """Return distinct entities from semantic_triples with triple counts and recency."""
    sql = (
        "SELECT s.entity_id, COUNT(*) AS triple_count, MAX(s.created_at) AS latest_ingest "
        "FROM semantic_triples s "
        "JOIN tenant_runs t ON t.tenant_id = s.tenant_id AND t.current_run_id = s.run_id "
        "WHERE s.is_active = true "
        "GROUP BY s.entity_id ORDER BY latest_ingest DESC"
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
    for i, (entity_id, triple_count, latest_ingest) in enumerate(rows):
        entities.append({
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
    extra_filter = ""
    params: list = []
    if tenant_id:
        extra_filter += " AND tenant_id = %s"
        params.append(tenant_id)
        extra_filter += " AND run_id IN (SELECT current_run_id FROM tenant_runs WHERE tenant_id = %s)"
        params.append(tenant_id)
    if source_run_tag:
        extra_filter += " AND source_run_tag = %s"
        params.append(source_run_tag)

    sql_total = f"SELECT COUNT(*) FROM semantic_triples WHERE is_active = true{extra_filter}"
    sql_entities = (
        f"SELECT entity_id, COUNT(*) AS triple_count "
        f"FROM semantic_triples WHERE is_active = true{extra_filter} "
        f"GROUP BY entity_id ORDER BY triple_count DESC"
    )
    sql_domains = (
        f"SELECT split_part(concept, '.', 1) AS domain, entity_id, COUNT(*) AS cnt "
        f"FROM semantic_triples WHERE is_active = true{extra_filter} "
        f"GROUP BY domain, entity_id ORDER BY domain, entity_id"
    )
    sql_periods = (
        f"SELECT DISTINCT period FROM semantic_triples "
        f"WHERE is_active = true AND period IS NOT NULL{extra_filter} "
        f"ORDER BY period"
    )
    sql_latest = (
        f"SELECT run_id, MIN(created_at) AS timestamp, COUNT(*) AS triple_count "
        f"FROM semantic_triples WHERE is_active = true{extra_filter} "
        f"GROUP BY run_id ORDER BY MIN(created_at) DESC LIMIT 1"
    )

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

                cur.execute(sql_latest, params)
                latest_row = cur.fetchone()
                last_ingest = None
                if latest_row:
                    last_ingest = {
                        "dcl_ingest_id": str(latest_row[0]),
                        "timestamp": latest_row[1].isoformat() if latest_row[1] else None,
                        "triple_count": latest_row[2],
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
    """List all ingest runs with per-run summary.

    Uses a single connection for all 3 queries to avoid pool exhaustion
    under concurrent load (N+1 pattern previously opened 2 connections
    per run, exhausting the 10-connection pool).
    """
    runs_sql = (
        "SELECT run_id, tenant_id, COUNT(*) AS triple_count, "
        "MIN(created_at) AS created_at, "
        "bool_and(is_active) AS is_active "
        "FROM semantic_triples "
        "GROUP BY run_id, tenant_id "
        "ORDER BY MIN(created_at) DESC"
    )

    domain_sql = (
        "SELECT run_id, split_part(concept, '.', 1) AS domain, COUNT(*) AS cnt "
        "FROM semantic_triples WHERE is_active = true "
        "GROUP BY run_id, domain ORDER BY run_id, domain"
    )

    entity_sql = (
        "SELECT run_id, entity_id, COUNT(*) AS cnt "
        "FROM semantic_triples "
        "GROUP BY run_id, entity_id"
    )

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(runs_sql)
                columns = [desc[0] for desc in cur.description]
                raw_runs = [dict(zip(columns, row)) for row in cur.fetchall()]

                cur.execute(domain_sql)
                domain_rows = cur.fetchall()

                cur.execute(entity_sql)
                entity_rows = cur.fetchall()
    except PoolExhausted as e:
        raise HTTPException(
            status_code=503,
            detail=f"DCL database pool exhausted — too many concurrent requests. {e}",
        )
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))

    # Index domain summaries by run_id
    domain_by_run: dict[str, dict[str, int]] = {}
    for run_id_val, domain, cnt in domain_rows:
        rid = str(run_id_val)
        domain_by_run.setdefault(rid, {})[domain] = cnt

    # Index entity summaries by run_id
    entity_by_run: dict[str, dict[str, int]] = {}
    for run_id_val, entity_id, cnt in entity_rows:
        rid = str(run_id_val)
        entity_by_run.setdefault(rid, {})[entity_id] = cnt

    runs = []
    for r in raw_runs:
        run_id_str = str(r["run_id"])
        tenant_id_str = str(r["tenant_id"])
        entity_ids = list(entity_by_run.get(run_id_str, {}).keys())
        real = [e for e in entity_ids if e and e != "combined"]
        label = (
            " · ".join(_entity_display_name(e) for e in sorted(real))
            if real else tenant_id_str[:8]
        )

        runs.append({
            "dcl_ingest_id": run_id_str,
            "tenant_id": tenant_id_str,
            "tenant_label": label,
            "timestamp": r["created_at"].isoformat() if r["created_at"] else None,
            "triple_count": r["triple_count"],
            "is_active": r["is_active"],
            "domain_summary": domain_by_run.get(run_id_str, {}),
            "entity_summary": entity_by_run.get(run_id_str, {}),
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
        "SELECT DISTINCT ON (entity_id, period, concept) "
        "  entity_id, concept, period, value "
        "FROM semantic_triples "
        "WHERE is_active = true AND property = 'amount' "
        f"  AND ({like_clauses}) "
        "ORDER BY entity_id, period, concept"
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
    run_id: Optional[str] = Query(None, description="Scope to a single ingest batch (dcl_ingest_id / aam_inference_id)"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """Browse raw triples with filtering and pagination.

    When `run_id` is explicitly provided, is_active filter is dropped:
    the caller wants THIS batch's data regardless of whether tenant_runs
    has marked a later run as current. Required for AAM Fabrics drill
    view of a recent push that's been superseded by a subsequent push
    in the same trigger (5-sync trigger batches flip earlier ones
    inactive within seconds of each other). Without this, drilling into
    any but the absolute-latest run returns 0 triples even though the
    rows are present. See aam_deferred_work.md#20 for the cross-source
    aggregation case that needs the same opt-out.
    """
    clauses: list[str] = []
    if not run_id:
        clauses.append("is_active = true")
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
    if run_id:
        # run_id is a UUID column; reject malformed input cleanly instead of letting psycopg raise.
        import uuid as _uuid
        try:
            _uuid.UUID(run_id)
        except (ValueError, AttributeError):
            raise HTTPException(status_code=400, detail=f"run_id must be a UUID; got {run_id!r}")
        clauses.append("run_id = %s")
        params.append(run_id)

    where = " AND ".join(clauses) if clauses else "TRUE"

    # Deduplicate triples that differ only by run_id or source_run_tag
    # (multiple pipeline runs produce duplicates). Keep the most recent
    # triple per (entity_id, concept, property, period).
    count_sql = (
        f"SELECT COUNT(DISTINCT (entity_id, concept, property, period)) "
        f"FROM semantic_triples WHERE {where}"
    )
    data_sql = (
        f"SELECT DISTINCT ON (entity_id, concept, property, period) * "
        f"FROM semantic_triples WHERE {where} "
        f"ORDER BY entity_id, concept, property, period, created_at DESC "
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
    if run_id:
        filters_applied["run_id"] = run_id

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
    clauses = ["is_active = true"]
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
        f"SELECT DISTINCT ON (entity_id, concept, property, period) * "
        f"FROM semantic_triples WHERE {where} "
        f"ORDER BY entity_id, concept, property, period, created_at DESC"
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
# POST /api/dcl/triples/deactivate-run
# ---------------------------------------------------------------------------

@router.post("/api/dcl/triples/deactivate-run")
def deactivate_run(run_id: str = Query(...)):
    """Deactivate all triples for a specific run."""
    count = _triple_store.deactivate_run(run_id)
    logger.info(f"[triple-monitor] Deactivated {count} triples for run_id={run_id}")
    return {"dcl_ingest_id": run_id, "deactivated_count": count}


# ---------------------------------------------------------------------------
# GET /api/dcl/contextualization-summary
# ---------------------------------------------------------------------------

from backend.registry.concept_registry import ConceptRegistry

_concept_registry = ConceptRegistry()


@router.get("/api/dcl/contextualization-summary")
def contextualization_summary(
    entity_id: Optional[str] = Query(None),
    run_id: Optional[str] = Query(None),
    tenant_id: Optional[str] = Query(None, description="Filter by tenant_id (deal scope)"),
):
    """Contextualization quality summary: domain coverage, confidence, resolution, sources."""
    clauses = ["is_active = true"]
    params: list = []

    if tenant_id:
        clauses.append("tenant_id = %s")
        params.append(tenant_id)
        clauses.append("run_id IN (SELECT current_run_id FROM tenant_runs WHERE tenant_id = %s)")
        params.append(tenant_id)
    if entity_id:
        clauses.append("entity_id = %s")
        params.append(entity_id)
    if run_id:
        clauses.append("run_id = %s")
        params.append(run_id)

    where = " AND ".join(clauses)

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
        f"FROM semantic_triples WHERE {where} "
        f"GROUP BY domain ORDER BY triple_count DESC"
    )

    # Query 2: source system breakdown
    source_sql = (
        f"SELECT source_system, COUNT(*) AS triple_count, "
        f"AVG(confidence_score) AS avg_confidence "
        f"FROM semantic_triples WHERE {where} "
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
    entity_id: Optional[str] = Query(None),
    domain: Optional[str] = Query(None),
    source_system: Optional[str] = Query(None),
    period: Optional[str] = Query(None),
    run_id: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
):
    """Paginated, filterable triple data with aggregations for the Dashboard tab."""
    clauses = ["is_active = true"]
    params: list = []

    if entity_id:
        clauses.append("entity_id = %s")
        params.append(entity_id)
    if run_id:
        clauses.append("run_id = %s")
        params.append(run_id)
    if domain:
        clauses.append("concept LIKE %s")
        params.append(f"{domain}.%")
    if source_system:
        clauses.append("source_system = %s")
        params.append(source_system)
    if period:
        clauses.append("period = %s")
        params.append(period)

    where = " AND ".join(clauses)
    offset = (page - 1) * page_size

    # Count query (deduplicated)
    count_sql = (
        f"SELECT COUNT(DISTINCT (entity_id, concept, property, period)) "
        f"FROM semantic_triples WHERE {where}"
    )

    # Paginated data query (deduplicated)
    data_sql = (
        f"SELECT DISTINCT ON (entity_id, concept, property, period) "
        f"id, entity_id, concept, property, value, period, "
        f"source_system, confidence_score, confidence_tier, pipe_id, run_id "
        f"FROM semantic_triples WHERE {where} "
        f"ORDER BY entity_id, concept, property, period, created_at DESC "
        f"LIMIT %s OFFSET %s"
    )

    # Aggregation queries (ignore pagination, apply same filters)
    agg_domain_sql = (
        f"SELECT split_part(concept, '.', 1) AS domain, "
        f"COUNT(DISTINCT (entity_id, concept, property, period)) AS cnt "
        f"FROM semantic_triples WHERE {where} "
        f"GROUP BY domain ORDER BY cnt DESC"
    )
    agg_source_sql = (
        f"SELECT source_system, "
        f"COUNT(DISTINCT (entity_id, concept, property, period)) AS cnt "
        f"FROM semantic_triples WHERE {where} "
        f"GROUP BY source_system ORDER BY cnt DESC"
    )
    agg_period_sql = (
        f"SELECT period, "
        f"COUNT(DISTINCT (entity_id, concept, property, period)) AS cnt "
        f"FROM semantic_triples WHERE {where} AND period IS NOT NULL "
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
