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
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import yaml
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from typing import List, Optional

from backend.db.triple_store import TripleStore
from backend.db.engagement_store import EngagementStore
from backend.db.resolution_store import ResolutionStore
from backend.core.db import get_connection
from backend.engine.engagement import get_active_engagement
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)


def _entity_display_name(entity_id: str) -> str:
    """Human-readable display name from entity_id.

    Pipeline tenants already carry a readable name like "BlueFlow-8XHJ" —
    preserve those as-is.  Only apply the underscore→space→title transform
    to generic ids like "company_a" that lack uppercase or hyphens.
    """
    if not entity_id:
        return entity_id
    # If the id already has mixed case or hyphens, it's a readable name
    if any(c.isupper() for c in entity_id) or "-" in entity_id:
        return entity_id
    return entity_id.replace("_", " ").title()


router = APIRouter(tags=["Triple Monitor"])

_triple_store = TripleStore()
_engagement_store = EngagementStore()
_resolution_store = ResolutionStore()


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
# GET /api/dcl/triples/overview
# ---------------------------------------------------------------------------

@router.get("/api/dcl/triples/overview")
def triples_overview(
    source_run_tag: Optional[str] = Query(None, description="Filter by Farm-originated source_run_tag"),
):
    """High-level summary of the triple store.

    Optional source_run_tag filter narrows results to triples from a specific run.
    """
    tag_filter = ""
    params: list = []
    if source_run_tag:
        tag_filter = " AND source_run_tag = %s"
        params = [source_run_tag]

    sql_total = f"SELECT COUNT(*) FROM semantic_triples WHERE is_active = true{tag_filter}"
    sql_entities = (
        f"SELECT entity_id, COUNT(*) AS triple_count "
        f"FROM semantic_triples WHERE is_active = true{tag_filter} "
        f"GROUP BY entity_id ORDER BY triple_count DESC"
    )
    sql_domains = (
        f"SELECT split_part(concept, '.', 1) AS domain, entity_id, COUNT(*) AS cnt "
        f"FROM semantic_triples WHERE is_active = true{tag_filter} "
        f"GROUP BY domain, entity_id ORDER BY domain, entity_id"
    )
    sql_periods = (
        f"SELECT DISTINCT period FROM semantic_triples "
        f"WHERE is_active = true AND period IS NOT NULL{tag_filter} "
        f"ORDER BY period"
    )
    sql_latest = (
        f"SELECT run_id, MIN(created_at) AS timestamp, COUNT(*) AS triple_count "
        f"FROM semantic_triples WHERE is_active = true{tag_filter} "
        f"GROUP BY run_id ORDER BY MIN(created_at) DESC LIMIT 1"
    )
    # Conflict count: distinct conflict IDs from cofa_conflict.* triples
    sql_conflicts = (
        f"SELECT COUNT(DISTINCT split_part(concept, '.', 2)) "
        f"FROM semantic_triples "
        f"WHERE is_active = true AND split_part(concept, '.', 1) = 'cofa_conflict'{tag_filter}"
    )

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
                    "run_id": str(latest_row[0]),
                    "timestamp": latest_row[1].isoformat() if latest_row[1] else None,
                    "triple_count": latest_row[2],
                }

            cur.execute(sql_conflicts, params)
            conflict_count = cur.fetchone()[0]

    return {
        "total_triples": total_triples,
        "active_triples": total_triples,
        "entities": entities,
        "domains": domains,
        "periods": periods,
        "last_ingest": last_ingest,
        "conflict_count": conflict_count,
    }


# ---------------------------------------------------------------------------
# GET /api/dcl/triples/runs
# ---------------------------------------------------------------------------

@router.get("/api/dcl/triples/runs")
def triples_runs():
    """List all ingest runs with per-run summary."""
    sql = (
        "SELECT run_id, tenant_id, COUNT(*) AS triple_count, "
        "MIN(created_at) AS created_at, "
        "bool_and(is_active) AS is_active "
        "FROM semantic_triples "
        "GROUP BY run_id, tenant_id "
        "ORDER BY MIN(created_at) DESC"
    )

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            columns = [desc[0] for desc in cur.description]
            raw_runs = [dict(zip(columns, row)) for row in cur.fetchall()]

    runs = []
    for r in raw_runs:
        run_id_str = str(r["run_id"])

        # Domain summary for this run
        domain_summary = _triple_store.count_by_domain(
            tenant_id=None, run_id=run_id_str,
        )

        # Entity summary for this run
        entity_sql = (
            "SELECT entity_id, COUNT(*) AS cnt "
            "FROM semantic_triples WHERE run_id = %s "
            "GROUP BY entity_id"
        )
        entity_summary = {}
        with get_connection() as conn2:
            with conn2.cursor() as cur2:
                cur2.execute(entity_sql, (run_id_str,))
                for erow in cur2.fetchall():
                    entity_summary[erow[0]] = erow[1]

        runs.append({
            "run_id": run_id_str,
            "timestamp": r["created_at"].isoformat() if r["created_at"] else None,
            "triple_count": r["triple_count"],
            "is_active": r["is_active"],
            "domain_summary": domain_summary,
            "entity_summary": entity_summary,
        })

    return {"runs": runs}


# ---------------------------------------------------------------------------
# GET /api/dcl/triples/identity-checks
# ---------------------------------------------------------------------------

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


def _get_triple_value(
    cur, entity_id: str, concept_prefix: str, period: str,
) -> float | None:
    """Fetch a single numeric triple value. Returns None if not found."""
    sql = (
        "SELECT value FROM semantic_triples "
        "WHERE is_active = true AND entity_id = %s "
        "AND concept LIKE %s AND property = 'amount' "
        "AND period = %s "
        "LIMIT 1"
    )
    cur.execute(sql, (entity_id, concept_prefix + "%", period))
    row = cur.fetchone()
    if row is None:
        return None
    raw = row[0]
    return _coerce_to_float(raw)


def _sum_triple_values(
    cur, entity_id: str, concept_prefixes: list[str], period: str,
) -> float | None:
    """Sum multiple triple values. Returns None if any are missing."""
    total = 0.0
    for prefix in concept_prefixes:
        val = _get_triple_value(cur, entity_id, prefix, period)
        if val is None:
            return None
        total += val
    return total


@router.get("/api/dcl/triples/identity-checks")
def triples_identity_checks():
    """Run accounting identity checks against the live triple store."""
    # Get all entity_ids and periods
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT entity_id FROM semantic_triples "
                "WHERE is_active = true ORDER BY entity_id"
            )
            entity_ids = [r[0] for r in cur.fetchall()]

            cur.execute(
                "SELECT DISTINCT period FROM semantic_triples "
                "WHERE is_active = true AND period IS NOT NULL "
                "ORDER BY period"
            )
            periods = [r[0] for r in cur.fetchall()]

            checks = []

            # --- BS Identity: Assets = Liabilities + Equity ---
            bs_results = []
            bs_pass = 0
            bs_fail = 0
            for eid in entity_ids:
                for period in periods:
                    assets = _get_triple_value(cur, eid, "asset", period)
                    liabilities = _get_triple_value(cur, eid, "liability", period)
                    equity = _get_triple_value(cur, eid, "equity", period)
                    if assets is None or liabilities is None or equity is None:
                        continue
                    lhs = assets
                    rhs = liabilities + equity
                    status = "PASS" if abs(lhs - rhs) < 0.01 else "FAIL"
                    if status == "PASS":
                        bs_pass += 1
                    else:
                        bs_fail += 1
                    bs_results.append({
                        "entity_id": eid,
                        "period": period,
                        "status": status,
                        "lhs": round(lhs, 2),
                        "rhs": round(rhs, 2),
                    })

            checks.append({
                "name": "BS Identity",
                "description": "Assets = Liabilities + Equity",
                "results": bs_results,
                "overall": "FAIL" if bs_fail > 0 else ("PASS" if bs_pass > 0 else "N/A"),
                "pass_count": bs_pass,
                "fail_count": bs_fail,
            })

            # --- CF Identity: Operating + Investing + Financing = Net Change ---
            cf_results = []
            cf_pass = 0
            cf_fail = 0
            for eid in entity_ids:
                for period in periods:
                    operating = _get_triple_value(cur, eid, "cash_flow.operating", period)
                    investing = _get_triple_value(cur, eid, "cash_flow.investing", period)
                    financing = _get_triple_value(cur, eid, "cash_flow.financing", period)
                    net_change = _get_triple_value(cur, eid, "cash_flow.net_change", period)
                    if any(v is None for v in [operating, investing, financing, net_change]):
                        continue
                    lhs = operating + investing + financing
                    rhs = net_change
                    status = "PASS" if abs(lhs - rhs) < 0.01 else "FAIL"
                    if status == "PASS":
                        cf_pass += 1
                    else:
                        cf_fail += 1
                    cf_results.append({
                        "entity_id": eid,
                        "period": period,
                        "status": status,
                        "lhs": round(lhs, 2),
                        "rhs": round(rhs, 2),
                    })

            checks.append({
                "name": "CF Identity",
                "description": "Operating + Investing + Financing = Net Change",
                "results": cf_results,
                "overall": "FAIL" if cf_fail > 0 else ("PASS" if cf_pass > 0 else "N/A"),
                "pass_count": cf_pass,
                "fail_count": cf_fail,
            })

            # --- P&L Identity: Revenue - COGS - OpEx = EBITDA ---
            pnl_results = []
            pnl_pass = 0
            pnl_fail = 0
            for eid in entity_ids:
                for period in periods:
                    revenue = _get_triple_value(cur, eid, "revenue.total", period)
                    cogs_val = _get_triple_value(cur, eid, "cogs.total", period)
                    opex_val = _get_triple_value(cur, eid, "opex.total", period)
                    ebitda = _get_triple_value(cur, eid, "pnl.ebitda", period)
                    if any(v is None for v in [revenue, cogs_val, opex_val, ebitda]):
                        continue
                    lhs = revenue - cogs_val - opex_val
                    rhs = ebitda
                    status = "PASS" if abs(lhs - rhs) < 0.01 else "FAIL"
                    if status == "PASS":
                        pnl_pass += 1
                    else:
                        pnl_fail += 1
                    pnl_results.append({
                        "entity_id": eid,
                        "period": period,
                        "status": status,
                        "lhs": round(lhs, 2),
                        "rhs": round(rhs, 2),
                    })

            checks.append({
                "name": "P&L Identity",
                "description": "Revenue - COGS - OpEx = EBITDA",
                "results": pnl_results,
                "overall": "FAIL" if pnl_fail > 0 else ("PASS" if pnl_pass > 0 else "N/A"),
                "pass_count": pnl_pass,
                "fail_count": pnl_fail,
            })

            # --- Cash Continuity: Cash[Q(n)] + Net Change[Q(n+1)] = Cash[Q(n+1)] ---
            cc_results = []
            cc_pass = 0
            cc_fail = 0
            for eid in entity_ids:
                sorted_periods = sorted(periods)
                for i in range(len(sorted_periods) - 1):
                    p_curr = sorted_periods[i]
                    p_next = sorted_periods[i + 1]
                    cash_curr = _get_triple_value(cur, eid, "asset.current.cash", p_curr)
                    net_change_next = _get_triple_value(cur, eid, "cash_flow.net_change", p_next)
                    cash_next = _get_triple_value(cur, eid, "asset.current.cash", p_next)
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
    clauses = ["is_active = true"]
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

    where = " AND ".join(clauses)

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

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(count_sql, params)
            total_count = cur.fetchone()[0]

            cur.execute(data_sql, params + [limit, offset])
            columns = [desc[0] for desc in cur.description]
            triples = [_serialize_row(dict(zip(columns, row))) for row in cur.fetchall()]

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

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(data_sql, params)
            columns = [desc[0] for desc in cur.description]
            all_triples = [_serialize_row(dict(zip(columns, row))) for row in cur.fetchall()]

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
def triples_engagement():
    """Current engagement state from the engagement config file.

    The engagement config (data/engagements/demo-001.json) is the source of
    truth for which entities are in scope — not the engagement_state table,
    which can be polluted by test runs (e.g. COFA merge tests writing x/y).
    """
    eng = get_active_engagement()
    return {
        "engagement_id": eng.engagement_id,
        "entity_a": {
            "id": eng.entity_a.id,
            "display_name": eng.entity_a.display_name,
        },
        "entity_b": {
            "id": eng.entity_b.id,
            "display_name": eng.entity_b.display_name,
        },
        "status": "active",
    }


# ---------------------------------------------------------------------------
# GET /api/dcl/triples/resolution-summary
# ---------------------------------------------------------------------------

@router.get("/api/dcl/triples/resolution-summary")
def triples_resolution_summary():
    """Resolution workspace aggregate stats."""
    # Try v2 table first, fall back to v1
    for table in ("resolution_workspaces_v2", "resolution_workspaces"):
        type_col = "domain" if table == "resolution_workspaces_v2" else "workspace_type"
        try:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT COUNT(*) FROM {table}")
                    total = cur.fetchone()[0]
                    if total == 0:
                        continue

                    cur.execute(
                        f"SELECT status, COUNT(*) FROM {table} GROUP BY status"
                    )
                    by_status = {r[0]: r[1] for r in cur.fetchall()}

                    cur.execute(
                        f"SELECT {type_col}, COUNT(*) FROM {table} GROUP BY {type_col}"
                    )
                    by_type = {r[0]: r[1] for r in cur.fetchall()}

                    # Recent decisions
                    decided_col = "decided_at" if table == "resolution_workspaces" else "updated_at"
                    cur.execute(
                        f"SELECT id, {type_col}, status, decided_by, {decided_col} "
                        f"FROM {table} "
                        f"WHERE status IN ('resolved', 'confirmed', 'rejected', 'escalated') "
                        f"ORDER BY {decided_col} DESC NULLS LAST LIMIT 10"
                    )
                    columns = [desc[0] for desc in cur.description]
                    recent = []
                    for row in cur.fetchall():
                        d = dict(zip(columns, row))
                        recent.append({
                            "workspace_id": str(d["id"]),
                            "type": d[type_col],
                            "decision": d["status"],
                            "decided_by": d.get("decided_by"),
                            "decided_at": d[decided_col].isoformat() if d.get(decided_col) else None,
                        })

                    return {
                        "total_workspaces": total,
                        "by_status": by_status,
                        "by_type": by_type,
                        "recent_decisions": recent,
                    }

        except Exception as e:
            logger.debug(f"[resolution-summary] Table {table} query failed: {e}")
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
    return {"run_id": run_id, "deactivated_count": count}
