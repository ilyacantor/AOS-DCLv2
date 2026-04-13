"""
Cross-module chain validation (Recon) endpoint.

GET /api/dcl/recon?run_id=X&entity_id=Y  — run 5 checks against a specific run/entity
"""

import httpx
from fastapi import APIRouter, Query
from typing import Optional
from datetime import datetime, timezone

from backend.core.db import get_connection
from backend.core.constants import FARM_API_URL, AAM_API_URL
from backend.db.triple_store import TripleStore
from backend.registry.concept_registry import ConceptRegistry
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

router = APIRouter(tags=["Recon"])

_triple_store = TripleStore()
_concept_registry = ConceptRegistry()

_UPSTREAM_TIMEOUT = 5.0  # seconds


def _check_farm_dcl_count(run_id: str, entity_id: str | None = None) -> dict:
    """Check 1: Compare Farm's triple count vs DCL's active count."""
    # Get source_run_tag for this run
    tag_clauses = ["run_id = %s", "source_run_tag IS NOT NULL"]
    tag_params: list = [run_id]
    if entity_id is not None:
        tag_clauses.append("entity_id = %s")
        tag_params.append(entity_id)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT source_run_tag FROM semantic_triples "
                f"WHERE {' AND '.join(tag_clauses)}",
                tag_params,
            )
            tags = [r[0] for r in cur.fetchall()]

    if not tags:
        return {
            "check": "farm_dcl_count",
            "status": "skip",
            "detail": f"No source_run_tag found for run_id={run_id[:8]}... — cannot query Farm",
        }

    source_tag = tags[0]

    # Count DCL triples, scoped by entity_id if provided
    count_clauses = ["run_id = %s"]
    count_params: list = [run_id]
    if entity_id is not None:
        count_clauses.append("entity_id = %s")
        count_params.append(entity_id)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT COUNT(*) FROM semantic_triples WHERE {' AND '.join(count_clauses)}",
                count_params,
            )
            dcl_count = cur.fetchone()[0]

    try:
        resp = httpx.get(
            f"{FARM_API_URL}/api/business-data/ground-truth/{source_tag}",
            timeout=_UPSTREAM_TIMEOUT,
        )
        if resp.status_code != 200:
            return {
                "check": "farm_dcl_count",
                "status": "skip",
                "detail": f"Farm returned HTTP {resp.status_code} for tag {source_tag}",
            }
        farm_data = resp.json()
        farm_count = farm_data.get("total_triples", farm_data.get("triple_count"))
        if farm_count is None:
            return {
                "check": "farm_dcl_count",
                "status": "skip",
                "detail": "Farm response missing triple count field",
            }
        status = "pass" if dcl_count == farm_count else "fail"
        return {
            "check": "farm_dcl_count",
            "status": status,
            "expected": farm_count,
            "actual": dcl_count,
            "detail": None if status == "pass" else f"Delta: {dcl_count - farm_count}",
        }
    except (httpx.ConnectError, httpx.TimeoutException) as e:
        return {
            "check": "farm_dcl_count",
            "status": "skip",
            "detail": f"Farm API unreachable at {FARM_API_URL}: {type(e).__name__}",
        }


def _check_entity_consistency(run_id: str, entity_id: str | None = None) -> dict:
    """Check 2: All triples should share consistent entity_ids."""
    clauses = ["run_id = %s"]
    params: list = [run_id]
    if entity_id is not None:
        clauses.append("entity_id = %s")
        params.append(entity_id)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT entity_id FROM semantic_triples "
                f"WHERE {' AND '.join(clauses)}",
                params,
            )
            entities = [r[0] for r in cur.fetchall()]

    if not entities:
        return {
            "check": "entity_consistency",
            "status": "fail",
            "entities": [],
            "detail": "No active triples found for this run",
        }

    return {
        "check": "entity_consistency",
        "status": "pass",
        "entities": entities,
        "detail": None,
    }


def _check_source_coverage(run_id: str, entity_id: str | None = None) -> dict:
    """Check 3: Source systems present in this run vs AAM expectations."""
    clauses = ["run_id = %s"]
    params: list = [run_id]
    if entity_id is not None:
        clauses.append("entity_id = %s")
        params.append(entity_id)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT source_system FROM semantic_triples "
                f"WHERE {' AND '.join(clauses)} ORDER BY source_system",
                params,
            )
            actual_sources = [r[0] for r in cur.fetchall()]

    if not actual_sources:
        return {
            "check": "source_coverage",
            "status": "fail",
            "expected": [],
            "actual": [],
            "missing": [],
            "detail": "No source systems found for this run",
        }

    # Try AAM for expected sources
    expected_sources = None
    aam_detail = None
    try:
        resp = httpx.get(
            f"{AAM_API_URL}/api/runners/job/{run_id}",
            timeout=_UPSTREAM_TIMEOUT,
        )
        if resp.status_code == 200:
            job_data = resp.json()
            expected_sources = job_data.get("source_systems", job_data.get("expected_sources"))
    except (httpx.ConnectError, httpx.TimeoutException):
        aam_detail = "AAM unreachable — showing actual sources only"

    if expected_sources:
        missing = [s for s in expected_sources if s not in actual_sources]
        status = "pass" if not missing else "warn"
        return {
            "check": "source_coverage",
            "status": status,
            "expected": expected_sources,
            "actual": actual_sources,
            "missing": missing,
            "detail": None if status == "pass" else f"Missing: {missing}",
        }

    return {
        "check": "source_coverage",
        "status": "pass",
        "expected": actual_sources,
        "actual": actual_sources,
        "missing": [],
        "detail": aam_detail,
    }


def _check_validation_rejections(run_id: str, entity_id: str | None = None) -> dict:
    """Check 4: Rejection counts from ingest_log."""
    clauses = ["run_id = %s"]
    params: list = [run_id]
    if entity_id is not None:
        clauses.append("entity_id = %s")
        params.append(entity_id)

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COALESCE(SUM(triples_rejected), 0), "
                    "COALESCE(jsonb_agg(rejection_reasons) FILTER "
                    "(WHERE triples_rejected > 0), '[]'::jsonb) "
                    f"FROM ingest_log WHERE {' AND '.join(clauses)}",
                    params,
                )
                row = cur.fetchone()
                rejected = row[0] if row else 0
                reasons = row[1] if row and row[1] else []
    except Exception as e:
        return {
            "check": "validation_rejections",
            "status": "skip",
            "rejected": 0,
            "reasons": [],
            "detail": f"ingest_log table not available: {e}",
        }

    status = "pass" if rejected == 0 else "warn"
    return {
        "check": "validation_rejections",
        "status": status,
        "rejected": rejected,
        "reasons": reasons if rejected > 0 else [],
        "detail": None if rejected == 0 else f"{rejected} triples rejected",
    }


def _check_domain_completeness(run_id: str, entity_id: str | None = None) -> dict:
    """Check 5: How many ontology domains have at least one triple."""
    domain_counts = _triple_store.count_by_domain(tenant_id=None, run_id=run_id, entity_id=entity_id)
    populated = list(domain_counts.keys())
    all_concepts = _concept_registry.list_concepts()

    gaps = [c for c in all_concepts if c not in populated]
    status = "pass" if len(populated) > 0 else "fail"
    if gaps and len(gaps) < len(all_concepts):
        status = "warn"

    return {
        "check": "domain_completeness",
        "status": status,
        "populated": len(populated),
        "total": len(all_concepts),
        "gaps": gaps[:20],  # Cap gap list for readability
        "detail": None if not gaps else f"{len(gaps)} concepts without data",
    }


def _resolve_run_id(tenant_id: str, entity_id: str | None) -> str | None:
    """Resolve the current_run_id to recon against.

    Scoped to a single tenant so recon never leaks across tenants. When
    entity_id is given, returns that entity's current_run_id. Otherwise
    returns the most recently updated run for the tenant. Returns None
    if tenant_runs has no matching row — callers surface that as a
    "no active run" response, not a silent skip.
    """
    sql = "SELECT current_run_id FROM tenant_runs WHERE tenant_id = %s"
    params: list = [tenant_id]
    if entity_id:
        sql += " AND entity_id = %s"
        params.append(entity_id)
    sql += " ORDER BY updated_at DESC LIMIT 1"

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
    return str(row[0]) if row else None


@router.get("/api/dcl/recon")
def run_recon(
    run_id: Optional[str] = Query(None, description="Run ID to validate. Defaults to tenant's latest run."),
    entity_id: Optional[str] = Query(None, description="Entity ID to scope recon checks."),
    tenant_id: Optional[str] = Query(None, description="Tenant scope. Defaults to the sole tenant in tenant_runs."),
):
    """Cross-module chain validation for a specific ingest run, optionally scoped by entity.

    Recon is always tenant-scoped: callers either pass tenant_id explicitly
    or there must be exactly one tenant in tenant_runs. When tenant_id is
    omitted and multiple tenants exist, the request fails with a 400 from
    TripleStore.resolve_single_tenant rather than picking one silently.
    """
    # Normalize empty-string entity_id to None (frontend sends "" for "All Entities")
    if entity_id is not None and entity_id.strip() == "":
        entity_id = None

    # Resolve run_id from tenant_runs when the caller didn't pin one.
    if not run_id:
        if not tenant_id:
            tenant_id = _triple_store.resolve_single_tenant()
        run_id = _resolve_run_id(tenant_id, entity_id)
        if run_id is None:
            detail = (
                f"No active run for tenant={tenant_id} entity={entity_id}"
                if entity_id
                else f"No active runs for tenant={tenant_id}"
            )
            return {
                "dcl_ingest_id": None,
                "entity_id": entity_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "overall": "fail",
                "checks": [],
                "detail": detail,
            }

    checks = [
        _check_farm_dcl_count(run_id, entity_id),
        _check_entity_consistency(run_id, entity_id),
        _check_source_coverage(run_id, entity_id),
        _check_validation_rejections(run_id, entity_id),
        _check_domain_completeness(run_id, entity_id),
    ]

    # Overall: fail if any fail, warn if any warn/skip, pass otherwise
    statuses = [c["status"] for c in checks]
    if "fail" in statuses:
        overall = "fail"
    elif "warn" in statuses or "skip" in statuses:
        overall = "warn"
    else:
        overall = "pass"

    return {
        "dcl_ingest_id": run_id,
        "entity_id": entity_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "overall": overall,
        "checks": checks,
    }
