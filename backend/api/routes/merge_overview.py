"""
Merge overview endpoint — read-only COFA side-by-side view.

GET /api/dcl/merge/overview  — COFA triples for acquirer vs target
"""

from datetime import datetime
from decimal import Decimal

from fastapi import APIRouter, HTTPException, Query
from typing import Optional

from backend.core.db import get_connection
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

router = APIRouter(tags=["Merge Overview"])


def _entity_display_name(entity_id: str) -> str:
    """Human-readable display name from entity_id.

    Duplicated from triple_monitor to avoid cross-module import coupling.
    If the id already has mixed case or hyphens, it's a readable name.
    """
    if not entity_id:
        return entity_id
    if any(c.isupper() for c in entity_id) or "-" in entity_id:
        return entity_id
    return entity_id.replace("_", " ").title()


def _serialize_value(val):
    """Make a value JSON-serializable.

    Also strips embedded JSON quotes from double-encoded jsonb strings
    (e.g. '"Cash & Equivalents"' → 'Cash & Equivalents').
    """
    if isinstance(val, Decimal):
        return float(val)
    if isinstance(val, datetime):
        return val.isoformat()
    if isinstance(val, bytes):
        return val.decode("utf-8", errors="replace")
    if isinstance(val, str) and len(val) > 2 and val.startswith('"') and val.endswith('"'):
        return val[1:-1]
    return val


# ---------------------------------------------------------------------------
# Entity resolution: query params → engagement_state → COFA distinct entities
# ---------------------------------------------------------------------------

def _get_cofa_entity_ids(cur) -> list[str]:
    """Return distinct entity_ids that have active COFA-related triples.

    Includes coa (chart of accounts) and all cofa-prefixed domains so the
    merge tab works both before and after Maestra runs.
    """
    cur.execute(
        "SELECT DISTINCT entity_id FROM semantic_triples "
        "WHERE is_active = true "
        "  AND (split_part(concept, '.', 1) = 'coa' "
        "       OR split_part(concept, '.', 1) LIKE 'cofa%%') "
        "ORDER BY entity_id"
    )
    return [r[0] for r in cur.fetchall()]


def _resolve_entities(cur, acquirer_id: Optional[str], target_id: Optional[str]) -> tuple[str, str, Optional[str]]:
    """Resolve acquirer and target entity IDs plus engagement_id.

    Priority:
    1. Explicit query params (validated against triple store)
    2. engagement_state table, mapped to actual COFA entity_ids
    3. Distinct entities from COFA triples (alphabetical)

    Returns (acquirer_id, target_id, engagement_id).
    engagement_id is None when entities were resolved without engagement_state.

    Raises HTTPException if fewer than 2 entities have COFA triples.
    """
    cofa_entities = _get_cofa_entity_ids(cur)

    if len(cofa_entities) < 2:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Need at least 2 entities with COFA triples to show merge view. "
                f"Found {len(cofa_entities)}: {cofa_entities}. "
                f"Ingest COFA data for both entities first, or set an engagement via the engagement API."
            ),
        )

    # Look up engagement_state regardless — we need the engagement_id
    eng_id, engagement_a, engagement_b = None, None, None
    try:
        cur.execute(
            "SELECT engagement_id, entity_a_id, entity_b_id FROM engagement_state "
            "ORDER BY created_at DESC LIMIT 1"
        )
        row = cur.fetchone()
        if row and row[1] and row[2]:
            eng_id, engagement_a, engagement_b = row[0], row[1], row[2]
    except Exception as e:
        logger.debug(f"[merge] engagement_state lookup failed (non-fatal): {e}")

    # 1. Explicit params — validate they exist in triple store
    if acquirer_id and target_id:
        missing = [eid for eid in (acquirer_id, target_id) if eid not in cofa_entities]
        if missing:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Requested entity IDs {missing} have no COFA triples in the triple store. "
                    f"Available COFA entities: {cofa_entities}. "
                    f"Check entity_id values — they must match what was ingested."
                ),
            )
        return acquirer_id, target_id, eng_id

    # 2. Engagement state — map to actual COFA entity_ids
    if engagement_a and engagement_b:
        # Direct match — engagement IDs exist in triple store
        if engagement_a in cofa_entities and engagement_b in cofa_entities:
            return engagement_a, engagement_b, eng_id

        # Case-insensitive match — engagement may use different casing
        cofa_lower = {e.lower(): e for e in cofa_entities}
        mapped_a = cofa_lower.get(engagement_a.lower())
        mapped_b = cofa_lower.get(engagement_b.lower())
        if mapped_a and mapped_b and mapped_a != mapped_b:
            logger.info(
                f"[merge] Mapped engagement entity IDs to COFA triple store: "
                f"'{engagement_a}' → '{mapped_a}', '{engagement_b}' → '{mapped_b}'"
            )
            return mapped_a, mapped_b, eng_id

        # Engagement IDs don't match triple store — log clearly and fall through
        logger.warning(
            f"[merge] engagement_state entity IDs ('{engagement_a}', '{engagement_b}') "
            f"do not match any COFA entity_ids in the triple store: {cofa_entities}. "
            f"Falling through to COFA entity discovery. "
            f"Fix: ensure engagement entity IDs match the entity_id values used during Farm ingestion."
        )

    # 3. First two COFA entities alphabetically
    return cofa_entities[0], cofa_entities[1], eng_id


# ---------------------------------------------------------------------------
# GET /api/dcl/merge/overview
# ---------------------------------------------------------------------------

@router.get("/api/dcl/merge/overview")
def merge_overview(
    acquirer_id: Optional[str] = Query(None),
    target_id: Optional[str] = Query(None),
):
    """COFA merge overview — side-by-side comparison of two entities."""
    with get_connection() as conn:
        if conn is None:
            raise HTTPException(
                status_code=503,
                detail=(
                    "merge/overview failed: database connection unavailable. "
                    "Check DATABASE_URL and Supabase connectivity."
                ),
            )
        with conn.cursor() as cur:
            # --- Entity resolution ---
            acq_id, tgt_id, eng_id = _resolve_entities(cur, acquirer_id, target_id)

            # --- Section 1: Overview stats ---
            # Count both coa (source accounts) and cofa-prefixed (mapping results) triples.
            cur.execute(
                "SELECT entity_id, COUNT(*) AS cofa_count, MAX(created_at) AS last_ingest "
                "FROM semantic_triples "
                "WHERE is_active = true "
                "  AND (split_part(concept, '.', 1) = 'coa' "
                "       OR split_part(concept, '.', 1) LIKE 'cofa%%') "
                "  AND entity_id IN (%s, %s) "
                "GROUP BY entity_id",
                (acq_id, tgt_id),
            )
            entity_stats = {}
            for row in cur.fetchall():
                entity_stats[row[0]] = {
                    "entity_id": row[0],
                    "display_name": _entity_display_name(row[0]),
                    "cofa_count": row[1],
                    "last_ingest": row[2].isoformat() if row[2] else None,
                }

            # Ensure both entities appear even with zero COFA triples
            for eid in (acq_id, tgt_id):
                if eid not in entity_stats:
                    entity_stats[eid] = {
                        "entity_id": eid,
                        "display_name": _entity_display_name(eid),
                        "cofa_count": 0,
                        "last_ingest": None,
                    }

            total_cofa = sum(e["cofa_count"] for e in entity_stats.values())

            overview = {
                "entities": [entity_stats[acq_id], entity_stats[tgt_id]],
                "total_cofa_count": total_cofa,
            }

            # --- Section 2: Side-by-side comparison ---
            # Use CoA (chart of accounts) triples for the account comparison,
            # not COFA conflict triples.  COFA conflicts are shown in section 3.
            cur.execute(
                "SELECT entity_id, concept, property, value, period "
                "FROM semantic_triples "
                "WHERE is_active = true AND split_part(concept, '.', 1) = 'coa' "
                "  AND entity_id IN (%s, %s) "
                "ORDER BY concept, entity_id, property",
                (acq_id, tgt_id),
            )
            # Group by concept
            concept_map: dict[str, dict] = {}
            for row in cur.fetchall():
                eid, concept, prop, value, period = row
                if concept not in concept_map:
                    concept_map[concept] = {
                        "concept": concept,
                        "acquirer_triples": [],
                        "target_triples": [],
                    }
                triple_entry = {
                    "property": prop,
                    "value": _serialize_value(value),
                    "period": period,
                }
                if eid == acq_id:
                    concept_map[concept]["acquirer_triples"].append(triple_entry)
                else:
                    concept_map[concept]["target_triples"].append(triple_entry)

            comparison = {
                "concepts": list(concept_map.values()),
            }

            # --- Section 3: Resolution matches (canonical_id join) ---
            # Match COFA-related domains: cofa, cofa_mapping, cofa_conflict, cofa_unified
            cur.execute(
                "SELECT DISTINCT ON (a.canonical_id) "
                "  a.concept AS acquirer_concept, b.concept AS target_concept, "
                "  a.canonical_id, a.resolution_confidence, a.source_field, a.resolution_method "
                "FROM semantic_triples a "
                "JOIN semantic_triples b ON a.canonical_id = b.canonical_id AND a.id != b.id "
                "WHERE a.is_active = true AND b.is_active = true "
                "  AND a.entity_id = %s AND b.entity_id = %s "
                "  AND a.canonical_id IS NOT NULL "
                "  AND split_part(a.concept, '.', 1) LIKE 'cofa%%' "
                "  AND split_part(b.concept, '.', 1) LIKE 'cofa%%'",
                (acq_id, tgt_id),
            )
            columns = [desc[0] for desc in cur.description]
            match_rows = []
            for row in cur.fetchall():
                d = dict(zip(columns, row))
                match_rows.append({
                    "acquirer_concept": d["acquirer_concept"],
                    "target_concept": d["target_concept"],
                    "canonical_id": str(d["canonical_id"]) if d["canonical_id"] else None,
                    "resolution_confidence": (
                        float(d["resolution_confidence"])
                        if d["resolution_confidence"] is not None
                        else None
                    ),
                    "source_field": d["source_field"],
                    "resolution_method": d["resolution_method"],
                })

            has_matches = len(match_rows) > 0
            matches = {
                "has_matches": has_matches,
                "rows": match_rows,
                "message": (
                    f"{len(match_rows)} COFA account(s) resolved across entities"
                    if has_matches
                    else "No cross-entity resolution matches found yet. Run entity resolution to match COFA accounts."
                ),
            }

            # --- Section 4: Orphans (CoA accounts without COFA mappings) ---
            # CoA concepts (coa.*) and mapping concepts (cofa_mapping.*) use
            # different namespaces, so we cannot compare concept names directly.
            # Instead, compare distinct concept counts per entity: each CoA
            # account produces exactly one cofa_mapping concept on its entity's
            # side (entity-specific accounts get mapping_target/source = N/A).
            # If the counts match, all accounts are mapped → 0 orphans.
            cur.execute(
                "SELECT entity_id, "
                "  COUNT(DISTINCT CASE WHEN split_part(concept, '.', 1) = 'coa' "
                "        THEN concept END) AS coa_count, "
                "  COUNT(DISTINCT CASE WHEN split_part(concept, '.', 1) = 'cofa_mapping' "
                "        THEN concept END) AS mapping_count "
                "FROM semantic_triples "
                "WHERE is_active = true AND entity_id IN (%s, %s) "
                "  AND split_part(concept, '.', 1) IN ('coa', 'cofa_mapping') "
                "GROUP BY entity_id",
                (acq_id, tgt_id),
            )
            coverage = {}
            for row in cur.fetchall():
                coverage[row[0]] = {"coa": row[1], "mapped": row[2]}

            acq_cov = coverage.get(acq_id, {"coa": 0, "mapped": 0})
            tgt_cov = coverage.get(tgt_id, {"coa": 0, "mapped": 0})

            acq_gap = max(0, acq_cov["coa"] - acq_cov["mapped"])
            tgt_gap = max(0, tgt_cov["coa"] - tgt_cov["mapped"])
            has_orphans = acq_gap > 0 or tgt_gap > 0

            orphans = {
                "show_section": has_orphans,
                "acquirer_unmatched_count": acq_gap,
                "target_unmatched_count": tgt_gap,
                "acquirer_coa_total": acq_cov["coa"],
                "acquirer_mapped": acq_cov["mapped"],
                "target_coa_total": tgt_cov["coa"],
                "target_mapped": tgt_cov["mapped"],
                "message": (
                    f"Acquirer: {acq_cov['mapped']}/{acq_cov['coa']} mapped, "
                    f"Target: {tgt_cov['mapped']}/{tgt_cov['coa']} mapped"
                ),
            }

    return {
        "engagement_id": eng_id,
        "acquirer": {"entity_id": acq_id, "display_name": _entity_display_name(acq_id)},
        "target": {"entity_id": tgt_id, "display_name": _entity_display_name(tgt_id)},
        "overview": overview,
        "comparison": comparison,
        "matches": matches,
        "orphans": orphans,
    }
