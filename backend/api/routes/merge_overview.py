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
    """Make a value JSON-serializable."""
    if isinstance(val, Decimal):
        return float(val)
    if isinstance(val, datetime):
        return val.isoformat()
    if isinstance(val, bytes):
        return val.decode("utf-8", errors="replace")
    return val


# ---------------------------------------------------------------------------
# Entity resolution: query params → engagement_state → COFA distinct entities
# ---------------------------------------------------------------------------

def _resolve_entities(cur, acquirer_id: Optional[str], target_id: Optional[str]) -> tuple[str, str]:
    """Resolve acquirer and target entity IDs.

    Priority:
    1. Explicit query params
    2. engagement_state table (entity_a_id = acquirer, entity_b_id = target)
    3. Distinct entities from COFA triples (alphabetical)

    Raises HTTPException if fewer than 2 entities are available.
    """
    # 1. Explicit params
    if acquirer_id and target_id:
        return acquirer_id, target_id

    # 2. Engagement state
    try:
        cur.execute(
            "SELECT entity_a_id, entity_b_id FROM engagement_state "
            "ORDER BY created_at DESC LIMIT 1"
        )
        row = cur.fetchone()
        if row and row[0] and row[1]:
            return row[0], row[1]
    except Exception as e:
        logger.debug(f"[merge] engagement_state lookup failed (non-fatal): {e}")

    # 3. Distinct COFA entities
    cur.execute(
        "SELECT DISTINCT entity_id FROM semantic_triples "
        "WHERE is_active = true AND split_part(concept, '.', 1) = 'cofa' "
        "ORDER BY entity_id"
    )
    entities = [r[0] for r in cur.fetchall()]
    if len(entities) < 2:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Need at least 2 entities with COFA triples to show merge view. "
                f"Found {len(entities)}: {entities}. "
                f"Ingest COFA data for both entities first, or set an engagement via the engagement API."
            ),
        )
    return entities[0], entities[1]


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
            acq_id, tgt_id = _resolve_entities(cur, acquirer_id, target_id)

            # --- Section 1: Overview stats ---
            cur.execute(
                "SELECT entity_id, COUNT(*) AS cofa_count, MAX(created_at) AS last_ingest "
                "FROM semantic_triples "
                "WHERE is_active = true AND split_part(concept, '.', 1) = 'cofa' "
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
            cur.execute(
                "SELECT entity_id, concept, property, value, period "
                "FROM semantic_triples "
                "WHERE is_active = true AND split_part(concept, '.', 1) = 'cofa' "
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
            cur.execute(
                "SELECT DISTINCT ON (a.canonical_id) "
                "  a.concept AS acquirer_concept, b.concept AS target_concept, "
                "  a.canonical_id, a.resolution_confidence, a.source_field, a.resolution_method "
                "FROM semantic_triples a "
                "JOIN semantic_triples b ON a.canonical_id = b.canonical_id AND a.id != b.id "
                "WHERE a.is_active = true AND b.is_active = true "
                "  AND a.entity_id = %s AND b.entity_id = %s "
                "  AND a.canonical_id IS NOT NULL "
                "  AND split_part(a.concept, '.', 1) = 'cofa' "
                "  AND split_part(b.concept, '.', 1) = 'cofa'",
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

            # --- Section 4: Orphans (concepts without resolution matches) ---
            if has_matches:
                matched_acq_concepts = {r["acquirer_concept"] for r in match_rows}
                matched_tgt_concepts = {r["target_concept"] for r in match_rows}

                all_acq_concepts = {
                    c["concept"] for c in concept_map.values()
                    if len(c["acquirer_triples"]) > 0
                }
                all_tgt_concepts = {
                    c["concept"] for c in concept_map.values()
                    if len(c["target_triples"]) > 0
                }

                orphans = {
                    "show_section": True,
                    "acquirer_unmatched": sorted(all_acq_concepts - matched_acq_concepts),
                    "target_unmatched": sorted(all_tgt_concepts - matched_tgt_concepts),
                }
            else:
                orphans = {
                    "show_section": False,
                    "acquirer_unmatched": [],
                    "target_unmatched": [],
                }

    return {
        "acquirer": {"entity_id": acq_id, "display_name": _entity_display_name(acq_id)},
        "target": {"entity_id": tgt_id, "display_name": _entity_display_name(tgt_id)},
        "overview": overview,
        "comparison": comparison,
        "matches": matches,
        "orphans": orphans,
    }
