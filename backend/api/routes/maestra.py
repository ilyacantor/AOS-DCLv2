"""
Maestra status endpoint — read-only window into DCL semantic layer state.

Returns structured JSON for a given tenant describing:
  - concepts (metrics) count
  - dimensions (entities) count
  - valid pairings count
  - entities with list
  - extraction rules status
  - last update timestamp
  - health flag
"""

from fastapi import APIRouter, Query
from typing import Any, Dict, List

from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

router = APIRouter(tags=["Maestra"])


@router.get("/maestra/status")
def maestra_status(tenant_id: str = Query("default")) -> Dict[str, Any]:
    """Return structured DCL semantic layer state for Maestra."""
    from backend.api.semantic_export import (
        PUBLISHED_METRICS,
        PUBLISHED_ENTITIES,
        build_metric_entity_matrix,
    )
    from backend.engine.metric_materializer import _load_extraction_rules
    from backend.core.mode_state import get_current_mode

    # --- Concepts (published metrics) ---
    concept_count = len(PUBLISHED_METRICS)

    # --- Dimensions (published entities/dimensions) ---
    dimension_count = len(PUBLISHED_ENTITIES)

    # --- Pairings (valid metric-dimension combinations) ---
    matrix = build_metric_entity_matrix()
    pairing_count = sum(len(dims) for dims in matrix.values())

    # --- Entities (dimension definitions with their IDs) ---
    entity_list = [e.id for e in PUBLISHED_ENTITIES]

    # --- Extraction rules ---
    rules = _load_extraction_rules()
    rule_count = len(rules)
    active_count = rule_count
    errored_count = 0

    # --- Last update (from mode state) ---
    mode = get_current_mode()
    last_update_at = mode.last_updated

    # --- Health: DCL is healthy if we have metrics + dimensions loaded ---
    healthy = concept_count > 0 and dimension_count > 0

    return {
        "module": "dcl",
        "tenant_id": tenant_id,
        "concepts": {"count": concept_count},
        "dimensions": {"count": dimension_count},
        "pairings": {"count": pairing_count},
        "entities": {"count": dimension_count, "list": entity_list},
        "extraction_rules": {
            "count": rule_count,
            "active": active_count,
            "errored": errored_count,
        },
        "last_update_at": last_update_at,
        "healthy": healthy,
    }
