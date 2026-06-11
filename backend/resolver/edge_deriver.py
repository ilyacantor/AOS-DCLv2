"""Edge derivation from record structure (ContextOS Gate 1B, §7).

DCL classifies RELATIONSHIPS from the same raw records it classifies values
from — relationship semantics live here, never in AAM (transport-only).

Derivations are structural, not speculative — an edge is derived only where
the record shape itself asserts the relationship:

  headcount_by_department: {dept: n}   — the org's workforce is structured into
      these departments → (department:<dept>) BELONGS_TO (org_unit:<entity_id>)
  uptime_pct_by_service: {svc: pct}    — the org operates these services
      → (org_unit:<entity_id>) HAS (service:<svc>)

Provenance: each derived edge carries the pipe's source_system/pipe_id/
fabric_plane and the exact source_field it was derived from; derivation=
'derived'; confidence 'high' (structural inference from record shape, not an
exact source assertion).
"""

from typing import Any

from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

_CONF_SCORE = 0.90
_CONF_TIER = "high"

# field name -> (relationship builder semantics)
_DERIVABLE_FIELDS = ("headcount_by_department", "uptime_pct_by_service")


def derive_edges_from_pipes(entity_id: str, pipes: list[dict]) -> list[dict]:
    """Derive entity↔entity edges from record structure across a pipes batch.

    Deduplicates within the batch (a per-period records list re-asserts the
    same membership every period — one edge per distinct relationship, the
    latest occurrence's provenance wins deterministically by iteration order).
    Returns edge payload dicts in EdgeStore.assert_edges shape (run_id is
    stamped by the caller — it owns the ingest identity).
    """
    out: dict[tuple, dict] = {}
    for pipe in pipes:
        source_system = pipe.get("source_system")
        fabric_plane = pipe.get("fabric_plane")
        fabric_product = pipe.get("fabric_product")
        pipe_id = pipe.get("pipe_id")
        for record in pipe.get("records") or []:
            if not isinstance(record, dict):
                continue
            for fname in _DERIVABLE_FIELDS:
                members = record.get(fname)
                if not isinstance(members, dict) or not members:
                    continue
                for member in members:
                    member_key = str(member).strip()
                    if not member_key:
                        continue
                    if fname == "headcount_by_department":
                        edge = {
                            "src_type": "department", "src_key": member_key,
                            "edge_type": "BELONGS_TO",
                            "dst_type": "org_unit", "dst_key": entity_id,
                        }
                    else:  # uptime_pct_by_service
                        edge = {
                            "src_type": "org_unit", "src_key": entity_id,
                            "edge_type": "HAS",
                            "dst_type": "service", "dst_key": member_key,
                        }
                    coord = (edge["src_type"], edge["src_key"], edge["edge_type"],
                             edge["dst_type"], edge["dst_key"])
                    out[coord] = {
                        **edge,
                        "properties": None,
                        "source_system": source_system,
                        "source_table": f"fabric_via:{source_system}",
                        "source_field": fname,
                        "pipe_id": pipe_id,
                        "source_run_tag": None,
                        "confidence_score": _CONF_SCORE,
                        "confidence_tier": _CONF_TIER,
                        "fabric_plane": fabric_plane,
                        "fabric_product": fabric_product,
                        "derivation": "derived",
                    }
    edges = list(out.values())
    if edges:
        logger.info(
            "[edge_deriver] derived %d distinct edge(s) for entity=%s from %d pipe(s)",
            len(edges), entity_id, len(pipes),
        )
    return edges
