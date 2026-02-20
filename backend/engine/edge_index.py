"""
In-memory index of AAM semantic edges for fast field lookup.

Used by the normalizer's Tier 0: before any heuristic/RAG/LLM classification,
check whether AAM already has an explicit mapping for a given field.
"""

from typing import Dict, List, Optional, Tuple
from backend.domain import SemanticEdge


class EdgeIndex:
    """In-memory index of AAM semantic edges for fast field lookup."""

    def __init__(self, edges: List[SemanticEdge]):
        self._edges = edges
        # (system, object, field) → list of edges where this is the source
        self._by_source: Dict[Tuple[str, str, str], List[SemanticEdge]] = {}
        # (system, object, field) → list of edges where this is the target
        self._by_target: Dict[Tuple[str, str, str], List[SemanticEdge]] = {}

        for edge in edges:
            src_key = (
                edge.source_system.lower(),
                edge.source_object.lower(),
                edge.source_field.lower(),
            )
            tgt_key = (
                edge.target_system.lower(),
                edge.target_object.lower(),
                edge.target_field.lower(),
            )
            self._by_source.setdefault(src_key, []).append(edge)
            self._by_target.setdefault(tgt_key, []).append(edge)

    def lookup(
        self, system: str, object_name: str, field: str
    ) -> Optional[SemanticEdge]:
        """
        Find the highest-confidence AAM edge for a given field.

        Checks source-side first (this field maps TO something),
        then target-side (this field is mapped FROM something).
        Returns None if no edge exists.
        """
        key = (system.lower(), object_name.lower(), field.lower())

        candidates: List[SemanticEdge] = []
        candidates.extend(self._by_source.get(key, []))
        candidates.extend(self._by_target.get(key, []))

        if not candidates:
            return None

        return max(candidates, key=lambda e: e.confidence)

    def get_related_fields(
        self, system: str, object_name: str, field: str
    ) -> List[SemanticEdge]:
        """Return ALL edges involving this field (both directions)."""
        key = (system.lower(), object_name.lower(), field.lower())
        related: List[SemanticEdge] = []
        related.extend(self._by_source.get(key, []))
        related.extend(self._by_target.get(key, []))
        return related

    @property
    def coverage(self) -> Dict:
        """Stats: total edges, edges by plane, edges by confidence tier."""
        by_plane: Dict[str, int] = {}
        by_confidence: Dict[str, int] = {"high_90": 0, "mid_80": 0, "low": 0}

        for edge in self._edges:
            plane = edge.fabric_plane
            by_plane[plane] = by_plane.get(plane, 0) + 1

            if edge.confidence >= 0.9:
                by_confidence["high_90"] += 1
            elif edge.confidence >= 0.8:
                by_confidence["mid_80"] += 1
            else:
                by_confidence["low"] += 1

        return {
            "total_edges": len(self._edges),
            "by_plane": by_plane,
            "by_confidence": by_confidence,
        }

    @property
    def empty(self) -> bool:
        return len(self._edges) == 0
