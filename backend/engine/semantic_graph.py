"""
In-memory semantic graph for query-time traversal.

Built at engine startup from existing data sources (normalizer output,
AAM edges, ontology config, contour map).  NOT a separate database —
this is a runtime data structure navigated by QueryResolver.
"""

from __future__ import annotations

import statistics
from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING, Optional

import yaml

from backend.engine.graph_types import (
    ConfidenceBreakdown,
    FieldLocation,
    GraphStats,
    JoinHop,
    JoinPath,
    ResolvedFilter,
    SGraphEdge,
    SGraphNode,
    SystemAuthority,
)
from backend.utils.log_utils import get_logger

if TYPE_CHECKING:
    from backend.domain import Mapping, SemanticEdge

logger = get_logger(__name__)

_CONFIG_DIR = Path(__file__).resolve().parent.parent.parent / "config"
_PAIRINGS_PATH = _CONFIG_DIR / "concept_dimension_pairings.yaml"
_CONTOUR_PATH = _CONFIG_DIR / "sample_contour.yaml"


class SemanticGraph:
    """In-memory semantic graph for query-time traversal."""

    def __init__(self) -> None:
        self.nodes: dict[str, SGraphNode] = {}
        self.edges: list[SGraphEdge] = []
        self._adjacency: dict[str, list[SGraphEdge]] = defaultdict(list)
        self._reverse_adj: dict[str, list[SGraphEdge]] = defaultdict(list)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _add_node(self, node: SGraphNode) -> None:
        if node.id not in self.nodes:
            self.nodes[node.id] = node

    def _add_edge(self, edge: SGraphEdge) -> None:
        self.edges.append(edge)
        self._adjacency[edge.source_id].append(edge)
        self._reverse_adj[edge.target_id].append(edge)

    # ------------------------------------------------------------------
    # Build methods (called at startup)
    # ------------------------------------------------------------------

    def load_from_normalizer(self, mappings: list[Mapping]) -> None:
        """Add ConceptNodes, FieldNodes, SystemNodes, and
        CLASSIFIED_AS + LIVES_IN edges from normalizer output."""
        for m in mappings:
            # System node
            sys_id = f"system:{m.source_system}"
            self._add_node(SGraphNode(
                id=sys_id, type="system", label=m.source_system,
            ))

            # Field node
            field_id = f"field:{m.source_system}.{m.source_table}.{m.source_field}"
            self._add_node(SGraphNode(
                id=field_id, type="field", label=f"{m.source_table}.{m.source_field}",
                metadata={
                    "system": m.source_system,
                    "table": m.source_table,
                    "field": m.source_field,
                },
            ))

            # Concept node
            concept_id = f"concept:{m.ontology_concept}"
            self._add_node(SGraphNode(
                id=concept_id, type="concept", label=m.ontology_concept,
            ))

            # CLASSIFIED_AS  field → concept
            self._add_edge(SGraphEdge(
                source_id=field_id, target_id=concept_id,
                type="CLASSIFIED_AS", confidence=m.confidence,
                provenance=m.method,
            ))

            # LIVES_IN  field → system
            self._add_edge(SGraphEdge(
                source_id=field_id, target_id=sys_id,
                type="LIVES_IN", confidence=1.0,
                provenance="normalizer",
            ))

    def load_from_aam(self, semantic_edges: list[SemanticEdge]) -> None:
        """Add MAPS_TO edges between FieldNodes from AAM."""
        for se in semantic_edges:
            src_field_id = (
                f"field:{se.source_system}.{se.source_object}.{se.source_field}"
            )
            tgt_field_id = (
                f"field:{se.target_system}.{se.target_object}.{se.target_field}"
            )

            # Ensure field and system nodes exist
            self._add_node(SGraphNode(
                id=src_field_id, type="field",
                label=f"{se.source_object}.{se.source_field}",
                metadata={"system": se.source_system, "table": se.source_object,
                          "field": se.source_field},
            ))
            self._add_node(SGraphNode(
                id=tgt_field_id, type="field",
                label=f"{se.target_object}.{se.target_field}",
                metadata={"system": se.target_system, "table": se.target_object,
                          "field": se.target_field},
            ))
            self._add_node(SGraphNode(
                id=f"system:{se.source_system}", type="system",
                label=se.source_system,
            ))
            self._add_node(SGraphNode(
                id=f"system:{se.target_system}", type="system",
                label=se.target_system,
            ))

            self._add_edge(SGraphEdge(
                source_id=src_field_id, target_id=tgt_field_id,
                type="MAPS_TO", confidence=se.confidence,
                provenance=se.extraction_source,
                metadata={"edge_type": se.edge_type,
                          "fabric_plane": se.fabric_plane},
            ))

    def load_from_ontology(self, pairings_path: Path | None = None) -> None:
        """Add DimensionNodes and SLICEABLE_BY edges from
        concept_dimension_pairings.yaml."""
        path = pairings_path or _PAIRINGS_PATH
        if not path.exists():
            logger.warning(f"[SemanticGraph] Pairings file not found: {path}")
            return

        data = yaml.safe_load(path.read_text())
        dimensions: list[str] = data.get("dimensions", [])
        pairings: dict = data.get("pairings", {})

        for dim in dimensions:
            dim_id = f"dimension:{dim}"
            self._add_node(SGraphNode(
                id=dim_id, type="dimension", label=dim,
            ))

        for concept_key, dims in pairings.items():
            concept_id = f"concept:{concept_key}"
            # Ensure concept node exists (may not if normalizer didn't
            # produce a mapping for this concept — still useful for
            # validity checks).
            self._add_node(SGraphNode(
                id=concept_id, type="concept", label=concept_key,
            ))
            for dim in (dims or []):
                dim_id = f"dimension:{dim}"
                self._add_edge(SGraphEdge(
                    source_id=concept_id, target_id=dim_id,
                    type="SLICEABLE_BY", confidence=1.0,
                    provenance="concept_dimension_pairings",
                ))

    def load_from_contour_map(
        self, contour_path: Path | None = None, contour_data: dict | None = None,
    ) -> None:
        """Add HIERARCHY_PARENT, AUTHORITATIVE_FOR, REPORTS_AS edges.

        In dev: load from sample_contour.yaml.
        In prod: pass contour_data from the approved contour map API.
        """
        if contour_data is None:
            path = contour_path or _CONTOUR_PATH
            if not path.exists():
                logger.warning(f"[SemanticGraph] Contour file not found: {path}")
                return
            contour_data = yaml.safe_load(path.read_text())

        # --- Hierarchy ---
        for dim_name, roots in (contour_data.get("hierarchy") or {}).items():
            self._load_hierarchy_tree(dim_name, roots, parent_id=None)

        # --- SOR authority ---
        for dim_name, auth in (contour_data.get("sor_authority") or {}).items():
            sys_id = f"system:{auth['system']}"
            dim_id = f"dimension:{dim_name}"
            self._add_node(SGraphNode(id=sys_id, type="system", label=auth["system"]))
            self._add_node(SGraphNode(id=dim_id, type="dimension", label=dim_name))
            self._add_edge(SGraphEdge(
                source_id=sys_id, target_id=dim_id,
                type="AUTHORITATIVE_FOR",
                confidence=auth.get("confidence", 0.8),
                provenance="contour_map",
            ))

        # --- Management overlay ---
        for overlay in (contour_data.get("management_overlay") or []):
            board_name = overlay["board_segment"]
            board_id = f"dimval:{board_name}"
            self._add_node(SGraphNode(
                id=board_id, type="dimension_value", label=board_name,
                metadata={"overlay": True},
            ))
            for target_name in overlay.get("maps_to", []):
                target_id = f"dimval:{target_name}"
                self._add_node(SGraphNode(
                    id=target_id, type="dimension_value", label=target_name,
                ))
                self._add_edge(SGraphEdge(
                    source_id=board_id, target_id=target_id,
                    type="REPORTS_AS", confidence=1.0,
                    provenance="management_overlay",
                ))

    def _load_hierarchy_tree(
        self, dimension: str, items: list[dict], parent_id: str | None,
    ) -> None:
        """Recursively load hierarchy nodes and HIERARCHY_PARENT edges."""
        for item in items:
            node_id = f"dimval:{item['name']}"
            self._add_node(SGraphNode(
                id=node_id, type="dimension_value", label=item["name"],
                metadata={"dimension": dimension, "contour_id": item.get("id", "")},
            ))
            if parent_id is not None:
                self._add_edge(SGraphEdge(
                    source_id=node_id, target_id=parent_id,
                    type="HIERARCHY_PARENT",
                    confidence=1.0,
                    provenance="contour_map",
                ))
            children = item.get("children", [])
            if children:
                self._load_hierarchy_tree(dimension, children, parent_id=node_id)

    # ------------------------------------------------------------------
    # Query methods
    # ------------------------------------------------------------------

    def find_concept_sources(self, concept_id: str) -> list[FieldLocation]:
        """Find all fields classified as this concept, ranked by confidence."""
        cid = f"concept:{concept_id}" if not concept_id.startswith("concept:") else concept_id
        results: list[FieldLocation] = []

        for edge in self._reverse_adj.get(cid, []):
            if edge.type != "CLASSIFIED_AS":
                continue
            field_node = self.nodes.get(edge.source_id)
            if field_node is None:
                continue
            meta = field_node.metadata
            results.append(FieldLocation(
                system=meta.get("system", ""),
                object_name=meta.get("table", ""),
                field=meta.get("field", ""),
                concept=concept_id.removeprefix("concept:"),
                confidence=edge.confidence,
            ))

        results.sort(key=lambda fl: fl.confidence, reverse=True)
        return results

    def check_dimension_validity(self, concept_id: str, dimension: str) -> bool:
        """Is this concept sliceable by this dimension?"""
        cid = f"concept:{concept_id}" if not concept_id.startswith("concept:") else concept_id
        dim_id = f"dimension:{dimension}"

        for edge in self._adjacency.get(cid, []):
            if edge.type == "SLICEABLE_BY" and edge.target_id == dim_id:
                return True
        return False

    def find_dimension_authority(self, dimension: str) -> Optional[SystemAuthority]:
        """Which system is authoritative for this dimension?"""
        dim_id = f"dimension:{dimension}"

        best: Optional[SGraphEdge] = None
        for edge in self._reverse_adj.get(dim_id, []):
            if edge.type == "AUTHORITATIVE_FOR":
                if best is None or edge.confidence > best.confidence:
                    best = edge

        if best is None:
            return None
        return SystemAuthority(
            system=best.source_id.removeprefix("system:"),
            dimension=dimension,
            confidence=best.confidence,
            source="contour_map",
        )

    def find_join_path(
        self, system_a: str, system_b: str, max_hops: int = 3,
    ) -> Optional[JoinPath]:
        """BFS for the shortest path connecting two systems via MAPS_TO edges.

        Returns the path with intermediate systems, join fields, and confidence.
        """
        if system_a == system_b:
            return JoinPath(
                hops=[], total_confidence=1.0,
                description=f"Same system: {system_a}",
            )

        # Collect all MAPS_TO edges, keyed by source system
        maps_to_edges: list[SGraphEdge] = [
            e for e in self.edges if e.type == "MAPS_TO"
        ]
        if not maps_to_edges:
            return None

        # Build a system-level adjacency from MAPS_TO edges
        # (system_a) --[field_a → field_b]--> (system_b)
        sys_adj: dict[str, list[tuple[str, SGraphEdge]]] = defaultdict(list)
        for e in maps_to_edges:
            src_node = self.nodes.get(e.source_id)
            tgt_node = self.nodes.get(e.target_id)
            if not src_node or not tgt_node:
                continue
            src_sys = src_node.metadata.get("system", "")
            tgt_sys = tgt_node.metadata.get("system", "")
            if src_sys and tgt_sys and src_sys != tgt_sys:
                sys_adj[src_sys].append((tgt_sys, e))
                # Bidirectional — AAM edges can be traversed both ways
                sys_adj[tgt_sys].append((src_sys, e))

        # BFS
        from collections import deque
        queue: deque[tuple[str, list[tuple[str, str, SGraphEdge]]]] = deque()
        queue.append((system_a, []))
        visited = {system_a}

        while queue:
            current, path = queue.popleft()
            if len(path) >= max_hops:
                continue

            for neighbor_sys, edge in sys_adj.get(current, []):
                if neighbor_sys in visited:
                    continue

                src_node = self.nodes.get(edge.source_id)
                tgt_node = self.nodes.get(edge.target_id)
                src_meta = src_node.metadata if src_node else {}
                tgt_meta = tgt_node.metadata if tgt_node else {}

                new_path = path + [(current, neighbor_sys, edge)]

                if neighbor_sys == system_b:
                    # Build JoinPath from BFS result
                    hops = []
                    total_conf = 1.0
                    for from_sys, to_sys, e in new_path:
                        s_node = self.nodes.get(e.source_id)
                        t_node = self.nodes.get(e.target_id)
                        s_m = s_node.metadata if s_node else {}
                        t_m = t_node.metadata if t_node else {}
                        hop = JoinHop(
                            from_system=from_sys,
                            from_field=f"{s_m.get('table','')}.{s_m.get('field','')}",
                            to_system=to_sys,
                            to_field=f"{t_m.get('table','')}.{t_m.get('field','')}",
                            via=e.metadata.get("edge_type", "MAPS_TO"),
                            confidence=e.confidence,
                        )
                        hops.append(hop)
                        total_conf *= e.confidence

                    desc_parts = [f"{h.from_system}→{h.to_system}" for h in hops]
                    return JoinPath(
                        hops=hops,
                        total_confidence=total_conf,
                        description=" → ".join(desc_parts),
                    )

                visited.add(neighbor_sys)
                queue.append((neighbor_sys, new_path))

        return None

    def resolve_dimension_filter(self, dimension: str, value: str) -> ResolvedFilter:
        """Resolve a dimension value through hierarchy and management overlay.

        'Cloud' → ['Cloud East', 'Cloud West'] via REPORTS_AS edge.
        """
        val_id = f"dimval:{value}"

        # Check REPORTS_AS (management overlay)
        overlay_targets: list[str] = []
        for edge in self._adjacency.get(val_id, []):
            if edge.type == "REPORTS_AS":
                target_node = self.nodes.get(edge.target_id)
                if target_node:
                    overlay_targets.append(target_node.label)

        if overlay_targets:
            return ResolvedFilter(
                dimension=dimension,
                original_value=value,
                resolved_values=overlay_targets,
                resolution_type="management_overlay",
            )

        # Check HIERARCHY_PARENT — find children of this node
        children = self.resolve_hierarchy(dimension, value)
        if children:
            return ResolvedFilter(
                dimension=dimension,
                original_value=value,
                resolved_values=children,
                resolution_type="hierarchy_expansion",
            )

        # Exact match — value used as-is
        return ResolvedFilter(
            dimension=dimension,
            original_value=value,
            resolved_values=[value],
            resolution_type="exact",
        )

    def resolve_hierarchy(self, dimension: str, value: str) -> list[str]:
        """Get all leaf values under a hierarchy node (children only)."""
        val_id = f"dimval:{value}"
        children: list[str] = []

        for edge in self._reverse_adj.get(val_id, []):
            if edge.type == "HIERARCHY_PARENT":
                child_node = self.nodes.get(edge.source_id)
                if child_node:
                    children.append(child_node.label)

        return children

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    @property
    def stats(self) -> GraphStats:
        """Node counts by type, edge counts by type, connectivity metrics."""
        type_counts: dict[str, int] = defaultdict(int)
        for node in self.nodes.values():
            type_counts[node.type] += 1

        edge_counts: dict[str, int] = defaultdict(int)
        for edge in self.edges:
            edge_counts[edge.type] += 1

        # Connected systems: systems with at least one MAPS_TO path
        systems_with_maps = set()
        for e in self.edges:
            if e.type == "MAPS_TO":
                src = self.nodes.get(e.source_id)
                tgt = self.nodes.get(e.target_id)
                if src:
                    systems_with_maps.add(src.metadata.get("system", ""))
                if tgt:
                    systems_with_maps.add(tgt.metadata.get("system", ""))
        systems_with_maps.discard("")

        # Average confidence of MAPS_TO edges
        maps_to_confs = [e.confidence for e in self.edges if e.type == "MAPS_TO"]
        avg_conf = statistics.mean(maps_to_confs) if maps_to_confs else 0.0

        return GraphStats(
            concept_nodes=type_counts.get("concept", 0),
            dimension_nodes=type_counts.get("dimension", 0),
            system_nodes=type_counts.get("system", 0),
            field_nodes=type_counts.get("field", 0),
            dimension_value_nodes=type_counts.get("dimension_value", 0),
            edges_by_type=dict(edge_counts),
            connected_systems=len(systems_with_maps),
            avg_path_confidence=round(avg_conf, 4),
        )
