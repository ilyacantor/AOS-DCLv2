"""
Lineage and Impact Graph Service for NLQ Semantic Layer.

Tracks dependencies between semantic layer objects:
- Definition -> Events -> Bindings -> Source Systems
- Enables "What breaks if I deprecate this event?" analysis
- Provides upstream and downstream dependency queries
"""

from typing import List, Dict, Any, Optional, Set, Tuple
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict

from backend.utils.log_utils import get_logger
from backend.nlq.persistence import NLQPersistence

logger = get_logger(__name__)


class NodeType(Enum):
    """Types of nodes in the lineage graph."""
    DEFINITION = "definition"
    EVENT = "event"
    ENTITY = "entity"
    BINDING = "binding"
    SOURCE_SYSTEM = "source_system"


class EdgeType(Enum):
    """Types of edges in the lineage graph."""
    REQUIRES = "requires"  # definition requires event
    PRODUCES = "produces"  # event produces dimension
    JOINS_TO = "joins_to"  # definition joins to entity
    BINDS_TO = "binds_to"  # binding binds to event
    SOURCES_FROM = "sources_from"  # binding sources from system
    GROUPS_BY = "groups_by"  # definition groups by entity


@dataclass
class LineageNode:
    """A node in the lineage graph."""
    id: str
    node_type: NodeType
    label: str
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "type": self.node_type.value,
            "label": self.label,
            "metadata": self.metadata,
        }


@dataclass
class LineageEdge:
    """An edge in the lineage graph."""
    source_id: str
    target_id: str
    edge_type: EdgeType
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source": self.source_id,
            "target": self.target_id,
            "type": self.edge_type.value,
            "metadata": self.metadata,
        }


@dataclass
class LineageGraph:
    """The full lineage graph."""
    nodes: List[LineageNode] = field(default_factory=list)
    edges: List[LineageEdge] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "nodes": [n.to_dict() for n in self.nodes],
            "edges": [e.to_dict() for e in self.edges],
            "node_count": len(self.nodes),
            "edge_count": len(self.edges),
        }


@dataclass
class ImpactAnalysis:
    """Result of impact analysis for an object."""
    object_type: str
    object_id: str
    directly_affected: List[Dict[str, Any]] = field(default_factory=list)
    indirectly_affected: List[Dict[str, Any]] = field(default_factory=list)
    total_affected: int = 0
    severity: str = "none"  # none, low, medium, high, critical

    def to_dict(self) -> Dict[str, Any]:
        return {
            "object_type": self.object_type,
            "object_id": self.object_id,
            "directly_affected": self.directly_affected,
            "indirectly_affected": self.indirectly_affected,
            "total_affected": self.total_affected,
            "severity": self.severity,
        }


class LineageService:
    """
    Service for building and querying lineage graphs.

    Provides:
    - Full lineage graph construction
    - Upstream dependency queries (what does X depend on?)
    - Downstream dependency queries (what depends on X?)
    - Impact analysis (what breaks if X is removed?)
    """

    def __init__(self, persistence: Optional[NLQPersistence] = None):
        """
        Initialize the lineage service.

        Args:
            persistence: NLQPersistence instance. Creates default if not provided.
        """
        self.persistence = persistence or NLQPersistence()

    def build_full_graph(self, tenant_id: str = "default") -> LineageGraph:
        """
        Build the complete lineage graph for a tenant.

        Args:
            tenant_id: Tenant ID

        Returns:
            LineageGraph with all nodes and edges
        """
        nodes = []
        edges = []

        # Load all data
        events = self.persistence.get_events(tenant_id)
        entities = self.persistence.get_entities(tenant_id)
        bindings = self.persistence.get_bindings(tenant_id)
        definitions = self.persistence.get_definitions(tenant_id)
        versions = self.persistence.get_definition_versions(tenant_id)

        # Build node index for deduplication
        node_ids: Set[str] = set()

        # Add event nodes
        for event in events:
            node_id = f"event:{event.id}"
            if node_id not in node_ids:
                nodes.append(LineageNode(
                    id=node_id,
                    node_type=NodeType.EVENT,
                    label=event.id,
                    metadata={
                        "description": event.description or "",
                        "field_count": len(event.schema_json.get("fields", [])),
                    },
                ))
                node_ids.add(node_id)

        # Add entity nodes
        for entity in entities:
            node_id = f"entity:{entity.id}"
            if node_id not in node_ids:
                nodes.append(LineageNode(
                    id=node_id,
                    node_type=NodeType.ENTITY,
                    label=entity.id,
                    metadata={
                        "description": entity.description or "",
                    },
                ))
                node_ids.add(node_id)

        # Add definition nodes
        for defn in definitions:
            node_id = f"definition:{defn.id}"
            if node_id not in node_ids:
                nodes.append(LineageNode(
                    id=node_id,
                    node_type=NodeType.DEFINITION,
                    label=defn.id,
                    metadata={
                        "description": defn.description or "",
                        "kind": defn.kind,
                    },
                ))
                node_ids.add(node_id)

        # Add binding nodes and source system nodes
        source_systems: Set[str] = set()
        for binding in bindings:
            # Binding node
            node_id = f"binding:{binding.id}"
            if node_id not in node_ids:
                nodes.append(LineageNode(
                    id=node_id,
                    node_type=NodeType.BINDING,
                    label=binding.id,
                    metadata={
                        "source_system": binding.source_system,
                        "quality_score": binding.quality_score,
                        "freshness_score": binding.freshness_score,
                    },
                ))
                node_ids.add(node_id)

            # Source system node
            sys_id = f"source:{binding.source_system}"
            if sys_id not in node_ids:
                nodes.append(LineageNode(
                    id=sys_id,
                    node_type=NodeType.SOURCE_SYSTEM,
                    label=binding.source_system,
                    metadata={},
                ))
                node_ids.add(sys_id)
                source_systems.add(binding.source_system)

            # Edge: binding -> event
            edges.append(LineageEdge(
                source_id=node_id,
                target_id=f"event:{binding.canonical_event_id}",
                edge_type=EdgeType.BINDS_TO,
                metadata={
                    "quality": binding.quality_score,
                    "freshness": binding.freshness_score,
                },
            ))

            # Edge: binding -> source system
            edges.append(LineageEdge(
                source_id=node_id,
                target_id=sys_id,
                edge_type=EdgeType.SOURCES_FROM,
                metadata={},
            ))

        # Add definition -> event and definition -> entity edges
        for v in versions:
            if v.status != "published":
                continue

            def_node_id = f"definition:{v.definition_id}"

            # Definition requires events
            for event_id in v.spec.required_events:
                edges.append(LineageEdge(
                    source_id=def_node_id,
                    target_id=f"event:{event_id}",
                    edge_type=EdgeType.REQUIRES,
                    metadata={"version": v.version},
                ))

            # Definition groups by entities (allowed_dims)
            for dim in v.spec.allowed_dims:
                edges.append(LineageEdge(
                    source_id=def_node_id,
                    target_id=f"entity:{dim}",
                    edge_type=EdgeType.GROUPS_BY,
                    metadata={},
                ))

            # Definition joins to entities
            for field_name, entity in v.spec.joins.items():
                edges.append(LineageEdge(
                    source_id=def_node_id,
                    target_id=f"entity:{entity}",
                    edge_type=EdgeType.JOINS_TO,
                    metadata={"field": field_name},
                ))

        return LineageGraph(nodes=nodes, edges=edges)

    def get_upstream_dependencies(
        self,
        object_type: str,
        object_id: str,
        tenant_id: str = "default",
        max_depth: int = 10
    ) -> LineageGraph:
        """
        Get all upstream dependencies (what does this object depend on?).

        Args:
            object_type: Type of object (definition, event, binding, entity)
            object_id: ID of the object
            tenant_id: Tenant ID
            max_depth: Maximum traversal depth

        Returns:
            LineageGraph with upstream nodes and edges
        """
        full_graph = self.build_full_graph(tenant_id)
        start_node_id = f"{object_type}:{object_id}"

        # Build adjacency list for upstream traversal (follow edges backward)
        upstream: Dict[str, List[LineageEdge]] = defaultdict(list)
        for edge in full_graph.edges:
            upstream[edge.source_id].append(edge)

        # BFS to find all upstream nodes
        visited: Set[str] = set()
        queue = [(start_node_id, 0)]
        result_nodes: List[LineageNode] = []
        result_edges: List[LineageEdge] = []

        while queue:
            current_id, depth = queue.pop(0)
            if current_id in visited or depth > max_depth:
                continue
            visited.add(current_id)

            # Find the node
            for node in full_graph.nodes:
                if node.id == current_id:
                    result_nodes.append(node)
                    break

            # Traverse upstream edges
            for edge in upstream.get(current_id, []):
                result_edges.append(edge)
                if edge.target_id not in visited:
                    queue.append((edge.target_id, depth + 1))

        return LineageGraph(nodes=result_nodes, edges=result_edges)

    def get_downstream_dependencies(
        self,
        object_type: str,
        object_id: str,
        tenant_id: str = "default",
        max_depth: int = 10
    ) -> LineageGraph:
        """
        Get all downstream dependencies (what depends on this object?).

        Args:
            object_type: Type of object (definition, event, binding, entity)
            object_id: ID of the object
            tenant_id: Tenant ID
            max_depth: Maximum traversal depth

        Returns:
            LineageGraph with downstream nodes and edges
        """
        full_graph = self.build_full_graph(tenant_id)
        start_node_id = f"{object_type}:{object_id}"

        # Build adjacency list for downstream traversal (follow edges forward)
        downstream: Dict[str, List[LineageEdge]] = defaultdict(list)
        for edge in full_graph.edges:
            downstream[edge.target_id].append(edge)

        # BFS to find all downstream nodes
        visited: Set[str] = set()
        queue = [(start_node_id, 0)]
        result_nodes: List[LineageNode] = []
        result_edges: List[LineageEdge] = []

        while queue:
            current_id, depth = queue.pop(0)
            if current_id in visited or depth > max_depth:
                continue
            visited.add(current_id)

            # Find the node
            for node in full_graph.nodes:
                if node.id == current_id:
                    result_nodes.append(node)
                    break

            # Traverse downstream edges
            for edge in downstream.get(current_id, []):
                result_edges.append(edge)
                if edge.source_id not in visited:
                    queue.append((edge.source_id, depth + 1))

        return LineageGraph(nodes=result_nodes, edges=result_edges)

    def analyze_impact(
        self,
        object_type: str,
        object_id: str,
        tenant_id: str = "default"
    ) -> ImpactAnalysis:
        """
        Analyze the impact of removing or modifying an object.

        Args:
            object_type: Type of object (definition, event, binding, entity)
            object_id: ID of the object
            tenant_id: Tenant ID

        Returns:
            ImpactAnalysis with affected objects and severity
        """
        downstream = self.get_downstream_dependencies(object_type, object_id, tenant_id)

        directly_affected = []
        indirectly_affected = []

        start_node_id = f"{object_type}:{object_id}"

        # Find directly connected nodes
        direct_ids: Set[str] = set()
        for edge in downstream.edges:
            if edge.target_id == start_node_id:
                direct_ids.add(edge.source_id)

        # Categorize affected nodes
        for node in downstream.nodes:
            if node.id == start_node_id:
                continue

            affected_info = {
                "id": node.id,
                "type": node.node_type.value,
                "label": node.label,
            }

            if node.id in direct_ids:
                directly_affected.append(affected_info)
            else:
                indirectly_affected.append(affected_info)

        # Calculate severity
        total = len(directly_affected) + len(indirectly_affected)
        definition_count = sum(
            1 for n in directly_affected + indirectly_affected
            if n["type"] == "definition"
        )

        if total == 0:
            severity = "none"
        elif definition_count == 0 and total <= 2:
            severity = "low"
        elif definition_count <= 2:
            severity = "medium"
        elif definition_count <= 5:
            severity = "high"
        else:
            severity = "critical"

        return ImpactAnalysis(
            object_type=object_type,
            object_id=object_id,
            directly_affected=directly_affected,
            indirectly_affected=indirectly_affected,
            total_affected=total,
            severity=severity,
        )

    def get_definition_lineage(
        self,
        definition_id: str,
        tenant_id: str = "default"
    ) -> Dict[str, Any]:
        """
        Get complete lineage for a definition.

        Returns the full path from source systems through bindings and events
        to the definition.

        Args:
            definition_id: Definition ID
            tenant_id: Tenant ID

        Returns:
            Dict with complete lineage information
        """
        upstream = self.get_upstream_dependencies("definition", definition_id, tenant_id)

        # Categorize nodes
        events = []
        bindings = []
        source_systems = []
        entities = []

        for node in upstream.nodes:
            if node.node_type == NodeType.EVENT:
                events.append({"id": node.label, "metadata": node.metadata})
            elif node.node_type == NodeType.BINDING:
                bindings.append({"id": node.label, "metadata": node.metadata})
            elif node.node_type == NodeType.SOURCE_SYSTEM:
                source_systems.append({"name": node.label})
            elif node.node_type == NodeType.ENTITY:
                entities.append({"id": node.label, "metadata": node.metadata})

        return {
            "definition_id": definition_id,
            "events": events,
            "bindings": bindings,
            "source_systems": source_systems,
            "entities": entities,
            "edge_count": len(upstream.edges),
        }

    def get_event_consumers(
        self,
        event_id: str,
        tenant_id: str = "default"
    ) -> List[Dict[str, Any]]:
        """
        Get all definitions that consume a specific event.

        Args:
            event_id: Event ID
            tenant_id: Tenant ID

        Returns:
            List of definition info dicts
        """
        downstream = self.get_downstream_dependencies("event", event_id, tenant_id)

        consumers = []
        for node in downstream.nodes:
            if node.node_type == NodeType.DEFINITION:
                consumers.append({
                    "definition_id": node.label,
                    "description": node.metadata.get("description", ""),
                    "kind": node.metadata.get("kind", "metric"),
                })

        return consumers

    def get_binding_chain(
        self,
        binding_id: str,
        tenant_id: str = "default"
    ) -> Dict[str, Any]:
        """
        Get the full chain from binding to consuming definitions.

        Args:
            binding_id: Binding ID
            tenant_id: Tenant ID

        Returns:
            Dict with source system, event, and consuming definitions
        """
        bindings = self.persistence.get_bindings(tenant_id)
        binding = next((b for b in bindings if b.id == binding_id), None)

        if not binding:
            return {"error": "Binding not found"}

        # Get event consumers
        consumers = self.get_event_consumers(binding.canonical_event_id, tenant_id)

        return {
            "binding_id": binding_id,
            "source_system": binding.source_system,
            "event_id": binding.canonical_event_id,
            "quality_score": binding.quality_score,
            "freshness_score": binding.freshness_score,
            "consuming_definitions": consumers,
        }

    def find_data_path(
        self,
        source_system: str,
        definition_id: str,
        tenant_id: str = "default"
    ) -> List[Dict[str, Any]]:
        """
        Find the data path from a source system to a definition.

        Args:
            source_system: Source system name
            definition_id: Target definition ID
            tenant_id: Tenant ID

        Returns:
            List of path steps from source to definition
        """
        full_graph = self.build_full_graph(tenant_id)

        start_id = f"source:{source_system}"
        end_id = f"definition:{definition_id}"

        # Build adjacency list
        adj: Dict[str, List[Tuple[str, LineageEdge]]] = defaultdict(list)
        for edge in full_graph.edges:
            # Reverse direction for path finding (source -> definition)
            adj[edge.target_id].append((edge.source_id, edge))

        # BFS to find path
        visited: Set[str] = set()
        queue = [(start_id, [])]

        while queue:
            current_id, path = queue.pop(0)
            if current_id in visited:
                continue
            visited.add(current_id)

            if current_id == end_id:
                return path

            for next_id, edge in adj.get(current_id, []):
                if next_id not in visited:
                    new_path = path + [{
                        "from": edge.target_id,
                        "to": edge.source_id,
                        "type": edge.edge_type.value,
                        "metadata": edge.metadata,
                    }]
                    queue.append((next_id, new_path))

        return []  # No path found
