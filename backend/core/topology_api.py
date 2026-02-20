"""
TopologyAPI - Graph Visualization Service

This module provides the TopologyAPI service that:
1. Ingests connection health data from AAM
2. Exposes JSON graph structure for frontend rendering
3. Provides unified topology view across the mesh

Architecture: Self-Healing Mesh & Zero-Trust Vision
- DCL absorbs visualization responsibility
- AAM provides connection health via gRPC/REST
- DCL renders the topology for the frontend
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
import json

logger = logging.getLogger("dcl.topology_api")


class ConnectionStatus(Enum):
    """Health status of a connection in the mesh."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"
    DISCONNECTED = "disconnected"


class NodeType(Enum):
    """Types of nodes in the topology graph."""
    PIPELINE = "pipeline"
    SOURCE = "source"
    ONTOLOGY = "ontology"
    PERSONA = "persona"
    CONNECTOR = "connector"
    STREAM = "stream"


@dataclass
class ConnectionHealth:
    """
    Health data for a connection from AAM.
    
    This is the data structure that AAM exposes via GetConnectionHealth.
    """
    connector_id: str
    connector_type: str
    target_url: str
    
    status: ConnectionStatus
    last_heartbeat_ms: int
    error_count: int
    success_count: int
    
    latency_p50_ms: float
    latency_p99_ms: float
    throughput_rps: float
    
    is_self_healing: bool
    last_repair_attempt_ms: Optional[int] = None
    repair_success_count: int = 0
    repair_failure_count: int = 0
    
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TopologyNode:
    """A node in the topology graph."""
    id: str
    label: str
    node_type: NodeType
    layer: int
    
    status: ConnectionStatus = ConnectionStatus.UNKNOWN
    metrics: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TopologyLink:
    """A link between nodes in the topology graph."""
    id: str
    source: str
    target: str
    value: float = 1.0
    
    status: ConnectionStatus = ConnectionStatus.UNKNOWN
    metadata: Dict[str, Any] = field(default_factory=dict)


class TopologyAPI:
    """
    Service for managing and exposing topology data.
    
    Responsibilities:
    1. Ingest GetConnectionHealth data from AAM
    2. Merge with DCL's semantic graph
    3. Expose unified JSON structure for frontend
    
    This implements the visualization absorption directive:
    AAM is headless, DCL renders the topology.
    """
    
    def __init__(self):
        self._aam_health_cache: Dict[str, ConnectionHealth] = {}
        self._topology_cache: Optional[Dict[str, Any]] = None
        self._cache_timestamp: Optional[datetime] = None
        self._cache_ttl_seconds = 5
        
        self._dcl_nodes: List[TopologyNode] = []
        self._dcl_links: List[TopologyLink] = []
        
        self._lock = asyncio.Lock()
    
    async def ingest_aam_health(self, health_data: List[ConnectionHealth]) -> int:
        """
        Ingest connection health data from AAM.
        
        This is called periodically or on-demand to sync with AAM.
        
        Args:
            health_data: List of ConnectionHealth from AAM's GetConnectionHealth
        
        Returns:
            Count of connections updated
        """
        async with self._lock:
            updated = 0
            for health in health_data:
                self._aam_health_cache[health.connector_id] = health
                updated += 1
            
            self._topology_cache = None
            
            logger.info(f"Ingested {updated} connection health records from AAM")
            return updated
    
    async def update_dcl_graph(
        self,
        nodes: List[Dict[str, Any]],
        links: List[Dict[str, Any]],
    ) -> None:
        """
        Update the DCL semantic graph.
        
        This is called when DCL rebuilds its graph.
        """
        async with self._lock:
            self._dcl_nodes = [
                TopologyNode(
                    id=n["id"],
                    label=n.get("label", n["id"]),
                    node_type=NodeType(n.get("type", "source")),
                    layer=n.get("layer", 1),
                    metrics=n.get("metrics", {}),
                    metadata=n.get("metadata", {}),
                )
                for n in nodes
            ]
            
            self._dcl_links = [
                TopologyLink(
                    id=f"{l['source']}_{l['target']}",
                    source=l["source"],
                    target=l["target"],
                    value=l.get("value", 1.0),
                    metadata=l.get("metadata", {}),
                )
                for l in links
            ]
            
            self._topology_cache = None
    
    async def get_topology(
        self,
        include_health: bool = True,
        personas: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Get the unified topology graph.
        
        Merges DCL semantic graph with AAM health data.
        
        Args:
            include_health: Whether to include AAM health data
            personas: Optional persona filter
        
        Returns:
            JSON-serializable graph structure for frontend
        """
        async with self._lock:
            if (
                self._topology_cache is not None
                and self._cache_timestamp is not None
                and (datetime.now() - self._cache_timestamp).seconds < self._cache_ttl_seconds
            ):
                return self._topology_cache
            
            nodes = []
            for node in self._dcl_nodes:
                node_data = {
                    "id": node.id,
                    "label": node.label,
                    "type": node.node_type.value,
                    "layer": node.layer,
                    "metrics": node.metrics,
                    "metadata": node.metadata,
                    "status": node.status.value,
                }
                
                if include_health and node.id in self._aam_health_cache:
                    health = self._aam_health_cache[node.id]
                    node_data["health"] = {
                        "status": health.status.value,
                        "latency_p50_ms": health.latency_p50_ms,
                        "latency_p99_ms": health.latency_p99_ms,
                        "throughput_rps": health.throughput_rps,
                        "error_count": health.error_count,
                        "is_self_healing": health.is_self_healing,
                    }
                    node_data["status"] = health.status.value
                
                nodes.append(node_data)
            
            links = []
            for link in self._dcl_links:
                link_data = {
                    "id": link.id,
                    "source": link.source,
                    "target": link.target,
                    "value": link.value,
                    "metadata": link.metadata,
                    "status": link.status.value,
                }
                links.append(link_data)
            
            result = {
                "nodes": nodes,
                "links": links,
                "metadata": {
                    "generated_at": datetime.now().isoformat(),
                    "node_count": len(nodes),
                    "link_count": len(links),
                    "aam_connections": len(self._aam_health_cache),
                },
            }
            
            self._topology_cache = result
            self._cache_timestamp = datetime.now()
            
            return result
    
    async def get_connection_health(
        self,
        connector_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get connection health data.
        
        Args:
            connector_id: Optional filter for specific connector
        
        Returns:
            List of connection health records
        """
        async with self._lock:
            if connector_id:
                health = self._aam_health_cache.get(connector_id)
                if health:
                    return [self._health_to_dict(health)]
                return []
            
            return [
                self._health_to_dict(h)
                for h in self._aam_health_cache.values()
            ]
    
    def _health_to_dict(self, health: ConnectionHealth) -> Dict[str, Any]:
        """Convert ConnectionHealth to dictionary."""
        return {
            "connector_id": health.connector_id,
            "connector_type": health.connector_type,
            "target_url": health.target_url,
            "status": health.status.value,
            "last_heartbeat_ms": health.last_heartbeat_ms,
            "error_count": health.error_count,
            "success_count": health.success_count,
            "latency_p50_ms": health.latency_p50_ms,
            "latency_p99_ms": health.latency_p99_ms,
            "throughput_rps": health.throughput_rps,
            "is_self_healing": health.is_self_healing,
            "last_repair_attempt_ms": health.last_repair_attempt_ms,
            "repair_success_count": health.repair_success_count,
            "repair_failure_count": health.repair_failure_count,
            "metadata": health.metadata,
        }
    
    def get_stats(self) -> Dict[str, Any]:
        """Get topology service statistics."""
        return {
            "dcl_nodes": len(self._dcl_nodes),
            "dcl_links": len(self._dcl_links),
            "aam_connections": len(self._aam_health_cache),
            "cache_valid": self._topology_cache is not None,
            "healthy_connections": sum(
                1 for h in self._aam_health_cache.values()
                if h.status == ConnectionStatus.HEALTHY
            ),
            "degraded_connections": sum(
                1 for h in self._aam_health_cache.values()
                if h.status == ConnectionStatus.DEGRADED
            ),
            "unhealthy_connections": sum(
                1 for h in self._aam_health_cache.values()
                if h.status == ConnectionStatus.UNHEALTHY
            ),
        }


topology_api = TopologyAPI()
