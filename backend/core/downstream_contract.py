"""
DownstreamConsumerContract - BLL Interface Definition

This module defines the abstract interface for downstream consumers
(Business Logic Layer) to integrate with DCL.

Architecture: Self-Healing Mesh & Zero-Trust Vision
- DCL is "The Brain" - provides ontology and change events
- BLL consumers subscribe to DCL for semantic data
- Clean boundary for future Agent integration
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Protocol
from datetime import datetime


class ChangeEventType(Enum):
    """Types of change events emitted by DCL."""
    SCHEMA_DISCOVERED = "schema_discovered"
    SCHEMA_UPDATED = "schema_updated"
    MAPPING_CREATED = "mapping_created"
    MAPPING_UPDATED = "mapping_updated"
    MAPPING_DELETED = "mapping_deleted"
    SOURCE_REGISTERED = "source_registered"
    SOURCE_DEREGISTERED = "source_deregistered"
    ONTOLOGY_UPDATED = "ontology_updated"
    DRIFT_DETECTED = "drift_detected"
    GRAPH_REBUILT = "graph_rebuilt"


@dataclass
class ChangeEvent:
    """
    Change event emitted by DCL to downstream consumers.
    
    Contains ONLY metadata about the change, never raw data.
    """
    event_id: str
    event_type: ChangeEventType
    timestamp: datetime
    source_id: Optional[str]
    
    affected_concepts: List[str]
    affected_mappings: List[str]
    
    metadata: Dict[str, Any]
    
    snapshot_id: Optional[str] = None


@dataclass
class OntologySnapshot:
    """
    Point-in-time snapshot of the DCL ontology.
    
    Used for querying the current state of semantic mappings.
    """
    snapshot_id: str
    created_at: datetime
    
    concepts: List[Dict[str, Any]]
    mappings: List[Dict[str, Any]]
    sources: List[Dict[str, Any]]
    personas: List[Dict[str, Any]]
    
    graph_nodes: List[Dict[str, Any]]
    graph_links: List[Dict[str, Any]]
    
    metadata: Dict[str, Any]


class ChangeEventHandler(Protocol):
    """Protocol for handling change events."""
    
    async def handle(self, event: ChangeEvent) -> None:
        """Process a change event from DCL."""
        ...


class DownstreamConsumerContract(ABC):
    """
    Abstract interface for Business Logic Layer (BLL) consumers.
    
    This contract defines how downstream systems integrate with DCL:
    1. Subscribe to semantic change events
    2. Query ontology snapshots
    3. Access graph topology
    
    Future Agents (FinOps, RevOps, etc.) will implement this contract
    to receive semantic data from DCL without modifying DCL core.
    
    Example Implementation:
    
        class FinOpsConsumer(DownstreamConsumerContract):
            async def subscribe_to_change_events(self, topic: str, handler: ChangeEventHandler):
                # Register handler for finance-related changes
                self._handlers[topic] = handler
            
            async def query_ontology(self, snapshot_id: Optional[str] = None) -> OntologySnapshot:
                # Fetch current or historical ontology state
                return await self._dcl_client.get_snapshot(snapshot_id)
    """
    
    @abstractmethod
    async def subscribe_to_change_events(
        self,
        topic: str,
        handler: ChangeEventHandler,
        event_types: Optional[List[ChangeEventType]] = None,
    ) -> str:
        """
        Subscribe to change events from DCL.
        
        Args:
            topic: Topic name for the subscription (e.g., "finops.revenue", "revops.pipeline")
            handler: Callback to process events
            event_types: Optional filter for specific event types
        
        Returns:
            Subscription ID for management
        """
        pass
    
    @abstractmethod
    async def unsubscribe(self, subscription_id: str) -> bool:
        """
        Unsubscribe from change events.
        
        Args:
            subscription_id: ID returned from subscribe_to_change_events
        
        Returns:
            True if unsubscribed successfully
        """
        pass
    
    @abstractmethod
    async def query_ontology(
        self,
        snapshot_id: Optional[str] = None,
    ) -> OntologySnapshot:
        """
        Query the DCL ontology.
        
        Args:
            snapshot_id: Optional specific snapshot ID. If None, returns current state.
        
        Returns:
            OntologySnapshot with concepts, mappings, and graph structure
        """
        pass
    
    @abstractmethod
    async def query_concepts_by_persona(
        self,
        persona: str,
        snapshot_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Query ontology concepts filtered by persona relevance.
        
        Args:
            persona: Persona identifier (e.g., "CFO", "CRO", "COO", "CTO")
            snapshot_id: Optional specific snapshot ID
        
        Returns:
            List of concepts relevant to the persona
        """
        pass
    
    @abstractmethod
    async def query_source_mappings(
        self,
        source_id: str,
        snapshot_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Query mappings for a specific source system.
        
        Args:
            source_id: Source system identifier
            snapshot_id: Optional specific snapshot ID
        
        Returns:
            List of field-to-concept mappings for the source
        """
        pass
    
    @abstractmethod
    async def get_graph_topology(
        self,
        personas: Optional[List[str]] = None,
        snapshot_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Get the current graph topology for visualization.
        
        Args:
            personas: Optional persona filter
            snapshot_id: Optional specific snapshot ID
        
        Returns:
            Graph structure with nodes and links
        """
        pass


class DCLEventPublisher:
    """
    Publisher for DCL change events.
    
    This class is used internally by DCL to emit events
    to registered downstream consumers.
    """
    
    def __init__(self):
        self._subscriptions: Dict[str, Dict[str, ChangeEventHandler]] = {}
        self._subscription_filters: Dict[str, List[ChangeEventType]] = {}
    
    def register_subscription(
        self,
        subscription_id: str,
        topic: str,
        handler: ChangeEventHandler,
        event_types: Optional[List[ChangeEventType]] = None,
    ) -> None:
        """Register a new subscription."""
        if topic not in self._subscriptions:
            self._subscriptions[topic] = {}
        
        self._subscriptions[topic][subscription_id] = handler
        
        if event_types:
            self._subscription_filters[subscription_id] = event_types
    
    def unregister_subscription(self, subscription_id: str) -> bool:
        """Unregister a subscription."""
        for topic_handlers in self._subscriptions.values():
            if subscription_id in topic_handlers:
                del topic_handlers[subscription_id]
                self._subscription_filters.pop(subscription_id, None)
                return True
        return False
    
    async def publish(self, topic: str, event: ChangeEvent) -> int:
        """
        Publish an event to all subscribers of a topic.
        
        Returns count of handlers notified.
        """
        handlers = self._subscriptions.get(topic, {})
        notified = 0
        
        for sub_id, handler in handlers.items():
            filters = self._subscription_filters.get(sub_id)
            if filters and event.event_type not in filters:
                continue
            
            try:
                await handler.handle(event)
                notified += 1
            except Exception as e:
                import logging
                logging.getLogger("dcl.publisher").error(
                    f"Error notifying subscriber {sub_id}: {e}"
                )
        
        return notified
    
    async def broadcast(self, event: ChangeEvent) -> int:
        """
        Broadcast an event to ALL subscribers across all topics.
        
        Returns total count of handlers notified.
        """
        total = 0
        for topic in self._subscriptions:
            total += await self.publish(topic, event)
        return total
