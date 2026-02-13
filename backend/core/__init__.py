"""
DCL Core Module - Zero-Trust Architecture Components

**ARCHITECTURE PIVOT (January 2026): Fabric Plane Mesh**

This module contains the core architectural components for DCL
aligned with the Self-Healing Mesh & Zero-Trust vision.

Components:
- FabricPlane: Abstraction for the 4 Fabric Planes (iPaaS, API Gateway, Event Bus, Data Warehouse)
- FabricPointerBuffer: Pointer-only buffering (offsets, cursors) - NEVER payloads
- DownstreamConsumerContract: Abstract interface for BLL consumers
- TopologyAPI: Graph visualization service

Pointer Buffering Strategy:
- DCL buffers ONLY Fabric Pointers (offsets, cursors)
- Just-in-Time fetching: payload retrieved from Fabric Plane only when semantic mapper requests
- Leverages Fabric durability (Kafka, Snowflake) for Zero-Trust compliance

DCL Scope (Metadata-Only):
- Schema structures (field names, types)
- Semantic mappings (field → concept)
- Ontology management
- Graph visualization
- Pointer buffering (NOT payload buffering)

Out of Scope (Moved to AAM):
- Raw data stream consumption
- Raw data buffering
- Self-healing repair
- Payload sanitization
- Fabric Plane connection management
"""

from backend.core.metadata_buffer import (
    MetadataEvent,
    MetadataEventBuffer,
    PayloadSecurityGuard,
    SecurityError,
)

from backend.core.downstream_contract import (
    ChangeEvent,
    ChangeEventType,
    OntologySnapshot,
    ChangeEventHandler,
    DownstreamConsumerContract,
    DCLEventPublisher,
)

from backend.core.topology_api import (
    ConnectionHealth,
    ConnectionStatus,
    TopologyNode,
    TopologyLink,
    NodeType,
    TopologyAPI,
    topology_api,
)

from backend.core.security_constraints import (
    ZeroTrustViolation,
    enforce_metadata_only,
    SecureLogger,
    validate_no_disk_payload_writes,
    assert_metadata_only_mode,
    MetadataOnlyDict,
)

from backend.core.fabric_plane import (
    FabricPlaneType,
    FabricPointer,
    KafkaPointer,
    SnowflakePointer,
    BigQueryPointer,
    EventBridgePointer,
    IPaaSPointer,
    APIGatewayPointer,
    FabricPlaneClient,
    get_pointer_class,
    register_pointer_class,
)

from backend.core.pointer_buffer import (
    BufferedPointer,
    FabricPointerBuffer,
    fabric_pointer_buffer,
)

__all__ = [
    "MetadataEvent",
    "MetadataEventBuffer",
    "PayloadSecurityGuard",
    "SecurityError",
    "ChangeEvent",
    "ChangeEventType",
    "OntologySnapshot",
    "ChangeEventHandler",
    "DownstreamConsumerContract",
    "DCLEventPublisher",
    "ConnectionHealth",
    "ConnectionStatus",
    "TopologyNode",
    "TopologyLink",
    "NodeType",
    "TopologyAPI",
    "topology_api",
    "ZeroTrustViolation",
    "enforce_metadata_only",
    "SecureLogger",
    "validate_no_disk_payload_writes",
    "assert_metadata_only_mode",
    "MetadataOnlyDict",
    "FabricPlaneType",
    "FabricPointer",
    "KafkaPointer",
    "SnowflakePointer",
    "BigQueryPointer",
    "EventBridgePointer",
    "IPaaSPointer",
    "APIGatewayPointer",
    "FabricPlaneClient",
    "get_pointer_class",
    "register_pointer_class",
    "BufferedPointer",
    "FabricPointerBuffer",
    "fabric_pointer_buffer",
]
