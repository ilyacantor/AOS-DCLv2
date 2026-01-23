"""
DCL Core Module - Zero-Trust Architecture Components

This module contains the core architectural components for DCL
aligned with the Self-Healing Mesh & Zero-Trust vision.

Components:
- MetadataEventBuffer: In-memory buffer for metadata-only events
- PayloadSecurityGuard: Prevents raw payload data from being stored
- DownstreamConsumerContract: Abstract interface for BLL consumers
- TopologyAPI: Graph visualization service

DCL Scope (Metadata-Only):
- Schema structures (field names, types)
- Semantic mappings (field → concept)
- Ontology management
- Graph visualization

Out of Scope (Moved to AAM):
- Raw data stream consumption
- Raw data buffering
- Self-healing repair
- Payload sanitization
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
]
