"""
Fabric Plane Abstraction for DCL Engine.

**ARCHITECTURE PIVOT (January 2026)**: AAM connects to Fabric Planes, not individual SaaS apps.
DCL ingests metadata from these Planes using Pointer Buffering (Zero-Trust compliance).

The 4 Fabric Planes:
1. IPAAS (Workato, MuleSoft) - Control plane for integration flows
2. API_GATEWAY (Kong, Apigee) - Direct managed API access
3. EVENT_BUS (Kafka, EventBridge) - Streaming backbone
4. DATA_WAREHOUSE (Snowflake, BigQuery) - Source of Truth storage

Pointer Buffering Strategy:
- DCL buffers ONLY Fabric Pointers (offsets, cursors) - never payloads
- Just-in-Time fetching: payload retrieved only when semantic mapper requests it
- Leverages Fabric durability for Zero-Trust compliance
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional, Dict, Any, List, TypeVar, Generic
import hashlib
import logging

logger = logging.getLogger("dcl.fabric_plane")


class FabricPlaneType(Enum):
    """The 4 canonical Fabric Plane types."""
    IPAAS = "ipaas"
    API_GATEWAY = "api_gateway"
    EVENT_BUS = "event_bus"
    DATA_WAREHOUSE = "data_warehouse"


class FabricProvider(Enum):
    """Specific providers within each Fabric Plane."""
    WORKATO = "workato"
    MULESOFT = "mulesoft"
    KONG = "kong"
    APIGEE = "apigee"
    KAFKA = "kafka"
    EVENTBRIDGE = "eventbridge"
    SNOWFLAKE = "snowflake"
    BIGQUERY = "bigquery"
    REDSHIFT = "redshift"


@dataclass(frozen=True)
class FabricPointer:
    """
    Base class for Fabric Pointers.
    
    A Fabric Pointer is a durable reference to data in a Fabric Plane.
    It contains ONLY offset/cursor information - never the actual payload.
    """
    plane_type: FabricPlaneType = FabricPlaneType.EVENT_BUS
    provider: FabricProvider = FabricProvider.KAFKA
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    def fingerprint(self) -> str:
        """Generate a unique fingerprint for this pointer."""
        content = f"{self.plane_type.value}:{self.provider.value}:{self.timestamp.isoformat()}"
        return hashlib.sha256(content.encode()).hexdigest()[:16]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "plane_type": self.plane_type.value,
            "provider": self.provider.value,
            "timestamp": self.timestamp.isoformat(),
            "fingerprint": self.fingerprint()
        }


@dataclass(frozen=True)
class KafkaPointer(FabricPointer):
    """
    Pointer for Kafka Event Bus.
    
    Contains ONLY: Topic, Partition, Offset
    The actual payload remains in Kafka until JIT fetch.
    """
    plane_type: FabricPlaneType = FabricPlaneType.EVENT_BUS
    provider: FabricProvider = FabricProvider.KAFKA
    topic: str = ""
    partition: int = 0
    offset: int = 0
    consumer_group: Optional[str] = None
    
    def fingerprint(self) -> str:
        content = f"kafka:{self.topic}:{self.partition}:{self.offset}"
        return hashlib.sha256(content.encode()).hexdigest()[:16]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "plane_type": self.plane_type.value,
            "provider": self.provider.value,
            "topic": self.topic,
            "partition": self.partition,
            "offset": self.offset,
            "consumer_group": self.consumer_group,
            "timestamp": self.timestamp.isoformat(),
            "fingerprint": self.fingerprint()
        }


@dataclass(frozen=True)
class SnowflakePointer(FabricPointer):
    """
    Pointer for Snowflake Data Warehouse.
    
    Contains ONLY: Table, Stream_ID, Row_Cursor
    The actual data remains in Snowflake until JIT fetch.
    """
    plane_type: FabricPlaneType = FabricPlaneType.DATA_WAREHOUSE
    provider: FabricProvider = FabricProvider.SNOWFLAKE
    database: str = ""
    schema: str = ""
    table: str = ""
    stream_id: Optional[str] = None
    row_cursor: Optional[str] = None
    query_id: Optional[str] = None
    
    def fingerprint(self) -> str:
        content = f"snowflake:{self.database}.{self.schema}.{self.table}:{self.stream_id}:{self.row_cursor}"
        return hashlib.sha256(content.encode()).hexdigest()[:16]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "plane_type": self.plane_type.value,
            "provider": self.provider.value,
            "database": self.database,
            "schema": self.schema,
            "table": self.table,
            "stream_id": self.stream_id,
            "row_cursor": self.row_cursor,
            "query_id": self.query_id,
            "timestamp": self.timestamp.isoformat(),
            "fingerprint": self.fingerprint()
        }


@dataclass(frozen=True)
class BigQueryPointer(FabricPointer):
    """Pointer for BigQuery Data Warehouse."""
    plane_type: FabricPlaneType = FabricPlaneType.DATA_WAREHOUSE
    provider: FabricProvider = FabricProvider.BIGQUERY
    project: str = ""
    dataset: str = ""
    table: str = ""
    stream_name: Optional[str] = None
    read_session: Optional[str] = None
    
    def fingerprint(self) -> str:
        content = f"bigquery:{self.project}.{self.dataset}.{self.table}:{self.stream_name}"
        return hashlib.sha256(content.encode()).hexdigest()[:16]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "plane_type": self.plane_type.value,
            "provider": self.provider.value,
            "project": self.project,
            "dataset": self.dataset,
            "table": self.table,
            "stream_name": self.stream_name,
            "read_session": self.read_session,
            "timestamp": self.timestamp.isoformat(),
            "fingerprint": self.fingerprint()
        }


@dataclass(frozen=True)
class EventBridgePointer(FabricPointer):
    """Pointer for AWS EventBridge Event Bus."""
    plane_type: FabricPlaneType = FabricPlaneType.EVENT_BUS
    provider: FabricProvider = FabricProvider.EVENTBRIDGE
    event_bus_name: str = ""
    event_id: str = ""
    source: str = ""
    detail_type: str = ""
    archive_name: Optional[str] = None
    
    def fingerprint(self) -> str:
        content = f"eventbridge:{self.event_bus_name}:{self.event_id}"
        return hashlib.sha256(content.encode()).hexdigest()[:16]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "plane_type": self.plane_type.value,
            "provider": self.provider.value,
            "event_bus_name": self.event_bus_name,
            "event_id": self.event_id,
            "source": self.source,
            "detail_type": self.detail_type,
            "archive_name": self.archive_name,
            "timestamp": self.timestamp.isoformat(),
            "fingerprint": self.fingerprint()
        }


@dataclass(frozen=True)
class IPaaSPointer(FabricPointer):
    """Pointer for iPaaS integration flows (Workato, MuleSoft)."""
    plane_type: FabricPlaneType = FabricPlaneType.IPAAS
    provider: FabricProvider = FabricProvider.WORKATO
    flow_id: str = ""
    execution_id: str = ""
    step_id: Optional[str] = None
    connection_id: Optional[str] = None
    
    def fingerprint(self) -> str:
        content = f"ipaas:{self.provider.value}:{self.flow_id}:{self.execution_id}"
        return hashlib.sha256(content.encode()).hexdigest()[:16]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "plane_type": self.plane_type.value,
            "provider": self.provider.value,
            "flow_id": self.flow_id,
            "execution_id": self.execution_id,
            "step_id": self.step_id,
            "connection_id": self.connection_id,
            "timestamp": self.timestamp.isoformat(),
            "fingerprint": self.fingerprint()
        }


@dataclass(frozen=True)
class APIGatewayPointer(FabricPointer):
    """Pointer for API Gateway access logs/requests."""
    plane_type: FabricPlaneType = FabricPlaneType.API_GATEWAY
    provider: FabricProvider = FabricProvider.KONG
    gateway_id: str = ""
    request_id: str = ""
    route: str = ""
    method: str = ""
    
    def fingerprint(self) -> str:
        content = f"api_gateway:{self.provider.value}:{self.gateway_id}:{self.request_id}"
        return hashlib.sha256(content.encode()).hexdigest()[:16]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "plane_type": self.plane_type.value,
            "provider": self.provider.value,
            "gateway_id": self.gateway_id,
            "request_id": self.request_id,
            "route": self.route,
            "method": self.method,
            "timestamp": self.timestamp.isoformat(),
            "fingerprint": self.fingerprint()
        }


class FabricPlaneClient(ABC):
    """
    Abstract interface for connecting to a Fabric Plane.
    
    AAM implements concrete versions of this for each provider.
    DCL uses this interface for Just-in-Time fetching.
    """
    
    @abstractmethod
    async def connect(self) -> bool:
        """Establish connection to the Fabric Plane."""
        pass
    
    @abstractmethod
    async def disconnect(self) -> None:
        """Close connection to the Fabric Plane."""
        pass
    
    @abstractmethod
    async def fetch_payload(self, pointer: FabricPointer) -> Optional[Dict[str, Any]]:
        """
        Just-in-Time fetch: retrieve payload from Fabric Plane using pointer.
        
        This is called ONLY when the semantic mapper needs the actual data.
        The pointer must be valid and the data must still exist in the Plane.
        """
        pass
    
    @abstractmethod
    async def get_latest_pointer(self) -> Optional[FabricPointer]:
        """Get the latest available pointer from this Fabric Plane."""
        pass
    
    @abstractmethod
    def get_plane_type(self) -> FabricPlaneType:
        """Return the type of Fabric Plane this client connects to."""
        pass


PLANE_TO_PROVIDERS = {
    FabricPlaneType.IPAAS: [FabricProvider.WORKATO, FabricProvider.MULESOFT],
    FabricPlaneType.API_GATEWAY: [FabricProvider.KONG, FabricProvider.APIGEE],
    FabricPlaneType.EVENT_BUS: [FabricProvider.KAFKA, FabricProvider.EVENTBRIDGE],
    FabricPlaneType.DATA_WAREHOUSE: [FabricProvider.SNOWFLAKE, FabricProvider.BIGQUERY, FabricProvider.REDSHIFT],
}


def get_pointer_class(provider: FabricProvider):
    """Get the appropriate pointer class for a given provider."""
    mapping = {
        FabricProvider.KAFKA: KafkaPointer,
        FabricProvider.EVENTBRIDGE: EventBridgePointer,
        FabricProvider.SNOWFLAKE: SnowflakePointer,
        FabricProvider.BIGQUERY: BigQueryPointer,
        FabricProvider.WORKATO: IPaaSPointer,
        FabricProvider.MULESOFT: IPaaSPointer,
        FabricProvider.KONG: APIGatewayPointer,
        FabricProvider.APIGEE: APIGatewayPointer,
    }
    return mapping.get(provider, FabricPointer)
