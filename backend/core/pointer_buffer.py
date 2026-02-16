"""
Fabric Pointer Buffer for DCL Engine.

**ARCHITECTURE PIVOT (January 2026)**: Pointer Buffering Strategy
- DCL buffers ONLY Fabric Pointers (offsets, cursors) - NEVER payloads or metadata
- Just-in-Time fetching: payload retrieved from Fabric Plane only when semantic mapper requests
- Leverages Fabric durability (Kafka, Snowflake) for Zero-Trust compliance

This replaces the previous MetadataEventBuffer with a stricter Pointer-only model.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Any, Callable, Awaitable
from collections import OrderedDict
import asyncio
import logging

from backend.core.fabric_plane import (
    FabricPointer,
    FabricPlaneType,
    FabricPlaneClient,
    KafkaPointer,
    SnowflakePointer,
    BigQueryPointer,
    EventBridgePointer,
    IPaaSPointer,
    APIGatewayPointer,
)

logger = logging.getLogger("dcl.pointer_buffer")


@dataclass
class BufferedPointer:
    """
    A buffered Fabric Pointer with metadata for DCL processing.
    
    Contains ONLY:
    - The pointer itself (offset/cursor reference)
    - Processing state (pending, fetched, mapped)
    - Timestamps for lifecycle tracking
    
    Does NOT contain:
    - Payload data
    - Schema content
    - Any actual record data
    """
    pointer: FabricPointer
    state: str = "pending"
    buffered_at: datetime = field(default_factory=datetime.utcnow)
    fetched_at: Optional[datetime] = None
    mapped_at: Optional[datetime] = None
    error: Optional[str] = None
    
    def mark_fetched(self) -> None:
        self.state = "fetched"
        self.fetched_at = datetime.utcnow()
    
    def mark_mapped(self) -> None:
        self.state = "mapped"
        self.mapped_at = datetime.utcnow()
    
    def mark_error(self, error: str) -> None:
        self.state = "error"
        self.error = error
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "pointer": self.pointer.to_dict() if hasattr(self.pointer, 'to_dict') else str(self.pointer),
            "state": self.state,
            "buffered_at": self.buffered_at.isoformat(),
            "fetched_at": self.fetched_at.isoformat() if self.fetched_at else None,
            "mapped_at": self.mapped_at.isoformat() if self.mapped_at else None,
            "error": self.error,
        }


class FabricPointerBuffer:
    """
    Zero-Trust Pointer Buffer for Fabric Plane integration.
    
    Key Principles:
    1. ONLY stores pointers (offsets, cursors) - never payloads
    2. Pointers reference durable data in Fabric Planes
    3. Just-in-Time fetching retrieves payload only when needed
    4. Buffer has fixed capacity with LRU eviction
    
    Workflow:
    1. AAM sends pointer to DCL (e.g., Kafka offset)
    2. DCL buffers the pointer here
    3. Semantic mapper requests data -> JIT fetch from Fabric
    4. Payload is processed and immediately discarded
    5. Only mapping result (metadata) is persisted
    """
    
    def __init__(
        self,
        max_size: int = 10000,
        ttl_seconds: int = 3600,
    ):
        self._buffer: OrderedDict[str, BufferedPointer] = OrderedDict()
        self._max_size = max_size
        self._ttl_seconds = ttl_seconds
        self._fabric_clients: Dict[str, FabricPlaneClient] = {}
        
        self._stats = {
            "pointers_buffered": 0,
            "pointers_evicted": 0,
            "jit_fetches": 0,
            "jit_fetch_failures": 0,
            "mappings_completed": 0,
        }
        
        logger.info(f"[POINTER_BUFFER] Initialized with max_size={max_size}, ttl={ttl_seconds}s")
    
    def register_fabric_client(self, provider: str, client: FabricPlaneClient) -> None:
        """Register a Fabric Plane client for JIT fetching."""
        self._fabric_clients[provider] = client
        logger.info(f"[POINTER_BUFFER] Registered client for {provider}")
    
    def buffer_pointer(self, pointer: FabricPointer) -> str:
        """
        Buffer a Fabric Pointer.
        
        Returns the fingerprint (unique ID) of the buffered pointer.
        Does NOT store any payload data.
        """
        fingerprint = pointer.fingerprint()
        
        if len(self._buffer) >= self._max_size:
            oldest_key = next(iter(self._buffer))
            del self._buffer[oldest_key]
            self._stats["pointers_evicted"] += 1
            logger.debug(f"[POINTER_BUFFER] Evicted oldest pointer: {oldest_key}")
        
        self._buffer[fingerprint] = BufferedPointer(pointer=pointer)
        self._stats["pointers_buffered"] += 1
        
        logger.debug(f"[POINTER_BUFFER] Buffered pointer: {fingerprint} ({pointer.provider})")
        return fingerprint
    
    def buffer_kafka_pointer(
        self,
        topic: str,
        partition: int,
        offset: int,
        consumer_group: Optional[str] = None,
    ) -> str:
        """Convenience method to buffer a Kafka pointer."""
        pointer = KafkaPointer(
            topic=topic,
            partition=partition,
            offset=offset,
            consumer_group=consumer_group,
        )
        return self.buffer_pointer(pointer)
    
    def buffer_snowflake_pointer(
        self,
        database: str,
        schema: str,
        table: str,
        stream_id: Optional[str] = None,
        row_cursor: Optional[str] = None,
    ) -> str:
        """Convenience method to buffer a Snowflake pointer."""
        pointer = SnowflakePointer(
            database=database,
            schema=schema,
            table=table,
            stream_id=stream_id,
            row_cursor=row_cursor,
        )
        return self.buffer_pointer(pointer)
    
    def get_pending_pointers(self, limit: int = 100) -> List[BufferedPointer]:
        """Get pointers that are pending processing."""
        pending = [
            bp for bp in self._buffer.values()
            if bp.state == "pending"
        ]
        return pending[:limit]
    
    def get_pointer(self, fingerprint: str) -> Optional[BufferedPointer]:
        """Get a specific buffered pointer by fingerprint."""
        return self._buffer.get(fingerprint)
    
    async def jit_fetch(
        self,
        fingerprint: str,
        on_payload: Callable[[Dict[str, Any]], Awaitable[None]],
    ) -> bool:
        """
        Just-in-Time fetch: retrieve payload from Fabric Plane.
        
        The payload is passed to the callback and NOT stored.
        This ensures Zero-Trust compliance - DCL never persists raw data.
        
        Args:
            fingerprint: The pointer fingerprint to fetch
            on_payload: Async callback to process the payload
        
        Returns:
            True if fetch and processing succeeded, False otherwise
        """
        buffered = self._buffer.get(fingerprint)
        if not buffered:
            logger.warning(f"[JIT_FETCH] Pointer not found: {fingerprint}")
            return False
        
        pointer = buffered.pointer
        client = self._fabric_clients.get(pointer.provider)
        
        if not client:
            logger.warning(f"[JIT_FETCH] No client registered for {pointer.provider}")
            buffered.mark_error(f"No client for {pointer.provider}")
            return False
        
        self._stats["jit_fetches"] += 1
        
        try:
            payload = await client.fetch_payload(pointer)
            
            if payload is None:
                logger.warning(f"[JIT_FETCH] No payload returned for {fingerprint}")
                buffered.mark_error("Payload not found in Fabric")
                self._stats["jit_fetch_failures"] += 1
                return False
            
            buffered.mark_fetched()
            await on_payload(payload)
            buffered.mark_mapped()
            self._stats["mappings_completed"] += 1
            
            logger.debug(f"[JIT_FETCH] Successfully processed {fingerprint}")
            return True
            
        except Exception as e:
            logger.error(f"[JIT_FETCH] Failed for {fingerprint}: {e}")
            buffered.mark_error(str(e))
            self._stats["jit_fetch_failures"] += 1
            return False
    
    def cleanup_expired(self) -> int:
        """Remove expired pointers from buffer."""
        now = datetime.utcnow()
        expired_keys = []
        
        for key, bp in self._buffer.items():
            age = (now - bp.buffered_at).total_seconds()
            if age > self._ttl_seconds:
                expired_keys.append(key)
        
        for key in expired_keys:
            del self._buffer[key]
        
        if expired_keys:
            logger.info(f"[POINTER_BUFFER] Cleaned up {len(expired_keys)} expired pointers")
        
        return len(expired_keys)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get buffer statistics."""
        return {
            **self._stats,
            "current_size": len(self._buffer),
            "max_size": self._max_size,
            "registered_clients": list(self._fabric_clients.keys()),
            "pending_count": sum(1 for bp in self._buffer.values() if bp.state == "pending"),
            "fetched_count": sum(1 for bp in self._buffer.values() if bp.state == "fetched"),
            "mapped_count": sum(1 for bp in self._buffer.values() if bp.state == "mapped"),
            "error_count": sum(1 for bp in self._buffer.values() if bp.state == "error"),
        }
    
    def clear(self) -> None:
        """Clear all buffered pointers."""
        self._buffer.clear()
        logger.info("[POINTER_BUFFER] Buffer cleared")


fabric_pointer_buffer = FabricPointerBuffer()
