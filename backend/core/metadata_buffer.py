"""
MetadataEventBuffer - Zero-Trust Compliant Buffer

This module implements metadata-only buffering for DCL.
Raw payload data is NEVER serialized to disk or external storage.
Only metadata (request IDs, headers, schema info) is persisted.

Architecture: Self-Healing Mesh & Zero-Trust Vision
- DCL is "The Brain" - Metadata-Only
- Raw data handling belongs to AAM ("The Mesh")
"""

import asyncio
import json
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set
from collections import deque
import hashlib

logger = logging.getLogger("dcl.metadata_buffer")


@dataclass
class MetadataEvent:
    """
    Metadata-only event representation.
    
    Contains ONLY:
    - Request/trace identifiers
    - Schema metadata (field names, types)
    - Timing information
    - Source identifiers
    
    NEVER contains:
    - Raw payload body
    - PII or sensitive data
    - Actual record values
    """
    event_id: str
    trace_id: str
    source_id: str
    source_type: str
    timestamp_ms: int
    
    schema_fingerprint: str
    field_names: List[str]
    field_types: Dict[str, str]
    record_type: str
    
    is_drifted: bool = False
    drift_fields: List[str] = field(default_factory=list)
    
    processing_status: str = "pending"
    
    @classmethod
    def from_payload_metadata(
        cls,
        event_id: str,
        trace_id: str,
        source_id: str,
        source_type: str,
        payload: Dict[str, Any],
    ) -> "MetadataEvent":
        """
        Extract metadata from a payload WITHOUT storing the payload itself.
        
        The payload is inspected for structure but never persisted.
        """
        field_names = []
        field_types = {}
        
        def extract_fields(obj: Any, prefix: str = "") -> None:
            if isinstance(obj, dict):
                for k, v in obj.items():
                    full_key = f"{prefix}.{k}" if prefix else k
                    if isinstance(v, dict):
                        extract_fields(v, full_key)
                    elif isinstance(v, list):
                        field_names.append(full_key)
                        field_types[full_key] = "array"
                    else:
                        field_names.append(full_key)
                        field_types[full_key] = type(v).__name__
        
        extract_fields(payload)
        
        schema_str = json.dumps(sorted(field_names))
        schema_fingerprint = hashlib.sha256(schema_str.encode()).hexdigest()[:16]
        
        record_type = payload.get("record_type", "unknown")
        if isinstance(record_type, dict):
            record_type = "complex"
        
        return cls(
            event_id=event_id,
            trace_id=trace_id,
            source_id=source_id,
            source_type=source_type,
            timestamp_ms=int(time.time() * 1000),
            schema_fingerprint=schema_fingerprint,
            field_names=field_names,
            field_types=field_types,
            record_type=str(record_type),
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary (metadata only, safe for storage)."""
        return {
            "event_id": self.event_id,
            "trace_id": self.trace_id,
            "source_id": self.source_id,
            "source_type": self.source_type,
            "timestamp_ms": self.timestamp_ms,
            "schema_fingerprint": self.schema_fingerprint,
            "field_names": self.field_names,
            "field_types": self.field_types,
            "record_type": self.record_type,
            "is_drifted": self.is_drifted,
            "drift_fields": self.drift_fields,
            "processing_status": self.processing_status,
        }


class MetadataEventBuffer:
    """
    In-memory buffer for metadata events only.
    
    Zero-Trust Principles:
    1. Raw payloads are NEVER stored
    2. Only metadata (IDs, schema structure) is buffered
    3. Buffer is in-memory only (no disk serialization)
    4. Automatic eviction prevents memory bloat
    
    This replaces the previous RecordBuffer which violated
    the metadata-only security posture.
    """
    
    def __init__(
        self,
        max_size: int = 10000,
        ttl_seconds: int = 300,
    ):
        self._buffer: deque[MetadataEvent] = deque(maxlen=max_size)
        self._max_size = max_size
        self._ttl_seconds = ttl_seconds
        self._event_index: Dict[str, MetadataEvent] = {}
        self._source_counts: Dict[str, int] = {}
        self._lock = asyncio.Lock()
        
        self._total_received = 0
        self._total_evicted = 0
    
    async def push(self, event: MetadataEvent) -> None:
        """
        Add a metadata event to the buffer.
        
        Note: This stores ONLY metadata, never raw payloads.
        """
        async with self._lock:
            if len(self._buffer) >= self._max_size:
                evicted = self._buffer.popleft()
                self._event_index.pop(evicted.event_id, None)
                self._total_evicted += 1
            
            self._buffer.append(event)
            self._event_index[event.event_id] = event
            
            self._source_counts[event.source_id] = (
                self._source_counts.get(event.source_id, 0) + 1
            )
            self._total_received += 1
    
    async def get_by_id(self, event_id: str) -> Optional[MetadataEvent]:
        """Retrieve a metadata event by ID."""
        async with self._lock:
            return self._event_index.get(event_id)
    
    async def get_recent(self, limit: int = 100) -> List[MetadataEvent]:
        """Get most recent metadata events."""
        async with self._lock:
            return list(self._buffer)[-limit:]
    
    async def get_by_source(self, source_id: str, limit: int = 100) -> List[MetadataEvent]:
        """Get metadata events from a specific source."""
        async with self._lock:
            return [
                e for e in self._buffer
                if e.source_id == source_id
            ][-limit:]
    
    async def get_drifted_events(self, limit: int = 100) -> List[MetadataEvent]:
        """Get events that had drift detected."""
        async with self._lock:
            return [
                e for e in self._buffer
                if e.is_drifted
            ][-limit:]
    
    def get_stats(self) -> Dict[str, Any]:
        """Get buffer statistics (no payload data)."""
        return {
            "current_size": len(self._buffer),
            "max_size": self._max_size,
            "total_received": self._total_received,
            "total_evicted": self._total_evicted,
            "source_counts": dict(self._source_counts),
            "unique_sources": len(self._source_counts),
        }
    
    async def clear(self) -> int:
        """Clear the buffer, return count of cleared events."""
        async with self._lock:
            count = len(self._buffer)
            self._buffer.clear()
            self._event_index.clear()
            return count


class PayloadSecurityGuard:
    """
    Security guard that prevents raw payload data from being persisted.
    
    This class provides utilities to:
    1. Validate that only metadata is being stored
    2. Redact sensitive fields from logs
    3. Detect and block attempts to serialize payloads
    """
    
    BLOCKED_FIELDS = {
        "body", "payload", "data", "content", "raw",
        "ssn", "password", "secret", "token", "key",
        "credit_card", "card_number", "cvv", "pin",
    }
    
    PII_PATTERNS = {
        "email", "phone", "address", "name", "dob",
        "birth", "social", "license", "passport",
    }
    
    @classmethod
    def is_safe_for_storage(cls, data: Dict[str, Any]) -> bool:
        """
        Check if data is safe for storage (metadata only).
        
        Returns False if payload body or sensitive data detected.
        """
        def check_keys(obj: Any, path: str = "") -> bool:
            if isinstance(obj, dict):
                for k, v in obj.items():
                    lower_key = k.lower()
                    if lower_key in cls.BLOCKED_FIELDS:
                        logger.warning(
                            f"SECURITY: Blocked field '{k}' at path '{path}' "
                            f"- payload data must not be stored"
                        )
                        return False
                    if not check_keys(v, f"{path}.{k}"):
                        return False
            elif isinstance(obj, list):
                for i, item in enumerate(obj):
                    if not check_keys(item, f"{path}[{i}]"):
                        return False
            return True
        
        return check_keys(data)
    
    @classmethod
    def extract_safe_metadata(cls, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract only safe metadata fields from a record.
        
        Strips out payload body and sensitive data.
        """
        safe_fields = {
            "event_id", "trace_id", "request_id", "correlation_id",
            "source_id", "source_type", "timestamp", "schema_version",
            "record_type", "field_count", "is_valid",
        }
        
        result = {}
        for key, value in record.items():
            lower_key = key.lower()
            if lower_key in safe_fields:
                result[key] = value
            elif lower_key in cls.BLOCKED_FIELDS:
                result[f"{key}_redacted"] = "[REDACTED]"
        
        return result
    
    @classmethod
    def assert_no_payload(cls, data: Dict[str, Any], context: str = "") -> None:
        """
        Raise an error if payload data is detected.
        
        Use this as a build-time/runtime constraint.
        """
        if not cls.is_safe_for_storage(data):
            raise SecurityError(
                f"Zero-Trust Violation: Attempted to store payload data. "
                f"Context: {context}. DCL is metadata-only."
            )


class SecurityError(Exception):
    """Raised when Zero-Trust security policy is violated."""
    pass
