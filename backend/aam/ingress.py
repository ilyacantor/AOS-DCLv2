"""
AAM Ingress Adapter — single entry point for all AAM data into DCL.

All external AAM data MUST pass through this adapter before being consumed
by any DCL component. The adapter:

1. Validates raw API responses against strict Pydantic schemas
2. Normalizes source identifiers through a single canonical function
3. Returns typed, validated objects that DCL components can trust

NO ad-hoc normalization should exist outside this module.
"""

import hashlib
from typing import List, Optional, Dict, Any

from pydantic import BaseModel, Field

from backend.utils.log_utils import get_logger

logger = get_logger(__name__)


# =============================================================================
# Canonical normalizer — THE ONLY normalization function for source names
# =============================================================================


def normalize_source_id(raw_name: str) -> str:
    """
    Canonical source ID normalization for all AAM data.

    This is the SINGLE source of truth for converting AAM display names
    to canonical IDs.  No other code path should perform its own normalization.

    Rules:
        1. Lowercase
        2. Strip leading/trailing whitespace
        3. Replace spaces and hyphens with underscores
        4. Collapse consecutive underscores
    """
    normalized = raw_name.lower().strip()
    normalized = normalized.replace(" ", "_").replace("-", "_")
    while "__" in normalized:
        normalized = normalized.replace("__", "_")
    return normalized


# =============================================================================
# Wire-format schemas — strict validation of raw AAM API responses
# =============================================================================


class AAMConnectionWire(BaseModel):
    """Schema for a single connection in AAM's export-pipes response."""

    source_name: str = Field(default="Unknown")
    pipe_id: Optional[str] = None
    vendor: Optional[str] = None
    fields: List[str] = Field(default_factory=list)
    category: Optional[str] = None
    governance_status: Optional[str] = None


class AAMFabricPlaneWire(BaseModel):
    """Schema for a fabric plane in AAM's export-pipes response."""

    plane_type: str = Field(default="unknown")
    vendor: str = Field(default="unknown")
    connections: List[AAMConnectionWire] = Field(default_factory=list)


class AAMPipesExportWire(BaseModel):
    """Schema for the top-level AAM export-pipes response."""

    fabric_planes: List[AAMFabricPlaneWire] = Field(default_factory=list)
    total_connections: int = Field(default=0)
    snapshot_name: Optional[str] = Field(default=None)


class AAMPushWire(BaseModel):
    """Schema for a single push in AAM's push history."""

    push_id: Optional[str] = None
    pushed_at: Optional[str] = None
    pipe_count: int = Field(default=0)
    payload_hash: Optional[str] = None
    aod_run_id: Optional[str] = None


# =============================================================================
# Normalized output types — what DCL consumers receive
# =============================================================================


class NormalizedPipe(BaseModel):
    """A single AAM pipe/connection, validated and normalized."""

    canonical_id: str
    display_name: str
    pipe_id: Optional[str]
    fabric_plane: str
    vendor: str
    fields: List[str]
    field_count: int
    category: str
    governance_status: str
    trust_score: int
    data_quality_score: int


class NormalizedFabricPlane(BaseModel):
    """A fabric plane with its normalized pipes."""

    plane_type: str
    vendor: str
    pipes: List[NormalizedPipe]
    pipe_count: int


class IngestedPayload(BaseModel):
    """Complete validated+normalized result of ingesting AAM export-pipes."""

    planes: List[NormalizedFabricPlane]
    pipes: List[NormalizedPipe]
    total_connections_reported: int
    total_connections_actual: int
    payload_hash: str
    snapshot_name: Optional[str] = None

    def get_canonical_ids(self) -> set:
        """Return the set of all canonical source IDs in this payload."""
        return {p.canonical_id for p in self.pipes}

    def get_pipe_by_canonical_id(self, canonical_id: str) -> Optional[NormalizedPipe]:
        """Look up a pipe by canonical ID."""
        for p in self.pipes:
            if p.canonical_id == canonical_id:
                return p
        return None


class NormalizedPush(BaseModel):
    """A single push from AAM's history, validated."""

    push_id: Optional[str]
    pushed_at: Optional[str]
    pipe_count: int
    payload_hash: Optional[str]
    aod_run_id: Optional[str]


# =============================================================================
# Ingress Adapter
# =============================================================================


class AAMIngressAdapter:
    """
    Single entry point for all AAM data into DCL.

    Usage::

        adapter = AAMIngressAdapter()
        payload = adapter.ingest_pipes(raw_dict_from_aam)
        # payload.pipes → List[NormalizedPipe], safe to use everywhere
    """

    def ingest_pipes(self, raw: Dict[str, Any]) -> IngestedPayload:
        """
        Validate and normalize an AAM export-pipes response.

        Args:
            raw: The raw dict from AAMClient.get_pipes()

        Returns:
            IngestedPayload with all pipes validated and normalized

        Raises:
            ValidationError: If the raw data doesn't match expected schema
        """
        wire = AAMPipesExportWire.model_validate(raw)

        all_pipes: List[NormalizedPipe] = []
        planes: List[NormalizedFabricPlane] = []

        for plane_wire in wire.fabric_planes:
            plane_pipes: List[NormalizedPipe] = []

            for conn in plane_wire.connections:
                canonical_id = normalize_source_id(conn.source_name)
                governance = (conn.governance_status or "unknown").lower()

                pipe = NormalizedPipe(
                    canonical_id=canonical_id,
                    display_name=conn.source_name,
                    pipe_id=conn.pipe_id,
                    fabric_plane=plane_wire.plane_type.lower(),
                    vendor=conn.vendor or plane_wire.vendor,
                    fields=conn.fields,
                    field_count=len(conn.fields),
                    category=(conn.category or "other").lower(),
                    governance_status=governance,
                    trust_score=85 if governance == "governed" else 60,
                    data_quality_score=80 if governance == "governed" else 50,
                )
                plane_pipes.append(pipe)
                all_pipes.append(pipe)

            planes.append(NormalizedFabricPlane(
                plane_type=plane_wire.plane_type.lower(),
                vendor=plane_wire.vendor,
                pipes=plane_pipes,
                pipe_count=len(plane_pipes),
            ))

        # Deterministic hash from sorted canonical IDs
        sorted_ids = sorted(p.canonical_id for p in all_pipes)
        payload_hash = hashlib.sha256(
            "|".join(sorted_ids).encode()
        ).hexdigest()[:12]

        return IngestedPayload(
            planes=planes,
            pipes=all_pipes,
            total_connections_reported=wire.total_connections,
            total_connections_actual=len(all_pipes),
            payload_hash=payload_hash,
            snapshot_name=wire.snapshot_name,
        )

    def ingest_push_history(self, raw: Any) -> List[NormalizedPush]:
        """
        Validate and normalize AAM push history.

        Args:
            raw: The raw list from AAMClient.get_push_history()

        Returns:
            List of NormalizedPush, sorted by pushed_at descending
        """
        if not isinstance(raw, list):
            logger.warning(f"[AAMIngress] Push history is not a list: {type(raw)}")
            return []

        pushes: List[NormalizedPush] = []
        for item in raw:
            try:
                wire = AAMPushWire.model_validate(item)
                pushes.append(NormalizedPush(
                    push_id=wire.push_id,
                    pushed_at=wire.pushed_at,
                    pipe_count=wire.pipe_count,
                    payload_hash=wire.payload_hash,
                    aod_run_id=wire.aod_run_id,
                ))
            except Exception as e:
                logger.warning(f"[AAMIngress] Skipping invalid push entry: {e}")

        pushes.sort(key=lambda p: p.pushed_at or "", reverse=True)
        return pushes
