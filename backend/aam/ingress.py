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


class AAMProvenanceWire(BaseModel):
    """Provenance block inside each pipe."""

    discovered_by: Optional[str] = None
    discovered_at: Optional[str] = None
    aod_run_id: Optional[str] = None


class AAMPipeWire(BaseModel):
    """Schema for a single pipe in AAM's export-pipes flat response.

    Matches the actual AAM API format:
    {
        "pipe_id": "uuid",
        "display_name": "Tableau",
        "fabric_plane": "IPAAS",
        "source_system": "salesforce",
        "trust_labels": ["governed"],
        "schema_info": null | {...},
        "provenance": {"aod_run_id": "run_xxx"},
        ...
    }
    """

    pipe_id: Optional[str] = None
    display_name: str = Field(default="Unknown")
    fabric_plane: str = Field(default="UNMAPPED")
    modality: Optional[str] = None
    source_system: Optional[str] = None
    transport_kind: Optional[str] = None
    endpoint_ref: Optional[Dict[str, Any]] = None
    entity_scope: List[str] = Field(default_factory=list)
    identity_keys: List[str] = Field(default_factory=list)
    change_semantics: Optional[str] = None
    provenance: AAMProvenanceWire = Field(default_factory=AAMProvenanceWire)
    owner_signals: List[Any] = Field(default_factory=list)
    trust_labels: List[str] = Field(default_factory=list)
    schema_info: Optional[Dict[str, Any]] = None
    freshness: Optional[Dict[str, Any]] = None
    access: Optional[Dict[str, Any]] = None
    version: Optional[int] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class AAMPipesExportWire(BaseModel):
    """Schema for the top-level AAM export-pipes response (flat format).

    AAM returns: { "pipe_count": N, "pipes": [...] }
    """

    export_version: Optional[str] = None
    exported_at: Optional[str] = None
    pipe_count: int = Field(default=0)
    pipes: List[AAMPipeWire] = Field(default_factory=list)


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
    aod_run_id: Optional[str] = None

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

        Handles the flat AAM format: { "pipe_count": N, "pipes": [...] }
        Each pipe has display_name, fabric_plane, trust_labels, provenance, etc.
        """
        wire = AAMPipesExportWire.model_validate(raw)

        all_pipes: List[NormalizedPipe] = []
        plane_buckets: Dict[str, List[NormalizedPipe]] = {}
        discovered_run_id: Optional[str] = None

        for pipe_wire in wire.pipes:
            canonical_id = normalize_source_id(pipe_wire.display_name)
            fabric_plane = (pipe_wire.fabric_plane or "UNMAPPED").lower()
            vendor = pipe_wire.source_system or "unknown"

            # Governance from trust_labels list
            trust_labels = [t.lower() for t in pipe_wire.trust_labels]
            governance = "governed" if "governed" in trust_labels else "ungoverned"

            # Extract fields from schema_info if available
            fields: List[str] = []
            if pipe_wire.schema_info and isinstance(pipe_wire.schema_info, dict):
                fields = list(pipe_wire.schema_info.get("fields", {}).keys()) if isinstance(pipe_wire.schema_info.get("fields"), dict) else []
                if not fields:
                    fields = pipe_wire.schema_info.get("columns", []) if isinstance(pipe_wire.schema_info.get("columns"), list) else []

            # Category from entity_scope or source_system
            category = "other"
            if pipe_wire.entity_scope:
                category = pipe_wire.entity_scope[0]
            elif pipe_wire.source_system:
                category = pipe_wire.source_system

            # Extract aod_run_id from provenance
            if pipe_wire.provenance and pipe_wire.provenance.aod_run_id:
                discovered_run_id = pipe_wire.provenance.aod_run_id

            pipe = NormalizedPipe(
                canonical_id=canonical_id,
                display_name=pipe_wire.display_name,
                pipe_id=pipe_wire.pipe_id,
                fabric_plane=fabric_plane,
                vendor=vendor,
                fields=fields,
                field_count=len(fields),
                category=category,
                governance_status=governance,
                trust_score=85 if governance == "governed" else 60,
                data_quality_score=80 if governance == "governed" else 50,
            )
            all_pipes.append(pipe)

            if fabric_plane not in plane_buckets:
                plane_buckets[fabric_plane] = []
            plane_buckets[fabric_plane].append(pipe)

        # Build plane summaries
        planes: List[NormalizedFabricPlane] = []
        for plane_type, plane_pipes in sorted(plane_buckets.items()):
            vendors = set(p.vendor for p in plane_pipes)
            planes.append(NormalizedFabricPlane(
                plane_type=plane_type,
                vendor=", ".join(sorted(vendors)),
                pipes=plane_pipes,
                pipe_count=len(plane_pipes),
            ))

        # Deterministic hash from sorted canonical IDs
        sorted_ids = sorted(p.canonical_id for p in all_pipes)
        payload_hash = hashlib.sha256(
            "|".join(sorted_ids).encode()
        ).hexdigest()[:12]

        logger.info(
            f"[AAMIngress] Ingested {len(all_pipes)} pipes across "
            f"{len(planes)} fabric planes (aod_run_id={discovered_run_id})"
        )

        return IngestedPayload(
            planes=planes,
            pipes=all_pipes,
            total_connections_reported=wire.pipe_count,
            total_connections_actual=len(all_pipes),
            payload_hash=payload_hash,
            aod_run_id=discovered_run_id,
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
