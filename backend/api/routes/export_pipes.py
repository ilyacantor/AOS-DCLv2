"""
DCL Pipe Definition routes — receives structure from AAM /export-pipes.

Handles:
  POST /api/dcl/export-pipes   — receive pipe schemas from AAM (Path 1)
  GET  /api/dcl/export-pipes   — list registered pipe definitions
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import List, Optional

from backend.core.constants import utc_now
from backend.api.pipe_store import PipeDefinition, get_pipe_store
from backend.api.ingest import get_ingest_store, ActivityEntry
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/api/dcl/export-pipes", tags=["Export Pipes"])


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

class ExportPipesConnection(BaseModel):
    """A single connection from AAM's export-pipes payload."""
    pipe_id: str = Field(..., description="THE JOIN KEY — must match Farm's x-pipe-id")
    candidate_id: str = Field("", description="Original candidate ID (provenance)")
    source_name: str = ""
    vendor: str = ""
    category: str = ""
    governance_status: Optional[str] = None
    fields: List[str] = Field(default_factory=list)
    entity_scope: Optional[str] = None
    identity_keys: List[str] = Field(default_factory=list)
    transport_kind: Optional[str] = None
    modality: Optional[str] = None
    change_semantics: Optional[str] = None
    health: str = "unknown"
    last_sync: Optional[str] = None
    asset_key: str = ""
    aod_asset_id: Optional[str] = None


class ExportPipesFabricPlane(BaseModel):
    """A fabric plane containing connections."""
    plane_type: str
    vendor: str = ""
    connection_count: int = 0
    health: str = "unknown"
    connections: List[ExportPipesConnection] = Field(default_factory=list)


class SkippedConnection(BaseModel):
    """A connection AAM skipped (pending inference, etc.)."""
    candidate_id: str = ""
    vendor: str = ""
    reason: str = ""
    discovered_at: Optional[str] = None


class ExportPipesRequest(BaseModel):
    """The DCLExportResponse schema from AAM."""
    aod_run_id: Optional[str] = None
    timestamp: Optional[str] = None
    source: str = "aam"
    total_connections: int = 0
    fabric_planes: List[ExportPipesFabricPlane] = Field(default_factory=list)
    skipped_connections: List[SkippedConnection] = Field(default_factory=list)
    skipped_count: int = 0


class ExportPipesResponse(BaseModel):
    """Confirmation that pipe definitions were stored."""
    status: str
    pipes_registered: int
    pipe_ids: List[str]
    skipped_noted: int
    aod_run_id: Optional[str]
    timestamp: str


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("", response_model=ExportPipesResponse)
def receive_export_pipes(request: ExportPipesRequest):
    """
    Receive pipe definitions from AAM (Path 1 — Structure Path).

    AAM pushes pipe schemas here so DCL knows what data to expect.
    These definitions are the join target for Farm's /ingest data.
    The JOIN key is pipe_id.
    """
    now = utc_now()
    pipe_store = get_pipe_store()
    definitions = []

    for plane in request.fabric_planes:
        for conn in plane.connections:
            if not conn.pipe_id:
                logger.warning(
                    f"[ExportPipes] Skipping connection with empty pipe_id "
                    f"(source_name={conn.source_name})"
                )
                continue

            defn = PipeDefinition(
                pipe_id=conn.pipe_id,
                candidate_id=conn.candidate_id,
                source_name=conn.source_name,
                vendor=conn.vendor,
                category=conn.category,
                governance_status=conn.governance_status,
                fields=conn.fields,
                entity_scope=conn.entity_scope,
                identity_keys=conn.identity_keys,
                transport_kind=conn.transport_kind,
                modality=conn.modality,
                change_semantics=conn.change_semantics,
                health=conn.health,
                last_sync=conn.last_sync,
                asset_key=conn.asset_key,
                aod_asset_id=conn.aod_asset_id,
                fabric_plane=plane.plane_type,
                received_at=now,
            )
            definitions.append(defn)

    if not definitions:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "NO_PIPE_DEFINITIONS",
                "message": "No valid pipe definitions found in payload. "
                           "Each connection must have a non-empty pipe_id.",
            },
        )

    receipt = pipe_store.register_batch(
        definitions=definitions,
        aod_run_id=request.aod_run_id,
        source=request.source,
    )

    logger.info(
        f"[ExportPipes] Stored {len(definitions)} pipe definitions "
        f"from {len(request.fabric_planes)} fabric planes "
        f"(aod_run_id={request.aod_run_id})"
    )

    # --- Record Path 1 activity (Structure) ---
    unique_sources = sorted(set(d.source_name for d in definitions if d.source_name))
    unique_fabrics = sorted(set(d.fabric_plane for d in definitions if d.fabric_plane))
    # Derive a snapshot_name from the first pipe_id pattern or aod_run_id
    snap_name = ""
    if definitions:
        # pipe_ids often encode the snapshot, e.g. "pipe_salesforce_NetLabs-RWC4"
        # Use aod_run_id as the primary identifier
        snap_name = request.aod_run_id or f"export-{now[:10]}"

    try:
        ingest_store = get_ingest_store()
        ingest_store.record_activity(ActivityEntry(
            phase="structure",
            source="AAM",
            snapshot_name=snap_name,
            run_id=request.aod_run_id or "",
            timestamp=now,
            pipes=len(definitions),
            sors=len(unique_sources),
            fabrics=len(unique_fabrics),
            dispatch_id=f"aam_{request.aod_run_id[:20]}" if request.aod_run_id else "",
            aod_run_id=request.aod_run_id or "",
        ))
    except Exception as e:
        logger.warning(f"[ExportPipes] Failed to record activity: {e}")

    skipped_noted = len(request.skipped_connections) if request.skipped_connections else request.skipped_count
    if request.skipped_connections:
        for sc in request.skipped_connections:
            logger.info(
                f"[ExportPipes] Skipped connection noted: "
                f"candidate={sc.candidate_id} vendor={sc.vendor} reason={sc.reason}"
            )

    return ExportPipesResponse(
        status="accepted",
        pipes_registered=len(definitions),
        pipe_ids=receipt.pipe_ids,
        skipped_noted=skipped_noted,
        aod_run_id=request.aod_run_id,
        timestamp=now,
    )


@router.get("")
def list_pipe_definitions():
    """List all registered pipe definitions (for diagnostics)."""
    pipe_store = get_pipe_store()
    definitions = pipe_store.get_all_definitions()
    return {
        "pipe_count": len(definitions),
        "pipes": [
            {
                "pipe_id": d.pipe_id,
                "source_name": d.source_name,
                "vendor": d.vendor,
                "category": d.category,
                "fabric_plane": d.fabric_plane,
                "field_count": len(d.fields),
                "fields": d.fields,
                "health": d.health,
                "received_at": d.received_at,
            }
            for d in definitions
        ],
        "stats": pipe_store.get_stats(),
    }
