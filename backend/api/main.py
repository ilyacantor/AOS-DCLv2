import os
import json
import uuid
from pathlib import Path
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import List, Literal, Optional, Dict, Any
import redis
from backend.domain import Persona, GraphSnapshot, RunMetrics
from backend.engine import DCLEngine
from backend.engine.schema_loader import SchemaLoader
from backend.semantic_mapper import SemanticMapper
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

app = FastAPI(title="DCL Engine API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

engine = DCLEngine()


class RunRequest(BaseModel):
    mode: Literal["Demo", "Farm"] = "Demo"
    run_mode: Literal["Dev", "Prod"] = "Dev"
    personas: Optional[List[Persona]] = None
    source_limit: Optional[int] = 5  # Number of sources to fetch from Farm


class RunResponse(BaseModel):
    graph: GraphSnapshot
    run_metrics: RunMetrics
    run_id: str


@app.get("/api/health")
def health():
    return {"status": "DCL Engine API is running", "version": "1.0.0"}


@app.post("/api/dcl/run", response_model=RunResponse)
def run_dcl(request: RunRequest):
    run_id = str(uuid.uuid4())
    
    personas = request.personas or [Persona.CFO, Persona.CRO, Persona.COO, Persona.CTO]
    
    try:
        snapshot, metrics = engine.build_graph_snapshot(
            mode=request.mode,
            run_mode=request.run_mode,
            personas=personas,
            run_id=run_id,
            source_limit=request.source_limit or 5
        )
        
        return RunResponse(
            graph=snapshot,
            run_metrics=metrics,
            run_id=run_id
        )
    except Exception as e:
        logger.error(f"DCL run failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/dcl/narration/{run_id}")
def get_narration(run_id: str):
    messages = engine.narration.get_messages(run_id)
    return {"run_id": run_id, "messages": messages}


REDIS_CONFIG_KEY = "dcl.ingest.config"

def _get_redis():
    """Get Redis client for config storage."""
    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")
    return redis.from_url(redis_url, decode_responses=True)


class ProvisionPolicy(BaseModel):
    repair_enabled: bool = True


class ProvisionRequest(BaseModel):
    connector_id: str
    source_type: str
    target_url: str
    policy: Optional[ProvisionPolicy] = None


class ProvisionResponse(BaseModel):
    status: str
    connector_id: str
    message: str


@app.post("/api/ingest/provision", response_model=ProvisionResponse)
def provision_connector(request: ProvisionRequest):
    """
    Receive connector configuration from AOD and reconfigure the Ingest Sidecar.
    
    This is the "Handshake" endpoint that allows AOD to dynamically provision
    data connectors without manual configuration.
    """
    try:
        r = _get_redis()
        
        config = {
            "connector_id": request.connector_id,
            "source_type": request.source_type,
            "target_url": request.target_url,
            "policy": request.policy.model_dump() if request.policy else {"repair_enabled": True},
            "provisioned_at": __import__("datetime").datetime.now().isoformat(),
            "version": str(uuid.uuid4())[:8]
        }
        
        r.set(REDIS_CONFIG_KEY, json.dumps(config))
        
        logger.info(f"[Provision] Connector {request.connector_id} provisioned: {request.target_url}")
        
        return ProvisionResponse(
            status="provisioned",
            connector_id=request.connector_id,
            message=f"Sidecar will pick up new config on next poll. Target: {request.target_url}"
        )
    except Exception as e:
        logger.error(f"Provisioning failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/ingest/config")
def get_ingest_config():
    """Get current ingest configuration."""
    try:
        r = _get_redis()
        config_str = r.get(REDIS_CONFIG_KEY)
        if config_str:
            return json.loads(config_str)
        return {"status": "no_config", "message": "No connector provisioned yet"}
    except Exception as e:
        logger.error(f"Failed to get config: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/dcl/monitor/{run_id}")
def get_monitor(run_id: str):
    return {
        "run_id": run_id,
        "monitor_data": {
            "message": "Monitor data endpoint ready",
            "sources": [],
            "ontology": [],
            "conflicts": []
        }
    }


REDIS_TELEMETRY_KEY = "dcl.telemetry"


@app.get("/api/ingest/telemetry")
def get_telemetry():
    """
    Get live telemetry metrics from the Ingest Pipeline.
    
    Returns TPS, processed counts, blocked/healed/verified statistics.
    """
    try:
        r = _get_redis()
        telemetry_str = r.get(REDIS_TELEMETRY_KEY)
        if telemetry_str:
            return json.loads(telemetry_str)
        return {
            "ts": 0,
            "metrics": {
                "total_processed": 0,
                "toxic_blocked": 0,
                "drift_detected": 0,
                "repaired_success": 0,
                "repair_failed": 0,
                "verified_count": 0,
                "verified_failed": 0,
                "tps": 0.0,
                "quality_score": 100.0,
                "repair_rate": 100.0,
                "uptime_seconds": 0.0
            }
        }
    except Exception as e:
        logger.error(f"Failed to get telemetry: {e}")
        raise HTTPException(status_code=500, detail=str(e))


class MappingRequest(BaseModel):
    mode: Literal["Demo", "Farm"] = "Demo"
    mapping_mode: Literal["heuristic", "full"] = "heuristic"
    clear_existing: bool = False


class MappingResponse(BaseModel):
    status: str
    mappings_created: int
    sources_processed: int
    stats: dict


@app.post("/api/dcl/batch-mapping", response_model=MappingResponse)
def run_batch_mapping(request: MappingRequest):
    
    try:
        if request.mode == "Demo":
            sources = SchemaLoader.load_demo_schemas()
        else:
            sources = SchemaLoader.load_farm_schemas(engine.narration, str(uuid.uuid4()))
        
        semantic_mapper = SemanticMapper()
        mappings, stats = semantic_mapper.run_mapping(
            sources=sources,
            mode=request.mapping_mode,
            clear_existing=request.clear_existing
        )
        
        return MappingResponse(
            status="success",
            mappings_created=stats['mappings_created'],
            sources_processed=stats['sources_processed'],
            stats=stats
        )
    except Exception as e:
        logger.error(f"Batch mapping failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


DIST_DIR = Path(__file__).parent.parent.parent / "dist"

if DIST_DIR.exists() and (DIST_DIR / "assets").exists():
    app.mount("/assets", StaticFiles(directory=DIST_DIR / "assets"), name="assets")


@app.get("/")
async def serve_root():
    index_file = DIST_DIR / "index.html"
    if index_file.exists():
        return FileResponse(index_file)
    return {"status": "DCL Engine API is running", "version": "1.0.0", "note": "Frontend not built"}


@app.get("/{full_path:path}")
async def serve_spa(full_path: str):
    if full_path.startswith("api/"):
        raise HTTPException(status_code=404, detail="API route not found")
    index_file = DIST_DIR / "index.html"
    if index_file.exists():
        return FileResponse(index_file)
    raise HTTPException(status_code=404, detail="Frontend not built")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
