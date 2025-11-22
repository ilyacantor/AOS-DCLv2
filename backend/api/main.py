import os
import uuid
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Literal, Optional
from backend.domain import Persona, GraphSnapshot, RunMetrics
from backend.engine import DCLEngine

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


class RunResponse(BaseModel):
    graph: GraphSnapshot
    run_metrics: RunMetrics
    run_id: str


@app.get("/")
def root():
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
            run_id=run_id
        )
        
        return RunResponse(
            graph=snapshot,
            run_metrics=metrics,
            run_id=run_id
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/dcl/narration/{run_id}")
def get_narration(run_id: str):
    messages = engine.narration.get_messages(run_id)
    return {"run_id": run_id, "messages": messages}


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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
