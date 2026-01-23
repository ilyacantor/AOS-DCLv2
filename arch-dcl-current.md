# DCL Engine Architecture - Current State

## System Overview

The DCL (Data Connectivity Layer) Engine is a 4-layer data unification platform that:
1. Ingests schemas from diverse sources (CRM, ERP, databases)
2. Maps fields to a common ontology using AI/heuristics
3. Routes unified data to persona-specific business logic
4. Visualizes the entire flow via interactive Sankey diagrams

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              DCL ENGINE                                      │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐           │
│  │   L0     │     │   L1     │     │   L2     │     │   L3     │           │
│  │  PIPE    │────▶│ SOURCES  │────▶│ ONTOLOGY │────▶│   BLL    │           │
│  │          │     │          │     │          │     │          │           │
│  │ Demo/    │     │ Salesforce│    │ Account  │     │   CFO    │           │
│  │ Farm     │     │ HubSpot  │     │ Revenue  │     │   CRO    │           │
│  │          │     │ SAP      │     │ Cost     │     │   COO    │           │
│  │          │     │ NetSuite │     │ Health   │     │   CTO    │           │
│  │          │     │ MongoDB  │     │ Usage    │     │          │           │
│  │          │     │ MuleSoft │     │ ...      │     │          │           │
│  └──────────┘     └──────────┘     └──────────┘     └──────────┘           │
│                                                                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                          INGEST PIPELINE                                     │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐                      │
│  │   SIDECAR   │───▶│    REDIS    │───▶│  CONSUMER   │                      │
│  │             │    │             │    │             │                      │
│  │ Stream SSE  │    │ dcl.ingest  │    │ Semantic    │                      │
│  │ Detect Drift│    │ .raw        │    │ Mapping     │                      │
│  │ Repair      │    │             │    │             │                      │
│  │ Verify      │    │ dcl.logs    │    │ Persist     │                      │
│  └─────────────┘    └─────────────┘    └─────────────┘                      │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Component Details

### Frontend (React + TypeScript + Vite)

| Component | Purpose |
|-----------|---------|
| `App.tsx` | State management, data fetching, layout |
| `ControlPanel.tsx` | Mode toggles (Demo/Farm, Dev/Prod), persona selection |
| `SankeyGraph.tsx` | D3-based 4-layer flow visualization |
| `NarrationPanel.tsx` | Terminal-style real-time logs |
| `MonitorPanel.tsx` | Persona-specific metrics dashboard |
| `TelemetryRibbon.tsx` | Live industrial counters (Farm mode) |

### Backend (FastAPI + Python)

| Module | Purpose |
|--------|---------|
| `api/main.py` | REST endpoints, CORS, request handling |
| `domain/models.py` | Pydantic models for all entities |
| `engine/dcl_engine.py` | Main orchestrator, graph building |
| `engine/schema_loader.py` | Load schemas from CSV or Farm API |
| `engine/mapping_service.py` | Heuristic + LLM field mapping |
| `engine/rag_service.py` | Pinecone vector search |
| `ingest/ingest_agent.py` | Sidecar: stream → detect → repair → Redis |
| `ingest/consumer.py` | Consumer: Redis → semantic mapping → DB |
| `utils/metrics.py` | MetricsCollector for telemetry broadcast |

### Data Flow

```
1. DEMO MODE:
   CSV Files → SchemaLoader → MappingService → GraphBuilder → Frontend

2. FARM MODE:
   Farm SSE Stream → Sidecar → Redis → Consumer → DB
                         ↓
                   Detect Drift
                         ↓
                   Repair (Farm SoT API)
                         ↓
                   Verify (Farm Verify API)
                         ↓
                   Broadcast Telemetry
```

## Key Features by Phase

### Phase 1-3: Core Pipeline
- Multi-source schema ingestion (9 demo sources)
- Heuristic field mapping (string similarity)
- LLM-enhanced mapping (Prod mode)
- RAG semantic matching (Pinecone)
- Interactive Sankey visualization
- Persona-driven data routing

### Phase 4: Connector Provisioning
- Dynamic connector provisioning from AAM
- Redis-based configuration polling
- Policy-driven repair toggling
- Zero-touch stream switching

### Phase 5: Industrial Dashboard
- Maximum throughput (no artificial delays)
- MetricsCollector broadcasting every 0.5s
- Closed-loop verification with Farm
- TelemetryRibbon with live counters
- Terminal-style narration panel

## Redis Keys

| Key | Type | Purpose |
|-----|------|---------|
| `dcl.logs` | Pub/Sub | Narration message broadcast |
| `dcl.ingest.raw` | Stream | Raw ingested records |
| `dcl.ingest.config` | Hash | Dynamic connector config (AAM) |
| `dcl.telemetry` | String | Live metrics JSON |

## Telemetry Metrics

| Metric | Description |
|--------|-------------|
| `total_processed` | Records processed |
| `toxic_blocked` | Records rejected as toxic |
| `drift_detected` | Records with drift detected |
| `repaired_success` | Successfully repaired records |
| `repair_failed` | Failed repair attempts |
| `verified_count` | Records verified by Farm |
| `verified_failed` | Verification failures |
| `tps` | Transactions per second |
| `quality_score` | Farm verification accuracy % |
| `repair_rate` | Successful repair percentage |

## API Reference

### Core Endpoints
```
POST /api/dcl/run?data_mode={demo|farm}&run_mode={dev|prod}
     Body: { "personas": ["CFO", "CRO", "COO", "CTO"] }
     Returns: { "graph": {...}, "session_id": "..." }

GET  /api/dcl/graph
     Returns: Current graph snapshot

GET  /api/dcl/narration/{session_id}
     Returns: { "messages": [...] }
```

### Ingest Endpoints
```
POST /api/ingest/provision
     Body: { "connector_id": "...", "source_type": "...", 
             "target_url": "...", "policy": {...} }
     Returns: { "status": "provisioned", "version": 2 }

GET  /api/ingest/telemetry
     Returns: { "ts": ..., "metrics": {...} }
```

## Node Inventory

**Total: 24 nodes**

- L0 (1): Pipeline entry
- L1 (11): Salesforce, HubSpot, Dynamics, SAP, NetSuite, MongoDB, Supabase, DW, Legacy SQL, MuleSoft Mock, MuleSoft Stream
- L2 (8): Account, Opportunity, Revenue, Cost, Date, Health, Usage, AWS Resource
- L3 (4): CFO, CRO, COO, CTO

## Technology Stack

| Layer | Technology |
|-------|------------|
| Frontend | React 18, TypeScript, Vite, D3.js, Tailwind CSS |
| Backend | FastAPI, Python 3.11, Pydantic V2, Uvicorn |
| Database | PostgreSQL (persistence), Redis (streaming) |
| Vector DB | Pinecone (RAG operations) |
| AI/LLM | Google Gemini, OpenAI GPT-4 |
| HTTP | httpx (async), SSE streaming |
