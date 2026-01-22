# DCL Engine - Data Connectivity Layer

## Overview
The DCL (Data Connectivity Layer) Engine is a full-stack application designed to ingest and unify schemas and sample data from diverse sources into a common ontology using AI and heuristics. It visualizes data flow via an interactive Sankey diagram and supports two data modes: Demo (legacy sources) and Farm (synthetic data). The system provides persona-driven business logic for roles like CFO, CRO, COO, and CTO. Its core capabilities include multi-source schema ingestion, AI-powered ontology unification, RAG for intelligent mapping, real-time process narration, and enterprise monitoring with flexible runtime modes.

## User Preferences
- Preferred communication style: Simple, everyday language
- No fake/mock data to pass tests - always use real integrations
- PST timezone for all timestamps (12-hour in controls, 24-hour in terminal)

## System Architecture

### Frontend Architecture
- **Framework**: React 18 + TypeScript + Vite
- **Visualization**: D3.js (d3-sankey) for 4-layer Sankey diagrams
- **Styling**: Tailwind CSS with glassmorphism effects
- **Key Components**:
  - `App.tsx` - State management and data flow
  - `ControlPanel.tsx` - Mode selection (Demo/Farm, Dev/Prod), persona toggles
  - `SankeyGraph.tsx` - Interactive 4-layer visualization (L0→L1→L2→L3)
  - `NarrationPanel.tsx` - Terminal-style real-time processing logs
  - `MonitorPanel.tsx` - Persona-specific metrics dashboard
  - `TelemetryRibbon.tsx` - Live industrial metrics (Farm mode only)

### Backend Architecture
- **Framework**: FastAPI + Python 3.11 + Pydantic V2
- **Server**: Uvicorn on port 8000
- **Layers**:
  - **API Layer**: RESTful endpoints, CORS, validation
  - **Domain Layer**: Core models (SourceSystem, TableSchema, OntologyConcept, Mapping, Persona)
  - **Engine Layer**: DCLEngine orchestrator, SchemaLoader, MappingService, RAGService, NarrationService
  - **Ingest Layer**: Sidecar (stream consumer), Consumer (semantic mapping), MetricsCollector

### Data Storage
- **PostgreSQL**: Schema persistence, mapping storage, source registration
- **Redis**: Real-time streams and pub/sub
  - `dcl.logs` - Narration broadcast
  - `dcl.ingest.raw` - Raw ingested records
  - `dcl.ingest.config` - Dynamic connector configuration (AOD handshake)
  - `dcl.telemetry` - Live metrics broadcast
- **Pinecone**: Vector database for RAG semantic matching (Prod mode)
- **Local CSV**: Demo mode schema files

## API Endpoints

### Core Pipeline
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/dcl/run` | POST | Execute pipeline (params: data_mode, run_mode, personas) |
| `/api/dcl/graph` | GET | Get current graph snapshot |
| `/api/dcl/narration/{session_id}` | GET | Poll narration messages |

### Ingest Pipeline
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/ingest/provision` | POST | AOD handshake - provision new connector |
| `/api/ingest/telemetry` | GET | Live industrial metrics |

## Node List (24 Nodes)

### L0 - Pipeline (1 node)
- `pipe_demo` / `pipe_farm` - Pipeline entry point

### L1 - Sources (11 nodes)
- `source_salesforce_crm` - Salesforce CRM
- `source_hubspot_crm` - HubSpot CRM
- `source_dynamics_crm` - Microsoft Dynamics CRM
- `source_sap_erp` - SAP ERP
- `source_netsuite_erp` - NetSuite ERP
- `source_mongodb_customer_db` - MongoDB Customer DB
- `source_supabase_app_db` - Supabase App DB
- `source_dw_dim_customer` - Data Warehouse Dim Customer
- `source_discovered_legacy_sql` - Legacy SQL (discovered)
- `source_mulesoft_mock` - MuleSoft ERP Sync
- `source_mulesoft_stream` - MuleSoft Stream (Farm)

### L2 - Ontology (8 concepts)
- `ontology_account` - Account
- `ontology_opportunity` - Opportunity
- `ontology_revenue` - Revenue
- `ontology_cost` - Cost
- `ontology_date` - Date/Timestamp
- `ontology_health` - Health Score
- `ontology_usage` - Usage Metrics
- `ontology_aws_resource` - AWS Resource

### L3 - Business Logic Layer (4 personas)
- `bll_cfo` - CFO persona
- `bll_cro` - CRO persona
- `bll_coo` - COO persona
- `bll_cto` - CTO persona

## Environment Variables
| Variable | Description |
|----------|-------------|
| `FARM_API_URL` | AOS-Farm API base URL |
| `RUN_MODE` | `dev` (heuristic) or `prod` (LLM/RAG) |
| `ENABLE_CHAOS` | Enable drift injection for testing |
| `DATABASE_URL` | PostgreSQL connection string |
| `REDIS_URL` | Redis connection string |
| `GEMINI_API_KEY` | Google Gemini API key |
| `OPENAI_API_KEY` | OpenAI API key |
| `PINECONE_API_KEY` | Pinecone API key |

## Phase 4: Autonomous Handshake (January 2026)

### Dynamic Connector Provisioning
AOD (Autonomous Operations Director) can dynamically provision connectors without manual intervention.

**Provision Endpoint:**
```bash
POST /api/ingest/provision
{
  "connector_id": "mule_auto_01",
  "source_type": "mulesoft",
  "target_url": "https://farm.url/api/stream/synthetic/mulesoft?chaos=true",
  "policy": { "repair_enabled": true }
}
```

**Handshake Flow:**
1. AOD calls POST /api/ingest/provision
2. Backend stores config in Redis (`dcl.ingest.config`)
3. Sidecar polls every 5 seconds, detects version change
4. Sidecar logs "[HANDSHAKE]" and reconnects to new stream
5. Policy settings (repair_enabled) take effect immediately

## Phase 5: Industrial Dashboard (January 2026)

### Industrial Mode
Pivots from "Story Mode" (slow, narrated) to "Industrial Mode" (fast, massive, validated).

**Key Changes:**
1. **Velocity** - Removed all artificial delays (no more 1.5-3s latency)
2. **Telemetry** - Live counters replace chat logs
3. **Closed-Loop Verification** - Farm confirms repairs are correct

**Telemetry Endpoint:**
```json
GET /api/ingest/telemetry
{
  "ts": 1768853167248,
  "metrics": {
    "total_processed": 1523,
    "toxic_blocked": 12,
    "drift_detected": 45,
    "repaired_success": 42,
    "repair_failed": 3,
    "verified_count": 42,
    "verified_failed": 0,
    "tps": 142.5,
    "quality_score": 100.0,
    "repair_rate": 93.3,
    "uptime_seconds": 35.2
  }
}
```

**Dashboard Features:**
- TelemetryRibbon shows: TPS, Processed, Blocked, Healed, Verified, Quality Score
- Terminal Mode narration: monospace, matrix-style green text, auto-scroll
- Updates every 500ms

## File Structure
```
├── backend/
│   ├── api/main.py           # FastAPI app, endpoints
│   ├── domain/models.py      # Pydantic models
│   ├── engine/
│   │   ├── dcl_engine.py     # Main orchestrator
│   │   ├── schema_loader.py  # CSV/Farm schema loading
│   │   ├── mapping_service.py # Heuristic + LLM mapping
│   │   └── rag_service.py    # Pinecone RAG
│   ├── ingest/
│   │   ├── ingest_agent.py   # Sidecar (stream → Redis)
│   │   └── consumer.py       # Consumer (Redis → semantic mapping)
│   └── utils/
│       └── metrics.py        # MetricsCollector for telemetry
├── src/
│   ├── App.tsx               # Main React app
│   └── components/
│       ├── ControlPanel.tsx
│       ├── SankeyGraph.tsx
│       ├── NarrationPanel.tsx
│       ├── MonitorPanel.tsx
│       └── TelemetryRibbon.tsx
├── data/schemas/             # Demo mode CSV files
└── run_backend.py            # Backend entry point
```

## External Dependencies
- **Google Gemini**: Schema understanding (Gemini 2.5 Flash)
- **OpenAI**: Mapping validation (GPT-4-mini)
- **Pinecone**: Vector database for RAG
- **PostgreSQL**: Schema and mapping persistence
- **Redis**: Real-time streaming and pub/sub
- **httpx**: Async HTTP client for Farm API
