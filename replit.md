# DCL Engine - Data Connectivity Layer

**Last Updated:** January 27, 2026

## Overview
The DCL (Data Connectivity Layer) Engine is a **metadata-only semantic mapping engine** that maps raw technical fields from source systems to business concepts and visualizes who uses what. It answers one question: "What does this field mean to the business?"

**DCL does NOT:**
- Store raw data
- Process payloads
- Track lineage
- Perform ETL
- Execute queries (moved to AOS-NLQ)
- Parse natural language (moved to AOS-NLQ)
- Assemble answers (moved to AOS-NLQ)

**DCL DOES:**
- Manage schema structures (field names, types)
- Maintain semantic mappings (field → concept)
- Provide ontology management
- Support graph visualization (Sankey diagrams)
- Handle pointer buffering (NOT payload buffering)

## Architecture Changes (January 27, 2026)

**NLQ & BLL Moved to AOS-NLQ Repository:**
- All NLQ (Natural Language Query) functionality has been moved to `AOS-NLQ`
- All BLL (Business Logic Layer) functionality has been absorbed into `AOS-NLQ`
- DCL is now purely a metadata/semantic layer
- Legacy endpoints return HTTP 410 Gone with `MOVED_TO_AOS_NLQ`

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

### Backend Architecture
- **Framework**: FastAPI + Python 3.11 + Pydantic V2
- **Server**: Uvicorn on port 8000
- **Layers**:
  - **API Layer**: RESTful endpoints, CORS, validation
  - **Domain Layer**: Core models (SourceSystem, TableSchema, OntologyConcept, Mapping, Persona)
  - **Engine Layer**: DCLEngine orchestrator, SchemaLoader, MappingService, RAGService, NarrationService
  - **Core Layer**: Zero-Trust components for metadata-only architecture
  - **LLM Layer**: Mapping validation with GPT-4o-mini

### Zero-Trust Core Components (backend/core/)

**Fabric Plane Mesh (January 2026 Pivot):**
AAM connects to 4 Fabric Planes (not individual SaaS apps):
- **iPaaS** (Workato, MuleSoft) - Integration flow control
- **API_GATEWAY** (Kong, Apigee) - Managed API access
- **EVENT_BUS** (Kafka, EventBridge) - Streaming backbone
- **DATA_WAREHOUSE** (Snowflake, BigQuery) - Source of Truth

**Pointer Buffering Strategy:**
DCL buffers ONLY Fabric Pointers (offsets, cursors) - NEVER payloads:
- Kafka: `{ topic, partition, offset }`
- Snowflake: `{ table, stream_id, row_cursor }`
- Just-in-Time fetching: payload retrieved only when semantic mapper requests

**Core Components:**
- **FabricPointerBuffer**: Pointer-only buffering with JIT fetch capability
- **FabricPlane Types**: KafkaPointer, SnowflakePointer, BigQueryPointer, EventBridgePointer
- **TopologyAPI**: Service absorbing visualization from AAM health data
- **SecurityConstraints**: Build/runtime guards preventing payload.body writes

### Data Storage
- **PostgreSQL**: Schema persistence, mapping storage, source registration
- **Redis**: Real-time pub/sub for narration (`dcl.logs`)
- **Pinecone**: Vector database for RAG semantic matching (Prod mode)
- **Local CSV**: Demo mode schema files

## API Endpoints

### Core Pipeline
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/dcl/run` | POST | Execute pipeline (params: data_mode, run_mode, personas) |
| `/api/dcl/narration/{session_id}` | GET | Poll narration messages |
| `/api/dcl/batch-mapping` | POST | Run semantic mapping batch |

### Topology API
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/topology` | GET | Unified topology graph (merges DCL semantic graph with AAM health) |
| `/api/topology/health` | GET | Connection health data from mesh |
| `/api/topology/stats` | GET | Topology service statistics |

### Dataset & Presets
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/datasets/current` | GET | Get current dataset info |
| `/api/presets` | GET | Get UI preset suggestions |

### Legacy Endpoints (REMOVED)
All legacy endpoints return HTTP 410 Gone:
- `/api/ingest/*` → `MOVED_TO_AAM`
- `/api/nlq/*` → `MOVED_TO_AOS_NLQ`
- `/api/bll/*` → `MOVED_TO_AOS_NLQ`
- `/api/execute` → `MOVED_TO_AOS_NLQ`

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
| `DATABASE_URL` | PostgreSQL connection string |
| `REDIS_URL` | Redis connection string |
| `OPENAI_API_KEY` | OpenAI API key (embeddings + validation) |
| `PINECONE_API_KEY` | Pinecone API key |

## File Structure
```
├── backend/
│   ├── api/main.py           # FastAPI app, endpoints
│   ├── domain/models.py      # Pydantic models
│   ├── engine/
│   │   ├── dcl_engine.py     # Main orchestrator
│   │   ├── schema_loader.py  # CSV/Farm schema loading
│   │   ├── mapping_service.py # Heuristic mapping
│   │   └── rag_service.py    # Pinecone RAG
│   ├── core/
│   │   ├── fabric_plane.py   # Fabric Plane types
│   │   ├── pointer_buffer.py # Pointer buffering
│   │   └── topology_api.py   # Topology service
│   ├── dcl/
│   │   ├── __init__.py       # DCL module (metadata-only)
│   │   └── routes.py         # Dataset/preset endpoints
│   └── llm/
│       └── mapping_validator.py # GPT-4o-mini validation
├── src/
│   ├── App.tsx               # Main React app
│   └── components/
│       ├── ControlPanel.tsx
│       ├── SankeyGraph.tsx
│       ├── NarrationPanel.tsx
│       └── MonitorPanel.tsx
├── data/schemas/             # Demo mode CSV files
└── run_backend.py            # Backend entry point
```

## External Dependencies
- **OpenAI**: Mapping validation (GPT-4o-mini), embeddings (text-embedding-3-small)
- **Pinecone**: Vector database for RAG
- **PostgreSQL**: Schema and mapping persistence
- **Redis**: Narration broadcast
- **httpx**: Async HTTP client for Farm API

## Related Repositories
- **AOS-NLQ**: Natural Language Query layer (NLQ + BLL functionality)
- **AOS-Farm**: Ground truth data and scenario management
- **AAM**: Asset & Availability Management (ingest pipeline)
