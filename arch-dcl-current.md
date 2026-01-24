# DCL Engine Architecture - Current State

**Last Updated:** January 24, 2026

## Core Function

**DCL answers one question: "What does this field mean to the business?"**

It maps raw technical fields (`acct_id`, `KUNNR`, `cust_rev_ytd`) to business concepts (`Account`, `Revenue`) and visualizes who uses what.

## System Overview

The DCL (Data Connectivity Layer) Engine is a **metadata-only semantic mapping engine** that:
1. Loads schema metadata (field names, types) from source systems
2. Maps fields to a common ontology using AI/heuristics
3. Routes mappings to persona-specific business logic
4. Visualizes the entire flow via interactive Sankey diagrams

**DCL does NOT:** Store raw data, process payloads, track lineage, or perform ETL.

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
│  │ Demo/    │     │ 11 Systems│    │ 8 Concepts│    │ 4 Personas│          │
│  │ Farm     │     │          │     │          │     │          │           │
│  └──────────┘     └──────────┘     └──────────┘     └──────────┘           │
│                                                                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                     ZERO-TRUST CORE LAYER (NEW)                             │
│                                                                              │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐         │
│  │ FABRIC POINTER  │    │   TOPOLOGY      │    │   DOWNSTREAM    │         │
│  │    BUFFER       │    │     API         │    │   CONTRACT      │         │
│  │                 │    │                 │    │                 │         │
│  │ Kafka offsets   │    │ Merges DCL      │    │ Abstract BLL    │         │
│  │ Snowflake cursors│   │ graph + AAM     │    │ consumer        │         │
│  │ BigQuery cursors │   │ health data     │    │ interface       │         │
│  │ EventBridge IDs │    │                 │    │                 │         │
│  └─────────────────┘    └─────────────────┘    └─────────────────┘         │
│                                                                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                     FABRIC PLANE MESH (AAM Managed)                         │
│                                                                              │
│  ┌───────────┐  ┌───────────┐  ┌───────────┐  ┌───────────┐                │
│  │  iPaaS    │  │ API_GW    │  │ EVENT_BUS │  │ DATA_WH   │                │
│  │           │  │           │  │           │  │           │                │
│  │ Workato   │  │ Kong      │  │ Kafka     │  │ Snowflake │                │
│  │ MuleSoft  │  │ Apigee    │  │ EventBridge│ │ BigQuery  │                │
│  └───────────┘  └───────────┘  └───────────┘  └───────────┘                │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## January 2026 Architecture Pivot

### Fabric Plane Mesh
AAM (Asset & Availability Management) connects to **4 Fabric Planes**, not individual SaaS apps:

| Plane | Examples | What It Provides |
|-------|----------|------------------|
| iPaaS | Workato, MuleSoft | Integration flow control |
| API_GATEWAY | Kong, Apigee | Managed API access |
| EVENT_BUS | Kafka, EventBridge | Streaming backbone |
| DATA_WAREHOUSE | Snowflake, BigQuery | Source of Truth |

### Pointer Buffering (Zero-Trust)
DCL buffers **ONLY Fabric Pointers** (offsets, cursors) — NEVER payloads:

```python
# What DCL stores:
KafkaPointer    = { topic, partition, offset }
SnowflakePointer = { table, stream_id, row_cursor }
BigQueryPointer  = { dataset, table, page_token }
EventBridgePointer = { event_bus, rule, event_id }

# What DCL does NOT store:
payload.body  # NEVER
record.data   # NEVER
```

**Just-in-Time Fetching:** Payload retrieved from Fabric Plane only when semantic mapper requests it.

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

### Backend Layers

| Layer | Module | Purpose |
|-------|--------|---------|
| **API** | `api/main.py` | REST endpoints, CORS, request handling |
| **Domain** | `domain/models.py` | Pydantic models for all entities |
| **Engine** | `engine/dcl_engine.py` | Main orchestrator, graph building |
| **Engine** | `engine/schema_loader.py` | Load schemas from CSV or Farm API |
| **Engine** | `engine/mapping_service.py` | Heuristic field mapping |
| **Engine** | `engine/rag_service.py` | Pinecone lesson storage |
| **Core** | `core/fabric_plane.py` | Fabric Plane types and pointers |
| **Core** | `core/pointer_buffer.py` | FabricPointerBuffer with JIT fetch |
| **Core** | `core/downstream_contract.py` | Abstract BLL consumer interface |
| **Core** | `core/topology_api.py` | Merged DCL + AAM visualization |
| **LLM** | `llm/mapping_validator.py` | GPT-4o-mini validation |
| **Ingest** | `ingest/*` | **DEPRECATED** - migrating to AAM |

### Core Layer Components (backend/core/)

| Component | Status | Purpose |
|-----------|--------|---------|
| `FabricPointerBuffer` | Functional | Pointer-only buffering with JIT fetch capability |
| `FabricPlane` types | Functional | KafkaPointer, SnowflakePointer, BigQueryPointer, EventBridgePointer |
| `DownstreamConsumerContract` | Interface | Abstract base for BLL consumers |
| `TopologyAPI` | Functional | Merges DCL semantic graph with AAM health data |
| `SecurityConstraints` | Defined | Build/runtime guards preventing payload.body writes |

## Inference / RAG Capabilities

RAG in DCL is used **only for mapping enhancement** — NOT for querying data.

### 1. Lesson Storage (RAGService)
Stores high-confidence mappings as embeddings in Pinecone:
```
Input: { field: "customer_name", concept: "account", confidence: 0.92 }
Output: Vector embedding in Pinecone index "dcl-mapping-lessons"
```
**Model:** OpenAI `text-embedding-3-small` (1536 dimensions)

### 2. LLM Validation (MappingValidator)
Validates low-confidence mappings using GPT-4o-mini:

| Step | Action |
|------|--------|
| 1 | Filter mappings with confidence < 0.80 |
| 2 | Sort by lowest confidence first |
| 3 | Send top 10 to LLM for validation |
| 4 | LLM returns corrections with reasoning |
| 5 | Update mappings with validated concepts |

**Example Correction:**
```
Field: GL_ACCOUNT (in INVOICES table)
Original: "account" (Customer Account)
Corrected: "gl_account" (General Ledger)
Reason: "GL_ACCOUNT in financial context = General Ledger"
```

### LLM Usage Summary

| Service | Model | Purpose |
|---------|-------|---------|
| RAGService | `text-embedding-3-small` | Generate embeddings for lessons |
| MappingValidator | `gpt-4o-mini` | Validate ambiguous mappings |
| (Planned) | Gemini 2.5 Flash | Schema understanding |

## Data Flow

```
1. DEMO MODE:
   CSV Files → SchemaLoader → MappingService → GraphBuilder → Frontend

2. FARM MODE (New Architecture):
   Fabric Planes → AAM → Pointer Buffer → JIT Fetch → Semantic Mapper
                                              ↓
                                    Validate (LLM if low confidence)
                                              ↓
                                    Build Graph → Frontend
```

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

### Topology Endpoints (NEW)
```
GET  /api/topology
     Returns: Unified topology graph (DCL semantic + AAM health)

GET  /api/topology/health
     Returns: Connection health data from mesh

GET  /api/topology/stats
     Returns: Topology service statistics
```

### Ingest Endpoints (DEPRECATED)
```
POST /api/ingest/provision    # Migrating to AAM
GET  /api/ingest/telemetry    # Migrating to AAM
```

## Node Inventory

**Total: 24 nodes**

| Layer | Count | Nodes |
|-------|-------|-------|
| L0 | 1 | Pipeline entry (demo/farm) |
| L1 | 11 | Salesforce, HubSpot, Dynamics, SAP, NetSuite, MongoDB, Supabase, DW, Legacy SQL, MuleSoft Mock, MuleSoft Stream |
| L2 | 8 | Account, Opportunity, Revenue, Cost, Date, Health, Usage, AWS Resource |
| L3 | 4 | CFO, CRO, COO, CTO |

## Redis Keys

| Key | Type | Purpose |
|-----|------|---------|
| `dcl.logs` | Pub/Sub | Narration message broadcast |
| `dcl.ingest.raw` | Stream | **DEPRECATED** |
| `dcl.ingest.config` | Hash | **DEPRECATED** |
| `dcl.telemetry` | String | Live metrics JSON |

## Technology Stack

| Layer | Technology |
|-------|------------|
| Frontend | React 18, TypeScript, Vite, D3.js, Tailwind CSS |
| Backend | FastAPI, Python 3.11, Pydantic V2, Uvicorn |
| Database | PostgreSQL (persistence), Redis (streaming) |
| Vector DB | Pinecone (mapping lessons) |
| AI/LLM | OpenAI GPT-4o-mini, text-embedding-3-small |
| HTTP | httpx (async) |

## Environment Variables

| Variable | Purpose |
|----------|---------|
| `DATABASE_URL` | PostgreSQL connection |
| `REDIS_URL` | Redis connection |
| `OPENAI_API_KEY` | Embeddings + LLM validation |
| `PINECONE_API_KEY` | Vector storage |
| `GEMINI_API_KEY` | (Reserved) Schema understanding |
| `FARM_API_URL` | AOS-Farm API base URL |
| `RUN_MODE` | `dev` (heuristic) or `prod` (LLM/RAG) |

## What DCL Does NOT Do

| Misconception | Reality |
|---------------|---------|
| Stores raw data | Only stores pointers (offsets, cursors) |
| ETL pipeline | No data transformation |
| Data lineage | No provenance tracking |
| Data governance | No compliance features |
| Query interface | No natural language querying |
| Semantic search over data | Only maps field→concept |
