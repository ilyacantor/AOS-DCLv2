# DCL Engine - Data Connectivity Layer

**Last Updated:** February 7, 2026

## What is DCL?

The Data Connectivity Layer (DCL) is a **semantic mapping and visualization platform** that helps organizations understand their data landscape. It answers the critical business question: **"Where does our data come from, what does it mean, and who uses it?"**

### The Problem DCL Solves

Modern enterprises have data scattered across dozens of systems—CRMs, ERPs, databases, data warehouses, and integration platforms. Business users struggle to understand:
- Which systems contain the data they need
- How technical field names translate to business concepts
- Which data flows are relevant to their role

DCL provides a visual map that connects these dots, making data discovery intuitive and role-specific.

## Functional Capabilities

### 1. Interactive Data Flow Visualization (Sankey Graph)

The centerpiece of DCL is a **4-layer Sankey diagram** showing data flow:

| Layer | Name | Purpose | Example |
|-------|------|---------|---------|
| **L0** | Pipeline | Entry point showing data mode | Demo Pipeline, Farm Pipeline |
| **L1** | Sources | Connected data systems | Salesforce CRM, SAP ERP, MongoDB |
| **L2** | Ontology | Business concepts | Revenue, Cost, Account, Opportunity |
| **L3** | Personas | Who uses this data | CFO, CRO, COO, CTO, CHRO |

Users can:
- **Filter by persona** to see only data relevant to their role
- **Hover over connections** to see relationship details
- **Click nodes** to explore source details
- **Collapse/expand** the side panel for more graph space

### 2. Three Data Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| **Demo** | Pre-configured schemas from CSV files | Training, demonstrations, testing |
| **Farm** | Live schemas from AOS-Farm API | Production data discovery |
| **AAM** | Live connections from AAM fabric planes | Connection discovery across iPaaS, Gateway, EventBus, DW |

### 3. Run Modes (Dev vs Prod)

| Mode | Mapping Strategy | Speed | Accuracy |
|------|------------------|-------|----------|
| **Dev** | Heuristic pattern matching | Fast (~1s) | Good for common patterns |
| **Prod** | AI-powered semantic matching with RAG | Slower (~5s) | Higher accuracy for edge cases |

### 4. AI-Powered Semantic Mapping (LLM + RAG)

DCL uses AI to intelligently map technical field names to business concepts:

**How It Works:**
1. **Field Extraction** - Technical fields are extracted from source schemas (e.g., `acct_id`, `cust_revenue_ytd`)
2. **Embedding Generation** - OpenAI's text-embedding-3-small converts fields to vector representations
3. **RAG Query** - Pinecone vector database finds semantically similar concepts from the ontology
4. **LLM Validation** - GPT-4o-mini validates and scores mapping confidence

**Example Mapping Flow:**
```
Source Field: "cust_revenue_ytd"
    ↓ Embedding
Vector: [0.12, -0.34, 0.56, ...]
    ↓ RAG Query (Pinecone)
Top Match: "Revenue" (similarity: 0.92)
    ↓ LLM Validation
Result: Revenue concept, 95% confidence
```

**RAG History Panel:**
- View semantic matches in the Monitor panel's "RAG History" tab
- Shows query field, matched concept, and confidence score
- Tracks total RAG reads during pipeline execution

**When AI is Used:**
- **Prod Mode Only** - Dev mode uses fast heuristics instead
- **Ambiguous Fields** - Fields that don't match simple patterns
- **New Sources** - First-time discovery of unknown schemas

### 5. Persona-Based Filtering

Filter the visualization by executive role to see only relevant data flows:

| Persona | Focus Areas | Key Concepts |
|---------|-------------|--------------|
| **CFO** | Financial oversight | Revenue, Cost, Budget |
| **CRO** | Revenue operations | Opportunity, Account, Pipeline |
| **CHRO** | People operations | Headcount, Attrition, Engagement |
| **COO** | Operational efficiency | Usage Metrics, Health Score |
| **CTO** | Technical infrastructure | AWS Resources, System Health |

### 6. Real-Time Narration

A terminal-style panel shows processing activity in real-time:
- Schema loading progress
- Mapping operations
- Source normalization
- Pipeline orchestration steps

### 7. Collapsible Monitor Panel

The right sidebar contains:
- **Persona Views**: Role-specific metrics and KPIs
- **RAG History**: Semantic matching results (Prod mode)
- Toggle collapse/expand for more visualization space

## Key Data Sources (L1 Layer)

### CRM Systems
- **Salesforce CRM** - Customer accounts, opportunities, contacts
- **HubSpot CRM** - Marketing and sales pipeline
- **Microsoft Dynamics CRM** - Enterprise CRM data

### ERP Systems
- **SAP ERP** - Financial and operational data
- **NetSuite ERP** - Cloud ERP suite

### Databases
- **MongoDB Customer DB** - Document-based customer data
- **Supabase App DB** - Application database
- **Legacy SQL** - Discovered legacy systems

### Data Warehouse
- **DW Dim Customer** - Dimensional customer data

### Integration Platforms
- **MuleSoft ERP Sync** - Real-time streaming data from MuleSoft

## Business Concepts (L2 Ontology)

| Concept | Description | Common Fields |
|---------|-------------|---------------|
| **Account** | Customer/company entity | account_id, company_name, customer |
| **Opportunity** | Sales pipeline deals | opportunity_id, deal_stage, amount |
| **Revenue** | Income metrics | revenue, sales, mrr, arr |
| **Cost** | Expense tracking | cost, expense, spend |
| **Date/Timestamp** | Time dimensions | created_at, updated_at, date |
| **Health Score** | Customer health metrics | health_score, nps, satisfaction |
| **Usage Metrics** | Product usage data | usage, active_users, sessions |
| **AWS Resource** | Cloud infrastructure | instance_id, resource_arn |

## User Interface Guide

### Top Navigation Bar
- **DCL Logo** - Application identifier
- **Graph / Dashboard** - Toggle between views
- **Data Mode** - Switch between Demo, Farm, and AAM
- **Run Mode** - Switch between Dev and Prod
- **Persona Toggles** - CFO, CRO, COO, CTO, CHRO buttons
- **Run Button** - Execute the pipeline

### Main Graph Area
- Sankey diagram fills the main viewport
- Nodes are color-coded by layer (teal→blue→purple gradient)
- Links show data flow strength through opacity

### Right Panel (Collapsible)
- **Monitor Tab** - Persona metrics and views
- **Narration Tab** - Real-time processing log
- **Collapse Button** - Toggle panel visibility

### Status Display
- Elapsed time counter (during run)
- Processing time (after completion)
- RAG query count (Prod mode)

## Recent Changes

**February 13, 2026:**
- Externalized semantic catalog: metrics, entities, bindings, persona concepts moved from Python code to YAML config files (backend/config/definitions/)
- semantic_export.py refactored from 958 to ~283 lines — now loads from YAML at startup
- Replaced hardcoded FabricProvider enum with dynamic string-based provider registry
- Removed PLANE_TO_PROVIDERS map (caused "5 fabrics instead of 4" bug)
- Pointer classes now self-register via @register_pointer_class decorator
- Unknown providers gracefully fall back to base FabricPointer
- Default startup mode changed from AAM to Demo

**February 7, 2026:**
- Updated all docs: merged ARCH + Functionality into DCL_ARCHITECTURE.md
- Refreshed DCL_SEMANTIC_CATALOG.md from live API (37 metrics, 29 entities, 13 bindings)
- Fixed AAM mode performance: skip expensive normalizer for AAM connections (2s vs timeout)
- Added "fabric" node kind for AAM fabric plane grouping
- Fixed RunMetrics missing `total_mappings` field

**February 2, 2026:**
- Consolidated rep performance data, extended to all 36 reps
- Major NLQ data expansion: 51 data arrays, 1500+ records in fact_base.json
- Added CHRO as first-class persona with 12 metrics
- Semantic catalog: 37 metrics, 29 entities, 13 bindings
- Added query endpoint (POST /api/dcl/query) for NLQ consumption

**January 28, 2026:**
- Fixed cache mutation bug, collapsible right panel, UI polish

**January 27, 2026:**
- Moved NLQ & BLL to AOS-NLQ; DCL refocused as metadata-only semantic layer

## User Preferences
- Communication style: Simple, everyday language
- No mock data - use real integrations
- PST timezone (12-hour in controls, 24-hour in terminal)

---

# Technical Reference

## System Architecture

### Frontend Stack
- **React 18** + TypeScript + Vite
- **D3.js** (d3-sankey) for Sankey diagrams
- **Tailwind CSS** with glassmorphism effects
- **Lucide React** for icons

### Backend Stack
- **FastAPI** + Python 3.11 + Pydantic V2
- **Uvicorn** on port 8000
- **PostgreSQL** for persistence
- **Redis** for real-time pub/sub

### Key Components

| Component | Location | Purpose |
|-----------|----------|---------|
| `App.tsx` | src/ | Main React app, state management |
| `SankeyGraph.tsx` | src/components/ | D3 Sankey visualization |
| `MonitorPanel.tsx` | src/components/ | Persona metrics display |
| `NarrationPanel.tsx` | src/components/ | Real-time log viewer |
| `dcl_engine.py` | backend/engine/ | Main orchestrator |
| `schema_loader.py` | backend/engine/ | Demo/Farm/AAM schema loading |
| `mapping_service.py` | backend/engine/ | Heuristic field mapping |
| `rag_service.py` | backend/engine/ | Pinecone RAG queries |

## API Endpoints

### Core Pipeline
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/dcl/run` | POST | Execute pipeline with params: data_mode, run_mode, personas |
| `/api/dcl/narration/{session_id}` | GET | Poll narration messages |
| `/api/dcl/batch-mapping` | POST | Run semantic mapping batch |

### Semantic Export (for NLQ)
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/dcl/semantic-export` | GET | Full semantic catalog (metrics, entities, bindings, mode) |
| `/api/dcl/semantic-export/resolve/metric` | GET | Resolve metric alias to canonical definition |
| `/api/dcl/semantic-export/resolve/entity` | GET | Resolve entity alias to canonical definition |
| `/api/dcl/query` | POST | Execute data query against fact base |

### Query Endpoint Usage
```json
POST /api/dcl/query
{
  "metric": "arr",
  "dimensions": ["segment"],
  "filters": {"region": "AMER"},
  "time_range": {"start": "2025-Q1", "end": "2025-Q4"},
  "grain": "quarter"
}
```
Returns data points with metadata (sources, freshness, quality_score).

### Topology
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/topology` | GET | Unified topology graph |
| `/api/topology/health` | GET | Connection health data |

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `DATABASE_URL` | Yes | PostgreSQL connection string |
| `REDIS_URL` | Yes | Redis connection string |
| `OPENAI_API_KEY` | Prod mode | OpenAI API for embeddings |
| `PINECONE_API_KEY` | Prod mode | Pinecone vector database |
| `FARM_API_URL` | Farm mode | AOS-Farm API base URL |
| `AAM_URL` | AAM mode | AAM API base URL |

## File Structure

```
├── backend/
│   ├── api/
│   │   ├── main.py              # FastAPI app, endpoints
│   │   ├── query.py             # Query endpoint logic
│   │   └── semantic_export.py   # Semantic catalog loader (from YAML)
│   ├── config/
│   │   └── definitions/         # Externalized semantic catalog
│   │       ├── metrics.yaml     # 38 metric definitions
│   │       ├── entities.yaml    # 29 entity definitions
│   │       ├── bindings.yaml    # 13 source system bindings
│   │       └── persona_concepts.yaml  # Persona-to-metric mappings
│   ├── aam/
│   │   └── client.py            # AAM Fabric Plane client
│   ├── domain/models.py         # Pydantic data models
│   ├── engine/
│   │   ├── dcl_engine.py        # Main orchestrator
│   │   ├── schema_loader.py     # Demo/Farm/AAM schema loading
│   │   ├── mapping_service.py   # Heuristic mapping
│   │   ├── rag_service.py       # Pinecone RAG
│   │   ├── ontology.py          # Core ontology concepts
│   │   ├── persona_view.py      # Persona filtering
│   │   └── source_normalizer.py # Source deduplication
│   ├── semantic_mapper/
│   │   ├── heuristic_mapper.py  # Pattern matching
│   │   └── persist_mappings.py  # DB persistence
│   ├── core/
│   │   ├── fabric_plane.py      # Fabric Plane types
│   │   └── pointer_buffer.py    # Pointer buffering
│   ├── farm/
│   │   └── client.py            # AOS-Farm API client
│   └── llm/
│       └── mapping_validator.py # GPT-4o-mini validation
├── src/
│   ├── App.tsx                  # Main React app
│   └── components/
│       ├── SankeyGraph.tsx      # D3 visualization
│       ├── MonitorPanel.tsx     # Metrics dashboard
│       └── NarrationPanel.tsx   # Log viewer
├── data/
│   ├── schemas/                 # Demo mode CSV files
│   └── fact_base.json           # Demo mode query data
├── docs/
│   ├── DCL_ARCHITECTURE.md      # Architecture & functionality
│   └── DCL_SEMANTIC_CATALOG.md  # Semantic catalog reference
└── run_backend.py               # Backend entry point
```

## Related Systems
- **AOS-NLQ**: Natural Language Query layer (queries data from DCL mappings)
- **AOS-Farm**: Ground truth data and scenario management
- **AAM**: Asset & Availability Management (data ingestion)

## Workflows
- **Backend API**: `python run_backend.py` - FastAPI server on port 8000
- **Frontend Dev Server**: `npm run dev` - Vite dev server on port 5000
- **Redis Server**: `redis-server --port 6379` - Pub/sub for narration
