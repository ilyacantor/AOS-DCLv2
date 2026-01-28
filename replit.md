# DCL Engine - Data Connectivity Layer

**Last Updated:** January 28, 2026

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
| **L3** | Personas | Who uses this data | CFO, CRO, COO, CTO |

Users can:
- **Filter by persona** to see only data relevant to their role
- **Hover over connections** to see relationship details
- **Click nodes** to explore source details
- **Collapse/expand** the side panel for more graph space

### 2. Dual Operating Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| **Demo** | Pre-configured schemas from CSV files | Training, demonstrations, testing |
| **Farm** | Live schemas from AOS-Farm API | Production data discovery |

### 3. Run Modes (Dev vs Prod)

| Mode | Mapping Strategy | Speed | Accuracy |
|------|------------------|-------|----------|
| **Dev** | Heuristic pattern matching | Fast (~1s) | Good for common patterns |
| **Prod** | AI-powered semantic matching with RAG | Slower (~5s) | Higher accuracy for edge cases |

### 4. Persona-Based Filtering

Four executive personas with distinct data views:

| Persona | Focus Areas | Key Concepts |
|---------|-------------|--------------|
| **CFO** | Financial oversight | Revenue, Cost, Budget |
| **CRO** | Revenue operations | Opportunity, Account, Pipeline |
| **COO** | Operational efficiency | Usage Metrics, Health Score |
| **CTO** | Technical infrastructure | AWS Resources, System Health |

### 5. Real-Time Narration

A terminal-style panel shows processing activity in real-time:
- Schema loading progress
- Mapping operations
- Source normalization
- Pipeline orchestration steps

### 6. Collapsible Monitor Panel

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
- **Data Mode** - Switch between Demo and Farm
- **Run Mode** - Switch between Dev and Prod
- **Persona Toggles** - CFO, CRO, COO, CTO buttons
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

**January 28, 2026:**
- Fixed cache mutation bug causing duplicate MuleSoft nodes
- Made right panel collapsible with toggle button
- Timer now displays 1 decimal place
- Improved overall UI polish

**January 27, 2026:**
- Moved NLQ & BLL functionality to AOS-NLQ repository
- DCL refocused as metadata-only semantic layer

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
| `schema_loader.py` | backend/engine/ | Demo/Farm schema loading |
| `mapping_service.py` | backend/engine/ | Heuristic field mapping |
| `rag_service.py` | backend/engine/ | Pinecone RAG queries |

## API Endpoints

### Core Pipeline
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/dcl/run` | POST | Execute pipeline with params: data_mode, run_mode, personas |
| `/api/dcl/narration/{session_id}` | GET | Poll narration messages |
| `/api/dcl/batch-mapping` | POST | Run semantic mapping batch |

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

## File Structure

```
├── backend/
│   ├── api/main.py           # FastAPI app, endpoints
│   ├── domain/models.py      # Pydantic data models
│   ├── engine/
│   │   ├── dcl_engine.py     # Main orchestrator
│   │   ├── schema_loader.py  # CSV/Farm schema loading
│   │   ├── mapping_service.py # Heuristic mapping
│   │   ├── rag_service.py    # Pinecone RAG
│   │   └── source_normalizer.py # Source deduplication
│   ├── core/
│   │   ├── fabric_plane.py   # Fabric Plane types
│   │   └── pointer_buffer.py # Pointer buffering
│   └── llm/
│       └── mapping_validator.py # GPT-4o-mini validation
├── src/
│   ├── App.tsx               # Main React app
│   └── components/
│       ├── SankeyGraph.tsx   # D3 visualization
│       ├── MonitorPanel.tsx  # Metrics dashboard
│       └── NarrationPanel.tsx # Log viewer
├── data/schemas/             # Demo mode CSV files
└── run_backend.py            # Backend entry point
```

## Related Systems
- **AOS-NLQ**: Natural Language Query layer (queries data from DCL mappings)
- **AOS-Farm**: Ground truth data and scenario management
- **AAM**: Asset & Availability Management (data ingestion)

## Workflows
- **Backend API**: `python run_backend.py` - FastAPI server on port 8000
- **Frontend Dev Server**: `npm run dev` - Vite dev server on port 5000
- **Redis Server**: `redis-server --port 6379` - Pub/sub for narration
