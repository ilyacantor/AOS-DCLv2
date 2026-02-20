# DCL Architecture & Functionality

**Last Updated:** February 7, 2026
**Version:** 2.0.0 (Metadata-Only Mode)

## Overview

DCL (Data Connectivity Layer) is the **semantic translation and visualization platform** for AutonomOS. It answers: *"Where does our data come from, what does it mean, and who uses it?"*

DCL is a **metadata-only** semantic layer. It does NOT parse natural language, execute business logic, or ingest raw data. Those responsibilities belong to other AutonomOS services.

| Responsibility | Owner |
|---------------|-------|
| Semantic mapping & visualization | **DCL** (this service) |
| Natural Language Query (NLQ) | AOS-NLQ |
| Business Logic Layer (BLL) | AOS-NLQ |
| Data ingestion & discovery | AAM |
| Ground truth / scenarios | AOS-Farm |

---

## 1. Core Architecture

| Component | Technology | Purpose |
|-----------|------------|---------|
| Frontend | React 18 + TypeScript + Vite | Interactive Sankey visualization |
| Backend | FastAPI + Python 3.11 + Uvicorn | API endpoints, orchestration |
| Database | PostgreSQL (Neon) | Mappings, ontology, personas |
| Pub/Sub | Redis | Real-time narration streaming |

---

## 2. Visualization Layer (4-Layer Sankey Graph)

### Demo Mode (typical: ~23 nodes, ~134 links)

| Layer | Name | Count | Examples |
|-------|------|-------|----------|
| **L0** | Pipeline | 1 | Demo Pipeline / Farm Pipeline / AAM Pipeline |
| **L1** | Sources | 10-11 | Salesforce CRM, SAP ERP, MongoDB, MuleSoft |
| **L2** | Ontology | 8 | Revenue, Cost, Account, Opportunity, Health Score |
| **L3** | Personas | 5 | CFO, CRO, COO, CTO, CHRO |

### AAM Mode: Variable (fabric plane grouping)

In AAM mode, L1 sources are grouped into **fabric plane nodes** (iPaaS, Gateway, EventBus, Data Warehouse) when the source count is high, keeping the graph readable.

### Features

- Persona-based filtering (CFO/CRO/COO/CTO/CHRO buttons)
- Collapsible right panel (Monitor + Narration tabs)
- Real-time processing narration (terminal-style)
- RAG History tab (Prod mode only)
- Hover tooltips on nodes and links
- Color-coded nodes by layer (teal-to-purple gradient)

---

## 3. Data Modes

| Mode | Source | Use Case |
|------|--------|----------|
| **Demo** | CSV files in `data/schemas/` + `data/fact_base.json` | Training, demos, development |
| **Farm** | AOS-Farm API (live) | Production data discovery |
| **AAM** | AAM Fabric Plane export (live) | Connection discovery across iPaaS, Gateway, EventBus, DW |

### AAM Mode Details

AAM groups discovered connections by fabric plane and exports them for DCL consumption. DCL fetches this export and converts connections to SourceSystem objects without expensive normalization (AAM data is already canonical).

- Fetches from `AAM_URL/api/pipes`
- Supports optional `aod_run_id` parameter to filter by specific AOD run
- Processes 700+ connections in ~2 seconds
- Groups sources into fabric plane nodes for graph display

---

## 4. Run Modes

| Mode | Mapping Strategy | Speed | Accuracy |
|------|------------------|-------|----------|
| **Dev** | Heuristic pattern matching | ~1s | Good for common patterns |
| **Prod** | AI-powered (OpenAI + Pinecone RAG + LLM validation) | ~5s | Higher accuracy for edge cases |

---

## 5. Semantic Catalog

DCL publishes a semantic catalog consumed by AOS-NLQ for natural language queries.

### 5.1 Metrics: 37 Total

| Pack | Count | Key Metrics |
|------|-------|-------------|
| **CFO** | 9 | ARR, MRR, Revenue, DSO, Burn Rate, AR Aging, Gross Margin, Services Revenue, AR |
| **CRO** | 8 | Pipeline, Win Rate, Quota Attainment, Churn Rate, NRR, Pipeline Value, Churn Risk, NRR by Cohort |
| **COO** | 3 | Throughput, Cycle Time, SLA Compliance |
| **CTO** | 5 | Deploy Frequency, MTTR, Uptime, SLO Attainment, Cloud Cost |
| **CHRO** | 12 | Headcount, Attrition, Time to Fill, Engagement, eNPS, Compensation Ratio, Training Hours, DEI Index, Absenteeism, Offer Acceptance Rate, Internal Mobility, Span of Control |

### 5.2 Entities: 29 Dimensions

| Category | Examples |
|----------|----------|
| Business | customer, segment, region, product, rep |
| Pipeline | stage, cohort |
| Operations | team, project, project_type, work_type, priority |
| Technical | service, severity, resource_type, environment, sla_type, slo_type |
| HR | department, level, tenure_band, role, training_type, location |

### 5.3 Bindings: 13

| Source System | Canonical Event | Quality | Freshness |
|---------------|-----------------|---------|-----------|
| Salesforce CRM | deal_won | 0.95 | 0.98 |
| NetSuite ERP | revenue_recognized | 0.92 | 0.95 |
| NetSuite ERP | invoice_posted | 0.90 | 0.95 |
| Chargebee | subscription_started | 0.88 | 0.92 |
| Jira | work_item_completed | 0.85 | 0.90 |
| GitHub Actions | deployment_completed | 0.90 | 0.98 |
| PagerDuty | incident_resolved | 0.88 | 0.95 |
| AWS Cost Explorer | cloud_cost_incurred | 0.92 | 0.85 |
| Workday | employee_hired | 0.92 | 0.95 |
| Workday | employee_terminated | 0.90 | 0.95 |
| Greenhouse | requisition_opened | 0.88 | 0.92 |
| Greenhouse | requisition_filled | 0.88 | 0.92 |
| Culture Amp | survey_completed | 0.85 | 0.90 |

### 5.4 Alias Resolution

The semantic layer resolves aliases to canonical IDs:
- "AR" -> `ar`
- "monthly revenue" -> `mrr`
- "attainment" -> `quota_attainment`
- "sales rep" -> `rep`
- "eNPS" -> `enps`

---

## 6. Semantics Engine

### What is "Semantics" in DCL?

Semantics refers to the **business meaning** behind technical data. DCL translates cryptic field names (e.g., `cust_rev_ytd_amt`) into understandable business concepts (e.g., `Revenue`).

### Mapping Flow

```
1. Schema Loading
   - Demo: CSV files from data/schemas/
   - Farm: Fetch from FARM_API_URL
   - AAM: Fetch from AAM_URL/api/pipes

2. Ontology Loading
   - Load from database (ontology_concepts table)
   - Fallback to default concepts (ontology.py)

3. Mapping Generation
   - Load existing mappings from PostgreSQL
   - Generate new mappings for unmapped sources
   - HeuristicMapper: pattern rules, field name analysis, semantic hints

4. LLM Validation (Prod mode only)
   - Low-confidence mappings -> GPT-4o-mini validation
   - Corrects mismatches (e.g., GL_ACCOUNT vs customer account)

5. RAG Learning
   - High-confidence mappings stored as lessons in Pinecone
   - Future runs benefit from learned mappings

6. Persistence
   - All mappings saved to PostgreSQL
   - Reused across runs (not regenerated each time)
```

### Mapping Confidence

| Score | Source | Example |
|-------|--------|---------|
| 95% | Exact concept match in field name | `revenue_amount` -> Revenue |
| 85% | Pattern rule match | `rev_ytd` -> Revenue |
| 75% | Semantic similarity (RAG) | `annual_recurring` -> Revenue |
| <70% | Low confidence, needs validation | `misc_amt` -> ? |

---

## 7. Query Endpoint

### POST /api/dcl/query

Executes data queries against the fact base for NLQ consumption.

**Request:**
```json
{
  "metric": "quota_attainment",
  "dimensions": ["rep"],
  "filters": {"region": "AMER"},
  "time_range": {"start": "2026-Q1", "end": "2026-Q4"},
  "grain": "quarter",
  "order_by": "desc",
  "limit": 3
}
```

**Response:**
```json
{
  "metric": "quota_attainment",
  "metric_name": "Quota Attainment",
  "dimensions": ["rep"],
  "grain": "quarter",
  "unit": "percent",
  "data": [
    {"period": "2026-Q4", "value": 115.0, "dimensions": {"rep": "Sarah Williams"}, "rank": 1},
    {"period": "2026-Q4", "value": 114.1, "dimensions": {"rep": "Michael Brown"}, "rank": 2}
  ],
  "metadata": {
    "sources": ["demo"],
    "freshness": "2026-02-02T...",
    "quality_score": 1.0,
    "record_count": 2,
    "total_count": 36,
    "ranking_type": "top_n",
    "order": "desc"
  }
}
```

### Ranking Support

| Query Type | Parameters | Use Case |
|------------|------------|----------|
| Top N | `order_by: "desc", limit: 3` | "Top 3 reps by quota" |
| Bottom N | `order_by: "asc", limit: 3` | "3 worst performers" |
| Best | `order_by: "desc", limit: 1` | "Best service uptime" |
| Worst | `order_by: "asc", limit: 1` | "Worst DSO segment" |

---

## 8. API Endpoints

### Active Endpoints

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/dcl/run` | POST | Execute pipeline, build graph snapshot |
| `/api/dcl/semantic-export` | GET | Full semantic catalog for NLQ |
| `/api/dcl/semantic-export/resolve/metric` | GET | Alias -> canonical metric |
| `/api/dcl/semantic-export/resolve/entity` | GET | Alias -> canonical dimension |
| `/api/dcl/query` | POST | Execute data query against fact base |
| `/api/dcl/narration/{run_id}` | GET | Real-time processing log |
| `/api/dcl/batch-mapping` | POST | Bulk semantic mapping |
| `/api/topology` | GET | Unified topology graph |
| `/api/topology/health` | GET | Connection health data |
| `/api/health` | GET | Service status |

### Deprecated (Moved to AOS-NLQ)

| Endpoint | Status |
|----------|--------|
| `/api/nlq/ask` | 410 MOVED |
| `/api/bll/*` | 410 MOVED |
| `/api/execute` | 410 MOVED |

---

## 9. Fact Base (Demo Mode Data)

**51 data arrays with 1,500+ records:**

| Category | Examples |
|----------|----------|
| Time Series | quarterly (12 periods: 2024-Q1 to 2026-Q4) |
| Financial | revenue_by_region, mrr_by_segment, ar_aging |
| Sales | quota_by_rep (36 reps), pipeline_by_stage, win_rate_by_rep |
| Operations | throughput_by_team, cycle_time_by_project_type |
| Technical | slo_attainment_by_service, mttr_by_severity |
| People | headcount_by_department, attrition_by_team |

**Differentiated Rep Data (36 reps):**
- Top performer: Sarah Williams (115% attainment, 52% win rate)
- Bottom performer: Thomas Anderson (83% attainment, 32% win rate)

---

## 10. File Structure

```
backend/
├── api/
│   ├── main.py                # FastAPI app with all routes
│   ├── query.py               # Query endpoint logic
│   └── semantic_export.py     # Semantic catalog export
│
├── aam/                       # AAM Integration
│   └── client.py              # AAM Fabric Plane client
│
├── engine/                    # DCL Core Layer
│   ├── dcl_engine.py          # Main orchestrator
│   ├── schema_loader.py       # Demo/Farm/AAM schema loading
│   ├── mapping_service.py     # Field -> concept mappings
│   ├── narration_service.py   # Real-time processing logs
│   ├── ontology.py            # Core ontology concepts
│   ├── persona_view.py        # Persona-based filtering
│   ├── rag_service.py         # Pinecone RAG for lessons
│   └── source_normalizer.py   # Source deduplication
│
├── semantic_mapper/           # Batch Mapping (Cold Path)
│   ├── heuristic_mapper.py    # Pattern-based matching
│   └── persist_mappings.py    # PostgreSQL persistence
│
├── llm/                       # AI Validation
│   └── mapping_validator.py   # GPT-4o-mini validation
│
├── farm/                      # Farm Integration
│   └── client.py              # AOS-Farm API client
│
├── core/                      # Infrastructure
│   ├── fabric_plane.py        # Fabric Plane types
│   ├── pointer_buffer.py      # Pointer buffering
│   └── topology_api.py        # Topology service
│
└── domain/
    └── models.py              # Core Pydantic models

src/                           # Frontend
├── App.tsx                    # Main React app
└── components/
    ├── SankeyGraph.tsx        # D3 Sankey visualization
    ├── MonitorPanel.tsx       # Persona metrics dashboard
    └── NarrationPanel.tsx     # Terminal-style log viewer

data/
├── schemas/                   # Demo mode CSV files
└── fact_base.json             # Demo mode query data
```

---

## 11. Database Schema

### Core Tables

```sql
CREATE TABLE ontology_concepts (
    id VARCHAR(128) PRIMARY KEY,
    name VARCHAR(256),
    cluster VARCHAR(64),
    synonyms JSONB DEFAULT '[]',
    example_fields JSONB DEFAULT '[]'
);

CREATE TABLE field_concept_mappings (
    id SERIAL PRIMARY KEY,
    source_id VARCHAR(128),
    table_name VARCHAR(256),
    field_name VARCHAR(256),
    concept_id VARCHAR(128) REFERENCES ontology_concepts(id),
    confidence FLOAT,
    mapped_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE persona_profiles (
    id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(256),
    description TEXT
);

CREATE TABLE persona_concept_relevance (
    persona_id VARCHAR(64) REFERENCES persona_profiles(id),
    concept_id VARCHAR(128) REFERENCES ontology_concepts(id),
    relevance FLOAT CHECK (relevance BETWEEN 0 AND 1),
    PRIMARY KEY (persona_id, concept_id)
);
```

---

## 12. Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `DATABASE_URL` | Yes | PostgreSQL connection string |
| `REDIS_URL` | Yes | Redis connection string |
| `AAM_URL` | AAM mode | AAM API base URL |
| `FARM_API_URL` | Farm mode | AOS-Farm API base URL |
| `OPENAI_API_KEY` | Prod mode | OpenAI API for embeddings + validation |
| `PINECONE_API_KEY` | Prod mode | Pinecone vector database |

---

## 13. Key Design Decisions

1. **Metadata-Only Mode** - DCL is a semantic layer, not an execution engine. NLQ/BLL live in AOS-NLQ.
2. **No LLM in Hot Path** - All scoring uses deterministic rules + stored metadata. LLM only used in batch mapping (cold path, Prod mode).
3. **Graph Not Persisted** - Graph snapshots are generated fresh each run. Semantic mappings persist in PostgreSQL.
4. **AAM Fast-Path** - AAM connections skip expensive normalization since they arrive pre-canonical.
5. **Fabric Plane Grouping** - When source count is high (AAM mode), sources are grouped into fabric plane nodes for readable visualization.
6. **Zero-Trust Security** - DCL enforces metadata-only constraints; no payload data passes through.

---

## 14. Related Systems

| System | Purpose |
|--------|---------|
| **AOS-NLQ** | Natural Language Query + Business Logic Layer |
| **AOS-Farm** | Ground truth data and scenario management |
| **AAM** | Asset & Availability Management (data ingestion, fabric plane discovery) |
