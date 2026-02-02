# DCL Engine - Complete Functionality Review

**Last Updated:** February 02, 2026

## Executive Summary

DCL (Data Connectivity Layer) is the **semantic translation and visualization platform** for AutonomOS. It answers: *"Where does our data come from, what does it mean, and who uses it?"*

---

## 1. Core Architecture

| Component | Technology | Purpose |
|-----------|------------|---------|
| Frontend | React 18 + TypeScript + Vite | Interactive Sankey visualization |
| Backend | FastAPI + Python 3.11 | API endpoints, orchestration |
| Database | PostgreSQL | Mappings, stream sources |
| Pub/Sub | Redis | Real-time narration |

---

## 2. Visualization Layer (4-Layer Sankey Graph)

**Current Status: 23 nodes, 134 links**

| Layer | Name | Count | Examples |
|-------|------|-------|----------|
| **L0** | Pipeline | 1 | Demo Pipeline |
| **L1** | Sources | 10 | Salesforce CRM, SAP ERP, MongoDB, MuleSoft |
| **L2** | Ontology | 8 | Revenue, Cost, Account, Opportunity, Health Score |
| **L3** | Personas | 4 | CFO, CRO, COO, CTO |

**Features:**
- Persona-based filtering (CFO/CRO/COO/CTO buttons)
- Collapsible right panel (Monitor + Narration)
- Real-time processing narration
- RAG History tab (Prod mode)

---

## 3. Semantic Catalog (Core of DCL)

### 3.1 Published Metrics: 37 Total

| Pack | Count | Key Metrics |
|------|-------|-------------|
| **CFO** | 9 | ARR, MRR, Revenue, DSO, Burn Rate, AR Aging, Gross Margin |
| **CRO** | 8 | Pipeline, Win Rate, Quota Attainment, Churn Rate, NRR |
| **COO** | 3 | Throughput, Cycle Time, SLA Compliance |
| **CTO** | 5 | Deploy Frequency, MTTR, Uptime, SLO Attainment, Cloud Cost |
| **CHRO** | 12 | Headcount, Attrition, Time to Fill, Engagement, eNPS |

### 3.2 Metric Definition Structure

Each metric includes:
```
id, name, description, aliases[], pack,
allowed_dims[], allowed_grains[], measure_op,
default_grain, best_direction, rankable_dimensions[]
```

**Example:**
```
quota_attainment:
  - aliases: ["attainment", "quota achievement", "target attainment"]
  - allowed_dims: ["rep", "segment", "region"]
  - allowed_grains: [month, quarter]
  - best_direction: "high"
  - rankable_dimensions: ["rep", "region"]
```

### 3.3 Published Entities: 29 Dimensions

| Category | Examples |
|----------|----------|
| Business | customer, segment, region, product, rep |
| Pipeline | stage, cohort |
| Operations | team, department, project_type, work_type, priority |
| Technical | service, severity, resource_type, environment |
| HR | tenure_band, level, location |

### 3.4 Alias Resolution

The semantic layer resolves aliases to canonical IDs:
- "AR" → `ar`
- "monthly revenue" → `mrr`
- "attainment" → `quota_attainment`
- "sales rep" → `rep`

---

## 4. Query Endpoint (`POST /api/dcl/query`)

### 4.1 Request Model

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

### 4.2 Response Model

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

### 4.3 Ranking/Superlative Support

| Query Type | Parameters | Use Case |
|------------|------------|----------|
| Top N | `order_by: "desc", limit: 3` | "Top 3 reps by quota" |
| Bottom N | `order_by: "asc", limit: 3` | "3 worst performers" |
| Best | `order_by: "desc", limit: 1` | "Best service uptime" |
| Worst | `order_by: "asc", limit: 1` | "Worst DSO segment" |

---

## 5. Fact Base (Demo Mode Data)

**51 data arrays with 1,500+ records:**

| Category | Examples |
|----------|----------|
| Time Series | quarterly (12 periods: 2024-Q1 to 2026-Q4) |
| Financial | revenue_by_region, mrr_by_segment, ar_aging |
| Sales | quota_by_rep (36 reps), pipeline_by_stage, win_rate_by_rep |
| Operations | throughput_by_team, cycle_time_by_project_type |
| Technical | slo_attainment_by_service, mttr_by_severity |
| People | headcount_by_department, attrition_by_team |

**Differentiated Rep Data:**
- Top performer: Sarah Williams (115% attainment, 52% win rate)
- Bottom performer: Thomas Anderson (83% attainment, 32% win rate)

---

## 6. Operating Modes

### 6.1 Data Mode

| Mode | Source | Use Case |
|------|--------|----------|
| **Demo** | CSV files + fact_base.json | Training, demos |
| **Farm** | AOS-Farm API | Production discovery |

### 6.2 Run Mode

| Mode | Mapping Strategy | Speed |
|------|------------------|-------|
| **Dev** | Heuristic pattern matching | ~1s |
| **Prod** | AI (OpenAI + Pinecone RAG) | ~5s |

---

## 7. API Endpoints

### Active Endpoints

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/dcl/run` | POST | Execute pipeline, build graph |
| `/api/dcl/semantic-export` | GET | Full catalog for NLQ |
| `/api/dcl/semantic-export/resolve/metric` | GET | Alias → canonical metric |
| `/api/dcl/semantic-export/resolve/entity` | GET | Alias → canonical dimension |
| `/api/dcl/query` | POST | Execute data query |
| `/api/dcl/narration/{run_id}` | GET | Real-time processing log |
| `/api/dcl/batch-mapping` | POST | Bulk semantic mapping |
| `/api/topology` | GET | Unified topology graph |
| `/api/health` | GET | Service status |

### Deprecated (Moved to AOS-NLQ)

| Endpoint | Status |
|----------|--------|
| `/api/nlq/ask` | 410 MOVED |
| `/api/bll/*` | 410 MOVED |
| `/api/execute` | 410 MOVED |

---

## 8. Engine Orchestration Flow

```
1. Schema Loading
   ├── Demo: Load from data/schemas/*.csv
   ├── Farm: Fetch from FARM_API_URL
   └── Stream: Load from database (real-time sources)

2. Ontology Loading (8 concepts)

3. Mapping Generation
   ├── Load stored mappings from DB
   └── Generate new mappings for unmapped sources

4. Mapping Evaluation
   └── MappingEvaluator checks for issues

5. LLM Validation (Prod mode only)
   └── Low-confidence mappings → GPT-4o-mini validation

6. Graph Building
   └── Create nodes (L0-L3) and links
```

---

## 9. What's NOT in DCL

| Component | Location |
|-----------|----------|
| Natural Language Query (NLQ) | AOS-NLQ |
| Business Logic Layer (BLL) | AOS-NLQ |
| Data Ingestion Pipeline | AAM |
| Ground Truth / Scenarios | AOS-Farm |

---

## 10. Summary

**DCL is the semantic metadata layer that:**
1. Maps technical fields → business concepts
2. Visualizes data flow (Sankey graph)
3. Publishes semantic catalog for NLQ consumption
4. Executes validated queries against fact base
5. Supports ranking/superlative queries
6. Provides alias resolution
7. Validates queries against allowed dimensions/grains

**DCL does NOT:**
- Parse natural language (NLQ's job)
- Execute business logic (BLL's job)
- Ingest raw data (AAM's job)
