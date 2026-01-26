# DCL Architecture - Current State

**Last Updated:** January 26, 2026  
**Version:** 3.0 (BLL/NLQ/DCL Merge + Farm Integration)

## Overview

The DCL (Data Connectivity Layer) Engine is a **unified semantic mapping and query engine** that answers business questions through deterministic definition matching and execution. The January 2026 merge combined:

- **DCL Core** - 4-layer Sankey visualization (L0→L1→L2→L3)
- **BLL** - Business Logic Layer with stable consumption contracts
- **NLQ** - Natural Language Query compiler

**Core Principle:** NLQ is a compiler (extracts intent + parameters), BLL is the executor (applies ordering, computes summaries).

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     DCL Engine v3.0 (Post-Merge)                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  "Top 5 customers by revenue"                                                │
│          │                                                                   │
│          ▼                                                                   │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                        NLQ Compiler                                     │ │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                  │ │
│  │  │   Operator   │  │    Intent    │  │  Parameter   │                  │ │
│  │  │   Extractor  │→ │   Matcher    │→ │  Extractor   │                  │ │
│  │  └──────────────┘  └──────────────┘  └──────────────┘                  │ │
│  │        ↓                  ↓                  ↓                          │ │
│  │   temporal: null    crm.top_customers   limit: 5                        │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                              │                                               │
│                              ▼                                               │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                        BLL Executor                                     │ │
│  │  1. Load definition (crm.top_customers)                                 │ │
│  │  2. Load data (Demo CSV or Farm ground truth)                           │ │
│  │  3. Apply default_order_by: [AnnualRevenue DESC]                        │ │
│  │  4. Apply tie_breaker: Id                                               │ │
│  │  5. Apply limit: 5                                                      │ │
│  │  6. Compute summary with share-of-total                                 │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                              │                                               │
│                              ▼                                               │
│                      ExecuteResponse                                         │
│                (data + summary + lineage + quality)                          │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Production Boundary

The key architectural decision: **NLQ extracts intent, BLL applies logic**.

```
┌────────────────────────────────┬────────────────────────────────────────┐
│           NLQ Layer            │           BLL Layer                     │
│         (Compiler)             │         (Executor)                      │
├────────────────────────────────┼────────────────────────────────────────┤
│ Extract operators              │ Load definition spec                   │
│ Match question → definition    │ Apply default_order_by                 │
│ Extract limit (TopN)           │ Apply tie_breaker for determinism      │
│ Extract time_window            │ Apply limit AFTER sorting              │
│ Detect ambiguity               │ Compute summary with population        │
│                                │ Track lineage to sources               │
│                                │                                        │
│ Does NOT:                      │ Does NOT:                              │
│ - Build SQL                    │ - Parse natural language               │
│ - Apply ordering               │ - Infer intent                         │
│ - Execute queries              │ - Handle ambiguity                     │
└────────────────────────────────┴────────────────────────────────────────┘
```

**Why This Matters:**
- Ordering is **declared** in definition specs, not **inferred** from questions
- Results are **deterministic** - same question always yields same ordering
- Definitions control their own behavior via `capabilities`

---

## Architectural Layers

### 1. NLQ Compiler Layer

**Location:** `backend/nlq/`

**Purpose:** Translate natural language questions into definition executions.

**Components:**
- `operator_extractor.py` - Detect temporal (MoM/QoQ/YoY), comparison, aggregation
- `intent_matcher.py` - Match question → definition using keywords + capabilities
- `param_extractor.py` - Extract TopN limit, time_window (NOT ordering)
- `models.py` - NLQ domain models

**Key Insight:** Operators are INDEPENDENT of metrics. "How did revenue change MoM?" needs `supports_delta` capability, regardless of which definition answers it.

### 2. BLL Executor Layer

**Location:** `backend/bll/`

**Purpose:** Execute definitions with deterministic ordering and summary computation.

**Components:**
- `definitions.py` - Definition registry with 10+ seeded definitions
- `models.py` - BLL domain models (Definition, ExecuteResponse, etc.)
- `executor.py` - Load data, apply ordering, compute summaries
- `routes.py` - API endpoints

**Ordering Strategy:**
```python
capabilities=DefinitionCapabilities(
    default_order_by=[OrderBySpec(field="AnnualRevenue", direction="desc")],
    allowed_order_by=["AnnualRevenue", "NumberOfEmployees"],
    tie_breaker="Id",  # Deterministic secondary sort
)
```

### 3. DCL Core Layer

**Location:** `backend/engine/`

**Purpose:** Build 4-layer graph snapshots for Sankey visualization.

**Components:**
- `dcl_engine.py` - Main orchestrator
- `schema_loader.py` - Load schemas from CSV or Farm API
- `mapping_service.py` - Field → concept mappings
- `narration_service.py` - Real-time processing logs

**Graph Layers:**
- **L0: Pipeline** - Entry point (pipe_demo/pipe_farm)
- **L1: Sources** - 11 source systems (Salesforce, SAP, MongoDB, etc.)
- **L2: Ontology** - 8 business concepts (Account, Revenue, Cost, etc.)
- **L3: Personas** - 4 BLL consumers (CFO, CRO, COO, CTO)

### 4. Farm Integration Layer

**Location:** `backend/farm/`

**Purpose:** Fetch deterministic ground truth data from Farm API.

**Components:**
- `client.py` - Farm API client with scenario generation

**Two Integration Modes:**

| Mode | Purpose | Endpoint |
|------|---------|----------|
| **Drift Repair** | Toxic stream detection → repair → verify | `/api/source`, `/api/target/validate` |
| **Ground Truth** | Fetch deterministic scenario data | `/api/scenarios/{id}/metrics/*` |

**Usage:**
```python
# Generate scenario (seed ensures reproducibility)
POST /api/scenarios/generate {"seed": 12345, "scale": "medium"}
# Returns: {"scenario_id": "dfa0ae0d57c9", ...}

# Query DCL with Farm data
POST /api/nlq/ask {
  "question": "top 5 customers by revenue",
  "dataset_id": "farm:dfa0ae0d57c9"
}
```

---

## Data Sources

### Demo Mode (dataset_id: "demo9")

Static CSV files for development:

```
dcl/demo/datasets/demo9/
├── salesforce_account.csv
├── dynamics_accounts.csv
├── hubspot_companies.csv
├── netsuite_customers.csv
├── sap_bkpf.csv
├── mongodb_customer_profiles.csv
├── supabase_user_events.csv
├── dw_dim_customer.csv
├── aws_cost_explorer.csv
└── ...
```

### Farm Mode (dataset_id: "farm:{scenario_id}")

Deterministic ground truth from Farm API:

| Endpoint | Data |
|----------|------|
| `/metrics/top-customers?limit=N` | Customer revenue data |
| `/metrics/revenue` | Total/period revenue |
| `/metrics/vendor-spend` | Vendor spending breakdown |

**Current Status:** Only `crm.top_customers` wired to Farm mode; other definitions fall back to demo CSVs.

---

## Definition Model

Definitions are the core abstraction - pre-configured business logic that NLQ routes to.

### Definition Structure

```python
Definition(
    definition_id="crm.top_customers",
    name="Top Customers by Revenue",
    category=DefinitionCategory.CRM,
    version="1.0.0",
    
    output_schema=[
        ColumnSchema(name="Id", dtype="string"),
        ColumnSchema(name="Name", dtype="string"),
        ColumnSchema(name="AnnualRevenue", dtype="float"),
    ],
    
    sources=[
        SourceReference(
            source_id="salesforce",
            table_id="salesforce_account",
            columns=["Id", "Name", "AnnualRevenue"]
        ),
    ],
    
    keywords=["top customers", "largest customers", "customer revenue"],
    
    capabilities=DefinitionCapabilities(
        supports_top_n=True,
        supports_delta=False,
        primary_metric="revenue",
        entity_type="customer",
        default_order_by=[OrderBySpec(field="AnnualRevenue", direction="desc")],
        tie_breaker="Id",
    ),
)
```

### Seeded Definitions (10+)

| Definition ID | Category | Purpose |
|--------------|----------|---------|
| `crm.top_customers` | CRM | Top customers by revenue |
| `crm.pipeline` | CRM | Sales pipeline deals |
| `finops.saas_spend` | FinOps | SaaS spending by vendor |
| `finops.arr` | FinOps | Annual recurring revenue |
| `finops.burn_rate` | FinOps | Monthly burn rate |
| `finops.unallocated_spend` | FinOps | Unallocated cloud spend |
| `aod.zombies_overview` | AOD | Idle/underutilized resources |
| `aod.findings_by_severity` | AOD | Security findings |
| `aod.identity_gap` | AOD | Resources without owners |
| `infra.deploy_frequency` | Infra | DORA deployment frequency |

---

## API Endpoints

### NLQ Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/nlq/ask` | POST | Execute natural language question |
| `/api/nlq/extract_params` | POST/GET | Extract parameters from question |
| `/api/nlq/answerability_rank` | POST | Get ranked definition matches |

### BLL Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/bll/definitions` | GET | List all definitions |
| `/api/bll/definitions/{id}` | GET | Get definition details |
| `/api/bll/execute` | POST | Execute definition directly |
| `/api/bll/proof/{id}` | GET | Get execution proof |

### DCL Core Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/dcl/run` | POST | Execute pipeline, return graph |
| `/api/dcl/narration/{session_id}` | GET | Poll narration messages |
| `/api/topology` | GET | Unified topology graph |

### Request/Response Examples

**POST /api/nlq/ask**
```json
// Request
{
  "question": "top 5 customers by revenue",
  "dataset_id": "demo9"
}

// Response
{
  "definition_id": "crm.top_customers",
  "confidence": 0.92,
  "matched_keywords": ["top", "customers", "revenue"],
  "data": [
    {"Id": "001...", "Name": "Enterprise Plus Corp", "AnnualRevenue": 89000000}
  ],
  "summary": {
    "answer": "Top 5 customers by revenue:\n1. Enterprise Plus Corp: $89M",
    "aggregations": {
      "customer_count": 5,
      "shown_total": 234500000,
      "share_of_total": 0.514
    }
  },
  "lineage": [
    {"source_id": "salesforce", "table_id": "salesforce_account"}
  ]
}
```

---

## File Structure

```
backend/
├── api/
│   └── main.py               # FastAPI app with all routes
│
├── nlq/                      # NLQ Compiler Layer
│   ├── intent_matcher.py     # Question → Definition matching
│   ├── operator_extractor.py # Temporal/comparison operators
│   ├── param_extractor.py    # TopN limit, time window
│   ├── models.py             # NLQ domain models
│   ├── compiler.py           # SQL template generation
│   ├── scorer.py             # Answerability scoring
│   ├── hypothesis.py         # Hypothesis generation
│   ├── validator.py          # Definition validation
│   ├── lineage.py            # Lineage tracking
│   ├── persistence.py        # In-memory persistence
│   ├── db_persistence.py     # PostgreSQL persistence
│   └── fixtures/             # Seeded data (JSON)
│
├── bll/                      # BLL Executor Layer
│   ├── definitions.py        # Definition registry
│   ├── models.py             # BLL domain models
│   ├── executor.py           # Definition execution
│   └── routes.py             # API routes
│
├── engine/                   # DCL Core Layer
│   ├── dcl_engine.py         # Main orchestrator
│   ├── schema_loader.py      # Schema loading
│   ├── mapping_service.py    # Field → concept mapping
│   └── narration_service.py  # Real-time logs
│
├── farm/                     # Farm Integration
│   └── client.py             # Farm API client
│
├── core/                     # Zero-Trust Core
│   ├── fabric_plane.py       # Fabric Plane types
│   ├── pointer_buffer.py     # Pointer buffering
│   └── topology_api.py       # Topology service
│
├── semantic_mapper/          # Batch Mapping (Cold Path)
│   ├── heuristic_mapper.py   # Pattern matching
│   └── persist_mappings.py   # DB persistence
│
└── domain/
    └── models.py             # Core domain models

src/                          # Frontend
├── App.tsx                   # Main React app
└── components/
    ├── ControlPanel.tsx      # Mode selection, persona toggles
    ├── SankeyGraph.tsx       # 4-layer visualization
    ├── NarrationPanel.tsx    # Terminal-style logs
    └── MonitorPanel.tsx      # Persona metrics

dcl/demo/datasets/demo9/      # Demo data (CSVs)
```

---

## Database Schema

### Core Tables

```sql
-- Ontology concepts
CREATE TABLE ontology_concepts (
    id VARCHAR(128) PRIMARY KEY,
    name VARCHAR(256),
    cluster VARCHAR(64),
    synonyms JSONB DEFAULT '[]',
    example_fields JSONB DEFAULT '[]'
);

-- Field → concept mappings
CREATE TABLE field_concept_mappings (
    id SERIAL PRIMARY KEY,
    source_id VARCHAR(128),
    table_name VARCHAR(256),
    field_name VARCHAR(256),
    concept_id VARCHAR(128) REFERENCES ontology_concepts(id),
    confidence FLOAT,
    mapped_at TIMESTAMP DEFAULT NOW()
);

-- Persona profiles
CREATE TABLE persona_profiles (
    id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(256),
    description TEXT
);

-- Persona → concept relevance
CREATE TABLE persona_concept_relevance (
    persona_id VARCHAR(64) REFERENCES persona_profiles(id),
    concept_id VARCHAR(128) REFERENCES ontology_concepts(id),
    relevance FLOAT CHECK (relevance BETWEEN 0 AND 1),
    PRIMARY KEY (persona_id, concept_id)
);

-- NLQ canonical events
CREATE TABLE canonical_events (
    id VARCHAR(128) PRIMARY KEY,
    schema_json JSONB DEFAULT '{}',
    time_semantics_json JSONB DEFAULT '{}'
);

-- NLQ bindings
CREATE TABLE bindings (
    id VARCHAR(128) PRIMARY KEY,
    source_system VARCHAR(128),
    canonical_event_id VARCHAR(128) REFERENCES canonical_events(id),
    mapping_json JSONB DEFAULT '{}',
    quality_score FLOAT,
    freshness_score FLOAT
);
```

---

## Environment Variables

| Variable | Description |
|----------|-------------|
| `DATABASE_URL` | PostgreSQL connection string |
| `REDIS_URL` | Redis connection string |
| `FARM_API_URL` | AOS-Farm API base URL |
| `RUN_MODE` | `dev` (heuristic) or `prod` (LLM/RAG) |
| `OPENAI_API_KEY` | OpenAI API key (embeddings + validation) |
| `PINECONE_API_KEY` | Pinecone API key |

---

## Key Design Decisions

### 1. NLQ = Compiler Pattern
NLQ extracts what the user wants (definition + parameters) but does NOT build SQL or apply ordering. Ordering is declared in definition specs.

### 2. Capability-Based Routing
Definitions declare `supports_delta`, `supports_trend`, etc. Intent matching filters to only definitions that support detected operators.

### 3. Deterministic Ordering
- `default_order_by`: Concrete columns with direction
- `tie_breaker`: Secondary sort for reproducible results
- `allowed_order_by`: Whitelist for future override support

### 4. Farm Ground Truth
Farm generates deterministic scenarios from seed values. Same seed = same data = reproducible tests.

### 5. No LLM in Hot Path
All scoring uses deterministic rules + stored metadata. LLM is only used in batch mapping (cold path).

---

## Related Documentation

- [NLQ_ARCHITECTURE.md](NLQ_ARCHITECTURE.md) - Detailed NLQ compiler documentation
- [SEMANTIC-LAYER.md](SEMANTIC-LAYER.md) - Unified semantic layer after BLL/NLQ merge
- [DCL-OVERVIEW.md](DCL-OVERVIEW.md) - What DCL does
- [ARCH-GLOBAL-PIVOT.md](ARCH-GLOBAL-PIVOT.md) - Zero-Trust Fabric Plane architecture
