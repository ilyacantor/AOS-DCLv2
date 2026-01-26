# DCL Semantic Layer

**Last Updated:** January 26, 2026

## Overview

The DCL Semantic Layer is the unified metadata infrastructure that enables natural language query answering through deterministic definition matching and execution. After the BLL/NLQ/DCL merge, it provides:

1. **BLL Definitions** - Reusable business logic definitions with capabilities
2. **NLQ Compiler** - Natural language → definition matching + parameter extraction
3. **Executor** - Definition execution with ordering and summary computation
4. **Source Bindings** - Mappings from source systems to canonical events
5. **Proof Chains** - Traceability to source system evidence

### Core Principle

**NLQ is a compiler, not a SQL builder.** NLQ extracts intent (which definition) and parameters (limit, time_window). Ordering and execution logic live in the definition spec.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     DCL Semantic Layer (Post-Merge)                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                         NLQ Compiler                                 │    │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐               │    │
│  │  │   Operator   │  │    Intent    │  │  Parameter   │               │    │
│  │  │   Extractor  │  │   Matcher    │  │  Extractor   │               │    │
│  │  └──────────────┘  └──────────────┘  └──────────────┘               │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                    │                                         │
│                                    ▼                                         │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                        BLL Executor                                  │    │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐               │    │
│  │  │  Definition  │  │    Data      │  │   Summary    │               │    │
│  │  │   Lookup     │  │   Loader     │  │   Computer   │               │    │
│  │  └──────────────┘  └──────────────┘  └──────────────┘               │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐    │
│  │   Registry   │  │  Validation  │  │   Lineage    │  │    Proof     │    │
│  │   Service    │  │   Services   │  │   Service    │  │   Resolver   │    │
│  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘    │
│                                                                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                         Data Sources                                         │
├─────────────────────────────────────────────────────────────────────────────┤
│  Demo CSVs (demo9/)  ←→  Farm Ground Truth (farm:{scenario_id})             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Production Boundary

The key architectural decision is the separation between NLQ (compiler) and BLL (executor):

```
┌────────────────────────────────┬────────────────────────────────────────┐
│           NLQ Layer            │           BLL Layer                     │
│         (Compiler)             │         (Executor)                      │
├────────────────────────────────┼────────────────────────────────────────┤
│ Extract operators              │ Apply default_order_by                 │
│ Match question → definition    │ Apply tie_breaker for determinism      │
│ Extract limit (TopN)           │ Apply limit AFTER sorting              │
│ Extract time_window            │ Compute share-of-total summary         │
│ Detect ambiguity               │ Track lineage to sources               │
│                                │                                        │
│ Does NOT:                      │ Does NOT:                              │
│ - Build SQL                    │ - Parse natural language               │
│ - Apply ordering               │ - Infer intent                         │
│ - Execute queries              │ - Handle ambiguity                     │
└────────────────────────────────┴────────────────────────────────────────┘
```

---

## BLL Definition Model

Definitions are the core abstraction - pre-configured business logic that NLQ routes to.

### Definition Structure

```python
class Definition(BaseModel):
    definition_id: str           # e.g., "crm.top_customers"
    name: str                    # Human-readable name
    description: str             # What this definition answers
    category: DefinitionCategory # finops, aod, crm, infra
    version: str                 # Semantic version
    
    output_schema: List[ColumnSchema]   # Expected columns
    sources: List[SourceReference]      # Where data comes from
    joins: Optional[List[JoinSpec]]     # How to join tables
    default_filters: Optional[List[FilterSpec]]
    
    dimensions: List[str]        # Grouping dimensions
    metrics: List[str]           # Computed metrics
    keywords: List[str]          # NLQ matching keywords
    capabilities: DefinitionCapabilities  # What operators it supports
```

### Definition Capabilities

Capabilities declare what operations a definition supports:

```python
class DefinitionCapabilities(BaseModel):
    supports_top_n: bool = True      # Can be limited/ranked
    supports_delta: bool = False     # Supports MoM/QoQ/YoY comparison
    supports_trend: bool = False     # Supports time-series trending
    supports_aggregation: bool = True
    
    primary_metric: Optional[str]    # "revenue", "cost", "count"
    entity_type: Optional[str]       # "customer", "vendor", "resource"
    
    # Production-grade ordering (declared, not inferred)
    default_order_by: List[OrderBySpec]  # Concrete columns
    allowed_order_by: List[str]          # Whitelist for overrides
    tie_breaker: Optional[str]           # Secondary sort for determinism
```

### Example Definition

```python
Definition(
    definition_id="crm.top_customers",
    name="Top Customers by Revenue",
    description="Ranked list of customers by annual revenue",
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
    dimensions=["Name"],
    metrics=["AnnualRevenue"],
    keywords=["top customers", "largest customers", "biggest accounts",
              "customer revenue", "revenue by customer"],
    capabilities=DefinitionCapabilities(
        supports_top_n=True,
        supports_delta=False,
        primary_metric="revenue",
        entity_type="customer",
        default_order_by=[
            OrderBySpec(field="AnnualRevenue", direction="desc")
        ],
        allowed_order_by=["AnnualRevenue", "NumberOfEmployees"],
        tie_breaker="Id",
    ),
)
```

### Definition Categories

| Category | Focus | Example Definitions |
|----------|-------|---------------------|
| **finops** | FinOps/Cloud | `saas_spend`, `arr`, `burn_rate`, `unallocated_spend` |
| **aod** | Asset Operations | `zombies_overview`, `identity_gap`, `findings_by_severity` |
| **crm** | CRM | `top_customers`, `pipeline_summary`, `deal_velocity` |
| **infra** | Infrastructure | `deploy_frequency`, `mttr`, `slo_attainment` |

---

## NLQ Processing Pipeline

### 1. Operator Extraction

Detects temporal, comparison, and aggregation operators:

| Type | Operators | Example Phrases |
|------|-----------|-----------------|
| **Temporal** | MoM, QoQ, YoY | "month over month", "vs last quarter" |
| **Comparison** | change, delta, growth | "how did X change", "variance" |
| **Aggregation** | top, total, average | "top 5", "total spend" |

```python
# "How did revenue change MoM?" extracts:
ExtractedOperators(
    temporal=TemporalOperator.MOM,
    comparison=ComparisonOperator.CHANGE,
    requires_delta=True
)
```

### 2. Intent Matching

Maps question to best definition using:
- Keyword matching with synonyms
- Capability filtering (route only to definitions that support detected operators)
- Ambiguity detection

```python
# "top 5 customers by revenue" matches:
MatchResult(
    best_match="crm.top_customers",
    confidence=0.92,
    matched_keywords=["top", "customers", "revenue"],
    capability_routed=True
)
```

### 3. Parameter Extraction

Extracts execution parameters (NOT ordering):

| Parameter | Patterns | Example |
|-----------|----------|---------|
| `limit` | "top N", "first N" | "top 5" → 5 |
| `time_window` | "last month", "YTD" | "this quarter" → current_quarter |

```python
# "Show me top 10 vendors from last quarter" extracts:
ExecutionArgs(
    limit=10,
    time_window="last_quarter"
)
```

---

## Data Sources

### Demo Mode (dataset_id: "demo9")

Static CSV files for development and testing:

```
dcl/demo/datasets/demo9/
├── salesforce_account.csv
├── dynamics_accounts.csv
├── hubspot_companies.csv
├── netsuite_customers.csv
└── ...
```

### Farm Mode (dataset_id: "farm:{scenario_id}")

Deterministic ground truth from Farm API:

```bash
# Generate scenario (seed ensures reproducibility)
POST /api/scenarios/generate {"seed": 12345, "scale": "medium"}
# Returns: {"scenario_id": "dfa0ae0d57c9", ...}

# Query DCL with Farm data
POST /api/nlq/ask {
  "question": "top 5 customers by revenue",
  "dataset_id": "farm:dfa0ae0d57c9"
}
```

**Farm Ground Truth Endpoints:**
- `/api/scenarios/{id}/metrics/top-customers?limit=N`
- `/api/scenarios/{id}/metrics/revenue`
- `/api/scenarios/{id}/metrics/vendor-spend`

---

## API Reference

### Core NLQ Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/nlq/ask` | POST | Execute natural language question |
| `/api/nlq/extract_params` | POST/GET | Extract parameters from question |
| `/api/nlq/answerability_rank` | POST | Get ranked hypotheses (circles) |
| `/api/nlq/explain` | POST | Generate explanation for hypothesis |

### BLL Execution Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/bll/execute` | POST | Execute definition directly |
| `/api/bll/definitions` | GET | List all definitions |
| `/api/bll/definitions/{id}` | GET | Get definition details |

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
    {"Id": "001...", "Name": "Enterprise Plus Corp", "AnnualRevenue": 89000000},
    {"Id": "002...", "Name": "TechFlow Inc", "AnnualRevenue": 48000000}
  ],
  "summary": {
    "answer": "Top 5 customers by revenue:\n1. Enterprise Plus Corp: $89M\n...",
    "aggregations": {
      "customer_count": 5,
      "shown_total": 234500000,
      "population_total": 456000000,
      "share_of_total": 0.514
    }
  },
  "lineage": [
    {
      "source_id": "salesforce",
      "table_id": "salesforce_account",
      "columns_used": ["Id", "Name", "AnnualRevenue"],
      "row_contribution": 847
    }
  ]
}
```

---

## Canonical Events & Bindings

### Canonical Events

System-agnostic business event types:

```json
{
  "id": "revenue_recognized",
  "tenant_id": "default",
  "schema_json": {
    "fields": [
      {"name": "event_id", "type": "string"},
      {"name": "amount", "type": "decimal"},
      {"name": "customer_id", "type": "string"},
      {"name": "occurred_at", "type": "timestamp"},
      {"name": "effective_at", "type": "timestamp"}
    ]
  },
  "time_semantics_json": {
    "occurred_at": "created_timestamp",
    "effective_at": "recognition_date"
  }
}
```

### Event Categories

| Category | Events |
|----------|--------|
| Revenue/Billing | `invoice_issued`, `revenue_recognized`, `payment_received` |
| Subscription | `subscription_started`, `subscription_changed`, `subscription_canceled` |
| CRM | `opportunity_created`, `deal_won`, `customer_onboarded` |
| Operations | `work_item_completed`, `sla_breached`, `ticket_resolved` |
| Engineering | `deployment_completed`, `incident_opened`, `incident_resolved` |

### Bindings

Map source systems to canonical events:

```json
{
  "id": "netsuite_revenue_recognized",
  "source_system": "NetSuite",
  "canonical_event_id": "revenue_recognized",
  "mapping_json": {
    "transaction_id": "event_id",
    "amount": "amount",
    "customer_id": "customer_id"
  },
  "dims_coverage_json": {
    "customer": true,
    "service_line": true,
    "region": false
  },
  "quality_score": 0.92,
  "freshness_score": 0.95
}
```

---

## Validation & Lineage

### DefinitionValidator

Validates definition answerability:

```python
result = validator.validate(
    definition_id="arr",
    version="v1",
    requested_dims=["customer"],
    time_window="QoQ"
)

# Returns:
ValidationResult(
    ok=True,
    missing_events=[],
    missing_dims=[],
    weak_bindings=[],
    coverage_score=0.92,
    freshness_score=0.88,
    proof_score=0.75
)
```

### LineageService

Track data flow from definitions to sources:

```python
lineage = lineage_service.get_definition_lineage("arr")

# Returns:
{
    "definition_id": "arr",
    "events": ["subscription_started", "subscription_changed"],
    "bindings": ["chargebee_subscription"],
    "source_systems": ["Chargebee"]
}
```

### ConsistencyValidator

Checks semantic layer integrity:

| Check | Description | Severity |
|-------|-------------|----------|
| `orphan_events` | Events without bindings | Warning |
| `orphan_definitions` | Definitions missing events | Error |
| `circular_dependencies` | Cycles in dependencies | Error |
| `binding_coverage` | Incomplete dimension coverage | Warning |

---

## Database Schema

### PostgreSQL Tables

```sql
-- Canonical event types
CREATE TABLE canonical_events (
    id VARCHAR(128) PRIMARY KEY,
    tenant_id VARCHAR(64) DEFAULT 'default',
    schema_json JSONB NOT NULL DEFAULT '{}',
    time_semantics_json JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMP DEFAULT NOW()
);

-- Business entities (dimensions)
CREATE TABLE entities (
    id VARCHAR(128) PRIMARY KEY,
    tenant_id VARCHAR(64) DEFAULT 'default',
    identifiers_json JSONB NOT NULL DEFAULT '{}'
);

-- Source system bindings
CREATE TABLE bindings (
    id VARCHAR(128) PRIMARY KEY,
    tenant_id VARCHAR(64) DEFAULT 'default',
    source_system VARCHAR(128) NOT NULL,
    canonical_event_id VARCHAR(128) NOT NULL,
    mapping_json JSONB NOT NULL DEFAULT '{}',
    dims_coverage_json JSONB NOT NULL DEFAULT '{}',
    quality_score FLOAT DEFAULT 0.5,
    freshness_score FLOAT DEFAULT 0.5
);

-- Metric/view definitions
CREATE TABLE definitions (
    id VARCHAR(128) PRIMARY KEY,
    tenant_id VARCHAR(64) DEFAULT 'default',
    kind VARCHAR(32) DEFAULT 'metric',
    pack VARCHAR(64),
    description TEXT,
    default_time_semantics_json JSONB DEFAULT '{}'
);

-- Versioned definition specs
CREATE TABLE definition_versions (
    id VARCHAR(128) PRIMARY KEY,
    definition_id VARCHAR(128) NOT NULL,
    version VARCHAR(32) DEFAULT 'v1',
    status VARCHAR(32) DEFAULT 'draft',
    spec_json JSONB NOT NULL DEFAULT '{}',
    published_at TIMESTAMP
);

-- Query execution audit
CREATE TABLE query_executions (
    id VARCHAR(64) PRIMARY KEY,
    definition_id VARCHAR(128),
    sql_hash VARCHAR(64),
    status VARCHAR(32),
    execution_time_ms FLOAT,
    row_count INTEGER
);
```

---

## File Structure

```
backend/
├── nlq/                      # NLQ Compiler Layer
│   ├── intent_matcher.py     # Question → Definition matching
│   ├── operator_extractor.py # Temporal/comparison operators
│   ├── param_extractor.py    # TopN limit, time window
│   ├── models.py             # NLQ domain models
│   ├── validator.py          # Definition validation
│   ├── compiler.py           # SQL template generation
│   ├── scorer.py             # Answerability scoring
│   ├── lineage.py            # Lineage tracking
│   ├── persistence.py        # In-memory persistence
│   └── db_persistence.py     # PostgreSQL persistence
│
├── bll/                      # BLL Executor Layer
│   ├── definitions.py        # Definition registry (seeds)
│   ├── models.py             # BLL domain models
│   ├── executor.py           # Definition execution
│   └── routes.py             # API routes
│
├── farm/                     # Farm Integration
│   └── client.py             # Farm API client
│
└── api/
    └── main.py               # FastAPI app with NLQ/BLL routes
```

---

## Related Documentation

- [NLQ_ARCHITECTURE.md](NLQ_ARCHITECTURE.md) - Detailed NLQ layer documentation
- [ARCH-DCL-CURRENT.md](ARCH-DCL-CURRENT.md) - Overall DCL architecture
- [DCL-OVERVIEW.md](DCL-OVERVIEW.md) - What DCL does
- [ARCH-GLOBAL-PIVOT.md](ARCH-GLOBAL-PIVOT.md) - Zero-Trust architecture
