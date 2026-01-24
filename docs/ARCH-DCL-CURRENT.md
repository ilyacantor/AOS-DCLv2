# DCL Architecture - Current State

**Last Updated:** January 24, 2026

## Overview

DCL (Data Connectivity Layer) is a **metadata-only semantic mapping engine** with two major subsystems:

1. **Semantic Mapping Engine** - Transforms source schemas into ontology concepts
2. **NLQ Semantic Layer** - Provides natural language query answerability through deterministic hypothesis ranking

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         DCL Architecture                                 │
├────────────────────────────────┬────────────────────────────────────────┤
│     Semantic Mapping Engine    │         NLQ Semantic Layer             │
├────────────────────────────────┼────────────────────────────────────────┤
│  ├── SchemaLoader              │  ├── DefinitionRegistry                │
│  ├── SourceNormalizer          │  ├── ConsistencyValidator              │
│  ├── MappingService            │  ├── LineageService                    │
│  ├── RAGService                │  ├── SchemaEnforcer                    │
│  ├── PersonaView               │  ├── QueryExecutor                     │
│  └── NarrationService          │  └── ProofResolver                     │
├────────────────────────────────┴────────────────────────────────────────┤
│                          Shared Infrastructure                          │
├─────────────────────────────────────────────────────────────────────────┤
│  ├── NLQPersistence (JSON/PostgreSQL)                                   │
│  ├── Database Models (SQLAlchemy)                                       │
│  └── Zero-Trust Security Layer                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 1. Semantic Mapping Engine

The original DCL pipeline that maps source fields to ontology concepts.

### Pipeline Flow

```
L0 Pipeline → L1 Sources (11) → L2 Ontology (8 concepts) → L3 Personas (4)
```

### Components

| Component | Purpose |
|-----------|---------|
| `SchemaLoader` | Load schemas from CSV or Farm API |
| `SourceNormalizer` | Register and deduplicate sources |
| `MappingService` | Heuristic field-to-concept mapping |
| `RAGService` | Vector-based semantic matching (Prod) |
| `PersonaView` | Filter graph by business role |
| `NarrationService` | Real-time status broadcasting |

### Data Model

```
ontology_concepts (8)     → Business concepts (Revenue, Cost, etc.)
field_concept_mappings    → Source field → Concept mappings
persona_profiles (4)      → CFO, CRO, COO, CTO
persona_concept_relevance → Which concepts matter to which persona
```

---

## 2. NLQ Semantic Layer

The new semantic layer providing hypothesis ranking for natural language questions.

### Core Principle

**No LLM in the hot path.** All answerability scoring uses deterministic rules + stored metadata.

### Components

| Component | Purpose | Location |
|-----------|---------|----------|
| `DefinitionRegistry` | Catalog management, search, publish workflow | `registry.py` |
| `ConsistencyValidator` | Orphan detection, cycles, binding coverage | `consistency.py` |
| `LineageService` | Dependency graph, impact analysis | `lineage.py` |
| `SchemaEnforcer` | Validate events, bindings, specs | `schema_enforcer.py` |
| `QueryExecutor` | Execute queries with caching | `executor.py` |
| `ProofResolver` | Source system URL generation | `proof.py` |
| `AnswerabilityScorer` | Rank hypotheses by probability | `scorer.py` |
| `HypothesisGenerator` | Generate hypotheses from specs | `hypothesis.py` |

### Data Model

```
canonical_events (45)     → Business events (revenue_recognized, invoice_posted)
entities (33)             → Dimensions (customer, service_line, region)
definitions (40)          → Metrics organized by pack (CFO/CTO/COO/CEO)
definition_versions       → Versioned specs with measure operations
bindings (20)             → Source system → Canonical event mappings
proof_hooks               → Links to source system evidence
```

### Metric Packs

| Pack | Metrics | Examples |
|------|---------|----------|
| CFO | 16 | recognized_revenue, arr, dso, burn_rate |
| CTO | 9 | deploy_frequency, mttr, slo_attainment |
| COO | 8 | throughput, cycle_time, sla_compliance |
| CEO | 8 | revenue_growth, churn_rate, runway |

---

## API Endpoints

### Semantic Mapping

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/dcl/run` | POST | Execute mapping pipeline |
| `/api/dcl/narration/{run_id}` | GET | Poll narration messages |
| `/api/dcl/batch-mapping` | POST | Run mapping on sources |
| `/api/topology` | GET | Get graph structure |

### NLQ Answerability

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/nlq/answerability_rank` | POST | Rank hypotheses for question |
| `/api/nlq/explain` | POST | Get explanation for hypothesis |

### NLQ Registration

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/nlq/events` | GET/POST | Manage canonical events |
| `/api/nlq/entities` | GET/POST | Manage entities |
| `/api/nlq/definitions` | GET/POST | Manage definitions |
| `/api/nlq/bindings` | GET/POST | Manage bindings |

### NLQ Registry (New)

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/nlq/registry/definitions` | GET | List with filtering |
| `/api/nlq/registry/definitions/search` | GET | Full-text search |
| `/api/nlq/registry/definitions/{id}` | GET | Get detail with lineage |
| `/api/nlq/registry/definitions/{id}/publish` | POST | Publish workflow |
| `/api/nlq/registry/consistency/check` | GET | Run all checks |
| `/api/nlq/registry/lineage/graph` | GET | Full dependency graph |
| `/api/nlq/registry/lineage/impact` | POST | Impact analysis |
| `/api/nlq/registry/schema/validate` | GET | Validate all schemas |
| `/api/nlq/registry/execute` | POST | Execute queries |
| `/api/nlq/registry/proof/chain/{id}` | GET | Full proof chain |

---

## Database Schema

### Existing Tables (Semantic Mapping)

```sql
ontology_concepts         -- 8 business concepts
field_concept_mappings    -- Field to concept mappings
persona_profiles          -- 4 personas
persona_concept_relevance -- Persona to concept relevance
```

### New Tables (NLQ Semantic Layer)

```sql
canonical_events          -- Event types with schemas
entities                  -- Business dimensions
bindings                  -- Source system mappings
definitions               -- Metric/view definitions
definition_versions       -- Versioned specs
proof_hooks               -- Source evidence links
lineage_edges             -- Dependency graph
query_executions          -- Audit log
consistency_checks        -- Validation results
```

---

## Zero-Trust Architecture

DCL operates in **metadata-only mode**:

### What DCL Stores
- Schema structures (field names, types)
- Semantic mappings (field → concept)
- Definition specs (events, measures, filters)
- Lineage graphs (dependencies)
- Proof pointers (not actual records)

### What DCL Does NOT Store
- Raw record data
- Customer PII
- Actual field values
- Payloads of any kind

### Fabric Pointer Buffering
When data is needed, DCL performs Just-in-Time fetch from Fabric Planes:
- Kafka: `{ topic, partition, offset }`
- Snowflake: `{ database, schema, table, stream_id }`

---

## Technology Stack

| Component | Technology |
|-----------|------------|
| Backend | FastAPI + Python 3.11 |
| Frontend | React 18 + TypeScript + Vite |
| Visualization | D3.js (d3-sankey) |
| Database | PostgreSQL (SQLAlchemy + Alembic) |
| Cache | Redis |
| Vector DB | Pinecone (mapping only) |
| LLM | OpenAI GPT-4o-mini (mapping validation) |

---

## File Structure

```
backend/
├── engine/                 # DCL Engine (semantic mapping)
│   ├── __init__.py
│   ├── dcl_engine.py
│   ├── persona_view.py
│   └── schema_loader.py
├── nlq/                    # NLQ Semantic Layer
│   ├── models.py           # Pydantic models
│   ├── persistence.py      # JSON/DB persistence
│   ├── db_models.py        # SQLAlchemy models
│   ├── db_persistence.py   # Database operations
│   ├── scorer.py           # Answerability scoring
│   ├── validator.py        # Definition validation
│   ├── compiler.py         # SQL compilation
│   ├── hypothesis.py       # Hypothesis generation
│   ├── explainer.py        # Explanation generation
│   ├── registry.py         # Definition registry
│   ├── consistency.py      # Consistency validation
│   ├── lineage.py          # Lineage/impact service
│   ├── schema_enforcer.py  # Schema validation
│   ├── executor.py         # Query execution
│   ├── proof.py            # Proof resolution
│   ├── routes_registry.py  # Registry API routes
│   ├── fixtures/           # JSON fixtures
│   └── migrations/         # Alembic migrations
└── api/
    └── main.py             # FastAPI application
```

---

## Related Documentation

- [DCL-OVERVIEW.md](DCL-OVERVIEW.md) - What DCL actually does
- [ARCH-DCL-TARGET.md](ARCH-DCL-TARGET.md) - Original refactoring goals
- [SEMANTIC-LAYER.md](SEMANTIC-LAYER.md) - Detailed semantic layer documentation
- [ARCH-GLOBAL-PIVOT.md](ARCH-GLOBAL-PIVOT.md) - Zero-Trust architecture
