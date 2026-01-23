# DCL Engine - What It Actually Does

**Last Updated:** January 23, 2026

## Definition

DCL (Data Connectivity Layer) is a **metadata-only semantic mapping engine**. It transforms schema structures from source systems into a unified ontology, then visualizes the relationships in a 4-layer Sankey diagram filtered by business persona.

**DCL does NOT:**
- Store or process raw payload data
- Provide natural language querying
- Track data lineage or compliance
- Perform data governance

---

## Core Pipeline

```
L0 Pipeline → L1 Sources (11) → L2 Ontology (8 concepts) → L3 Personas (4)
```

**Input:** Schema metadata (field names, types) from source systems  
**Output:** Semantic graph showing which sources map to which business concepts, filtered by persona

---

## Functional Capabilities

### 1. Schema Loading
| Mode | Source | What It Loads |
|------|--------|---------------|
| Demo | Local CSV files | 9 pre-defined source schemas |
| Farm | Remote API | Synthetic schemas from AOS-Farm |

**What it captures:** Field names, data types, table structures  
**What it does NOT capture:** Actual row data, record values

### 2. Source Normalization
- Registers source systems with unique IDs
- Resolves aliases (e.g., "SFDC" → "salesforce_crm")
- Detects duplicate/similar sources
- Assigns layer positions for visualization

### 3. Semantic Mapping
| Mode | Method | Accuracy |
|------|--------|----------|
| Dev (Heuristic) | Pattern matching, field name analysis | ~85% |
| Prod (RAG + LLM) | Pinecone vectors + OpenAI validation | ~95% |

**Maps:** Source fields → Ontology concepts  
**Example:** `salesforce.Account.Name` → `ontology_account`

---

## Inference / RAG Capabilities

### What RAG Actually Does

RAG in DCL is used **only for mapping enhancement** - NOT for querying data.

#### 1. Lesson Storage (RAGService)
Stores high-confidence mappings as "lessons" in Pinecone:
```
Input: { field: "customer_name", concept: "account", confidence: 0.92 }
Output: Vector embedding stored in Pinecone index "dcl-mapping-lessons"
```

**Embedding Model:** OpenAI `text-embedding-3-small` (1536 dimensions)

#### 2. LLM Validation (MappingValidator)
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
Original mapping: "account" (Customer Account)
LLM correction: "gl_account" (General Ledger) 
Reason: "GL_ACCOUNT in financial context refers to General Ledger, not Customer"
```

#### 3. What RAG Does NOT Do
| Misconception | Reality |
|---------------|---------|
| "Query data with natural language" | RAG is for mapping, not data querying |
| "Search across structured data" | No query interface exists |
| "Semantic search over records" | DCL doesn't access record data |
| "AI-powered data exploration" | Only validates field-to-concept mappings |

### LLM Usage Summary

| Service | Model | Purpose |
|---------|-------|---------|
| RAGService | OpenAI `text-embedding-3-small` | Generate embeddings for lessons |
| MappingValidator | OpenAI `gpt-4o-mini` | Validate ambiguous mappings |
| (Planned) | Gemini 2.5 Flash | Schema understanding |

### Keys Required
| Key | Used For |
|-----|----------|
| `OPENAI_API_KEY` | Embeddings + Validation |
| `PINECONE_API_KEY` | Vector storage |
| `GEMINI_API_KEY` | (Reserved for schema understanding)

### 4. Ontology Concepts (8 Fixed)
| Concept | Business Meaning |
|---------|------------------|
| Account | Customer/company entities |
| Opportunity | Sales pipeline deals |
| Revenue | Income/billing amounts |
| Cost | Expenses/spend |
| Date | Timestamps/periods |
| Health | Customer health scores |
| Usage | Product usage metrics |
| AWS Resource | Cloud infrastructure |

### 5. Persona Filtering (4 Views)
| Persona | Relevant Concepts |
|---------|-------------------|
| CFO | Revenue, Cost, Date |
| CRO | Account, Opportunity, Revenue |
| COO | Usage, Health, Cost |
| CTO | AWS Resource, Usage, Health |

### 6. Graph Visualization
- 4-layer Sankey diagram (L0 → L1 → L2 → L3)
- Interactive node highlighting
- Link thickness = mapping confidence
- Real-time updates during pipeline run

### 7. Narration Service
- Broadcasts status messages during pipeline execution
- Terminal-style output in UI
- Timestamps in PST (user preference)

---

## API Endpoints

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/dcl/run` | POST | Execute mapping pipeline |
| `/api/dcl/narration/{run_id}` | GET | Poll narration messages |
| `/api/dcl/monitor/{run_id}` | GET | Get run metrics |
| `/api/dcl/batch-mapping` | POST | Run mapping on specific sources |
| `/api/topology` | GET | Get graph structure |
| `/api/topology/health` | GET | Connection health (from AAM) |
| `/api/topology/stats` | GET | Service statistics |

---

## Zero-Trust Architecture (January 2026)

### Fabric Pointer Buffering
DCL buffers **only pointers** to data in Fabric Planes:
- Kafka: `{ topic, partition, offset }`
- Snowflake: `{ database, schema, table, stream_id, row_cursor }`

**Payload is NEVER stored.** When the semantic mapper needs actual data, it performs a Just-in-Time fetch from the Fabric Plane, processes in-memory, and discards.

### What DCL Stores
- Schema structures (field names, types)
- Semantic mappings (field → concept)
- Graph snapshots (nodes, links)
- Run metrics (counts, durations)

### What DCL Does NOT Store
- Raw record data
- Customer PII
- Actual field values
- Payloads of any kind

---

## Technology Stack

| Component | Technology |
|-----------|------------|
| Backend | FastAPI + Python 3.11 |
| Frontend | React 18 + TypeScript + Vite |
| Visualization | D3.js (d3-sankey) |
| Database | PostgreSQL |
| Cache/Streams | Redis |
| Vector DB | Pinecone (Prod mode only) |
| LLM | Gemini 2.5 Flash, OpenAI GPT-4-mini |

---

## Services Architecture

```
DCLEngine (Orchestrator)
├── SchemaLoader        → Load schemas from CSV or Farm API
├── SourceNormalizer    → Register and deduplicate sources
├── MappingService      → Heuristic field-to-concept mapping
├── RAGService          → Vector-based semantic matching (Prod)
├── NarrationService    → Real-time status broadcasting
└── PersonaView         → Filter graph by business role
```

---

## What DCL Is NOT

| Misconception | Reality |
|---------------|---------|
| "DCL enriches raw data" | DCL only processes metadata (schemas), never touches raw data |
| "Natural language queries" | RAG is for mapping enhancement, not querying data |
| "Data lineage tracking" | No lineage graph exists; only shows current mappings |
| "Compliance/governance" | No compliance features; no audit trails |
| "Data quality metrics" | Quality score is repair success rate, not data quality |
| "ETL/data transformation" | DCL does not move or transform data |

---

## Summary

DCL is a **semantic mapping visualization tool**. It:
1. Reads schema metadata from sources
2. Maps fields to business concepts using heuristics/AI
3. Renders a Sankey graph filtered by persona
4. Buffers only pointers (Zero-Trust compliance)

It does NOT handle raw data, lineage, compliance, or governance.
