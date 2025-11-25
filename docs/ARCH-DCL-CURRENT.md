# DCL Architecture - Current State (Post-Refactoring)

**Last Updated:** November 25, 2025  
**Version:** 2.1 (Semantic Mapping Architecture + Interactive Drill-Down)

## Overview

The DCL (Data Connectivity Layer) Engine is now a **3-layer architecture** that separates batch semantic mapping (cold path) from runtime graph generation (hot path). The system uses database-driven configuration for ontology concepts, persona profiles, and field-to-concept mappings.

## Architectural Layers

### 1. Semantic Mapper (Cold Path / Batch)

**Location:** `backend/semantic_mapper/`

**Purpose:** Analyze source schemas and create persistent field→concept mappings using heuristics, RAG, and LLM.

**Components:**
- `heuristic_mapper.py` - Stage 1: Pattern matching using ontology metadata
- `persist_mappings.py` - Database persistence layer
- `runner.py` - Orchestrates the mapping pipeline

**Execution Triggers:**
- Manual via `/api/dcl/batch-mapping` endpoint
- On source connection (future)
- Scheduled refresh (future)

**Output:** Rows in `field_concept_mappings` table

### 2. Semantic Model (Data Layer)

**Location:** PostgreSQL database

**Tables:**
- `ontology_concepts` - Core concepts (account, revenue, cost, etc.) with cluster tags (Finance, Growth, Infra, Ops)
- `field_concept_mappings` - Persistent mappings from source fields to ontology concepts with confidence scores
- `persona_profiles` - CFO, CRO, COO, CTO definitions
- `persona_concept_relevance` - Which concepts each persona cares about (0.0-1.0 relevance scores)

**Configuration:**
- `config/ontology_concepts.yaml` - Ontology definition with synonyms and example fields
- `config/persona_profiles.yaml` - Persona descriptions and concept relevance mappings
- `backend/utils/config_sync.py` - Syncs YAML configs to database on startup

### 3. DCL Engine (Hot Path / Runtime)

**Location:** `backend/engine/dcl_engine.py`

**Purpose:** Build graph snapshots using stored mappings and persona-filtered concepts.

**Flow:**
1. Load source schemas (Demo or Farm mode)
2. Check for stored mappings in database
3. Fall back to creating new mappings if none exist
4. Filter ontology concepts by selected persona relevance
5. Build 4-layer graph (L0: pipe → L1: sources → L2: ontology → L3: personas)
6. Return graph snapshot with explanations and metrics

**Key Change:** NO LLM/RAG calls at runtime - all intelligence is pre-computed and stored.

## Data Flow

```
┌─────────────────────────────────────┐
│  Semantic Mapper (Batch / Cold)     │
│  - Heuristic matching                │
│  - RAG enhancement (optional)        │
│  - LLM refinement (optional)         │
│  └─> field_concept_mappings          │
└─────────────────────────────────────┘
              ↓
┌─────────────────────────────────────┐
│  Database (Persistent Storage)       │
│  - ontology_concepts                 │
│  - field_concept_mappings            │
│  - persona_profiles                  │
│  - persona_concept_relevance         │
└─────────────────────────────────────┘
              ↓
┌─────────────────────────────────────┐
│  DCL Engine (Runtime / Hot)          │
│  - Read stored mappings              │
│  - Filter by persona relevance       │
│  - Build graph snapshot              │
│  - Add explanations                  │
└─────────────────────────────────────┘
```

## Persona-Driven Filtering

**Before:** Hardcoded dictionary mapping personas to concept lists

```python
persona_mappings = {
    Persona.CFO: ["revenue", "cost"],
    ...
}
```

**After:** Database-driven relevance matrix

The system now:
1. Queries `persona_concept_relevance` for selected personas
2. Filters ontology nodes to only show relevant concepts
3. Only creates edges to/from relevant concepts
4. Adaptively includes concepts based on available data

**Example:**
- CTO selects with Farm data → Shows aws_resource, usage, cost concepts
- If no mappings exist for those concepts → Shows clean L1 nodes with no misleading edges
- If CFO+CRO both selected → Shows union of their relevant concepts

## API Endpoints

### Runtime Endpoints (Fast)

**POST /api/dcl/run**
- Builds graph snapshot using stored mappings
- Personas filter which concepts are shown
- No LLM calls, deterministic output

### Batch Endpoints (Slow)

**POST /api/dcl/batch-mapping**
- Triggers semantic mapper on sources
- Creates/updates field→concept mappings
- Should be run when sources change

## Key Benefits

1. **Performance:** Runtime graph building is 10-100x faster (no LLM calls)
2. **Determinism:** Same sources + personas always produce same graph
3. **Adaptability:** Persona views automatically adapt to available data
4. **Explainability:** Ontology nodes include "derived from X fields" explanations
5. **Maintainability:** Config-driven ontology and personas (YAML → DB)

## Migration Summary

**What Changed:**
- ✅ Removed hardcoded `persona_mappings` dictionary
- ✅ Added 4 database tables for semantic model
- ✅ Created `semantic_mapper/` module for batch mapping
- ✅ Added `PersonaView` class for DB-driven persona logic
- ✅ DCL engine now reads stored mappings instead of computing live
- ✅ Added batch mapping API endpoint

**What Stayed the Same:**
- Graph structure (L0 → L1 → L2 → L3)
- Frontend rendering logic
- Demo/Farm mode support
- Narration service
- Monitor panel

## Frontend Features (November 2025)

### Interactive Drill-Down (Monitor Panel)

The Monitor panel provides a 3-level drill-down for exploring data lineage:

**Persona Views Tab:**
1. **Level 1 - Ontology Concepts:** Click to expand and see contributing sources
2. **Level 2 - Source Systems:** Click to expand and see tables with mapped fields
3. **Level 3 - Fields:** Click for full mapping details

**Detail Panel Modal:**
- Click info icons on any level to view:
  - Source details: type, status, table count, total fields
  - Table details: parent source, mapped fields with confidence
  - Field details: full path (source→table→field), confidence bar, mapping explanation

**Key Design:** Only mapped fields are shown (not raw schema), reflecting actual data flow through DCL.

### Source Hierarchy Data Structure

The backend provides `source_hierarchy` in L2 ontology node metrics:

```json
{
  "source_hierarchy": {
    "salesforce": {
      "accounts": [
        {"field": "account_name", "confidence": 0.95},
        {"field": "account_id", "confidence": 0.92}
      ]
    },
    "dynamics": {
      "customers": [
        {"field": "customername", "confidence": 0.88}
      ]
    }
  }
}
```

This enables the UI to show exactly which source→table→field combinations contribute to each ontology concept.

## Source Data Requirements

For DCL to properly map and visualize data sources, the following information is needed:

### Required Fields

| Field | Description | Example |
|-------|-------------|---------|
| `source_id` | Unique identifier for the source system | `salesforce`, `dynamics_erp`, `snowflake_dw` |
| `source_type` | Category of the source | `crm`, `erp`, `datawarehouse`, `nosql`, `api` |
| `vendor` | Platform/vendor name | `Salesforce`, `Microsoft Dynamics`, `Snowflake` |
| `tables` | List of table schemas | See below |

### Table Schema

| Field | Description | Example |
|-------|-------------|---------|
| `table_name` | Name of the table/collection | `accounts`, `invoices`, `customers` |
| `fields` | List of field definitions | See below |

### Field Schema

| Field | Description | Example |
|-------|-------------|---------|
| `field_name` | Name of the field/column | `account_name`, `total_amount` |
| `field_type` | Data type | `string`, `number`, `date`, `boolean` |
| `semantic_hints` | Optional hints for mapping | `is_identifier`, `is_currency`, `is_date` |

### Why Vendor/Platform ID Matters

Different vendors have distinct conventions that affect mapping accuracy:

| Vendor | Account Field | Date Format | ID Pattern |
|--------|--------------|-------------|------------|
| Salesforce | `AccountName` | `CreatedDate` | `001...` (15/18 char) |
| Dynamics | `accountname` | `createdon` | GUID |
| HubSpot | `company_name` | `created_at` | Integer |
| PostgreSQL | `account_name` | `created_at` | Serial/UUID |

With vendor identification, DCL can apply vendor-specific mapping heuristics instead of generic pattern matching, significantly improving accuracy.

## Future Enhancements

1. **Full 3-Stage Pipeline:** Add RAG and LLM stages to semantic mapper
2. **Automatic Remapping:** Trigger batch mapping when sources change
3. **Confidence Thresholds:** Filter low-confidence mappings
4. **Conflict Resolution:** Handle multiple concepts matching same field
5. **Cluster-Based Views:** Allow filtering by concept cluster (Finance, Growth, Infra, Ops)
6. **Vendor-Specific Mapping:** Apply platform-specific conventions based on source vendor ID
