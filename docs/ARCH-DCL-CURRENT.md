# DCL Architecture - Current State

**Last Updated:** January 23, 2026  
**Version:** 3.0 (Industrial Dashboard + Connector Provisioning)

## Overview

The DCL (Data Connectivity Layer) Engine is a full-stack application designed to ingest and unify schemas and sample data from diverse sources into a common ontology using AI and heuristics. It visualizes data flow via an interactive Sankey diagram and supports two data modes: Demo (legacy sources) and Farm (synthetic data).

## System Architecture Context

| Component | Responsibility |
|-----------|---------------|
| **AAM** | Acquires and maintains connections to enterprise integration fabric (iPaaS, API managers, streams, warehouses). Routes pipes to DCL. |
| **DCL** | Ingests schemas and data from routed pipes, performs semantic mapping to unified ontology, serves visualization and telemetry. |
| **Farm** | Provides synthetic data streams, source of truth for verification, chaos injection for testing. |

## Architectural Layers

### 1. Semantic Mapper (Cold Path / Batch)

**Location:** `backend/semantic_mapper/`

**Purpose:** Analyze source schemas and create persistent field-to-concept mappings using heuristics, RAG, and LLM.

**Components:**
- `heuristic_mapper.py` - Stage 1: Pattern matching using ontology metadata
- `persist_mappings.py` - Database persistence layer with connection pooling
- `runner.py` - Orchestrates the mapping pipeline

**Execution Triggers:**
- Manual via `/api/dcl/batch-mapping` endpoint
- On source connection (future)
- Scheduled refresh (future)

**Output:** Rows in `field_concept_mappings` table

### 2. Semantic Model (Data Layer)

**Location:** PostgreSQL database

**Tables:**
- `ontology_concepts` - Core concepts (account, revenue, cost, etc.) with cluster tags
- `field_concept_mappings` - Persistent mappings from source fields to ontology concepts
- `persona_profiles` - CFO, CRO, COO, CTO definitions
- `persona_concept_relevance` - Which concepts each persona cares about (0.0-1.0 relevance)

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

### 4. Ingest Pipeline (Stream Processing)

**Location:** `backend/ingest/`

**Components:**
- `ingest_agent.py` - Sidecar that consumes Farm streams and buffers to Redis
- `consumer.py` - Consumes from Redis, performs semantic mapping
- `run_sidecar.py` / `run_consumer.py` - Entry points

**Features:**
- Real-time stream consumption at 10+ TPS
- Drift detection and toxic record blocking
- Self-healing repair (calls Farm Source of Truth API)
- Verification with Farm (closed-loop confirmation)
- MetricsCollector for telemetry broadcasting

## Data Flow

```
┌─────────────────────────────────────┐
│  AAM (Asset & Availability Mgmt)    │
│  - Acquires enterprise connections  │
│  - Routes pipes to DCL              │
└─────────────────────────────────────┘
              ↓ POST /api/ingest/provision
┌─────────────────────────────────────┐
│  Ingest Sidecar (Stream)            │
│  - Consumes from Farm/iPaaS         │
│  - Drift detection                  │
│  - Self-healing repair              │
│  └─> Redis stream: dcl.ingest.raw   │
└─────────────────────────────────────┘
              ↓
┌─────────────────────────────────────┐
│  Ingest Consumer                    │
│  - Semantic mapping                 │
│  - Source registration              │
│  - Telemetry collection             │
└─────────────────────────────────────┘
              ↓
┌─────────────────────────────────────┐
│  Database (Persistent Storage)      │
│  - ontology_concepts                │
│  - field_concept_mappings           │
│  - persona_profiles                 │
│  - persona_concept_relevance        │
└─────────────────────────────────────┘
              ↓
┌─────────────────────────────────────┐
│  DCL Engine (Runtime / Hot)         │
│  - Read stored mappings             │
│  - Filter by persona relevance      │
│  - Build graph snapshot             │
│  - Add explanations                 │
└─────────────────────────────────────┘
```

## Connector Provisioning (Phase 4)

AAM dynamically provisions connectors by calling DCL's provision endpoint.

**Endpoint:** `POST /api/ingest/provision`

```json
{
  "connector_id": "mule_auto_01",
  "source_type": "mulesoft",
  "target_url": "https://farm.url/api/stream/synthetic/mulesoft?chaos=true",
  "policy": { "repair_enabled": true }
}
```

**Handshake Flow:**
1. AAM calls POST /api/ingest/provision to route pipe to DCL
2. Backend stores config in Redis (`dcl.ingest.config`)
3. Sidecar polls every 5 seconds, detects version change
4. Sidecar logs "[HANDSHAKE]" and reconnects to new stream
5. Policy settings (repair_enabled) take effect immediately

## Industrial Dashboard (Phase 5)

Pivots from "Story Mode" (slow, narrated) to "Industrial Mode" (fast, massive, validated).

**Key Changes:**
1. **Velocity** - Removed all artificial delays (no more 1.5-3s latency)
2. **Telemetry** - Live counters replace chat logs
3. **Closed-Loop Verification** - Farm confirms repairs are correct

**Telemetry Endpoint:** `GET /api/ingest/telemetry`

```json
{
  "ts": 1768853167248,
  "metrics": {
    "total_processed": 1523,
    "toxic_blocked": 12,
    "drift_detected": 45,
    "repaired_success": 42,
    "repair_failed": 3,
    "verified_count": 42,
    "verified_failed": 0,
    "tps": 142.5,
    "quality_score": 100.0,
    "repair_rate": 93.3,
    "uptime_seconds": 35.2
  }
}
```

**Dashboard Features:**
- TelemetryRibbon shows: TPS, Processed, Blocked, Healed, Verified, Quality Score
- Terminal Mode narration: monospace, matrix-style green text, auto-scroll
- Updates every 500ms

## Redis Keys

| Key | Type | Purpose |
|-----|------|---------|
| `dcl.logs` | Pub/Sub | Narration message broadcast |
| `dcl.ingest.raw` | Stream | Raw ingested records |
| `dcl.ingest.config` | Hash | Dynamic connector config (AAM) |
| `dcl.telemetry` | String | Live metrics JSON |

## API Endpoints

### Core Pipeline
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/dcl/run` | POST | Execute pipeline (params: data_mode, run_mode, personas) |
| `/api/dcl/graph` | GET | Get current graph snapshot |
| `/api/dcl/narration/{session_id}` | GET | Poll narration messages |

### Ingest Pipeline
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/ingest/provision` | POST | AAM handshake - provision new connector |
| `/api/ingest/telemetry` | GET | Live industrial metrics |

### Batch Endpoints
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/dcl/batch-mapping` | POST | Trigger semantic mapper on sources |

## Node Inventory (24 Nodes)

### L0 - Pipeline (1 node)
- `pipe_demo` / `pipe_farm` - Pipeline entry point

### L1 - Sources (11 nodes)
- `source_salesforce_crm` - Salesforce CRM
- `source_hubspot_crm` - HubSpot CRM
- `source_dynamics_crm` - Microsoft Dynamics CRM
- `source_sap_erp` - SAP ERP
- `source_netsuite_erp` - NetSuite ERP
- `source_mongodb_customer_db` - MongoDB Customer DB
- `source_supabase_app_db` - Supabase App DB
- `source_dw_dim_customer` - Data Warehouse Dim Customer
- `source_discovered_legacy_sql` - Legacy SQL (discovered)
- `source_mulesoft_mock` - MuleSoft ERP Sync
- `source_mulesoft_stream` - MuleSoft Stream (Farm)

### L2 - Ontology (8 concepts)
- `ontology_account` - Account
- `ontology_opportunity` - Opportunity
- `ontology_revenue` - Revenue
- `ontology_cost` - Cost
- `ontology_date` - Date/Timestamp
- `ontology_health` - Health Score
- `ontology_usage` - Usage Metrics
- `ontology_aws_resource` - AWS Resource

### L3 - Business Logic Layer (4 personas)
- `bll_cfo` - CFO persona
- `bll_cro` - CRO persona
- `bll_coo` - COO persona
- `bll_cto` - CTO persona

## Key Benefits

1. **Performance:** Runtime graph building is 10-100x faster (no LLM calls)
2. **Determinism:** Same sources + personas always produce same graph
3. **Adaptability:** Persona views automatically adapt to available data
4. **Explainability:** Ontology nodes include "derived from X fields" explanations
5. **Maintainability:** Config-driven ontology and personas (YAML → DB)
6. **Industrial Scale:** 10+ TPS with real-time telemetry

## Future Enhancements

1. **Full 3-Stage Pipeline:** Add RAG and LLM stages to semantic mapper
2. **Automatic Remapping:** Trigger batch mapping when sources change
3. **Confidence Thresholds:** Filter low-confidence mappings
4. **Conflict Resolution:** Handle multiple concepts matching same field
5. **Cluster-Based Views:** Allow filtering by concept cluster (Finance, Growth, Infra, Ops)
6. **Vendor-Specific Mapping:** Apply platform-specific conventions based on source vendor ID
