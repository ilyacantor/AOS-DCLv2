# AOS Platform - Global Architecture Pivot

**Last Updated:** January 23, 2026  
**Version:** 2.0 - Fabric Plane Mesh Architecture

## Executive Summary

The AOS platform is being refactored to align with the "Fabric Plane Mesh" architecture. 

**CRITICAL CONSTRAINT:** AAM (The Mesh) does NOT connect directly to individual SaaS applications (e.g., Salesforce, HubSpot) unless running in "Preset 6 (Scrappy)" mode. AAM connects ONLY to "Fabric Planes" that aggregate data.

## The 4 Fabric Planes

| Plane | Examples | Purpose |
|-------|----------|---------|
| **iPaaS** | Workato, MuleSoft | Control plane for integration flows |
| **API_GATEWAY** | Kong, Apigee | Direct managed API access |
| **EVENT_BUS** | Kafka, EventBridge | Streaming backbone |
| **DATA_WAREHOUSE** | Snowflake, BigQuery | Source of Truth storage |

## Pointer Buffering Strategy (DCL)

**Previous:** "Don't buffer PII" (Generic)  
**New Reality:** The Fabric Plane allows us to be specific. We don't buffer data or metadata - we buffer **Pointers (Offsets)** only.

| Fabric Plane | Pointer Format |
|--------------|----------------|
| Kafka | `{ topic, partition, offset }` |
| Snowflake | `{ table, stream_id, row_cursor }` |
| EventBridge | `{ event_bus_name, event_id }` |
| BigQuery | `{ project, dataset, table, read_session }` |

**Just-in-Time Fetching:** The ingestion worker fetches the actual payload from the Fabric Plane ONLY at the exact moment the semantic mapper requests it. This leverages Fabric durability and guarantees Zero-Trust compliance.

## Architectural Drift (Previous State)

| Component | Previous Behavior | Issue |
|-----------|-------------------|-------|
| AAM | Passive connection manager | Did not own repair/healing |
| FARM | God Object handling Ops, SoT, verification | Overloaded, boundary violation |
| DCL | Buffering raw data in Redis | Security risk, scope creep |
| AOA | Underutilized | Infrastructure duties unassigned |

## New Component Boundaries

### AAM - The Mesh (Self-Healing Owner)

**Role:** Asset & Availability Management  
**New Responsibility:** Owns "Self-Healing" and "Repair"

| Capability | Status |
|------------|--------|
| Acquire enterprise connections | RETAINED |
| Maintain iPaaS/API connections | RETAINED |
| Route pipes to DCL | RETAINED |
| **Self-healing repair** | **NEW - MOVED FROM DCL** |
| **Drift remediation** | **NEW - MOVED FROM DCL** |
| **Connection health monitoring** | **NEW** |

**Rationale:** As "The Mesh," AAM is the natural owner of infrastructure health. Self-healing belongs at the connection layer, not the metadata layer.

### FARM - The Verifier (Test Oracle Only)

**Role:** Synthetic Data & Test Orchestration  
**New Responsibility:** Strictly a "Test Oracle"

| Capability | Status |
|------------|--------|
| Synthetic data generation | RETAINED |
| Chaos injection | RETAINED |
| Source of Truth (for verification) | RETAINED |
| **Verification response (pass/fail)** | RETAINED |
| ~~Stream operations~~ | **REMOVED - TO AOA** |
| ~~Infrastructure duties~~ | **REMOVED - TO AOA** |

**Rationale:** FARM must not be a God Object. Its sole purpose is to be an oracle that answers "Is this data correct?" - nothing more.

### DCL - The Brain (Metadata-Only)

**Role:** Data Connectivity Layer  
**New Responsibility:** Metadata-Only Processing

| Capability | Status |
|------------|--------|
| Schema ingestion | RETAINED |
| Semantic mapping (ontology) | RETAINED |
| Graph building & visualization | RETAINED |
| Persona-driven filtering | RETAINED |
| ~~Raw data buffering~~ | **REMOVED - SECURITY RISK** |
| ~~Stream consumption~~ | **REMOVED - TO AAM** |
| ~~Self-healing repair~~ | **REMOVED - TO AAM** |

**Rationale:** DCL handling raw payload data is a security risk. DCL should only see metadata: schemas, field names, types, mappings. Actual data flows through AAM.

### AOA - The Orchestrator (Execution Owner)

**Role:** Autonomous Operations Agent  
**New Responsibility:** Owns "Execution" and Infrastructure

| Capability | Status |
|------------|--------|
| **Workflow orchestration** | **NEW - FROM FARM** |
| **Infrastructure management** | **NEW - FROM FARM** |
| **Pipeline scheduling** | **NEW** |
| **Cross-component coordination** | **NEW** |

**Rationale:** AOA must pick up the infrastructure duties previously overloaded onto FARM.

## Revised RACI Summary

| Function | AAM | DCL | FARM | AOA |
|----------|-----|-----|------|-----|
| Connection Acquisition | A/R | I | I | C |
| Pipe Routing | A/R | C | I | C |
| **Self-Healing Repair** | **A/R** | I | C | C |
| Schema Ingestion | C | A/R | I | I |
| Semantic Mapping | I | A/R | I | I |
| Graph Visualization | I | A/R | I | I |
| Verification (Oracle) | C | C | A/R | I |
| Synthetic Data Gen | I | I | A/R | I |
| **Execution/Orchestration** | C | I | I | **A/R** |
| **Infrastructure Ops** | C | I | I | **A/R** |

## Security Model: Zero-Trust

### Data Flow Boundaries

```
┌─────────────────────────────────────────────────────────────────┐
│                        ENTERPRISE FABRIC                         │
│  (iPaaS, API Managers, Streams, Warehouses)                      │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  AAM - The Mesh                                                  │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │ RAW DATA ZONE (Zero-Trust Boundary)                         ││
│  │ - Stream consumption                                         ││
│  │ - Drift detection                                            ││
│  │ - Self-healing repair                                        ││
│  │ - Payload sanitization                                       ││
│  └─────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘
                              │
                    METADATA ONLY (schemas, types, mappings)
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  DCL - The Brain                                                 │
│  - Schema analysis                                               │
│  - Semantic mapping                                              │
│  - Ontology unification                                          │
│  - Graph visualization                                           │
│  - NO RAW DATA                                                   │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  FARM - The Verifier                                             │
│  - Verification oracle only                                      │
│  - Synthetic data generation                                     │
│  - Chaos injection                                               │
│  - NO OPERATIONS                                                 │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  AOA - The Orchestrator                                          │
│  - Workflow execution                                            │
│  - Infrastructure management                                     │
│  - Cross-component coordination                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Migration Path for DCL

### Removed (January 24, 2026)
The following ingest components have been fully removed from DCL:
- ~~`backend/ingest/ingest_agent.py`~~ - DELETED
- ~~`backend/ingest/consumer.py`~~ - DELETED
- ~~`backend/ingest/run_sidecar.py`~~ - DELETED
- ~~`backend/ingest/run_consumer.py`~~ - DELETED
- ~~Redis stream `dcl.ingest.raw`~~ - REMOVED
- ~~`/api/ingest/*` endpoints~~ - Return HTTP 410 Gone with `{"error": "MOVED_TO_AAM"}`

### Current State (Metadata-Only) ✓
- Schema metadata ingestion only
- Field-to-concept mapping
- Graph building from mappings
- No raw payload handling
- Pointer buffering (offsets/cursors only)

### Migration Status: COMPLETE
1. ✓ Stream consumption moved to AAM
2. ✓ Self-healing repair moved to AAM
3. ✓ DCL receives only schema metadata from AAM
4. ✓ Raw data Redis streams removed from DCL
5. ✓ APIs updated to metadata-only contracts

## Compliance Checklist

| Requirement | Owner | Status |
|-------------|-------|--------|
| AAM owns all self-healing | AAM | PENDING |
| FARM is oracle-only | FARM | PENDING |
| DCL handles metadata-only | DCL | **COMPLETE** |
| AOA owns execution | AOA | PENDING |
| Zero-trust data boundaries | ALL | IN PROGRESS |
