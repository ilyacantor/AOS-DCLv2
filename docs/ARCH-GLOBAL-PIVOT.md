# AOS Platform - Global Architecture Pivot

**Last Updated:** January 23, 2026  
**Version:** 1.0 - Self-Healing Mesh & Zero-Trust Vision

## Executive Summary

The AOS platform is being refactored to align with the "Self-Healing Mesh" and "Zero-Trust" vision. This document defines the new component boundaries and responsibilities.

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

### Current State (To Be Removed)
- `backend/ingest/ingest_agent.py` - Raw data stream consumption
- `backend/ingest/consumer.py` - Raw data processing
- Redis stream `dcl.ingest.raw` - Raw data buffering

### Target State (Metadata-Only)
- Schema metadata ingestion only
- Field-to-concept mapping
- Graph building from mappings
- No raw payload handling

### Migration Steps
1. Move stream consumption to AAM
2. Move self-healing repair to AAM
3. DCL receives only schema metadata from AAM
4. Remove raw data Redis streams from DCL
5. Update APIs to metadata-only contracts

## Compliance Checklist

| Requirement | Owner | Status |
|-------------|-------|--------|
| AAM owns all self-healing | AAM | PENDING |
| FARM is oracle-only | FARM | PENDING |
| DCL handles metadata-only | DCL | PENDING |
| AOA owns execution | AOA | PENDING |
| Zero-trust data boundaries | ALL | PENDING |
