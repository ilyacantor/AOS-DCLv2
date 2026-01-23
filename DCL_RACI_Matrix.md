# DCL Engine - RACI Matrix

## Component: DCL (Data Connectivity Layer)

**Last Updated:** January 23, 2026

## System Architecture Context

| Component | Responsibility |
|-----------|---------------|
| **AAM** | Acquires and maintains connections to enterprise integration fabric (iPaaS, API managers, streams, warehouses). Routes pipes to DCL. |
| **DCL** | Ingests schemas and data from routed pipes, performs semantic mapping to unified ontology, serves visualization and telemetry. |
| **Farm** | Provides synthetic data streams, source of truth for verification, chaos injection for testing. |

## Feature Status Summary

| Feature | Status | Notes |
|---------|--------|-------|
| Demo Schema Loading | FUNCTIONAL | 18 nodes, 97 links |
| Farm Schema Fetching | FUNCTIONAL | 21 nodes, 112 links |
| Stream Source Loading | FUNCTIONAL | Real-time from Farm |
| Source Normalization | FUNCTIONAL | Registry from Farm API |
| Heuristic Mapping | FUNCTIONAL | 127+ mappings created |
| RAG Enhancement (Prod) | FUNCTIONAL | Pinecone integration |
| LLM Refinement (Prod) | FUNCTIONAL | Gemini/OpenAI integration |
| Graph Building | FUNCTIONAL | 4-layer Sankey |
| Persona Filtering | FUNCTIONAL | CFO/CRO/COO/CTO |
| Narration Service | FUNCTIONAL | 100+ messages |
| Ingest Sidecar | FUNCTIONAL | 10.4 TPS |
| Drift Detection | FUNCTIONAL | 30 toxic blocked |
| Self-Healing Repair | PARTIAL | Code exists, Farm API returns 503 |
| Verification with Farm | PARTIAL | Code exists, depends on repair |
| Telemetry Broadcasting | FUNCTIONAL | Every 0.5s to Redis |
| Connector Provisioning | FUNCTIONAL | Dynamic provisioning via AAM |
| Sankey Visualization | FUNCTIONAL | Interactive 4-layer graph |
| Telemetry Ribbon | FUNCTIONAL | Live counters in Farm mode |
| Terminal Narration | FUNCTIONAL | Matrix-style auto-scroll |

## RACI Matrix

| Activity/Process | DCL | AAM | Farm |
|-----------------|-----|-----|------|
| **Connection Management** |
| Acquire Enterprise Connections | I | A/R | I |
| Maintain iPaaS/API Connections | I | A/R | I |
| Route Pipe to DCL | C | A/R | I |
| **Schema Ingestion** |
| Demo Schema Loading | A/R | I | I |
| Farm Schema Fetching | R | I | A |
| Stream Source Loading | R | C | A |
| **Source Normalization** |
| Registry Loading | R | I | A |
| Alias Resolution | A/R | I | C |
| Pattern Matching | A/R | I | I |
| Discovery Mode (New Sources) | R | C | A |
| **Semantic Mapping** |
| Heuristic Mapping | A/R | I | I |
| RAG Enhancement (Prod) | A/R | I | I |
| LLM Refinement (Prod) | A/R | I | I |
| Mapping Persistence | A/R | I | I |
| **Pipeline Execution** |
| Graph Building | A/R | I | I |
| Persona Filtering | A/R | I | I |
| Narration Broadcasting | A/R | I | I |
| Run Metrics Collection | A/R | I | I |
| **Ingest Pipeline** |
| Stream Consumption (Sidecar) | A/R | C | C |
| Drift Detection | A/R | I | C |
| Self-Healing Repair | R | I | A |
| Verification with Farm | C | I | A/R |
| Record Buffering | A/R | I | I |
| **Telemetry** |
| Metrics Collection | A/R | I | I |
| Telemetry Broadcasting | A/R | I | I |
| TPS/Quality Calculation | A/R | I | C |
| **Connector Provisioning** |
| Provision Endpoint | R | A | I |
| Config Storage | A/R | C | I |
| Dynamic Reconnection | A/R | C | C |
| Policy Enforcement | R | A | I |
| **Visualization** |
| Sankey Graph Rendering | A/R | I | I |
| Monitor Dashboard | A/R | I | I |
| Telemetry Ribbon | A/R | I | I |
| Terminal Narration | A/R | I | I |

## Legend
- **R** = Responsible (does the work)
- **A** = Accountable (final authority/approval - exactly one per row)
- **C** = Consulted (provides input)
- **I** = Informed (kept updated)

## Key Integration Points

| Integration | DCL Role | Partner | Partner Role | Status |
|-------------|----------|---------|--------------|--------|
| Pipe Routing | Consumer | AAM | Provider | FUNCTIONAL |
| Connector Provisioning | Provider | AAM | Consumer | FUNCTIONAL |
| Farm Registry API | Consumer | Farm | Provider | FUNCTIONAL |
| Farm Stream API | Consumer | Farm | Provider | FUNCTIONAL |
| Farm Source of Truth API | Consumer | Farm | Provider | PARTIAL (503) |
| Farm Verify API | Consumer | Farm | Provider | PARTIAL (503) |

## Verified Metrics (Live)

| Metric | Value |
|--------|-------|
| Records Processed | 1,950+ |
| TPS | 10.4 |
| Toxic Blocked | 30 |
| Drift Detected | 19 |
| Sources Loaded | 11 (Demo) + 5 (Farm) |
| Mappings Created | 127+ |
| Ontology Concepts | 8 |
| Personas | 4 (CFO, CRO, COO, CTO) |

## Notes
- AAM acquires and maintains connections to enterprise integration fabric
- AAM routes pipes to DCL for schema ingestion and semantic mapping
- DCL operates in Dev (heuristic-only) or Prod (LLM/RAG-enabled) modes
- Farm provides Source of Truth for drift repair and verification
- Self-healing and verification depend on Farm's SoT API being available
