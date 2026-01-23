# DCL Engine - RACI Matrix

## Component: DCL (Data Connectivity Layer)

**Last Verified:** January 23, 2026

## System Architecture Context

| Component | Responsibility |
|-----------|---------------|
| **AAM** | Acquires and maintains connections to enterprise integration fabric (iPaaS, API managers, streams, warehouses). Routes pipes to DCL. |
| **DCL** | Ingests schemas and data from routed pipes, performs semantic mapping to unified ontology, serves visualization and telemetry. |
| **Farm** | Provides synthetic data streams, source of truth for verification, chaos injection for testing. |
| **AOD** | Autonomous Operations Director - does NOT communicate directly with DCL. |

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

| Activity/Process | DCL Engine | AAM | Farm | Database |
|-----------------|------------|-----|------|----------|
| **Connection Management** |
| Acquire Enterprise Connections | I | R/A | I | C |
| Maintain iPaaS/API Connections | I | R/A | I | C |
| Route Pipe to DCL | C | R/A | I | I |
| **Schema Ingestion** |
| Demo Schema Loading | R/A | I | I | C |
| Farm Schema Fetching | R | I | A | C |
| Stream Source Loading | R | C | A | C |
| **Source Normalization** |
| Registry Loading | R | I | A | C |
| Alias Resolution | R/A | I | C | I |
| Pattern Matching | R/A | I | I | I |
| Discovery Mode (New Sources) | R | C | C | C |
| **Semantic Mapping** |
| Heuristic Mapping | R/A | I | I | C |
| RAG Enhancement (Prod) | R | I | I | A |
| LLM Refinement (Prod) | R | I | I | C |
| Mapping Persistence | R | I | I | A |
| **Pipeline Execution** |
| Graph Building | R/A | I | I | C |
| Persona Filtering | R/A | I | I | I |
| Narration Broadcasting | R | I | I | A |
| Run Metrics Collection | R/A | I | I | C |
| **Ingest Pipeline** |
| Stream Consumption (Sidecar) | R/A | C | C | C |
| Drift Detection | R/A | I | C | I |
| Self-Healing Repair | R | I | A | C |
| Verification with Farm | C | I | R/A | I |
| Record Buffering | R | I | I | A |
| **Telemetry** |
| Metrics Collection | R/A | I | I | C |
| Telemetry Broadcasting | R | I | I | A |
| TPS/Quality Calculation | R/A | I | C | I |
| **Connector Provisioning** |
| Provision Endpoint | R/A | R | I | C |
| Config Storage | R | C | I | A |
| Dynamic Reconnection | R/A | C | C | C |
| Policy Enforcement | R | A | I | I |
| **Visualization** |
| Sankey Graph Rendering | R/A | I | I | I |
| Monitor Dashboard | R/A | I | I | I |
| Telemetry Ribbon | R/A | I | I | I |
| Terminal Narration | R/A | I | I | I |

## Legend
- **R** = Responsible (does the work)
- **A** = Accountable (final authority/approval)
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
| Redis Telemetry | Publisher | - | - | FUNCTIONAL |
| Redis Logs | Publisher | - | - | FUNCTIONAL |
| PostgreSQL | Consumer | - | Provider | FUNCTIONAL |

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
