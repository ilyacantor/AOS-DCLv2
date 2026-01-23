# DCL Engine - RACI Matrix

## Component: DCL (Data Connectivity Layer)

**Last Verified:** January 23, 2026

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
| AOD Handshake | FUNCTIONAL | Dynamic provisioning works |
| Sankey Visualization | FUNCTIONAL | Interactive 4-layer graph |
| Telemetry Ribbon | FUNCTIONAL | Live counters in Farm mode |
| Terminal Narration | FUNCTIONAL | Matrix-style auto-scroll |

## RACI Matrix

| Activity/Process | DCL Engine | Farm | AOD | User/Operator | Database |
|-----------------|------------|------|-----|---------------|----------|
| **Schema Ingestion** |
| Demo Schema Loading | R/A | I | I | I | C |
| Farm Schema Fetching | R | A | I | I | C |
| Stream Source Loading | R | C | I | I | A |
| **Source Normalization** |
| Registry Loading | R | A | I | I | C |
| Alias Resolution | R/A | C | I | I | I |
| Pattern Matching | R/A | I | I | I | I |
| Discovery Mode (New Sources) | R | C | I | A | C |
| **Semantic Mapping** |
| Heuristic Mapping | R/A | I | I | I | C |
| RAG Enhancement (Prod) | R | I | I | I | A |
| LLM Refinement (Prod) | R | I | I | A | C |
| Mapping Persistence | R | I | I | I | A |
| **Pipeline Execution** |
| Graph Building | R/A | I | I | I | C |
| Persona Filtering | R/A | I | I | C | I |
| Narration Broadcasting | R | I | I | I | A |
| Run Metrics Collection | R/A | I | I | I | C |
| **Ingest Pipeline** |
| Stream Consumption (Sidecar) | R/A | C | I | I | C |
| Drift Detection | R/A | C | I | I | I |
| Self-Healing Repair | R | A | I | I | C |
| Verification with Farm | C | R/A | I | I | I |
| Record Buffering | R | I | I | I | A |
| **Telemetry** |
| Metrics Collection | R/A | I | I | I | C |
| Telemetry Broadcasting | R | I | I | I | A |
| TPS/Quality Calculation | R/A | C | I | I | I |
| **Phase 4: Handshake** |
| Provision Endpoint | R/A | I | R | I | C |
| Config Storage | R | I | A | I | A |
| Dynamic Reconnection | R/A | C | C | I | C |
| Policy Enforcement | R | I | A | I | I |
| **Visualization** |
| Sankey Graph Rendering | R/A | I | I | C | I |
| Monitor Dashboard | R/A | I | I | C | I |
| Telemetry Ribbon | R/A | I | I | C | I |
| Terminal Narration | R/A | I | I | C | I |

## Legend
- **R** = Responsible (does the work)
- **A** = Accountable (final authority/approval)
- **C** = Consulted (provides input)
- **I** = Informed (kept updated)

## Key Integration Points

| Integration | DCL Role | Partner Role | Status |
|-------------|----------|--------------|--------|
| Farm Registry API | Consumer | Provider | FUNCTIONAL |
| Farm Stream API | Consumer | Provider | FUNCTIONAL |
| Farm Source of Truth API | Consumer | Provider | PARTIAL (503) |
| Farm Verify API | Consumer | Provider | PARTIAL (503) |
| AOD Provision API | Provider | Consumer | FUNCTIONAL |
| Redis Telemetry | Publisher | - | FUNCTIONAL |
| Redis Logs | Publisher | - | FUNCTIONAL |
| PostgreSQL | Consumer | Provider | FUNCTIONAL |

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
- DCL operates in Dev (heuristic-only) or Prod (LLM/RAG-enabled) modes
- Farm provides Source of Truth for drift repair and verification
- AOD provisions connectors dynamically via handshake pattern
- All telemetry flows through Redis for real-time dashboard updates
- Self-healing and verification depend on Farm's SoT API being available (currently returning 503)
