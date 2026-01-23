# DCL Engine - RACI Matrix

## Component: DCL (Data Connectivity Layer)

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

| Integration | DCL Role | Partner Role |
|-------------|----------|--------------|
| Farm Registry API | Consumer | Provider |
| Farm Stream API | Consumer | Provider |
| Farm Source of Truth API | Consumer | Provider |
| Farm Verify API | Consumer | Provider |
| AOD Provision API | Provider | Consumer |
| Redis Telemetry | Publisher | - |
| Redis Logs | Publisher | - |
| PostgreSQL | Consumer | Provider |

## Notes
- DCL operates in Dev (heuristic-only) or Prod (LLM/RAG-enabled) modes
- Farm provides Source of Truth for drift repair and verification
- AOD provisions connectors dynamically via handshake pattern
- All telemetry flows through Redis for real-time dashboard updates
