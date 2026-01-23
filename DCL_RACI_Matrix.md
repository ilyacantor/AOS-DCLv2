# DCL Engine - RACI Matrix

## Component: DCL (Data Connectivity Layer)

**Last Updated:** January 23, 2026  
**Architecture Version:** Self-Healing Mesh & Zero-Trust

## Global Architecture Pivot

This RACI reflects the refactored AOS platform boundaries:

| Component | Role | Boundary |
|-----------|------|----------|
| **AAM** | The Mesh | Owns Self-Healing, Repair, Raw Data Handling |
| **DCL** | The Brain | Metadata-Only (schemas, mappings, ontology) |
| **FARM** | The Verifier | Test Oracle Only (verification, synthetic data) |
| **AOA** | The Orchestrator | Owns Execution, Infrastructure |

## DCL Scope (Metadata-Only)

DCL handles ONLY metadata - never raw payload data:
- Schema structures (field names, types)
- Semantic mappings (field → concept)
- Ontology concepts
- Persona relevance
- Graph visualization

## Feature Status Summary

| Feature | Status | Notes |
|---------|--------|-------|
| Demo Schema Loading | FUNCTIONAL | Metadata-only |
| Farm Schema Fetching | FUNCTIONAL | Metadata-only |
| Source Normalization | FUNCTIONAL | Metadata-only |
| Heuristic Mapping | FUNCTIONAL | 127+ mappings |
| RAG Enhancement (Prod) | FUNCTIONAL | Pinecone integration |
| LLM Refinement (Prod) | FUNCTIONAL | Gemini/OpenAI |
| Graph Building | FUNCTIONAL | 4-layer Sankey |
| Persona Filtering | FUNCTIONAL | CFO/CRO/COO/CTO |
| Narration Service | FUNCTIONAL | Status updates |
| Sankey Visualization | FUNCTIONAL | Interactive |
| ~~Raw Data Buffering~~ | DEPRECATED | Moving to AAM |
| ~~Stream Consumption~~ | DEPRECATED | Moving to AAM |
| ~~Self-Healing Repair~~ | DEPRECATED | Moving to AAM |

## RACI Matrix

### Connection & Pipe Management

| Activity | AAM | DCL | FARM | AOA |
|----------|-----|-----|------|-----|
| Acquire Enterprise Connections | A/R | I | I | C |
| Maintain iPaaS/API Connections | A/R | I | I | C |
| Route Pipe to DCL | A/R | C | I | C |
| Connection Health Monitoring | A/R | I | I | C |

### Data Handling (Zero-Trust Boundary)

| Activity | AAM | DCL | FARM | AOA |
|----------|-----|-----|------|-----|
| Raw Data Stream Consumption | A/R | I | I | C |
| Drift Detection | A/R | I | C | C |
| Self-Healing Repair | A/R | I | C | C |
| Payload Sanitization | A/R | I | I | C |
| Metadata Extraction | A/R | C | I | I |

### Schema & Semantic Processing (DCL Domain)

| Activity | AAM | DCL | FARM | AOA |
|----------|-----|-----|------|-----|
| Schema Metadata Ingestion | C | A/R | I | I |
| Source Normalization | I | A/R | C | I |
| Heuristic Mapping | I | A/R | I | I |
| RAG Enhancement | I | A/R | I | I |
| LLM Refinement | I | A/R | I | I |
| Mapping Persistence | I | A/R | I | I |

### Pipeline Execution

| Activity | AAM | DCL | FARM | AOA |
|----------|-----|-----|------|-----|
| Graph Building | I | A/R | I | I |
| Persona Filtering | I | A/R | I | I |
| Narration Broadcasting | I | A/R | I | C |
| Metrics Collection | C | A/R | I | C |

### Verification (FARM Oracle)

| Activity | AAM | DCL | FARM | AOA |
|----------|-----|-----|------|-----|
| Synthetic Data Generation | I | I | A/R | I |
| Chaos Injection | I | I | A/R | C |
| Source of Truth Lookup | C | I | A/R | I |
| Verification Response | C | I | A/R | C |

### Orchestration & Infrastructure (AOA Domain)

| Activity | AAM | DCL | FARM | AOA |
|----------|-----|-----|------|-----|
| Workflow Orchestration | C | I | I | A/R |
| Infrastructure Management | C | I | I | A/R |
| Pipeline Scheduling | C | C | I | A/R |
| Cross-Component Coordination | C | C | C | A/R |

### Visualization (DCL Domain)

| Activity | AAM | DCL | FARM | AOA |
|----------|-----|-----|------|-----|
| Sankey Graph Rendering | I | A/R | I | I |
| Monitor Dashboard | I | A/R | I | I |
| Telemetry Display | I | A/R | I | I |
| Terminal Narration | I | A/R | I | I |

## Legend

- **R** = Responsible (does the work)
- **A** = Accountable (final authority - exactly one per row)
- **C** = Consulted (provides input)
- **I** = Informed (kept updated)

## Key Integration Points

| Integration | DCL Role | Partner | Partner Role | Status |
|-------------|----------|---------|--------------|--------|
| Schema Metadata | Consumer | AAM | Provider | PENDING PIVOT |
| Connector Provisioning | Provider | AAM | Consumer | FUNCTIONAL |
| Verification Oracle | Consumer | FARM | Provider | FUNCTIONAL |
| Execution Commands | Consumer | AOA | Provider | PENDING |

## Migration Status

| Change | From | To | Status |
|--------|------|-----|--------|
| Raw data buffering | DCL | AAM | PENDING |
| Stream consumption | DCL | AAM | PENDING |
| Self-healing repair | DCL | AAM | PENDING |
| Infrastructure ops | FARM | AOA | PENDING |

## Notes

- DCL must NOT handle raw payload data (security risk)
- AAM owns the Zero-Trust boundary for raw data
- FARM is strictly a verification oracle
- AOA coordinates execution across components
