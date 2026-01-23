| Category | Capability | AOD | AAM | DCL | AOA | FARM | Architectural Notes |
|----------|------------|-----|-----|-----|-----|------|---------------------|
| **DISCOVERY (AOD)** | | | | | | | *The Senses* |
| **Fabric Detection** | **Fabric Plane Identification** (MuleSoft, Kafka, Snowflake) | **A/R** | C | | | | *Detects the Backbone, not just endpoints* |
| | **Enterprise Preset Inference** (Scrappy vs Platform) | **A/R** | C | | | | *Classifies Org Architecture* |
| | Connection Routing Logic (Set `connected_via_plane`) | **A/R** | I | | | | *Context for AAM Handoff* |
| **Asset Scan** | Asset Discovery (Legacy Endpoints) | A/R | | | | | *Legacy Fallback* |
| | **Policy Manifest Export** (Governance Rules) | **A/R** | C | | | | *Export rules for AAM to enforce* |
| **Handoff** | ConnectionCandidate Export | A/R | I | | | | *Sends Target + Preset to AAM* |
| **MESH (AAM)** | | | | | | | *The Fabric* |
| **Fabric Mgmt** | **Fabric Plane Connection** (The Backbone) | | **A/R** | | | I | *Connects to Planes, not Apps* |
| | **Preset Config Loading** (6, 8, 9, 11) | | **A/R** | | | I | *Configures Mesh behavior* |
| | Routing Policy Enforcement | | A/R | | | I | *Enforces "Block Direct Access"* |
| **Adapters** | Adapter Factory Resolution | | A/R | | | I | *Instantiates Strategy (Gateway vs Bus)* |
| **Self-Healing** | **Connection/Fabric Drift Detection** | | **A/R** | | | I | *Detects lost connectivity to Plane* |
| | **Execute Self-Heal** (Restart Consumers) | | **A/R** | | | I | *Restarts Consumers/Reconnects* |
| **Governance** | PII Redaction (Edge) | | **A/R** | | | I | *Redacts before data enters DCL* |
| **CONNECTIVITY (DCL)** | | | | | | | *The Brain* |
| **Ingestion** | **Fabric Pointer Buffering** (Zero-Trust) | | | **A/R** | | I | *buffers Pointers, NOT Payloads* |
| | Just-in-Time Payload Fetching | | C | **R** | | | *Fetch only on Semantic Map request* |
| | Schema Drift Detection | | C | A/R | | C | *Field changes (not connection)* |
| **Contract** | **Downstream Consumer Protocol** (BLL Stub) | | | **A/R** | | C | *Interface for BLL Agents* |
| **Visualization** | **Topology API Exposure** | | I | A/R | | | *Backend for Graph UI* |
| **ORCHESTRATION (AOA)** | | | | | | | *The Hands (Synthesized)* |
| **Action** | **Fabric Action Routing** | | C | | **A/R** | I | *Routes "Write" intent to Fabric Plane* |
| | Transaction Execution | | R | | **A/R** | I | *Executes via AAM Adapters* |
| **Runtime** | **Worker Pool Management** (Infra) | | | | **A/R** | I | *Moved from FARM (Ops)* |
| | Task Queue Management | | | | **A/R** | I | *Moved from FARM (Ops)* |
| **Security** | **Context Sanitization** | | | | **A/R** | C | *Security layer before Agent Logic* |
| **VERIFICATION (FARM)** | | | | | | | *The Judge* |
| **Truth** | **Ground Truth Validation** | C | C | C | | **A/R** | *The "Test Oracle"* |
| | End-to-End Injection Tests | | I | I | | **A/R** | *Injects at AAM, verifies at DCL* |
| | Accuracy Measurement | R | R | | | **A/R** | |
