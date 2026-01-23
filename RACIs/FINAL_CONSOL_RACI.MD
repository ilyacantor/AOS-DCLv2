Category	Capability	AOD	AAM	DCL	AOA	FARM	Notes
Action	Fabric Action Routing		C		A/R	I	Routes "Write" intent to Fabric Plane
Action	Transaction Execution		R		A/R	I	Executes via AAM Adapters
Adapters	API Gateway Adapter (Kong, Apigee)		A/R			I	Managed API Proxy
Adapters	API Gateway Connection (Kong, Apigee)		A/R			I	Proxy/REST modality
Adapters	Adapter Factory Resolution		A/R			I	Instantiates Strategy (Gateway vs Bus
Adapters	Data Warehouse Connection (Snowflake, BigQuery)		A/R			I	JDBC/Bulk Read modality
Adapters	Event Bus Adapter (Kafka, EventBridge)		A/R			I	Streaming Consumer
Adapters	Event Bus Connection (Kafka, EventBridge)		A/R			I	Streaming Consumer modality
Adapters	IPAAS Connection (Workato, MuleSoft)		A/R			I	Webhooks/Signals modality
Adapters	Pipe Discovery from Plane		A/R			I	Inventory reusable data pipes
Adapters	Plane Health Check		A/R			I	Monitor adapter connectivity
Adapters	Warehouse Adapter (Snowflake, BigQuery)		A/R			I	JDBC Read
Adapters	iPaaS Adapter (Workato, MuleSoft)		A/R			I	Webhooks/Signals
Asset Scan	Policy Manifest Export (Governance Rules)	A/R	C				Export rules for AAM to enforce
Asset Scan	Asset Discovery (Legacy Endpoints)	A/R					Legacy Fallback
Candidate Intake	Candidate Defer		A/R			I	Defer with reason
Candidate Intake	Candidate Ingestion	R	A			I	Receives from AOD
Candidate Intake	Candidate Match to Pipe		A/R			I	Link candidate to existing pipe
Candidate Intake	Candidate Triage		A/R			I	Status management
Collector Operations	Collector Registration		A/R			I	Register collection strategies
Collector Operations	Collector Run Execution		A/R			I	Execute with tracking
Collector Operations	Observation Storage		A/R			I	Raw collector output
Contract	Downstream Consumer Protocol (BLL Stub)			A/R		C	Interface for BLL Agents
Drift Detection	Drift Acknowledgement		A/R			I	Operator action
Drift Detection	Fabric Drift Detection		A/R			I	Connectivity drift
Drift Detection	Schema Drift Detection		A/R			C	Field changes (not connection
Drift Detection	Schema Hash Computation		A/R			C	Data structure drift
Export	DCL Pipe Export		A/R	I		I	DeclaredPipes for DCL consumption
Export	Pipe Snapshot Export		A/R			I	Point-in-time export
Fabric Detection	Enterprise Preset Inference (Scrappy vs Platform)	A/R	C				Classifies Org Architecture
Fabric Detection	Fabric Plane Identification (MuleSoft, Kafka, Snowflake, Kong)	A/R	C				Detects the Backbone, not just endpoints
Fabric Detection	Connection Routing Logic (Set `connected_via_plane`)	A/R	I				Context for AAM Handoff
Fabric Mgmt	Fabric Plane Connection (The Backbone)		A/R			I	Connects to Planes, not Apps
Fabric Mgmt	Preset Config Loading (6, 8, 9, 10, 11)		A/R			I	Configures Mesh behavior
Fabric Mgmt	Preset Config Loading (6, 8, 9, 11)		A/R			I	Configures Mesh behavior
Fabric Mgmt	Routing Policy Enforcement		A/R			I	Enforces "Block Direct Access"
Governance	PII Redaction (Edge)		A/R			I	Redacts before data enters DCL
Governance	Auth Policy Enforcement		A/R			I	Plane-level auth validation
Governance	Block Direct App Access		A/R			I	Enforced for non-Scrappy presets
Governance	PII Redaction (Edge)		A/R			I	Redacts before data enters DCL
Governance	Rate Limit Enforcement		A/R			I	Plane-level rate limiting
Handoff	ConnectionCandidate Export	A/R	I				Sends Target + Preset to AAM
Ingestion	Fabric Pointer Buffering (Zero-Trust)			A/R		I	buffers Pointers, NOT Payloads
Ingestion	Just-in-Time Payload Fetching		C	R			Fetch only on Semantic Map request
Ingestion	Schema Drift Detection		C	A/R		C	Field changes (not connection
Pipe Inference	DeclaredPipe Creation		A/R	I		I	Publish to DCL
Pipe Inference	Fabric Plane Inference		A/R			I	WHERE pipes live
Pipe Inference	Modality Inference		A/R			I	HOW pipes are accessed
Pipe Inference	Transport Kind Inference		A/R			I	API, EVENT_STREAM, TABLE, FILE
Runtime	Worker Pool Management (Infra)				A/R	I	Moved from FARM (Ops
Runtime	Task Queue Management				A/R	I	Moved from FARM (Ops
Security	Context Sanitization				A/R	C	Security layer before Agent Logic
Self-Healing	Connection/Fabric Drift Detection		A/R			I	Detects lost connectivity to Plane
Self-Healing	Execute Self-Heal (Restart Consumers)		A/R			I	Restarts Consumers/Reconnects
Self-Healing	Consumer Lag Detection		A/R			I	Event Bus specific drift
Self-Healing	Heal History Logging		A/R			I	Audit trail of repairs
Self-Healing	Warehouse Suspend Detection		A/R			I	Warehouse specific drift
Truth	Ground Truth Validation	C	C	C		A/R	The "Test Oracle"
Truth	Accuracy Measurement	R	R			A/R	
Truth	End-to-End Injection Tests		I	I		A/R	Injects at AAM, verifies at DCL
Verification	End-to-End Injection Tests		I	I		A/R	Injects at AAM, verifies at DCL
Verification	Ground Truth Validation	C	C	C		A/R	FARM is Test Oracle
Visualization	Topology API Exposure		I	A/R			Backend for Graph UI
A2A Protocol	Agent discovery				A/R	I	
A2A Protocol	Context sharing				A/R	I	
A2A Protocol	Task delegation				A/R	C	
AAM Handoff	ConnectionCandidate export	A/R					
Agent Lifecycle	Agent health monitoring				A/R	C	
Agent Lifecycle	Agent registration & configuration				A/R	I	
Agent Lifecycle	Agent version management				A/R	I	
Agent Registry	Agent discovery & inventory	C	C	I	A/R	I	Agents as first-class assets
Agent Registry	Agent metadata & ownership	I	I	C	A/R	I	Domain, owner, trust
Approval Workflows	Approval request generation				A/R	I	
Approval Workflows	Human-in-the-loop routing				A/R	I	
Approval Workflows	Override tracking				A/R	I	
Candidate Management	Defer Candidate		A/R			I	
Candidate Management	Match Candidate to Pipe		A/R			I	
Candidate Management	Receive ConnectionCandidate		A/R			I	
Candidate Management	Triage Candidates		A/R			I	
Capabilities	Capability contract registration	I	I	C	A/R	I	Declared agent abilities
Capabilities	Capability drift detection	C	C	C	A/R	I	Observed vs declared behavior
Classification & Governance	Governance status determination	A/R				C	
Classification & Governance	IdP governance policy enforcement	A/R				C	
Classification & Governance	Shadow IT classification	A/R				C	
Classification & Governance	Vendor governance propagation	A/R					
Classification & Governance	Zombie asset detection	A/R				C	
Collector Operations	Generate Observations		A/R			C	
Collector Operations	Register Collectors		A/R			I	
Collector Operations	Run Collectors		A/R			C	
Collector Operations	Track Collector Runs		A/R			I	
Connector Provisioning	Config Storage			A/R		I	
Connector Provisioning	Dynamic Reconnection			A/R		C	
Coordination	Arbitration & conflict resolution	I	I	C	A/R	I	Deterministic outcomes
Coordination	Multi-agent workflow orchestration	I	C	C	A/R	I	Fan-in / fan-out
Discovery & Cataloging	Asset discovery (find data sources)	A/R					
Discovery & Cataloging	Domain canonicalization (alias collapsing)	A/R				C	
Discovery & Cataloging	Source catalog management	A/R					
Drift Detection	Acknowledge Drift		A/R			I	
Drift Detection	Detect Contract Drift		A/R			C	
Drift Detection	Detect Freshness Drift		A/R			C	
Drift Detection	Detect Schema Drift		A/R			C	
Drift Detection	Suppress Drift		A/R			I	
Economics	Budget enforcement	I	C	I	A/R	I	Prevent runaway spend
Economics	Cost attribution per agent	I	C	C	A/R	I	Tokens & infra
Execution	Fabric action routing	I	C	I	A/R	I	Writes routed via AAM
Execution	Transaction execution	I	R	I	A/R	I	Execution via adapters
Findings & Issues	CMDB gap detection	A/R				C	
Findings & Issues	Data conflict detection	A/R				C	
Findings & Issues	Finance gap detection	A/R				C	
Findings & Issues	Identity gap detection	A/R				C	
Identity & Trust	Agent identity issuance	I	C	I	A/R	I	Separate from service identity
Identity & Trust	Trust tier enforcement	I	C	I	A/R	I	Native vs customer vs third-party
Ingest Pipeline	Drift Detection			A/R		C	
Ingest Pipeline	Record Buffering			A/R		I	
Ingest Pipeline	Self-Healing Repair			R		A	
Ingest Pipeline	Stream Consumption (Sidecar)			A/R		C	
Ingest Pipeline	Verification with Farm			C		A/R	
Lifecycle	Agent onboarding & versioning	I	I	I	A/R	I	Promotion & rollback
Lifecycle	Zombie agent detection	C	C	C	A/R	I	Sprawl control
Observability	Cost tracking				A/R	I	
Observability	Deterministic replay	I	I	C	A/R	C	Debug & audit
Observability	Execution trace & audit	I	I	C	A/R	C	Who did what, when
Observability	Performance metrics				A/R	C	
Observability	Vitals aggregation				A/R	C	
Operator UI	Display Candidates		A/R			I	
Operator UI	Display Drift Events		A/R			I	
Operator UI	Display Pipe Inventory		A/R			I	
Operator UI	Load Enterprise Presets		A/R			I	
Pipe Inference	Create DeclaredPipe		A/R			I	
Pipe Inference	Infer Change Semantics		A/R			I	
Pipe Inference	Infer Entity Scope		A/R			I	
Pipe Inference	Infer Fabric Plane		A/R			I	
Pipe Inference	Infer Identity Keys		A/R			I	
Pipe Inference	Infer Modality		A/R			I	
Pipe Inference	Infer Transport Kind		A/R			I	
Pipe Registry	Compute Schema Hash		A/R			I	
Pipe Registry	Serve Pipe Queries		A/R			I	
Pipe Registry	Store Declared Pipes		A/R			I	
Pipe Registry	Version Pipe Changes		A/R			I	
Pipeline Execution	Graph Building			A/R		I	
Pipeline Execution	Narration Broadcasting			A/R		I	
Pipeline Execution	Persona Filtering			A/R		I	
Pipeline Execution	Run Metrics Collection			A/R		I	
Policy & Governance	Agent-level policy evaluation	I	C	C	A/R	I	Autonomy, scope, cost
Policy & Governance	Autonomy bounds enforcement	I	C	I	A/R	I	Guardrails
Policy & Governance	HITL escalation	I	I	C	A/R	I	Confidence-based approval
Reconciliation	Accuracy measurement	R				A/R	
Reconciliation	Ground truth validation	R				A	
Reconciliation	Pipeline execution	A/R				C	
Runtime	Task queue management	I	I	I	A/R	I	Async orchestration
Runtime	Worker pool management	I	I	I	A/R	I	Scalable execution
SOR Identification	Data domain classification	A/R				C	
SOR Identification	System of Record scoring	A/R				C	
Scaling Infrastructure	Auto-recovery				I	A/R	
Scaling Infrastructure	Load balancing				I	A/R	
Scaling Infrastructure	Task queue management				C	A/R	
Scaling Infrastructure	Worker pool orchestration				I	A/R	
Scheduling	Cron-based scheduling				A/R	I	
Scheduling	Event-driven triggers				A/R	R	
Scheduling	Webhook triggers				A	R	
Schema Ingestion	Demo Schema Loading			A/R		I	
Schema Ingestion	Farm Schema Fetching			R		A	
Schema Ingestion	Stream Source Loading			R		A	
Semantic Mapping	Heuristic Mapping			A/R		I	
Semantic Mapping	LLM Refinement (Prod)			A/R		I	
Semantic Mapping	Mapping Persistence			A/R		I	
Semantic Mapping	RAG Enhancement (Prod)			A/R		I	
Source Normalization	Alias Resolution			A/R		C	
Source Normalization	Discovery Mode (New Sources)			R		A	
Source Normalization	Pattern Matching			A/R		I	
Source Normalization	Registry Loading			R		A	
Tee Request Management	Approve/Reject Tee		A/R			I	
Tee Request Management	Create Tee Request		A/R			I	
Tee Request Management	Track Tee Status		A/R			I	
Telemetry	Metrics Collection			A/R		I	
Telemetry	TPS/Quality Calculation			A/R		C	
Telemetry	Telemetry Broadcasting			A/R		I	
Triage & Prioritization	Asset prioritization scoring	A/R					
Triage & Prioritization	Provisioning status assignment	A/R					
Trust & Security	Injection defense				A/R	I	
Trust & Security	PII detection & redaction				A/R	I	
Trust & Security	Policy enforcement				A/R	C	
Verification	Generate Test Data		I			A/R	
Verification	Provide Source of Truth		C			A/R	
Verification	Verify Against Truth		R			A	
Visualization	Monitor Dashboard			A/R		I	
Visualization	Sankey Graph Rendering			A/R		I	
Visualization	Telemetry Ribbon			A/R		I	
Visualization	Terminal Narration			A/R		I	
Workflow Orchestration	Run cancellation				A	R	
Workflow Orchestration	Run creation & tracking				A/R	R	
Workflow Orchestration	Step sequencing				A/R	C	
