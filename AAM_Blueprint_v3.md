# AAM Connectivity Architecture

**Blueprint \+ CC Build Specs** Adaptive API Mesh | AOS Engineering | May 2026 (v3)

---

## Version note (v3)

v3 reconciles AAM scope with `AOS_MASTER_RACI_v8.6.csv` (authoritative). RACI v8.6 assigns A/R for **Semantic Mapping** (Heuristic, LLM Refinement, Batch API, Persistence) to **DCL**, and **Identity resolver v2** to **Convergence** for the ME path. v2 of this blueprint described both as AAM functions. v3 corrects this: AAM owns transport and produces raw records with transport-level provenance, then hands off to DCL via `POST /api/dcl/ingest-triples`. DCL performs field→concept semantic mapping and identity resolution. For Convergence (M&A) engagements, identity resolution runs in Convergence's `identity_resolver_v2` module.

Sections changed: §0.1 (pipeline ownership clarified), §1.0 (identity tables labeled DCL-owned), §1.4 (rewrite — Ingest Layer is now a handoff, not a mapping pipeline), §3 Day 4 (ownership labels), §3.5 (structural claim, demo UI screen attribution, demo done criteria), §8 (demo and full production done criteria).

This blueprint is subordinate to `AOS_MASTER_RACI_v8.6.csv`. If prose here conflicts with the CSV, the CSV wins.

---

## 0\. AOS Platform Context

### 0.1 What AOS Is

AOS (autonomOS) is an enterprise platform that delivers unified context across enterprise systems. The platform has two product lines: ContextOS (single-entity enterprise intelligence) and Convergence (multi-entity M\&A integration intelligence). Both share a common engine.

The pipeline is: AOD (discovery) → AAM (connection mapping and transport) → Farm (financial model generation) → DCL (semantic context layer) → NLQ (natural language query). AOD scans the enterprise and discovers what exists. AAM connects to what AOD found, establishes data pipes, and transports raw records to DCL. DCL maps raw fields to concepts, resolves identity across sources, converts to semantic triples with full provenance, and stores them. Farm generates financial models from the data. NLQ lets users ask questions in plain English.

This document covers AAM — the Adaptive API Mesh. AAM's job is to connect to the customer's integration infrastructure, discover what data pipes exist, and transport data through those pipes into DCL. It is the bridge between the customer's enterprise systems and the AOS semantic layer. AAM's responsibility ends at delivery to DCL's ingest endpoint — field→concept semantic mapping and identity resolution are **DCL** functions (per `AOS_MASTER_RACI_v8.6.csv` §Semantic Mapping and §Semantic Intelligence; for ME engagements, §Convergence Resolver).

### 0.2 Why Four Fabric Planes

A mid-market enterprise runs 200–500 applications. A large enterprise runs 1,000+. The traditional approach to connecting these is to build a connector for each application — a Salesforce connector, a NetSuite connector, a Workday connector, a ServiceNow connector, and so on. This is how Informatica, Fivetran, and most integration vendors work. The problem with this approach is that it scales linearly: 200 apps means 200 connectors, each with its own authentication, API versioning, pagination logic, rate limiting, and schema handling. Building and maintaining these connectors is a permanent engineering tax. It's why Informatica has thousands of engineers and why deployment takes months.

AOS takes a different approach. We observe that enterprises don't connect their apps directly to each other. They run integration infrastructure — middleware, API gateways, message buses, data warehouses — that already connects their apps. Salesforce doesn't talk directly to NetSuite; a Workato recipe moves data between them. The marketing stack doesn't write to the data warehouse directly; a Kafka pipeline streams events into Snowflake. Internal APIs aren't exposed raw; they're fronted by Kong or Apigee with auth, rate limiting, and routing.

This infrastructure falls into four categories. We call them fabric planes:

**iPaaS (Integration Platform as a Service).** Workato, MuleSoft, Boomi. These are the workflow engines. They hold authenticated connections to dozens or hundreds of enterprise apps and run recipes/flows/processes that move data between them. Connecting to Workato once gives AOS visibility into every app Workato touches. The iPaaS plane's management API returns the recipe catalog, connection inventory, and data schemas. Its data transport mechanisms deliver actual records via webhooks, message queues, data pipeline runs, or callable endpoints.

**API Gateway.** Kong, Apigee, AWS API Gateway. These sit in front of internal services and manage routing, auth, rate limiting, and observability. The gateway's admin API returns every registered service, route, and upstream target. Its data transport mechanism is proxied REST — AOS routes read requests through the gateway to upstream services, and the gateway handles auth injection and traffic management.

**Event Bus.** Kafka (Confluent Cloud, Azure Event Hubs, AWS MSK), AWS EventBridge. These carry real-time event streams between systems. The bus's management API returns topic catalogs, consumer group state, and schema definitions. Its data transport mechanism is consumer subscription — AOS subscribes to topics and receives events as they flow. Azure Event Hubs speaks the Kafka wire protocol, so one consumer implementation covers three vendors.

**Data Warehouse.** Snowflake, BigQuery, Databricks, Amazon Redshift. These are where enterprise data lands for analytics. Source systems dump data into landing tables via ETL, Snowpipe, streaming ingestion, or zero-ETL integrations. The warehouse's metadata API (INFORMATION\_SCHEMA, Unity Catalog) returns the full table and column catalog. Its data transport mechanism is SQL query execution via REST API, with CDC support (Snowflake Streams, Delta Change Data Feed) for incremental reads.

The leverage is in the math. A typical mid-market enterprise runs one or two products per plane type — maybe 6–10 plane instances total. Connecting to those 6–10 plane instances gives AOS visibility into the entire application landscape that's connected through them. One Workato connection reveals 50–100 app connections. One Kong connection reveals every registered API route. One Snowflake connection reveals every landing table from every source system. Instead of building 200 connectors, AOS builds adapters for 4 plane types, connects to whichever vendors the customer runs, and discovers existing integrations that are already flowing.

This is why deployment is days, not months. AOS doesn't build integrations. It discovers integrations that already exist and reads data through infrastructure the customer already operates.

### 0.3 Why MCP for Discovery — and Only for Discovery

Each fabric plane vendor exposes a management API for programmatic access to their platform: Workato's Platform API, Kong's Admin API, Snowflake's SQL API, Confluent's REST API. The traditional approach is to build a vendor-specific adapter for each one — code that knows how to call that vendor's API, parse their response format, and translate it into AOS's internal data model. This works, but it means AOS maintains a codebase for every vendor API, and when the vendor changes their API, AOS's adapter breaks.

As of early 2026, a better option exists. Major fabric plane vendors are shipping Model Context Protocol (MCP) servers: Workato, MuleSoft, Boomi, Apigee, Snowflake, Databricks. An MCP server exposes the vendor's capabilities as structured tools that any MCP client can discover and invoke. Instead of AOS maintaining vendor-specific code that calls GET /api/recipes and parses the Workato response format, AOS runs a universal MCP client that connects to Workato's MCP server, discovers a "list\_recipes" tool, and calls it. The vendor maintains the mapping between their internal API and the MCP tool interface. When Workato changes their API, their MCP server updates. AOS's MCP client doesn't change.

**MCP is for discovery only. It is not for record transport.** This distinction is architectural and load-bearing. iPaaS MCP servers expose skills (RPC tools) — `look_up_customer`, `create_ticket`, `list_recipes`. They are not pub/sub or change-data-capture surfaces. Using MCP calls to scrape records at production volume has three failure modes:

- **Wrong protocol.** Calling a "search customers" skill 100K times to fetch records is brittle. MCP wasn't designed for this load shape.  
- **Financial cost.** Workato bills per recipe execution; Boomi by connection. Routing record-stream volume through MCP can drive a 2–5x increase in the customer's iPaaS bill.  
- **Rate limit ceilings.** iPaaS runtimes are sized for business-logic execution, not record-stream volume. AOS would hit rate limits and breach SLAs.

So the architecture is two layers: **MCP for discovery** (universal, vendor-maintained, low-volume — schemas, change pointers, what flows exist, last-modified timestamps), and **vendor-native push/CDC for record transport** (webhooks, event streams, Kafka protocol, CDC reads — designed for the load shape, no MCP-tax penalty).

For vendors that haven't shipped MCP servers yet (Kong, AWS API Gateway, EventBridge, Redshift, BigQuery), a thin vendor shim calls their native REST API and presents results in MCP tool output format. The shim is small (just API call translation) and disposable — it gets deleted once the vendor ships their MCP server. The shim is a bridge, not an architecture.

---

## 1\. Architecture

AAM connects to enterprise fabric planes to do two things: discover what integration pipes exist, and transport data through those pipes into DCL. The architecture is grounded by a persistence model, with five operational layers, plus a proof-of-value layer for pre-credential demonstration.

### 1.0 Persistence Model

**AOS hosts context. Source systems remain authoritative for raw records.** This is the Plaid pattern applied to enterprise systems: vendor-hosted persistence of the abstraction layer's working set, with sources remaining the system of record for the underlying data.

What lives in AOS (the hot tier):

- Normalized records as triples with full provenance (DCL-owned; produced by DCL's semantic mapping pipeline downstream of AAM handoff)  
- Concept catalog and ontology (DCL-owned; DCL's existing 19 verified domains, 131 integration tests)  
- Identity resolution tables — precomputed cross-source entity matches with confidence scores (DCL-owned for AOS path; for ME engagements populated by Convergence's `identity_resolver_v2` per `AOS_MASTER_RACI_v8.6.csv` §Convergence Resolver)  
- Schema definitions per DeclaredPipe (AAM-owned at the transport boundary; DCL holds the canonical concept-side schema)  
- Field mapping rules (DCL-owned per `AOS_MASTER_RACI_v8.6.csv` §Semantic Mapping)

What does not live in AOS:

- Long-tail historical data (source retention is authoritative)  
- Raw fields outside the mapped working set  
- Anything outside the customer's authorized scope

For queries against data outside the authorized hot-tier scope, AOS provides federation fallback — push the query to source, normalize results in flight, return to consumer. Federation is the exception, not the default.

**Customer-facing posture:** "AOS holds your context. Your source systems hold your records." Defensible under security review, accurate to the architecture, structurally distinct from catalogs (no records) and from full ETL replacement (no source mirror).

**Egress:** Customer can extract all AOS-held triples via standard export API at any time. AOS retains nothing after termination notice. Detailed procedural specification handled in security review documentation, not in this architecture document.

**Scope authorization:** Day 1 is concept-level — the customer authorizes AOS to operate on named concepts (customer, invoice, vendor, etc.). Field-level granularity is on the 6-month roadmap. Attribute-class-level is the regulatory ceiling and beyond demo and Day 1\.

### 1.1 Network Layer: AOS Edge Agent

Enterprise fabric plane endpoints (Workato admin API, Kong admin API, Snowflake account, Kafka cluster) are often behind corporate firewalls, VPNs, or private VPCs. The AOS Edge Agent is a lightweight sidecar deployed inside the customer's network. It establishes an outbound-only encrypted tunnel (WireGuard-based) to the AOS control plane on port 443\. Outbound HTTPS is almost always permitted by corporate firewalls, eliminating the need for inbound firewall changes, VPC peering requests, or security team involvement for network-level access. The Edge Agent proxies AAM's MCP and transport connections to local fabric plane endpoints.

### 1.2 Discovery Layer: Universal MCP Client

AAM runs a universal MCP client that connects to whatever MCP servers the customer's fabric planes expose, discovers available tools, and invokes them to build the pipe catalog. For vendors without MCP servers, a thin vendor shim calls the vendor's native REST API and presents results in MCP tool output format. The Tool Output Translator converts MCP tool responses into DeclaredPipe objects — the canonical AOS representation of a data pipe, carrying source system, target system, fabric plane, modality, transport type, schema, and confidence score.

### 1.3 Transport Layer: Protocol Shims

Four transport shims handle actual data movement using standard protocols. **KafkaTransport** handles the Kafka wire protocol, covering Confluent Cloud, Azure Event Hubs (Kafka-compatible), and AWS MSK. **HTTPTransport** handles webhooks, REST proxy passthrough, callable endpoint invocation, and data pipeline status polling. **SQLTransport** handles async SQL query execution against warehouse REST APIs. **StreamTransport** handles WebSocket and SSE for real-time push scenarios.

### 1.4 Ingest Layer: Handoff to DCL

Transport shims deliver raw enterprise records with vendor-specific field names. **AAM does not perform field→concept semantic mapping or identity resolution.** Both are DCL responsibilities per `AOS_MASTER_RACI_v8.6.csv` — §Semantic Mapping (Heuristic, LLM Refinement, Batch API, Persistence — all DCL A/R) and §Semantic Intelligence (Concept Registry, Skeleton Layer, Inference Layer, Runtime Resolution — all DCL A/R). For ME (Convergence) engagements, identity resolution runs in Convergence's `identity_resolver_v2` module per §Convergence Resolver.

AAM's role at the ingest boundary is bounded:

- Emit raw records with their source-system field names preserved (no field→concept renaming inside AAM).  
- Tag every record with transport-level provenance: `source_system`, `pipe_id`, `run_id`, `transport_timestamp`, and the source-system entity_id (the identifier the source system itself assigned — e.g., Salesforce account ID, NetSuite customer ID — not an AOS canonical entity_id).  
- Handle continuous flow control: backpressure, deduplication of in-flight retries, transport-level error handling.  
- Hand off to DCL via `POST /api/dcl/ingest-triples`.

Downstream of the handoff, DCL performs field→concept mapping (heuristic and LLM-assisted, with human confirmation for mid-confidence proposals), resolves identity across sources (canonical entity_id assignment), converts to semantic triples `(entity_id, concept, property, value, period)`, and writes to PG. For Convergence (M&A) engagements, identity resolution runs in Convergence's `identity_resolver_v2` per `convergence_transition_master`. AAM does not own any of these downstream functions.

The split exists so AAM stays generic across all fabric planes and vendors: transport is plane-specific; semantic mapping is concept-specific and lives where the concept catalog lives (DCL). Any blueprint passage describing "LLM-assisted semantic mapping" or "identity resolution producing merged entities" describes a DCL (or, for ME, Convergence) surface invoked downstream of AAM — not an AAM function.

### 1.5 Orchestration Layer: Maestra Prework

Maestra is the AOS AI engagement lead. She runs the entire deployment prework before AAM touches a single endpoint. Tech stack identification from AOD discovery plus structured interview. Vendor-specific credential provisioning checklists with exact commands (SQL GRANTs, API token generation steps, Edge Agent deployment instructions). MCP server availability assessment. Credential validation as credentials arrive. Discovery review with the customer. Transport configuration walkthrough. Maestra is the interface between the customer's IT team and the AOS pipeline.

### 1.6 Proof-of-Value Layer: Schema-Aware Synthetic Data

Farm generates synthetic data conforming to DeclaredPipe schemas discovered by MCP. The customer sees their actual system topology (Salesforce → Workato → DCL, Snowflake landing tables → DCL) populated with realistic fake data before live data access is granted. This delivers Day 0 demonstration value while security teams are still reviewing live data credentials. Synthetic triples are tagged with source\_system='synthetic' and can be purged when live data arrives.

---

## 2\. Fabric Plane Coverage

Transport mechanisms marked as **push** (vendor pushes records to AOS, no polling cost), **pull** (AOS reads on demand or schedule), or **CDC** (change-data-capture, designed for record-stream volume).

| Plane | Vendors | Discovery (MCP) | Transport (record movement) |
| :---- | :---- | :---- | :---- |
| iPaaS | Workato, MuleSoft, Boomi | MCP servers (all three). Recipe/flow/process catalogs, connection inventory, schemas. | **Push**: Workato webhooks, Boomi Event Streams, MuleSoft event listeners. **Pull**: callable endpoints (limited use). Webhook/event subscription is the default — no MCP polling for records. |
| API Gateway | Kong, Apigee, AWS API GW | MCP server (Apigee). Vendor shims (Kong, AWS). Service/route catalogs, upstream health, OpenAPI export. | **Pull**: REST proxy passthrough with auth injection on read requests. AWS adds Lambda integration and VPC link. Mostly stateless request-response. |
| Event Bus | Kafka/Confluent, Azure Event Hubs, EventBridge | MCP (Confluent, emerging). Vendor shim (EventBridge). Topic/schema/consumer metadata. Event Hubs uses Kafka wire protocol — same adapter, different auth. | **Push** (subscription-based): Kafka protocol consumer subscription, offset tracking (covers Confluent \+ Event Hubs \+ MSK). EventBridge pipe delivery to registered targets. |
| Warehouse | Snowflake, BigQuery, Databricks, Redshift | MCP servers (Snowflake, Databricks). Vendor shims (BigQuery, Redshift). INFORMATION\_SCHEMA, Unity Catalog, warehouse/cluster status. | **CDC**: Snowflake Streams, Delta Change Data Feed, Redshift streaming MV (designed for record-stream volume). **Pull**: async SQL query for bulk loads and ad-hoc reads (all four). Snowpipe Streaming for real-time ingest. |

---

## 3\. Deployment Timeline

| Day | Activity | Who | Dependency |
| :---- | :---- | :---- | :---- |
| 0 | Maestra engagement. AOD scan \+ tech stack interview. Vendor manifest confirmed. Credential checklist \+ Edge Agent instructions sent to customer IT. | Maestra \+ customer stakeholder | None |
| 0 | Farm generates synthetic data matching discovered pipe schemas. Customer sees their topology with fake data. Full pipeline demonstrated. | Farm (automated) | Vendor manifest from Maestra |
| 1–3 | Customer provisions credentials and deploys Edge Agent. Maestra tracks progress, answers questions, validates credentials as they arrive. | Customer IT \+ Maestra | Customer IT governance tempo |
| 3 | Edge Agent tunnel established. MCP client connects to fabric planes. Discovery runs. Maestra reviews pipe catalog with customer. | AAM \+ Maestra \+ customer | Credentials \+ network access |
| 4 | Transport configured. AAM begins delivering raw records to DCL via `POST /api/dcl/ingest-triples`. DCL semantic field mapping runs (LLM-proposed, human-confirmed) — DCL-owned per RACI v8.6 §Semantic Mapping. DCL identity resolution produces canonical entity_ids. Triple conversion validated. First live data flows into DCL. | AAM (transport) \+ DCL (mapping, identity) \+ Maestra \+ customer | Discovery complete |
| 5 | DCL semantic layer active on live data. NLQ connected. Maestra runs quality checks. Customer asks first question against their own data. | DCL \+ NLQ \+ Maestra \+ customer | Live data in DCL |

Critical path: customer credential provisioning and Edge Agent deployment (Days 1–3). For PE portfolio companies where the fund mandates access, this compresses to Day 1\. For enterprises with formal change management, this can stretch to 1–2 weeks. Maestra minimizes friction by providing exact, vendor-specific provisioning instructions.

### 3.5 Demo Proof Point Specification

A standalone milestone preceding general production deployment. Proves the architectural claims before any sales conversation requires them.

**Structural claim being proved:** Same AAM code path connects to two structurally different iPaaS fabrics and delivers raw records with transport-level provenance to DCL via `POST /api/dcl/ingest-triples`. DCL normalizes the records via semantic mapping (per RACI v8.6 §Semantic Mapping — DCL A/R), resolves identity across the two sources (per §Semantic Intelligence — DCL A/R), persists as unified context, and exposes that context via AOS-MCP. A real-shape consumer queries AOS-MCP and gets an answer neither source could provide alone. The demo proves the **AAM** transport abstraction (one code path across two iPaaS fabrics) and the **handoff contract** to DCL, not AAM-owned semantic mapping.

**Vendor pair:** Workato \+ Boomi. Structurally different MCP surfaces (pre-built server vs gateway pattern), different push mechanisms (webhook vs Event Stream). Same AAM code path handles both.

**Dataset:** Combined financials across two ERPs.

- NetSuite (via Workato) — chart of accounts, AR aging, vendor payables  
- Sage Intacct (via Boomi) — same logical entities, different chart, different periods  
- \~500 customers/source, \~5,000 invoices/source, \~500 vendors/source, 24 months history

**Demo question (consumer query):** "Show me combined Q3 AR aging across both entities, with vendors that appear in both books flagged for consolidation." Requires cross-source schema normalization, identity resolution, temporal alignment, provenance to source records.

**Consumer surfaces:**

1. FinOps agent (primary) — calls AOS-MCP, renders answer with inline citations  
2. AOS Intelligence dashboard (NLQ) — same question, dashboard view, drill-through to source  
3. Simulated BI/API consumer — curl call to AOS-MCP, JSON response with provenance

**Demo UI (four screens):**

1. Pipe Catalog View (AAM-owned surface) — live list of DeclaredPipes from both MCPs, single AAM module visible  
2. Semantic Mapping UI (**DCL-owned surface**, per RACI v8.6 §Semantic Mapping — invoked downstream of AAM handoff) — LLM-proposed mappings, human confirmation flow, mid-confidence field requiring explicit click  
3. Identity Resolution UI (**DCL-owned for AOS path**, per RACI v8.6 §Semantic Intelligence; **Convergence-owned for ME engagements** via `identity_resolver_v2`, per §Convergence Resolver) — side-by-side source records, proposed merge with confidence, match rules  
4. Consumer View (NLQ/FinOps surfaces) — FinOps agent chat with provenance drill-through

**One injected failure scenario:** A record set with identity match confidence 0.71 (below 0.8 threshold) queued for review, system continues functioning. Reinforces production-readiness without scripted theater. Pre-injected into demo dataset, not live-triggered.

**Code-path proof moment:** Demo operator switches to CC, walks one function in AAM transport showing same code invoked for both Workato and Boomi. Five seconds, no narration.

**Demo done criteria:**

- Same AAM code path operational against Workato \+ Boomi (no vendor branching)  
- Raw records flowing through AAM transport with transport-level provenance (`source_system`, `pipe_id`, `run_id`, `transport_timestamp`, source-system entity_id) and handed off to DCL via `POST /api/dcl/ingest-triples`  
- **DCL** LLM-assisted semantic mapping (per RACI v8.6 §Semantic Mapping — DCL A/R) demonstrable live on records delivered by AAM  
- **DCL** identity resolution producing merged entities with confidence scores (per RACI v8.6 §Semantic Intelligence — DCL A/R for AOS; for the Convergence demo path, **Convergence's `identity_resolver_v2`** per §Convergence Resolver)  
- AOS-MCP server live with real MCP protocol  
- Consumer query that requires both sources, answered with traceable provenance  
- One handled failure scenario prepared (mid-confidence identity match queued for human review at the DCL or Convergence resolution surface)  
- Three CIO questions answerable live: (1) same code, both vendors; (2) change a record at source, see it propagate; (3) trace any answer back to source records

---

## 4\. Health Monitoring and Self-Healing

Every fabric plane connection supports four health states: reachable, degraded, unreachable, and auth\_expired. AAM monitors continuously and self-heals where possible.

### 4.1 Health Signals by Vendor

The full table covers production deployment across all four planes. Demo build exercises only the Workato \+ Boomi rows.

| Vendor | Health Signal | Detection | Self-Heal |
| :---- | :---- | :---- | :---- |
| Workato | Connection error. Pipeline schema drift. Webhook delivery stop. | MCP health tool. Pipeline status poll. Delivery watchdog. | Re-auth. Pause pipeline. Re-register webhook. |
| MuleSoft | Application FAILED. MQ stall. | Runtime Manager MCP. MQ consumer lag. | Restart app. Reconnect MQ. |
| Boomi | Atom OFFLINE. Process error. Event Stream drop. | AtomSphere MCP. Execution history. | Re-trigger process. Resubscribe. |
| Kong | Upstream unhealthy. Proxy 502\. | Health endpoint via shim. | Circuit break. Reroute. |
| Apigee | Error rate spike. Undocumented API. | Analytics MCP. Advanced Security. | Rate limit. Alert. |
| AWS API GW | Lambda timeout. 5xx spike. | CloudWatch via shim. | Throttle. Retry with backoff. |
| Kafka/Event Hubs | Consumer lag spike. Rebalance failure. | Consumer group metadata. | Restart consumer. Reset offsets. |
| EventBridge | Pipe delivery failure. DLQ growth. | CloudWatch via shim. | Reconfigure pipe. Retry. |
| Snowflake | Warehouse SUSPENDED. Query hang. | SHOW WAREHOUSES via MCP. | Resume warehouse. |
| BigQuery | Job quota exceeded. Timeout. | Jobs API via shim. | Retry with backoff. |
| Databricks | Warehouse STOPPED. CDF stale. | Warehouse API via MCP. | Start warehouse. Resync. |
| Redshift | Cluster PAUSED. Streaming MV fail. | Management API via shim. | Resume cluster. Refresh MV. |

### 4.2 RACI Stub Capabilities

The RACI marks AAM capabilities as Stub, spanning adapters (pipe discovery, plane health, adapter factory resolution across all four plane types), self-healing (connection/fabric drift, consumer lag, warehouse suspend, execute self-heal, heal history logging), and their connections. The harness exercises all stub capabilities through scenario-based testing with injectable failure conditions. Authoritative stub list: `AOS_MASTER_RACI_v8.6.csv` (filter on AAM column = R or A/R and Status = Stub).

---

## 5\. Synthetic Harness

The harness replaces live vendor endpoints with local stubs so AAM's real MCP client, transport shims, and ingest pipeline can run in CI without credentials.

### 5.1 Stub Architecture

**ipaas\_stub.** Simulates Workato/MuleSoft/Boomi MCP server responses \+ webhook delivery \+ data pipeline status \+ event streams. Vendor flavor selected per test. **Demo build uses this stub.**

**gateway\_stub.** Simulates Kong/Apigee/AWS API GW admin API responses \+ proxy passthrough. Auth validation per vendor mechanism.

**kafka\_stub.** Simulates Kafka broker. Consumer subscription, message delivery, offset tracking, schema registry. Dual auth: Confluent API key mode and Azure AD/SAS mode (Event Hubs).

**eventbridge\_stub.** Simulates EventBridge APIs. Rules, pipes, schema registry, event delivery.

**warehouse\_stub.** Simulates Snowflake/BigQuery/Databricks/Redshift SQL API, INFORMATION\_SCHEMA, status commands, CDC responses. Vendor flavor selected per test.

### 5.2 Scenario System

Tests declare a named scenario. Stubs configure accordingly. Scenarios: healthy (all normal), degraded (partial catalog, intermittent delivery), auth\_failure (401 on all authenticated endpoints), drift\_connectivity (reachable → unreachable between health checks), consumer\_lag (event bus offset lag injection), warehouse\_suspended (SUSPENDED/STOPPED/PAUSED), webhook\_failure (delivery stops), pipeline\_schema\_drift (Workato pipeline encounters schema change mid-sync), gateway\_502 (proxy returns 502), recovery (failure → self-heal → recovery verified), multi\_vendor (mixed states across vendors).

### 5.3 Design Rules

Stubs are external to AAM — AAM's real code runs, only external endpoints are replaced. Config-driven switching via HARNESS\_MODE. Stub response shapes match real vendor API shapes exactly. No hardcoded ground truth — stubs generate from scenario config. Every test asserts a positive expected outcome. No silent fallbacks. Vendor auth mechanisms are testable per vendor.

---

## 6\. CC Build Specs

Nine work packages. Designed for parallel CC agents. Each prompt is self-contained.

**Demo build scope (active now):** WP-1, WP-2 (HTTPTransport path only), WP-3 (Workato \+ Boomi shims only), WP-5 (ipaas\_stub only), WP-6 (factory wiring for two vendors), WP-8 (production ingest pipeline, demo path), WP-9 (synthetic data for demo dataset).

**Deferred to production build (after demo proves abstraction):** WP-2 KafkaTransport/SQLTransport/StreamTransport, WP-3 Kong/AWS API GW/EventBridge/Redshift/BigQuery shims, WP-4 Maestra credential module (demo uses sandbox creds), WP-5 gateway\_stub/kafka\_stub/eventbridge\_stub/warehouse\_stub, WP-7 AOS Edge Agent (demo uses local proxy).

The full WP catalog below remains the production architecture target. Individual prompts unchanged from prior version.

### WP-1: MCP Client Infrastructure

*CC time: 2–3 hours. One agent. AAM repo. No dependencies. **Demo: active.***

\[Prompt unchanged from prior version\]

### WP-2: Transport Shims

*CC time: 3–4 hours (HTTP only for demo; full when production). One agent. AAM repo. No dependencies. **Demo: HTTPTransport only.***

\[Prompt unchanged from prior version. For demo, only HTTPTransport implementation required; KafkaTransport, SQLTransport, StreamTransport deferred.\]

### WP-3: Vendor Shims

*CC time: 1–2 hours per vendor. Parallel agents. AAM repo. Depends on WP-1 base class. **Demo: Workato \+ Boomi only.***

\[Template prompt unchanged from prior version. For demo, only Workato and Boomi shims required. Kong, AWS API Gateway, EventBridge, Redshift, BigQuery deferred.\]

### WP-4: Maestra Credential Onboarding Module

*CC time: 2–3 hours. One agent. Platform repo. No dependencies. **Demo: deferred.***

\[Prompt unchanged from prior version. Demo uses sandbox credentials; module deferred to production build.\]

### WP-5: Harness Stubs

*CC time: 4–6 hours total (1.5–2 hours for ipaas\_stub alone). **Demo: ipaas\_stub only.***

\[Prompt unchanged from prior version. For demo, only ipaas\_stub required.\]

### WP-6: Factory \+ Integration Wiring

*CC time: 2–3 hours. One agent. AAM repo. Depends on WP-1 \+ WP-2. **Demo: active, scoped to two vendors.***

\[Prompt unchanged from prior version. For demo, factory only wires Workato \+ Boomi.\]

### WP-7: AOS Edge Agent

*CC time: 3–4 hours. **Demo: deferred (local proxy used).***

\[Prompt unchanged from prior version. Demo uses local proxy in place of full Edge Agent.\]

### WP-8: Production Ingest Pipeline

*CC time: 4–6 hours. One agent. AAM \+ DCL \+ Platform repos. Depends on WP-2. **Demo: active.***

\[Prompt unchanged from prior version.\]

### WP-9: Schema-Aware Synthetic Data

*CC time: 2–3 hours. One agent. Farm repo. Depends on WP-1 \+ WP-8. **Demo: active, scoped to demo dataset.***

\[Prompt unchanged from prior version. For demo, synthetic data generated for combined-financials dataset (NetSuite \+ Sage Intacct schemas).\]

---

## 7\. Build Execution Plan

### Demo Build (active)

| WP | Name | CC Hrs | Depends On | Parallel With |
| :---- | :---- | :---- | :---- | :---- |
| 1 | MCP Client Infrastructure | 2–3 | None | WP-2, 5 |
| 2 | HTTPTransport only | 1–2 | None | WP-1, 5 |
| 3 | Workato \+ Boomi shims | 2–4 | WP-1 base class | WP-2, 5 |
| 5 | ipaas\_stub | 1.5–2 | None | WP-1, 2 |
| 6 | Factory wiring (two vendors) | 1–2 | WP-1, WP-2 | After WP-1+2 land |
| 8 | Production Ingest Pipeline (demo path) | 3–4 | WP-2 | After WP-2 lands |
| 9 | Synthetic data for demo dataset | 1–2 | WP-1, WP-8 | After WP-1+8 land |

Total demo CC execution: \~12–19 hours across all agents. Wall-clock with parallel agents: 1–2 days of human oversight. Demo-end-state UI work runs in parallel.

### Full Production Build (deferred until demo proves abstraction)

| WP | Name | CC Hrs | Depends On |
| :---- | :---- | :---- | :---- |
| 1 | MCP Client Infrastructure | already built | None |
| 2 | Transport Shims (Kafka, SQL, Stream) | 2–3 | None |
| 3 | Vendor Shims (Kong, AWS, EventBridge, Redshift, BigQuery) | 5–10 | WP-1 base class |
| 4 | Maestra Credential Module | 2–3 | None |
| 5 | Harness Stubs (gateway, kafka, eventbridge, warehouse) | 3–5 | None |
| 6 | Factory wiring expansion to all planes | 1–2 | WP-1, WP-2 |
| 7 | AOS Edge Agent | 3–4 | None |
| 8 | Production Ingest Pipeline (full) | 1–2 | already built |
| 9 | Synthetic data (full vendor coverage) | 1–2 | already built |

---

## 8\. Done Criteria

### Demo Done Criteria

- MCP client connects to ipaas\_stub and discovers tools for Workato \+ Boomi flavors  
- Tool outputs translate to DeclaredPipes with correct schema, source, target, modality  
- HTTPTransport moves raw records (webhook for Workato, event for Boomi) with transport-level provenance and hands them off to DCL via `POST /api/dcl/ingest-triples`; DCL converts to semantic triples downstream (DCL-owned per RACI v8.6)  
- Adapter factory selects correct discovery client \+ HTTPTransport based on Workato/Boomi config  
- Same code path verified across both vendors (no vendor branching)  
- **DCL** LLM-assisted semantic mapping (RACI v8.6 §Semantic Mapping — DCL A/R) operates against demo schemas with human confirmation flow — demonstrated on records delivered by AAM  
- **DCL** identity resolution (RACI v8.6 §Semantic Intelligence — DCL A/R for AOS; **Convergence** §Convergence Resolver — A/R for ME `identity_resolver_v2`) produces merged entities across Workato \+ Boomi sources with confidence scores  
- AOS-MCP server exposes unified context as real MCP protocol; FinOps agent and dashboard consume successfully  
- Combined Q3 AR aging query returns answer requiring both sources, with provenance traceable end-to-end  
- One handled failure scenario (mid-confidence identity match) demonstrably queued for review at the DCL or Convergence resolution surface with system functioning  
- Three CIO questions answerable live (same code both vendors; record change propagates; provenance traces to source)

### Full Production Done Criteria (deferred to production build)

MCP client connects to harness stubs and discovers tools for all vendor flavors. Tool outputs translate to DeclaredPipes with correct schema, source, target, and modality. Transport shims move raw records from harness stubs with transport-level provenance and hand them off to DCL via `POST /api/dcl/ingest-triples`; DCL converts to semantic triples downstream (DCL-owned per RACI v8.6 §Semantic Mapping and §Semantic Intelligence). Adapter factory selects correct discovery client \+ transport shim based on vendor config. Edge Agent establishes tunnel and proxies MCP \+ transport connections to simulated internal endpoints. Self-healing detects injected failures and recovers across all plane types with audit logging. Maestra credential module generates correct provisioning checklists for all vendors and validates credentials against harness stubs. **DCL's** semantic field mapper (RACI v8.6 §Semantic Mapping — DCL A/R) proposes reasonable mappings against DCL's known domains with human confirmation. **DCL** identity resolution produces canonical entity_ids for the AOS path; **Convergence's `identity_resolver_v2`** (RACI v8.6 §Convergence Resolver — Convergence A/R) handles ME engagement identity resolution. Farm generates synthetic data conforming to DeclaredPipe schemas and pushes through the production ingest pipeline. All 16 RACI Stub capabilities have passing integration tests. No test is skipped or marked xfail. Pre-existing failures fixed, not routed around.  
