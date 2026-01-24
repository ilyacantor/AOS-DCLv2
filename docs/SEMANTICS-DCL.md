NLQ Semantic Layer
Last Updated: January 24, 2026

Overview
The NLQ Semantic Layer is a metadata-only infrastructure that enables natural language query answerability through deterministic hypothesis ranking. It provides:

Canonical Events - System-agnostic business event types
Entity Dimensions - Business entities for grouping/filtering
Metric Definitions - Reusable metric specifications
Source Bindings - Mappings from source systems to canonical events
Proof Chains - Traceability to source system evidence
Core Principle
No LLM in the hot path. All scoring uses deterministic rules + stored metadata.

Architecture
┌────────────────────────────────────────────────────────────────────────────┐
│                        NLQ Semantic Layer                                  │
├────────────────────────────────────────────────────────────────────────────┤
│                                                                            │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                 │
│  │   Registry   │    │  Validation  │    │   Lineage    │                 │
│  │   Service    │    │   Services   │    │   Service    │                 │
│  ├──────────────┤    ├──────────────┤    ├──────────────┤                 │
│  │ • List/Search│    │ • Consistency│    │ • Dep Graph  │                 │
│  │ • Publish    │    │ • Schema     │    │ • Impact     │                 │
│  │ • Deprecate  │    │ • Coverage   │    │ • Upstream   │                 │
│  └──────────────┘    └──────────────┘    └──────────────┘                 │
│                                                                            │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                 │
│  │   Executor   │    │    Proof     │    │Answerability │                 │
│  │   Service    │    │   Resolver   │    │   Scorer     │                 │
│  ├──────────────┤    ├──────────────┤    ├──────────────┤                 │
│  │ • Compile SQL│    │ • URL Gen    │    │ • Rank       │                 │
│  │ • Execute    │    │ • Chains     │    │ • Hypotheses │                 │
│  │ • Cache      │    │ • Verify     │    │ • Explain    │                 │
│  └──────────────┘    └──────────────┘    └──────────────┘                 │
│                                                                            │
├────────────────────────────────────────────────────────────────────────────┤
│                         Persistence Layer                                  │
├────────────────────────────────────────────────────────────────────────────┤
│  JSON Fixtures (Development) ←→ PostgreSQL (Production)                   │
└────────────────────────────────────────────────────────────────────────────┘
Data Model
Canonical Events
System-agnostic business event types following the NOUN_VERB_PAST naming pattern.

{
  "id": "revenue_recognized",
  "tenant_id": "default",
  "schema_json": {
    "fields": [
      {"name": "event_id", "type": "string"},
      {"name": "amount", "type": "decimal"},
      {"name": "customer_id", "type": "string"},
      {"name": "service_line", "type": "string"},
      {"name": "occurred_at", "type": "timestamp"},
      {"name": "effective_at", "type": "timestamp"}
    ]
  },
  "time_semantics_json": {
    "occurred_at": "created_timestamp",
    "effective_at": "recognition_date",
    "calendar": "fiscal"
  }
}
Key Principle: Dual Time Axes

occurred_at - When the event was recorded (system time)
effective_at - Business effective date (e.g., revenue recognition date)
Event Categories
Category	Events
Revenue/Billing	invoice_issued, invoice_posted, revenue_recognized, payment_received, refund_issued
Subscription	subscription_started, subscription_changed, subscription_canceled
CRM	lead_created, opportunity_created, deal_won, deal_lost, customer_onboarded
Operations	work_item_created, work_item_completed, sla_breached, ticket_resolved
Engineering	deployment_completed, incident_opened, incident_resolved, slo_breached
Cloud/Security	cloud_cost_incurred, security_finding_raised, security_finding_resolved
Entities (Dimensions)
Business entities that events can be grouped/filtered by.

{
  "id": "customer",
  "tenant_id": "default",
  "identifiers_json": {
    "primary": "customer_id",
    "aliases": ["account_id", "client_id"]
  },
  "description": "Customer or account"
}
Entity Categories
Category	Entities
Business/Finance	customer, account, contract, subscription, invoice, payment, vendor, expense
Operations/Work	work_item, ticket, sla, employee, team, project
Engineering/Platform	deployment, build, repo, incident, slo, cloud_resource, security_finding
Metric Definitions
Reusable metric specifications organized by pack.

{
  "id": "arr",
  "tenant_id": "default",
  "kind": "metric",
  "pack": "cfo",
  "description": "Annual Recurring Revenue",
  "default_time_semantics_json": {
    "event": "subscription_started",
    "time_field": "effective_at",
    "calendar": "fiscal"
  }
}
Metric Packs
Pack	Focus Area	Example Metrics
CFO	Finance	recognized_revenue, arr, mrr, dso, burn_rate, gross_margin
CTO	Engineering	deploy_frequency, lead_time_for_changes, mttr, slo_attainment
COO	Operations	throughput, cycle_time, sla_compliance, backlog_health
CEO	Executive	revenue_growth, churn_rate, runway, reliability_score
Definition Versions
Versioned specifications with full computation details.

{
  "id": "arr_v1",
  "tenant_id": "default",
  "definition_id": "arr",
  "version": "v1",
  "status": "published",
  "spec": {
    "required_events": ["subscription_started", "subscription_changed", "subscription_canceled"],
    "measure": {"op": "point_in_time_sum", "field": "arr"},
    "filters": {},
    "allowed_grains": ["month", "quarter"],
    "allowed_dims": ["customer", "service_line", "region"],
    "joins": {"customer_id": "customer"},
    "time_field": "effective_at"
  }
}
Measure Operations
Operation	Description	Example Use
sum	Simple sum	Total revenue
count	Count records	Number of deals
avg	Average	Average deal size
ratio	A / B ratio	Win rate
point_in_time_sum	Balance at point in time	ARR, MRR
cohort_retention	Retention by cohort	Customer retention
event_sourced_balance	Running balance from events	AR balance
avg_days_between	Average duration	DSO, lead time
period_over_period_growth	Growth rate	Revenue growth
net_count	Adds minus subtracts	Net new customers
difference	A - B	Variance
Bindings
Map source systems to canonical events.

{
  "id": "netsuite_revenue_recognized",
  "tenant_id": "default",
  "source_system": "NetSuite",
  "canonical_event_id": "revenue_recognized",
  "mapping_json": {
    "transaction_id": "event_id",
    "amount": "amount",
    "customer_id": "customer_id",
    "tran_date": "occurred_at",
    "rev_rec_date": "effective_at"
  },
  "dims_coverage_json": {
    "customer": true,
    "service_line": true,
    "region": false
  },
  "quality_score": 0.92,
  "freshness_score": 0.95
}
Supported Source Systems
System	Event Types
NetSuite	Revenue, invoices, payments, vendor bills
Salesforce	Opportunities, deals, leads
Chargebee	Subscriptions
Jira	Work items
GitHub	Deployments
PagerDuty	Incidents
AWS Cost Explorer	Cloud costs
Zendesk	Tickets
Snyk	Security findings
Expensify	Expenses
Services
DefinitionRegistry
Catalog management with search and publish workflow.

from backend.nlq import DefinitionRegistry
registry = DefinitionRegistry()
# List with filtering
summaries, total = registry.list_definitions(
    tenant_id="default",
    pack="cfo",
    status="published"
)
# Search
results = registry.search_definitions("revenue")
# Get details with lineage
detail = registry.get_definition_detail("arr")
# Publish workflow
success, errors = registry.publish_definition("arr", "v2")
ConsistencyValidator
Validates semantic layer integrity.

from backend.nlq import ConsistencyValidator
validator = ConsistencyValidator()
# Run all checks
report = validator.run_all_checks("default")
print(f"Status: {report.overall_status}")
print(f"Issues: {report.total_issues}")
# Individual checks
result = validator.check_orphan_events("default")
result = validator.check_binding_coverage("default")
result = validator.check_entity_references("default")
Checks Performed
Check	Description	Severity
orphan_events	Events without bindings	Warning
orphan_definitions	Definitions missing events/versions	Error
orphan_bindings	Bindings referencing missing events	Error
circular_dependencies	Cycles in dependencies	Error
binding_coverage	Incomplete dimension coverage	Warning
entity_references	Invalid entity references	Warning
version_consistency	Multiple published versions	Error
LineageService
Track dependencies and analyze impact.

from backend.nlq import LineageService
lineage = LineageService()
# Full graph
graph = lineage.build_full_graph("default")
print(f"Nodes: {len(graph.nodes)}, Edges: {len(graph.edges)}")
# Definition lineage
result = lineage.get_definition_lineage("arr")
# Returns: events, bindings, source_systems
# Impact analysis
impact = lineage.analyze_impact("event", "revenue_recognized")
print(f"Severity: {impact.severity}")
print(f"Affected: {impact.total_affected}")
SchemaEnforcer
Validate schemas before creation.

from backend.nlq import SchemaEnforcer
enforcer = SchemaEnforcer()
# Validate all
result = enforcer.validate_all("default")
print(f"Valid: {result.valid}, Errors: {result.errors}")
# Validate new event
valid, errors = enforcer.validate_event(
    event_id="new_event",
    schema_json={"fields": [...]},
    time_semantics_json={...}
)
# Get suggestions
suggestions = enforcer.suggest_schema_improvements("invoice_posted")
QueryExecutor
Execute queries with caching and audit.

from backend.nlq import QueryExecutor
executor = QueryExecutor()
# Execute for definition
result = executor.execute_definition(
    definition_id="arr",
    dims=["customer", "region"],
    time_window="QoQ"
)
if result.status == ExecutionStatus.COMPLETED:
    for row in result.rows:
        print(row)
# Audit log
audits = executor.get_audit_log(definition_id="arr")
# Stats
stats = executor.get_execution_stats()
print(f"Total: {stats['total_executions']}")
ProofResolver
Generate source system URLs.

from backend.nlq import ProofResolver
resolver = ProofResolver()
# Resolve proofs for definition
proofs = resolver.resolve_definition_proofs("services_revenue")
for proof in proofs:
    print(f"{proof.system}: {proof.url}")
# Build full chain
chain = resolver.build_proof_chain("services_revenue")
print(f"Query hash: {chain.query_hash}")
print(f"Sources: {[e['source_system'] for e in chain.event_traces]}")
API Reference
Registry Endpoints
GET  /api/nlq/registry/definitions
     ?pack=cfo&status=published&search=revenue&limit=50
GET  /api/nlq/registry/definitions/search?q=revenue
GET  /api/nlq/registry/definitions/{definition_id}
POST /api/nlq/registry/definitions
     {"id": "new_metric", "kind": "metric", "pack": "cfo", "spec": {...}}
POST /api/nlq/registry/definitions/{id}/versions
     {"version": "v2", "spec": {...}}
POST /api/nlq/registry/definitions/{id}/publish
     {"version": "v2"}
POST /api/nlq/registry/definitions/{id}/deprecate
     {"version": "v1"}
Catalog Endpoints
GET  /api/nlq/registry/catalog/stats
     → total_definitions, by_pack, avg_coverage
GET  /api/nlq/registry/catalog/packs
     → [{pack: "cfo", definition_count: 16}, ...]
Consistency Endpoints
GET  /api/nlq/registry/consistency/check
     → Full report with all checks
GET  /api/nlq/registry/consistency/orphan-events
GET  /api/nlq/registry/consistency/orphan-definitions
GET  /api/nlq/registry/consistency/binding-coverage
Lineage Endpoints
GET  /api/nlq/registry/lineage/graph
     → Full dependency graph (nodes + edges)
GET  /api/nlq/registry/lineage/definition/{id}
     → Events, bindings, sources for definition
GET  /api/nlq/registry/lineage/event/{id}/consumers
     → Definitions that use this event
POST /api/nlq/registry/lineage/impact
     {"object_type": "event", "object_id": "revenue_recognized"}
     → Severity, affected objects
GET  /api/nlq/registry/lineage/upstream/{type}/{id}
GET  /api/nlq/registry/lineage/downstream/{type}/{id}
Schema Endpoints
GET  /api/nlq/registry/schema/validate
     → Validate all schemas
POST /api/nlq/registry/schema/validate/event
     {"event_id": "...", "schema_json": {...}}
POST /api/nlq/registry/schema/validate/binding
     {"binding_id": "...", "canonical_event_id": "...", "mapping_json": {...}}
GET  /api/nlq/registry/schema/suggestions/{event_id}
Execution Endpoints
POST /api/nlq/registry/execute
     {"definition_id": "arr", "dims": ["customer"], "time_window": "QoQ"}
POST /api/nlq/registry/execute/raw
     {"sql": "SELECT ...", "params": [...]}
GET  /api/nlq/registry/execute/stats
GET  /api/nlq/registry/execute/audit?definition_id=arr
DELETE /api/nlq/registry/execute/cache
Proof Endpoints
GET  /api/nlq/registry/proof/definition/{id}
     → Resolved proofs with URLs
GET  /api/nlq/registry/proof/chain/{id}
     → Full proof chain from definition to sources
GET  /api/nlq/registry/proof/coverage
     → Coverage statistics
Database Schema
PostgreSQL Tables
-- Canonical event types
CREATE TABLE canonical_events (
    id VARCHAR(128),
    tenant_id VARCHAR(64) DEFAULT 'default',
    description TEXT,
    schema_json JSONB NOT NULL DEFAULT '{}',
    time_semantics_json JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (id, tenant_id)
);
-- Business entities (dimensions)
CREATE TABLE entities (
    id VARCHAR(128),
    tenant_id VARCHAR(64) DEFAULT 'default',
    description TEXT,
    identifiers_json JSONB NOT NULL DEFAULT '{}',
    PRIMARY KEY (id, tenant_id)
);
-- Source system bindings
CREATE TABLE bindings (
    id VARCHAR(128),
    tenant_id VARCHAR(64) DEFAULT 'default',
    source_system VARCHAR(128) NOT NULL,
    canonical_event_id VARCHAR(128) NOT NULL,
    mapping_json JSONB NOT NULL DEFAULT '{}',
    dims_coverage_json JSONB NOT NULL DEFAULT '{}',
    quality_score FLOAT DEFAULT 0.5 CHECK (quality_score BETWEEN 0 AND 1),
    freshness_score FLOAT DEFAULT 0.5 CHECK (freshness_score BETWEEN 0 AND 1),
    PRIMARY KEY (id, tenant_id),
    FOREIGN KEY (canonical_event_id, tenant_id)
        REFERENCES canonical_events(id, tenant_id)
);
-- Metric/view definitions
CREATE TABLE definitions (
    id VARCHAR(128),
    tenant_id VARCHAR(64) DEFAULT 'default',
    kind VARCHAR(32) DEFAULT 'metric' CHECK (kind IN ('metric', 'view')),
    pack VARCHAR(64),
    description TEXT,
    default_time_semantics_json JSONB NOT NULL DEFAULT '{}',
    PRIMARY KEY (id, tenant_id)
);
-- Versioned definition specs
CREATE TABLE definition_versions (
    id VARCHAR(128),
    tenant_id VARCHAR(64) DEFAULT 'default',
    definition_id VARCHAR(128) NOT NULL,
    version VARCHAR(32) DEFAULT 'v1',
    status VARCHAR(32) DEFAULT 'draft' CHECK (status IN ('draft', 'published', 'deprecated')),
    spec_json JSONB NOT NULL DEFAULT '{}',
    published_at TIMESTAMP,
    PRIMARY KEY (id, tenant_id),
    FOREIGN KEY (definition_id, tenant_id)
        REFERENCES definitions(id, tenant_id),
    UNIQUE (definition_id, version, tenant_id)
);
-- Lineage tracking
CREATE TABLE lineage_edges (
    id VARCHAR(256),
    tenant_id VARCHAR(64) DEFAULT 'default',
    source_type VARCHAR(64) NOT NULL,
    source_id VARCHAR(128) NOT NULL,
    target_type VARCHAR(64) NOT NULL,
    target_id VARCHAR(128) NOT NULL,
    edge_type VARCHAR(64) NOT NULL,
    metadata_json JSONB NOT NULL DEFAULT '{}',
    PRIMARY KEY (id, tenant_id)
);
-- Query execution audit
CREATE TABLE query_executions (
    id VARCHAR(64),
    tenant_id VARCHAR(64) DEFAULT 'default',
    definition_id VARCHAR(128),
    version VARCHAR(32),
    sql_hash VARCHAR(64) NOT NULL,
    sql_text TEXT NOT NULL,
    params_json JSONB NOT NULL DEFAULT '{}',
    status VARCHAR(32) DEFAULT 'pending',
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    row_count INTEGER,
    execution_time_ms FLOAT,
    error_message TEXT,
    PRIMARY KEY (id, tenant_id)
);
Configuration
Environment Variables
# Database
NLQ_DATABASE_URL=postgresql://user:pass@localhost/dcl_nlq
# Query execution
NLQ_QUERY_CACHE_TTL=300  # seconds
NLQ_QUERY_TIMEOUT=60000  # milliseconds
# Snowflake (production)
SNOWFLAKE_ACCOUNT=xxx
SNOWFLAKE_USER=xxx
SNOWFLAKE_PASSWORD=xxx
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=DCL
SNOWFLAKE_SCHEMA=NLQ
Running Migrations
cd backend/nlq
alembic upgrade head
Best Practices
Event Design
Use NOUN_VERB_PAST naming: invoice_posted, subscription_started
Include both time axes: occurred_at (system) and effective_at (business)
Add event_id: For traceability
Use decimal for money: Avoid floating point
Definition Design
Declare valid grains: Not all metrics work at all grains
Specify time_field: occurred_at vs effective_at
Limit allowed_dims: Don't allow impossible drilldowns
Document joins: How to get to entities
Binding Design
Map all required fields: Check schema coverage
Score quality honestly: Don't inflate scores
Track freshness: How stale is this data?
Document coverage gaps: Which dims are missing?
Troubleshooting
Common Issues
Issue	Cause	Solution
"Definition not answerable"	Missing bindings	Add bindings for required events
"Dimension not available"	No binding covers dim	Update binding dims_coverage
"Low confidence score"	Weak bindings	Improve quality/freshness scores
"Missing events"	Event not registered	Register canonical event first
Checking Health
curl localhost:8000/api/nlq/registry/health
curl localhost:8000/api/nlq/registry/consistency/check
curl localhost:8000/api/nlq/registry/catalog/stats
