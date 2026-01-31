# AutonomOS (AOS) Platform - Comprehensive Technical Reference

> **Last Updated**: 2026-01-28
> **Version**: 2.0 (Comprehensive Edition)
> **Purpose**: Complete unified technical documentation across all AOS modules with full functional capabilities
> **Repos Reviewed**: All 8 modules (NLQ, Farm, DCLv2, AODv3, AAM, AOA, RevOps Agent, FinOps Agent)

---

## Table of Contents

1. [Platform Vision & Problem Statement](#1-platform-vision--problem-statement)
2. [Core Philosophy & Principles](#2-core-philosophy--principles)
3. [Architecture Overview](#3-architecture-overview)
4. [Module Deep Dives](#4-module-deep-dives)
   - [AOD - Asset Observation & Discovery](#41-aod---asset-observation--discovery)
   - [AAM - Adaptive API Mesh](#42-aam---adaptive-api-mesh)
   - [DCL - Data Connectivity Layer](#43-dcl---data-connectivity-layer)
   - [NLQ - Natural Language Query](#44-nlq---natural-language-query)
   - [AOA - Agentic Orchestration Architecture](#45-aoa---agentic-orchestration-architecture)
   - [Farm - Test Oracle Platform](#46-farm---test-oracle-platform)
   - [RevOps Agent](#47-revops-agent---revenue-operations)
   - [FinOps Agent](#48-finops-agent---cloud-cost-optimization)
5. [Canonical Data Model](#5-canonical-data-model)
6. [Enterprise Integration Patterns](#6-enterprise-integration-patterns)
7. [Business Personas](#7-business-personas)
8. [Design System](#8-design-system)
9. [Technology Stack](#9-technology-stack)
10. [API Reference](#10-api-reference)
11. [Development Guidelines](#11-development-guidelines)
12. [Deployment & Operations](#12-deployment--operations)

---

## 1. Platform Vision & Problem Statement

### 1.1 The Crisis: Digital Chaos in the Modern Enterprise

**The Scale of the Problem:**

| Metric | Value | Impact |
|--------|-------|--------|
| **Average Applications** | 1,000+ | Large enterprises manage 275-370 applications, up to 976 including Shadow IT |
| **Annual Legacy Spend** | $2.6 Trillion | Global cost just keeping legacy systems running |
| **Modernization Time** | 16 months | Average per-project, costing $1.5M each |
| **IT Budget on Maintenance** | 55-80% | Spent on maintenance and operations |
| **Shadow IT Control** | 74% | Business units control SaaS spending, not IT |

**Root Causes:**
- **Disconnected Systems**: Massive operational fragmentation across hundreds of applications
- **Slow Adaptation**: Legacy software cannot support agentic AI revolution
- **Manual Orchestration**: "Swivel chair" work manually moving data between systems
- **Shadow IT Proliferation**: 85-90% of SaaS applications operate outside IT oversight
- **Agent Chaos**: AI agents operating in silos without shared context or governance

### 1.2 The Solution: AutonomOS

**AutonomOS (AOS)** is an AI-native "operating system" for the intelligent enterprise that abstracts complexity from disparate enterprise systems to enable **intent-driven operations**.

**Core Value Proposition:**
> *"From Insight to Action, Instantly"* - We connect your enterprise's brain to its hands, creating a system that doesn't just know, but does.

**What Makes AOS Different:**

| Traditional Approach | AutonomOS Approach |
|---------------------|-------------------|
| Users navigate rigid, disconnected applications | Users engage directly with data through natural language |
| Operations require manual orchestration | Operations managed autonomously by AI systems |
| Siloed systems create complexity | Unified interface abstracts technical complexity |
| Value trapped within applications | Value shifts to the intelligence layer |

**Built on Top of Existing IT - Not Instead of It:**
AutonomOS overlays your current systems, abstracts the complexity, and delivers unified intelligence—without replatforming, rewiring, or rip-and-replace.

---

## 2. Core Philosophy & Principles

### 2.1 The Moat

> **"The Moat is NOT the Runtime. The Moat is the Data."**

**What AOS Builds (Differentiators):**
- Introspective data tools exposing DCL/AAM/AOD to agents
- Semantic schema discovery and mapping
- Cross-system lineage tracking
- Enterprise governance controls
- Fabric Plane compliance

**What AOS Buys/Integrates (Commodity):**
- Agent execution (LangGraph)
- Tool protocol (MCP)
- LLM routing (AI Gateway)

### 2.2 Core Development Principles

| Principle | Description | Implementation |
|-----------|-------------|----------------|
| **No "Green-Test Theater"** | Tests must validate real behavior | Forbidden: tests that pass while real usage fails |
| **Fail Loudly** | Surface errors explicitly | No silent fallbacks or permissive schemas |
| **Semantics over Syntax** | Behavior matches real-world meaning | Never convert errors to empty results |
| **Determinism** | Same inputs = same outputs | Seed-based generation for reproducibility |
| **Foundational Fixes Only** | Fix root causes | No workarounds, quick patches, or shortcuts |

### 2.3 Definition of DONE

Every feature must satisfy ALL four criteria:
1. **Semantics preserved** - Behavior matches real-world meaning
2. **No cheating** - No silent fallbacks, no optional-everything
3. **Proof is real** - Actual run showing failure-before / success-after
4. **Negative test included** - Verify the cheat can't return

### 2.4 Forbidden Patterns

- "All tests pass" while real usage fails
- Making schemas permissive to avoid contract mismatches
- Converting errors into empty results
- Adding hidden shortcuts that work for demos but fail IRL
- Band-aids, quick patches, or workarounds

### 2.5 User Interaction
- Main user / developer (Ilya) is the CEO and co-founder.
- Ilya is not a developer, but he knows enough about software development to be dangerous and mess up the codebase - keep him away from code.
- His IDE is Replit which he uses because its an integrated env - chat - see - debug - deploy - repeat.
- DO NOT alter the build/run configuration which has been set to deploy from Replit.
- He vowed never to set up a terminal and work in CLI env as he hates that setup, and he works from as many as 4 different machines.  So far it's working well - the entire AOS Suite has been developed via remote. 
- Now with addition of Claude Code for Web, things are proceeding much better / faster.
- Bottom line is that he hates shortcuts, bandaids, tech debt, overhead, janky code and monoliths. If you the developer avoid these things, you will be rewarded handsomely.

## Environment: Replit

This project uses Replit as IDE. Key differences from local dev:

### Port Configuration
- **Backend runs on port 5000** (Replit standard)
- Vite proxy in `vite.config.ts` must target `localhost:5000`
- Never assume port 8000 for backend

### Common Issues
- If frontend API calls fail with 404/connection refused, check vite.config.ts proxy target
- Replit uses port 5000 for the main exposed service

### Dev Preferences
- No local environment setup — everything runs in Replit
- Use Replit's built-in shell and deployment tools
- Avoid solutions requiring local CLI tools or Docker

### Environment Variables in Replit

- Replit uses Secrets (not .env files) for environment variables
- Secrets are accessed via os.environ or os.getenv() in Python, but are stored securely in Replit's Secrets tab, not in any file in the codebase
- Never create .env files — they would expose secrets in version control
- When documentation or code references .env, translate that to adding the variable in Replit's Secrets panel instead
---

## 3. Architecture Overview

### 3.1 Three-Layer Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        TAILORED APPLICATIONS                                 │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌────────────────────────┐ │
│  │  FinOps    │  │  RevOps    │  │  HROps     │  │  Custom Domain Agents  │ │
│  │  Agent     │  │  Agent     │  │  Agent     │  │  (CXOps, SecOps, etc)  │ │
│  └────────────┘  └────────────┘  └────────────┘  └────────────────────────┘ │
├─────────────────────────────────────────────────────────────────────────────┤
│                         PLATFORM SERVICES                                    │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌────────────────────────┐ │
│  │    AOA     │  │    NLQ     │  │  Control   │  │  Stress Testing &      │ │
│  │ (Orchestr) │  │  (Query)   │  │   Center   │  │  Simulation (Farm)     │ │
│  └────────────┘  └────────────┘  └────────────┘  └────────────────────────┘ │
├─────────────────────────────────────────────────────────────────────────────┤
│                     OPERATIONAL INFRASTRUCTURE                               │
│  ┌────────────┐  ┌────────────┐  ┌────────────────────────────────────────┐ │
│  │    AOD     │  │    AAM     │  │              DCL                       │ │
│  │ (Discover) │  │ (Connect)  │  │            (Unify)                     │ │
│  │            │  │            │  │                                        │ │
│  │ What runs? │  │ How to     │  │  What does this data mean to the      │ │
│  │            │  │ connect?   │  │  business?                            │ │
│  └────────────┘  └────────────┘  └────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         FABRIC PLANE MESH                                    │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │
│  │   iPaaS     │  │ API Gateway │  │  Event Bus  │  │  Data Warehouse     │ │
│  │   Plane     │  │   Plane     │  │   Plane     │  │     Plane           │ │
│  │  ─────────  │  │  ─────────  │  │  ─────────  │  │  ─────────          │ │
│  │  Workato    │  │  Kong       │  │  Kafka      │  │  Snowflake          │ │
│  │  MuleSoft   │  │  Apigee     │  │  EventBridge│  │  BigQuery           │ │
│  │  Tray.io    │  │  AWS GW     │  │  Pulsar     │  │  Redshift           │ │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 Data Flow Pipeline

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Source    │     │    AOD      │     │    AAM      │     │    DCL      │
│   Systems   │────▶│  (Discover) │────▶│  (Connect)  │────▶│  (Unify)    │
│             │     │             │     │             │     │             │
│ Salesforce  │     │ Find what   │     │ Establish   │     │ Map to      │
│ SAP, NetSuite│    │ exists      │     │ connections │     │ ontology    │
│ 1000+ apps  │     │             │     │             │     │             │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
                                                                   │
                    ┌──────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Domain    │◀────│  Canonical  │◀────│    NLQ      │
│   Agents    │     │   Streams   │     │  (Query)    │
│             │     │             │     │             │
│ FinOps      │     │ Unified     │     │ "What's     │
│ RevOps      │     │ business    │     │ the margin?"|
│ Custom      │     │ entities    │     │             │
└─────────────┘     └─────────────┘     └─────────────┘
```

### 3.3 Component Responsibilities Matrix

| Layer | Component | Core Question | Responsibility |
|-------|-----------|---------------|----------------|
| Infrastructure | **AOD** | "What runs here?" | Discovers, catalogs, scores everything in the environment |
| Infrastructure | **AAM** | "How do we connect?" | Self-healing connection mesh, manages authentication |
| Infrastructure | **DCL** | "What does this mean?" | Semantic translator - maps fields to business concepts |
| Platform | **AOA** | "Who does what?" | RACI-compliant workflow orchestration |
| Platform | **NLQ** | "What do you need?" | Natural language interface with persona classification |
| Applications | **Agents** | "Execute the intent" | Domain-specific actions consuming canonical streams |

---

## 4. Module Deep Dives

### 4.1 AOD - Asset Observation & Discovery

**Purpose:** The discovery engine answering "What software assets does this organization actually use?"

#### 4.1.1 The Problem AOD Solves

Enterprises typically have 1,000+ applications, but IT only knows about a fraction:
- **Shadow IT**: Apps employees adopted without IT approval
- **Zombie Assets**: Licensed software nobody uses but still costs money
- **Ungoverned Systems**: Critical tools operating outside security controls

#### 4.1.2 Core Functional Capabilities

**1. Multi-Source Discovery**
| Source Type | Examples | Signals Extracted |
|-------------|----------|-------------------|
| Identity Providers | Okta, Azure AD | User access patterns, SSO coverage |
| Expense Systems | Concur, Expensify | Software spending, vendor relationships |
| Browser Telemetry | - | Actual application usage |
| Network Logs | - | Traffic patterns, shadow IT detection |
| CMDBs | ServiceNow | Official asset inventory |
| Cloud Inventories | AWS, Azure, GCP | Infrastructure assets |

**2. Governance Classification**

| Classification | Definition | Action Required |
|----------------|------------|-----------------|
| **Governed** | Has Visibility (CMDB) + Validation (SSO) + Control (lifecycle) | Monitor |
| **Shadow IT** | In use but missing from official inventories | Security review |
| **Zombie** | In CMDB/licensed but no recent activity | Cost optimization |

**3. Finding Generation**

| Finding Type | Severity | Description |
|--------------|----------|-------------|
| **Identity Gap** | 🔴 Red | Users accessing app without SSO - security risk |
| **Finance Gap** | 🟡 Yellow | No expense records for paid service |
| **Data Conflict** | 🟡 Yellow | Multiple sources disagree on ownership |
| **Stale Activity** | 🟡 Yellow | No usage in 90+ days - zombie candidate |

**4. Triage Workflow**

| Workqueue | Purpose | Possible Actions |
|-----------|---------|------------------|
| **Firewall** | Security-critical Shadow IT | Sanction, Ban |
| **Risk** | Compliance gaps | Review, Escalate |
| **Hygiene** | Zombie cleanup | Deprovision, Archive |

**5. System of Record (SOR) Detection**
- Identifies authoritative data sources for specific domains (HR, Finance, CRM)
- Uses signal-based scoring: CMDB flags, known SOR vendors, middleware presence
- Outputs confidence bands (high/medium/low)

**6. Fabric Plane Detection**
- Recognizes integration "motherships" (MuleSoft, Workato, Snowflake, Kafka, Kong)
- Tags assets with `connected_via_plane` routing
- Enables efficient AAM connection strategy

**7. AAM Handoff**
- Exports `ConnectionCandidates` with execution signals
- `execution_allowed` + `action_type` fields
- Blocking findings → `inventory_only` (human review required)
- Clear/overridden → `provision` (safe for auto-connection)

#### 4.1.3 DiscoveryScan Pipeline

7-stage sequential pipeline:
1. **Validation** - Verify input data integrity
2. **Normalization** - Standardize domains, names, identifiers
3. **Indexing** - Build searchable asset registry
4. **Correlation** - Match signals to unique assets across sources
5. **Admission** - Apply governance rules, generate findings
6. **Artifact Handling** - SOR scoring, fabric plane detection
7. **Output** - Produce catalog and ConnectionCandidates

#### 4.1.4 Key APIs

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/catalog/runs/{run_id}` | GET | Fetch assets for a discovery run |
| `/api/catalog/assets/{id}/provisioning` | POST | Apply triage action to asset |
| `/api/triage/data/{run_id}` | GET | Get triage workqueue for run |
| `/api/triage/action` | POST | Record triage decision |
| `/api/handoff/aam/candidates` | POST | Export ConnectionCandidates to AAM |
| `/api/policy/manifest` | GET | Export governance rules for AAM |
| `/api/runs/{run_id}/derived` | GET | Get derived classifications |

---

### 4.2 AAM - Adaptive API Mesh

**Purpose:** Self-healing integration mesh that observes, documents, and maintains enterprise data pipes.

#### 4.2.1 Core Philosophy

AAM's philosophy is to make data pipe behavior and meaning explicit **without changing how data moves**. It connects to enterprise Fabric Planes to:
- Inventory reusable data pipes
- Infer metadata
- Detect drift (schema changes, connectivity loss)
- Automatically self-heal connectivity issues
- Publish clean pipe inventory for downstream systems

#### 4.2.2 What AAM Does vs. Does NOT Do

| ✅ AAM Does | ❌ AAM Does NOT |
|-------------|-----------------|
| Observe and document integration fabrics | Move or transform data |
| Self-heal connectivity issues | Act as iPaaS replacement |
| Publish pipe inventory | Build per-app SaaS connectors |
| Detect schema drift | Handle infrastructure operations |

#### 4.2.3 Connectivity Modalities

| Mode | Description | Use Case |
|------|-------------|----------|
| **Control-Plane Attachment** | Read-only visibility into APIs, integrations, ownership | Primary enterprise pattern |
| **Declared Interface Consumption** | MuleSoft System APIs or enterprise-approved APIs | Standardized access |
| **Passive Subscription** | Kafka topics, Event Hub, Snowflake tables/streams | Event-driven data |
| **Minimal Tee** | One additional sink added to existing flow | Explicit enablement only |

#### 4.2.4 Fabric Plane Integrations

| Plane Type | Systems | Capabilities |
|------------|---------|--------------|
| **iPaaS** | Workato, MuleSoft, Tray.io, Celigo | Webhook signals, recipe changes |
| **API Gateway** | Kong, Apigee, AWS API Gateway | API catalogs, traffic patterns |
| **Event Bus** | Kafka, EventBridge, Pulsar | Schema registries, topic metadata |
| **Data Warehouse** | Snowflake, BigQuery, Redshift | Table schemas, freshness metadata |

#### 4.2.5 Core Capabilities

**1. Pipe Discovery**
- Automatic detection of existing integration endpoints
- Protocol inference (REST, GraphQL, SOAP, gRPC)
- Ownership and responsibility mapping

**2. Schema Drift Detection**
- Fingerprinting for change detection
- Automatic alerting on breaking changes
- Historical tracking of schema evolution

**3. Self-Healing**
- Connectivity restoration
- Automatic retry with backoff
- Health monitoring and alerting

**4. Enterprise Maturity Presets**
- iPaaS-centric configuration
- Warehouse-centric configuration
- API Gateway-centric configuration
- Hybrid configurations

**5. Candidate Workflow**
- New data source evaluation
- Governance rule application
- Automated or HITL approval

#### 4.2.6 Key APIs

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/dcl/connectors` | GET | List all connectors with status |
| `/api/dcl/connectors/{name}` | GET | Connector details with live data |
| `/api/pipes` | GET | List discovered pipes |
| `/api/pipes/{id}/health` | GET | Pipe health status |
| `/api/candidates` | GET/POST | Manage new source candidates |
| `/api/health` | GET | Overall system health |

---

### 4.3 DCL - Data Connectivity Layer

**Purpose:** Semantic mapping and visualization platform answering "Where does our data come from, what does it mean, and who uses it?"

#### 4.3.1 The Core Question DCL Answers

> *"What does this field mean to the business?"*

DCL is a semantic translator that takes cryptic field names (`KUNNR`, `acct_id`, `cust_rev_ytd`), maps them to business concepts (`Account`, `Revenue`), and shows who in the business uses each concept.

#### 4.3.2 Interactive Sankey Visualization

4-layer data flow diagram:

| Layer | Name | Purpose | Examples |
|-------|------|---------|----------|
| **L0** | Pipeline | Entry point showing data mode | Demo Pipeline, Farm Pipeline |
| **L1** | Sources | Connected data systems | Salesforce CRM, SAP ERP, MongoDB |
| **L2** | Ontology | Business concepts | Revenue, Cost, Account, Opportunity |
| **L3** | Personas | Who uses this data | CFO, CRO, COO, CTO |

#### 4.3.3 Operating Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| **Demo** | Pre-configured schemas from CSV files | Training, demonstrations, testing |
| **Farm** | Live schemas from AOS-Farm API | Production data discovery |

| Run Mode | Mapping Strategy | Speed | Accuracy |
|----------|------------------|-------|----------|
| **Dev** | Heuristic pattern matching | Fast (~1s) | Good for common patterns |
| **Prod** | AI-powered semantic matching with RAG | Slower (~5s) | Higher accuracy |

#### 4.3.4 Business Concepts (L2 Ontology)

| Concept | Description | Common Field Patterns |
|---------|-------------|----------------------|
| **Account** | Customer/company entity | account_id, company_name, customer |
| **Opportunity** | Sales pipeline deals | opportunity_id, deal_stage, amount |
| **Revenue** | Income metrics | revenue, sales, mrr, arr |
| **Cost** | Expense tracking | cost, expense, spend |
| **Date/Timestamp** | Time dimensions | created_at, updated_at, date |
| **Health Score** | Customer health metrics | health_score, nps, satisfaction |
| **Usage Metrics** | Product usage data | usage, active_users, sessions |
| **AWS Resource** | Cloud infrastructure | instance_id, resource_arn |

#### 4.3.5 Data Sources (L1 Layer)

**CRM Systems:**
- Salesforce CRM, HubSpot CRM, Microsoft Dynamics CRM

**ERP Systems:**
- SAP ERP, NetSuite ERP

**Databases:**
- MongoDB Customer DB, Supabase App DB, Legacy SQL

**Data Warehouse:**
- DW Dim Customer (dimensional customer data)

**Integration Platforms:**
- MuleSoft ERP Sync (real-time streaming)

#### 4.3.6 Core Capabilities

**1. Auto-Discovery**
- Finds schemas across source systems automatically
- Protocol detection and adaptation

**2. AI-Powered Mapping**
- Gemini / OpenAI for semantic understanding
- Near-perfect accuracy matching fields to business concepts
- Confidence scoring with validation prompts

**3. Intelligent Learning**
- Learns from every mapping decision
- Low confidence triggers AI validation
- Continuous improvement over time

**4. Real-Time Visualization**
- Interactive Sankey diagram shows data flow instantly
- Color-coded nodes by layer
- Link opacity shows flow strength

**5. Source Normalization**
- 34 canonical sources from potentially hundreds of raw sources
- Deduplication and alias resolution

#### 4.3.7 Zero-Trust Security

> **DCL never stores your data. Ever.**

| Stores | Never Stores |
|--------|--------------|
| Schema metadata | Row data |
| Mapping decisions | Customer records |
| Pointers | Actual payloads |

#### 4.3.8 Key APIs

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/dcl/run` | POST | Execute pipeline (data_mode, run_mode, personas) |
| `/api/dcl/narration/{session_id}` | GET | Poll narration messages |
| `/api/dcl/batch-mapping` | POST | Run semantic mapping batch |
| `/api/topology` | GET | Unified topology graph |
| `/api/topology/health` | GET | Connection health data |

---

### 4.4 NLQ - Natural Language Query

**Purpose:** Transform business intelligence into natural conversation, eliminating complexity of data analysis.

#### 4.4.1 Core Value Proposition

Instead of wrestling with SQL queries or navigating labyrinthine dashboards, simply type questions like "what's the margin?" or "how's pipeline looking?" and get instant answers.

#### 4.4.2 Natural Language Understanding

NLQ's intelligent engine interprets casual business questions with remarkable accuracy:
- "churn?" → Customer churn metrics
- "are we profitable?" → P&L summary
- "show me Q3 performance" → Quarterly report

**Supported Domains:**
- Finance (Revenue, Cost, Margin, Cash)
- Sales (Pipeline, Opportunities, Win Rate)
- Operations (Usage, Health, SLAs)
- HR (Headcount, Retention, Engagement)

#### 4.4.3 Dual Visualization Modes

**Galaxy View:**
- Interactive node-based visualization
- Primary answer with semantically related metrics
- Color-coded confidence indicators:
  - 🟢 Green: High confidence
  - 🟡 Yellow: Medium confidence
  - 🔴 Red: Low confidence

**Text View:**
- Structured responses with values, units, time periods
- Confidence scores
- Parsed intent for transparency

#### 4.4.4 Multi-Persona Dashboards

Each persona sees only what matters to them:

| Persona | Focus Areas | Key Questions |
|---------|-------------|---------------|
| **CFO** | Revenue, Cost, Margin, Cash, Risk | "What's our actual MRR?" |
| **CRO** | Accounts, Pipeline, Opportunities, Churn | "Which deals are at risk?" |
| **COO** | Usage, Health, SLAs, Incidents | "Are we meeting SLAs?" |
| **CTO** | Assets, Cloud Resources, Tech Debt, Security | "What shadow IT exists?" |
| **People** | Headcount, Retention, Engagement | "What's our turnover rate?" |

#### 4.4.5 Key APIs

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `POST /v1/query` | POST | Text response |
| `POST /v1/query/galaxy` | POST | Galaxy view with nodes |
| `GET /v1/history` | GET | Query history |

---

### 4.5 AOA - Agentic Orchestration Architecture

**Purpose:** Runtime orchestration layer managing AI agent workflows across enterprise systems with RACI compliance.

#### 4.5.1 What is AOA?

AOA provides:
- **Unified Task Orchestration**: Single runtime for all agent tasks with priority queuing
- **Fabric Plane Mesh Integration**: Routes ALL actions through enterprise integration planes (NOT direct SaaS connections)
- **RACI Compliance**: Every action has clear Responsible, Accountable, Consulted, Informed roles
- **Multi-Agent Coordination**: A2A protocol for agent-to-agent communication
- **PII Protection**: Detection and policy enforcement at context sharing ingress

#### 4.5.2 Implementation Status

| Component | Status | Description |
|-----------|--------|-------------|
| **AOARuntime** | ✅ COMPLETE | Unified runtime (TaskQueue + WorkerPool) |
| **AOAScheduler** | ✅ COMPLETE | Fabric-aware job scheduling |
| **A2A Protocol** | ✅ COMPLETE | Agent-to-agent messaging |
| **Context Sharing** | ✅ COMPLETE | PII detection at ingress |
| **Fabric Routing** | ✅ COMPLETE | ActionRouter with 6 Enterprise Presets |
| **RACI Audit Logging** | ✅ COMPLETE | All actions logged with Primary_Plane_ID |
| **MCP Servers** | 🔜 PLANNED | DCL/AAM/AOD as Model Context Protocol |
| **LangGraph Integration** | 🔜 PLANNED | Durable execution with checkpointing |

#### 4.5.3 Core Components

**1. AOA Runtime**

```python
class AOARuntime:
    """
    Unified AOA Runtime combining TaskQueue and WorkerPool management.
    RACI: AOA is RESPONSIBLE for runtime orchestration.
    """
    async def submit_task(self, task: AOATask) -> str
    async def submit_fabric_action(self, target_system, action_type, entity_id, data) -> str
```

Features:
- Task priority queuing (CRITICAL, HIGH, NORMAL, LOW, BACKGROUND)
- Worker pool auto-scaling
- Fabric context enrichment on all tasks
- RACI metadata on every task

**2. AOA Scheduler**

```python
class AOAScheduler:
    """
    Fabric-aware job scheduler.
    Schedule Types: ONCE, INTERVAL, HOURLY, DAILY, CRON
    """
    async def schedule_daily(self, name, hour, minute=0, payload=None) -> ScheduledJob
```

Features:
- Multiple schedule types
- Automatic fabric context injection
- Job pause/resume/cancel
- Max runs limit

**3. A2A Protocol**

Agent-to-agent communication with fabric routing:
- Message Types: EXECUTE, DELEGATE, CONTEXT_SHARE, QUERY, RESPONSE
- PII filtering at context sharing boundaries
- Correlation tracking across agent chains

#### 4.5.4 Fabric Plane Presets

| Preset | Primary Plane | Use Case |
|--------|---------------|----------|
| **IPAAS_CENTRIC** | Workato/MuleSoft | Complex workflow orchestration |
| **API_GATEWAY_CENTRIC** | Kong/Apigee | API-first organizations |
| **EVENT_BUS_CENTRIC** | Kafka/EventBridge | Event-driven architectures |
| **WAREHOUSE_CENTRIC** | Snowflake/BigQuery | Analytics-heavy workloads |
| **DIRECT** | P2P API | Scrappy/dev/test only |
| **CUSTOM** | Enterprise-specific | Custom iPaaS/on-prem gateway |

#### 4.5.5 RACI Compliance

Every task includes:
```python
@dataclass
class AOATask:
    raci_responsible: str = "AOA"  # Who does the work
    raci_accountable: str           # Who owns the outcome
    raci_consulted: List[str]       # Who provides input
    raci_informed: List[str]        # Who needs to know
```

#### 4.5.6 PII Protection

Policy enforcement at context sharing ingress:

| Policy | Action |
|--------|--------|
| **BLOCK** | Reject message with PII |
| **REDACT** | Remove PII, continue |
| **WARN** | Log warning, continue |
| **ALLOW** | Pass through |

#### 4.5.7 Key APIs

**Runtime APIs:**
| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/v1/aoa/tasks` | POST | Submit a task |
| `/api/v1/aoa/tasks/{id}` | GET | Get task status |
| `/api/v1/aoa/tasks/{id}` | DELETE | Cancel task |
| `/api/v1/aoa/metrics` | GET | Get runtime metrics |

**Scheduler APIs:**
| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/v1/aoa/jobs` | POST | Schedule a job |
| `/api/v1/aoa/jobs/{id}/pause` | POST | Pause job |
| `/api/v1/aoa/jobs/{id}/resume` | POST | Resume job |

**Fabric APIs:**
| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/v1/fabric/context` | GET | Get current fabric context |
| `/api/v1/fabric/preset` | POST | Set active preset |
| `/api/v1/fabric/planes` | GET | List available planes |

---

### 4.6 Farm - Test Oracle Platform

**Purpose:** Generate test data with known correct answers so you can verify other systems produce the right results.

#### 4.6.1 Core Philosophy

Farm is the "answer key" for testing enterprise software. Same seed + scale = identical output, enabling:
- Reproducible bug reports
- Consistent CI/CD testing
- Cross-environment validation

#### 4.6.2 Modules Served

**AOD Module: Discovery Testing**
| Capability | Description |
|------------|-------------|
| Generate Enterprise Snapshots | 7 correlated data planes (accounts, contacts, deals, etc.) |
| Set Scale & Reproducibility | Small/medium/large with deterministic seeds |
| Grade Discovery Results | Built-in answer key with expected outcomes |
| Run Reconciliations | Precision/recall/accuracy scoring |

**AOA Module: Agent Stress Testing**
| Capability | Description |
|------------|-------------|
| Generate Agent Fleets | Planners, Workers, Specialists, Reviewers |
| Generate Workflows | Linear, DAG, Parallel, Saga patterns |
| Inject Chaos | tool_timeout, agent_conflict, resource_exhaustion, partial_failure |
| Run Stress Tests | Execute scenarios, capture results, validate |

**NLQ Module: Query Validation**
| Capability | Description |
|------------|-------------|
| Generate Business Scenarios | Customers, invoices, vendors, assets |
| Get Ground Truth Metrics | Pre-computed correct answers |
| Use Question Bank | 100 questions across 23 categories |
| Validate NLQ Output | Compare against ground truth |

**DCL Module: Data Ingestion Testing**
| Capability | Description |
|------------|-------------|
| Generate Toxic Streams | missing_fields, duplicate_invoice, incorrect_currency, stale_timestamp, orphaned_reference |
| Detect Chaos | Metadata describing what's wrong |
| Fetch Pristine Source | Original "clean" version |
| Verify Repairs | Field-by-field validation |

#### 4.6.3 Key APIs

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/snapshots` | POST | Generate new snapshot |
| `/api/reconcile` | POST | Run reconciliation |
| `/api/agents/fleet` | POST | Generate agent fleet |
| `/api/agents/stress-test-runs` | POST | Execute stress test |
| `/api/scenarios/generate` | POST | Generate business scenario |
| `/api/nlq/questions` | GET | Get test question bank |
| `/api/stream/synthetic/mulesoft` | GET | Get toxic data stream |
| `/api/verify/salesforce/invoice` | POST | Verify repaired record |

---

### 4.7 RevOps Agent - Revenue Operations

**Purpose:** CRM data validation and pipeline health monitoring dashboard.

#### 4.7.1 Core Capabilities

**1. Dashboard (Pipeline Health)**
| Feature | Description |
|---------|-------------|
| Key Metrics | 8 KPIs: pipeline value, win rate, average deal size, etc. |
| Pipeline Breakdown | Visual charts by stage |
| High-Risk Deals | Risk score > 70 flagged for attention |
| Slack Alerts | Notify team about at-risk opportunities |

**Key Metrics Explained:**
- **Health Score** (0-100): Higher = better deal quality
- **Risk Score** (0-100): Lower = better, likelihood of problems
- **Pipeline Velocity**: How fast deals move through pipeline
- **Stalled Deals**: No activity for 14+ days

**2. Operations (CRM Integrity)**
| Feature | Description |
|---------|-------------|
| BANT Validation | Budget, Authority, Need, Timeline completeness |
| Data Quality Scores | Completeness scoring per opportunity |
| Missing Field Detection | Identify incomplete records |
| Risk Level Filtering | Low, Medium, High segmentation |

**3. Connectivity (Data Sources)**
| Feature | Description |
|---------|-------------|
| Real-Time Status | Healthy, degraded, or offline |
| Live vs Mock Mode | Indicator for credential status |
| Connector Details | Overview, Health, Live Data tabs |

#### 4.7.2 Operating Modes

| Mode | Description | When to Use |
|------|-------------|-------------|
| **LIVE** | Real data from connected services | Production with credentials |
| **DEMO** | Realistic mock data | Demos, testing, no credentials |

#### 4.7.3 Data Sources

- **Salesforce**: CRM deals and opportunities
- **Supabase**: Customer health scores and engagement
- **MongoDB**: User activity and usage analytics

---

### 4.8 FinOps Agent - Cloud Cost Optimization

**Purpose:** Intelligent AWS cloud cost optimization with automated recommendations.

#### 4.8.1 Core Value Proposition

> Reduce cloud costs by 20-40% through automated analysis and intelligent recommendations, while maintaining full control over high-risk changes.

#### 4.8.2 Core Capabilities

**1. Automated Cost Analysis**
| Feature | Description |
|---------|-------------|
| Resource Monitoring | EC2 instances, RDS databases, Redshift clusters |
| Utilization Analysis | CPU, memory, storage tracking |
| Cost Tracking | Monthly and YTD spending trends |
| Waste Detection | Percentage of spend that could be optimized |

**2. Intelligent Recommendations**

| Type | Action | Example |
|------|--------|---------|
| **Rightsizing** | Downsize underutilized instances | "m5.xlarge → m5.large saves $150/mo" |
| **Scheduling** | Stop resources during off-hours | "Stop dev DB outside hours saves $200/mo" |
| **Storage Tiering** | Move to cheaper storage classes | "gp2 → gp3 saves $50/mo" |

**3. Dual Execution Mode**

| Mode | Risk Level | Behavior | Default Split |
|------|------------|----------|---------------|
| **Autonomous** | Low | Executes automatically | 80% |
| **HITL** | High | Requires manual approval | 20% |

**4. Executive Dashboard**
| Metric | Description |
|--------|-------------|
| Monthly Spend | Current month's cloud costs |
| YTD Spend | Year-to-date total |
| Identified Savings | Total potential found |
| Realized Savings | Actual savings executed |
| Waste Optimized % | Percentage eliminated |

**5. Approval Workflows**
- **Pending** → **Approved** → **Executed** (or **Rejected**)
- Multi-stage approval for high-impact changes
- Slack notifications

#### 4.8.3 Operating Modes

| Mode | Description |
|------|-------------|
| **Simulation** | Synthetic data, no AWS credentials, 3-second cycles |
| **Production** | Real AWS, AI-powered analysis, configurable schedules |

#### 4.8.4 AWS Integrations

| Service | Purpose |
|---------|---------|
| Cost Explorer | Cost data retrieval |
| CloudWatch | Utilization metrics |
| Trusted Advisor | Optimization checks |

---

## 5. Canonical Data Model

### 5.1 Core Concepts

| Term | Definition |
|------|------------|
| **Asset** | Anything that RUNS: has runtime/process, can be up/down, consumes resources. NOT assets: files, docs, repos |
| **Source** | Logical system-of-record for business entities. About meaning, not infrastructure |
| **Ontology** | Formal definition of business entities and relationships |
| **Canonical Entity** | Unified, deduplicated representation of real-world thing across sources |
| **Canonical Stream** | Time-ordered stream of canonical entities for agents/dashboards |
| **Fabric Plane** | Integration "motherships" that aggregate connections |

### 5.2 Source Types

| Type | Role | Is DCL Source? |
|------|------|----------------|
| **SYSTEM_OF_RECORD** | Where events originate (Salesforce, Stripe) | ✅ Yes |
| **CURATED** | Cleaned/modeled warehouse tables | ✅ Yes |
| **AGGREGATED** | Rollups for reporting | Usually No |
| **CONSUMER_ONLY** | Read-only visualization (Tableau) | ❌ No |

### 5.3 Governance Trinity

An asset is **governed** if it has:
1. **Visibility**: In CMDB
2. **Validation**: SSO/IdP enabled
3. **Control**: Vendor-managed lifecycle

---

## 6. Enterprise Integration Patterns

### 6.1 How AutonomOS Connects

AutonomOS does NOT connect to every application. Instead:

1. **Connect once** to the integration fabric (e.g., MuleSoft)
2. **Discover existing** APIs, flows, topics, and sinks
3. **Consume existing outputs**: System APIs, API Gateways, Kafka topics, Snowflake tables
4. **Unify and govern** in DCL

> MuleSoft keeps the keys. AutonomOS consumes the results.

### 6.2 Connectivity Modalities

| Modality | Description | Enterprise Scale? |
|----------|-------------|-------------------|
| **Control-Plane Attachment** | Read-only visibility into APIs, integrations | ✅ Yes |
| **Declared Interface Consumption** | MuleSoft System APIs or approved APIs | ✅ Yes |
| **Passive Subscription** | Kafka topics, Event Hub, Snowflake streams | ✅ Yes |
| **Minimal Tee** | One additional sink added to existing flow | ✅ With approval |
| **Direct P2P** | Point-to-point API calls | ❌ Scrappy only |

---

## 7. Business Personas

### 7.1 Executive Persona Views

| Persona | Focus Domain | Key Metrics | Sample Questions |
|---------|--------------|-------------|------------------|
| **CFO** | Finance | Revenue, Cost, Margin, Cash, Risk | "What's our actual MRR?" "Where are we over-spending?" |
| **CRO** | Sales | Accounts, Pipeline, Opportunities, Churn | "Which deals are at risk?" "What's our true pipeline?" |
| **COO** | Operations | Usage, Health, SLAs, Incidents | "Are we meeting SLAs?" "What's affecting customer health?" |
| **CTO** | Technology | Assets, Cloud, Tech Debt, Security | "What shadow IT exists?" "Where's our infrastructure risk?" |
| **People** | HR | Headcount, Retention, Engagement | "What's our turnover rate?" "Which teams are growing?" |

### 7.2 Domain Colors

| Persona | Primary Color | Accent Color |
|---------|---------------|--------------|
| CFO | `#10B981` (Emerald) | `#059669` |
| CRO | `#3B82F6` (Blue) | `#2563EB` |
| COO | `#F59E0B` (Amber) | `#D97706` |
| CTO | `#A855F7` (Purple) | `#7C3AED` |
| People | `#EC4899` (Pink) | `#DB2777` |

---

## 8. Design System

### 8.1 Canonical Color Palette

```css
/* Primary Accent - THE BRAND COLOR */
--aos-cyan: #0BCAD9;

/* Backgrounds */
--aos-bg-black: #000000;
--aos-bg-slate-950: #020617;
--aos-bg-slate-900: #0f172a;
--aos-bg-enterprise: #0A2540;

/* Borders */
--aos-border-blue: #1E4A6F;
--aos-border-slate: #334155;

/* Text */
--aos-text-white: #FFFFFF;
--aos-text-secondary: #A0AEC0;
--aos-text-muted: #64748b;

/* Status Colors */
--aos-success: #22C55E;
--aos-warning: #F59E0B;
--aos-error: #EF4444;
--aos-info: #3B82F6;
```

### 8.2 Typography

| Element | Font | Size | Weight |
|---------|------|------|--------|
| Headings | Quicksand | 24-32px | 600-700 |
| Body | Quicksand | 14-16px | 400 |
| Code | JetBrains Mono | 13px | 400 |
| Labels | Quicksand | 12px | 500 |

### 8.3 Component Patterns

| Component | Pattern |
|-----------|---------|
| Cards | Dark slate with cyan border accent |
| Buttons | Cyan primary, slate secondary |
| Inputs | Dark background, cyan focus ring |
| Tables | Alternating row shading, cyan headers |
| Notifications | Minimal cyan toast messages |

---

## 9. Technology Stack

### 9.1 Frontend Stack

| Module | React | Build | CSS | UI Library |
|--------|-------|-------|-----|------------|
| NLQ | 18 | Vite | Tailwind v4 | Custom |
| DCL | 18 | Vite | CSS Modules | D3.js |
| AOD | - | Jinja2 | Tailwind | Vanilla JS |
| AAM | - | Jinja2 | Tailwind | Vanilla JS |
| RevOps | 19 | Vite 7 | Tailwind v4 | Recharts |
| FinOps | 18 | Vite | Tailwind | Shadcn/Radix |
| Platform | 18 | Vite | Tailwind | Custom |
| Farm | - | Jinja2 | Tailwind | Vanilla JS |

### 9.2 Backend Stack

| Module | Framework | Language | Database | Cache |
|--------|-----------|----------|----------|-------|
| NLQ | FastAPI | Python | Supabase PG | - |
| DCL | FastAPI | Python | Supabase PG | Pinecone |
| AOD | FastAPI | Python | Supabase PG | - |
| AAM | FastAPI | Python | SQLite | - |
| Farm | FastAPI | Python | Supabase PG | - |
| RevOps | FastAPI | Python | Supabase PG | - |
| FinOps | Express | TypeScript | Neon PG | Pinecone |
| Platform | FastAPI | Python | Supabase PG | Redis |

### 9.3 AI/LLM Integrations

| Module | Primary LLM | Vector DB | RAG |
|--------|-------------|-----------|-----|
| NLQ | Anthropic Claude | - | No |
| DCL | Gemini 2.5 Flash / OpenAI | Pinecone | Yes |
| AOA | Claude/Gemini (via Portkey) | pgvector | Yes |
| FinOps | Gemini 2.5 Flash | Pinecone | Yes |
| Platform | Gemini / OpenAI | pgvector | Yes |

---

## 10. API Reference

### 10.1 Default Ports

| Service | Port |
|---------|------|
| Frontend dev | 5000 |
| Backend API | 8000 |
| Farm API | https://autonomos.farm/ |

### 10.2 Common Environment Variables

```bash
# Database
SUPABASE_URL=
SUPABASE_KEY=
DATABASE_URL=

# AI Services
ANTHROPIC_API_KEY=      # NLQ
GEMINI_API_KEY=         # DCL, FinOps, Platform
OPENAI_API_KEY=         # DCL, Platform
PINECONE_API_KEY=       # DCL, FinOps

# External Integrations
SALESFORCE_USERNAME=
SALESFORCE_PASSWORD=
SALESFORCE_SECURITY_TOKEN=
SLACK_WEBHOOK_URL=
FARM_API_URL=https://autonomos.farm

# Redis (Platform)
UPSTASH_REDIS_URL=

# AWS (FinOps)
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_REGION=
```

### 10.3 Authentication

- **JWT-based**: 2-hour sessions
- **Password hashing**: Argon2 (Platform), bcrypt (FinOps)
- **Multi-tenant**: UUID-based tenant_id scoping
- **Role-based**: Admin, User roles

---

## 11. Development Guidelines

### 11.1 Run Commands

```bash
# Development (Python modules)
npm run dev              # Frontend (port 5000)
uvicorn src.main:app --port 8000  # Backend

# Development (FinOps - Node)
npm run dev              # Full stack

# Production
./start.sh               # Builds frontend, serves via backend
```

### 11.2 Project Structure Pattern

```
module/
├── src/
│   ├── api/routes/       # FastAPI endpoints
│   ├── domain/models.py  # Pydantic models
│   ├── engine/           # Core business logic
│   ├── core/             # Cross-cutting concerns
│   └── db/               # Database operations
├── static/               # Static assets
│   ├── css/
│   └── js/
├── templates/            # Jinja2 templates (if applicable)
├── frontend/             # React app (if applicable)
│   ├── src/
│   │   ├── components/
│   │   ├── pages/
│   │   └── lib/
│   └── dist/
└── config/               # Configuration files
```

### 11.3 Testing Philosophy

**Forbidden Patterns:**
- Tests that pass while feature fails in production
- Permissive schemas to avoid contract mismatches
- Converting errors into empty results
- Hidden shortcuts for demos

**Required:**
- Failure-before / success-after proof
- Negative tests included
- Deterministic (seed-based) test data
- Real semantic validation

---

## 12. Deployment & Operations

### 12.1 Security Features

| Feature | Implementation |
|---------|----------------|
| Security Headers | HSTS, CSP, X-Frame-Options |
| CORS Protection | Fail-closed in production |
| API Rate Limiting | Prevents abuse |
| Audit Logging | All critical actions |
| Database Transactions | With rollback |
| PII Detection | At ingress with policy enforcement |
| TLS | All Redis connections |

### 12.2 Resilience Patterns

| Pattern | Purpose |
|---------|---------|
| Circuit Breakers | External service calls |
| Retry Logic | Exponential backoff |
| Graceful Degradation | When services unavailable |
| Feature Flags | Safe rollouts |
| Health Checks | Continuous monitoring |

### 12.3 External Services

| Service | Purpose | Modules Using |
|---------|---------|---------------|
| Supabase PostgreSQL | Primary database | All |
| Upstash Redis | Caching, queues, pub/sub | Platform, DCL |
| Pinecone | Vector database | DCL, FinOps |
| Airbyte | Data integration monitoring | AAM |
| Slack Webhooks | Notifications | All |

---

## Appendix A: Identified Inconsistencies & Recommendations

### A.1 Inconsistencies Found

| Issue | Description | Impact |
|-------|-------------|--------|
| React Versions | RevOps uses React 19, others use 18 | Type incompatibilities |
| Backend Language | FinOps uses Node.js, all others FastAPI/Python | Maintenance burden |
| Module Naming | Some docs use different names | Confusion |
| Persona Count | DCL has 4 personas, others have 5 | Inconsistent views |

### A.2 Recommendations

**P0 - Immediate:**
1. Standardize on React 18.x across all modules
2. Migrate FinOps from Node.js to FastAPI
3. Add "People" persona to DCL, RevOps, FinOps
4. Standardize module naming

**P1 - Short-term:**
1. Create `@aos/design-system` npm package
2. Create `@aos/types` for shared interfaces
3. Formalize aosClient into `@aos/platform-sdk`
4. Standardize on pytest + Playwright

**P2 - Long-term:**
1. Consider Turborepo/Nx monorepo
2. Unified API Gateway
3. Single SSO across modules
4. Unified observability stack

---

## Document History

| Date | Version | Changes |
|------|---------|---------|
| 2026-01-28 | 1.0 | Initial consolidated document (Claude Code) |
| 2026-01-28 | 2.0 | Comprehensive expansion with full functional capabilities |

---

*This document serves as the canonical technical reference for the AutonomOS platform. For module-specific details, refer to each module's individual replit.md file.*
