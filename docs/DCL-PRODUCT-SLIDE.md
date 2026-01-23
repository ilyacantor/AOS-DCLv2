# DCL Engine

---

## Core Function

**DCL answers one question:**

> "What does this field mean to the business?"

It maps raw technical fields (`acct_id`, `cust_rev_ytd`, `opp_stage`) to business concepts (`Account`, `Revenue`, `Opportunity`) — then shows you who uses what.

---

## The Problem It Solves

You have 11 source systems. Each uses different names for the same thing:
- Salesforce calls it `AccountId`
- SAP calls it `KUNNR`  
- NetSuite calls it `customer_internal_id`
- Your data warehouse calls it `dim_customer_key`

**They all mean "Customer Account."** But nobody documented that. Until now.

---

### How It Works

```
   Sources           →      Unified Ontology      →    Business Value
   ─────────────────────────────────────────────────────────────────
   Salesforce CRM    ┐                            ┌→  CFO Dashboard
   HubSpot           │      ┌─ Account ─┐         │
   SAP ERP           ├──→   │ Revenue   │   ──────┼→  CRO Insights  
   NetSuite          │      │ Cost      │         │
   Snowflake DW      ┘      └─ Health ──┘         └→  COO/CTO Ops
```

---

### Key Capabilities

| Capability | What You Get |
|------------|--------------|
| **Auto-Discovery** | Finds schemas across 11 source systems automatically |
| **AI-Powered Mapping** | 95% accuracy matching fields to business concepts |
| **Real-Time Visualization** | Interactive Sankey diagram shows data flow instantly |
| **Persona Views** | CFO, CRO, COO, CTO each see what matters to them |

---

### Intelligent Mapping

DCL learns from every mapping decision:

| Low Confidence? | AI Validates |
|-----------------|--------------|
| `GL_ACCOUNT` → "account"? | **Corrected:** → "general_ledger" |
| `MRR` → "revenue"? | **Confirmed:** ✓ |

*Continuous learning improves accuracy over time.*

---

### Zero-Trust Architecture

DCL never stores your data. Ever.

| What DCL Stores | What DCL Does NOT Store |
|-----------------|------------------------|
| Schema metadata (field names, types) | Row data |
| Mapping decisions | Customer records |
| Pointers to data locations | Actual payloads |

**Your data stays in your systems. DCL only maps the structure.**

---

### The 4-Layer View

```
┌─────────────────────────────────────────────────────────────┐
│                                                             │
│   L0: Pipeline    →   L1: Sources   →   L2: Ontology   →   L3: BLL    │
│   ───────────────────────────────────────────────────────   │
│                                                             │
│   [Ingest]  ──→  [11 Systems]  ──→  [8 Concepts]  ──→  [4 Personas]   │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

---

### Business Outcomes

| For | Benefit |
|-----|---------|
| **Data Teams** | See all sources in one place, instantly |
| **Executives** | Understand which systems drive which metrics |
| **Architects** | Validate integration coverage visually |
| **Compliance** | Prove where data flows (without storing it) |

---

### Get Started in Minutes

1. Connect your Fabric Planes (iPaaS, API Gateway, Event Bus, Warehouse)
2. Run discovery — DCL maps everything automatically
3. View your unified data landscape in the Sankey visualization

---

### DCL Engine

**See your data. Understand your business.**

*Metadata-first. Zero-trust. AI-enhanced.*
