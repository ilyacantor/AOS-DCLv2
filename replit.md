# DCL Engine - Data Connectivity Layer

## Overview
The Data Connectivity Layer (DCL) is a **semantic mapping and visualization platform** designed to provide organizations with a clear understanding of their data landscape. It visually maps data origin, meaning, and usage across various enterprise systems like CRMs, ERPs, databases, and data warehouses. DCL's core purpose is to make data discovery intuitive and role-specific by resolving issues such as identifying relevant data systems, translating technical field names to business concepts, and understanding data flows. The platform features an interactive 4-layer Sankey diagram that visualizes data flow from pipelines and sources through business ontologies to specific user personas. It supports different data modes (Demo, Farm, AAM) and run modes (Dev for heuristic mapping, Prod for AI-powered semantic mapping with RAG).

## User Preferences
- Communication style: Simple, everyday language
- No mock data - use real integrations
- PST timezone (12-hour in controls, 24-hour in terminal)

## System Architecture

### Frontend
- **React 18** with TypeScript and Vite
- **D3.js** for Sankey diagrams
- **Tailwind CSS** for styling
- **Lucide React** for icons

### Backend
- **FastAPI** with Python 3.11 and Pydantic V2
- **Uvicorn**
- **PostgreSQL** for persistence
- **Redis** for real-time pub/sub

### Core Functional Capabilities
1.  **Interactive Data Flow Visualization**: A 4-layer Sankey diagram illustrating data flow from Pipelines (L0) and Sources (L1) to Ontology (L2) and Personas (L3). Users can filter by persona, hover for details, and click nodes for source information.
2.  **Three Data Modes**:
    *   **Demo**: Uses pre-configured schemas from CSVs for training and testing.
    *   **Farm**: Connects to live schemas via AOS-Farm API for production data.
    *   **AAM**: Connects to live AAM fabric planes for iPaaS, Gateway, EventBus, and DW connection discovery.
3.  **Run Modes**:
    *   **Dev**: Employs heuristic pattern matching for fast, good-enough mapping.
    *   **Prod**: Utilizes AI-powered semantic matching with LLM (GPT-4o-mini) and RAG (Pinecone) for higher accuracy, especially for ambiguous fields and new sources.
4.  **AI-Powered Semantic Mapping**: Extracts technical fields, generates embeddings (OpenAI's text-embedding-3-small), queries Pinecone for similar concepts, and uses an LLM for validation and confidence scoring. A "RAG History" panel tracks these mappings.
5.  **Persona-Based Filtering**: Allows filtering the visualization to show data relevant to specific executive roles (CFO, CRO, COO, CTO, CHRO).
6.  **Real-Time Narration**: A terminal-style panel displays live processing activity, including schema loading and mapping operations.
7.  **Collapsible Monitor Panel**: A sidebar displaying persona-specific metrics/KPIs and RAG history.

### Key Data Sources (L1)
-   **CRM Systems**: Salesforce, HubSpot, Microsoft Dynamics
-   **ERP Systems**: SAP, NetSuite
-   **Databases**: MongoDB, Supabase, Legacy SQL
-   **Data Warehouse**: DW Dim Customer
-   **Integration Platforms**: MuleSoft ERP Sync

### Business Concepts (L2 Ontology)
Concepts include Account, Opportunity, Revenue, Cost, Date/Timestamp, Health Score, Usage Metrics, and AWS Resource, each with associated common fields. The semantic catalog is externalized into YAML configuration files (`metrics.yaml`, `entities.yaml`, `bindings.yaml`, `persona_concepts.yaml`).

### API Endpoints
-   `/api/dcl/run`: Executes the DCL pipeline.
-   `/api/dcl/narration/{session_id}`: Polls real-time narration messages.
-   `/api/dcl/batch-mapping`: Runs semantic mapping in batch.
-   `/api/dcl/semantic-export`: Provides the full semantic catalog.
-   `/api/dcl/query`: Executes data queries against the fact base using metrics, dimensions, filters, and time ranges.
-   `/api/dcl/reconciliation`: AAM reconciliation - compares push payload vs DCL loaded state.
-   `/api/dcl/reconciliation/sor`: SOR reconciliation - compares bindings.yaml expected sources vs DCL loaded sources; detects coverage gaps, SOR conflicts, orphan/missing sources.
-   `/api/topology`, `/api/topology/health`: Unified topology and connection health data.

## External Dependencies
-   **OpenAI API**: For embedding generation (`text-embedding-3-small`) and LLM validation (`GPT-4o-mini`).
-   **Pinecone**: Vector database for RAG queries.
-   **AOS-Farm API**: Provides live schemas for the Farm data mode.
-   **AAM API**: Provides live connections and fabric planes for the AAM data mode.
-   **PostgreSQL**: Primary database for persistence.
-   **Redis**: For pub/sub messaging.