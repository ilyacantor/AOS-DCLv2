# DCL Engine - Data Connectivity Layer

## Overview
The DCL (Data Connectivity Layer) Engine is a full-stack application designed to ingest and unify schemas and sample data from diverse sources into a common ontology using AI and heuristics. It visualizes data flow via an interactive Sankey diagram and supports two data modes: Demo (legacy sources) and Farm (synthetic data). The system provides persona-driven business logic for roles like CFO, CRO, COO, and CTO. Its core capabilities include multi-source schema ingestion, AI-powered ontology unification, RAG for intelligent mapping, real-time process narration, and enterprise monitoring with flexible runtime modes. The project aims to deliver a comprehensive data connectivity solution, enabling autonomous data operations and dynamic connector provisioning.

## User Preferences
Preferred communication style: Simple, everyday language.

## System Architecture

### Frontend Architecture
The frontend is built with React 18 and TypeScript, utilizing Vite and D3.js (d3-sankey) for interactive data visualization. It employs a component-based architecture with CSS modules. Key components include `App.tsx` for state management, `ControlPanel` for user inputs (data mode, run mode, personas), `SankeyGraph` for visualizing data flow across four layers (pipe, sources, ontology, persona endpoints), `NarrationPanel` for real-time processing updates, and `MonitorPanel` for persona-specific metrics. The design features a modern UI with gradient backgrounds, glassmorphism effects, and responsiveness.

### Backend Architecture
The backend is developed with FastAPI and Python 3.x, using Pydantic V2 for data validation and uvicorn. It follows a layered architecture:
- **API Layer**: Handles RESTful endpoints, CORS, and Pydantic validation.
- **Domain Layer**: Defines core business models like `SourceSystem`, `TableSchema`, `FieldSchema`, `OntologyConcept`, `Mapping`, `Persona` enums, and `GraphSnapshot`.
- **Engine Layer**: Contains the `DCLEngine` orchestrator, `SchemaLoader`, `MappingService` (heuristic and LLM mapping), `RAGService` (Pinecone integration), and `NarrationService`.

The processing flow involves schema loading, ontology definition, mapping creation, and `GraphSnapshot` generation for visualization. It supports Dev and Prod runtime modes, with Prod activating LLM/RAG operations. Integration with the AOS-Farm platform enables fetching real-time synthetic data with vendor-based source identification. The ingest pipeline includes a Sidecar for "Detect & Repair" functionality, automatically fixing drifted records in transit by calling a Source of Truth API and broadcasting events to the UI. It also supports dynamic connector provisioning via a "Handshake" pattern, allowing AOD to reconfigure DCL without manual intervention.

### Data Storage Solutions
- **Local File Storage**: For Demo mode schemas (CSV files).
- **PostgreSQL Database**: Configured via environment variables, primarily for future persistence using SQLAlchemy ORM.
- **Vector Database (Pinecone)**: Used in Prod mode for RAG operations and semantic field matching, storing embedding vectors for intelligent ontology mapping.
- **Redis**: Utilized for real-time log broadcasting (`dcl.logs`), dynamic connector configuration (`dcl.ingest.config`), raw ingested records (`dcl.ingest.raw`), and telemetry metrics (`dcl.telemetry`).

### Authentication and Authorization
Currently operates without authentication for internal tool use with permissive CORS. Future considerations include API key validation and tenant isolation.

### Environmental Configuration
- `FARM_API_URL`: Points to the Farm API.
- `RUN_MODE`: `dev` (heuristic mapping only) or `prod` (full LLM/RAG operations).
- `ENABLE_CHAOS`: Enables chaos mode for testing drift detection.

## External Dependencies

-   **Google Gemini**: For schema understanding and field mapping enhancement (`GEMINI_API_KEY`, Gemini 2.5 Flash).
-   **OpenAI**: For mapping validation and ontology enrichment (`OPENAI_API_KEY`, GPT-4-mini, GPT-4-nano).
-   **Pinecone**: Vector database for RAG operations and semantic field search (`PINECONE_API_KEY`), active in Prod mode.
-   **PostgreSQL**: Database for persistent storage (via `psycopg2-binary` and SQLAlchemy).
-   **Pandas & NumPy**: For CSV parsing, data processing, and schema inference.
-   **httpx**: Asynchronous HTTP client for Farm mode API calls.
-   **Vite Dev Server**: Frontend development server.