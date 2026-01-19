# DCL Engine - Data Connectivity Layer

## Overview
The DCL (Data Connectivity Layer) Engine is a full-stack application designed to ingest and unify schemas and sample data from diverse sources into a common ontology. It leverages AI and heuristics to achieve this unification and visualizes the data flow using an interactive Sankey diagram. The system supports two data modes (Demo for legacy sources and Farm for synthetic data) and provides persona-driven business logic tailored for roles like CFO, CRO, COO, and CTO. Its core capabilities include multi-source schema ingestion, AI-powered ontology unification, RAG for intelligent mapping, and real-time process narration. The project aims to provide a comprehensive data connectivity solution with enterprise monitoring and flexible runtime modes.

## User Preferences
Preferred communication style: Simple, everyday language.

## System Architecture

### Frontend Architecture
The frontend is built with React 18 and TypeScript, using Vite for development and D3.js for data visualization, specifically d3-sankey for the interactive Sankey diagram. It features a component-based architecture with CSS modules. Key components include `App.tsx` for state management, `ControlPanel` for user inputs (data mode, run mode, personas), `SankeyGraph` for visualizing data flow across four layers (pipe, sources, ontology, persona endpoints), `NarrationPanel` for real-time processing updates, and `MonitorPanel` with persona-specific metrics and interactive drill-down capabilities. The design emphasizes a modern UI with a gradient background, glassmorphism effects, and responsiveness.

### Backend Architecture
The backend is developed with FastAPI and Python 3.x, utilizing Pydantic V2 for data validation and uvicorn as the ASGI server. It follows a layered architecture:
- **API Layer**: Handles RESTful endpoints, CORS, and Pydantic-based request/response validation.
- **Domain Layer**: Defines core business models such as `SourceSystem`, `TableSchema`, `FieldSchema`, `OntologyConcept`, and `Mapping`, along with `Persona` enums and `GraphSnapshot` for visualization data.
- **Engine Layer**: Contains the `DCLEngine` orchestrator, `SchemaLoader` (for Demo or Farm data), `MappingService` (for heuristic and optional LLM mapping), `RAGService` (for Pinecone integration), and `NarrationService` for real-time updates.

The processing flow involves loading schemas, defining ontology, creating mappings, and building a `GraphSnapshot` for visualization. It supports Dev and Prod runtime modes, with Prod mode activating expensive LLM/RAG operations. Integration with the AOS-Farm platform allows fetching real-time synthetic data, with vendor-based source identification for improved mapping accuracy.

### Data Storage Solutions
- **Local File Storage**: Used for Demo mode schemas (CSV files).
- **PostgreSQL Database**: Configured via environment variables (`SUPABASE_DB_URL`, `DATABASE_URL`) with SQLAlchemy ORM, primarily for future persistence.
- **Vector Database (Pinecone)**: Used in Prod mode for RAG operations and semantic field matching, storing embedding vectors for intelligent ontology mapping.

### Authentication and Authorization
Currently, there is no authentication implemented, operating under an internal tool assumption with permissive CORS. Future considerations include API key validation and tenant isolation.

## External Dependencies

1.  **Google Gemini**: For schema understanding and field mapping enhancement (`GEMINI_API_KEY`, Gemini 2.5 Flash).
2.  **OpenAI**: For mapping validation and ontology enrichment (`OPENAI_API_KEY`, GPT-4-mini, GPT-4-nano).
3.  **Pinecone**: Vector database for RAG operations and semantic field search (`PINECONE_API_KEY`), active only in Prod mode.
4.  **PostgreSQL**: Database for persistent storage (via `psycopg2-binary` and SQLAlchemy).
5.  **Pandas & NumPy**: For CSV parsing, data processing, and schema inference.
6.  **httpx**: Asynchronous HTTP client for Farm mode API calls to the synthetic data service.
7.  **Vite Dev Server**: Frontend development server with hot reload and proxy configuration for backend integration.