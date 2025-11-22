# DCL Engine - Data Connectivity Layer

## Overview

The DCL (Data Connectivity Layer) Engine is a full-stack application that ingests schemas and sample data from multiple sources, unifies them into a common ontology using heuristics and AI, and visualizes the data flow through an interactive Sankey diagram. The system supports two data modes (Demo with 9 legacy sources from local schemas, and Farm for synthetic data), and provides persona-driven business logic targeting CFO, CRO, COO, and CTO roles.

The application features:
- Multi-source schema ingestion and mapping
- AI-powered ontology unification (Gemini, OpenAI)
- RAG (Retrieval-Augmented Generation) with Pinecone for intelligent mapping
- Interactive Sankey visualization showing data flow from sources → ontology → persona endpoints
- Real-time narration of processing steps
- Enterprise monitoring dashboard
- Dev/Prod runtime modes with different LLM strategies

## User Preferences

Preferred communication style: Simple, everyday language.

## Recent Changes

**November 22, 2025:**
- Converted entire UI to AutonomOS color palette with dark mode aesthetic
- Implemented Quicksand font from Google Fonts
- Added color-coordinated pill-shaped labels on Sankey edge links for better visibility against dark background
- Enhanced link labels with:
  - Tight pill boxes with rounded corners
  - Layer-specific colors (Cyan for L0, Green for L1, Blue for L2, Purple for L3)
  - Dark text (#0f172a) for high contrast
  - Automatic sizing based on label content
  - Positioned at link midpoints
- Updated all UI components (panels, buttons, tabs) to use Slate backgrounds with Cyan accents
- Added custom scrollbars and subtle glow effects

## System Architecture

### Frontend Architecture

**Technology Stack:**
- React 18 with TypeScript
- Vite as build tool and dev server
- D3.js and d3-sankey for data visualization
- Component-based architecture with CSS modules

**Key Components:**
1. **App.tsx** - Main application container managing state for graph data, metrics, and active views
2. **ControlPanel** - User controls for selecting data mode (Demo/Farm), run mode (Dev/Prod), and personas (CFO/CRO/COO/CTO)
3. **SankeyGraph** - D3-powered Sankey diagram visualizing data flow through 4 layers: L0 (pipe) → L1 (sources) → L2 (ontology) → L3 (persona endpoints)
4. **NarrationPanel** - Real-time message stream showing processing steps with auto-refresh
5. **MonitorPanel** - Enterprise monitoring view for system health and status

**Design Decisions:**
- Tabbed interface to switch between graph visualization and monitoring views
- Gradient background with glassmorphism effects for modern UI aesthetic
- Real-time polling (2-second intervals) for narration updates during processing
- Responsive layout using flexbox for different screen sizes

### Backend Architecture

**Technology Stack:**
- FastAPI for REST API framework
- Python 3.x with Pydantic V2 for data validation
- SQLAlchemy for database ORM
- Pandas for CSV/data processing
- uvicorn as ASGI server

**Layered Architecture:**

1. **API Layer** (`backend/api/`)
   - FastAPI application with CORS middleware
   - RESTful endpoints for DCL operations
   - Request/response validation using Pydantic models
   - Main endpoint: POST `/api/dcl/run` for triggering DCL pipeline

2. **Domain Layer** (`backend/domain/`)
   - Core business models: SourceSystem, TableSchema, FieldSchema, OntologyConcept, Mapping
   - Persona enum (CFO, CRO, COO, CTO)
   - GraphSnapshot model for Sankey visualization data
   - RunMetrics for tracking LLM calls, RAG operations, and performance

3. **Engine Layer** (`backend/engine/`)
   - **DCLEngine** - Main orchestrator coordinating the entire pipeline
   - **SchemaLoader** - Loads schemas from Demo (local CSV files) or Farm (API)
   - **MappingService** - Maps source fields to ontology concepts using heuristics and optional LLM enhancement
   - **NarrationService** - In-memory message queue for real-time processing updates
   - **Ontology module** - Defines core ontology concepts (Account, Opportunity, Revenue, Cost, AWS Resource, Health, Usage)

**Processing Flow:**
1. Load schemas based on mode (Demo from `schemas/` directory or Farm from external API)
2. Load ontology concepts defining unified data model
3. Create mappings from source fields to ontology using heuristics (field name matching, semantic hints)
4. In Prod mode: Enhance mappings with LLM calls (Gemini/OpenAI) and RAG lookups (Pinecone)
5. Build GraphSnapshot with 4-layer structure for Sankey visualization
6. Return graph data and performance metrics

**Design Rationale:**
- Separation of concerns with distinct layers (API, Domain, Engine)
- Mode abstraction allows switching between Demo and Farm data sources without engine changes
- Dev/Prod modes enable cost control (Dev uses only heuristics, Prod adds expensive LLM/RAG operations)
- In-memory narration service for simplicity (would use message queue in production at scale)
- Confidence scoring for mappings to identify weak links

### Data Storage Solutions

**Local File Storage:**
- Demo mode schemas stored in `schemas/schemas/` directory
- CSV files organized by source system (salesforce, hubspot, mongodb, supabase, snowflake, sap, netsuite, dynamics, legacy_sql)
- Pandas used for CSV parsing and schema inference

**PostgreSQL Database:**
- Configured via `SUPABASE_DB_URL` or `DATABASE_URL` environment variables
- SQLAlchemy ORM for database interactions
- Used for persistent storage (schemas mention it but current implementation is primarily in-memory)
- Future use: Storing mappings, run history, cached RAG results

**Vector Database (Pinecone):**
- API key configured via `PINECONE_API_KEY`
- Used in Prod mode for RAG operations
- Stores embedding vectors for semantic field matching
- Enables similarity search for intelligent ontology mapping

**Design Trade-offs:**
- Current implementation uses in-memory storage for simplicity and speed
- PostgreSQL integration prepared but not fully utilized (allows future persistence)
- Vector DB only activated in Prod mode to control costs

### Authentication and Authorization

**Current State:**
- No authentication implemented (internal tool assumption)
- CORS configured to allow all origins for development flexibility

**Future Considerations:**
- API key validation would be added for multi-tenant deployments
- Environment variables already structured for secure credential management
- Tenant ID concept exists in DTO models but not enforced

### External Dependencies

**AI/LLM Services:**
1. **Google Gemini** (via `google-generativeai` package)
   - API Key: `GEMINI_API_KEY`
   - Model: Gemini 2.5 Flash (mentioned in attached requirements)
   - Used for: Schema understanding, field mapping enhancement

2. **OpenAI** (via `openai` package)
   - API Key: `OPENAI_API_KEY`
   - Models: GPT-4-mini, GPT-4-nano (mentioned in requirements)
   - Used for: Mapping validation, ontology enrichment

**Vector Database:**
3. **Pinecone** (via `pinecone-client` package)
   - API Key: `PINECONE_API_KEY`
   - Used for: RAG operations, semantic field search
   - Only active in Prod mode

**Database:**
4. **PostgreSQL** (via `psycopg2-binary` and SQLAlchemy)
   - Connection: `SUPABASE_DB_URL` or `DATABASE_URL`
   - Used for: Persistent data storage
   - Supabase-compatible connection string format

**Data Processing:**
5. **Pandas** and **NumPy**
   - Used for: CSV parsing, statistical analysis, schema inference
   - No external API dependencies

**HTTP Client:**
6. **httpx**
   - Used for: Farm mode API calls to synthetic data service
   - Async HTTP requests

**Development Tools:**
7. **Vite Dev Server**
   - Frontend development server with hot reload
   - Proxy configuration routes `/api/*` to backend on port 8000

**Runtime Configuration:**
- Frontend runs on port 5000 (Vite)
- Backend runs on port 8000 (uvicorn)
- Vite proxy eliminates CORS issues during development