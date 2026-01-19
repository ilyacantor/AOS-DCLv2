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

## Phase 2: Value Realization (January 2026)

### Stream-to-Graph Integration
The Consumer has been upgraded to bridge the gap between the Redis stream and the Sankey visualization:

1. **Schema Inference**: Automatically infers field types and schema from JSON payloads
2. **Semantic Mapping**: Uses HeuristicMapper to link fields to ontology concepts (e.g., `invoice_id` → `Invoice`, `total_amount` → `Revenue`)
3. **Database Persistence**: Stores mappings in PostgreSQL for DCLEngine visibility
4. **Live Node Registration**: Stream sources appear in the Sankey diagram with proper flow connections

**Key Mappings for Invoice Stream:**
- `invoice_id`, `invoice_date`, `due_date` → `Invoice` concept (Finance cluster)
- `total_amount`, `subtotal`, `tax_amount` → `Revenue` concept (Finance cluster)
- `vendor.*` fields → `Vendor` concept (Ops cluster)
- `payment_status` → `Payment Status` concept (Finance cluster)
- `currency` → `Currency` concept
- `sync_timestamp` → `Date/Timestamp` concept

**New Ontology Concepts Added:**
- `invoice` - Invoice or billing record (Finance)
- `vendor` - Supplier or vendor entity (Ops)
- `payment_status` - Payment or transaction status (Finance)

**Run Commands:**
- Start Sidecar: `python backend/ingest/run_sidecar.py`
- Start Consumer: `python backend/ingest/run_consumer.py`
- View in UI: Switch to "Farm" mode and click "Run Pipeline"

## Phase 3: Active Repair Agent (January 2026)

### Self-Healing Capability
The Ingest Sidecar now implements "Detect & Repair" - automatically fixing drifted records in transit.

**Expected Invoice Schema:**
```python
EXPECTED_INVOICE_FIELDS = ["invoice_id", "total_amount", "vendor", "payment_status"]
```

**Drift Detection:**
- `detect_drift(record)` checks if expected fields are missing
- Returns list of missing fields for repair

**Gap Fill Repair:**
- `repair_record(record, missing_fields)` calls Farm's Source of Truth API
- Endpoint: `GET /api/source/salesforce/invoice/{invoice_id}`
- Merges missing fields from repair response into the record
- Tags envelope with `is_repaired: true` and `repaired_fields: [...]`

**Updated AOS_Envelope Metadata:**
```json
{
  "meta": {
    "ingest_ts": 1768848236575,
    "source": "mulesoft_mock",
    "trace_id": "uuid",
    "is_repaired": true,
    "repaired_fields": ["vendor"]
  },
  "payload": { ... }
}
```

**Metrics Tracking:**
- `records_repaired` count added to IngestMetrics
- Progress logs now show: "100 valid, 0 dropped, 5 repaired"

**Why This Matters:** This proves Active Ingest (AAM) - data isn't just moved, it's improved in transit.

**Chaos Mode Configuration:**
- Set `ENABLE_CHAOS=true` (default) to enable chaos mode
- Stream URL automatically appends `?chaos=true`
- Chaos control messages (latency spikes, etc.) are filtered out from drift detection
- Only invoice records with missing expected fields trigger repair

**Testing:**
1. Watch Ingest Pipeline logs for "Drift Detected" messages
2. Successful repairs show "Record Repaired" and increment `records_repaired` counter
3. Redis records will have `is_repaired: true` and `repaired_fields: [...]` in metadata