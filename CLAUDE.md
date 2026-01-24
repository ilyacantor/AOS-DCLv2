# CLAUDE.md - DCL Engine Codebase Guide

## Project Overview

The DCL (Data Connectivity Layer) Engine is a full-stack application that:
- Ingests schemas and data from multiple sources (Demo or Farm mode)
- Unifies data into a common ontology using heuristics and AI
- Visualizes data flow through an interactive Sankey diagram
- Provides persona-driven business views (CFO, CRO, COO, CTO)

## Quick Start

### Running the Application

**Backend (FastAPI):**
```bash
python run_backend.py
# OR
uvicorn backend.api.main:app --host 0.0.0.0 --port 8000
```

**Frontend (React/Vite):**
```bash
npm install
npm run dev
```

- Frontend: http://localhost:5000
- Backend API: http://localhost:8000
- API docs: http://localhost:8000/docs

### Build for Production
```bash
npm run build  # Outputs to dist/
```

## Architecture

### Three-Layer Design

```
┌─────────────────────────────────────┐
│  Semantic Mapper (Batch / Cold)     │ ← Pre-computes field→concept mappings
│  backend/semantic_mapper/           │
└─────────────────────────────────────┘
              ↓
┌─────────────────────────────────────┐
│  Semantic Model (Data Layer)        │ ← YAML configs + Database tables
│  config/*.yaml → PostgreSQL         │
└─────────────────────────────────────┘
              ↓
┌─────────────────────────────────────┐
│  DCL Engine (Runtime / Hot)         │ ← Fast graph generation at request time
│  backend/engine/dcl_engine.py       │
└─────────────────────────────────────┘
```

### Graph Structure (Sankey Layers)

- **L0 (Pipe)**: Pipeline entry point (Demo or Farm)
- **L1 (Sources)**: Data sources (Salesforce, HubSpot, NetSuite, etc.)
- **L2 (Ontology)**: Unified concepts (Account, Revenue, Cost, etc.)
- **L3 (BLL)**: Business Logic Layer per persona (CFO, CRO, COO, CTO)

## Directory Structure

```
AOS-DCLv2/
├── backend/
│   ├── api/
│   │   └── main.py              # FastAPI app, REST endpoints
│   ├── domain/
│   │   └── models.py            # Pydantic models (SourceSystem, Mapping, GraphNode, etc.)
│   ├── engine/
│   │   ├── dcl_engine.py        # Main orchestrator - builds graph snapshots
│   │   ├── schema_loader.py     # Loads schemas from Demo/Farm
│   │   ├── mapping_service.py   # Field→concept mapping logic
│   │   ├── ontology.py          # Ontology concept definitions
│   │   ├── persona_view.py      # DB-driven persona filtering
│   │   ├── narration_service.py # Real-time processing messages
│   │   ├── rag_service.py       # Pinecone vector DB integration
│   │   └── source_normalizer.py # Normalizes raw source IDs to canonical
│   ├── semantic_mapper/
│   │   ├── heuristic_mapper.py  # Pattern-based field matching
│   │   ├── persist_mappings.py  # DB persistence layer
│   │   └── runner.py            # Orchestrates mapping pipeline
│   ├── llm/
│   │   └── mapping_validator.py # LLM-based mapping validation (Prod mode)
│   ├── eval/
│   │   └── mapping_evaluator.py # Validates mapping quality
│   └── utils/
│       └── config_sync.py       # Syncs YAML configs to database
├── src/
│   ├── App.tsx                  # Main React component
│   ├── types.ts                 # TypeScript type definitions
│   ├── components/
│   │   ├── SankeyGraph.tsx      # D3 Sankey visualization
│   │   ├── ControlsBar.tsx      # Mode/persona selection controls
│   │   ├── MonitorPanel.tsx     # Data lineage drill-down
│   │   ├── NarrationPanel.tsx   # Real-time processing log
│   │   ├── EnterpriseDashboard.tsx # Source registry view
│   │   └── ui/                  # Radix UI components
│   └── hooks/
│       └── use-toast.ts         # Toast notification hook
├── config/
│   ├── ontology_concepts.yaml   # Ontology definitions with clusters
│   └── persona_profiles.yaml    # Persona→concept relevance mappings
├── docs/
│   ├── ARCH-DCL-CURRENT.md      # Current architecture documentation
│   └── ARCH-DCL-TARGET.md       # Target architecture (completed)
├── package.json                 # Frontend dependencies
├── requirements.txt             # Python dependencies
├── vite.config.ts               # Vite bundler configuration
└── run_backend.py               # Backend entry point
```

## Key Concepts

### Data Modes
- **Demo**: Uses local CSV schemas from 9 legacy sources
- **Farm**: Fetches real-time synthetic data from AOS-Farm API (https://autonomos.farm)

### Run Modes
- **Dev**: Uses only heuristic mappings (fast, no LLM costs)
- **Prod**: Enhances mappings with LLM validation (slower, uses OpenAI API)

### Personas
| Persona | Focus Areas | Key Concepts |
|---------|-------------|--------------|
| CFO | Financial metrics | revenue, cost |
| CRO | Sales & growth | account, opportunity, revenue |
| COO | Operations | usage, health |
| CTO | Infrastructure | aws_resource, usage, cost |

### Ontology Concepts
8 core concepts organized into clusters:
- **Finance**: revenue, cost
- **Growth**: account, opportunity
- **Infra**: aws_resource
- **Ops**: health, usage, date

## API Endpoints

### Core Endpoints
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/health` | Health check |
| POST | `/api/dcl/run` | Build graph snapshot |
| POST | `/api/dcl/batch-mapping` | Trigger batch semantic mapping |
| GET | `/api/dcl/narration/{run_id}` | Get processing messages |
| GET | `/api/dcl/monitor/{run_id}` | Get monitor data |

### POST /api/dcl/run Request Body
```json
{
  "mode": "Demo" | "Farm",
  "run_mode": "Dev" | "Prod",
  "personas": ["CFO", "CRO", "COO", "CTO"],
  "source_limit": 5
}
```

## Environment Variables

| Variable | Purpose | Required |
|----------|---------|----------|
| `OPENAI_API_KEY` | LLM validation in Prod mode | For Prod mode |
| `GEMINI_API_KEY` | Alternative LLM provider | Optional |
| `PINECONE_API_KEY` | Vector DB for RAG | Optional |
| `SUPABASE_DB_URL` | PostgreSQL connection | Optional |
| `DATABASE_URL` | PostgreSQL connection (alt) | Optional |
| `FARM_API_URL` | Farm API base URL | Default: https://autonomos.farm |

## Development Conventions

### Python Backend
- Use Pydantic V2 for data validation
- Follow FastAPI patterns for endpoints
- Type hints required on all functions
- Use `snake_case` for variables and functions
- Domain models in `backend/domain/models.py`

### TypeScript Frontend
- React functional components with hooks
- TypeScript strict mode enabled
- Use Tailwind CSS for styling
- Component files use PascalCase
- Types defined in `src/types.ts`

### File Naming
- Python: `snake_case.py`
- TypeScript/React: `PascalCase.tsx` for components, `kebab-case.ts` for utilities
- Config files: `snake_case.yaml`

### Git Conventions
- Descriptive commit messages
- Feature branches off main
- No force pushes to main/dev

## Common Tasks

### Adding a New Ontology Concept
1. Edit `config/ontology_concepts.yaml`
2. Add concept with id, name, description, cluster, and metadata
3. Update persona relevance in `config/persona_profiles.yaml` if needed
4. Config sync happens automatically on backend startup

### Adding a New Data Source
1. For Demo: Add CSV files to `schemas/` directory
2. For Farm: Source is automatically inferred from API response
3. Source normalizer handles ID canonicalization

### Running Batch Mapping
```bash
curl -X POST http://localhost:8000/api/dcl/batch-mapping \
  -H "Content-Type: application/json" \
  -d '{"mode": "Demo", "mapping_mode": "heuristic"}'
```

### Testing the Pipeline
1. Start backend: `python run_backend.py`
2. Start frontend: `npm run dev`
3. Open http://localhost:5000
4. Select data mode and personas
5. Click "Run" to generate graph

## Domain Models Reference

### SourceSystem
```python
class SourceSystem(BaseModel):
    id: str                    # Unique identifier
    name: str                  # Display name
    type: str                  # crm, erp, datawarehouse, etc.
    tables: List[TableSchema]  # Tables in this source
    canonical_id: str          # Normalized ID
    discovery_status: DiscoveryStatus  # canonical, pending_triage, custom
    trust_score: int           # 0-100 trust rating
```

### Mapping
```python
class Mapping(BaseModel):
    source_field: str          # Field name
    source_table: str          # Table name
    source_system: str         # Source ID
    ontology_concept: str      # Target concept ID
    confidence: float          # 0.0-1.0 confidence score
    method: str                # heuristic, rag, llm, llm_validated
```

### GraphSnapshot
```python
class GraphSnapshot(BaseModel):
    nodes: List[GraphNode]     # L0, L1, L2, L3 nodes
    links: List[GraphLink]     # Connections between nodes
    meta: Dict[str, Any]       # Mode, run_id, stats
```

## Troubleshooting

### Backend won't start
- Check Python dependencies: `pip install -r requirements.txt`
- Verify port 8000 is available
- Check for syntax errors in Python files

### Frontend shows no data
- Ensure backend is running on port 8000
- Check browser console for API errors
- Verify Vite proxy configuration in `vite.config.ts`

### Mappings not appearing
- Run batch mapping: POST to `/api/dcl/batch-mapping`
- Check narration panel for errors
- Verify ontology concepts match field patterns

### LLM validation not working
- Set `OPENAI_API_KEY` environment variable
- Use `run_mode: "Prod"` in request
- Check API key validity

## Performance Notes

- **Dev mode**: No LLM calls, graph builds in <100ms
- **Prod mode**: LLM validation adds 1-3 seconds per batch
- **Source limit**: Farm mode respects `source_limit` to control data volume
- **Caching**: Source normalizer caches registry for 5 minutes

## Related Documentation

- `docs/ARCH-DCL-CURRENT.md` - Detailed architecture documentation
- `docs/ARCH-DCL-TARGET.md` - Target architecture (completed)
- `replit.md` - Replit-specific configuration notes
