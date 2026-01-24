# AOS BLL (Business Logic Layer)

BLL is a consumption layer that sits between users/agents and DCL (Data Connectivity Layer). It handles natural language queries, policy enforcement, and response assembly.

## Architecture

```
Frontend (React) → Express Proxy → FastAPI Backend → DCL
     :5000              :5000           :8001        :8000
```

**Single-port architecture**: FastAPI serves both the API and static frontend files on port 5000 in production mode.

## Quick Start

```bash
# Backend
cd bll/backend && python -m app.main

# Frontend build (for production)
npx vite build
```

## Key Directories

```
bll/backend/app/
├── api/           # FastAPI routers (nlq, answers, tools, policies, plans)
├── core/          # Business logic (dcl_client, query_planner, policy_engine, answer_assembler)
├── models/        # SQLAlchemy models
└── main.py        # Entry point, serves frontend + API

client/src/
├── pages/         # React pages (ask, tools, policies, plans, metrics, health)
├── components/    # UI components (shadcn/ui)
└── lib/           # Query client, utilities

shared/schema.ts   # TypeScript types (Drizzle + Zod)
```

## Critical Conventions

### API Responses: camelCase

All API responses use **camelCase** for frontend compatibility. Python uses snake_case internally.

```python
# In API handlers, return camelCase:
return {
    "questionText": p.question_text,
    "confidenceScore": p.confidence_score,
    "createdAt": p.created_at.isoformat()
}

# For Pydantic models, use serialization_alias:
class AskResponse(BaseModel):
    confidence_score: float = Field(serialization_alias="confidenceScore")
```

### DCL Integration

BLL consumes DCL, never defines its own metrics. The flow:

1. `dcl_client.rank_answerability(question)` - semantic matching to definitions
2. `query_planner.create_plan()` - builds execution plan
3. `policy_engine.check_policy()` - enforces consumption rules
4. `dcl_client.execute_definition()` - runs the query
5. `answer_assembler.assemble()` - uses DCL's computed summary

### Consumer Types

Three types with different quality thresholds:
- `nlq` - Natural language (0.3 threshold)
- `dashboard` - UI widgets (0.5 threshold)
- `agent` - Autonomous (0.8 threshold, strictest)

## API Endpoints

```
POST /api/bll/ask                    # Ask natural language question
GET  /api/bll/answers/recent         # Recent answers (camelCase!)
GET  /api/bll/plans                  # Query plans (camelCase!)
GET  /api/bll/tools                  # Tool registry (camelCase!)
GET  /api/bll/policies               # Consumption policies (camelCase!)
GET  /api/bll/health                 # Health check
```

## Environment

```bash
DCL_BASE_URL=http://localhost:8000   # DCL service
DCL_DATASET_ID=demo9                 # Default dataset
DATABASE_URL=postgresql+psycopg://...
```

## Key Files

| File | Purpose |
|------|---------|
| `bll/backend/app/main.py` | FastAPI app, serves frontend + API |
| `bll/backend/app/core/dcl_client.py` | DCL HTTP client with semantic mappings |
| `bll/backend/app/api/nlq.py` | Main /ask endpoint |
| `bll/backend/app/api/*.py` | All return camelCase JSON |
| `client/src/pages/ask.tsx` | NLQ interface |

## Common Issues

**Pages not loading**: Rebuild frontend with `npx vite build`, restart server.

**"Invalid Date"**: API returning snake_case instead of camelCase. Check the API handler.

**DCL connection errors**: Ensure DCL is running on port 8000 and `DCL_BASE_URL` is correct.
