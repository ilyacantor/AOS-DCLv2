# AOS-DCLv2 Code Review Report

**Date:** January 2026
**Scope:** Monoliths, janky code, undebuggable logic, tech debt, Farm integration

---

## Executive Summary

The DCL codebase shows signs of rapid prototyping with evolving requirements. While the core architecture is reasonable (React frontend + FastAPI backend), several components have grown monolithic, and the Farm integration is incomplete/broken. The Sankey graph limitations are acknowledged and the dashboard is the correct mitigation strategy.

**Critical Issues:**
1. Farm integration is hardcoded and broken for DCL stress testing
2. MonitorPanel.tsx is a 687-line monolith
3. Inconsistent data contracts between frontend/backend (snake_case vs camelCase)
4. Silent error handling throughout the codebase

---

## 1. MONOLITHIC COMPONENTS

### 1.1 Frontend: MonitorPanel.tsx (687 lines)

**Location:** `src/components/MonitorPanel.tsx`

**Problem:** This single component handles:
- Persona view rendering (lines 328-616)
- Hierarchical tree expansion (sources → tables → fields)
- Detail panel modal (lines 148-316)
- RAG message fetching and display (lines 618-681)
- 7 separate expansion state variables (lines 32-39)

```typescript
// Lines 32-39: State explosion
const [expandedSections, setExpandedSections] = useState<Record<string, {...}>>({});
const [expandedOntologies, setExpandedOntologies] = useState<Record<string, boolean>>({});
const [expandedSources, setExpandedSources] = useState<Record<string, boolean>>({});
const [expandedTables, setExpandedTables] = useState<Record<string, boolean>>({});
const [selectedDetail, setSelectedDetail] = useState<DetailSelection | null>(null);
```

**Consequences:**
- Difficult to test individual features
- State management is scattered
- JSX nesting goes 12+ levels deep (lines 458-592)
- Any change risks breaking multiple features

**Recommended Refactor:**
```
MonitorPanel/
├── index.tsx              # Main orchestrator (~100 lines)
├── PersonaViewCard.tsx    # Individual persona card
├── HierarchyTree.tsx      # Tree expansion logic
│   ├── OntologyNode.tsx
│   ├── SourceNode.tsx
│   └── TableNode.tsx
├── DetailModal.tsx        # Modal overlay
├── RAGHistoryTab.tsx      # RAG operations display
└── hooks/
    └── useExpansionState.ts  # Consolidated expansion logic
```

### 1.2 Backend: dcl_engine.py (352 lines)

**Location:** `backend/engine/dcl_engine.py`

**Problem:** The `build_graph_snapshot` method does too much:
- Schema loading (lines 38-43)
- Mapping retrieval/creation (lines 54-76)
- Evaluation (lines 79-96)
- LLM validation (lines 97-141)
- Graph construction (line 143)

**Consequences:**
- Hard to test individual phases
- Changes to mapping logic affect graph building
- No clear separation between orchestration and business logic

**Recommended Refactor:**
```python
class DCLEngine:
    def build_graph_snapshot(self, ...):
        # Step 1: Load sources
        sources = self._load_sources(mode, source_limit)

        # Step 2: Get/create mappings
        mappings = self._resolve_mappings(sources, run_mode)

        # Step 3: Build graph
        return self._construct_graph(sources, mappings, personas)
```

### 1.3 Backend: schema_loader.py (401 lines)

**Location:** `backend/engine/schema_loader.py`

**Problem:** Two major static methods handle completely different data sources:
- `load_demo_schemas()` - CSV file loading (lines 14-108)
- `load_farm_schemas()` - HTTP API integration (lines 110-236)

Both methods duplicate normalization logic and schema inference.

---

## 2. JANKY CODE PATTERNS

### 2.1 Inconsistent Property Naming (Critical)

**Location:** `src/types.ts:38-48`

```typescript
export interface GraphLink {
  flowType?: string;    // camelCase
  flow_type?: string;   // snake_case - BOTH exist!
  infoSummary?: string;
  info_summary?: string; // BOTH exist!
}
```

**Impact:** EnterpriseDashboard.tsx has to handle both:
```typescript
// Line 57-58
const flowType = link.flowType || link.flow_type;
const infoSummary = link.infoSummary || link.info_summary;
```

**Root Cause:** Backend uses snake_case (Python convention), frontend uses camelCase (JS convention), but no serialization layer transforms between them.

**Fix:** Add a response transformer or use Pydantic's `by_alias` configuration.

### 2.2 String Parsing for Structured Data

**Location:** `src/components/EnterpriseDashboard.tsx:65`

```typescript
const parts = infoSummary.split(' → ');
```

**Problem:** Relies on the backend producing `info_summary` in the exact format `"field → concept (method, confidence)"`. Any backend change breaks the UI silently.

**Fix:** Return structured data instead:
```typescript
// Backend should return:
{
  source_field: "customer_name",
  target_concept: "CustomerName",
  method: "heuristic",
  confidence: 0.85
}
```

### 2.3 Type Coercion Gymnastics

**Location:** `src/components/MonitorPanel.tsx:77`

```typescript
const hierarchy = node.metrics.source_hierarchy as unknown as SourceHierarchy;
```

**Problem:** Double cast through `unknown` indicates the type system doesn't know what `metrics` actually contains.

**Root Cause:** `GraphNode.metrics` is typed as `SourceMetrics & Record<string, unknown>` which loses type safety for nested structures like `source_hierarchy`.

### 2.4 Hardcoded Magic Strings

**Locations:**
- `src/components/ControlsBar.tsx:125` - Persona IDs: `['CFO', 'CRO', 'COO', 'CTO']`
- `backend/engine/schema_loader.py:123-129` - Farm endpoints hardcoded
- Various files - Confidence thresholds: 0.85, 0.6, 0.75 scattered

**Fix:** Create constants files:
```typescript
// src/constants.ts
export const PERSONAS = ['CFO', 'CRO', 'COO', 'CTO'] as const;
export const CONFIDENCE_THRESHOLDS = {
  HIGH: 0.8,
  MEDIUM: 0.5,
  LOW: 0.3
} as const;
```

---

## 3. UNDEBUGGABLE LOGIC

### 3.1 Silent Error Handling

**Location:** `backend/engine/schema_loader.py:80-81`

```python
except Exception as e:
    continue  # Silent failure - no logging!
```

**Problem:** CSV loading errors are completely swallowed. If a schema file is malformed, there's no way to know.

**Other occurrences:**
- `schema_loader.py:274-277` - API errors logged to narration but easily missed
- `dcl_engine.py:135-136` - LLM errors caught and logged but processing continues

**Fix:** At minimum, add proper logging:
```python
import logging
logger = logging.getLogger(__name__)

except Exception as e:
    logger.warning(f"Failed to load {csv_file}: {e}")
    continue
```

### 3.2 Nested Callback State Updates

**Location:** `src/components/SankeyGraph.tsx:119-146`

```typescript
const handleMouseEnter = (event: React.MouseEvent<SVGPathElement>) => {
  const containerRect = containerRef.current?.getBoundingClientRect();
  if (containerRect) {
    // Complex position calculations...
    setTooltip({ ... });  // State update buried in callback
  }
};
```

**Problem:** Tooltip positioning logic is inside an event handler, making it impossible to unit test.

### 3.3 Implicit Data Flow in Persona Filtering

**Locations:**
- `src/App.tsx:66-68` - `generatePersonaViews` filters ontologies
- `backend/engine/persona_view.py` - `get_all_relevant_concept_ids` filters concepts
- `backend/engine/dcl_engine.py:245` - Uses `relevant_concept_ids` for graph building

**Problem:** Three different places filter data based on personas. If they get out of sync, the UI shows incorrect data with no obvious error.

### 3.4 In-Memory Narration Service

**Location:** `backend/engine/narration_service.py`

```python
def __init__(self):
    self.messages: Dict[str, List[Dict]] = {}  # All data lost on restart!
```

**Problem:** Run narration/logs are stored only in memory. Server restart = all debug info gone.

---

## 4. TECH DEBT

### 4.1 Duplicate Dependencies

**Location:** `requirements.txt`

```
fastapi==0.115.0           # Line 1
fastapi                    # Line 14 (duplicate, no version)
google-generativeai==0.8.3 # Line 8
google-generativeai        # Line 15 (duplicate)
```

### 4.2 Database Connection Pattern

**Location:** Multiple files in `backend/`

```python
# Repeated pattern - no connection pool, manual cleanup
conn = psycopg2.connect(self.database_url)
cursor = conn.cursor()
try:
    # ... query
finally:
    cursor.close()
    conn.close()
```

**Fix:** Use a connection pool or context manager:
```python
from contextlib import contextmanager

@contextmanager
def get_db_cursor(database_url):
    conn = psycopg2.connect(database_url)
    try:
        with conn.cursor() as cursor:
            yield cursor
        conn.commit()
    finally:
        conn.close()
```

### 4.3 No API Schema Validation

The API has no request/response validation beyond Pydantic models. No OpenAPI schema documentation or contract testing.

### 4.4 No Unit Tests

No `tests/` directory exists. Zero automated test coverage.

### 4.5 Global Engine Instance

**Location:** `backend/api/main.py:25`

```python
engine = DCLEngine()  # Single global instance
```

**Problem:** No request isolation. Narration service accumulates messages across all requests.

---

## 5. FARM INTEGRATION ANALYSIS

### 5.1 Current State: Broken/Incomplete

**Location:** `backend/engine/schema_loader.py:111-236`

The Farm integration has several critical issues:

#### Issue 1: Hardcoded Endpoints (lines 123-129)

```python
browser_endpoints = [
    {"endpoint": "/api/browser/customers", "table_name": "customers", ...},
    {"endpoint": "/api/browser/invoices", "table_name": "invoices", ...},
    {"endpoint": "/api/synthetic", "table_name": "assets", ...},
    {"endpoint": "/api/synthetic/events", "table_name": "events", ...},
    {"endpoint": "/api/synthetic/crm/accounts", "table_name": "crm_accounts", ...},
]
```

**Problem:** Only 5 fixed endpoints. No dynamic discovery. If Farm adds new data sources, DCL doesn't see them.

#### Issue 2: No Schema Discovery Endpoint

**Lines 280-339:** Schema is *inferred* from JSON records:
```python
def _infer_table_schema_from_json(records, ...):
    all_field_names = set()
    for record in records:
        all_field_names.update(record.keys())  # Guess fields from data
```

**Problem:** This is fragile. Missing fields in sample data = missing from schema.

#### Issue 3: Inefficient Source Limiting

**Lines 212-226:**
```python
# Fetches ALL sources first, then limits
sources.sort(key=lambda s: ...)
if source_limit and source_limit < total_available:
    sources = sources[:source_limit]  # Wasteful!
```

**Problem:** Fetches 500 records from each endpoint, processes them all, THEN limits. Should limit at the API level.

#### Issue 4: No Real API Contract

**Line 149:**
```python
raw_source = record.get("sourceSystem", "unknown")
```

Expects records to have `sourceSystem` field. No validation, no fallback strategy.

### 5.2 Outstanding Questions (from farm_integration_questions.md)

The following questions remain unanswered:
1. **Catalog Discovery:** No endpoint like `/api/synthetic/catalog`
2. **Schema Discovery:** No endpoint like `/api/sources/{id}/schema`
3. **Authentication:** Unclear (API key? Session? None?)
4. **Tenant Context:** How does DCL specify tenant for multi-tenant Farm?

### 5.3 Recommended Farm Integration Architecture

**Option A: Expand Farm for DCL (Preferred)**

Farm should expose:
```
GET /api/dcl/catalog
  → Returns list of available data sources

GET /api/dcl/sources/{source_id}/schema
  → Returns table/field schema for a source

GET /api/dcl/sources/{source_id}/sample?limit=10
  → Returns sample records for schema inference fallback

POST /api/dcl/sources/{source_id}/records?limit=500
  → Returns paginated records with explicit limit
```

DCL changes:
```python
class FarmClient:
    def __init__(self, base_url: str, api_key: str = None):
        self.base_url = base_url
        self.api_key = api_key

    def discover_sources(self) -> List[SourceDescriptor]:
        """Call catalog endpoint to get available sources"""
        response = self._get("/api/dcl/catalog")
        return [SourceDescriptor(**s) for s in response]

    def get_schema(self, source_id: str) -> SourceSchema:
        """Get schema for a specific source"""
        response = self._get(f"/api/dcl/sources/{source_id}/schema")
        return SourceSchema(**response)

    def get_records(self, source_id: str, limit: int) -> List[dict]:
        """Get records with explicit limit"""
        response = self._post(
            f"/api/dcl/sources/{source_id}/records",
            params={"limit": limit}
        )
        return response["data"]
```

**Option B: Separate DCL Farm (Not Recommended)**

Creating a separate farm increases operational complexity:
- Two codebases to maintain
- Data synchronization issues
- Duplicate infrastructure

---

## 6. PRIORITY MATRIX

| Issue | Severity | Effort | Recommendation |
|-------|----------|--------|----------------|
| Farm integration broken | **Critical** | Medium | Fix in next sprint |
| MonitorPanel monolith | High | High | Refactor incrementally |
| snake_case/camelCase inconsistency | High | Low | Add serialization layer |
| Silent error handling | High | Low | Add logging throughout |
| String parsing for data | Medium | Low | Return structured data |
| No unit tests | Medium | High | Add critical path tests |
| Duplicate dependencies | Low | Low | Clean up requirements.txt |
| Global engine instance | Medium | Medium | Add request scoping |

---

## 7. IMMEDIATE ACTION ITEMS

### Week 1: Farm Integration
1. Define DCL-specific endpoints with Farm team
2. Implement `FarmClient` class with proper error handling
3. Add dynamic source discovery
4. Add request-level source limiting (not post-fetch)

### Week 2: Data Contract Cleanup
1. Choose snake_case OR camelCase (recommend snake_case for consistency)
2. Add Pydantic response transformation
3. Replace string parsing with structured data
4. Update frontend types to match

### Week 3: Error Handling & Observability
1. Add Python logging throughout backend
2. Replace silent `continue` with logged warnings
3. Add error boundaries in React components
4. Consider persisting narration to DB

### Ongoing: Refactoring
1. Extract DetailModal from MonitorPanel
2. Extract RAGHistoryTab from MonitorPanel
3. Create HierarchyTree subcomponent
4. Add unit tests for critical paths

---

## 8. APPENDIX: FILE-BY-FILE ISSUES

| File | Lines | Issues |
|------|-------|--------|
| `MonitorPanel.tsx` | 687 | Monolith, 7 state vars, 12-level nesting |
| `schema_loader.py` | 401 | Hardcoded endpoints, silent errors, schema inference |
| `dcl_engine.py` | 352 | Mixed concerns, LLM logic inline |
| `EnterpriseDashboard.tsx` | 323 | String parsing, dual property handling |
| `SankeyGraph.tsx` | 235 | Hardcoded positions, callback state updates |
| `types.ts` | 104 | Duplicate property names (snake_case + camelCase) |
| `main.py` | 155 | Global engine instance, basic error handling |
| `requirements.txt` | ~30 | Duplicate dependencies |
