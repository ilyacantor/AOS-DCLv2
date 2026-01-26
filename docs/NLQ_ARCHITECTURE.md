# NLQ (Natural Language Query) Architecture

**Last Updated:** January 26, 2026

## Overview

The NLQ layer is a **compiler** that translates natural language questions into structured BLL (Business Logic Layer) definition executions. It extracts intent, parameters, and operators from questions, then routes them to the appropriate definition for execution.

**Key Design Principle:** NLQ is a pure compiler - it extracts what the user wants (TopN limit, time window) but does NOT build SQL or apply ordering. Ordering is declared in the definition specification.

```
┌────────────────────────────────────────────────────────────────────┐
│                      NLQ Processing Pipeline                        │
│                                                                      │
│  "Top 5 customers by revenue"                                        │
│          │                                                           │
│          ▼                                                           │
│  ┌───────────────┐   ┌───────────────┐   ┌───────────────────────┐  │
│  │   Operator    │ → │    Intent     │ → │    Parameter          │  │
│  │   Extractor   │   │    Matcher    │   │    Extractor          │  │
│  └───────────────┘   └───────────────┘   └───────────────────────┘  │
│          │                   │                      │                │
│          ▼                   ▼                      ▼                │
│   temporal: null       crm.top_customers       limit: 5             │
│   comparison: null     confidence: 0.85        time_window: null    │
│                                                                      │
│          └───────────────────┬───────────────────┘                  │
│                              ▼                                       │
│                    ┌───────────────────┐                            │
│                    │   BLL Executor    │                            │
│                    │ (applies ordering │                            │
│                    │  from definition) │                            │
│                    └───────────────────┘                            │
│                              │                                       │
│                              ▼                                       │
│                         ExecuteResponse                              │
│                    (data + summary + lineage)                        │
└────────────────────────────────────────────────────────────────────┘
```

---

## File Structure

```
backend/nlq/
├── __init__.py
├── models.py              # Pydantic models for NLQ domain
├── intent_matcher.py      # Question → Definition matching
├── operator_extractor.py  # Temporal/comparison operator detection
├── param_extractor.py     # TopN limit, time window extraction
├── compiler.py            # SQL template generation (unused in BLL path)
├── validator.py           # Definition answerability validation
├── scorer.py              # Answerability scoring
├── hypothesis.py          # Hypothesis generation for circles
├── explainer.py           # Natural language explanation generation
├── proof.py               # Proof pointer resolution
├── lineage.py             # Data lineage tracking
├── consistency.py         # Consistency checking
├── schema_enforcer.py     # Schema validation
├── persistence.py         # In-memory persistence layer
├── db_persistence.py      # PostgreSQL persistence layer
├── db_models.py           # SQLAlchemy models
├── registry.py            # Definition registry
├── routes_registry.py     # API route helpers
├── normalized_intent.py   # Intent normalization
├── executor.py            # NLQ-specific executor
└── migrations/
    ├── env.py
    └── versions/
        └── 001_initial_schema.py
```

---

## Core Components

### 1. Operator Extractor (`operator_extractor.py`)

Detects **temporal**, **comparison**, and **aggregation** operators from questions.

**Operator Types:**

| Type | Operators | Example Phrases |
|------|-----------|-----------------|
| **Temporal** | MoM, QoQ, YoY, WoW, DoD | "month over month", "vs last quarter", "year over year" |
| **Comparison** | change, delta, increase, decrease, growth, trend | "how did X change", "variance", "grew", "dropped" |
| **Aggregation** | total, sum, average, count, top, bottom | "top 5", "total spend", "average cost" |

**Key Insight:** Operators are INDEPENDENT of the metric being queried. "How did revenue change MoM?" and "How did costs change MoM?" both need `supports_delta` capability.

```python
@dataclass
class ExtractedOperators:
    temporal: Optional[TemporalOperator] = None      # MoM, QoQ, YoY
    comparison: Optional[ComparisonOperator] = None  # change, delta, growth
    aggregation: Optional[AggregationOperator] = None # total, top, count
    requires_delta: bool = False   # Needs supports_delta capability
    requires_trend: bool = False   # Needs supports_trend capability
```

**Pattern Matching:**
```python
TEMPORAL_PATTERNS = {
    TemporalOperator.MOM: [
        r'\bmom\b',
        r'\bmonth[- ]over[- ]month\b',
        r'\bvs\.?\s*last month\b',
    ],
    TemporalOperator.QOQ: [
        r'\bqoq\b',
        r'\bquarter[- ]over[- ]quarter\b',
    ],
    # ...
}
```

---

### 2. Intent Matcher (`intent_matcher.py`)

Maps questions to BLL definitions using keyword matching and capability filtering.

**Matching Algorithm:**

1. **Extract Operators** - Detect temporal/comparison operators first
2. **Get Required Capabilities** - Map operators to capability requirements
3. **Score Candidates** - Match keywords with synonym expansion
4. **Filter by Capabilities** - Only route to definitions that support required operators
5. **Detect Ambiguity** - If top candidates are within 15% score, flag as ambiguous

```python
@dataclass
class MatchResult:
    best_match: str              # Definition ID (e.g., "crm.top_customers")
    confidence: float            # 0.0-1.0 confidence score
    matched_keywords: List[str]  # Keywords that matched
    top_candidates: List[MatchCandidate]  # Top-K for confusion reporting
    is_ambiguous: bool           # True if candidates are close in score
    ambiguity_gap: float         # Score gap between #1 and #2
    operators: ExtractedOperators # Extracted operators
    capability_routed: bool      # True if routed by capability
```

**Synonym Mappings:**
```python
SYNONYMS = {
    "arr": ["arr", "annual recurring revenue", "recurring revenue"],
    "customer": ["customer", "client", "account", "buyer"],
    "cost": ["cost", "spend", "spending", "expense", "price"],
    "zombie": ["zombie", "idle", "unused", "orphan", "wasted"],
    # ...
}
```

**Ambiguity Groups:**
```python
AMBIGUOUS_GROUPS = {
    "dora": {
        "definitions": ["infra.deploy_frequency", "infra.lead_time", ...],
        "clarification": "Which DORA metric: deployment frequency, lead time, ...?",
    },
    "orphan": {
        "definitions": ["aod.zombies_overview", "aod.identity_gap_financially_anchored"],
        "clarification": "Do you mean orphan resources or resources without an owner?",
    },
}
```

---

### 3. Parameter Extractor (`param_extractor.py`)

Extracts execution parameters from questions. **Does NOT extract ordering** - that's defined in the definition spec.

**Extracted Parameters:**

| Parameter | Pattern Examples | Extracted Value |
|-----------|------------------|-----------------|
| `limit` | "top 5", "first 10", "show me 3" | `5`, `10`, `3` |
| `time_window` | "last month", "YTD", "this quarter" | `last_month`, `ytd`, `current_quarter` |

**PRODUCTION BOUNDARY:** NLQ extracts TopN(limit) intent only. Ordering is determined by `definition.capabilities.default_order_by`.

```python
def extract_params(question: str, allowed_params: List[str] = None) -> ExecutionArgs:
    """
    Extract execution parameters from a question.
    
    PRODUCTION BOUNDARY: NLQ extracts TopN(limit) intent only.
    Ordering is determined by definition.capabilities.default_order_by.
    """
    args = ExecutionArgs()
    
    if "limit" in allowed:
        args.limit = extract_limit(question)
    
    if "time_window" in allowed:
        args.time_window = extract_time_window(question)
    
    return args
```

**Limit Extraction Patterns:**
```python
# "top 5 customers" → 5
r'\btop\s+(\d+)\b'

# "first 10 results" → 10
r'\bfirst\s+(\d+)\b'

# Word numbers: "top five" → 5
word_numbers = {'one': 1, 'two': 2, 'three': 3, ...}
```

**Time Window Patterns:**
```python
time_patterns = {
    r'\blast\s+month\b': 'last_month',
    r'\bthis\s+quarter\b': 'current_quarter',
    r'\b(?:ytd|year\s+to\s+date)\b': 'ytd',
    r'\blast\s+(\d+)\s+days?\b': 'last_n_days',
}
```

---

### 4. BLL Definition Model (`bll/models.py`)

Definitions declare their **capabilities** and **ordering** - not phrase variants.

```python
class DefinitionCapabilities(BaseModel):
    supports_top_n: bool = True      # Can be limited/ranked
    supports_delta: bool = False     # Supports MoM/QoQ/YoY comparison
    supports_trend: bool = False     # Supports time-series trending
    supports_aggregation: bool = True
    primary_metric: Optional[str]    # "revenue", "cost", etc.
    entity_type: Optional[str]       # "customer", "vendor", "resource"
    
    # Production-grade ordering
    default_order_by: List[OrderBySpec]  # Concrete columns for TopN
    allowed_order_by: List[str]          # Whitelist of override columns
    tie_breaker: Optional[str]           # Secondary sort for determinism
```

**Example Definition:**
```python
Definition(
    definition_id="crm.top_customers",
    name="Top Customers by Revenue",
    keywords=["top customers", "largest customers", "biggest accounts"],
    capabilities=DefinitionCapabilities(
        supports_top_n=True,
        primary_metric="revenue",
        entity_type="customer",
        default_order_by=[
            OrderBySpec(field="AnnualRevenue", direction="desc")
        ],
        allowed_order_by=["AnnualRevenue", "NumberOfEmployees"],
        tie_breaker="Id",  # Deterministic ordering
    ),
)
```

---

## API Endpoints

### Core NLQ Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/nlq/ask` | POST | Execute natural language question |
| `/api/nlq/extract_params` | POST/GET | Extract parameters from question |
| `/api/nlq/answerability_rank` | POST | Get ranked hypotheses (circles) |
| `/api/nlq/explain` | POST | Generate explanation for hypothesis |

### Registration Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/nlq/bindings` | GET/POST/DELETE | Manage source-to-canonical bindings |
| `/api/nlq/events` | GET/POST | Manage canonical events |
| `/api/nlq/entities` | GET/POST | Manage business entities |
| `/api/nlq/definitions` | GET/POST | Manage definitions |
| `/api/nlq/definition_versions` | GET/POST | Manage definition versions |
| `/api/nlq/proof_hooks` | GET/POST | Manage proof pointers |

---

## Request/Response Examples

### `/api/nlq/ask` - Execute Question

**Request:**
```json
{
  "question": "top 5 customers by revenue",
  "dataset_id": "demo9"
}
```

**Response:**
```json
{
  "definition_id": "crm.top_customers",
  "confidence": 0.92,
  "matched_keywords": ["top", "customers", "revenue"],
  "data": [
    {"Id": "001...", "Name": "Enterprise Plus Corp", "AnnualRevenue": 89000000},
    {"Id": "002...", "Name": "TechFlow Inc", "AnnualRevenue": 48000000},
    ...
  ],
  "summary": {
    "answer": "Top 5 customers by revenue:\n1. Enterprise Plus Corp: $89M\n...",
    "aggregations": {
      "customer_count": 5,
      "shown_total": 234500000,
      "population_total": 456000000
    }
  },
  "lineage": [
    {
      "source_id": "salesforce",
      "table_id": "salesforce_account",
      "columns_used": ["Id", "Name", "AnnualRevenue"]
    }
  ]
}
```

### `/api/nlq/extract_params` - Extract Parameters

**Request:**
```json
{
  "question": "show me top 10 vendors from last quarter"
}
```

**Response:**
```json
{
  "limit": 10,
  "time_window": "last_quarter",
  "order_by": null
}
```

---

## Execution Flow

### Step 1: Question Parsing
```
Input: "What are my top 5 customers by revenue this quarter?"
                                │
                                ▼
┌─────────────────────────────────────────────────────────────┐
│ Operator Extractor                                          │
│   temporal: null                                            │
│   comparison: null                                          │
│   aggregation: TOP                                          │
│   requires_delta: false                                     │
└─────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────┐
│ Intent Matcher                                              │
│   best_match: "crm.top_customers"                          │
│   confidence: 0.92                                          │
│   matched_keywords: ["top", "customers", "revenue"]        │
│   capability_routed: true (supports_top_n=true)            │
└─────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────┐
│ Parameter Extractor                                         │
│   limit: 5                                                  │
│   time_window: "current_quarter"                           │
└─────────────────────────────────────────────────────────────┘
```

### Step 2: Definition Execution
```
┌─────────────────────────────────────────────────────────────┐
│ BLL Executor                                                │
│                                                             │
│   1. Load definition: crm.top_customers                    │
│   2. Load data from sources (CSV or Farm)                  │
│   3. Apply default_order_by: [AnnualRevenue DESC]          │
│   4. Apply tie_breaker: Id                                 │
│   5. Apply limit: 5                                        │
│   6. Compute summary with share-of-total                   │
│   7. Return ExecuteResponse                                │
└─────────────────────────────────────────────────────────────┘
```

---

## Data Sources

### Demo Mode (dataset_id: "demo9")
- Reads from local CSV files: `dcl/demo/datasets/demo9/`
- Static test data for development
- No external dependencies

### Farm Mode (dataset_id: "farm:{scenario_id}")
- Fetches from Farm's ground truth API
- Deterministic scenarios generated from seed
- Used for integration testing

**Farm Integration:**
```python
# Generate scenario
POST /api/scenarios/generate {"seed": 12345, "scale": "medium"}
# Returns: {"scenario_id": "dfa0ae0d57c9", ...}

# Query DCL with Farm data
POST /api/nlq/ask {
  "question": "top 5 customers by revenue",
  "dataset_id": "farm:dfa0ae0d57c9"
}
```

---

## Key Design Decisions

### 1. NLQ = Compiler, Not SQL Builder
- NLQ extracts **intent** (which definition) and **parameters** (limit, time_window)
- NLQ does NOT generate SQL or apply ordering
- Ordering is declared in definition spec, applied by BLL executor

### 2. Capability-Based Routing
- Definitions declare capabilities (supports_delta, supports_trend, etc.)
- Operator extraction determines required capabilities
- Matching filters definitions that don't support required operations

### 3. Deterministic Ordering
- `default_order_by`: Concrete columns with direction
- `tie_breaker`: Secondary sort for reproducible results
- `allowed_order_by`: Whitelist for future override support

### 4. Production Boundary
```
┌────────────────────┬────────────────────────────────────┐
│       NLQ          │           BLL Executor             │
├────────────────────┼────────────────────────────────────┤
│ Extract limit      │ Apply default_order_by             │
│ Extract time_window│ Apply tie_breaker                  │
│ Match definition   │ Apply limit AFTER sorting          │
│ Extract operators  │ Compute summary with population    │
└────────────────────┴────────────────────────────────────┘
```

---

## Answerability Circles (Advanced)

The Answerability system provides ranked hypotheses when a question could be answered multiple ways.

### Circle Model
```python
class Circle:
    id: str                    # Hypothesis ID
    rank: int                  # Left-to-right order
    label: str                 # Human-readable label
    probability_of_answer: float  # How likely to answer (0.0-1.0)
    confidence: float          # Evidence quality (0.0-1.0)
    color: "hot" | "warm" | "cool"  # Confidence visualization
    requires: CircleRequirements    # What's needed to answer
```

### Validation Flow
```
Question → Hypotheses → Validate Each → Rank by Coverage → Return Circles
                           │
                           ▼
              ┌────────────────────────┐
              │ DefinitionValidator    │
              │   - Missing events?    │
              │   - Missing dimensions?│
              │   - Weak bindings?     │
              │   - Coverage score     │
              └────────────────────────┘
```

---

## Persistence Layer

### In-Memory (`persistence.py`)
- Fast, no database dependency
- Seeded with demo data on startup
- Used for development and testing

### PostgreSQL (`db_persistence.py`)
- Production persistence
- SQLAlchemy models in `db_models.py`
- Migrations in `migrations/`

**Tables:**
- `canonical_events` - Event type definitions
- `entities` - Business entities (dimensions)
- `bindings` - Source → canonical mappings
- `definitions` - Metric/view definitions
- `definition_versions` - Versioned specs
- `proof_hooks` - Source system pointers

---

## Summary

The NLQ layer is a **pure compiler** that:

1. **Extracts operators** (temporal, comparison, aggregation) from questions
2. **Matches intent** to BLL definitions using keywords + capability filtering
3. **Extracts parameters** (limit, time_window) but NOT ordering
4. **Routes to BLL executor** which applies definition-declared ordering

This architecture ensures:
- **Separation of concerns**: NLQ handles language, BLL handles data
- **Deterministic results**: Ordering declared in specs, not inferred
- **Capability-based routing**: Only valid definitions are considered
- **Extensibility**: New operators/definitions don't require NLQ changes
