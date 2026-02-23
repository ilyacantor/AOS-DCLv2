# DCL ← AAM Integration — Consume Semantic Edges

## What This Does

DCL's normalizer currently classifies fields using a 3-tier pipeline: heuristic matching → RAG lookup → LLM inference. All three tiers work from field names and schema context alone — they don't know how fields are actually connected across systems.

AAM now produces **SemanticEdge** data — explicit field-to-field mappings extracted from integration infrastructure (Workato recipes, dbt models, event bus schemas). These are 0.95 confidence edges that a human already built and tested.

This task adds a **Tier 0** to the normalizer pipeline: before any heuristic/RAG/LLM classification, check whether AAM already has an explicit mapping for this field. If it does, use it. If it doesn't, fall through to the existing pipeline.

## Why Tier 0 (Not a Replacement)

The existing 3 tiers still matter:
- AAM edges only exist where integration logic exists (maybe 30-40% of fields in a typical enterprise)
- New systems with no integrations yet have zero AAM edges
- Fields that are only in the warehouse (never flow through iPaaS/event bus) may not have edges
- The normalizer needs to classify ALL fields, not just connected ones

So the pipeline becomes:

```
Tier 0: AAM semantic edge lookup (0.95 confidence)
  ↓ miss
Tier 1: Heuristic pattern matching (0.70-0.95 confidence)
  ↓ low confidence
Tier 2: RAG embedding similarity (0.60-0.85 confidence)
  ↓ low confidence
Tier 3: LLM classification (0.50-0.90 confidence)
```

A "hit" at Tier 0 means: skip all other tiers for this field. The mapping is already known with high confidence from real infrastructure.

## What AAM Provides

AAM's new endpoint:
```
GET /api/topology/semantic-edges
  ?source_system=salesforce
  ?target_system=netsuite
  ?confidence_min=0.8
  ?fabric_plane=IPAAS

Returns: SemanticEdge[]
```

Each SemanticEdge contains:
```
{
  source_system: "salesforce",
  source_object: "Opportunity",
  source_field: "Amount",
  target_system: "netsuite",
  target_object: "SalesOrder",
  target_field: "total",
  edge_type: "DIRECT_MAP" | "TRANSFORMED" | "CONDITIONAL" | "INFERRED",
  confidence: 0.95,
  fabric_plane: "IPAAS",
  extraction_source: "workato_recipe_4782",
  transformation: null | "CONCAT(FirstName, LastName)"
}
```

## What to Build

### 1. AAM Client

Create a service that calls AAM's semantic edges endpoint.

File: `backend/clients/aam_client.py`

```python
class AAMClient:
    def __init__(self, base_url: str, timeout: float):
        # base_url from env: AAM_API_URL
        # timeout from constants

    async def get_semantic_edges(
        self,
        source_system: str | None = None,
        target_system: str | None = None,
        confidence_min: float = 0.8
    ) -> list[SemanticEdge]:
        # GET /api/topology/semantic-edges with query params
        # On failure: log warning, return empty list (graceful degradation)
        # Cache results for 5 minutes (edges don't change frequently)
```

### 2. SemanticEdge Model (DCL side)

File: `backend/domain/models.py`

Add a Pydantic model matching AAM's response shape. DCL doesn't need to store these — it reads them from AAM on demand (with caching).

### 3. Edge Index

The normalizer processes fields one at a time. It needs a fast lookup: "for this system + field, is there an AAM edge?"

File: `backend/engine/edge_index.py`

```python
class EdgeIndex:
    """In-memory index of AAM semantic edges for fast field lookup."""

    def __init__(self, edges: list[SemanticEdge]):
        # Build lookup dicts:
        # by_source: (system, object, field) → list[SemanticEdge]
        # by_target: (system, object, field) → list[SemanticEdge]
        # by_system_pair: (source_system, target_system) → list[SemanticEdge]

    def lookup(self, system: str, object_name: str, field: str) -> SemanticEdge | None:
        # Check by_source first (this field maps TO something)
        # Then check by_target (this field is mapped FROM something)
        # Return highest confidence edge if multiple matches
        # Return None if no edge exists

    def get_related_fields(self, system: str, object_name: str, field: str) -> list[SemanticEdge]:
        # Return ALL edges involving this field (both directions)
        # Used by the contour map builder to show cross-system relationships

    @property
    def coverage(self) -> dict:
        # Returns stats: total edges, edges by plane, edges by confidence tier
        # Used for telemetry and the intel brief
```

### 4. Tier 0 in the Normalizer

The normalizer's main classification function needs a new first step.

Where: Find the main classification function (likely in `dcl_engine.py` or `semantic_mapper/heuristic_mapper.py`)

Logic:
```python
def classify_field(self, field_name, table_name, system_name, ...):
    # --- NEW: Tier 0 — AAM edge lookup ---
    edge = self.edge_index.lookup(system_name, table_name, field_name)
    if edge and edge.confidence >= 0.8:
        return ClassificationResult(
            concept=self._edge_to_concept(edge),
            confidence=edge.confidence,
            tier="aam_edge",
            provenance=f"AAM {edge.fabric_plane}: {edge.extraction_source}",
            cross_system_mapping={
                "maps_to_system": edge.target_system,
                "maps_to_object": edge.target_object,
                "maps_to_field": edge.target_field,
                "edge_type": edge.edge_type,
                "transformation": edge.transformation
            }
        )

    # --- Existing: Tier 1 — Heuristic ---
    heuristic_result = self.heuristic_classify(field_name, ...)
    if heuristic_result.confidence >= 0.9:
        return heuristic_result

    # --- Existing: Tier 2 — RAG ---
    # ... existing code ...

    # --- Existing: Tier 3 — LLM ---
    # ... existing code ...
```

### 5. Edge-to-Concept Mapping

When an AAM edge says "Salesforce.Opportunity.Amount maps to NetSuite.SalesOrder.total", DCL needs to figure out which ontology concept that represents.

File: `backend/engine/edge_index.py` (method on EdgeIndex)

```python
def _edge_to_concept(self, edge: SemanticEdge) -> str:
    # Strategy 1: Check if source or target field name matches
    # an ontology concept alias (use the same alias lookup
    # the heuristic mapper uses)
    #
    # Strategy 2: Check if the source/target object name matches
    # a concept (e.g., object "Opportunity" → concept "opportunity")
    #
    # Strategy 3: If no match, return the edge but mark concept
    # as "unclassified_but_mapped" — the field has a known
    # cross-system relationship even if we can't name the concept yet
    #
    # This is a lightweight lookup, NOT an LLM call.
    # If we can't determine the concept cheaply, let Tier 1-3 handle it
    # but still attach the cross_system_mapping metadata.
```

### 6. Metrics Update

Add Tier 0 stats to RunMetrics:

```python
# In RunMetrics or equivalent
aam_edge_hits: int = 0        # Fields classified via AAM edge
aam_edge_misses: int = 0      # Fields with no AAM edge (fell through)
aam_edge_total: int = 0       # Total edges loaded from AAM
aam_cache_hit: bool = False   # Whether edge data came from cache
aam_unavailable: bool = False # Whether AAM API was unreachable
```

### 7. Startup / Initialization

When the DCL engine starts (or when a classification run begins):
1. Call AAM client to fetch all semantic edges
2. Build EdgeIndex from the response
3. Pass EdgeIndex to the normalizer
4. If AAM is unavailable, log warning and continue with empty EdgeIndex (Tier 0 returns no hits, pipeline falls through to Tier 1 as before)

The 5-minute cache means this only hits AAM once per 5 minutes, not once per field.

## Constants to Add

In `backend/core/constants.py`:

```python
AAM_API_URL = os.getenv("AAM_API_URL", "http://localhost:8001")
AAM_API_TIMEOUT = float(os.getenv("AAM_API_TIMEOUT", "10"))
AAM_EDGE_CACHE_TTL = int(os.getenv("AAM_EDGE_CACHE_TTL", "300"))  # 5 min
AAM_EDGE_CONFIDENCE_MIN = float(os.getenv("AAM_EDGE_CONFIDENCE_MIN", "0.8"))
```

## What NOT to Change

- The existing 3-tier pipeline (heuristic → RAG → LLM). Tier 0 is additive.
- The ontology config (107 concepts, just expanded — don't touch).
- The contour map or graph snapshot structure.
- Any AAM code (AAM is a separate repo, DCL only reads from its API).

## Build Order

1. Read the normalizer's current classification flow end-to-end. Find the main entry point, how fields flow through tiers, and where results are assembled. Report back.
2. Add constants (AAM_API_URL, timeout, cache TTL, confidence min). Commit.
3. Add SemanticEdge Pydantic model to domain/models.py. Commit.
4. Build AAM client with caching and graceful degradation. Commit.
5. Build EdgeIndex with lookup, get_related_fields, coverage stats. Commit.
6. Wire Tier 0 into the normalizer classification flow. Commit.
7. Build edge-to-concept mapping logic. Commit.
8. Add AAM metrics to RunMetrics. Commit.
9. Wire startup: fetch edges → build index → pass to normalizer. Commit.
10. Test: mock AAM responses, verify Tier 0 hits override heuristic, verify graceful degradation when AAM unavailable, verify metrics populated. Commit.

## Test Scenarios

1. **AAM has edge, high confidence**: Field "Opportunity.Amount" → AAM returns edge to "SalesOrder.total" at 0.95 → Tier 0 returns result, Tier 1-3 skipped
2. **AAM has edge, low confidence**: Edge at 0.65 → Tier 0 skips, falls through to Tier 1
3. **AAM has no edge**: Field not in any edge → Tier 0 miss, falls through to Tier 1 (existing behavior unchanged)
4. **AAM unavailable**: API timeout → empty EdgeIndex, all fields fall through to Tier 1 (existing behavior), aam_unavailable=True in metrics
5. **AAM edge + heuristic disagree**: Edge says "revenue", heuristic says "cost" → Tier 0 wins (higher confidence from real infrastructure)
6. **Transformed edge**: Edge has transformation formula → still classified, but provenance notes the transformation
7. **Cache test**: Two classification runs within 5 minutes → second run uses cached edges, doesn't call AAM
