# DCL Runtime Graph Traversal — The Semantic Query Engine

## What This Does

Today NLQ answers questions by looking up pre-computed answers in fact_base. "What's revenue by segment?" works because that exact combination was pre-built. "What's revenue by cost center for EMEA Cloud division?" fails because nobody pre-computed that specific slice.

Runtime graph traversal replaces flat lookup with graph navigation. When NLQ asks a question, DCL traverses the semantic graph at query time — finding which concepts are involved, which systems hold that data, how to join across systems, and which dimensional slices are valid. The answer is assembled from the graph path, not a pre-built table.

This is the core of Approach 5 (Hybrid: Skeleton + Inference + Runtime Resolution). The skeleton ontology (107 concepts) and AAM edges provide the graph structure. The runtime traversal navigates it.

## Why This Matters

- 107 concepts × 8 dimensions × hierarchy depth = thousands of potential query combinations
- Pre-computing all of them is the old approach (linear scaling, stale data, maintenance burden)
- Runtime traversal computes the answer path on demand — scales to any combination without pre-computation
- When a new system is connected or a reorg changes the hierarchy, the graph updates and traversal automatically follows the new paths
- This is the product differentiator: "Ask any question across any system, get a confidence-scored answer with full provenance"

## The Semantic Graph

The graph is built from 4 sources that already exist:

### Nodes
```
ConceptNode     — 107 ontology concepts (Invoice, Employee, Opportunity, etc.)
DimensionNode   — 8 organizational dimensions (Legal Entity, Division, Cost Center, etc.)
SystemNode      — Source systems discovered by AOD (Salesforce, NetSuite, Workday, etc.)
FieldNode       — Individual fields classified by the normalizer (Salesforce.Opportunity.Amount, etc.)
```

### Edges
```
CLASSIFIED_AS     — FieldNode → ConceptNode (from normalizer, confidence scored)
                    "Salesforce.Opportunity.Amount is classified as Revenue (0.92)"

LIVES_IN          — FieldNode → SystemNode (from AOD discovery)
                    "This field exists in Salesforce"

MAPS_TO           — FieldNode → FieldNode (from AAM semantic edges, Tier 0)
                    "Salesforce.Opportunity.Amount maps to NetSuite.SalesOrder.total (0.95)"

SLICEABLE_BY      — ConceptNode → DimensionNode (from concept-dimension pairings, 325 valid pairs)
                    "Revenue can be sliced by Cost Center"

HIERARCHY_PARENT  — DimensionValue → DimensionValue (from contour map)
                    "Cost Center 4110 rolls up to Cost Center 4100"

AUTHORITATIVE_FOR — SystemNode → DimensionNode (from contour map SOR authority)
                    "Workday is authoritative for Department"

REPORTS_AS        — DimensionValue → DimensionValue (from contour map management overlay)
                    "BU 'Cloud East' + BU 'Cloud West' report as Segment 'Cloud' on the board deck"
```

### Where the Data Comes From

| Edge Type | Source | Already Built? |
|-----------|--------|----------------|
| CLASSIFIED_AS | Normalizer output (heuristic/RAG/LLM/AAM Tier 0) | Yes |
| LIVES_IN | AOD asset inventory → normalizer context | Yes |
| MAPS_TO | AAM semantic edges API | Yes (just integrated) |
| SLICEABLE_BY | concept_dimension_pairings.yaml | Yes (ontology expansion) |
| HIERARCHY_PARENT | Onboarding agent → contour map | In progress (agent build) |
| AUTHORITATIVE_FOR | Onboarding agent → contour map SOR map | In progress |
| REPORTS_AS | Onboarding agent → contour map management overlay | In progress |

The last 3 come from the onboarding agent. For dev/testing, we use hardcoded sample contour data. In production, DCL reads the approved contour map.

## Query Resolution Flow

When NLQ sends a question to DCL, the traversal engine does this:

### Example: "What is revenue by cost center for the Cloud division?"

```
Step 1: INTENT PARSING (already exists in NLQ)
  → concepts needed: [Revenue]
  → dimensions needed: [Cost Center, Division]
  → filters: [Division = "Cloud"]

Step 2: CONCEPT LOCATION (new — graph traversal)
  → Find all FieldNodes classified as "Revenue"
  → Result: [
      Salesforce.Opportunity.Amount (confidence: 0.92, system: salesforce),
      NetSuite.SalesOrder.total (confidence: 0.95, system: netsuite),
      Snowflake.fct_revenue.amount (confidence: 0.95, system: snowflake, via dbt)
    ]
  → Pick highest confidence source, or SOR if contour map specifies one

Step 3: DIMENSION VALIDITY CHECK (new — graph traversal)
  → Is Revenue SLICEABLE_BY Cost Center? Check pairings → YES
  → Is Revenue SLICEABLE_BY Division? Check pairings → YES
  → If NO: return "Revenue cannot be broken down by [dimension]" with explanation

Step 4: DIMENSION SOURCE RESOLUTION (new — graph traversal)
  → Where do Cost Center values live?
    → Check AUTHORITATIVE_FOR edges: "NetSuite is authoritative for Cost Center"
    → Fallback: find systems with fields classified as Cost Center concept
  → Where do Division values live?
    → Check AUTHORITATIVE_FOR: "Workday is authoritative for Division"

Step 5: JOIN PATH DISCOVERY (new — graph traversal)
  → Revenue is in NetSuite (SalesOrder.total)
  → Cost Center is authoritative in NetSuite (same system — direct join)
  → Division is authoritative in Workday (different system — need cross-system path)
    → Check MAPS_TO edges: is there an AAM edge connecting NetSuite ↔ Workday?
    → If yes: follow the edge, note the join key
    → If no: check for intermediate system (both connect to Snowflake?)
    → If no path: return "Cannot connect Revenue to Division — no known data path"

Step 6: FILTER RESOLUTION (new — graph traversal)
  → Division = "Cloud"
    → Check HIERARCHY_PARENT edges: does "Cloud" exist as a dimension value?
    → Check REPORTS_AS edges: does "Cloud" map to specific BU values?
    → If management overlay: "Cloud" on board deck = BU "Cloud East" + "Cloud West" in Workday
    → Resolve filter to actual system values

Step 7: CONFIDENCE SCORING (new)
  → Path confidence = product of edge confidences along the path
  → Revenue field (0.95) × Cost Center SOR (0.90) × Division cross-system join (0.85)
  → Overall: 0.73
  → If below threshold: flag as low-confidence, explain which hop is weakest

Step 8: RESPONSE ASSEMBLY (enhanced)
  → Return to NLQ:
    {
      answer_path: [list of nodes and edges traversed],
      confidence: 0.73,
      confidence_breakdown: {revenue_source: 0.95, cost_center_sor: 0.90, division_join: 0.85},
      provenance: "Revenue from NetSuite SalesOrder.total, Cost Centers from NetSuite,
                   Division from Workday joined via Workato recipe #4782",
      data_query: {system: "snowflake", query_hint: "fct_revenue JOIN dim_cost_center JOIN dim_division"},
      warnings: ["Division join is cross-system via iPaaS — 0.85 confidence"],
      can_answer: true
    }
```

## What to Build

### 1. Graph Store

In-memory graph built at engine startup from existing data sources. NOT a separate database — this is a runtime data structure.

File: `backend/engine/semantic_graph.py`

```python
class SemanticGraph:
    """In-memory semantic graph for query-time traversal."""

    def __init__(self):
        self.nodes: dict[str, GraphNode] = {}    # node_id → node
        self.edges: list[GraphEdge] = []
        self._adjacency: dict[str, list[GraphEdge]] = {}  # node_id → outbound edges

    # --- Build methods (called at startup) ---

    def load_from_normalizer(self, mappings: list[Mapping]):
        """Add ConceptNodes, FieldNodes, SystemNodes, and CLASSIFIED_AS + LIVES_IN edges."""

    def load_from_aam(self, semantic_edges: list[SemanticEdge]):
        """Add MAPS_TO edges between FieldNodes."""

    def load_from_ontology(self, pairings: dict):
        """Add DimensionNodes and SLICEABLE_BY edges from concept_dimension_pairings.yaml."""

    def load_from_contour_map(self, contour_map: dict):
        """Add HIERARCHY_PARENT, AUTHORITATIVE_FOR, REPORTS_AS edges.
           In dev: load from sample contour data.
           In prod: load from approved contour map via API."""

    # --- Query methods ---

    def find_concept_sources(self, concept_id: str) -> list[FieldLocation]:
        """Find all fields classified as this concept, ranked by confidence."""

    def check_dimension_validity(self, concept_id: str, dimension: str) -> bool:
        """Is this concept sliceable by this dimension?"""

    def find_dimension_authority(self, dimension: str) -> SystemAuthority | None:
        """Which system is authoritative for this dimension?"""

    def find_join_path(self, system_a: str, system_b: str, max_hops: int = 3) -> JoinPath | None:
        """Find the shortest path connecting two systems via MAPS_TO edges.
           Returns the path with intermediate systems, join fields, and confidence."""

    def resolve_dimension_filter(self, dimension: str, value: str) -> ResolvedFilter:
        """Resolve a dimension value through hierarchy and management overlay.
           'Cloud' → ['Cloud East', 'Cloud West'] via REPORTS_AS edge."""

    def resolve_hierarchy(self, dimension: str, value: str) -> list[str]:
        """Get all child values under a hierarchy node.
           'Cost Center 4100' → ['4110', '4111', '4112']"""

    # --- Stats ---

    @property
    def stats(self) -> GraphStats:
        """Node counts by type, edge counts by type, connectivity metrics."""
```

### 2. Query Resolver

Takes NLQ's parsed intent and runs the 8-step resolution flow.

File: `backend/engine/query_resolver.py`

```python
class QueryResolver:
    """Resolves NLQ queries against the semantic graph."""

    def __init__(self, graph: SemanticGraph):
        self.graph = graph

    def resolve(self, query_intent: QueryIntent) -> QueryResolution:
        """Run the full 8-step resolution flow.

        Args:
            query_intent: From NLQ — concepts needed, dimensions, filters

        Returns:
            QueryResolution with answer_path, confidence, provenance, data_query, warnings
        """
        # Step 2: Concept location
        sources = self._locate_concepts(query_intent.concepts)

        # Step 3: Dimension validity
        invalid = self._check_dimensions(query_intent.concepts, query_intent.dimensions)
        if invalid:
            return QueryResolution(can_answer=False, reason=f"Cannot slice by {invalid}")

        # Step 4: Dimension source resolution
        dim_sources = self._resolve_dimension_sources(query_intent.dimensions)

        # Step 5: Join path discovery
        paths = self._find_join_paths(sources, dim_sources)
        if not paths:
            return QueryResolution(can_answer=False, reason="No data path found")

        # Step 6: Filter resolution
        resolved_filters = self._resolve_filters(query_intent.filters)

        # Step 7: Confidence scoring
        confidence = self._score_path(paths, sources, dim_sources)

        # Step 8: Response assembly
        return self._assemble_response(sources, dim_sources, paths, resolved_filters, confidence)

    def _locate_concepts(self, concepts: list[str]) -> list[FieldLocation]:
        """Step 2: Find best source for each concept."""

    def _check_dimensions(self, concepts: list[str], dimensions: list[str]) -> list[str]:
        """Step 3: Return invalid concept-dimension pairs."""

    def _resolve_dimension_sources(self, dimensions: list[str]) -> dict[str, SystemAuthority]:
        """Step 4: Find authoritative system for each dimension."""

    def _find_join_paths(self, concept_sources: list, dim_sources: dict) -> list[JoinPath]:
        """Step 5: Find paths connecting concept systems to dimension systems."""

    def _resolve_filters(self, filters: list[QueryFilter]) -> list[ResolvedFilter]:
        """Step 6: Resolve hierarchy and management overlay."""

    def _score_path(self, paths, sources, dim_sources) -> ConfidenceBreakdown:
        """Step 7: Product of edge confidences along traversal path."""

    def _assemble_response(self, *args) -> QueryResolution:
        """Step 8: Build full response with provenance."""
```

### 3. Data Types

File: `backend/engine/graph_types.py`

```python
class GraphNode:
    id: str
    type: Literal["concept", "dimension", "system", "field", "dimension_value"]
    label: str
    metadata: dict

class GraphEdge:
    source_id: str
    target_id: str
    type: Literal["CLASSIFIED_AS", "LIVES_IN", "MAPS_TO", "SLICEABLE_BY",
                  "HIERARCHY_PARENT", "AUTHORITATIVE_FOR", "REPORTS_AS"]
    confidence: float
    provenance: str
    metadata: dict

class FieldLocation:
    system: str
    object_name: str
    field: str
    concept: str
    confidence: float

class SystemAuthority:
    system: str
    dimension: str
    confidence: float
    source: Literal["contour_map", "inferred", "default"]

class JoinPath:
    hops: list[JoinHop]
    total_confidence: float
    description: str

class JoinHop:
    from_system: str
    from_field: str
    to_system: str
    to_field: str
    via: str              # "direct" | "workato_recipe_4782" | "snowflake_join"
    confidence: float

class ResolvedFilter:
    dimension: str
    original_value: str
    resolved_values: list[str]     # After hierarchy/overlay expansion
    resolution_type: str           # "exact" | "hierarchy_expansion" | "management_overlay"

class QueryIntent:
    concepts: list[str]
    dimensions: list[str]
    filters: list[QueryFilter]
    persona: str | None

class QueryFilter:
    dimension: str
    operator: str          # "equals" | "in" | "not"
    value: str | list[str]

class QueryResolution:
    can_answer: bool
    answer_path: list[GraphEdge] | None
    confidence: float
    confidence_breakdown: dict[str, float]
    provenance: str
    data_query: DataQueryHint | None
    warnings: list[str]
    reason: str | None     # If can_answer is False

class DataQueryHint:
    primary_system: str
    tables: list[str]
    join_keys: list[dict]
    filters: list[dict]
    description: str

class ConfidenceBreakdown:
    overall: float
    per_hop: dict[str, float]
    weakest_link: str
    weakest_confidence: float

class GraphStats:
    concept_nodes: int
    dimension_nodes: int
    system_nodes: int
    field_nodes: int
    edges_by_type: dict[str, int]
    connected_systems: int
    avg_path_confidence: float
```

### 4. API Endpoint

Add to DCL's existing API:

```
POST /api/dcl/resolve
  Body: { concepts: ["revenue"], dimensions: ["cost_center", "division"], filters: [{dimension: "division", operator: "equals", value: "Cloud"}], persona: "CFO" }
  Returns: QueryResolution

GET /api/dcl/graph/stats
  Returns: GraphStats (node/edge counts, connectivity metrics)

GET /api/dcl/graph/path?from_concept=revenue&to_dimension=cost_center
  Returns: JoinPath (for debugging/visualization — shows how DCL would connect these)
```

### 5. Sample Contour Data (for dev/testing)

Until the onboarding agent produces real contour maps, the graph needs sample dimensional data to test traversal.

File: `config/sample_contour.yaml`

```yaml
# Sample enterprise: mid-market SaaS company
# 3 divisions, 2 regions, 12 cost centers, 8 departments

hierarchy:
  division:
    - id: div-cloud
      name: Cloud
      children:
        - id: div-cloud-east
          name: Cloud East
        - id: div-cloud-west
          name: Cloud West
    - id: div-services
      name: Professional Services
    - id: div-platform
      name: Platform

  cost_center:
    - id: cc-4000
      name: Engineering
      children:
        - {id: cc-4100, name: Cloud Engineering}
        - {id: cc-4200, name: Platform Engineering}
    - id: cc-5000
      name: Sales
      children:
        - {id: cc-5100, name: Enterprise Sales}
        - {id: cc-5200, name: Mid-Market Sales}
    - id: cc-6000
      name: G&A
      children:
        - {id: cc-6100, name: Finance}
        - {id: cc-6200, name: HR}
        - {id: cc-6300, name: Legal}

  region:
    - id: reg-na
      name: North America
      children:
        - {id: reg-us, name: United States}
        - {id: reg-ca, name: Canada}
    - id: reg-emea
      name: EMEA
      children:
        - {id: reg-uk, name: United Kingdom}
        - {id: reg-de, name: Germany}

sor_authority:
  cost_center: {system: netsuite, confidence: 0.90}
  department: {system: workday, confidence: 0.95}
  division: {system: workday, confidence: 0.90}
  region: {system: salesforce, confidence: 0.85}

management_overlay:
  # Board sees 3 segments, but Workday has 5 BUs
  - board_segment: Cloud
    maps_to: [Cloud East, Cloud West]
  - board_segment: Services
    maps_to: [Professional Services]
  - board_segment: Platform
    maps_to: [Platform]
```

### 6. Graph Rebuild Strategy

The graph is rebuilt:
- On engine startup (always)
- When a new classification run completes (normalizer produced new mappings)
- When AAM edge cache refreshes (every 5 min if edges changed)
- When a contour map is approved (onboarding agent flow)
- NOT on every query (too expensive)

Rebuild is fast (in-memory dict construction from existing data). Target: < 500ms for a typical mid-market graph (500 fields, 200 edges, 107 concepts).

### 7. Caching Common Paths

The most common queries hit the same paths repeatedly. Cache resolved paths:

```python
# In QueryResolver
_path_cache: dict[str, QueryResolution] = {}
_path_cache_ttl: int = 300  # 5 minutes

def _cache_key(self, intent: QueryIntent) -> str:
    """Deterministic key from intent (concepts + dimensions + filters, sorted)."""

def resolve(self, intent: QueryIntent) -> QueryResolution:
    key = self._cache_key(intent)
    if key in self._path_cache and not expired:
        return self._path_cache[key]
    result = self._resolve_uncached(intent)
    self._path_cache[key] = result
    return result
```

Cache invalidates when graph rebuilds.

## What NOT to Change

- NLQ's intent parsing (NLQ still parses the question, sends structured intent to DCL)
- The normalizer's classification pipeline (Tier 0-3 stays as-is)
- The existing fact_base / graph snapshot for backward compatibility
- AAM or onboarding agent code (DCL only reads from their APIs/output)
- The ontology config

## Build Order

1. Read the current graph snapshot code (dcl_engine.py build_graph_snapshot) and understand how NLQ currently gets answers from DCL. Report back.
2. Create graph_types.py with all type definitions. Commit.
3. Build SemanticGraph class with all load methods. Use existing normalizer output for CLASSIFIED_AS/LIVES_IN, AAM client for MAPS_TO, ontology config for SLICEABLE_BY. Load sample contour YAML for hierarchy/authority/overlay edges. Commit.
4. Build QueryResolver with the 8-step resolution flow. Start with steps 2-4 (concept location, dimension validity, dimension source). Commit.
5. Add steps 5-6 (join path discovery, filter resolution). Commit.
6. Add steps 7-8 (confidence scoring, response assembly). Commit.
7. Add path caching. Commit.
8. Add API endpoints (POST /resolve, GET /graph/stats, GET /graph/path). Commit.
9. Wire graph build into engine startup and rebuild triggers. Commit.
10. Test: exercise all 8 steps against sample contour data + mock normalizer output + mock AAM edges. Test the example query from the spec ("revenue by cost center for Cloud division"). Verify confidence scoring, provenance, and graceful degradation when graph is incomplete. Commit.

## Performance Target

- Graph build: < 500ms for typical mid-market (500 fields, 200 AAM edges, 107 concepts, 50 hierarchy nodes)
- Query resolution: < 200ms for a 3-concept, 2-dimension query (excluding any data fetch)
- Path cache hit: < 5ms
- These are the compute-only targets. Actual data retrieval from source systems is a separate concern (NLQ handles that based on the DataQueryHint).
