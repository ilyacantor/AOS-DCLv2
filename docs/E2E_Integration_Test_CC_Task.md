# End-to-End Integration Test — NLQ → DCL Graph Traversal

## What This Proves

The full chain works: a human asks a question in plain English → NLQ parses intent → DCL traverses the semantic graph → finds the data path across systems → returns a confidence-scored answer with full provenance.

This is NOT about building new features. Everything is already built. This is about wiring the pieces together and proving the chain works end-to-end against sample data.

## The Chain

```
User: "What is revenue by cost center for the Cloud division?"
  ↓
NLQ: Parses intent → {concepts: ["revenue"], dimensions: ["cost_center", "division"], filters: [{dimension: "division", value: "Cloud"}]}
  ↓
DCL: POST /api/dcl/resolve → traverses semantic graph
  ↓
  Step 2: Finds revenue in NetSuite.SalesOrder.total (0.95 confidence)
  Step 3: Checks revenue × cost_center → valid. revenue × division → valid.
  Step 4: Cost center SOR = NetSuite. Division SOR = Workday.
  Step 5: Finds join path NetSuite ↔ Workday via AAM edge
  Step 6: "Cloud" → resolves via management overlay → ["Cloud East", "Cloud West"]
  Step 7: Path confidence = 0.73
  Step 8: Returns QueryResolution with provenance
  ↓
NLQ: Receives resolution, formats answer for user
```

## What Needs Wiring

### 1. NLQ → DCL Resolve Endpoint

NLQ currently calls `POST /api/dcl/query` with a metric alias and optional dimensions. It gets back pre-computed data from fact_base.

NLQ needs a **second path** that calls `POST /api/dcl/resolve` with a structured QueryIntent. This doesn't replace the existing path — it's a new resolution mode that NLQ can use when the graph is available.

Find in NLQ's codebase:
- Where NLQ calls DCL's API (likely an HTTP client or service)
- Where NLQ resolves a parsed question into a DCL query
- Where NLQ formats the DCL response for the user

Add:
- A `resolve_via_graph()` method that calls `POST /api/dcl/resolve`
- Logic to try graph resolution first, fall back to flat query if graph unavailable or can_answer=false
- Format the QueryResolution (provenance, confidence, warnings) into NLQ's response format

### 2. DCL Sample Data Setup

The graph traversal engine needs data to traverse. For the integration test, DCL needs:

**Already in place:**
- 107 ontology concepts with concept-dimension pairings (from ontology expansion)
- Sample contour YAML with hierarchy, SOR authority, management overlay (from graph traversal build)
- Normalizer that classifies fields into concepts (existing pipeline)

**Needs verification:**
- AAM semantic edges endpoint is reachable and returns edges (or gracefully degrades)
- Sample normalizer mappings exist that include revenue, cost_center, division fields
- The graph builds successfully on startup with all data sources

### 3. Test Queries

These are the queries to run end-to-end. Each tests a different capability:

```yaml
test_queries:
  # Basic concept lookup — simplest case
  - question: "What is total revenue?"
    expected:
      can_answer: true
      concepts_found: [revenue]
      min_confidence: 0.7

  # Concept + single dimension
  - question: "Show me revenue by region"
    expected:
      can_answer: true
      concepts_found: [revenue]
      dimensions_used: [region]
      min_confidence: 0.7

  # Concept + two dimensions (cross-system join required)
  - question: "What is revenue by cost center for the Cloud division?"
    expected:
      can_answer: true
      concepts_found: [revenue]
      dimensions_used: [cost_center, division]
      filters_resolved: [{dimension: division, value: Cloud, resolved_to: [Cloud East, Cloud West]}]
      join_path_exists: true
      min_confidence: 0.5

  # Invalid dimension combination
  - question: "What is sprint velocity by profit center?"
    expected:
      can_answer: false
      reason_contains: "cannot be sliced"

  # Hierarchy drill-down
  - question: "Show me headcount for Engineering cost centers"
    expected:
      can_answer: true
      concepts_found: [headcount]
      dimensions_used: [cost_center]
      filters_resolved: [{dimension: cost_center, value: Engineering, resolved_to: [Cloud Engineering, Platform Engineering]}]

  # Management overlay resolution
  - question: "Revenue by board segment"
    expected:
      can_answer: true
      dimensions_used: [division]
      management_overlay_used: true

  # Multiple concepts
  - question: "Compare revenue and headcount by department"
    expected:
      can_answer: true
      concepts_found: [revenue, headcount]
      dimensions_used: [department]

  # SOR authority test
  - question: "Show me employees by department"
    expected:
      can_answer: true
      primary_system: workday
      provenance_contains: "authoritative"

  # Graceful degradation — AAM unavailable
  - question: "Revenue by cost center"
    setup: mock_aam_unavailable
    expected:
      can_answer: true
      warnings_contain: "cross-system"
      # Should still work via fallback, just lower confidence

  # Unknown concept
  - question: "What is the florbatz by region?"
    expected:
      can_answer: false
      reason_contains: "not recognized"
```

## Build Order

1. **Read NLQ's query flow.** Find where NLQ parses questions, calls DCL, and formats responses. Report back the file paths and function names. Don't change anything yet.

2. **Verify DCL graph builds on startup.** Start the DCL server, hit `GET /api/dcl/graph/stats`. Confirm the graph has nodes and edges from all sources (normalizer, ontology, sample contour, AAM if available). If the graph is empty or missing edge types, fix the data loading. Commit if fixes needed.

3. **Create integration test script.** File: `tests/test_e2e_integration.py` (or `scripts/test_e2e.py` if pytest isn't wired for integration tests). This script:
   - Starts or connects to a running DCL server
   - Sends each test query to `POST /api/dcl/resolve`
   - Validates the response against expected results
   - Reports: pass/fail per query, confidence scores, provenance chains, any warnings
   - Does NOT require NLQ to be running (tests DCL's resolve endpoint directly first)
   Commit.

4. **Add graph resolution path to NLQ.** In the NLQ codebase:
   - Add a DCL graph client that calls `POST /api/dcl/resolve`
   - Add `resolve_via_graph()` in NLQ's query service
   - Try graph resolution first. If `can_answer: true`, use it. If false or unavailable, fall back to existing flat query.
   - Format QueryResolution into NLQ's existing response structure (answer text, provenance, confidence)
   Commit.

5. **NLQ → DCL integration test.** Send natural language questions through NLQ's full pipeline (parse → resolve via graph → format). Verify:
   - NLQ correctly parses intent from natural language
   - NLQ calls DCL resolve with correct QueryIntent
   - NLQ formats the graph resolution into a user-readable answer
   - NLQ falls back to flat query when graph can't answer
   Commit.

6. **Report results.** Run all 10 test queries. For each, report:
   - Pass/fail
   - Confidence score
   - Provenance chain (which systems, which edges)
   - Any warnings
   - Response time

## Important Notes

- This is a **dev branch** task. Work on dev, not a feature branch.
- NLQ and DCL are separate repos. Step 4 happens in the NLQ repo. Steps 1-3 happen in DCLv2.
- The existing flat query path (`POST /api/dcl/query`) stays untouched. Graph resolution is additive.
- AAM may or may not be running. The test should work with AAM available (richer graph, higher confidence) and without (graceful degradation, lower confidence).
- Use the sample contour data for hierarchy/authority/overlay. Don't require the onboarding agent.
- If DCL's graph is empty because normalizer hasn't run, run a classification first (`POST /api/dcl/run`) to populate mappings, then rebuild the graph.

## What NOT to Change

- NLQ's intent parsing logic (it already works)
- DCL's existing flat query path (backward compatibility)
- DCL's normalizer pipeline
- AAM's semantic edges endpoint
- Any production deployment configs
