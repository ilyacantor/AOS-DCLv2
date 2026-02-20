"""
Tests for the semantic graph traversal engine.

Covers all 8 resolution steps:
1. Intent parsing (NLQ-side, not tested here)
2. Concept location — find fields classified as a concept
3. Dimension validity — check SLICEABLE_BY edges
4. Dimension source resolution — find authoritative system
5. Join path discovery — BFS across MAPS_TO edges
6. Filter resolution — hierarchy and management overlay expansion
7. Confidence scoring — product of edge confidences
8. Response assembly — provenance, data query hint, warnings

Also covers:
- Graceful degradation with incomplete graph
- Path caching (hit, TTL expiry, invalidation)
- GraphStats accuracy
- The spec example: "revenue by division for Cloud division"
"""

import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from backend.domain import SemanticEdge, Mapping
from backend.engine.graph_types import (
    QueryFilter,
    QueryIntent,
    QueryResolution,
)
from backend.engine.semantic_graph import SemanticGraph
from backend.engine.query_resolver import QueryResolver


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_mapping(**overrides) -> Mapping:
    defaults = {
        "id": "test_mapping",
        "source_field": "amount",
        "source_table": "SalesOrder",
        "source_system": "netsuite",
        "ontology_concept": "revenue",
        "confidence": 0.95,
        "method": "heuristic",
        "status": "ok",
    }
    defaults.update(overrides)
    defaults["id"] = (
        f"{defaults['source_system']}_{defaults['source_table']}"
        f"_{defaults['source_field']}_{defaults['ontology_concept']}"
    )
    return Mapping(**defaults)


def _make_edge(**overrides) -> SemanticEdge:
    defaults = {
        "source_system": "netsuite",
        "source_object": "Vendor",
        "source_field": "vendor_id",
        "target_system": "workday",
        "target_object": "Supplier",
        "target_field": "ext_id",
        "edge_type": "DIRECT_MAP",
        "confidence": 0.88,
        "fabric_plane": "ERP",
        "extraction_source": "workato",
        "transformation": None,
    }
    defaults.update(overrides)
    return SemanticEdge(**defaults)


def _build_test_graph() -> SemanticGraph:
    """Build a representative graph for testing."""
    g = SemanticGraph()
    g.load_from_ontology()
    g.load_from_contour_map()

    g.load_from_normalizer([
        _make_mapping(
            source_system="netsuite", source_table="SalesOrder",
            source_field="total", ontology_concept="revenue",
            confidence=0.95,
        ),
        _make_mapping(
            source_system="salesforce", source_table="Opportunity",
            source_field="Amount", ontology_concept="revenue",
            confidence=0.92,
        ),
        _make_mapping(
            source_system="netsuite", source_table="CostCenter",
            source_field="amount", ontology_concept="cost",
            confidence=0.90,
        ),
        _make_mapping(
            source_system="workday", source_table="Employee",
            source_field="department_id", ontology_concept="employee",
            confidence=0.88,
        ),
    ])

    g.load_from_aam([
        _make_edge(
            source_system="netsuite", source_object="Vendor",
            source_field="vendor_id",
            target_system="workday", target_object="Supplier",
            target_field="ext_id",
            confidence=0.88,
        ),
        _make_edge(
            source_system="salesforce", source_object="Account",
            source_field="external_id",
            target_system="netsuite", target_object="Customer",
            target_field="sf_account_id",
            confidence=0.92,
        ),
    ])

    return g


# ---------------------------------------------------------------------------
# Step 2: Concept location
# ---------------------------------------------------------------------------

def test_step2_concept_location():
    """Find all fields classified as 'revenue', ranked by confidence."""
    g = _build_test_graph()
    sources = g.find_concept_sources("revenue")

    assert len(sources) == 2
    assert sources[0].system == "netsuite"
    assert sources[0].confidence == 0.95
    assert sources[1].system == "salesforce"
    assert sources[1].confidence == 0.92
    print("  PASS: Step 2 — concept location ranks by confidence")


def test_step2_no_sources():
    """Concept with no classified fields returns empty list."""
    g = _build_test_graph()
    sources = g.find_concept_sources("nonexistent_concept")
    assert sources == []
    print("  PASS: Step 2 — missing concept returns empty")


# ---------------------------------------------------------------------------
# Step 3: Dimension validity
# ---------------------------------------------------------------------------

def test_step3_valid_pair():
    """Revenue is sliceable by division (per ontology pairings)."""
    g = _build_test_graph()
    assert g.check_dimension_validity("revenue", "division") is True
    print("  PASS: Step 3 — revenue × division is valid")


def test_step3_invalid_pair():
    """Revenue is NOT sliceable by department."""
    g = _build_test_graph()
    assert g.check_dimension_validity("revenue", "department") is False
    print("  PASS: Step 3 — revenue × department is invalid")


def test_step3_cost_valid():
    """Cost IS sliceable by cost_center."""
    g = _build_test_graph()
    assert g.check_dimension_validity("cost", "cost_center") is True
    print("  PASS: Step 3 — cost × cost_center is valid")


# ---------------------------------------------------------------------------
# Step 4: Dimension source resolution
# ---------------------------------------------------------------------------

def test_step4_authority():
    """Contour map says workday is authoritative for division."""
    g = _build_test_graph()
    auth = g.find_dimension_authority("division")

    assert auth is not None
    assert auth.system == "workday"
    assert auth.confidence == 0.90
    assert auth.source == "contour_map"
    print("  PASS: Step 4 — division authority is workday (0.90)")


def test_step4_no_authority():
    """Unknown dimension returns None."""
    g = _build_test_graph()
    auth = g.find_dimension_authority("nonexistent_dim")
    assert auth is None
    print("  PASS: Step 4 — unknown dimension returns None")


# ---------------------------------------------------------------------------
# Step 5: Join path discovery
# ---------------------------------------------------------------------------

def test_step5_same_system():
    """Same system = direct join, no hops needed."""
    g = _build_test_graph()
    path = g.find_join_path("netsuite", "netsuite")

    assert path is not None
    assert path.hops == []
    assert path.total_confidence == 1.0
    print("  PASS: Step 5 — same system is direct (no hops)")


def test_step5_cross_system():
    """Cross-system path via MAPS_TO edge."""
    g = _build_test_graph()
    path = g.find_join_path("netsuite", "workday")

    assert path is not None
    assert len(path.hops) == 1
    assert path.hops[0].from_system == "netsuite"
    assert path.hops[0].to_system == "workday"
    assert path.total_confidence == 0.88
    print("  PASS: Step 5 — netsuite→workday via AAM edge (0.88)")


def test_step5_multi_hop():
    """salesforce→workday requires two hops: sf→netsuite→workday."""
    g = _build_test_graph()
    path = g.find_join_path("salesforce", "workday")

    assert path is not None
    assert len(path.hops) == 2
    # Confidence = 0.92 * 0.88 = 0.8096
    assert abs(path.total_confidence - 0.92 * 0.88) < 0.001
    print(f"  PASS: Step 5 — salesforce→workday in 2 hops (conf={path.total_confidence:.4f})")


def test_step5_no_path():
    """No MAPS_TO path to an unconnected system."""
    g = _build_test_graph()
    path = g.find_join_path("netsuite", "oracle_cloud")
    assert path is None
    print("  PASS: Step 5 — no path to unconnected system")


# ---------------------------------------------------------------------------
# Step 6: Filter resolution
# ---------------------------------------------------------------------------

def test_step6_management_overlay():
    """'Cloud' resolves to ['Cloud East', 'Cloud West'] via REPORTS_AS."""
    g = _build_test_graph()
    rf = g.resolve_dimension_filter("division", "Cloud")

    assert rf.resolution_type == "management_overlay"
    assert set(rf.resolved_values) == {"Cloud East", "Cloud West"}
    print("  PASS: Step 6 — Cloud → [Cloud East, Cloud West] via overlay")


def test_step6_hierarchy():
    """'Engineering' cost center expands to children."""
    g = _build_test_graph()
    rf = g.resolve_dimension_filter("cost_center", "Engineering")

    assert rf.resolution_type == "hierarchy_expansion"
    assert "Cloud Engineering" in rf.resolved_values
    assert "Platform Engineering" in rf.resolved_values
    print("  PASS: Step 6 — Engineering → children via hierarchy")


def test_step6_exact():
    """'Cloud East' has no children/overlay, resolves as exact."""
    g = _build_test_graph()
    rf = g.resolve_dimension_filter("division", "Cloud East")

    assert rf.resolution_type == "exact"
    assert rf.resolved_values == ["Cloud East"]
    print("  PASS: Step 6 — Cloud East resolves as exact")


# ---------------------------------------------------------------------------
# Step 7: Confidence scoring
# ---------------------------------------------------------------------------

def test_step7_confidence():
    """Full path confidence = product of hops."""
    g = _build_test_graph()
    resolver = QueryResolver(g)

    intent = QueryIntent(
        concepts=["revenue"], dimensions=["division"],
    )
    res = resolver.resolve(intent)

    assert res.can_answer is True
    cb = res.confidence
    # revenue_source=0.95, division_sor=0.90, join_netsuite→workday=0.88
    expected = 0.95 * 0.90 * 0.88
    assert abs(cb.overall - expected) < 0.01, f"Expected ~{expected:.4f}, got {cb.overall}"
    assert "revenue_source" in cb.per_hop
    assert "division_sor" in cb.per_hop
    print(f"  PASS: Step 7 — confidence {cb.overall:.4f} ≈ {expected:.4f}")


def test_step7_weakest_link():
    """Weakest link identified correctly."""
    g = _build_test_graph()
    resolver = QueryResolver(g)

    intent = QueryIntent(concepts=["revenue"], dimensions=["division"])
    res = resolver.resolve(intent)

    cb = res.confidence
    # The AAM join edge (0.88) is the weakest
    assert cb.weakest_confidence <= 0.90
    print(f"  PASS: Step 7 — weakest link: {cb.weakest_link} ({cb.weakest_confidence})")


# ---------------------------------------------------------------------------
# Step 8: Response assembly
# ---------------------------------------------------------------------------

def test_step8_provenance():
    """Provenance string describes the full path."""
    g = _build_test_graph()
    resolver = QueryResolver(g)

    intent = QueryIntent(
        concepts=["revenue"], dimensions=["division"],
        filters=[QueryFilter(dimension="division", operator="equals", value="Cloud")],
    )
    res = resolver.resolve(intent)

    assert res.can_answer is True
    assert "revenue" in res.provenance
    assert "netsuite" in res.provenance
    assert "division" in res.provenance
    print(f"  PASS: Step 8 — provenance: {res.provenance}")


def test_step8_data_query_hint():
    """DataQueryHint includes primary system, tables, and filters."""
    g = _build_test_graph()
    resolver = QueryResolver(g)

    intent = QueryIntent(
        concepts=["revenue"], dimensions=["division"],
        filters=[QueryFilter(dimension="division", operator="equals", value="Cloud")],
    )
    res = resolver.resolve(intent)

    dq = res.data_query
    assert dq is not None
    assert dq.primary_system == "netsuite"
    assert "SalesOrder" in dq.tables
    assert len(dq.filters) == 1
    assert dq.filters[0]["dimension"] == "division"
    assert set(dq.filters[0]["values"]) == {"Cloud East", "Cloud West"}
    print("  PASS: Step 8 — data query hint correct")


def test_step8_answer_path():
    """Answer path includes CLASSIFIED_AS and SLICEABLE_BY edges."""
    g = _build_test_graph()
    resolver = QueryResolver(g)

    intent = QueryIntent(concepts=["revenue"], dimensions=["division"])
    res = resolver.resolve(intent)

    edge_types = {e.type for e in res.answer_path}
    assert "CLASSIFIED_AS" in edge_types
    assert "SLICEABLE_BY" in edge_types
    print(f"  PASS: Step 8 — answer path has {len(res.answer_path)} edges: {edge_types}")


# ---------------------------------------------------------------------------
# Spec example: "revenue by division for Cloud division"
# ---------------------------------------------------------------------------

def test_spec_example_revenue_by_division_for_cloud():
    """Full end-to-end test of the spec's example query."""
    g = _build_test_graph()
    resolver = QueryResolver(g)

    intent = QueryIntent(
        concepts=["revenue"],
        dimensions=["division"],
        filters=[QueryFilter(dimension="division", operator="equals", value="Cloud")],
    )
    res = resolver.resolve(intent)

    # Can answer
    assert res.can_answer is True

    # Concept sources found (netsuite=0.95, salesforce=0.92)
    assert len(res.concept_sources) >= 2
    assert res.concept_sources[0].system == "netsuite"

    # Division authority is workday
    assert "division" in res.dimension_authorities
    assert res.dimension_authorities["division"].system == "workday"

    # Join path exists (netsuite→workday)
    assert len(res.join_paths) >= 1

    # Filter resolved via management overlay
    assert len(res.resolved_filters) == 1
    rf = res.resolved_filters[0]
    assert rf.original_value == "Cloud"
    assert set(rf.resolved_values) == {"Cloud East", "Cloud West"}
    assert rf.resolution_type == "management_overlay"

    # Confidence is reasonable (> 0.5)
    assert res.confidence.overall > 0.5

    # Provenance present
    assert len(res.provenance) > 0

    # Data query hint present
    assert res.data_query is not None
    assert res.data_query.primary_system == "netsuite"

    print(f"  PASS: Spec example — revenue by division for Cloud")
    print(f"         confidence={res.confidence.overall:.4f}")
    print(f"         provenance: {res.provenance}")


# ---------------------------------------------------------------------------
# Graceful degradation
# ---------------------------------------------------------------------------

def test_graceful_no_sources():
    """Missing concept returns can_answer=False with reason."""
    g = _build_test_graph()
    resolver = QueryResolver(g)

    intent = QueryIntent(concepts=["nonexistent"])
    res = resolver.resolve(intent)

    assert res.can_answer is False
    assert "No sources found" in res.reason
    print("  PASS: Graceful — missing concept handled")


def test_graceful_invalid_dimension():
    """Invalid dimension pairing returns can_answer=False."""
    g = _build_test_graph()
    resolver = QueryResolver(g)

    intent = QueryIntent(concepts=["revenue"], dimensions=["department"])
    res = resolver.resolve(intent)

    assert res.can_answer is False
    assert "Invalid concept-dimension" in res.reason
    print("  PASS: Graceful — invalid dimension pairing handled")


def test_graceful_no_join_path():
    """Missing cross-system path produces a warning but can still answer."""
    g = SemanticGraph()
    g.load_from_ontology()
    # Load contour with authority but no AAM edges
    g.load_from_contour_map()
    g.load_from_normalizer([
        _make_mapping(
            source_system="custom_erp", source_table="GL",
            source_field="amount", ontology_concept="revenue",
            confidence=0.90,
        ),
    ])
    # No AAM edges → no path to workday (division SOR)

    resolver = QueryResolver(g)
    intent = QueryIntent(concepts=["revenue"], dimensions=["division"])
    res = resolver.resolve(intent)

    assert res.can_answer is True
    assert any("No data path" in w for w in res.warnings)
    print("  PASS: Graceful — no join path produces warning")


def test_graceful_empty_graph():
    """Completely empty graph returns can_answer=False."""
    g = SemanticGraph()
    resolver = QueryResolver(g)

    intent = QueryIntent(concepts=["revenue"])
    res = resolver.resolve(intent)

    assert res.can_answer is False
    print("  PASS: Graceful — empty graph returns can_answer=False")


# ---------------------------------------------------------------------------
# Path caching
# ---------------------------------------------------------------------------

def test_cache_hit():
    """Second call with same intent returns cached result."""
    g = _build_test_graph()
    resolver = QueryResolver(g, cache_ttl=60)

    intent = QueryIntent(concepts=["revenue"], dimensions=["division"])

    t0 = time.perf_counter()
    r1 = resolver.resolve(intent)
    t1 = time.perf_counter()

    t2 = time.perf_counter()
    r2 = resolver.resolve(intent)
    t3 = time.perf_counter()

    assert r1.provenance == r2.provenance
    assert len(resolver._path_cache) == 1
    print(f"  PASS: Cache — hit ({(t1-t0)*1000:.3f}ms vs {(t3-t2)*1000:.3f}ms)")


def test_cache_invalidation():
    """invalidate_cache() clears all entries."""
    g = _build_test_graph()
    resolver = QueryResolver(g)

    resolver.resolve(QueryIntent(concepts=["revenue"]))
    assert len(resolver._path_cache) == 1

    resolver.invalidate_cache()
    assert len(resolver._path_cache) == 0
    print("  PASS: Cache — invalidation clears entries")


def test_cache_ttl_expiry():
    """Entries expire after TTL."""
    g = _build_test_graph()
    resolver = QueryResolver(g, cache_ttl=1)

    intent = QueryIntent(concepts=["revenue"])
    resolver.resolve(intent)

    time.sleep(1.1)
    # Next call should re-resolve (expired)
    resolver.resolve(intent)
    # Cache should have 1 fresh entry
    assert len(resolver._path_cache) == 1
    print("  PASS: Cache — TTL expiry works")


# ---------------------------------------------------------------------------
# GraphStats
# ---------------------------------------------------------------------------

def test_graph_stats():
    """Stats accurately reflect loaded graph."""
    g = _build_test_graph()
    s = g.stats

    # 107 ontology concepts from YAML + any extras from normalizer
    assert s.concept_nodes >= 107
    # 8 dimensions from pairings + extras from contour
    assert s.dimension_nodes >= 8
    # netsuite, salesforce, workday from mappings + SOR
    assert s.system_nodes >= 3
    # 4 mappings → 4 field nodes + 4 AAM edge endpoints
    assert s.field_nodes >= 4
    # Contour has dimension values
    assert s.dimension_value_nodes > 0

    assert "CLASSIFIED_AS" in s.edges_by_type
    assert "SLICEABLE_BY" in s.edges_by_type
    assert "MAPS_TO" in s.edges_by_type
    assert "HIERARCHY_PARENT" in s.edges_by_type
    assert "AUTHORITATIVE_FOR" in s.edges_by_type
    assert "REPORTS_AS" in s.edges_by_type

    assert s.connected_systems >= 2
    print(f"  PASS: GraphStats — {s.concept_nodes} concepts, "
          f"{s.field_nodes} fields, {s.system_nodes} systems, "
          f"{s.dimension_value_nodes} dim values")


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    tests = [
        # Step 2
        test_step2_concept_location,
        test_step2_no_sources,
        # Step 3
        test_step3_valid_pair,
        test_step3_invalid_pair,
        test_step3_cost_valid,
        # Step 4
        test_step4_authority,
        test_step4_no_authority,
        # Step 5
        test_step5_same_system,
        test_step5_cross_system,
        test_step5_multi_hop,
        test_step5_no_path,
        # Step 6
        test_step6_management_overlay,
        test_step6_hierarchy,
        test_step6_exact,
        # Step 7
        test_step7_confidence,
        test_step7_weakest_link,
        # Step 8
        test_step8_provenance,
        test_step8_data_query_hint,
        test_step8_answer_path,
        # Spec example
        test_spec_example_revenue_by_division_for_cloud,
        # Graceful degradation
        test_graceful_no_sources,
        test_graceful_invalid_dimension,
        test_graceful_no_join_path,
        test_graceful_empty_graph,
        # Cache
        test_cache_hit,
        test_cache_invalidation,
        test_cache_ttl_expiry,
        # Stats
        test_graph_stats,
    ]

    passed = 0
    failed = 0

    print(f"\n{'='*60}")
    print("DCL Semantic Graph Traversal Tests")
    print(f"{'='*60}\n")

    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"  FAIL: {test.__name__} — {e}")
            import traceback
            traceback.print_exc()
            failed += 1

    print(f"\n{'='*60}")
    print(f"Results: {passed} passed, {failed} failed out of {len(tests)}")
    print(f"{'='*60}")

    if failed > 0:
        sys.exit(1)
