"""
Tests for Tier 0: AAM semantic edge integration.

Covers:
1. AAM edge hit → field classified via edge, Tier 1-3 skipped
2. AAM edge below confidence threshold → falls through to Tier 1
3. No AAM edge → Tier 1 handles it (existing behavior unchanged)
4. AAM unavailable → empty EdgeIndex, all fields fall through
5. AAM edge + heuristic disagree → Tier 0 wins
6. Transformed edge → classified with provenance noting transformation
7. Edge-to-concept mapping resolves via aliases and example_fields
8. EdgeIndex coverage stats
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from backend.domain import SemanticEdge, SourceSystem, TableSchema, FieldSchema
from backend.engine.edge_index import EdgeIndex
from backend.semantic_mapper.heuristic_mapper import HeuristicMapper


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_edge(**overrides) -> SemanticEdge:
    defaults = {
        "source_system": "salesforce",
        "source_object": "Opportunity",
        "source_field": "Amount",
        "target_system": "netsuite",
        "target_object": "SalesOrder",
        "target_field": "total",
        "edge_type": "DIRECT_MAP",
        "confidence": 0.95,
        "fabric_plane": "IPAAS",
        "extraction_source": "workato_recipe_4782",
        "transformation": None,
    }
    defaults.update(overrides)
    return SemanticEdge(**defaults)


def _make_source(system_id: str, tables: list) -> SourceSystem:
    return SourceSystem(id=system_id, name=system_id, type="crm", tables=tables)


def _make_table(name: str, fields: list) -> TableSchema:
    return TableSchema(
        id=f"tbl_{name}",
        system_id="test",
        name=name,
        fields=[FieldSchema(name=f, type="string") for f in fields],
    )


ONTOLOGY_CONCEPTS = [
    {
        "id": "revenue",
        "name": "Revenue",
        "description": "Revenue metrics",
        "domain": "finance",
        "cluster": "Finance",
        "example_fields": ["Amount", "total_revenue", "revenue"],
        "aliases": ["sales_amount", "total"],
        "expected_type": "number",
        "typical_source_systems": ["salesforce", "netsuite"],
        "persona_relevance": {"CFO": 1.0},
    },
    {
        "id": "account",
        "name": "Account",
        "description": "Customer account",
        "domain": "crm",
        "cluster": "Growth",
        "example_fields": ["account_id", "AccountName"],
        "aliases": ["customer"],
        "expected_type": "string",
        "typical_source_systems": ["salesforce"],
        "persona_relevance": {"CRO": 1.0},
    },
    {
        "id": "cost",
        "name": "Cost",
        "description": "Cost metrics",
        "domain": "finance",
        "cluster": "Finance",
        "example_fields": ["cost", "total_cost", "expense"],
        "aliases": ["spend"],
        "expected_type": "number",
        "typical_source_systems": ["netsuite"],
        "persona_relevance": {"CFO": 1.0},
    },
]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_tier0_hit_high_confidence():
    """Scenario 1: AAM has edge at 0.95 → Tier 0 returns result, Tier 1 skipped."""
    edge = _make_edge(confidence=0.95)
    index = EdgeIndex([edge])
    mapper = HeuristicMapper(ONTOLOGY_CONCEPTS, edge_index=index)

    source = _make_source("salesforce", [_make_table("Opportunity", ["Amount"])])
    mappings = mapper.create_mappings([source])

    assert len(mappings) == 1
    m = mappings[0]
    assert m.method == "aam_edge"
    assert m.confidence == 0.95
    assert m.ontology_concept == "revenue"  # "Amount" → revenue via example_fields
    assert m.provenance is not None
    assert "IPAAS" in m.provenance
    assert m.cross_system_mapping is not None
    assert m.cross_system_mapping["maps_to_system"] == "netsuite"
    assert mapper.aam_edge_hits == 1
    assert mapper.aam_edge_misses == 0
    print("  PASS: Tier 0 hit, high confidence")


def test_tier0_low_confidence_falls_through():
    """Scenario 2: AAM edge at 0.65 → Tier 0 skips, falls through to Tier 1."""
    edge = _make_edge(confidence=0.65)
    index = EdgeIndex([edge])
    mapper = HeuristicMapper(ONTOLOGY_CONCEPTS, edge_index=index)

    source = _make_source("salesforce", [_make_table("Opportunity", ["Amount"])])
    mappings = mapper.create_mappings([source])

    assert len(mappings) == 1
    m = mappings[0]
    assert m.method == "heuristic"  # fell through to Tier 1
    assert mapper.aam_edge_hits == 0
    assert mapper.aam_edge_misses == 1
    print("  PASS: Low confidence edge falls through to Tier 1")


def test_tier0_no_edge_miss():
    """Scenario 3: No AAM edge for field → Tier 1 handles it."""
    # Edge exists for a DIFFERENT field
    edge = _make_edge(source_field="CloseDate")
    index = EdgeIndex([edge])
    mapper = HeuristicMapper(ONTOLOGY_CONCEPTS, edge_index=index)

    source = _make_source("salesforce", [_make_table("Opportunity", ["Amount"])])
    mappings = mapper.create_mappings([source])

    assert len(mappings) == 1
    m = mappings[0]
    assert m.method == "heuristic"
    assert mapper.aam_edge_misses == 1
    print("  PASS: No edge → Tier 1 miss, heuristic handles it")


def test_tier0_empty_index():
    """Scenario 4: AAM unavailable → empty EdgeIndex, all fields fall through."""
    index = EdgeIndex([])
    mapper = HeuristicMapper(ONTOLOGY_CONCEPTS, edge_index=index)

    source = _make_source("salesforce", [_make_table("Opportunity", ["Amount"])])
    mappings = mapper.create_mappings([source])

    assert len(mappings) == 1
    assert mappings[0].method == "heuristic"
    # With empty index, _try_aam_edge returns immediately, counts as miss
    assert mapper.aam_edge_misses == 1
    print("  PASS: Empty EdgeIndex → all fields fall through")


def test_tier0_wins_over_heuristic():
    """Scenario 5: AAM says revenue, heuristic would also match → Tier 0 wins."""
    edge = _make_edge(
        source_field="Amount",
        target_field="total",
        confidence=0.95,
    )
    index = EdgeIndex([edge])
    mapper = HeuristicMapper(ONTOLOGY_CONCEPTS, edge_index=index)

    source = _make_source("salesforce", [_make_table("Opportunity", ["Amount"])])
    mappings = mapper.create_mappings([source])

    assert len(mappings) == 1
    m = mappings[0]
    assert m.method == "aam_edge"  # Tier 0 wins, heuristic never runs
    assert m.confidence == 0.95
    print("  PASS: Tier 0 wins over heuristic")


def test_tier0_transformed_edge():
    """Scenario 6: Edge has transformation → classified with transformation in provenance."""
    edge = _make_edge(
        source_field="FirstName",
        target_field="full_name",
        edge_type="TRANSFORMED",
        transformation="CONCAT(FirstName, LastName)",
    )
    index = EdgeIndex([edge])
    mapper = HeuristicMapper(ONTOLOGY_CONCEPTS, edge_index=index)

    source = _make_source("salesforce", [_make_table("Contact", ["FirstName"])])
    mappings = mapper.create_mappings([source])

    assert len(mappings) == 1
    m = mappings[0]
    assert m.method == "aam_edge"
    assert m.cross_system_mapping["transformation"] == "CONCAT(FirstName, LastName)"
    assert m.cross_system_mapping["edge_type"] == "TRANSFORMED"
    print("  PASS: Transformed edge carries transformation metadata")


def test_edge_to_concept_alias_resolution():
    """Scenario 7: Edge field name matches an alias → resolves to correct concept."""
    edge = _make_edge(
        source_field="sales_amount",  # alias of 'revenue'
        target_field="total_revenue",
        confidence=0.92,
    )
    index = EdgeIndex([edge])
    mapper = HeuristicMapper(ONTOLOGY_CONCEPTS, edge_index=index)

    source = _make_source("salesforce", [_make_table("Deals", ["sales_amount"])])
    mappings = mapper.create_mappings([source])

    assert len(mappings) == 1
    assert mappings[0].ontology_concept == "revenue"
    print("  PASS: Edge field alias resolves to correct concept")


def test_edge_index_coverage():
    """Scenario 8: Coverage stats report correctly."""
    edges = [
        _make_edge(confidence=0.95, fabric_plane="IPAAS"),
        _make_edge(confidence=0.85, fabric_plane="IPAAS", source_field="CloseDate"),
        _make_edge(confidence=0.75, fabric_plane="EVENT_BUS", source_field="Status"),
    ]
    index = EdgeIndex(edges)
    cov = index.coverage

    assert cov["total_edges"] == 3
    assert cov["by_plane"]["IPAAS"] == 2
    assert cov["by_plane"]["EVENT_BUS"] == 1
    assert cov["by_confidence"]["high_90"] == 1
    assert cov["by_confidence"]["mid_80"] == 1
    assert cov["by_confidence"]["low"] == 1
    print("  PASS: EdgeIndex coverage stats correct")


def test_no_edge_index_backward_compatible():
    """HeuristicMapper works identically without edge_index (backward compat)."""
    mapper = HeuristicMapper(ONTOLOGY_CONCEPTS)  # no edge_index arg

    source = _make_source("salesforce", [_make_table("Opportunity", ["Amount"])])
    mappings = mapper.create_mappings([source])

    assert len(mappings) == 1
    assert mappings[0].method == "heuristic"
    print("  PASS: No edge_index → backward compatible")


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    tests = [
        test_tier0_hit_high_confidence,
        test_tier0_low_confidence_falls_through,
        test_tier0_no_edge_miss,
        test_tier0_empty_index,
        test_tier0_wins_over_heuristic,
        test_tier0_transformed_edge,
        test_edge_to_concept_alias_resolution,
        test_edge_index_coverage,
        test_no_edge_index_backward_compatible,
    ]

    print(f"\nRunning {len(tests)} Tier 0 AAM edge tests...\n")
    passed = 0
    failed = 0

    for test_fn in tests:
        try:
            test_fn()
            passed += 1
        except Exception as e:
            print(f"  FAIL: {test_fn.__name__}: {e}")
            failed += 1

    print(f"\nResults: {passed} passed, {failed} failed out of {len(tests)}")
    if failed > 0:
        sys.exit(1)
    print("All tests passed.")
