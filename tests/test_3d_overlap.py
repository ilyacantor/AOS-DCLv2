"""
Stage 3D Harness — Overlap + Cross-sell
Tests overlap detection and cross-sell scoring from triples.
"""
import pytest
from backend.engine.overlap_v2 import OverlapEngineV2
from backend.engine.cross_sell_v2 import CrossSellEngineV2

from tests.conftest import TENANT_ID, RUN_ID

CUSTOMER_OVERLAP = 34
VENDOR_OVERLAP = 170
EMPLOYEE_OVERLAP = 10
MERIDIAN_CUSTOMERS = 1218
CASCADIA_CUSTOMERS = 220
MERIDIAN_ONLY_CUSTOMERS = 1184
CASCADIA_ONLY_CUSTOMERS = 186


@pytest.fixture
def overlap():
    return OverlapEngineV2(TENANT_ID, RUN_ID)

@pytest.fixture
def cross_sell():
    return CrossSellEngineV2(TENANT_ID, RUN_ID)


# --- Test 1: Overlap summary ---
def test_overlap_summary(overlap):
    summary = overlap.get_overlap_summary()
    assert summary["customer"]["overlap_count"] == CUSTOMER_OVERLAP
    assert summary["vendor"]["overlap_count"] == VENDOR_OVERLAP
    assert summary["employee"]["overlap_count"] == EMPLOYEE_OVERLAP

# --- Test 2: Entity totals ---
def test_entity_totals(overlap):
    summary = overlap.get_overlap_summary()
    assert summary["customer"]["entity_a_total"] == MERIDIAN_CUSTOMERS
    assert summary["customer"]["entity_b_total"] == CASCADIA_CUSTOMERS

# --- Test 3: Overlap percentages ---
def test_overlap_percentages(overlap):
    summary = overlap.get_overlap_summary()
    assert summary["vendor"]["overlap_pct_a"] == 100.0
    assert summary["vendor"]["overlap_pct_b"] == 55.19

# --- Test 4: Overlapping concept list ---
def test_customer_overlap_list(overlap):
    concepts = overlap.get_overlapping_concepts("customer")
    assert len(concepts) == CUSTOMER_OVERLAP
    names = [c["concept"] for c in concepts]
    assert "customer.accenture" in names

# --- Test 5: Vendor complete overlap ---
def test_vendor_complete_overlap(overlap):
    concepts = overlap.get_overlapping_concepts("vendor")
    assert len(concepts) == VENDOR_OVERLAP

# --- Test 6: Entity-only concepts ---
def test_meridian_only_customers(overlap):
    only = overlap.get_entity_only_concepts("customer", "meridian")
    assert len(only) == MERIDIAN_ONLY_CUSTOMERS

def test_cascadia_only_customers(overlap):
    only = overlap.get_entity_only_concepts("customer", "cascadia")
    assert len(only) == CASCADIA_ONLY_CUSTOMERS

# --- Test 7: Entity-only vendors ---
def test_vendor_only(overlap):
    only_m = overlap.get_entity_only_concepts("vendor", "meridian")
    only_c = overlap.get_entity_only_concepts("vendor", "cascadia")
    assert len(only_m) == 0
    assert len(only_c) == 138

# --- Test 8: Cross-sell opportunities exist ---
def test_cross_sell_has_opportunities(cross_sell):
    opps = cross_sell.get_cross_sell_opportunities()
    assert len(opps) > 0

# --- Test 9: Cross-sell summary ---
def test_cross_sell_summary(cross_sell):
    summary = cross_sell.get_cross_sell_summary()
    assert summary["total_opportunities"] > 0
    assert summary["total_potential_acv"] > 0
    assert "by_service" in summary
    assert "by_direction" in summary

# --- Test 10: Cross-sell has both directions ---
def test_cross_sell_bidirectional(cross_sell):
    summary = cross_sell.get_cross_sell_summary()
    # Services are complementary, so both directions should have opportunities
    assert summary["by_direction"]["a_to_b"] > 0
    assert summary["by_direction"]["b_to_a"] > 0

# --- Test 11: Overlap concepts have both entity properties ---
def test_overlap_has_both_entities(overlap):
    concepts = overlap.get_overlapping_concepts("customer")
    for c in concepts[:5]:  # spot check first 5
        assert "entity_a_properties" in c
        assert "entity_b_properties" in c

# --- Test 12: Employee overlap ---
def test_employee_overlap(overlap):
    concepts = overlap.get_overlapping_concepts("employee")
    assert len(concepts) == EMPLOYEE_OVERLAP
