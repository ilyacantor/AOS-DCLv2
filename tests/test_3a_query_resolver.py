"""
Stage 3A Harness — Query Resolver on Triples
Tests TripleQueryResolver against seed data in PG.
All values are exact ground truth from the seed.
"""
import pytest
from backend.engine.query_resolver_v2 import TripleQueryResolver
from backend.engine.materialized_views import MaterializedViews

# === SEED CONSTANTS (from seed_manifest.json via conftest) ===
from tests.conftest import TENANT_ID, RUN_ID

# === GROUND TRUTH — exact values from PG ===
# Revenue
MERIDIAN_Q1_2025_REVENUE = 1323.43
CASCADIA_Q1_2025_REVENUE = 269.38
COMBINED_Q1_2025_REVENUE = 1592.81

MERIDIAN_Q1_2024_REVENUE = 1250.00
MERIDIAN_Q4_2026_REVENUE = 1446.53
CASCADIA_Q4_2026_REVENUE = 300.40

# P&L — Meridian 2025-Q1
MERIDIAN_Q1_2025_COGS = 803.32
MERIDIAN_Q1_2025_OPEX = 198.53
MERIDIAN_Q1_2025_EBITDA = 321.59
MERIDIAN_Q1_2025_NET_INCOME = 224.29
MERIDIAN_Q1_2025_OPERATING_PROFIT = 295.12
MERIDIAN_Q1_2025_DA = 26.47
MERIDIAN_Q1_2025_TAX = 70.83

# P&L — Cascadia 2025-Q1
CASCADIA_Q1_2025_COGS = 189.90
CASCADIA_Q1_2025_OPEX = 111.39
CASCADIA_Q1_2025_EBITDA = -31.91
CASCADIA_Q1_2025_NET_INCOME = -39.99

# BS — Meridian 2025-Q1
MERIDIAN_Q1_2025_ASSETS = 5552.52
MERIDIAN_Q1_2025_LIABILITIES = 789.24
MERIDIAN_Q1_2025_EQUITY = 4763.28
MERIDIAN_Q1_2025_CASH = 1298.27

# BS — Cascadia 2025-Q1
CASCADIA_Q1_2025_ASSETS = 652.25
CASCADIA_Q1_2025_LIABILITIES = 235.03
CASCADIA_Q1_2025_EQUITY = 417.22

# CF — Meridian 2025-Q1
MERIDIAN_Q1_2025_CF_OPERATING = 243.23
MERIDIAN_Q1_2025_CF_INVESTING = -19.85
MERIDIAN_Q1_2025_CF_FINANCING = -9.22
MERIDIAN_Q1_2025_CF_NET = 214.16

# CF — Cascadia 2025-Q1
CASCADIA_Q1_2025_CF_OPERATING = -32.42
CASCADIA_Q1_2025_CF_INVESTING = -14.43
CASCADIA_Q1_2025_CF_FINANCING = -3.39
CASCADIA_Q1_2025_CF_NET = -50.24

# Overlap counts
CUSTOMER_OVERLAP = 34
VENDOR_OVERLAP = 170
EMPLOYEE_OVERLAP = 10

# Revenue sub-components
MERIDIAN_Q1_2025_CONSULTING = 860.23
MERIDIAN_Q1_2025_FIXED_FEE = 463.20
CASCADIA_Q1_2025_MANAGED_SERVICES = 118.53
CASCADIA_Q1_2025_PER_FTE = 99.67
CASCADIA_Q1_2025_PER_TRANSACTION = 51.18


@pytest.fixture
def resolver():
    return TripleQueryResolver(TENANT_ID, RUN_ID)

@pytest.fixture
def views():
    return MaterializedViews(TENANT_ID, RUN_ID)


# --- Test 1: Single metric retrieval ---
def test_meridian_revenue_q1_2025(resolver):
    result = resolver.get_metric("revenue.total", "meridian", "2025-Q1")
    assert result["value"] == MERIDIAN_Q1_2025_REVENUE

def test_cascadia_revenue_q1_2025(resolver):
    result = resolver.get_metric("revenue.total", "cascadia", "2025-Q1")
    assert result["value"] == CASCADIA_Q1_2025_REVENUE

# --- Test 2: Timeseries retrieval ---
def test_meridian_revenue_timeseries(resolver):
    ts = resolver.get_metric_timeseries("revenue.total", "meridian")
    assert len(ts) == 12
    assert ts[0]["period"] == "2024-Q1"
    assert ts[0]["value"] == MERIDIAN_Q1_2024_REVENUE
    assert ts[-1]["period"] == "2026-Q4"
    assert ts[-1]["value"] == MERIDIAN_Q4_2026_REVENUE

# --- Test 3: Domain retrieval ---
def test_meridian_revenue_domain(resolver):
    items = resolver.get_domain("revenue", "meridian", "2025-Q1")
    concepts = {i["concept"] for i in items}
    assert "revenue.total" in concepts
    assert "revenue.consulting" in concepts
    assert "revenue.fixed_fee" in concepts
    # Meridian does NOT have managed_services
    assert "revenue.managed_services" not in concepts
    rev_total = next(i for i in items if i["concept"] == "revenue.total")
    assert rev_total["value"] == MERIDIAN_Q1_2025_REVENUE

def test_cascadia_revenue_domain(resolver):
    items = resolver.get_domain("revenue", "cascadia", "2025-Q1")
    concepts = {i["concept"] for i in items}
    assert "revenue.managed_services" in concepts
    assert "revenue.per_fte" in concepts
    assert "revenue.per_transaction" in concepts
    # Cascadia does NOT have consulting
    assert "revenue.consulting" not in concepts

# --- Test 4: Income statement ---
def test_meridian_income_statement(resolver):
    stmt = resolver.get_income_statement("meridian", "2025-Q1")
    assert stmt["revenue"]["total"] == MERIDIAN_Q1_2025_REVENUE
    assert stmt["cogs"]["total"] == MERIDIAN_Q1_2025_COGS
    assert stmt["opex"]["total"] == MERIDIAN_Q1_2025_OPEX
    assert stmt["ebitda"] == MERIDIAN_Q1_2025_EBITDA
    assert stmt["net_income"] == MERIDIAN_Q1_2025_NET_INCOME

# --- Test 5: P&L identity ---
# Financial values are 2-decimal-place; round() corrects IEEE 754 accumulation.
def test_pnl_identity_meridian(resolver):
    stmt = resolver.get_income_statement("meridian", "2025-Q1")
    calc_ebitda = round(stmt["revenue"]["total"] - stmt["cogs"]["total"] - stmt["opex"]["total"], 2)
    assert calc_ebitda == pytest.approx(stmt["ebitda"], abs=0.02)

def test_pnl_identity_cascadia(resolver):
    stmt = resolver.get_income_statement("cascadia", "2025-Q1")
    calc_ebitda = round(stmt["revenue"]["total"] - stmt["cogs"]["total"] - stmt["opex"]["total"], 2)
    assert calc_ebitda == stmt["ebitda"]

# --- Test 6: Balance sheet ---
def test_meridian_balance_sheet(resolver):
    bs = resolver.get_balance_sheet("meridian", "2025-Q1")
    assert bs["assets"]["total"] == MERIDIAN_Q1_2025_ASSETS
    assert bs["liabilities"]["total"] == MERIDIAN_Q1_2025_LIABILITIES
    assert bs["equity"]["total"] == MERIDIAN_Q1_2025_EQUITY

# --- Test 7: BS identity ---
def test_bs_identity_meridian(resolver):
    bs = resolver.get_balance_sheet("meridian", "2025-Q1")
    assert round(bs["assets"]["total"], 2) == round(bs["liabilities"]["total"] + bs["equity"]["total"], 2)

def test_bs_identity_cascadia(resolver):
    bs = resolver.get_balance_sheet("cascadia", "2025-Q1")
    assert round(bs["assets"]["total"], 2) == round(bs["liabilities"]["total"] + bs["equity"]["total"], 2)

# --- Test 8: Cash flow ---
def test_meridian_cash_flow(resolver):
    cf = resolver.get_cash_flow("meridian", "2025-Q1")
    assert cf["operating"]["total"] == MERIDIAN_Q1_2025_CF_OPERATING
    assert cf["investing"]["total"] == MERIDIAN_Q1_2025_CF_INVESTING
    assert cf["financing"]["total"] == MERIDIAN_Q1_2025_CF_FINANCING
    assert cf["net_change"] == MERIDIAN_Q1_2025_CF_NET

# --- Test 9: CF identity ---
def test_cf_identity_meridian(resolver):
    cf = resolver.get_cash_flow("meridian", "2025-Q1")
    assert round(cf["operating"]["total"] + cf["investing"]["total"] + cf["financing"]["total"], 2) == cf["net_change"]

# --- Test 10: Combining statement ---
def test_combining_income_statement(resolver):
    stmt = resolver.get_combining_statement("income_statement", "2025-Q1")
    assert stmt["entity_a"]["revenue"]["total"] == MERIDIAN_Q1_2025_REVENUE
    assert stmt["entity_b"]["revenue"]["total"] == CASCADIA_Q1_2025_REVENUE
    assert stmt["combined"]["revenue"]["total"] == COMBINED_Q1_2025_REVENUE

# --- Test 11: Overlap retrieval ---
def test_customer_overlap(resolver):
    overlaps = resolver.get_overlapping_concepts("customer")
    assert len(overlaps) == CUSTOMER_OVERLAP

def test_vendor_overlap(resolver):
    overlaps = resolver.get_overlapping_concepts("vendor")
    assert len(overlaps) == VENDOR_OVERLAP

def test_employee_overlap(resolver):
    overlaps = resolver.get_overlapping_concepts("employee")
    assert len(overlaps) == EMPLOYEE_OVERLAP

# --- Test 12: Error on missing data ---
def test_missing_concept_raises(resolver):
    with pytest.raises(ValueError, match="not found"):
        resolver.get_metric("revenue.nonexistent", "meridian", "2025-Q1")

def test_missing_entity_raises(resolver):
    with pytest.raises(ValueError, match="not found"):
        resolver.get_metric("revenue.total", "nonexistent_entity", "2025-Q1")

# --- Test 13: Provenance ---
def test_provenance_has_run_id(resolver):
    prov = resolver.get_provenance("revenue.total", "meridian", "2025-Q1")
    assert prov["run_id"] is not None, "Provenance must include a run_id"
    assert len(str(prov["run_id"])) == 36, f"run_id should be a UUID, got: {prov['run_id']}"

# --- Test 14: Materialized views ---
def test_all_periods(views):
    periods = views.get_all_periods()
    assert len(periods) == 24
    assert periods[0] == "2021-Q1"
    assert periods[-1] == "2026-Q4"

def test_all_entities(views):
    entities = views.get_all_entities()
    assert set(entities) == {"meridian", "cascadia", "BlueWave-RR3T", "x", "y"}

def test_entity_summary(views):
    summary = views.get_entity_summary("meridian")
    assert summary["total_triples"] > 0

# --- Test 15: Revenue sub-components ---
def test_meridian_consulting_revenue(resolver):
    result = resolver.get_metric("revenue.consulting", "meridian", "2025-Q1")
    assert result["value"] == MERIDIAN_Q1_2025_CONSULTING

def test_cascadia_managed_services_revenue(resolver):
    result = resolver.get_metric("revenue.managed_services", "cascadia", "2025-Q1")
    assert result["value"] == CASCADIA_Q1_2025_MANAGED_SERVICES
