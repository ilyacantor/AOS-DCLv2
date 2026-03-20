"""
Stage 3A Harness — Query Resolver on Triples
Tests TripleQueryResolver against seed data in PG.
All expected values fetched from Farm's ground truth API at runtime (B10).
"""
import pytest
from backend.engine.query_resolver_v2 import TripleQueryResolver
from backend.engine.materialized_views import MaterializedViews

# === SEED CONSTANTS (from seed_manifest.json via conftest) ===
from tests.conftest import TENANT_ID, RUN_ID, gt_metric, gt_overlap_count


@pytest.fixture
def resolver():
    return TripleQueryResolver(TENANT_ID, RUN_ID)

@pytest.fixture
def views():
    return MaterializedViews(TENANT_ID, RUN_ID)


# --- Test 1: Single metric retrieval ---
def test_meridian_revenue_q1_2025(resolver):
    result = resolver.get_metric("revenue.total", "meridian", "2025-Q1")
    assert result["value"] == gt_metric("meridian", "2025-Q1", "revenue.total")

def test_cascadia_revenue_q1_2025(resolver):
    result = resolver.get_metric("revenue.total", "cascadia", "2025-Q1")
    assert result["value"] == gt_metric("cascadia", "2025-Q1", "revenue.total")

# --- Test 2: Timeseries retrieval ---
def test_meridian_revenue_timeseries(resolver):
    ts = resolver.get_metric_timeseries("revenue.total", "meridian")
    assert len(ts) == 12
    assert ts[0]["period"] == "2024-Q1"
    assert ts[0]["value"] == gt_metric("meridian", "2024-Q1", "revenue.total")
    assert ts[-1]["period"] == "2026-Q4"
    assert ts[-1]["value"] == gt_metric("meridian", "2026-Q4", "revenue.total")

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
    assert rev_total["value"] == gt_metric("meridian", "2025-Q1", "revenue.total")

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
    assert stmt["revenue"]["total"] == gt_metric("meridian", "2025-Q1", "revenue.total")
    assert stmt["cogs"]["total"] == gt_metric("meridian", "2025-Q1", "cogs.total")
    assert stmt["opex"]["total"] == gt_metric("meridian", "2025-Q1", "opex.total")
    assert stmt["ebitda"] == gt_metric("meridian", "2025-Q1", "pnl.ebitda")
    assert stmt["net_income"] == gt_metric("meridian", "2025-Q1", "pnl.net_income")

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
    assert bs["assets"]["total"] == gt_metric("meridian", "2025-Q1", "asset.total")
    assert bs["liabilities"]["total"] == gt_metric("meridian", "2025-Q1", "liability.total")
    assert bs["equity"]["total"] == gt_metric("meridian", "2025-Q1", "equity.total")

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
    assert cf["operating"]["total"] == gt_metric("meridian", "2025-Q1", "cash_flow.operating.total")
    assert cf["investing"]["total"] == gt_metric("meridian", "2025-Q1", "cash_flow.investing.total")
    assert cf["financing"]["total"] == gt_metric("meridian", "2025-Q1", "cash_flow.financing.total")
    assert cf["net_change"] == gt_metric("meridian", "2025-Q1", "cash_flow.net_change")

# --- Test 9: CF identity ---
def test_cf_identity_meridian(resolver):
    cf = resolver.get_cash_flow("meridian", "2025-Q1")
    assert round(cf["operating"]["total"] + cf["investing"]["total"] + cf["financing"]["total"], 2) == cf["net_change"]

# --- Test 10: Combining statement ---
def test_combining_income_statement(resolver):
    stmt = resolver.get_combining_statement("income_statement", "2025-Q1")
    assert stmt["entity_a"]["revenue"]["total"] == gt_metric("meridian", "2025-Q1", "revenue.total")
    assert stmt["entity_b"]["revenue"]["total"] == gt_metric("cascadia", "2025-Q1", "revenue.total")
    combined = round(
        gt_metric("meridian", "2025-Q1", "revenue.total")
        + gt_metric("cascadia", "2025-Q1", "revenue.total"), 2
    )
    assert stmt["combined"]["revenue"]["total"] == combined

# --- Test 11: Overlap retrieval ---
def test_customer_overlap(resolver):
    overlaps = resolver.get_overlapping_concepts("customer")
    assert len(overlaps) == gt_overlap_count("customer")

def test_vendor_overlap(resolver):
    overlaps = resolver.get_overlapping_concepts("vendor")
    assert len(overlaps) == gt_overlap_count("vendor")

def test_employee_overlap(resolver):
    overlaps = resolver.get_overlapping_concepts("employee")
    assert len(overlaps) == gt_overlap_count("employee")

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
    assert len(periods) == 13
    assert periods[0] == "2023-Q4"
    assert periods[-1] == "2026-Q4"

def test_all_entities(views):
    entities = views.get_all_entities()
    assert "meridian" in set(entities)
    assert "cascadia" in set(entities)

def test_entity_summary(views):
    summary = views.get_entity_summary("meridian")
    assert summary["total_triples"] > 0

# --- Test 15: Revenue sub-components ---
def test_meridian_consulting_revenue(resolver):
    result = resolver.get_metric("revenue.consulting", "meridian", "2025-Q1")
    assert result["value"] == gt_metric("meridian", "2025-Q1", "revenue.consulting")

def test_cascadia_managed_services_revenue(resolver):
    result = resolver.get_metric("revenue.managed_services", "cascadia", "2025-Q1")
    assert result["value"] == gt_metric("cascadia", "2025-Q1", "revenue.managed_services")
