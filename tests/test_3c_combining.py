"""
Stage 3C Harness — Combining Financial Statements
Tests four-column combining with COFA adjustments and identity gates.
"""
import pytest
from backend.engine.combining_v2 import CombiningEngineV2

from tests.conftest import TENANT_ID, RUN_ID

# Ground truth
M_Q1_REV = 1323.43
C_Q1_REV = 269.38
M_Q1_COGS = 803.32
C_Q1_COGS = 189.91
M_Q1_OPEX = 198.52
C_Q1_OPEX = 111.38
M_Q1_EBITDA = 321.59
C_Q1_EBITDA = -31.91
M_Q1_NI = 224.29
C_Q1_NI = -39.99
M_Q1_ASSETS = 5664.22
C_Q1_ASSETS = 751.21
M_Q1_LIAB = 798.46
C_Q1_LIAB = 238.42
M_Q1_EQ = 4865.76
C_Q1_EQ = 512.79
M_Q1_CF_NET = 213.97
C_Q1_CF_NET = -50.33
COFA_COUNT = 6
COFA_REVENUE_GROSS_UP = -12.50


@pytest.fixture
def engine():
    return CombiningEngineV2(TENANT_ID, RUN_ID)


# --- Test 1: Entity A P&L values ---
def test_entity_a_pnl(engine):
    stmt = engine.get_combining_income_statement("2025-Q1")
    assert stmt["entity_a"]["revenue"]["total"] == M_Q1_REV
    assert stmt["entity_a"]["cogs"]["total"] == M_Q1_COGS
    assert stmt["entity_a"]["opex"]["total"] == M_Q1_OPEX
    assert stmt["entity_a"]["ebitda"] == M_Q1_EBITDA

# --- Test 2: Entity B P&L values ---
def test_entity_b_pnl(engine):
    stmt = engine.get_combining_income_statement("2025-Q1")
    assert stmt["entity_b"]["revenue"]["total"] == C_Q1_REV
    assert stmt["entity_b"]["cogs"]["total"] == C_Q1_COGS
    assert stmt["entity_b"]["opex"]["total"] == C_Q1_OPEX
    assert stmt["entity_b"]["ebitda"] == C_Q1_EBITDA

# --- Test 3: COFA adjustments present ---
def test_cofa_adjustments_count(engine):
    cofas = engine.get_cofa_adjustments()
    assert len(cofas) == COFA_COUNT

# --- Test 4: Revenue gross-up adjustment ---
def test_cofa_revenue_gross_up(engine):
    stmt = engine.get_combining_income_statement("2025-Q1")
    assert stmt["adjustments"]["revenue"]["cofa_revenue_gross_up"] == COFA_REVENUE_GROSS_UP

# --- Test 5: Combined revenue includes COFA ---
def test_combined_revenue_with_cofa(engine):
    stmt = engine.get_combining_income_statement("2025-Q1")
    expected = M_Q1_REV + C_Q1_REV + COFA_REVENUE_GROSS_UP
    assert stmt["combined"]["revenue"]["total"] == expected

# --- Test 6: P&L identity gate ---
def test_pnl_identity_gate(engine):
    stmt = engine.get_combining_income_statement("2025-Q1")
    assert stmt["identity_check"]["passed"] is True

# --- Test 7: BS identity ---
def test_bs_identity(engine):
    bs = engine.get_combining_balance_sheet("2025-Q1")
    combined = bs["combined"]
    assert combined["assets"]["total"] == combined["liabilities"]["total"] + combined["equity"]["total"]

# --- Test 8: BS entity values ---
def test_bs_entity_a(engine):
    bs = engine.get_combining_balance_sheet("2025-Q1")
    assert bs["entity_a"]["assets"]["total"] == M_Q1_ASSETS
    assert bs["entity_a"]["liabilities"]["total"] == M_Q1_LIAB
    assert bs["entity_a"]["equity"]["total"] == M_Q1_EQ

def test_bs_entity_b(engine):
    bs = engine.get_combining_balance_sheet("2025-Q1")
    assert bs["entity_b"]["assets"]["total"] == C_Q1_ASSETS
    assert bs["entity_b"]["liabilities"]["total"] == C_Q1_LIAB
    assert bs["entity_b"]["equity"]["total"] == C_Q1_EQ

# --- Test 9: CF identity ---
def test_cf_identity(engine):
    cf = engine.get_combining_cash_flow("2025-Q1")
    combined = cf["combined"]
    assert combined["operating"]["total"] + combined["investing"]["total"] + combined["financing"]["total"] == combined["net_change"]

# --- Test 10: CF entity values ---
def test_cf_net_meridian(engine):
    cf = engine.get_combining_cash_flow("2025-Q1")
    assert cf["entity_a"]["net_change"] == M_Q1_CF_NET

def test_cf_net_cascadia(engine):
    cf = engine.get_combining_cash_flow("2025-Q1")
    assert cf["entity_b"]["net_change"] == C_Q1_CF_NET

# --- Test 11: Multiple periods work ---
def test_q1_2024(engine):
    stmt = engine.get_combining_income_statement("2024-Q1")
    assert stmt["entity_a"]["revenue"]["total"] == 1250.00
    assert stmt["entity_b"]["revenue"]["total"] == 250.00

# --- Test 12: COFA details have required fields ---
def test_cofa_structure(engine):
    cofas = engine.get_cofa_adjustments()
    for cofa in cofas:
        assert "conflict_id" in cofa
        assert "concept" in cofa
        assert "description" in cofa
        assert "adjustment_amount" in cofa

# --- Test 13: Bad period raises ---
def test_bad_period_raises(engine):
    with pytest.raises(ValueError):
        engine.get_combining_income_statement("2099-Q1")

# --- Test 14: All 12 periods produce valid statements ---
def test_all_periods_valid(engine):
    periods = ["2024-Q1","2024-Q2","2024-Q3","2024-Q4",
               "2025-Q1","2025-Q2","2025-Q3","2025-Q4",
               "2026-Q1","2026-Q2","2026-Q3","2026-Q4"]
    for p in periods:
        stmt = engine.get_combining_income_statement(p)
        assert stmt["identity_check"]["passed"] is True, f"P&L identity failed for {p}"
