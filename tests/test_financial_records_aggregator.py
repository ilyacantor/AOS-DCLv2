"""Unit regression for the records-path SE financial classifier (DCL-owned CoA map).

Labelled regression (not the B17 acceptance gate — that is the e2e dashboard test that
drives a records-path entity and verifies the CFO tiles render). This proves the
deterministic account->canonical-concept mapping in isolation: feed period-keyed
financial-statement records, assert the canonical concepts NLQ resolves come out with
the right values, periods, and provenance, and that a drifted field surfaces loudly.
"""
from backend.resolver.financial_records_aggregator import (
    aggregate_financial_records,
    FINANCIAL_FIELD_CONCEPTS,
)


def _pipe():
    return {
        "pipe_id": "fin-1", "source_system": "netsuite",
        "fabric_plane": "erp", "fabric_product": "netsuite", "domain": "financials",
    }


def test_maps_pnl_bs_cf_fields_to_canonical_concepts_with_period():
    records = [{
        "period": "2024-Q1",
        "revenue": 102.3, "cogs": 74.26, "net_income": 12.1, "operating_profit": 18.0,
        "cash": 318.0, "accounts_receivable": 55.5, "asset_total": 900.1,
        "accounts_payable": 40.5, "liability_total": 300.0,
        "equity_total": 600.1, "retained_earnings": 480.0,
        "cfo": 15.2, "capex": 4.0,
    }]
    warnings: list = []
    out = aggregate_financial_records(
        entity_id="FinTest-AAAA", pipe=_pipe(), records=records, warnings=warnings,
    )
    by_concept = {p.concept: p for p in out}

    # Each source field landed on exactly the concept nlq/config/metric_concept_map.yaml
    # resolves (so the CFO dashboard + its derived metrics light up), value preserved,
    # period stamped, property "amount", records-path provenance intact.
    expected = {
        "revenue.total": 102.3, "cogs.total": 74.26, "pnl.net_income": 12.1,
        "pnl.operating_profit": 18.0, "asset.current.cash": 318.0,
        "asset.current.accounts_receivable": 55.5, "asset.total": 900.1,
        "liability.current.accounts_payable": 40.5, "liability.total": 300.0,
        "equity.total": 600.1, "equity.retained_earnings": 480.0,
        "cash_flow.operating.total": 15.2, "cash_flow.investing.capex": 4.0,
    }
    for concept, value in expected.items():
        assert concept in by_concept, f"missing canonical concept {concept}"
        p = by_concept[concept]
        assert p.value == value, f"{concept} value {p.value} != {value}"
        assert p.property == "amount"
        assert p.period == "2024-Q1"
        assert p.fabric_plane == "erp"
        assert p.confidence_tier == "exact"

    # No spurious warnings when every field is mapped.
    assert warnings == []


def test_unmapped_field_surfaces_loud_warning_not_silent_drop():
    records = [{"period": "2024-Q1", "revenue": 100.0, "mystery_metric": 7}]
    warnings: list = []
    out = aggregate_financial_records(
        entity_id="FinTest-AAAA", pipe=_pipe(), records=records, warnings=warnings,
    )
    concepts = {p.concept for p in out}
    assert "revenue.total" in concepts            # mapped field converts
    assert all("mystery" not in c for c in concepts)  # unmapped does NOT fabricate a concept
    assert len(warnings) == 1
    assert warnings[0]["type"] == "unmapped_financial_field"
    assert warnings[0]["field"] == "mystery_metric"


def test_revenue_by_customer_expands_one_triple_per_customer():
    records = [{
        "period": "2024-Q1",
        "revenue": 100.0,
        "revenue_by_customer": {"Accenture": 0.12, "Deloitte": 0.34},
    }]
    warnings: list = []
    out = aggregate_financial_records(
        entity_id="FinTest-AAAA", pipe=_pipe(), records=records, warnings=warnings,
    )
    by_cust = {p.property: p.value for p in out if p.concept == "revenue.by_customer"}
    assert by_cust == {"Accenture": 0.12, "Deloitte": 0.34}
    assert warnings == []


def test_contract_covers_the_cfo_dashboard_concepts():
    # The map must produce every concept the CFO P&L/BS/CF tiles resolve directly.
    produced = set(FINANCIAL_FIELD_CONCEPTS.values())
    must_have = {
        "revenue.total", "cogs.total", "opex.total", "pnl.ebitda",
        "pnl.operating_profit", "pnl.net_income", "asset.total", "asset.current.cash",
        "liability.total", "liability.current.accounts_payable", "equity.total",
        "cash_flow.operating.total", "cash_flow.investing.capex",
    }
    missing = must_have - produced
    assert not missing, f"CoA contract missing CFO concepts: {sorted(missing)}"
