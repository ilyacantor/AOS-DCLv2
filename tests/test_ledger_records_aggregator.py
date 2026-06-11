"""Unit regression for the records-path raw-ledger classifier (per-family field catalogs).

The raw ledgers arrive as SOURCE ROWS tagged with their source table (record_type) and
carrying genuine export columns — never concept fragments. DCL owns the classification:
per-record_type catalogs map source columns to (concept, property); per-key families
compose the concept from the row's source identifiers (gl/coa account_number, QoE
category+stage). This proves the composition per family, string-value preservation,
structural-context columns producing no triples, and the loud-on-malformed behaviour.
"""
from backend.resolver.ledger_records_aggregator import aggregate_ledger_records


def _pipe():
    return {"pipe_id": "led-1", "source_system": "netsuite",
            "fabric_plane": "ledger", "fabric_product": "netsuite", "domain": "ledger"}


def _run(records):
    warnings: list = []
    out = aggregate_ledger_records(
        entity_id="LedTest-AAAA", pipe=_pipe(), records=records, warnings=warnings,
    )
    return out, warnings


def test_gl_balance_composes_concept_from_account_number():
    records = [{
        "record_type": "gl_balance", "period": "2023-12", "account_number": "1100",
        "account_name": "Cash", "debit": 0.0, "credit": 12.5,
        "ending_balance": 88.25, "department": "corporate", "transaction_count": 13,
    }]
    out, warnings = _run(records)
    assert warnings == []
    by_prop = {p.property: p for p in out}
    # account_number forms the concept; the five measure columns classify.
    assert set(by_prop) == {"debit", "credit", "ending_balance", "department", "transaction_count"}
    assert all(p.concept == "gl.1100" for p in out)
    assert by_prop["credit"].value == 12.5 and by_prop["credit"].period == "2023-12"
    assert by_prop["department"].value == "corporate"      # string value preserved
    assert by_prop["debit"].source_field == "debit"        # provenance = the source column
    assert by_prop["debit"].fabric_plane == "ledger"
    # account_name is account-identity context (CoA owns that property) — no triple.
    assert "account_name" not in by_prop


def test_coa_account_maps_master_columns_to_canonical_properties():
    records = [{
        "record_type": "coa_account", "account_number": "4000",
        "account_name": "Subscription Revenue", "account_type": "revenue",
        "parent_account": "revenue", "hierarchy_level": 2,
        "statement_mapping": "revenue.total", "description": "Recurring revenue",
        "recognition_method": "gross",
    }]
    out, warnings = _run(records)
    assert warnings == []
    assert all(p.concept == "coa.4000" and p.period is None for p in out)
    by_prop = {p.property: p.value for p in out}
    assert by_prop["account_name"] == "Subscription Revenue"
    assert by_prop["hierarchy_parent"] == "revenue"        # parent_account -> hierarchy_parent
    assert by_prop["maps_to_financial"] == "revenue.total"  # statement_mapping -> maps_to_financial
    assert by_prop["hierarchy_level"] == 2
    assert by_prop["recognition_method"] == "gross"


def test_summary_and_metric_families_classify_per_period():
    records = [
        {"record_type": "ap_aging", "period": "2024-Q1", "total_amount_due": 21.71,
         "aging_1_30": 4.78, "days_outstanding_avg": 44.2},
        {"record_type": "journal_entry_summary", "period": "2024-Q1", "entry_count": 310},
        {"record_type": "invoice_summary", "period": "2024-Q1", "invoice_count": 4400},
        {"record_type": "efficiency_metrics", "period": "2024-Q1", "rule_of_40": 41.5},
        {"record_type": "event_stream_metrics", "period": "2024-Q1",
         "message_throughput_msgs_sec": 1250.0},
    ]
    out, warnings = _run(records)
    assert warnings == []
    triples = {(p.concept, p.property): p.value for p in out}
    assert triples[("accounts_payable.summary", "total_amount_due")] == 21.71
    assert triples[("accounts_payable.summary", "amount_due_aging_1_30")] == 4.78  # aging_1_30 ->
    assert triples[("journal_entry.summary", "count")] == 310        # entry_count -> count
    assert triples[("invoice.summary", "count")] == 4400             # invoice_count -> count
    assert triples[("efficiency.rule_of_40", "score")] == 41.5
    assert triples[("event_stream.message_throughput", "rate")] == 1250.0
    assert all(p.period == "2024-Q1" for p in out)


def test_qoe_adjustment_composes_concept_from_category_and_stage():
    records = [{
        "record_type": "qoe_adjustment", "period": "2025-Q1",
        "adjustment_category": "owner_compensation", "lifecycle_stage": "initial_diligence",
        "amount_current": 1.41, "lever": "cost_reduction",
        "adjustment_name": "owner_compensation",
    }]
    out, warnings = _run(records)
    assert warnings == []
    assert all(
        p.concept == "ebitda_adjustment.owner_compensation.initial_diligence" for p in out
    )
    by_prop = {p.property: p.value for p in out}
    assert by_prop["amount_current"] == 1.41
    assert by_prop["lifecycle_stage"] == "initial_diligence"  # stage is concept key AND property
    assert by_prop["name"] == "owner_compensation"            # adjustment_name -> name


def test_namespace_declaration_mints_meta_marker():
    records = [{"record_type": "namespace_declaration",
                "namespace": "gl", "namespace_type": "financial_fact"}]
    out, warnings = _run(records)
    assert warnings == []
    assert len(out) == 1
    p = out[0]
    assert (p.concept, p.property, p.value, p.period) == \
        ("gl._meta", "namespace_type", "financial_fact", "_meta")


def test_malformed_records_warn_loud_never_silent():
    records = [
        {"period": "2024-Q1", "debit": 1.0},                              # no record_type
        {"record_type": "gl_balance", "period": "2024-01", "debit": 1.0},  # no account_number
        {"record_type": "ap_aging", "period": "2024-Q1", "bogus_column": 9.9},  # unmapped field
        {"record_type": "invoice_summary", "period": "2024-Q1", "invoice_count": None},  # null value
        {"record_type": "general_ledger_export", "period": "2024-01"},    # unknown family
    ]
    out, warnings = _run(records)
    assert out == []
    types = [w["type"] for w in warnings]
    assert types == [
        "ledger_record_missing_record_type",
        "ledger_record_missing_key_field",
        "unmapped_ledger_field",
        "null_ledger_value",
        "unknown_ledger_record_type",
    ]


def test_concept_fragments_are_not_a_record_shape():
    """Negative regression (farm #19): the retired {root, key, property, value} wire
    shape must NOT classify — pre-formed concept fragments warn loud as unknown."""
    records = [
        {"root": "gl", "key": "1100", "property": "debit", "value": 0.0, "period": "2023-12"},
    ]
    out, warnings = _run(records)
    assert out == []
    assert len(warnings) == 1
    assert warnings[0]["type"] == "ledger_record_missing_record_type"
