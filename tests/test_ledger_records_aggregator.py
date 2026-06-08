"""Unit regression for the records-path raw-ledger classifier (generic concept composition).

The raw ledgers (gl/coa/journal_entry/invoice/AP/AR + observability/ops) are per-record detail
with thousands of distinct concepts — no fixed catalog. DCL composes concept = root.key from the
record's structural fields. This proves that composition, string-value preservation, and the
loud-on-malformed behaviour.
"""
from backend.resolver.ledger_records_aggregator import aggregate_ledger_records


def _pipe():
    return {"pipe_id": "led-1", "source_system": "netsuite",
            "fabric_plane": "ledger", "fabric_product": "netsuite", "domain": "ledger"}


def test_composes_concept_from_root_and_key():
    records = [
        {"root": "gl", "key": "1100", "property": "debit", "value": 0.0, "period": "2023-12"},
        {"root": "invoice", "key": "INV-1", "property": "amount", "value": 12.5, "period": "2024-Q1"},
        {"root": "coa", "key": "4000", "property": "account_name", "value": "Revenue", "period": None},
        {"root": "journal_entry", "key": "JE-9.line.2", "property": "credit", "value": 3.3, "period": "2024-Q2"},
    ]
    warnings: list = []
    out = aggregate_ledger_records(entity_id="LedTest-AAAA", pipe=_pipe(), records=records, warnings=warnings)
    by_concept = {p.concept: p for p in out}
    assert by_concept["gl.1100"].property == "debit" and by_concept["gl.1100"].value == 0.0
    assert by_concept["gl.1100"].period == "2023-12" and by_concept["gl.1100"].fabric_plane == "ledger"
    assert by_concept["invoice.INV-1"].value == 12.5
    assert by_concept["coa.4000"].value == "Revenue"          # non-numeric value preserved
    assert by_concept["journal_entry.JE-9.line.2"].property == "credit"  # multi-segment key intact
    assert warnings == []


def test_no_key_composes_to_root_and_malformed_warns():
    records = [
        {"root": "datadog", "property": "uptime", "value": 0.999, "period": "2024-Q1"},  # no key -> concept=datadog
        {"key": "x", "property": "p", "value": 1.0},                                     # no root -> warn
        {"root": "gl", "value": 1.0},                                                    # no property -> warn
    ]
    warnings: list = []
    out = aggregate_ledger_records(entity_id="LedTest-AAAA", pipe=_pipe(), records=records, warnings=warnings)
    assert "datadog" in {p.concept for p in out}
    assert len(warnings) == 2
    assert all(w["type"] == "ledger_record_missing_root_or_property" for w in warnings)
