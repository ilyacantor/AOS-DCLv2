"""Raw-ledger records -> concept triples (records-path SE cutover, full-depth/raw-ledger half).

Each ledger record is a SOURCE ROW tagged with its source table (record_type) and carrying
genuine export columns (account_number, aging_1_30, days_sales_outstanding, ...) — never
concept fragments. This classifier owns concept formation for the raw-ledger plane the way
financial_records_aggregator owns the statement catalog: a fixed per-record_type field
catalog maps source columns to (concept, property). Families that are per-key detail rather
than per-period summaries compose the concept key from the row's source identifiers — the
GL/CoA account number (concept gl.{account_number} / coa.{account_number}) and the QoE
adjustment category + lifecycle stage (concept ebitda_adjustment.{category}.{stage}) — the
same move the cloud_spend aggregator makes with by_service members.

domain == "ledger" pipe shape: records = [
  {"record_type": "gl_balance", "period": "2024-01", "account_number": "4000",
   "account_name": "Subscription Revenue", "debit": 0.0, "credit": 110.93,
   "ending_balance": 110.93, "transaction_count": 111},
  {"record_type": "ap_aging", "period": "2024-Q1", "total_amount_due": 21.71,
   "aging_current": 11.94, ...},
  {"record_type": "namespace_declaration", "namespace": "gl",
   "namespace_type": "financial_fact"},
  ...]

Values are preserved as-is (ledgers carry numbers AND strings like account names). Unknown
record_type, unmapped fields, missing key columns and null values all warn loud (A1) — never
silently dropped.
"""
from __future__ import annotations

from typing import Any, Optional

from backend.api.routes.ingest_triples import TriplePayload

_CONF = 0.95
_TIER = "exact"

# ── Per-key detail families ──────────────────────────────────────────────────
# GL trial-balance activity rows: one per account-month. account_number composes
# the concept; account_name is account-identity context (the CoA master already
# classifies it to the account_name property) — structural, no triple.
_GL_MEASURES = {
    "debit": "debit",
    "credit": "credit",
    "ending_balance": "ending_balance",
    "department": "department",
    "transaction_count": "transaction_count",
}
_GL_STRUCTURAL = frozenset({"record_type", "period", "account_number", "account_name"})

# CoA account-master rows: one per account, atemporal.
_COA_FIELDS = {
    "account_name": "account_name",
    "account_number": "account_number",
    "account_type": "account_type",
    "parent_account": "hierarchy_parent",
    "hierarchy_level": "hierarchy_level",
    "statement_mapping": "maps_to_financial",
    "description": "description",
    "department": "department",
    "recognition_method": "recognition_method",
    "cost_classification": "cost_classification",
    "capitalization_policy": "capitalization_policy",
    "depreciation_method": "depreciation_method",
}

# QoE workbook rows: one per adjustment x lifecycle stage. category + stage
# compose the concept; both also surface as properties where the canonical
# concept carries them (lifecycle_stage yes, category only via the concept).
_QOE_FIELDS = {
    "amount_current": "amount_current",
    "amount_low": "amount_low",
    "amount_high": "amount_high",
    "confidence": "confidence",
    "lifecycle_stage": "lifecycle_stage",
    "period_type": "period_type",
    "lever": "lever",
    "rationale": "rationale",
    "support_reference": "support_reference",
    "adjustment_name": "name",
}
_QOE_STRUCTURAL = frozenset({"record_type", "period", "adjustment_category"})

# ── Per-period summary families: record_type -> (fixed concept, field->property) ──
_SUMMARY_FAMILIES: dict[str, tuple[str, dict[str, str]]] = {
    "journal_entry_summary": ("journal_entry.summary", {
        "entry_count": "count",
        "total_debit_amount": "total_debit_amount",
        "total_credit_amount": "total_credit_amount",
        "avg_amount": "avg_amount",
        "distinct_debit_accounts": "distinct_debit_accounts",
        "distinct_credit_accounts": "distinct_credit_accounts",
        "days_to_post_avg": "days_to_post_avg",
    }),
    "invoice_summary": ("invoice.summary", {
        "invoice_count": "count",
        "total_amount": "total_amount",
        "avg_amount": "avg_amount",
        "line_items_avg": "line_items_avg",
        "due_days_avg": "due_days_avg",
        "distinct_vendors": "distinct_vendors",
    }),
    "ap_aging": ("accounts_payable.summary", {
        "total_amount_due": "total_amount_due",
        "open_invoice_count": "open_invoice_count",
        "distinct_vendors": "distinct_vendors",
        "days_outstanding_avg": "days_outstanding_avg",
        "aging_current": "amount_due_aging_current",
        "aging_1_30": "amount_due_aging_1_30",
        "aging_31_60": "amount_due_aging_31_60",
        "aging_61_90": "amount_due_aging_61_90",
        "aging_90_plus": "amount_due_aging_90_plus",
    }),
    "ar_aging": ("accounts_receivable.summary", {
        "total_amount_due": "total_amount_due",
        "open_invoice_count": "open_invoice_count",
        "distinct_customers": "distinct_customers",
        "days_sales_outstanding": "days_sales_outstanding",
        "aging_current": "amount_due_aging_current",
        "aging_1_30": "amount_due_aging_1_30",
        "aging_31_60": "amount_due_aging_31_60",
        "aging_61_90": "amount_due_aging_61_90",
        "aging_90_plus": "amount_due_aging_90_plus",
    }),
}

# ── Per-period metric-export families: record_type -> {field: (concept, property)} ──
_METRIC_FAMILIES: dict[str, dict[str, tuple[str, str]]] = {
    "efficiency_metrics": {
        "magic_number": ("efficiency.magic_number", "rate"),
        "burn_multiple": ("efficiency.burn_multiple", "rate"),
        "rule_of_40": ("efficiency.rule_of_40", "score"),
        "revenue_per_employee": ("efficiency.revenue_per_employee", "amount"),
        "arr_per_employee": ("efficiency.arr_per_employee", "amount"),
    },
    "ops_sla_metrics": {
        "automation_rate": ("operations.automation_rate", "rate"),
        "sla_attainment": ("operations.sla_attainment", "rate"),
    },
    "event_stream_metrics": {
        "topic_count": ("event_stream.topic_count", "count"),
        "partition_count": ("event_stream.partition_count", "count"),
        "message_throughput_msgs_sec": ("event_stream.message_throughput", "rate"),
        "consumer_lag_sec": ("event_stream.consumer_lag", "duration"),
        "event_volume_millions": ("event_stream.event_volume", "amount"),
    },
}

_PLAIN_STRUCTURAL = frozenset({"record_type", "period"})

LEDGER_RECORD_TYPES = frozenset(
    {"gl_balance", "coa_account", "qoe_adjustment", "namespace_declaration"}
    | set(_SUMMARY_FAMILIES) | set(_METRIC_FAMILIES)
)


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool, list, dict)):
        return value
    return str(value)


def aggregate_ledger_records(
    *, entity_id: str, pipe: dict, records: list[dict], warnings: list[dict],
) -> list[TriplePayload]:
    """Classify source-shaped raw-ledger records into canonical concept triples."""
    source_system = pipe.get("source_system")
    fabric_plane = pipe.get("fabric_plane")
    fabric_product = pipe.get("fabric_product")
    pipe_id = str(pipe.get("pipe_id"))
    raw_source = pipe.get("source_system")

    payloads: list[TriplePayload] = []

    def _warn(wtype: str, rec_idx: int, detail: str, **extra) -> None:
        warnings.append({
            "type": wtype, "pipe_id": pipe_id, "record_index": rec_idx,
            "detail": detail, **extra,
        })

    def _t(concept: str, prop: str, value: Any, period: Optional[str],
           source_field: str) -> TriplePayload:
        return TriplePayload(
            entity_id=entity_id, concept=concept, property=prop,
            value=_json_safe(value), period=period, currency=None, unit=None,
            source_system=source_system, source_table=f"fabric_via:{raw_source}",
            source_field=source_field, pipe_id=pipe_id,
            confidence_score=_CONF, confidence_tier=_TIER,
            fabric_plane=fabric_plane, fabric_product=fabric_product,
        )

    def _classify_fields(rec: dict, rec_idx: int, rt: str, concept: str,
                         field_props: dict[str, str], structural: frozenset,
                         period: Optional[str]) -> None:
        for fname, raw_value in rec.items():
            if fname in structural:
                continue
            prop = field_props.get(fname)
            if prop is None:
                _warn(
                    "unmapped_ledger_field", rec_idx,
                    f"ledger field '{fname}' has no property mapping for "
                    f"record_type '{rt}'; not converted", field=fname,
                )
                continue
            if raw_value is None:
                _warn(
                    "null_ledger_value", rec_idx,
                    f"ledger field '{fname}' is null for record_type '{rt}'; "
                    f"a source row never asserts a null fact — not converted",
                    field=fname,
                )
                continue
            payloads.append(_t(concept, prop, raw_value, period, fname))

    def _require(rec: dict, rec_idx: int, rt: str, field: str) -> Optional[str]:
        value = rec.get(field)
        if value is None or not str(value).strip():
            _warn(
                "ledger_record_missing_key_field", rec_idx,
                f"record_type '{rt}' needs '{field}' to form its concept; "
                f"record skipped", field=field,
            )
            return None
        return str(value)

    for rec_idx, rec in enumerate(records):
        rt = rec.get("record_type")
        if not rt:
            _warn(
                "ledger_record_missing_record_type", rec_idx,
                "ledger record carries no record_type (the source table tag); "
                "cannot classify — record skipped",
            )
            continue
        period = rec.get("period")
        period = str(period) if period is not None and str(period).strip() else None

        if rt == "gl_balance":
            account = _require(rec, rec_idx, rt, "account_number")
            if account is None:
                continue
            _classify_fields(
                rec, rec_idx, rt, f"gl.{account}", _GL_MEASURES,
                _GL_STRUCTURAL, period,
            )
        elif rt == "coa_account":
            account = _require(rec, rec_idx, rt, "account_number")
            if account is None:
                continue
            _classify_fields(
                rec, rec_idx, rt, f"coa.{account}", _COA_FIELDS,
                _PLAIN_STRUCTURAL, period,
            )
        elif rt == "qoe_adjustment":
            category = _require(rec, rec_idx, rt, "adjustment_category")
            stage = _require(rec, rec_idx, rt, "lifecycle_stage")
            if category is None or stage is None:
                continue
            _classify_fields(
                rec, rec_idx, rt, f"ebitda_adjustment.{category}.{stage}",
                _QOE_FIELDS, _QOE_STRUCTURAL, period,
            )
        elif rt in _SUMMARY_FAMILIES:
            concept, field_props = _SUMMARY_FAMILIES[rt]
            _classify_fields(rec, rec_idx, rt, concept, field_props,
                             _PLAIN_STRUCTURAL, period)
        elif rt in _METRIC_FAMILIES:
            catalog = _METRIC_FAMILIES[rt]
            for fname, raw_value in rec.items():
                if fname in _PLAIN_STRUCTURAL:
                    continue
                mapping = catalog.get(fname)
                if mapping is None:
                    _warn(
                        "unmapped_ledger_field", rec_idx,
                        f"ledger field '{fname}' has no concept mapping for "
                        f"record_type '{rt}'; not converted", field=fname,
                    )
                    continue
                if raw_value is None:
                    _warn(
                        "null_ledger_value", rec_idx,
                        f"ledger field '{fname}' is null for record_type "
                        f"'{rt}'; not converted", field=fname,
                    )
                    continue
                concept, prop = mapping
                payloads.append(_t(concept, prop, raw_value, period, fname))
        elif rt == "namespace_declaration":
            namespace = _require(rec, rec_idx, rt, "namespace")
            ns_type = _require(rec, rec_idx, rt, "namespace_type")
            if namespace is None or ns_type is None:
                continue
            # The source's catalog manifest row -> the {ns}._meta marker DCL's
            # domain queries key on. period is the _meta sentinel by protocol.
            payloads.append(_t(
                f"{namespace}._meta", "namespace_type", ns_type, "_meta",
                "namespace",
            ))
        else:
            _warn(
                "unknown_ledger_record_type", rec_idx,
                f"record_type '{rt}' is not a known raw-ledger family "
                f"({sorted(LEDGER_RECORD_TYPES)}); record skipped",
            )
    return payloads
