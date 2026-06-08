"""Raw-ledger records -> concept triples (records-path SE cutover, full-depth/raw-ledger half).

The financial STATEMENTS and operational KPIs are fixed catalogs (a hand-authored field->concept
map per domain). The raw ledgers — general ledger, chart of accounts, journal entries, invoices,
AP/AR aging, EBITDA adjustments, and the observability/ops detail (aws_cost, datadog, jira,
event_stream, operations, efficiency, pipeline stages) — are per-RECORD detail with thousands of
distinct concepts (gl alone is ~3.4k). There is no fixed catalog to author; the concept IS the
ledger key (e.g. gl.<account>). So this classifier COMPOSES the canonical concept from the
record's structural fields: concept = "<root>.<key>" (root = the ledger domain, key = the account/
line path). That is the genuine records-path shape for detail data — a source system exports its
ledger keys and DCL forms the concept — and it keeps DCL the concept-former (via ingest-records),
not Farm pre-forming triples.

domain == "ledger" pipe shape: records = [ {"root": "gl", "key": "<account>", "property": "...",
"value": ..., "period": "..."}, ... ]. Values are preserved as-is (ledgers carry numbers AND
strings like _meta markers / account names). Unknown/empty roots warn loud (A1).
"""
from __future__ import annotations

from typing import Any, Optional

from backend.api.routes.ingest_triples import TriplePayload

_CONF = 0.95
_TIER = "exact"
_STRUCTURAL = frozenset({"root", "key", "property", "value", "period"})


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool, list, dict)):
        return value
    return str(value)


def aggregate_ledger_records(
    *, entity_id: str, pipe: dict, records: list[dict], warnings: list[dict],
) -> list[TriplePayload]:
    """Compose canonical concepts from raw-ledger records (concept = root.key)."""
    source_system = pipe.get("source_system")
    fabric_plane = pipe.get("fabric_plane")
    fabric_product = pipe.get("fabric_product")
    pipe_id = str(pipe.get("pipe_id"))
    raw_source = pipe.get("source_system")

    payloads: list[TriplePayload] = []
    for rec_idx, rec in enumerate(records):
        root = rec.get("root")
        prop = rec.get("property")
        if not root or not prop:
            warnings.append({
                "type": "ledger_record_missing_root_or_property", "pipe_id": pipe_id,
                "record_index": rec_idx, "root": root, "property": prop,
                "detail": "ledger record needs both root and property to form a concept; skipped",
            })
            continue
        key = rec.get("key")
        concept = f"{root}.{key}" if key else str(root)
        period = rec.get("period")
        period = str(period) if period is not None and str(period).strip() else None
        payloads.append(TriplePayload(
            entity_id=entity_id, concept=concept, property=str(prop),
            value=_json_safe(rec.get("value")), period=period, currency=None, unit=None,
            source_system=source_system, source_table=f"fabric_via:{raw_source}",
            source_field=str(key) if key else str(root), pipe_id=pipe_id,
            confidence_score=_CONF, confidence_tier=_TIER,
            fabric_plane=fabric_plane, fabric_product=fabric_product,
        ))
    return payloads
