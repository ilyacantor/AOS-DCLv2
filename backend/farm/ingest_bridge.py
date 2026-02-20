"""
Farm Ingest Bridge — converts ingested Farm pipe data into DCL SourceSystem objects.

When Farm pushes 20 pipe payloads via POST /api/dcl/ingest, each payload
is buffered in the IngestStore. This bridge reads those buffered rows,
groups them by source system, infers schemas, and produces SourceSystem
objects that the DCL engine can feed into its standard mapping pipeline.

The 20 pipes map to 8 source systems:
  Salesforce (3 pipes), NetSuite (5 pipes), Chargebee (2 pipes),
  Workday (3 pipes), Zendesk (2 pipes), Jira (2 pipes),
  Datadog (2 pipes), AWS Cost (1 pipe).
"""

from typing import Dict, List, Any, Optional, Tuple
from backend.api.ingest import get_ingest_store, RunReceipt
from backend.domain import (
    SourceSystem, TableSchema, FieldSchema,
    DiscoveryStatus, ResolutionType,
)
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

# Pipe ID → (canonical_source_id, source_display_name, category, tier)
PIPE_SOURCE_MAP: Dict[str, Tuple[str, str, str, int]] = {
    "sf_users":              ("salesforce", "Salesforce", "crm", 1),
    "sf_accounts":           ("salesforce", "Salesforce", "crm", 1),
    "sf_opportunities":      ("salesforce", "Salesforce", "crm", 1),
    "ns-erp-001-invoices":   ("netsuite", "NetSuite", "erp", 1),
    "ns-erp-001-rev-schedules": ("netsuite", "NetSuite", "erp", 1),
    "ns-erp-001-gl-entries": ("netsuite", "NetSuite", "erp", 1),
    "ns-erp-001-ar":         ("netsuite", "NetSuite", "erp", 1),
    "ns-erp-001-ap":         ("netsuite", "NetSuite", "erp", 1),
    "cb_main_subscriptions": ("chargebee", "Chargebee", "billing", 1),
    "cb_main_invoices":      ("chargebee", "Chargebee", "billing", 1),
    "wd-workers-001":        ("workday", "Workday", "hr", 2),
    "wd-positions-001":      ("workday", "Workday", "hr", 2),
    "wd-timeoff-001":        ("workday", "Workday", "hr", 2),
    "zendesk_tickets":       ("zendesk", "Zendesk", "support", 2),
    "zendesk_organizations": ("zendesk", "Zendesk", "support", 2),
    "jira_issues":           ("jira", "Jira", "engineering", 3),
    "jira_sprints":          ("jira", "Jira", "engineering", 3),
    "datadog_incidents":     ("datadog", "Datadog", "monitoring", 3),
    "datadog_slos":          ("datadog", "Datadog", "monitoring", 3),
    "aws_cost_line_items":   ("aws_cost", "AWS Cost Explorer", "cloud", 3),
}

# Trust scores by tier: Tier 1 = financial SoR, Tier 2 = ops, Tier 3 = infra
TIER_TRUST = {1: 90, 2: 80, 3: 70}


def build_sources_from_ingest(
    farm_run_id: Optional[str] = None,
    dispatch_id: Optional[str] = None,
    narration=None,
    dcl_run_id: Optional[str] = None,
) -> List[SourceSystem]:
    """
    Read the IngestStore and build SourceSystem objects from Farm pipe data.

    Args:
        farm_run_id: Legacy — filter by single run_id.
        dispatch_id: If provided, use all receipts from this dispatch.
                     If None, auto-selects the latest dispatch.
        narration: Optional NarrationService for progress messages.
        dcl_run_id: Optional DCL run_id for narration.

    Returns:
        List of SourceSystem objects, one per canonical source.
    """
    store = get_ingest_store()
    receipts = store.get_all_receipts()

    if not receipts:
        if narration and dcl_run_id:
            narration.add_message(
                dcl_run_id, "FarmBridge",
                "No ingested data found — Farm has not pushed any pipes yet"
            )
        return []

    # Filter to a single Farm dispatch
    if dispatch_id:
        receipts = store.get_receipts_by_dispatch(dispatch_id)
        logger.info(
            f"[FarmBridge] Using dispatch_id={dispatch_id} "
            f"({len(receipts)} pipes)"
        )
    elif farm_run_id:
        receipts = [r for r in receipts if r.run_id == farm_run_id]
    else:
        # Auto-select latest dispatch (exclude AAM dispatches — those are
        # metadata-only pulls, not Farm content pushes)
        dispatches = store.get_dispatches()
        farm_dispatches = [
            d for d in dispatches
            if not d["dispatch_id"].startswith("aam_")
        ]
        if farm_dispatches:
            latest_dispatch = farm_dispatches[0]  # sorted by latest_received_at desc
            latest_did = latest_dispatch["dispatch_id"]
            receipts = store.get_receipts_by_dispatch(latest_did)
            logger.info(
                f"[FarmBridge] Auto-selected latest dispatch={latest_did} "
                f"({len(receipts)} pipes, {latest_dispatch['total_rows']:,} rows)"
            )
        else:
            latest = max(receipts, key=lambda r: r.received_at)
            receipts = [r for r in receipts if r.run_id == latest.run_id]
            logger.info(
                f"[FarmBridge] Fallback: latest run_id={latest.run_id} "
                f"({len(receipts)} pipes)"
            )

    if narration and dcl_run_id:
        narration.add_message(
            dcl_run_id, "FarmBridge",
            f"Processing {len(receipts)} ingested pipe receipts"
        )

    # Group receipts by source system (canonical)
    source_groups: Dict[str, List[RunReceipt]] = {}
    for receipt in receipts:
        pipe_info = PIPE_SOURCE_MAP.get(receipt.pipe_id)
        if pipe_info:
            canonical_id = pipe_info[0]
        else:
            # Fall back to source_system from the receipt
            canonical_id = receipt.canonical_source_id or receipt.source_system
        source_groups.setdefault(canonical_id, []).append(receipt)

    sources: List[SourceSystem] = []

    for canonical_id, group_receipts in sorted(source_groups.items()):
        # Determine source metadata from the first pipe's known mapping
        first_receipt = group_receipts[0]
        pipe_info = PIPE_SOURCE_MAP.get(first_receipt.pipe_id)

        if pipe_info:
            _, display_name, category, tier = pipe_info
        else:
            display_name = first_receipt.source_system
            category = "unknown"
            tier = 3

        trust_score = TIER_TRUST.get(tier, 70)

        # Build tables from each pipe's rows
        tables: List[TableSchema] = []
        total_records = 0

        for receipt in group_receipts:
            rows = store.get_rows(receipt.run_id, receipt.pipe_id)
            if not rows:
                # Rows may have been evicted — use receipt metadata only
                table = TableSchema(
                    id=f"{canonical_id}.{receipt.pipe_id}",
                    system_id=canonical_id,
                    name=receipt.pipe_id,
                    fields=_fields_from_receipt(receipt),
                    record_count=receipt.row_count,
                    stats={"pipe_id": receipt.pipe_id, "rows_buffered": 0},
                )
            else:
                from backend.engine.schema_loader import SchemaLoader
                table = SchemaLoader._infer_table_schema_from_json(
                    rows, canonical_id, receipt.pipe_id, len(rows)
                )
            tables.append(table)
            total_records += receipt.row_count

        source = SourceSystem(
            id=canonical_id,
            name=display_name,
            type=category.upper(),
            tags=["farm", "v2", f"tier_{tier}", category],
            tables=tables,
            canonical_id=canonical_id,
            raw_id=canonical_id,
            discovery_status=DiscoveryStatus.CANONICAL,
            resolution_type=ResolutionType.EXACT,
            trust_score=trust_score,
            data_quality_score=85,
            vendor=display_name,
            category=category,
            entities=[],
        )
        sources.append(source)

        if narration and dcl_run_id:
            total_fields = sum(len(t.fields) for t in tables)
            narration.add_message(
                dcl_run_id, "FarmBridge",
                f"  {display_name}: {len(tables)} pipes, "
                f"{total_fields} fields, {total_records:,} records (tier {tier})"
            )

    # Sort by tier (lower is higher priority)
    sources.sort(key=lambda s: (
        -s.trust_score,
        s.name,
    ))

    if narration and dcl_run_id:
        narration.add_message(
            dcl_run_id, "FarmBridge",
            f"Built {len(sources)} source systems from {len(receipts)} "
            f"ingested pipes ({sum(r.row_count for r in receipts):,} total records)"
        )

    return sources


def get_ingest_summary() -> Dict[str, Any]:
    """
    Return a summary of what's in the ingest store, suitable for API responses.
    """
    store = get_ingest_store()
    receipts = store.get_all_receipts()
    stats = store.get_stats()

    pipes_by_source: Dict[str, List[str]] = {}
    total_records = 0

    for receipt in receipts:
        pipe_info = PIPE_SOURCE_MAP.get(receipt.pipe_id)
        source_name = pipe_info[1] if pipe_info else receipt.source_system
        pipes_by_source.setdefault(source_name, []).append(receipt.pipe_id)
        total_records += receipt.row_count

    return {
        "pipe_count": len(receipts),
        "source_count": len(pipes_by_source),
        "total_records": total_records,
        "sources": {
            name: {"pipes": pipes, "pipe_count": len(pipes)}
            for name, pipes in sorted(pipes_by_source.items())
        },
        "store_stats": stats,
    }


def _fields_from_receipt(receipt: RunReceipt) -> List[FieldSchema]:
    """Build minimal FieldSchema list from schema registry when rows are evicted."""
    from backend.engine.schema_loader import SchemaLoader

    store = get_ingest_store()
    registry = store.get_schema_registry()
    schema_record = registry.get(receipt.pipe_id)
    if not schema_record:
        return []
    return [
        FieldSchema(
            name=field_name,
            type="string",
            semantic_hint=SchemaLoader._infer_semantic_hint_from_name(field_name),
            nullable=True,
        )
        for field_name in schema_record.field_names
        if not field_name.startswith("_")  # Skip internal tags (_run_id, etc.)
    ]
