"""
Provenance Trace Service - Traces any metric back to its source system, table, and field.

Provides complete lineage with freshness and quality information.
"""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from backend.api.semantic_export import (
    DEMO_BINDINGS,
    PUBLISHED_METRICS,
    resolve_metric,
)
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)


class ProvenanceSource(BaseModel):
    """Single source contributing to a metric."""
    source_system: str
    table_or_collection: str
    field_name: str
    freshness: str
    quality_score: float = Field(ge=0.0, le=1.0)


class ProvenanceTrace(BaseModel):
    """Complete provenance trace for a metric."""
    metric: str
    metric_name: str
    description: str
    sources: List[ProvenanceSource]


# Map metrics to their source systems, tables, and fields
METRIC_PROVENANCE: Dict[str, List[Dict[str, Any]]] = {
    "arr": [
        {"source_system": "Chargebee", "table_or_collection": "subscriptions", "field_name": "annual_value", "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.88},
        {"source_system": "Salesforce CRM", "table_or_collection": "opportunities", "field_name": "amount", "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.95},
    ],
    "mrr": [
        {"source_system": "Chargebee", "table_or_collection": "subscriptions", "field_name": "monthly_value", "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.88},
    ],
    "revenue": [
        {"source_system": "NetSuite ERP", "table_or_collection": "revenue_recognition", "field_name": "recognized_amount", "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.92},
        {"source_system": "Salesforce CRM", "table_or_collection": "opportunities", "field_name": "amount", "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.95},
    ],
    "services_revenue": [
        {"source_system": "NetSuite ERP", "table_or_collection": "revenue_recognition", "field_name": "services_amount", "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.92},
    ],
    "ar": [
        {"source_system": "NetSuite ERP", "table_or_collection": "accounts_receivable", "field_name": "balance", "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.90},
    ],
    "dso": [
        {"source_system": "NetSuite ERP", "table_or_collection": "accounts_receivable", "field_name": "days_outstanding", "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.90},
    ],
    "burn_rate": [
        {"source_system": "NetSuite ERP", "table_or_collection": "general_ledger", "field_name": "monthly_spend", "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.92},
    ],
    "gross_margin": [
        {"source_system": "NetSuite ERP", "table_or_collection": "profit_loss", "field_name": "gross_margin_pct", "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.92},
    ],
    "ar_aging": [
        {"source_system": "NetSuite ERP", "table_or_collection": "accounts_receivable", "field_name": "aging_bucket_amount", "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.90},
    ],
    "pipeline": [
        {"source_system": "Salesforce CRM", "table_or_collection": "opportunities", "field_name": "amount", "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.95},
    ],
    "pipeline_value": [
        {"source_system": "Salesforce CRM", "table_or_collection": "opportunities", "field_name": "amount", "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.95},
    ],
    "win_rate": [
        {"source_system": "Salesforce CRM", "table_or_collection": "opportunities", "field_name": "stage", "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.95},
    ],
    "quota_attainment": [
        {"source_system": "Salesforce CRM", "table_or_collection": "quotas", "field_name": "attainment_pct", "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.95},
    ],
    "churn_rate": [
        {"source_system": "Chargebee", "table_or_collection": "subscriptions", "field_name": "churn_flag", "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.88},
    ],
    "churn_risk": [
        {"source_system": "Salesforce CRM", "table_or_collection": "accounts", "field_name": "churn_score", "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.95},
    ],
    "nrr": [
        {"source_system": "Chargebee", "table_or_collection": "subscriptions", "field_name": "net_retention", "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.88},
    ],
    "nrr_by_cohort": [
        {"source_system": "Chargebee", "table_or_collection": "subscriptions", "field_name": "net_retention", "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.88},
    ],
    "throughput": [
        {"source_system": "Jira", "table_or_collection": "issues", "field_name": "completed_count", "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.85},
    ],
    "cycle_time": [
        {"source_system": "Jira", "table_or_collection": "issues", "field_name": "cycle_time_days", "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.85},
    ],
    "sla_compliance": [
        {"source_system": "Jira", "table_or_collection": "sla_tracking", "field_name": "compliance_pct", "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.85},
    ],
    "deploy_frequency": [
        {"source_system": "GitHub Actions", "table_or_collection": "deployments", "field_name": "deploy_count", "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.90},
    ],
    "mttr": [
        {"source_system": "PagerDuty", "table_or_collection": "incidents", "field_name": "resolution_time", "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.88},
    ],
    "uptime": [
        {"source_system": "PagerDuty", "table_or_collection": "services", "field_name": "uptime_pct", "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.88},
    ],
    "slo_attainment": [
        {"source_system": "PagerDuty", "table_or_collection": "slo_tracking", "field_name": "attainment_pct", "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.88},
    ],
    "cloud_cost": [
        {"source_system": "AWS Cost Explorer", "table_or_collection": "cost_and_usage", "field_name": "blended_cost", "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.92},
    ],
    "headcount": [
        {"source_system": "Workday", "table_or_collection": "employees", "field_name": "active_count", "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.92},
    ],
    "attrition_rate": [
        {"source_system": "Workday", "table_or_collection": "terminations", "field_name": "attrition_pct", "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.90},
    ],
    "time_to_fill": [
        {"source_system": "Greenhouse", "table_or_collection": "requisitions", "field_name": "days_to_fill", "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.88},
    ],
    "engagement_score": [
        {"source_system": "Culture Amp", "table_or_collection": "surveys", "field_name": "engagement_pct", "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.85},
    ],
    "compensation_ratio": [
        {"source_system": "Workday", "table_or_collection": "compensation", "field_name": "market_ratio", "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.92},
    ],
    "training_hours": [
        {"source_system": "Workday", "table_or_collection": "learning", "field_name": "hours_per_employee", "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.92},
    ],
    "offer_acceptance_rate": [
        {"source_system": "Greenhouse", "table_or_collection": "offers", "field_name": "acceptance_pct", "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.88},
    ],
    "internal_mobility_rate": [
        {"source_system": "Workday", "table_or_collection": "transfers", "field_name": "internal_fill_pct", "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.92},
    ],
    "span_of_control": [
        {"source_system": "Workday", "table_or_collection": "org_structure", "field_name": "avg_direct_reports", "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.92},
    ],
    "enps": [
        {"source_system": "Culture Amp", "table_or_collection": "surveys", "field_name": "enps_score", "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.85},
    ],
}


def get_provenance(metric_id: str) -> Optional[ProvenanceTrace]:
    """
    Get provenance trace for a metric.

    Returns None if metric is not found in the semantic catalog.
    """
    metric_def = resolve_metric(metric_id)
    if metric_def is None:
        return None

    sources_data = METRIC_PROVENANCE.get(metric_def.id, [])

    # If no specific provenance data, build from bindings
    if not sources_data:
        sources_data = _build_from_bindings(metric_def.id)

    sources = [ProvenanceSource(**s) for s in sources_data]

    return ProvenanceTrace(
        metric=metric_def.id,
        metric_name=metric_def.name,
        description=metric_def.description,
        sources=sources,
    )


def _build_from_bindings(metric_id: str) -> List[Dict[str, Any]]:
    """Fallback: build provenance from demo bindings."""
    sources = []
    for binding in DEMO_BINDINGS:
        sources.append({
            "source_system": binding.source_system,
            "table_or_collection": binding.canonical_event,
            "field_name": metric_id,
            "freshness": "2026-02-07T00:00:00Z",
            "quality_score": binding.quality_score,
        })
    return sources[:2]  # Limit to top 2 for generic fallback
