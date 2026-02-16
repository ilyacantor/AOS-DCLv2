"""
Provenance Trace Service - Traces any metric back to its source system, table, and field.

Provides complete lineage with freshness, quality, and SOR information.
Data source: entity_test_scenarios.json -> provenance for key metrics.
"""

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from backend.api.semantic_export import (
    DEMO_BINDINGS,
    PUBLISHED_METRICS,
    resolve_metric,
)
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

SCENARIO_FILE = Path("data/entity_test_scenarios.json")


class ProvenanceSource(BaseModel):
    """Single source contributing to a metric."""
    source_system: str
    table_or_collection: str
    field_name: str
    is_sor: bool = False
    freshness: str
    quality_score: float = Field(ge=0.0, le=1.0)


class ProvenanceTrace(BaseModel):
    """Complete provenance trace for a metric."""
    metric: str
    metric_name: str
    description: str
    sources: List[ProvenanceSource]


def _load_scenario_provenance() -> Dict[str, List[Dict[str, Any]]]:
    """Load provenance data from entity_test_scenarios.json."""
    if not SCENARIO_FILE.exists():
        return {}
    with open(SCENARIO_FILE, "r") as f:
        data = json.load(f)
    return data.get("provenance", {}).get("metric_sources", {})


# Load scenario-driven provenance at module level
_SCENARIO_PROVENANCE = _load_scenario_provenance()


# Fallback provenance for metrics not in scenario data
METRIC_PROVENANCE: Dict[str, List[Dict[str, Any]]] = {
    "arr": [
        {"source_system": "Chargebee", "table_or_collection": "subscriptions", "field_name": "annual_value", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.88},
        {"source_system": "Salesforce CRM", "table_or_collection": "opportunities", "field_name": "amount", "is_sor": False, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.95},
    ],
    "mrr": [
        {"source_system": "Chargebee", "table_or_collection": "subscriptions", "field_name": "monthly_value", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.88},
    ],
    "services_revenue": [
        {"source_system": "NetSuite ERP", "table_or_collection": "revenue_recognition", "field_name": "services_amount", "is_sor": True, "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.92},
    ],
    "ar": [
        {"source_system": "NetSuite ERP", "table_or_collection": "accounts_receivable", "field_name": "balance", "is_sor": True, "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.90},
    ],
    "dso": [
        {"source_system": "NetSuite ERP", "table_or_collection": "accounts_receivable", "field_name": "days_outstanding", "is_sor": True, "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.90},
    ],
    "burn_rate": [
        {"source_system": "NetSuite ERP", "table_or_collection": "general_ledger", "field_name": "monthly_spend", "is_sor": True, "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.92},
    ],
    "gross_margin": [
        {"source_system": "NetSuite ERP", "table_or_collection": "profit_loss", "field_name": "gross_margin_pct", "is_sor": True, "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.92},
    ],
    "ar_aging": [
        {"source_system": "NetSuite ERP", "table_or_collection": "accounts_receivable", "field_name": "aging_bucket_amount", "is_sor": True, "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.90},
    ],
    "pipeline": [
        {"source_system": "Salesforce CRM", "table_or_collection": "opportunities", "field_name": "amount", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.95},
    ],
    "pipeline_value": [
        {"source_system": "Salesforce CRM", "table_or_collection": "opportunities", "field_name": "amount", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.95},
    ],
    "win_rate": [
        {"source_system": "Salesforce CRM", "table_or_collection": "opportunities", "field_name": "stage", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.95},
    ],
    "quota_attainment": [
        {"source_system": "Salesforce CRM", "table_or_collection": "quotas", "field_name": "attainment_pct", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.95},
    ],
    "churn_rate": [
        {"source_system": "Chargebee", "table_or_collection": "subscriptions", "field_name": "churn_flag", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.88},
    ],
    "churn_risk": [
        {"source_system": "Salesforce CRM", "table_or_collection": "accounts", "field_name": "churn_score", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.95},
    ],
    "nrr": [
        {"source_system": "Chargebee", "table_or_collection": "subscriptions", "field_name": "net_retention", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.88},
    ],
    "nrr_by_cohort": [
        {"source_system": "Chargebee", "table_or_collection": "subscriptions", "field_name": "net_retention", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.88},
    ],
    "throughput": [
        {"source_system": "Jira", "table_or_collection": "issues", "field_name": "completed_count", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.85},
    ],
    "cycle_time": [
        {"source_system": "Jira", "table_or_collection": "issues", "field_name": "cycle_time_days", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.85},
    ],
    "sla_compliance": [
        {"source_system": "Jira", "table_or_collection": "sla_tracking", "field_name": "compliance_pct", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.85},
    ],
    "deploy_frequency": [
        {"source_system": "GitHub Actions", "table_or_collection": "deployments", "field_name": "deploy_count", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.90},
    ],
    "mttr": [
        {"source_system": "PagerDuty", "table_or_collection": "incidents", "field_name": "resolution_time", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.88},
    ],
    "uptime": [
        {"source_system": "PagerDuty", "table_or_collection": "services", "field_name": "uptime_pct", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.88},
    ],
    "slo_attainment": [
        {"source_system": "PagerDuty", "table_or_collection": "slo_tracking", "field_name": "attainment_pct", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.88},
    ],
    "cloud_cost": [
        {"source_system": "AWS Cost Explorer", "table_or_collection": "cost_and_usage", "field_name": "blended_cost", "is_sor": True, "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.92},
    ],
    "headcount": [
        {"source_system": "Workday", "table_or_collection": "employees", "field_name": "active_count", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.92},
    ],
    "attrition_rate": [
        {"source_system": "Workday", "table_or_collection": "terminations", "field_name": "attrition_pct", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.90},
    ],
    "time_to_fill": [
        {"source_system": "Greenhouse", "table_or_collection": "requisitions", "field_name": "days_to_fill", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.88},
    ],
    "engagement_score": [
        {"source_system": "Culture Amp", "table_or_collection": "surveys", "field_name": "engagement_pct", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.85},
    ],
    "compensation_ratio": [
        {"source_system": "Workday", "table_or_collection": "compensation", "field_name": "market_ratio", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.92},
    ],
    "training_hours": [
        {"source_system": "Workday", "table_or_collection": "learning", "field_name": "hours_per_employee", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.92},
    ],
    "offer_acceptance_rate": [
        {"source_system": "Greenhouse", "table_or_collection": "offers", "field_name": "acceptance_pct", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.88},
    ],
    "internal_mobility_rate": [
        {"source_system": "Workday", "table_or_collection": "transfers", "field_name": "internal_fill_pct", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.92},
    ],
    "span_of_control": [
        {"source_system": "Workday", "table_or_collection": "org_structure", "field_name": "avg_direct_reports", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.92},
    ],
    "enps": [
        {"source_system": "Culture Amp", "table_or_collection": "surveys", "field_name": "enps_score", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.85},
    ],
}


def _get_provenance_data(metric_id: str) -> List[Dict[str, Any]]:
    """Get provenance data from scenario file first, then fallback to hardcoded."""
    # Check scenario-driven provenance first
    if metric_id in _SCENARIO_PROVENANCE:
        scenario_sources = _SCENARIO_PROVENANCE[metric_id]
        return [
            {
                "source_system": s["source_system"],
                "table_or_collection": s["table"],
                "field_name": s["field"],
                "is_sor": s.get("is_sor", False),
                "freshness": s.get("freshness", "2026-02-07T00:00:00Z"),
                "quality_score": s.get("quality_score", 0.85),
            }
            for s in scenario_sources
        ]

    # Fallback to hardcoded
    return METRIC_PROVENANCE.get(metric_id, [])


def get_provenance(metric_id: str) -> Optional[ProvenanceTrace]:
    """
    Get provenance trace for a metric.

    Returns None if metric is not found in the semantic catalog.
    """
    metric_def = resolve_metric(metric_id)
    if metric_def is None:
        return None

    sources_data = _get_provenance_data(metric_def.id)

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
            "is_sor": False,
            "freshness": "2026-02-07T00:00:00Z",
            "quality_score": binding.quality_score,
        })
    return sources[:2]  # Limit to top 2 for generic fallback
