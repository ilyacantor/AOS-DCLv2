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
    BINDINGS,
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
    "revenue": [
        {"source_system": "netsuite_erp", "table_or_collection": "revenue_recognition", "field_name": "recognized_revenue", "is_sor": True, "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.92},
        {"source_system": "salesforce_crm", "table_or_collection": "opportunities", "field_name": "amount", "is_sor": False, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.95},
    ],
    "customer_count": [
        {"source_system": "salesforce_crm", "table_or_collection": "accounts", "field_name": "active_customer_count", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.95},
    ],
    "promotion_rate_pct": [
        {"source_system": "workday", "table_or_collection": "job_changes", "field_name": "promotion_pct", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.92},
    ],
    "diversity_index": [
        {"source_system": "workday", "table_or_collection": "demographics", "field_name": "diversity_score", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.92},
    ],
    "arr": [
        {"source_system": "chargebee", "table_or_collection": "subscriptions", "field_name": "annual_value", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.88},
        {"source_system": "salesforce_crm", "table_or_collection": "opportunities", "field_name": "amount", "is_sor": False, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.95},
    ],
    "mrr": [
        {"source_system": "chargebee", "table_or_collection": "subscriptions", "field_name": "monthly_value", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.88},
    ],
    "services_revenue": [
        {"source_system": "netsuite_erp", "table_or_collection": "revenue_recognition", "field_name": "services_amount", "is_sor": True, "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.92},
    ],
    "ar": [
        {"source_system": "netsuite_erp", "table_or_collection": "accounts_receivable", "field_name": "balance", "is_sor": True, "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.90},
    ],
    "dso": [
        {"source_system": "netsuite_erp", "table_or_collection": "accounts_receivable", "field_name": "days_outstanding", "is_sor": True, "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.90},
    ],
    "burn_rate": [
        {"source_system": "netsuite_erp", "table_or_collection": "general_ledger", "field_name": "monthly_spend", "is_sor": True, "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.92},
    ],
    "gross_margin_pct": [
        {"source_system": "netsuite_erp", "table_or_collection": "profit_loss", "field_name": "gross_margin_pct", "is_sor": True, "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.92},
    ],
    "ar_aging": [
        {"source_system": "netsuite_erp", "table_or_collection": "accounts_receivable", "field_name": "aging_bucket_amount", "is_sor": True, "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.90},
    ],
    "pipeline": [
        {"source_system": "salesforce_crm", "table_or_collection": "opportunities", "field_name": "amount", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.95},
    ],
    "pipeline_value": [
        {"source_system": "salesforce_crm", "table_or_collection": "opportunities", "field_name": "amount", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.95},
    ],
    "win_rate_pct": [
        {"source_system": "salesforce_crm", "table_or_collection": "opportunities", "field_name": "stage", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.95},
    ],
    "quota_attainment_pct": [
        {"source_system": "salesforce_crm", "table_or_collection": "quotas", "field_name": "attainment_pct", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.95},
    ],
    "churn_rate_pct": [
        {"source_system": "chargebee", "table_or_collection": "subscriptions", "field_name": "churn_flag", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.88},
    ],
    "churn_risk": [
        {"source_system": "salesforce_crm", "table_or_collection": "accounts", "field_name": "churn_score", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.95},
    ],
    "nrr": [
        {"source_system": "chargebee", "table_or_collection": "subscriptions", "field_name": "net_retention", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.88},
    ],
    "nrr_by_cohort": [
        {"source_system": "chargebee", "table_or_collection": "subscriptions", "field_name": "net_retention", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.88},
    ],
    "throughput": [
        {"source_system": "jira", "table_or_collection": "issues", "field_name": "completed_count", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.85},
    ],
    "cycle_time": [
        {"source_system": "jira", "table_or_collection": "issues", "field_name": "cycle_time_days", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.85},
    ],
    "sla_compliance_pct": [
        {"source_system": "jira", "table_or_collection": "sla_tracking", "field_name": "compliance_pct", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.85},
    ],
    "deploy_frequency": [
        {"source_system": "github_actions", "table_or_collection": "deployments", "field_name": "deploy_count", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.90},
    ],
    "mttr": [
        {"source_system": "pagerduty", "table_or_collection": "incidents", "field_name": "resolution_time", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.88},
    ],
    "uptime_pct": [
        {"source_system": "pagerduty", "table_or_collection": "services", "field_name": "uptime_pct", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.88},
    ],
    "slo_attainment_pct": [
        {"source_system": "pagerduty", "table_or_collection": "slo_tracking", "field_name": "attainment_pct", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.88},
    ],
    "cloud_spend": [
        {"source_system": "aws_cost_explorer", "table_or_collection": "cost_and_usage", "field_name": "blended_cost", "is_sor": True, "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.92},
    ],
    "headcount": [
        {"source_system": "workday", "table_or_collection": "employees", "field_name": "active_count", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.92},
    ],
    "attrition_rate_pct": [
        {"source_system": "workday", "table_or_collection": "terminations", "field_name": "attrition_pct", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.90},
    ],
    "time_to_fill": [
        {"source_system": "greenhouse", "table_or_collection": "requisitions", "field_name": "days_to_fill", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.88},
    ],
    "engagement_score": [
        {"source_system": "culture_amp", "table_or_collection": "surveys", "field_name": "engagement_pct", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.85},
    ],
    "compensation_ratio": [
        {"source_system": "workday", "table_or_collection": "compensation", "field_name": "market_ratio", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.92},
    ],
    "training_hours": [
        {"source_system": "workday", "table_or_collection": "learning", "field_name": "hours_per_employee", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.92},
    ],
    "offer_acceptance_rate_pct": [
        {"source_system": "greenhouse", "table_or_collection": "offers", "field_name": "acceptance_pct", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.88},
    ],
    "internal_mobility_rate_pct": [
        {"source_system": "workday", "table_or_collection": "transfers", "field_name": "internal_fill_pct", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.92},
    ],
    "span_of_control": [
        {"source_system": "workday", "table_or_collection": "org_structure", "field_name": "avg_direct_reports", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.92},
    ],
    "enps": [
        {"source_system": "culture_amp", "table_or_collection": "surveys", "field_name": "enps_score", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.85},
    ],
    # ── ARR Waterfall ──────────────────────────────────────────────────
    "beginning_arr": [
        {"source_system": "chargebee", "table_or_collection": "subscriptions", "field_name": "beginning_arr", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.88},
    ],
    "new_arr": [
        {"source_system": "chargebee", "table_or_collection": "subscriptions", "field_name": "new_arr", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.88},
    ],
    "new_logo_arr": [
        {"source_system": "salesforce_crm", "table_or_collection": "opportunities", "field_name": "new_logo_arr", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.95},
        {"source_system": "chargebee", "table_or_collection": "subscriptions", "field_name": "new_logo_arr", "is_sor": False, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.88},
    ],
    "expansion_arr": [
        {"source_system": "chargebee", "table_or_collection": "subscriptions", "field_name": "expansion_arr", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.88},
    ],
    "churned_arr": [
        {"source_system": "chargebee", "table_or_collection": "subscriptions", "field_name": "churned_arr", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.88},
    ],
    # ── Revenue Decomposition ─────────────────────────────────────────
    "new_logo_revenue": [
        {"source_system": "salesforce_crm", "table_or_collection": "opportunities", "field_name": "new_logo_amount", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.95},
    ],
    "expansion_revenue": [
        {"source_system": "chargebee", "table_or_collection": "subscriptions", "field_name": "expansion_amount", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.88},
    ],
    "renewal_revenue": [
        {"source_system": "chargebee", "table_or_collection": "subscriptions", "field_name": "renewal_amount", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.88},
    ],
    # ── P&L ────────────────────────────────────────────────────────────
    "cogs": [
        {"source_system": "netsuite_erp", "table_or_collection": "general_ledger", "field_name": "cogs_amount", "is_sor": True, "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.92},
    ],
    "gross_profit": [
        {"source_system": "netsuite_erp", "table_or_collection": "profit_loss", "field_name": "gross_profit", "is_sor": True, "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.92},
    ],
    "sm_expense": [
        {"source_system": "netsuite_erp", "table_or_collection": "general_ledger", "field_name": "sm_expense", "is_sor": True, "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.92},
    ],
    "rd_expense": [
        {"source_system": "netsuite_erp", "table_or_collection": "general_ledger", "field_name": "rd_expense", "is_sor": True, "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.92},
    ],
    "ga_expense": [
        {"source_system": "netsuite_erp", "table_or_collection": "general_ledger", "field_name": "ga_expense", "is_sor": True, "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.92},
    ],
    "opex": [
        {"source_system": "netsuite_erp", "table_or_collection": "profit_loss", "field_name": "total_opex", "is_sor": True, "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.92},
    ],
    "ebitda": [
        {"source_system": "netsuite_erp", "table_or_collection": "profit_loss", "field_name": "ebitda", "is_sor": True, "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.92},
    ],
    "ebitda_margin_pct": [
        {"source_system": "netsuite_erp", "table_or_collection": "profit_loss", "field_name": "ebitda_margin_pct", "is_sor": True, "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.92},
    ],
    "da_expense": [
        {"source_system": "netsuite_erp", "table_or_collection": "general_ledger", "field_name": "depreciation_amortization", "is_sor": True, "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.92},
    ],
    "operating_profit": [
        {"source_system": "netsuite_erp", "table_or_collection": "profit_loss", "field_name": "operating_profit", "is_sor": True, "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.92},
    ],
    "operating_margin_pct": [
        {"source_system": "netsuite_erp", "table_or_collection": "profit_loss", "field_name": "operating_margin_pct", "is_sor": True, "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.92},
    ],
    "tax_expense": [
        {"source_system": "netsuite_erp", "table_or_collection": "general_ledger", "field_name": "tax_provision", "is_sor": True, "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.92},
    ],
    "net_income": [
        {"source_system": "netsuite_erp", "table_or_collection": "profit_loss", "field_name": "net_income", "is_sor": True, "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.92},
    ],
    "net_margin_pct": [
        {"source_system": "netsuite_erp", "table_or_collection": "profit_loss", "field_name": "net_margin_pct", "is_sor": True, "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.92},
    ],
    # ── Balance Sheet ──────────────────────────────────────────────────
    "cash": [
        {"source_system": "netsuite_erp", "table_or_collection": "balance_sheet", "field_name": "cash_and_equivalents", "is_sor": True, "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.92},
    ],
    "unbilled_revenue": [
        {"source_system": "netsuite_erp", "table_or_collection": "balance_sheet", "field_name": "unbilled_revenue", "is_sor": True, "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.92},
    ],
    "prepaid_expenses": [
        {"source_system": "netsuite_erp", "table_or_collection": "balance_sheet", "field_name": "prepaid_expenses", "is_sor": True, "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.92},
    ],
    "pp_e": [
        {"source_system": "netsuite_erp", "table_or_collection": "balance_sheet", "field_name": "net_ppe", "is_sor": True, "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.92},
    ],
    "intangibles": [
        {"source_system": "netsuite_erp", "table_or_collection": "balance_sheet", "field_name": "intangible_assets", "is_sor": True, "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.92},
    ],
    "goodwill": [
        {"source_system": "netsuite_erp", "table_or_collection": "balance_sheet", "field_name": "goodwill", "is_sor": True, "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.92},
    ],
    "total_assets": [
        {"source_system": "netsuite_erp", "table_or_collection": "balance_sheet", "field_name": "total_assets", "is_sor": True, "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.92},
    ],
    "ap": [
        {"source_system": "netsuite_erp", "table_or_collection": "accounts_payable", "field_name": "balance", "is_sor": True, "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.92},
    ],
    "accrued_expenses": [
        {"source_system": "netsuite_erp", "table_or_collection": "balance_sheet", "field_name": "accrued_expenses", "is_sor": True, "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.92},
    ],
    "deferred_revenue": [
        {"source_system": "netsuite_erp", "table_or_collection": "balance_sheet", "field_name": "deferred_revenue", "is_sor": True, "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.92},
    ],
    "deferred_revenue_current": [
        {"source_system": "netsuite_erp", "table_or_collection": "balance_sheet", "field_name": "deferred_revenue_current", "is_sor": True, "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.92},
    ],
    "deferred_revenue_lt": [
        {"source_system": "netsuite_erp", "table_or_collection": "balance_sheet", "field_name": "deferred_revenue_noncurrent", "is_sor": True, "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.92},
    ],
    "total_liabilities": [
        {"source_system": "netsuite_erp", "table_or_collection": "balance_sheet", "field_name": "total_liabilities", "is_sor": True, "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.92},
    ],
    "retained_earnings": [
        {"source_system": "netsuite_erp", "table_or_collection": "balance_sheet", "field_name": "retained_earnings", "is_sor": True, "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.92},
    ],
    "stockholders_equity": [
        {"source_system": "netsuite_erp", "table_or_collection": "balance_sheet", "field_name": "stockholders_equity", "is_sor": True, "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.92},
    ],
    # ── Cash Flow ──────────────────────────────────────────────────────
    "cash_from_operations": [
        {"source_system": "netsuite_erp", "table_or_collection": "cash_flow", "field_name": "operating_cash_flow", "is_sor": True, "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.92},
    ],
    "capex": [
        {"source_system": "netsuite_erp", "table_or_collection": "cash_flow", "field_name": "capital_expenditures", "is_sor": True, "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.92},
    ],
    "fcf": [
        {"source_system": "netsuite_erp", "table_or_collection": "cash_flow", "field_name": "free_cash_flow", "is_sor": True, "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.92},
    ],
    # ── SaaS Efficiency ────────────────────────────────────────────────
    "gross_churn_pct": [
        {"source_system": "chargebee", "table_or_collection": "subscriptions", "field_name": "gross_churn_pct", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.88},
    ],
    "logo_churn_pct": [
        {"source_system": "salesforce_crm", "table_or_collection": "accounts", "field_name": "logo_churn_pct", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.95},
    ],
    "acv": [
        {"source_system": "salesforce_crm", "table_or_collection": "opportunities", "field_name": "acv", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.95},
    ],
    "ltv": [
        {"source_system": "chargebee", "table_or_collection": "subscriptions", "field_name": "ltv_computed", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.88},
    ],
    "cac": [
        {"source_system": "netsuite_erp", "table_or_collection": "profit_loss", "field_name": "cac_computed", "is_sor": True, "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.92},
    ],
    "ltv_cac_ratio": [
        {"source_system": "chargebee", "table_or_collection": "subscriptions", "field_name": "ltv_cac_ratio", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.88},
    ],
    "magic_number": [
        {"source_system": "netsuite_erp", "table_or_collection": "profit_loss", "field_name": "magic_number_computed", "is_sor": True, "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.92},
    ],
    "burn_multiple": [
        {"source_system": "netsuite_erp", "table_or_collection": "cash_flow", "field_name": "burn_multiple_computed", "is_sor": True, "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.92},
    ],
    "rule_of_40": [
        {"source_system": "netsuite_erp", "table_or_collection": "profit_loss", "field_name": "rule_of_40_computed", "is_sor": True, "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.92},
    ],
    "revenue_per_employee": [
        {"source_system": "netsuite_erp", "table_or_collection": "profit_loss", "field_name": "revenue_per_employee", "is_sor": True, "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.92},
    ],
    "arr_per_employee": [
        {"source_system": "chargebee", "table_or_collection": "subscriptions", "field_name": "arr_per_employee", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.88},
    ],
    # ── Pipeline ───────────────────────────────────────────────────────
    "sales_cycle_days": [
        {"source_system": "salesforce_crm", "table_or_collection": "opportunities", "field_name": "sales_cycle_days", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.95},
    ],
    "avg_deal_size": [
        {"source_system": "salesforce_crm", "table_or_collection": "opportunities", "field_name": "avg_deal_size", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.95},
    ],
    # ── Customer ───────────────────────────────────────────────────────
    "new_customers": [
        {"source_system": "salesforce_crm", "table_or_collection": "accounts", "field_name": "new_customer_count", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.95},
    ],
    "churned_customers": [
        {"source_system": "chargebee", "table_or_collection": "subscriptions", "field_name": "churned_customer_count", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.88},
    ],
    # ── People ─────────────────────────────────────────────────────────
    "new_hires": [
        {"source_system": "workday", "table_or_collection": "employees", "field_name": "hire_count", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.92},
    ],
    "terminations": [
        {"source_system": "workday", "table_or_collection": "terminations", "field_name": "termination_count", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.90},
    ],
    "engineering_headcount": [
        {"source_system": "workday", "table_or_collection": "employees", "field_name": "engineering_active_count", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.92},
    ],
    "sales_headcount": [
        {"source_system": "workday", "table_or_collection": "employees", "field_name": "sales_active_count", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.92},
    ],
    # ── Support ────────────────────────────────────────────────────────
    "support_tickets": [
        {"source_system": "zendesk", "table_or_collection": "tickets", "field_name": "ticket_count", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.88},
    ],
    "csat": [
        {"source_system": "zendesk", "table_or_collection": "tickets", "field_name": "satisfaction_rating", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.88},
    ],
    "nps": [
        {"source_system": "zendesk", "table_or_collection": "surveys", "field_name": "nps_score", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.88},
    ],
    "first_response_hours": [
        {"source_system": "zendesk", "table_or_collection": "tickets", "field_name": "first_response_time_hours", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.88},
    ],
    "resolution_hours": [
        {"source_system": "zendesk", "table_or_collection": "tickets", "field_name": "resolution_time_hours", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.88},
    ],
    # ── Engineering ────────────────────────────────────────────────────
    "sprint_velocity": [
        {"source_system": "jira", "table_or_collection": "sprints", "field_name": "story_points_completed", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.85},
    ],
    "story_points": [
        {"source_system": "jira", "table_or_collection": "issues", "field_name": "story_points", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.85},
    ],
    "features_shipped": [
        {"source_system": "jira", "table_or_collection": "issues", "field_name": "epic_completed_count", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.85},
    ],
    "tech_debt_pct": [
        {"source_system": "jira", "table_or_collection": "issues", "field_name": "tech_debt_ratio", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.85},
    ],
    # ── Infrastructure ─────────────────────────────────────────────────
    "cloud_spend_pct_revenue": [
        {"source_system": "aws_cost_explorer", "table_or_collection": "cost_and_usage", "field_name": "cloud_spend_pct_revenue", "is_sor": True, "freshness": "2026-02-06T00:00:00Z", "quality_score": 0.92},
    ],
    "p1_incidents": [
        {"source_system": "datadog", "table_or_collection": "incidents", "field_name": "p1_count", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.90},
    ],
    "p2_incidents": [
        {"source_system": "datadog", "table_or_collection": "incidents", "field_name": "p2_count", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.90},
    ],
    "incident_count": [
        {"source_system": "datadog", "table_or_collection": "incidents", "field_name": "total_incidents", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.90},
    ],
    "mttr_p1_hours": [
        {"source_system": "datadog", "table_or_collection": "incidents", "field_name": "mttr_p1_hours", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.90},
    ],
    "mttr_p2_hours": [
        {"source_system": "datadog", "table_or_collection": "incidents", "field_name": "mttr_p2_hours", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.90},
    ],
    "downtime_hours": [
        {"source_system": "datadog", "table_or_collection": "slos", "field_name": "downtime_hours", "is_sor": True, "freshness": "2026-02-07T00:00:00Z", "quality_score": 0.90},
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
    """Build provenance from demo bindings.

    This is demo-only seed data — not derived from real system connections.
    Only called when no scenario or catalog provenance exists for a metric.
    """
    if not BINDINGS:
        logger.warning(f"[provenance] No demo bindings available for metric={metric_id}")
        return []
    sources = []
    for binding in BINDINGS:
        sources.append({
            "source_system": binding.source_system,
            "table_or_collection": binding.canonical_event,
            "field_name": metric_id,
            "is_sor": False,
            "freshness": "2026-02-07T00:00:00Z",
            "quality_score": binding.quality_score,
        })
    return sources[:2]
