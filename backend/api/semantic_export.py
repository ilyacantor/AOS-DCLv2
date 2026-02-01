"""
DCL Semantic Export API - Exposes semantic layer for NLQ consumption.

This module provides a single endpoint that NLQ can poll to get the full
semantic catalog including metrics, entities (dimensions), and bindings.

NLQ uses this data to:
- Resolve aliases ("AR" → "ar")
- Know which dimensions are valid for each metric
- Fail fast with helpful messages when metrics don't exist
"""

from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field
from enum import Enum


class Pack(str, Enum):
    CFO = "cfo"
    CRO = "cro"
    COO = "coo"
    CTO = "cto"


class TimeGrain(str, Enum):
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    QUARTER = "quarter"
    YEAR = "year"


class MetricDefinition(BaseModel):
    """Published metric definition for NLQ consumption."""
    id: str
    name: str
    description: str
    aliases: List[str] = Field(default_factory=list)
    pack: Pack
    allowed_dims: List[str] = Field(default_factory=list)
    allowed_grains: List[TimeGrain] = Field(default_factory=list)
    measure_op: Optional[str] = None
    default_grain: Optional[TimeGrain] = None


class EntityDefinition(BaseModel):
    """Entity (dimension) definition for NLQ consumption."""
    id: str
    name: str
    description: str
    aliases: List[str] = Field(default_factory=list)


class BindingSummary(BaseModel):
    """Source system binding summary."""
    source_system: str
    canonical_event: str
    quality_score: float = Field(ge=0.0, le=1.0)
    freshness_score: float = Field(ge=0.0, le=1.0)
    dims_coverage: Dict[str, bool] = Field(default_factory=dict)


class ModeInfo(BaseModel):
    """Current DCL mode information."""
    data_mode: str  # "Demo" or "Farm"
    run_mode: str   # "Dev" or "Prod"
    last_updated: Optional[str] = None


class SemanticExport(BaseModel):
    """Full semantic export payload for NLQ."""
    version: str = "1.0.0"
    tenant_id: str = "default"
    mode: ModeInfo
    metrics: List[MetricDefinition] = Field(default_factory=list)
    entities: List[EntityDefinition] = Field(default_factory=list)
    persona_concepts: Dict[str, List[str]] = Field(default_factory=dict)
    bindings: List[BindingSummary] = Field(default_factory=list)
    metric_entity_matrix: Dict[str, List[str]] = Field(default_factory=dict)


PUBLISHED_METRICS: List[MetricDefinition] = [
    MetricDefinition(
        id="arr",
        name="Annual Recurring Revenue",
        description="Total annual value of recurring subscription revenue",
        aliases=["ARR", "annual recurring revenue", "recurring revenue", "annual revenue"],
        pack=Pack.CFO,
        allowed_dims=["customer", "service_line", "region", "segment"],
        allowed_grains=[TimeGrain.MONTH, TimeGrain.QUARTER, TimeGrain.YEAR],
        measure_op="point_in_time_sum",
        default_grain=TimeGrain.MONTH
    ),
    MetricDefinition(
        id="mrr",
        name="Monthly Recurring Revenue",
        description="Total monthly value of recurring subscription revenue",
        aliases=["MRR", "monthly recurring revenue", "monthly revenue"],
        pack=Pack.CFO,
        allowed_dims=["customer", "service_line", "region", "segment"],
        allowed_grains=[TimeGrain.MONTH, TimeGrain.QUARTER],
        measure_op="point_in_time_sum",
        default_grain=TimeGrain.MONTH
    ),
    MetricDefinition(
        id="revenue",
        name="Total Revenue",
        description="Total recognized revenue across all sources",
        aliases=["total revenue", "sales", "income", "top line"],
        pack=Pack.CFO,
        allowed_dims=["customer", "service_line", "region", "product", "segment"],
        allowed_grains=[TimeGrain.DAY, TimeGrain.WEEK, TimeGrain.MONTH, TimeGrain.QUARTER, TimeGrain.YEAR],
        measure_op="sum",
        default_grain=TimeGrain.MONTH
    ),
    MetricDefinition(
        id="services_revenue",
        name="Services Revenue",
        description="Revenue from professional services",
        aliases=["professional services", "PS revenue", "consulting revenue"],
        pack=Pack.CFO,
        allowed_dims=["customer", "service_line", "region", "project"],
        allowed_grains=[TimeGrain.MONTH, TimeGrain.QUARTER, TimeGrain.YEAR],
        measure_op="sum",
        default_grain=TimeGrain.MONTH
    ),
    MetricDefinition(
        id="ar",
        name="Accounts Receivable",
        description="Outstanding customer balances owed",
        aliases=["AR", "accounts receivable", "receivables", "outstanding invoices", "A/R"],
        pack=Pack.CFO,
        allowed_dims=["customer", "invoice", "aging_bucket"],
        allowed_grains=[TimeGrain.DAY, TimeGrain.WEEK, TimeGrain.MONTH],
        measure_op="point_in_time_sum",
        default_grain=TimeGrain.MONTH
    ),
    MetricDefinition(
        id="dso",
        name="Days Sales Outstanding",
        description="Average days to collect payment from customers",
        aliases=["DSO", "days sales outstanding", "collection days", "AR days"],
        pack=Pack.CFO,
        allowed_dims=["customer", "segment", "region"],
        allowed_grains=[TimeGrain.MONTH, TimeGrain.QUARTER],
        measure_op="avg_days_between",
        default_grain=TimeGrain.MONTH
    ),
    MetricDefinition(
        id="burn_rate",
        name="Burn Rate",
        description="Monthly cash consumption rate",
        aliases=["burn", "cash burn", "monthly burn", "spending rate"],
        pack=Pack.CFO,
        allowed_dims=["cost_center", "category"],
        allowed_grains=[TimeGrain.MONTH, TimeGrain.QUARTER],
        measure_op="sum",
        default_grain=TimeGrain.MONTH
    ),
    MetricDefinition(
        id="gross_margin",
        name="Gross Margin",
        description="Revenue minus cost of goods sold as percentage",
        aliases=["margin", "GM", "gross profit margin"],
        pack=Pack.CFO,
        allowed_dims=["product", "service_line", "segment"],
        allowed_grains=[TimeGrain.MONTH, TimeGrain.QUARTER, TimeGrain.YEAR],
        measure_op="ratio",
        default_grain=TimeGrain.QUARTER
    ),
    MetricDefinition(
        id="pipeline",
        name="Sales Pipeline",
        description="Total value of open opportunities",
        aliases=["sales pipeline", "open pipeline", "pipeline value", "opportunities"],
        pack=Pack.CRO,
        allowed_dims=["rep", "stage", "region", "segment"],
        allowed_grains=[TimeGrain.WEEK, TimeGrain.MONTH, TimeGrain.QUARTER],
        measure_op="point_in_time_sum",
        default_grain=TimeGrain.MONTH
    ),
    MetricDefinition(
        id="win_rate",
        name="Win Rate",
        description="Percentage of opportunities won",
        aliases=["close rate", "conversion rate", "deal win rate"],
        pack=Pack.CRO,
        allowed_dims=["rep", "segment", "region", "product"],
        allowed_grains=[TimeGrain.MONTH, TimeGrain.QUARTER],
        measure_op="ratio",
        default_grain=TimeGrain.QUARTER
    ),
    MetricDefinition(
        id="churn_rate",
        name="Churn Rate",
        description="Percentage of customers or revenue lost",
        aliases=["churn", "customer churn", "revenue churn", "attrition"],
        pack=Pack.CRO,
        allowed_dims=["segment", "region", "cohort"],
        allowed_grains=[TimeGrain.MONTH, TimeGrain.QUARTER],
        measure_op="ratio",
        default_grain=TimeGrain.MONTH
    ),
    MetricDefinition(
        id="nrr",
        name="Net Revenue Retention",
        description="Revenue retained plus expansion from existing customers",
        aliases=["NRR", "net retention", "dollar retention", "NDR"],
        pack=Pack.CRO,
        allowed_dims=["segment", "region", "cohort"],
        allowed_grains=[TimeGrain.MONTH, TimeGrain.QUARTER, TimeGrain.YEAR],
        measure_op="ratio",
        default_grain=TimeGrain.MONTH
    ),
    MetricDefinition(
        id="throughput",
        name="Throughput",
        description="Work items completed per time period",
        aliases=["velocity", "output", "completion rate"],
        pack=Pack.COO,
        allowed_dims=["team", "project", "work_type"],
        allowed_grains=[TimeGrain.DAY, TimeGrain.WEEK, TimeGrain.MONTH],
        measure_op="count",
        default_grain=TimeGrain.WEEK
    ),
    MetricDefinition(
        id="cycle_time",
        name="Cycle Time",
        description="Average time to complete work items",
        aliases=["lead time", "completion time", "turnaround time"],
        pack=Pack.COO,
        allowed_dims=["team", "project", "work_type", "priority"],
        allowed_grains=[TimeGrain.WEEK, TimeGrain.MONTH],
        measure_op="avg_days_between",
        default_grain=TimeGrain.WEEK
    ),
    MetricDefinition(
        id="sla_compliance",
        name="SLA Compliance",
        description="Percentage of SLAs met",
        aliases=["SLA", "service level", "compliance rate"],
        pack=Pack.COO,
        allowed_dims=["team", "customer", "sla_type"],
        allowed_grains=[TimeGrain.WEEK, TimeGrain.MONTH, TimeGrain.QUARTER],
        measure_op="ratio",
        default_grain=TimeGrain.MONTH
    ),
    MetricDefinition(
        id="deploy_frequency",
        name="Deployment Frequency",
        description="Number of deployments per time period",
        aliases=["deploys", "release frequency", "shipping velocity"],
        pack=Pack.CTO,
        allowed_dims=["team", "service", "environment"],
        allowed_grains=[TimeGrain.DAY, TimeGrain.WEEK, TimeGrain.MONTH],
        measure_op="count",
        default_grain=TimeGrain.WEEK
    ),
    MetricDefinition(
        id="mttr",
        name="Mean Time to Recovery",
        description="Average time to recover from incidents",
        aliases=["MTTR", "recovery time", "incident recovery"],
        pack=Pack.CTO,
        allowed_dims=["team", "service", "severity"],
        allowed_grains=[TimeGrain.WEEK, TimeGrain.MONTH],
        measure_op="avg_days_between",
        default_grain=TimeGrain.MONTH
    ),
    MetricDefinition(
        id="uptime",
        name="Uptime",
        description="Percentage of time services are available",
        aliases=["availability", "system uptime", "service availability"],
        pack=Pack.CTO,
        allowed_dims=["service", "environment", "region"],
        allowed_grains=[TimeGrain.DAY, TimeGrain.WEEK, TimeGrain.MONTH],
        measure_op="ratio",
        default_grain=TimeGrain.MONTH
    ),
    MetricDefinition(
        id="slo_attainment",
        name="SLO Attainment",
        description="Percentage of Service Level Objectives met",
        aliases=["SLO", "SLO compliance", "objective attainment"],
        pack=Pack.CTO,
        allowed_dims=["service", "slo_type", "team"],
        allowed_grains=[TimeGrain.WEEK, TimeGrain.MONTH],
        measure_op="ratio",
        default_grain=TimeGrain.MONTH
    ),
    MetricDefinition(
        id="cloud_spend",
        name="Cloud Spend",
        description="Total cloud infrastructure costs",
        aliases=["cloud cost", "infrastructure cost", "AWS spend", "GCP cost", "Azure cost"],
        pack=Pack.CTO,
        allowed_dims=["service", "team", "resource_type", "environment"],
        allowed_grains=[TimeGrain.DAY, TimeGrain.WEEK, TimeGrain.MONTH],
        measure_op="sum",
        default_grain=TimeGrain.MONTH
    ),
]


PUBLISHED_ENTITIES: List[EntityDefinition] = [
    EntityDefinition(
        id="customer",
        name="Customer",
        description="Business customer or account",
        aliases=["account", "client", "company", "org", "organization"]
    ),
    EntityDefinition(
        id="service_line",
        name="Service Line",
        description="Business service offering category",
        aliases=["product line", "offering", "LOB", "line of business"]
    ),
    EntityDefinition(
        id="region",
        name="Region",
        description="Geographic region",
        aliases=["geography", "geo", "territory", "area", "location"]
    ),
    EntityDefinition(
        id="segment",
        name="Segment",
        description="Customer or market segment",
        aliases=["market segment", "customer segment", "tier", "size"]
    ),
    EntityDefinition(
        id="rep",
        name="Sales Rep",
        description="Sales representative",
        aliases=["salesperson", "AE", "account executive", "seller"]
    ),
    EntityDefinition(
        id="team",
        name="Team",
        description="Organizational team",
        aliases=["squad", "group", "department", "unit"]
    ),
    EntityDefinition(
        id="product",
        name="Product",
        description="Product or SKU",
        aliases=["SKU", "item", "offering"]
    ),
    EntityDefinition(
        id="project",
        name="Project",
        description="Work project or initiative",
        aliases=["initiative", "program", "engagement"]
    ),
    EntityDefinition(
        id="service",
        name="Service",
        description="Technical service or microservice",
        aliases=["microservice", "application", "app", "system"]
    ),
    EntityDefinition(
        id="invoice",
        name="Invoice",
        description="Billing invoice",
        aliases=["bill", "statement"]
    ),
    EntityDefinition(
        id="stage",
        name="Stage",
        description="Pipeline or deal stage",
        aliases=["opportunity stage", "deal stage", "phase"]
    ),
    EntityDefinition(
        id="cohort",
        name="Cohort",
        description="Time-based customer cohort",
        aliases=["vintage", "signup cohort"]
    ),
    EntityDefinition(
        id="cost_center",
        name="Cost Center",
        description="Budget allocation unit",
        aliases=["budget", "department budget"]
    ),
    EntityDefinition(
        id="aging_bucket",
        name="Aging Bucket",
        description="AR aging time range",
        aliases=["aging range", "past due bucket"]
    ),
    EntityDefinition(
        id="priority",
        name="Priority",
        description="Work item priority level",
        aliases=["urgency", "severity"]
    ),
    EntityDefinition(
        id="work_type",
        name="Work Type",
        description="Category of work item",
        aliases=["issue type", "ticket type", "task type"]
    ),
    EntityDefinition(
        id="environment",
        name="Environment",
        description="Deployment environment",
        aliases=["env", "deploy target"]
    ),
    EntityDefinition(
        id="resource_type",
        name="Resource Type",
        description="Cloud resource category",
        aliases=["instance type", "service type"]
    ),
    EntityDefinition(
        id="sla_type",
        name="SLA Type",
        description="Service level agreement category",
        aliases=["SLA category"]
    ),
    EntityDefinition(
        id="slo_type",
        name="SLO Type",
        description="Service level objective category",
        aliases=["objective type"]
    ),
    EntityDefinition(
        id="severity",
        name="Severity",
        description="Incident severity level",
        aliases=["impact", "incident level"]
    ),
    EntityDefinition(
        id="category",
        name="Category",
        description="Generic category dimension",
        aliases=["type", "class"]
    ),
]


DEFAULT_PERSONA_CONCEPTS = {
    "cfo": ["arr", "mrr", "revenue", "services_revenue", "ar", "dso", "burn_rate", "gross_margin"],
    "cro": ["pipeline", "win_rate", "churn_rate", "nrr", "revenue", "arr"],
    "coo": ["throughput", "cycle_time", "sla_compliance"],
    "cto": ["deploy_frequency", "mttr", "uptime", "slo_attainment", "cloud_spend"]
}


DEMO_BINDINGS: List[BindingSummary] = [
    BindingSummary(
        source_system="Salesforce CRM",
        canonical_event="deal_won",
        quality_score=0.95,
        freshness_score=0.98,
        dims_coverage={"customer": True, "rep": True, "region": True, "segment": True}
    ),
    BindingSummary(
        source_system="NetSuite ERP",
        canonical_event="revenue_recognized",
        quality_score=0.92,
        freshness_score=0.95,
        dims_coverage={"customer": True, "service_line": True, "region": False}
    ),
    BindingSummary(
        source_system="NetSuite ERP",
        canonical_event="invoice_posted",
        quality_score=0.90,
        freshness_score=0.95,
        dims_coverage={"customer": True, "invoice": True, "aging_bucket": True}
    ),
    BindingSummary(
        source_system="Chargebee",
        canonical_event="subscription_started",
        quality_score=0.88,
        freshness_score=0.92,
        dims_coverage={"customer": True, "product": True, "segment": True}
    ),
    BindingSummary(
        source_system="Jira",
        canonical_event="work_item_completed",
        quality_score=0.85,
        freshness_score=0.90,
        dims_coverage={"team": True, "project": True, "work_type": True, "priority": True}
    ),
    BindingSummary(
        source_system="GitHub Actions",
        canonical_event="deployment_completed",
        quality_score=0.90,
        freshness_score=0.98,
        dims_coverage={"service": True, "team": True, "environment": True}
    ),
    BindingSummary(
        source_system="PagerDuty",
        canonical_event="incident_resolved",
        quality_score=0.88,
        freshness_score=0.95,
        dims_coverage={"service": True, "team": True, "severity": True}
    ),
    BindingSummary(
        source_system="AWS Cost Explorer",
        canonical_event="cloud_cost_incurred",
        quality_score=0.92,
        freshness_score=0.85,
        dims_coverage={"service": True, "team": True, "resource_type": True, "environment": True}
    ),
]

FARM_BINDINGS: List[BindingSummary] = []


def build_metric_entity_matrix() -> Dict[str, List[str]]:
    """Build matrix of metric → valid dimensions."""
    return {m.id: m.allowed_dims for m in PUBLISHED_METRICS}


def get_bindings_for_mode(data_mode: str) -> List[BindingSummary]:
    """Get bindings appropriate for the current mode."""
    if data_mode == "Demo":
        return DEMO_BINDINGS
    else:
        return FARM_BINDINGS


def resolve_metric(query: str) -> Optional[MetricDefinition]:
    """Resolve a query string to a canonical metric."""
    query_lower = query.lower().strip()
    
    for metric in PUBLISHED_METRICS:
        if query_lower == metric.id:
            return metric
        if query_lower in [a.lower() for a in metric.aliases]:
            return metric
    
    return None


def resolve_entity(query: str) -> Optional[EntityDefinition]:
    """Resolve a query string to a canonical entity."""
    query_lower = query.lower().strip()
    
    for entity in PUBLISHED_ENTITIES:
        if query_lower == entity.id:
            return entity
        if query_lower in [a.lower() for a in entity.aliases]:
            return entity
    
    return None


def get_semantic_export(tenant_id: str = "default") -> SemanticExport:
    """Build the full semantic export payload reflecting current DCL mode."""
    from backend.core.mode_state import get_current_mode
    
    current_mode = get_current_mode()
    
    mode_info = ModeInfo(
        data_mode=current_mode.data_mode,
        run_mode=current_mode.run_mode,
        last_updated=current_mode.last_updated
    )
    
    bindings = get_bindings_for_mode(current_mode.data_mode)
    
    return SemanticExport(
        version="1.0.0",
        tenant_id=tenant_id,
        mode=mode_info,
        metrics=PUBLISHED_METRICS,
        entities=PUBLISHED_ENTITIES,
        persona_concepts=DEFAULT_PERSONA_CONCEPTS,
        bindings=bindings,
        metric_entity_matrix=build_metric_entity_matrix()
    )
