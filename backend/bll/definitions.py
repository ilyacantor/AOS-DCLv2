"""
BLL Definition Seeds - Pre-configured definitions for FinOps and AOD use cases.
"""
from datetime import datetime
from .models import (
    Definition, DefinitionCategory, ColumnSchema, 
    SourceReference, JoinSpec, FilterSpec
)


DEFINITIONS: dict[str, Definition] = {}


def _register(d: Definition) -> Definition:
    DEFINITIONS[d.definition_id] = d
    return d


_register(Definition(
    definition_id="finops.saas_spend",
    name="SaaS Spend Summary",
    description="Total cloud/SaaS spending aggregated by vendor and service category",
    category=DefinitionCategory.FINOPS,
    version="1.0.0",
    output_schema=[
        ColumnSchema(name="vendor_name", dtype="string", description="Cloud vendor name"),
        ColumnSchema(name="service_category", dtype="string", description="Service category (EC2, S3, RDS, etc.)"),
        ColumnSchema(name="total_spend", dtype="float", description="Total spend in USD"),
        ColumnSchema(name="transaction_count", dtype="integer", description="Number of transactions"),
        ColumnSchema(name="avg_monthly_cost", dtype="float", description="Average monthly cost"),
    ],
    sources=[
        SourceReference(source_id="netsuite", table_id="cloud_spend",
                       columns=["VendorName", "ServiceCategory", "Monthly_Cost"]),
        SourceReference(source_id="sap", table_id="cloud_invoices",
                       columns=["VENDOR_CODE", "ServiceCategory", "Monthly_Cost"]),
    ],
    dimensions=["vendor_name", "service_category"],
    metrics=["total_spend", "transaction_count", "avg_monthly_cost"],
    keywords=["saas spend", "cloud spend", "software spend", "vendor spend",
              "total spend", "spending by vendor", "cloud costs", "saas costs",
              "how much are we spending", "current spend", "spend summary"],
))


_register(Definition(
    definition_id="finops.top_vendor_deltas_mom",
    name="Top Vendor Month-over-Month Deltas",
    description="Identifies vendors with largest cost changes compared to previous month",
    category=DefinitionCategory.FINOPS,
    version="1.0.0",
    output_schema=[
        ColumnSchema(name="vendor_name", dtype="string", description="Vendor name"),
        ColumnSchema(name="current_month_spend", dtype="float", description="Current month total"),
        ColumnSchema(name="previous_month_spend", dtype="float", description="Previous month total"),
        ColumnSchema(name="delta_absolute", dtype="float", description="Absolute change"),
        ColumnSchema(name="delta_percent", dtype="float", description="Percentage change"),
    ],
    sources=[
        SourceReference(source_id="snowflake", table_id="aws_costs",
                       columns=["BILLING_PERIOD", "SERVICE_CATEGORY", "MONTHLY_COST"]),
    ],
    dimensions=["vendor_name"],
    metrics=["current_month_spend", "previous_month_spend", "delta_absolute", "delta_percent"],
    keywords=["vendor delta", "month over month", "month-over-month", "mom",
              "cost change", "spending change", "revenue change", "change month over month",
              "vendor changes", "cost delta", "spend delta", "what changed in spend",
              "cost changes over time", "spending delta", "vendor cost changes",
              "month over month changes", "cost difference", "spending difference",
              "what costs changed", "spend variance", "cost variance",
              "how did revenue change", "how did costs change", "how did spending change"],
))


_register(Definition(
    definition_id="finops.unallocated_spend",
    name="Unallocated Cloud Spend",
    description="Cloud spend not assigned to a cost center or project",
    category=DefinitionCategory.FINOPS,
    version="1.0.0",
    output_schema=[
        ColumnSchema(name="resource_id", dtype="string", description="Resource identifier"),
        ColumnSchema(name="service", dtype="string", description="Cloud service"),
        ColumnSchema(name="monthly_cost", dtype="float", description="Monthly cost"),
        ColumnSchema(name="region", dtype="string", description="AWS region"),
        ColumnSchema(name="owner", dtype="string", nullable=True, description="Owner (if known)"),
    ],
    sources=[
        SourceReference(source_id="snowflake", table_id="aws_costs",
                       columns=["RESOURCE_ID", "SERVICE_CATEGORY", "MONTHLY_COST", "REGION"]),
        SourceReference(source_id="snowflake", table_id="aws_resources",
                       columns=["RESOURCE_ID", "COST_CENTER", "OWNER", "PROJECT"]),
    ],
    joins=[
        JoinSpec(left_table="aws_costs", right_table="aws_resources",
                left_key="RESOURCE_ID", right_key="RESOURCE_ID", join_type="left"),
    ],
    default_filters=[
        FilterSpec(column="COST_CENTER", operator="is_null", value=None),
    ],
    dimensions=["service", "region"],
    metrics=["monthly_cost"],
    keywords=["unallocated spend", "untagged resources", "untagged spend", "orphan spend",
              "unassigned cost", "missing tags", "no cost center", "spend without tags",
              "unassigned spend", "spend missing cost center"],
))


_register(Definition(
    definition_id="aod.findings_by_severity",
    name="Findings by Severity",
    description="Security/compliance findings grouped by severity level",
    category=DefinitionCategory.AOD,
    version="1.0.0",
    output_schema=[
        ColumnSchema(name="severity", dtype="string", description="Severity level (critical, high, medium, low)"),
        ColumnSchema(name="finding_count", dtype="integer", description="Number of findings"),
        ColumnSchema(name="affected_resources", dtype="integer", description="Unique resources affected"),
        ColumnSchema(name="compliance_level", dtype="string", description="Compliance category"),
    ],
    sources=[
        SourceReference(source_id="snowflake", table_id="aws_resources",
                       columns=["RESOURCE_ID", "COMPLIANCE_LEVEL", "ENVIRONMENT"]),
        SourceReference(source_id="supabase", table_id="account_health",
                       columns=["account_id", "risk_level", "health_score"]),
    ],
    dimensions=["severity", "compliance_level"],
    metrics=["finding_count", "affected_resources"],
    keywords=["findings", "security findings", "compliance findings", "severity",
              "critical findings", "high severity", "vulnerabilities", "issues"],
))


_register(Definition(
    definition_id="aod.identity_gap_financially_anchored",
    name="Identity Gap - Financially Anchored",
    description="Resources with missing or incomplete ownership tied to financial impact",
    category=DefinitionCategory.AOD,
    version="1.0.0",
    output_schema=[
        ColumnSchema(name="resource_id", dtype="string", description="Resource identifier"),
        ColumnSchema(name="service", dtype="string", description="Cloud service type"),
        ColumnSchema(name="monthly_cost", dtype="float", description="Monthly cost impact"),
        ColumnSchema(name="owner", dtype="string", nullable=True, description="Current owner (may be null)"),
        ColumnSchema(name="project", dtype="string", nullable=True, description="Project assignment"),
        ColumnSchema(name="gap_type", dtype="string", description="Type of identity gap"),
    ],
    sources=[
        SourceReference(source_id="snowflake", table_id="aws_resources",
                       columns=["RESOURCE_ID", "SERVICE", "OWNER", "PROJECT", "COST_CENTER"]),
        SourceReference(source_id="snowflake", table_id="aws_costs",
                       columns=["RESOURCE_ID", "MONTHLY_COST"]),
    ],
    joins=[
        JoinSpec(left_table="aws_resources", right_table="aws_costs",
                left_key="RESOURCE_ID", right_key="RESOURCE_ID", join_type="inner"),
    ],
    dimensions=["service", "gap_type"],
    metrics=["monthly_cost"],
    keywords=["identity gap", "ownership gap", "unowned resources", "no owner",
              "missing owner", "orphan resources", "resources without owner",
              "who owns this", "ownership missing", "resource ownership"],
))


_register(Definition(
    definition_id="aod.zombies_overview",
    name="Zombie Resources Overview",
    description="Idle or underutilized resources still incurring costs",
    category=DefinitionCategory.AOD,
    version="1.0.0",
    output_schema=[
        ColumnSchema(name="resource_id", dtype="string", description="Resource identifier"),
        ColumnSchema(name="resource_type", dtype="string", description="Type of resource"),
        ColumnSchema(name="service", dtype="string", description="Cloud service"),
        ColumnSchema(name="instance_state", dtype="string", description="Current state"),
        ColumnSchema(name="monthly_cost", dtype="float", description="Monthly cost"),
        ColumnSchema(name="last_activity", dtype="string", nullable=True, description="Last known activity"),
        ColumnSchema(name="days_idle", dtype="integer", description="Days since last activity"),
    ],
    sources=[
        SourceReference(source_id="snowflake", table_id="aws_resources",
                       columns=["RESOURCE_ID", "RESOURCE_TYPE", "SERVICE", "INSTANCE_STATE", "LAUNCH_TIME"]),
        SourceReference(source_id="snowflake", table_id="aws_costs",
                       columns=["RESOURCE_ID", "MONTHLY_COST"]),
        SourceReference(source_id="legacy_sql", table_id="usage_metrics",
                       columns=["ResourceID", "MetricValue", "Timestamp"]),
    ],
    joins=[
        JoinSpec(left_table="aws_resources", right_table="aws_costs",
                left_key="RESOURCE_ID", right_key="RESOURCE_ID", join_type="left"),
    ],
    dimensions=["resource_type", "service", "instance_state"],
    metrics=["monthly_cost", "days_idle"],
    keywords=["zombie", "zombies", "idle resources", "unused resources", "wasted resources",
              "underutilized", "stopped instances", "idle", "not used", "zombie resources",
              "idle spend", "wasted cloud spend", "resources not being used"],
))


_register(Definition(
    definition_id="finops.arr",
    name="Annual Recurring Revenue",
    description="Total annual recurring revenue (ARR) from subscription contracts, deals, and opportunities. Use this for questions about current ARR, revenue, MRR, bookings, or contract value.",
    category=DefinitionCategory.FINOPS,
    version="1.0.0",
    output_schema=[
        ColumnSchema(name="source", dtype="string", description="CRM source system"),
        ColumnSchema(name="deal_name", dtype="string", description="Deal or opportunity name"),
        ColumnSchema(name="amount", dtype="float", description="Deal amount"),
        ColumnSchema(name="stage", dtype="string", description="Deal stage"),
        ColumnSchema(name="close_date", dtype="string", description="Expected close date"),
        ColumnSchema(name="arr_contribution", dtype="float", description="ARR contribution"),
    ],
    sources=[
        SourceReference(source_id="hubspot", table_id="deals",
                       columns=["DealName", "Amount", "Stage", "CloseDate"]),
        SourceReference(source_id="salesforce", table_id="opportunity",
                       columns=["Name", "Amount", "CloseDate"]),
        SourceReference(source_id="dynamics", table_id="opportunities",
                       columns=["OpportunityName", "Amount", "CloseDate"]),
    ],
    dimensions=["source", "stage"],
    metrics=["amount", "arr_contribution"],
    keywords=["arr", "annual recurring revenue", "revenue", "mrr", "monthly recurring revenue", 
              "current arr", "total arr", "bookings", "contract value", "acv", "tcv", 
              "subscription revenue", "recurring", "what is our arr"],
))


_register(Definition(
    definition_id="finops.burn_rate",
    name="Burn Rate Analysis",
    description="Monthly cash burn rate and runway analysis. Use this for questions about burn rate, runway, cash consumption, or monthly spending trends.",
    category=DefinitionCategory.FINOPS,
    version="1.0.0",
    output_schema=[
        ColumnSchema(name="month", dtype="string", description="Billing month"),
        ColumnSchema(name="total_spend", dtype="float", description="Total monthly spend"),
        ColumnSchema(name="category", dtype="string", description="Spend category"),
        ColumnSchema(name="cost_center", dtype="string", description="Cost center"),
    ],
    sources=[
        SourceReference(source_id="netsuite", table_id="cloud_spend",
                       columns=["Monthly_Cost", "CostCenter", "PurchaseDate"]),
        SourceReference(source_id="snowflake", table_id="aws_costs",
                       columns=["MONTHLY_COST", "BILLING_PERIOD", "SERVICE_CATEGORY"]),
    ],
    dimensions=["month", "category", "cost_center"],
    metrics=["total_spend"],
    keywords=["burn rate", "burn", "runway", "cash burn", "monthly burn", "consumption", 
              "spending rate", "current burn rate", "what is our burn rate"],
))


_register(Definition(
    definition_id="crm.pipeline",
    name="Sales Pipeline",
    description="Current sales pipeline with deal stages, amounts, and forecasted revenue",
    category=DefinitionCategory.FINOPS,
    version="1.0.0",
    output_schema=[
        ColumnSchema(name="deal_id", dtype="string", description="Deal identifier"),
        ColumnSchema(name="deal_name", dtype="string", description="Deal name"),
        ColumnSchema(name="amount", dtype="float", description="Deal value"),
        ColumnSchema(name="stage", dtype="string", description="Current stage"),
        ColumnSchema(name="close_date", dtype="string", description="Expected close date"),
        ColumnSchema(name="pipeline", dtype="string", description="Pipeline name"),
    ],
    sources=[
        SourceReference(source_id="hubspot", table_id="deals",
                       columns=["DealID", "DealName", "Amount", "Stage", "CloseDate", "Pipeline"]),
    ],
    dimensions=["stage", "pipeline"],
    metrics=["amount"],
    keywords=["pipeline", "sales pipeline", "deal pipeline", "opportunities",
              "deals", "forecast", "sales forecast", "pipeline value"],
))


_register(Definition(
    definition_id="crm.top_customers",
    name="Top Customers by Revenue",
    description="Highest revenue customers ranked by annual revenue",
    category=DefinitionCategory.FINOPS,
    version="1.0.0",
    output_schema=[
        ColumnSchema(name="account_name", dtype="string", description="Account/customer name"),
        ColumnSchema(name="industry", dtype="string", description="Industry vertical"),
        ColumnSchema(name="annual_revenue", dtype="float", description="Annual revenue"),
        ColumnSchema(name="employee_count", dtype="integer", description="Number of employees"),
    ],
    sources=[
        SourceReference(source_id="salesforce", table_id="account",
                       columns=["Name", "Industry", "AnnualRevenue", "NumberOfEmployees"]),
        SourceReference(source_id="dynamics", table_id="accounts",
                       columns=["AccountName", "Industry", "AnnualRevenue", "EmployeeCount"]),
    ],
    dimensions=["industry"],
    metrics=["annual_revenue", "employee_count"],
    keywords=["top customers", "biggest customers", "largest customers", "customers by revenue",
              "top accounts", "best customers", "high value customers", "customer revenue",
              "largest accounts", "biggest accounts", "our largest customers",
              "who are our top customers", "customer list by revenue"],
))


# =============================================================================
# SRE / Platform Metrics (DORA, SLO, Incidents)
# =============================================================================

_register(Definition(
    definition_id="infra.slo_attainment",
    name="SLO Attainment",
    description="Service Level Objective attainment percentage across services",
    category=DefinitionCategory.INFRA,
    version="1.0.0",
    output_schema=[
        ColumnSchema(name="service_name", dtype="string", description="Service name"),
        ColumnSchema(name="slo_name", dtype="string", description="SLO name"),
        ColumnSchema(name="target_percent", dtype="float", description="Target percentage"),
        ColumnSchema(name="actual_percent", dtype="float", description="Actual attainment"),
        ColumnSchema(name="error_budget_remaining", dtype="float", description="Remaining error budget"),
    ],
    sources=[
        SourceReference(source_id="datadog", table_id="slo_metrics",
                       columns=["service", "slo_name", "target", "actual", "error_budget"]),
    ],
    dimensions=["service_name", "slo_name"],
    metrics=["target_percent", "actual_percent", "error_budget_remaining"],
    keywords=["slo", "slo attainment", "service level objective", "service level",
              "uptime", "availability", "reliability", "error budget", "sla",
              "how is our slo", "slo trending", "slo performance"],
))


_register(Definition(
    definition_id="infra.deploy_frequency",
    name="Deployment Frequency",
    description="DORA metric: How often code is deployed to production",
    category=DefinitionCategory.INFRA,
    version="1.0.0",
    output_schema=[
        ColumnSchema(name="service_name", dtype="string", description="Service name"),
        ColumnSchema(name="deploy_count", dtype="integer", description="Number of deployments"),
        ColumnSchema(name="period", dtype="string", description="Time period"),
        ColumnSchema(name="team", dtype="string", description="Team name"),
    ],
    sources=[
        SourceReference(source_id="datadog", table_id="dora_metrics",
                       columns=["service", "deploy_count", "period", "team"]),
    ],
    dimensions=["service_name", "period", "team"],
    metrics=["deploy_count"],
    keywords=["deploy frequency", "deployment frequency", "dora", "deployments",
              "how often deploy", "release frequency", "deploys per day", "cd metrics"],
))


_register(Definition(
    definition_id="infra.lead_time",
    name="Lead Time for Changes",
    description="DORA metric: Time from code commit to production deployment",
    category=DefinitionCategory.INFRA,
    version="1.0.0",
    output_schema=[
        ColumnSchema(name="service_name", dtype="string", description="Service name"),
        ColumnSchema(name="lead_time_hours", dtype="float", description="Lead time in hours"),
        ColumnSchema(name="team", dtype="string", description="Team name"),
        ColumnSchema(name="period", dtype="string", description="Time period"),
    ],
    sources=[
        SourceReference(source_id="datadog", table_id="dora_metrics",
                       columns=["service", "lead_time_hours", "team", "period"]),
    ],
    dimensions=["service_name", "team"],
    metrics=["lead_time_hours"],
    keywords=["lead time", "lead time for changes", "dora", "time to deploy",
              "commit to production", "cycle time", "deployment time"],
))


_register(Definition(
    definition_id="infra.change_failure_rate",
    name="Change Failure Rate",
    description="DORA metric: Percentage of deployments causing failures",
    category=DefinitionCategory.INFRA,
    version="1.0.0",
    output_schema=[
        ColumnSchema(name="service_name", dtype="string", description="Service name"),
        ColumnSchema(name="deploy_count", dtype="integer", description="Total deployments"),
        ColumnSchema(name="change_failure_rate", dtype="float", description="Failure rate percentage"),
        ColumnSchema(name="team", dtype="string", description="Team name"),
    ],
    sources=[
        SourceReference(source_id="datadog", table_id="dora_metrics",
                       columns=["service", "deploy_count", "change_failure_rate", "team"]),
    ],
    dimensions=["service_name", "team"],
    metrics=["deploy_count", "change_failure_rate"],
    keywords=["change failure rate", "failure rate", "dora", "failed deployments",
              "rollbacks", "deployment failures", "deploy failures"],
))


_register(Definition(
    definition_id="infra.mttr",
    name="Mean Time to Recovery",
    description="DORA metric: Average time to recover from failures",
    category=DefinitionCategory.INFRA,
    version="1.0.0",
    output_schema=[
        ColumnSchema(name="service", dtype="string", description="Service name"),
        ColumnSchema(name="severity", dtype="string", description="Incident severity"),
        ColumnSchema(name="mttr_minutes", dtype="float", description="MTTR in minutes"),
        ColumnSchema(name="title", dtype="string", description="Incident title"),
        ColumnSchema(name="team", dtype="string", description="Team name"),
    ],
    sources=[
        SourceReference(source_id="pagerduty", table_id="incidents",
                       columns=["service", "severity", "mttr_minutes", "title", "team"]),
    ],
    dimensions=["service", "severity", "team"],
    metrics=["mttr_minutes"],
    keywords=["mttr", "mean time to recovery", "recovery time", "dora",
              "incident recovery", "time to resolve", "resolution time",
              "how long to recover", "average recovery time", "time to fix incidents"],
))


_register(Definition(
    definition_id="infra.incidents",
    name="Incident Summary",
    description="Incident count and severity breakdown across services",
    category=DefinitionCategory.INFRA,
    version="1.0.0",
    output_schema=[
        ColumnSchema(name="incident_id", dtype="string", description="Incident ID"),
        ColumnSchema(name="service", dtype="string", description="Service name"),
        ColumnSchema(name="severity", dtype="string", description="Incident severity"),
        ColumnSchema(name="status", dtype="string", description="Incident status"),
        ColumnSchema(name="title", dtype="string", description="Incident title"),
        ColumnSchema(name="team", dtype="string", description="Team name"),
    ],
    sources=[
        SourceReference(source_id="pagerduty", table_id="incidents",
                       columns=["incident_id", "service", "severity", "status", "title", "team"]),
    ],
    dimensions=["service", "severity", "status", "team"],
    metrics=[],
    keywords=["incidents", "outages", "pages", "alerts", "incident count",
              "sev1", "sev2", "critical incidents", "production incidents"],
))


def get_definition(definition_id: str) -> Definition | None:
    return DEFINITIONS.get(definition_id)


def list_definitions() -> list[Definition]:
    return list(DEFINITIONS.values())
