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
))


def get_definition(definition_id: str) -> Definition | None:
    return DEFINITIONS.get(definition_id)


def list_definitions() -> list[Definition]:
    return list(DEFINITIONS.values())
