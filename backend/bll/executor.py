"""
BLL Demo Executor - Executes definitions against demo datasets using Pandas.
No persistence, in-process only.

ARCHITECTURE NOTE:
This executor is for DEMO MODE ONLY. It directly loads CSV files to simulate
the response that would come from Fabric Planes in production.

In production (Farm mode), the BLL contract endpoints would:
1. Use DCL's FabricPointerBuffer to get pointer references
2. JIT-fetch actual data from Fabric Planes (Snowflake, Kafka, etc.)
3. Never store payloads in DCL

The demo executor provides the same API contract surface so BLL consumers
can develop and test against stable schemas without needing live Fabric Planes.
"""
import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from .models import (
    Definition, ExecuteRequest, ExecuteResponse, ExecuteMetadata,
    QualityMetrics, LineageReference, ProofResponse, ProofBreadcrumb,
    ColumnSchema, FilterSpec, ComputedSummary, OrderBySpec
)
from .definitions import get_definition


# Default dataset - Farm if FARM_SCENARIO_ID is set, otherwise demo9
_FARM_SCENARIO = os.environ.get("FARM_SCENARIO_ID")
DATASET_ID = f"farm:{_FARM_SCENARIO}" if _FARM_SCENARIO else os.environ.get("DCL_DATASET_ID", "demo9")


def _load_manifest(dataset_id: str) -> dict:
    manifest_path = Path(f"dcl/demo/datasets/{dataset_id}/manifest.json")
    if not manifest_path.exists():
        raise FileNotFoundError(f"Dataset manifest not found: {dataset_id}")
    with open(manifest_path) as f:
        return json.load(f)


def _get_table_path(manifest: dict, source_id: str, table_id: str) -> str | None:
    for source in manifest.get("sources", []):
        if source["source_id"] == source_id:
            for table in source.get("tables", []):
                if table["table_id"] == table_id:
                    return table["file"]
    return None


def _load_table(file_path: str) -> pd.DataFrame:
    if not os.path.exists(file_path):
        return pd.DataFrame()
    return pd.read_csv(file_path)


def _format_currency(amount: float) -> str:
    """Format a number as currency."""
    if amount >= 1_000_000:
        return f"${amount/1_000_000:,.2f}M"
    elif amount >= 1_000:
        return f"${amount/1_000:,.1f}K"
    else:
        return f"${amount:,.2f}"


def _compute_summary(
    df: pd.DataFrame,
    definition: Definition,
    full_population_df: pd.DataFrame | None = None,
    applied_limit: int | None = None,
    is_aggregate_query: bool = False
) -> ComputedSummary:
    """
    Compute aggregations and generate human-readable answer based on definition type.

    Quality guidelines for top-N queries:
    - Compute totals on FULL POPULATION, not just top-N
    - Show top-N share of total (e.g., "top 5 represent 45% of total revenue")
    - Include 1-sentence interpretation
    - Add caveat if context is missing (demo data, limited scope)

    Args:
        df: The result rows (possibly limited to top-N)
        definition: The BLL definition being executed
        full_population_df: The full dataset BEFORE limit was applied (for share calculation)
        applied_limit: The limit that was applied (e.g., 5 for "top 5")
        is_aggregate_query: True if this is an AGGREGATE definition (returns totals, not ranked lists)
    """
    aggregations: dict[str, Any] = {}
    answer = ""
    limitations: list[str] = []

    # Use full population for totals if available
    pop_df = full_population_df if full_population_df is not None else df
    # AGGREGATE queries should NEVER be treated as top-N, regardless of limit
    is_top_n = (
        not is_aggregate_query and
        applied_limit is not None and
        len(df) <= applied_limit and
        full_population_df is not None
    )

    amount_cols = []
    for c in df.columns:
        if any(x in c.lower() for x in ['amount', 'revenue', 'monthly_cost', 'annual', 'net_value']):
            if df[c].dtype in ['int64', 'float64'] or pd.api.types.is_numeric_dtype(df[c]):
                amount_cols.append(c)

    defn_id = definition.definition_id.lower()
    row_count = len(df)

    try:
        if 'arr' in defn_id or 'revenue' in defn_id:
            pop_count = len(pop_df)
            if amount_cols:
                pop_total = pd.to_numeric(pop_df[amount_cols[0]], errors='coerce').sum()
                shown_total = pd.to_numeric(df[amount_cols[0]], errors='coerce').sum()
                aggregations['population_total'] = float(pop_total)
                aggregations['population_count'] = pop_count
                aggregations['shown_total'] = float(shown_total)
                aggregations['deal_count'] = row_count

                share_pct = (shown_total / pop_total * 100) if pop_total > 0 else 0
                aggregations['share_of_total_pct'] = float(share_pct)

                if is_top_n:
                    answer = f"Top {row_count} revenue items: {_format_currency(shown_total)} ({share_pct:.0f}% of {_format_currency(pop_total)} total ARR)."
                    if share_pct > 50:
                        answer += f" Revenue is concentrated in top {row_count} accounts."
                else:
                    answer = f"Your current ARR is {_format_currency(pop_total)} across {pop_count} deals/opportunities."

                if pop_count < 10:
                    limitations.append(f"Based on {pop_count} records in demo data; production may differ")
            else:
                aggregations['row_count'] = row_count
                answer = f"Found {row_count} revenue records."
                limitations.append("No amount column found for aggregation")

        elif 'delta' in defn_id or 'mom' in defn_id or 'change' in defn_id:
            pop_count = len(pop_df)
            aggregations['row_count'] = row_count
            aggregations['population_count'] = pop_count

            delta_cols = [c for c in pop_df.columns if 'delta' in c.lower()]
            if delta_cols:
                pop_delta_sum = pd.to_numeric(pop_df[delta_cols[0]], errors='coerce').sum()
                shown_delta_sum = pd.to_numeric(df[delta_cols[0]], errors='coerce').sum()
                aggregations['population_total'] = float(abs(pop_delta_sum))
                aggregations['shown_total'] = float(abs(shown_delta_sum))

                share_pct = (abs(shown_delta_sum) / abs(pop_delta_sum) * 100) if pop_delta_sum != 0 else 100
                aggregations['share_of_total_pct'] = float(share_pct)

                direction = "increased" if pop_delta_sum > 0 else "decreased" if pop_delta_sum < 0 else "unchanged"
                if is_top_n:
                    answer = f"Top {row_count} changes: {_format_currency(abs(shown_delta_sum))} ({share_pct:.0f}% of total change). Net: costs {direction} by {_format_currency(abs(pop_delta_sum))}."
                else:
                    answer = f"Month-over-month: costs {direction} by {_format_currency(abs(pop_delta_sum))} across {pop_count} vendors."
            elif amount_cols:
                pop_total = pd.to_numeric(pop_df[amount_cols[0]], errors='coerce').sum()
                shown_total = pd.to_numeric(df[amount_cols[0]], errors='coerce').sum()
                aggregations['population_total'] = float(pop_total)
                aggregations['shown_total'] = float(shown_total)

                share_pct = (shown_total / pop_total * 100) if pop_total > 0 else 100
                aggregations['share_of_total_pct'] = float(share_pct)

                if is_top_n:
                    answer = f"Top {row_count} vendors: {_format_currency(shown_total)} ({share_pct:.0f}% of {_format_currency(pop_total)} total spend)."
                else:
                    answer = f"Month-over-month analysis: {pop_count} vendors with {_format_currency(pop_total)} total spend."
            else:
                aggregations['share_of_total_pct'] = 100.0  # Default
                answer = f"Month-over-month analysis: {pop_count} records."
                limitations.append("Delta columns not available; showing raw records")

        elif 'burn' in defn_id:
            pop_count = len(pop_df)
            if amount_cols:
                pop_total = pd.to_numeric(pop_df[amount_cols[0]], errors='coerce').sum()
                shown_total = pd.to_numeric(df[amount_cols[0]], errors='coerce').sum()
                monthly_avg = pop_total / 12 if pop_total > 0 else 0
                aggregations['population_total'] = float(pop_total)
                aggregations['shown_total'] = float(shown_total)
                aggregations['monthly_avg'] = float(monthly_avg)
                aggregations['population_count'] = pop_count

                share_pct = (shown_total / pop_total * 100) if pop_total > 0 else 0
                aggregations['share_of_total_pct'] = float(share_pct)

                if is_top_n:
                    answer = f"Top {row_count} burn items: {_format_currency(shown_total)} ({share_pct:.0f}% of {_format_currency(pop_total)} total annual spend)."
                    answer += f" Monthly burn rate: ~{_format_currency(monthly_avg)}/month."
                else:
                    answer = f"Burn rate: approximately {_format_currency(monthly_avg)}/month ({_format_currency(pop_total)} annualized)."
                limitations.append("Burn rate estimated from available spend data; actual runway depends on cash reserves")
            else:
                answer = f"Found {row_count} cost records."
                limitations.append("No cost column found for burn calculation")

        elif 'unallocated' in defn_id:
            pop_count = len(pop_df)
            aggregations['resource_count'] = row_count
            aggregations['population_count'] = pop_count
            if amount_cols:
                pop_total = pd.to_numeric(pop_df[amount_cols[0]], errors='coerce').sum()
                shown_total = pd.to_numeric(df[amount_cols[0]], errors='coerce').sum()
                aggregations['population_unallocated_spend'] = float(pop_total)
                aggregations['shown_unallocated_spend'] = float(shown_total)

                share_pct = (shown_total / pop_total * 100) if pop_total > 0 else 0
                aggregations['share_of_total_pct'] = float(share_pct)

                if is_top_n:
                    answer = f"Top {row_count} unallocated items: {_format_currency(shown_total)} ({share_pct:.0f}% of {_format_currency(pop_total)} total unallocated)."
                    answer += f" {pop_count} resources total need cost center assignment."
                else:
                    answer = f"Unallocated spend: {_format_currency(pop_total)} across {pop_count} resources without cost center assignment."
                    if pop_total > 10000:
                        answer += " Consider tagging these resources to improve cost attribution."
            else:
                answer = f"Found {row_count} unallocated resources out of {pop_count} total."
            limitations.append("Definition: Resources lacking cost center or project tags")

        elif 'spend' in defn_id or 'cost' in defn_id:
            pop_count = len(pop_df)
            if amount_cols:
                pop_total = pd.to_numeric(pop_df[amount_cols[0]], errors='coerce').sum()
                shown_total = pd.to_numeric(df[amount_cols[0]], errors='coerce').sum()
                aggregations['population_total'] = float(pop_total)
                aggregations['population_count'] = pop_count
                aggregations['shown_total'] = float(shown_total)
                aggregations['transaction_count'] = row_count
                avg_per_txn = shown_total / row_count if row_count > 0 else 0

                share_pct = (shown_total / pop_total * 100) if pop_total > 0 else 0
                aggregations['share_of_total_pct'] = float(share_pct)

                if is_top_n:
                    answer = f"Top {row_count} spend items: {_format_currency(shown_total)} ({share_pct:.0f}% of {_format_currency(pop_total)} total spend)."
                    if share_pct > 50:
                        answer += f" Spend is concentrated in top items."
                    else:
                        answer += f" Spend is distributed across {pop_count} total items."
                else:
                    answer = f"Total spend: {_format_currency(pop_total)} across {pop_count} transactions (avg {_format_currency(avg_per_txn)} each)."
            else:
                answer = f"Found {row_count} spend records."
                limitations.append("No cost column found for aggregation")

        elif 'customer' in defn_id or 'account' in defn_id:
            aggregations['customer_count'] = row_count

            # Find name column
            name_cols = [c for c in df.columns if any(n in c.lower() for n in ['name', 'account', 'customer'])]
            name_col = name_cols[0] if name_cols else None

            if amount_cols:
                # Compute totals on FULL population
                pop_total = pd.to_numeric(pop_df[amount_cols[0]], errors='coerce').sum()
                pop_count = len(pop_df)
                aggregations['population_total'] = float(pop_total)
                aggregations['population_count'] = pop_count

                # Compute for displayed rows
                shown_total = pd.to_numeric(df[amount_cols[0]], errors='coerce').sum()
                avg_revenue = shown_total / row_count if row_count > 0 else 0
                aggregations['shown_total'] = float(shown_total)
                aggregations['avg_per_customer'] = float(avg_revenue)

                # Calculate share of total
                share_pct = (shown_total / pop_total * 100) if pop_total > 0 else 0
                aggregations['share_of_total_pct'] = float(share_pct)

                # Build answer with actual customer names for top-N
                if is_top_n and name_col:
                    # List the top N customers by name
                    customer_lines = []
                    for i, (_, row) in enumerate(df.head(row_count).iterrows(), 1):
                        cust_name = row.get(name_col, f"Customer {i}")
                        cust_rev = pd.to_numeric(row.get(amount_cols[0], 0), errors='coerce')
                        customer_lines.append(f"{i}. {cust_name}: {_format_currency(cust_rev)}")

                    answer = f"Top {row_count} customers by revenue:\n" + "\n".join(customer_lines)
                    answer += f"\n\nTotal: {_format_currency(shown_total)} ({share_pct:.0f}% of {_format_currency(pop_total)} portfolio)."

                    # Store customer list in aggregations for structured access
                    aggregations['top_customers'] = [
                        {"name": row.get(name_col), "revenue": float(pd.to_numeric(row.get(amount_cols[0], 0), errors='coerce'))}
                        for _, row in df.head(row_count).iterrows()
                    ]
                elif is_top_n:
                    # No name column - fall back to aggregate summary
                    answer = f"Top {row_count} customers represent {_format_currency(shown_total)} ({share_pct:.0f}% of {_format_currency(pop_total)} total portfolio)."
                    limitations.append("Customer name column not found in data")
                else:
                    answer = f"Total revenue: {_format_currency(pop_total)} across {pop_count} customers (avg {_format_currency(avg_revenue)} each)."

                # Concentration interpretation
                if share_pct > 50:
                    answer += f" High concentration: top {row_count} drive majority of revenue."
                elif share_pct > 25 and is_top_n:
                    answer += " Moderate concentration in top accounts."

                # Top customer concentration
                if row_count >= 1:
                    top_val = pd.to_numeric(df[amount_cols[0]], errors='coerce').iloc[0] if len(df) > 0 else 0
                    if pop_total > 0:
                        top_pct = (top_val / pop_total) * 100
                        aggregations['top_customer_pct'] = float(top_pct)
            else:
                answer = f"Found {row_count} customers."
                limitations.append("No revenue column for ranking impact")

            # Demo data caveat
            if pop_count < 20:
                limitations.append(f"Based on {pop_count} customers in demo dataset; production data may differ")

        elif 'pipeline' in defn_id or 'deal' in defn_id:
            aggregations['deal_count'] = row_count
            if amount_cols:
                # Full population totals
                pop_total = pd.to_numeric(pop_df[amount_cols[0]], errors='coerce').sum()
                pop_count = len(pop_df)
                aggregations['population_total'] = float(pop_total)
                aggregations['population_count'] = pop_count

                # Shown deals
                shown_total = pd.to_numeric(df[amount_cols[0]], errors='coerce').sum()
                avg_deal = shown_total / row_count if row_count > 0 else 0
                aggregations['shown_total'] = float(shown_total)
                aggregations['avg_deal_size'] = float(avg_deal)

                share_pct = (shown_total / pop_total * 100) if pop_total > 0 else 0
                aggregations['share_of_total_pct'] = float(share_pct)

                if is_top_n:
                    answer = f"Top {row_count} deals: {_format_currency(shown_total)} ({share_pct:.0f}% of {_format_currency(pop_total)} total pipeline)."
                    if share_pct > 50:
                        answer += f" Pipeline heavily weighted toward top opportunities."
                    else:
                        answer += f" Pipeline is diversified across {pop_count} total deals."
                else:
                    answer = f"Pipeline: {pop_count} deals worth {_format_currency(pop_total)} (avg deal: {_format_currency(avg_deal)})."

                # Stage breakdown if available
                if 'Stage' in df.columns or 'stage' in df.columns:
                    stage_col = 'Stage' if 'Stage' in df.columns else 'stage'
                    stages = df[stage_col].value_counts().to_dict()
                    aggregations['stage_breakdown'] = stages
            else:
                answer = f"Pipeline contains {row_count} deals."
                limitations.append("No amount column for pipeline value")

            if len(pop_df) < 20:
                limitations.append(f"Based on {len(pop_df)} deals in demo dataset")

        elif 'zombie' in defn_id or 'idle' in defn_id:
            pop_count = len(pop_df)
            aggregations['resource_count'] = row_count
            aggregations['population_count'] = pop_count
            if amount_cols:
                pop_total = pd.to_numeric(pop_df[amount_cols[0]], errors='coerce').sum()
                shown_total = pd.to_numeric(df[amount_cols[0]], errors='coerce').sum()
                aggregations['population_wasted_spend'] = float(pop_total)
                aggregations['shown_wasted_spend'] = float(shown_total)

                share_pct = (shown_total / pop_total * 100) if pop_total > 0 else 0
                aggregations['share_of_total_pct'] = float(share_pct)

                if is_top_n:
                    answer = f"Top {row_count} zombie resources: {_format_currency(shown_total)}/month ({share_pct:.0f}% of {_format_currency(pop_total)} total waste)."
                    answer += f" {pop_count} total idle resources identified."
                else:
                    answer = f"Zombie resources: {pop_count} idle instances costing {_format_currency(pop_total)}/month."
                answer += " Candidates for termination or rightsizing."
            else:
                answer = f"Found {row_count} zombie/idle resources out of {pop_count} total."
            limitations.append("Definition: Resources with no meaningful activity in the observation period")

        elif 'identity_gap' in defn_id or 'ownership' in defn_id:
            pop_count = len(pop_df)
            aggregations['resource_count'] = row_count
            aggregations['population_count'] = pop_count
            if amount_cols:
                pop_total = pd.to_numeric(pop_df[amount_cols[0]], errors='coerce').sum()
                shown_total = pd.to_numeric(df[amount_cols[0]], errors='coerce').sum()
                aggregations['population_unowned_spend'] = float(pop_total)
                aggregations['shown_unowned_spend'] = float(shown_total)

                share_pct = (shown_total / pop_total * 100) if pop_total > 0 else 0
                aggregations['share_of_total_pct'] = float(share_pct)

                if is_top_n:
                    answer = f"Top {row_count} ownership gaps: {_format_currency(shown_total)} ({share_pct:.0f}% of {_format_currency(pop_total)} total unowned spend)."
                    answer += f" {pop_count} resources total need owner assignment."
                else:
                    answer = f"Ownership gaps: {pop_count} resources without clear owners, representing {_format_currency(pop_total)} in spend."
            else:
                answer = f"Found {row_count} resources with ownership gaps out of {pop_count} total."
            limitations.append("Definition: Resources lacking owner tag or assignment")

        elif 'finding' in defn_id or 'security' in defn_id:
            pop_count = len(pop_df)
            aggregations['finding_count'] = row_count
            aggregations['population_count'] = pop_count
            aggregations['population_total'] = float(pop_count)  # For findings, count is the "total"
            aggregations['share_of_total_pct'] = float((row_count / pop_count * 100) if pop_count > 0 else 100)

            if 'severity' in pop_df.columns:
                pop_severity_counts = pop_df['severity'].value_counts().to_dict()
                aggregations['population_severity_breakdown'] = pop_severity_counts
                shown_severity_counts = df['severity'].value_counts().to_dict() if 'severity' in df.columns else {}
                aggregations['severity_breakdown'] = shown_severity_counts

                pop_critical = pop_severity_counts.get('critical', 0)
                pop_high = pop_severity_counts.get('high', 0)

                if is_top_n:
                    answer = f"Showing top {row_count} of {pop_count} findings ({pop_critical} critical, {pop_high} high across all)."
                    if pop_critical > 0:
                        answer += " Critical findings require immediate remediation."
                else:
                    if pop_critical > 0 or pop_high > 0:
                        answer = f"Security findings: {pop_count} total ({pop_critical} critical, {pop_high} high priority). Critical/high findings require immediate attention."
                    else:
                        answer = f"Security findings: {pop_count} total. No critical or high severity issues found."
            else:
                answer = f"Found {row_count} of {pop_count} security findings."
            limitations.append("Security findings reflect point-in-time scan results")

        # SLO Attainment - with interpretation
        elif 'slo' in defn_id:
            pop_count = len(pop_df)
            aggregations['service_count'] = row_count
            aggregations['population_count'] = pop_count
            aggregations['population_total'] = float(pop_count)  # For SLO, count is the "total"
            aggregations['share_of_total_pct'] = float((row_count / pop_count * 100) if pop_count > 0 else 100)

            avg_attainment = None
            if 'actual' in pop_df.columns:
                avg_attainment = pd.to_numeric(pop_df['actual'], errors='coerce').mean()
                aggregations['avg_attainment'] = float(avg_attainment)
            if 'status' in pop_df.columns:
                pop_status_counts = pop_df['status'].value_counts().to_dict()
                shown_status_counts = df['status'].value_counts().to_dict() if 'status' in df.columns else {}
                aggregations['population_status_breakdown'] = pop_status_counts
                aggregations['status_breakdown'] = shown_status_counts
                passing = pop_status_counts.get('passing', 0)
                at_risk = pop_status_counts.get('at_risk', 0)
                breached = pop_status_counts.get('breached', 0)

                # Interpretation
                if breached > 0:
                    health = "needs attention"
                elif at_risk > passing:
                    health = "trending down"
                else:
                    health = "healthy"

                if is_top_n:
                    answer = f"Showing {row_count} of {pop_count} services. Overall health ({health}): {passing} passing, {at_risk} at risk, {breached} breached."
                else:
                    answer = f"SLO health ({health}): {passing} passing, {at_risk} at risk, {breached} breached across {pop_count} services."
                if avg_attainment:
                    answer += f" Average attainment: {avg_attainment:.1f}%."
            else:
                answer = f"Tracking SLOs for {row_count} of {pop_count} services."
            limitations.append("SLO = Service Level Objective; target uptime/performance metric")

        # DORA Metrics - Deploy Frequency with benchmark
        elif 'deploy' in defn_id:
            aggregations['service_count'] = row_count
            if 'deploy_count' in df.columns:
                total_deploys = pd.to_numeric(df['deploy_count'], errors='coerce').sum()
                avg_deploys = pd.to_numeric(df['deploy_count'], errors='coerce').mean()
                aggregations['total_deploys'] = int(total_deploys)
                aggregations['avg_per_service'] = float(avg_deploys)

                # DORA benchmark interpretation (per 30 days)
                if avg_deploys >= 30:
                    tier = "Elite (daily+)"
                elif avg_deploys >= 4:
                    tier = "High (weekly)"
                elif avg_deploys >= 1:
                    tier = "Medium (monthly)"
                else:
                    tier = "Low (<monthly)"

                answer = f"Deployment frequency: {int(total_deploys)} total across {row_count} services. DORA tier: {tier} (avg {avg_deploys:.1f}/month per service)."
            else:
                answer = f"Tracking deployments for {row_count} services."
            limitations.append("DORA = DevOps Research and Assessment metrics")

        # DORA Metrics - Lead Time with benchmark
        elif 'lead_time' in defn_id:
            aggregations['service_count'] = row_count
            if 'lead_time_hours' in df.columns:
                avg_lead_time = pd.to_numeric(df['lead_time_hours'], errors='coerce').mean()
                aggregations['avg_lead_time_hours'] = float(avg_lead_time)

                # DORA benchmark
                if avg_lead_time < 1:
                    tier = "Elite (<1 hour)"
                elif avg_lead_time < 24:
                    tier = "High (<1 day)"
                elif avg_lead_time < 168:
                    tier = "Medium (<1 week)"
                else:
                    tier = "Low (>1 week)"

                answer = f"Lead time for changes: average {avg_lead_time:.1f} hours. DORA tier: {tier}."
            else:
                answer = f"Lead time data for {row_count} services."
            limitations.append("Lead time = time from commit to production")

        # DORA Metrics - Change Failure Rate with benchmark
        elif 'failure_rate' in defn_id:
            aggregations['service_count'] = row_count
            if 'change_failure_rate' in df.columns:
                avg_cfr = pd.to_numeric(df['change_failure_rate'], errors='coerce').mean() * 100
                aggregations['avg_failure_rate_pct'] = float(avg_cfr)

                # DORA benchmark
                if avg_cfr <= 5:
                    tier = "Elite (≤5%)"
                elif avg_cfr <= 10:
                    tier = "High (≤10%)"
                elif avg_cfr <= 15:
                    tier = "Medium (≤15%)"
                else:
                    tier = "Low (>15%)"

                answer = f"Change failure rate: {avg_cfr:.1f}% average. DORA tier: {tier}."
            else:
                answer = f"Change failure rate data for {row_count} services."
            limitations.append("CFR = % of deployments causing incidents/rollbacks")

        # DORA Metrics - MTTR with benchmark
        elif 'mttr' in defn_id:
            aggregations['incident_count'] = row_count
            if 'mttr_minutes' in df.columns:
                avg_mttr = pd.to_numeric(df['mttr_minutes'], errors='coerce').mean()
                aggregations['avg_mttr_minutes'] = float(avg_mttr)

                # DORA benchmark
                if avg_mttr < 60:
                    tier = "Elite (<1 hour)"
                elif avg_mttr < 1440:
                    tier = "High (<1 day)"
                elif avg_mttr < 10080:
                    tier = "Medium (<1 week)"
                else:
                    tier = "Low (>1 week)"

                answer = f"Mean time to recovery: {avg_mttr:.0f} minutes average. DORA tier: {tier}."
            else:
                answer = f"MTTR data for {row_count} incidents."
            limitations.append("MTTR = Mean Time To Recovery from incidents")

        # Incidents with severity interpretation
        elif 'incident' in defn_id:
            pop_count = len(pop_df)
            aggregations['incident_count'] = row_count
            aggregations['population_count'] = pop_count
            aggregations['population_total'] = float(pop_count)  # For incidents, count is the "total"
            aggregations['share_of_total_pct'] = float((row_count / pop_count * 100) if pop_count > 0 else 100)

            sev_answer = ""
            if 'severity' in pop_df.columns:
                pop_severity_counts = pop_df['severity'].value_counts().to_dict()
                shown_severity_counts = df['severity'].value_counts().to_dict() if 'severity' in df.columns else {}
                aggregations['population_severity_breakdown'] = pop_severity_counts
                aggregations['severity_breakdown'] = shown_severity_counts
                pop_sev1 = pop_severity_counts.get('sev1', 0)
                pop_sev2 = pop_severity_counts.get('sev2', 0)
                pop_sev3 = pop_severity_counts.get('sev3', 0)

                if is_top_n:
                    sev_answer = f"{row_count} of {pop_count} total incidents. Full breakdown: {pop_sev1} sev1, {pop_sev2} sev2, {pop_sev3} sev3."
                elif pop_sev1 > 0:
                    sev_answer = f"{pop_count} incidents ({pop_sev1} sev1 = critical, {pop_sev2} sev2, {pop_sev3} sev3)."
                else:
                    sev_answer = f"{pop_count} incidents ({pop_sev2} sev2, {pop_sev3} sev3). No sev1 (critical) incidents."
            else:
                sev_answer = f"{row_count} of {pop_count} incidents."

            answer = f"Incident summary: {sev_answer}"
            if 'status' in pop_df.columns:
                open_count = len(pop_df[pop_df['status'] == 'open'])
                if open_count > 0:
                    answer += f" {open_count} currently open across all incidents."
            limitations.append("Incident counts reflect observation period in demo data")

        else:
            pop_count = len(pop_df)
            aggregations['row_count'] = row_count
            aggregations['population_count'] = pop_count
            if amount_cols:
                pop_total = pd.to_numeric(pop_df[amount_cols[0]], errors='coerce').sum()
                shown_total = pd.to_numeric(df[amount_cols[0]], errors='coerce').sum()
                aggregations['population_total'] = float(pop_total)
                aggregations['shown_total'] = float(shown_total)

                share_pct = (shown_total / pop_total * 100) if pop_total > 0 else 0
                aggregations['share_of_total_pct'] = float(share_pct)

                if is_top_n:
                    answer = f"Top {row_count} of {pop_count} records: {_format_currency(shown_total)} ({share_pct:.0f}% of {_format_currency(pop_total)} total)."
                else:
                    answer = f"Retrieved {pop_count} records with total value {_format_currency(pop_total)}."
            else:
                if is_top_n:
                    answer = f"Showing top {row_count} of {pop_count} total records."
                else:
                    answer = f"Retrieved {pop_count} records."
            limitations.append("Generic response; definition-specific summary not available")

    except Exception as e:
        aggregations['row_count'] = row_count
        answer = f"Retrieved {row_count} records."
        limitations.append(f"Summary computation error: {str(e)}")

    # Build warnings list
    warnings: list[str] = []

    # Check for ranked-list definitions without limit
    ranked_list_patterns = ['top_', 'customer', 'vendor', 'deal', 'pipeline', 'zombie', 'identity_gap', 'finding']
    is_ranked_definition = any(p in defn_id for p in ranked_list_patterns)

    if is_ranked_definition and applied_limit is None:
        warnings.append("No limit specified for ranked list query; returning full dataset")

    # Append limitations to aggregations (for backward compat)
    if limitations:
        aggregations['limitations'] = limitations

    # Use consistent naming: topn_total (when limited), population_total (always)
    if 'shown_total' in aggregations and applied_limit is not None:
        aggregations['topn_total'] = aggregations.get('shown_total')

    return ComputedSummary(
        aggregations=aggregations,
        warnings=warnings,
        debug_summary=answer,  # Answer is now debug-only
        answer=answer,  # Keep for backward compat, but deprecated
    )


def _apply_definition_ordering(df: pd.DataFrame, definition: Definition, has_limit: bool = False) -> pd.DataFrame:
    """
    Apply ordering based on definition's declared default_order_by.
    
    PRODUCTION BOUNDARY: Ordering is defined in the definition spec, not inferred by NLQ.
    This ensures deterministic, reproducible results.
    
    Args:
        df: DataFrame to sort
        definition: Definition with capabilities.default_order_by
        has_limit: Whether a TopN limit is being applied (triggers sorting)
    
    Returns:
        Sorted DataFrame (or unchanged if no ordering declared)
    """
    if df.empty:
        return df
    
    caps = definition.capabilities
    
    # Only apply ordering when TopN is requested and definition declares ordering
    if not has_limit or not caps.default_order_by:
        return df
    
    sort_columns = []
    sort_ascending = []
    
    # Apply definition-declared ordering
    for order_spec in caps.default_order_by:
        col = order_spec.field
        ascending = order_spec.direction.lower() == "asc"
        
        # Find matching column (exact match or case-insensitive)
        col_to_use = None
        if col in df.columns:
            col_to_use = col
        else:
            # Try case-insensitive match
            for c in df.columns:
                if c.lower() == col.lower():
                    col_to_use = c
                    break
        
        if col_to_use:
            # Convert to numeric for proper sorting
            if df[col_to_use].dtype == 'object':
                df = df.copy()
                df[col_to_use] = pd.to_numeric(df[col_to_use], errors='coerce')
            sort_columns.append(col_to_use)
            sort_ascending.append(ascending)
    
    # Add tie-breaker for deterministic results
    if caps.tie_breaker and caps.tie_breaker in df.columns:
        if caps.tie_breaker not in sort_columns:
            sort_columns.append(caps.tie_breaker)
            sort_ascending.append(True)  # Always ascending for tie-breaker
    
    if sort_columns:
        return df.sort_values(by=sort_columns, ascending=sort_ascending, na_position='last')
    
    return df


def _apply_filter(df: pd.DataFrame, f: FilterSpec) -> pd.DataFrame:
    if f.column not in df.columns:
        return df
    
    if f.operator == "eq":
        return df[df[f.column] == f.value]
    elif f.operator == "ne":
        return df[df[f.column] != f.value]
    elif f.operator == "gt":
        return df[df[f.column] > f.value]
    elif f.operator == "gte":
        return df[df[f.column] >= f.value]
    elif f.operator == "lt":
        return df[df[f.column] < f.value]
    elif f.operator == "lte":
        return df[df[f.column] <= f.value]
    elif f.operator == "in":
        return df[df[f.column].isin(f.value)]
    elif f.operator == "is_null":
        return df[df[f.column].isna()]
    elif f.operator == "is_not_null":
        return df[df[f.column].notna()]
    elif f.operator == "contains":
        return df[df[f.column].astype(str).str.contains(str(f.value), na=False)]
    return df


def _execute_farm_definition(request: ExecuteRequest, definition: Definition) -> ExecuteResponse | None:
    """
    Execute definition against Farm's ground truth data.

    If dataset_id starts with "farm:", fetch from Farm's scenario endpoints.
    Returns None if not a Farm dataset or definition not supported.

    Supported definitions:
    - crm.top_customers: Top customers by revenue (with optional time_window)
    - finops.total_revenue: Total revenue aggregate (with optional time_window)
    """
    if not request.dataset_id.startswith("farm:"):
        return None

    scenario_id = request.dataset_id.replace("farm:", "")

    # Extract time_window string for Farm (NLQ-extracted)
    time_window = request.time_window_str

    from backend.farm.client import get_farm_client

    start_time = time.time()
    client = get_farm_client()

    # Route to appropriate Farm endpoint based on definition
    if request.definition_id == "finops.total_revenue":
        return _execute_farm_total_revenue(
            client, scenario_id, time_window, definition, start_time
        )
    elif request.definition_id == "crm.top_customers":
        return _execute_farm_top_customers(
            client, scenario_id, request.limit, time_window, definition, start_time
        )
    else:
        return None


def _execute_farm_total_revenue(
    client, scenario_id: str, time_window: str | None, definition: Definition, start_time: float
) -> ExecuteResponse:
    """Execute finops.total_revenue against Farm's total-revenue endpoint."""
    try:
        result = client.get_total_revenue(scenario_id, time_window=time_window)

        total_revenue = result.get("total_revenue", 0)
        period = result.get("period", "All Time")
        transaction_count = result.get("transaction_count", 0)
        time_window_applied = result.get("time_window_applied")
        date_range = result.get("date_range", {})

        execution_time_ms = int((time.time() - start_time) * 1000)

        # Build time-aware answer prose
        if time_window_applied:
            answer = f"Your {period} REVENUE is ${total_revenue/1_000_000:,.2f}M ({transaction_count:,} transactions)"
        else:
            answer = f"Your total REVENUE is ${total_revenue/1_000_000:,.2f}M ({transaction_count:,} transactions)"

        summary = ComputedSummary(
            answer=answer,
            aggregations={
                "population_total": total_revenue,
                "transaction_count": transaction_count,
                "period": period,
                "time_window_applied": time_window_applied,
                "date_range": date_range,
                "source": "farm_ground_truth",
                "scenario_id": scenario_id,
            }
        )

        # Scalar response - no row data
        return ExecuteResponse(
            data=[],  # Scalar queries return no rows
            metadata=ExecuteMetadata(
                dataset_id=f"farm:{scenario_id}",
                definition_id="finops.total_revenue",
                version=definition.version,
                executed_at=datetime.utcnow(),
                execution_time_ms=execution_time_ms,
                row_count=0,
                result_schema=[]
            ),
            quality=QualityMetrics(
                completeness=1.0,
                freshness_hours=0.0,
                row_count=0,
                null_percentage=0.0
            ),
            lineage=[
                LineageReference(
                    source_id="farm",
                    table_id=f"scenario/{scenario_id}/total-revenue",
                    columns_used=["total_revenue", "period", "transaction_count"],
                    row_contribution=transaction_count
                )
            ],
            summary=summary
        )
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"Farm total_revenue failed: {e}")
        return None


def _execute_farm_top_customers(
    client, scenario_id: str, limit: int, time_window: str | None, definition: Definition, start_time: float
) -> ExecuteResponse:
    """Execute crm.top_customers against Farm's top-customers endpoint."""
    try:
        result = client.get_top_customers(scenario_id, limit=limit, time_window=time_window)
        customers = result.get("customers", [])
        
        # Transform Farm format to BLL format
        data = []
        for c in customers:
            data.append({
                "Id": c.get("customer_id", ""),
                "Name": c.get("name", ""),
                "AnnualRevenue": c.get("revenue", 0),
                "percent_of_total": c.get("percent_of_total", 0),
            })
        
        # Compute summary
        total_revenue = sum(c.get("revenue", 0) for c in customers)
        
        # Get population total from Farm
        try:
            revenue_metrics = client.get_revenue_metrics(scenario_id)
            population_total = revenue_metrics.get("total_revenue", total_revenue)
        except Exception:
            population_total = total_revenue
        
        execution_time_ms = int((time.time() - start_time) * 1000)
        
        # Build summary
        lines = [f"Top {len(customers)} customers by revenue (Farm ground truth):"]
        for i, c in enumerate(customers[:5], 1):
            lines.append(f"{i}. {c.get('name', 'Unknown')}: ${c.get('revenue', 0):,.2f}")
        lines.append(f"\nTotal: ${total_revenue:,.2f} ({(total_revenue/population_total*100) if population_total else 0:.1f}% of ${population_total:,.2f})")
        
        summary = ComputedSummary(
            answer="\n".join(lines),
            aggregations={
                "customer_count": len(customers),
                "shown_total": total_revenue,
                "population_total": population_total,
                "source": "farm_ground_truth",
                "scenario_id": scenario_id,
            }
        )
        
        return ExecuteResponse(
            data=data,
            metadata=ExecuteMetadata(
                dataset_id=f"farm:{scenario_id}",
                definition_id="crm.top_customers",
                version=definition.version,
                executed_at=datetime.utcnow(),
                execution_time_ms=execution_time_ms,
                row_count=len(data),
                result_schema=[
                    ColumnSchema(name="Id", dtype="string"),
                    ColumnSchema(name="Name", dtype="string"),
                    ColumnSchema(name="AnnualRevenue", dtype="float"),
                    ColumnSchema(name="percent_of_total", dtype="float"),
                ]
            ),
            quality=QualityMetrics(
                completeness=1.0,
                freshness_hours=0.0,
                row_count=len(data),
                null_percentage=0.0
            ),
            lineage=[
                LineageReference(
                    source_id="farm",
                    table_id=f"scenario/{scenario_id}/top-customers",
                    columns_used=["customer_id", "name", "revenue"],
                    row_contribution=len(data)
                )
            ],
            summary=summary
        )
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"Farm top_customers failed: {e}")
        return None


def _parse_time_window(time_window: str | None) -> tuple[str | None, str | None]:
    """
    Parse time_window string into date range.

    Returns (start_date, end_date) as ISO strings, or (None, None) if no filter.
    """
    if not time_window:
        return None, None

    from datetime import datetime, timedelta

    today = datetime.now()
    current_year = today.year
    current_month = today.month
    current_quarter = (current_month - 1) // 3 + 1

    tw = time_window.lower().replace(" ", "_")

    if tw in ("last_year", "lastyear"):
        return f"{current_year - 1}-01-01", f"{current_year - 1}-12-31"
    elif tw in ("this_year", "thisyear", "ytd"):
        return f"{current_year}-01-01", today.strftime("%Y-%m-%d")
    elif tw == "2024":
        return "2024-01-01", "2024-12-31"
    elif tw == "2025":
        return "2025-01-01", "2025-12-31"
    elif tw in ("last_quarter", "lastquarter"):
        q = current_quarter - 1 if current_quarter > 1 else 4
        y = current_year if current_quarter > 1 else current_year - 1
        start_month = (q - 1) * 3 + 1
        end_month = q * 3
        return f"{y}-{start_month:02d}-01", f"{y}-{end_month:02d}-{28 if end_month == 2 else 30}"
    elif tw in ("this_quarter", "thisquarter"):
        start_month = (current_quarter - 1) * 3 + 1
        return f"{current_year}-{start_month:02d}-01", today.strftime("%Y-%m-%d")
    elif tw in ("last_month", "lastmonth"):
        first_of_month = today.replace(day=1)
        last_month_end = first_of_month - timedelta(days=1)
        last_month_start = last_month_end.replace(day=1)
        return last_month_start.strftime("%Y-%m-%d"), last_month_end.strftime("%Y-%m-%d")
    elif tw in ("this_month", "thismonth"):
        return today.replace(day=1).strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")
    elif tw in ("q1", "q2", "q3", "q4"):
        q = int(tw[1])
        start_month = (q - 1) * 3 + 1
        end_month = q * 3
        # Default to current year for quarter references
        return f"{current_year}-{start_month:02d}-01", f"{current_year}-{end_month:02d}-{28 if end_month == 2 else 30}"

    return None, None


def _execute_nlq_test_definition(request: ExecuteRequest, definition: Definition) -> ExecuteResponse | None:
    """
    Execute definition against local nlq_test dataset with time_window support.

    Supported definitions:
    - finops.total_revenue: Total revenue aggregate (with optional time_window)
    - crm.top_customers: Top customers by revenue (with optional time_window)
    """
    if request.dataset_id != "nlq_test":
        return None

    start_time = time.time()

    # Load invoice data
    invoice_path = Path("dcl/demo/datasets/nlq_test/invoices.csv")
    if not invoice_path.exists():
        return None

    df = pd.read_csv(invoice_path)
    df['invoice_date'] = pd.to_datetime(df['invoice_date'])

    # Apply time_window filter
    time_window = request.time_window_str
    start_date, end_date = _parse_time_window(time_window)

    if start_date and end_date:
        mask = (df['invoice_date'] >= start_date) & (df['invoice_date'] <= end_date)
        filtered_df = df[mask]
        time_window_applied = True
        period = f"{start_date} to {end_date}"
    else:
        filtered_df = df
        time_window_applied = False
        period = "All Time"

    if request.definition_id == "finops.total_revenue":
        return _execute_nlq_test_total_revenue(
            filtered_df, df, time_window, time_window_applied, period, definition, start_time
        )
    elif request.definition_id == "crm.top_customers":
        return _execute_nlq_test_top_customers(
            filtered_df, df, request.limit, time_window, time_window_applied, period, definition, start_time
        )

    return None


def _execute_nlq_test_total_revenue(
    filtered_df: pd.DataFrame,
    full_df: pd.DataFrame,
    time_window: str | None,
    time_window_applied: bool,
    period: str,
    definition: Definition,
    start_time: float
) -> ExecuteResponse:
    """Execute finops.total_revenue against local nlq_test dataset."""
    total_revenue = filtered_df['amount'].sum()
    transaction_count = len(filtered_df)

    execution_time_ms = int((time.time() - start_time) * 1000)

    # Build time-aware answer
    if time_window_applied:
        # Make period more human readable - extract year from date range
        import datetime as dt_module
        current_year = dt_module.datetime.now().year
        if time_window and "last_year" in time_window.lower():
            period_label = f"Last Year ({current_year - 1})"
        elif time_window and "2024" in time_window:
            period_label = "2024"
        elif time_window and "2025" in time_window:
            period_label = "2025"
        else:
            period_label = period
        answer = f"Your {period_label} REVENUE is ${total_revenue/1_000_000:,.2f}M ({transaction_count:,} transactions)"
    else:
        answer = f"Your total REVENUE is ${total_revenue/1_000_000:,.2f}M ({transaction_count:,} transactions)"

    summary = ComputedSummary(
        answer=answer,
        aggregations={
            "population_total": float(total_revenue),
            "transaction_count": transaction_count,
            "period": period,
            "time_window_applied": time_window_applied,
            "time_window_requested": time_window,
            "source": "nlq_test_local",
        }
    )

    return ExecuteResponse(
        data=[],
        metadata=ExecuteMetadata(
            dataset_id="nlq_test",
            definition_id="finops.total_revenue",
            version=definition.version,
            executed_at=datetime.utcnow(),
            execution_time_ms=execution_time_ms,
            row_count=0,
            result_schema=[]
        ),
        quality=QualityMetrics(
            completeness=1.0,
            freshness_hours=0.0,
            row_count=0,
            null_percentage=0.0
        ),
        lineage=[
            LineageReference(
                source_id="nlq_test",
                table_id="invoices",
                columns_used=["amount", "invoice_date"],
                row_contribution=transaction_count
            )
        ],
        summary=summary
    )


def _execute_nlq_test_top_customers(
    filtered_df: pd.DataFrame,
    full_df: pd.DataFrame,
    limit: int,
    time_window: str | None,
    time_window_applied: bool,
    period: str,
    definition: Definition,
    start_time: float
) -> ExecuteResponse:
    """Execute crm.top_customers against local nlq_test dataset."""
    # Aggregate by customer
    customer_totals = filtered_df.groupby(['customer_id', 'customer_name']).agg({
        'amount': 'sum'
    }).reset_index()
    customer_totals = customer_totals.sort_values('amount', ascending=False)

    # Apply limit
    top_customers = customer_totals.head(limit)

    # Calculate totals
    total_revenue = filtered_df['amount'].sum()

    # Build response data
    data = []
    for _, row in top_customers.iterrows():
        pct = (row['amount'] / total_revenue * 100) if total_revenue > 0 else 0
        data.append({
            "Id": row['customer_id'],
            "Name": row['customer_name'],
            "AnnualRevenue": row['amount'],
            "percent_of_total": round(pct, 1),
        })

    execution_time_ms = int((time.time() - start_time) * 1000)

    # Build summary
    shown_total = top_customers['amount'].sum()
    lines = [f"Top {len(data)} customers by revenue (nlq_test local data):"]
    for i, c in enumerate(data[:5], 1):
        lines.append(f"{i}. {c['Name']}: ${c['AnnualRevenue']:,.2f}")
    lines.append(f"\nTotal: ${shown_total:,.2f} ({(shown_total/total_revenue*100) if total_revenue else 0:.1f}% of ${total_revenue:,.2f})")

    summary = ComputedSummary(
        answer="\n".join(lines),
        aggregations={
            "customer_count": len(data),
            "shown_total": float(shown_total),
            "population_total": float(total_revenue),
            "time_window_applied": time_window_applied,
            "source": "nlq_test_local",
        }
    )

    return ExecuteResponse(
        data=data,
        metadata=ExecuteMetadata(
            dataset_id="nlq_test",
            definition_id="crm.top_customers",
            version=definition.version,
            executed_at=datetime.utcnow(),
            execution_time_ms=execution_time_ms,
            row_count=len(data),
            result_schema=[
                ColumnSchema(name="Id", dtype="string"),
                ColumnSchema(name="Name", dtype="string"),
                ColumnSchema(name="AnnualRevenue", dtype="float"),
                ColumnSchema(name="percent_of_total", dtype="float"),
            ]
        ),
        quality=QualityMetrics(
            completeness=1.0,
            freshness_hours=0.0,
            row_count=len(data),
            null_percentage=0.0
        ),
        lineage=[
            LineageReference(
                source_id="nlq_test",
                table_id="invoices",
                columns_used=["customer_id", "customer_name", "amount", "invoice_date"],
                row_contribution=len(filtered_df)
            )
        ],
        summary=summary
    )


def execute_definition(request: ExecuteRequest) -> ExecuteResponse:
    start_time = time.time()

    definition = get_definition(request.definition_id)
    if not definition:
        raise ValueError(f"Definition not found: {request.definition_id}")

    # Farm mode - don't fall through to local manifests
    if request.dataset_id.startswith("farm:"):
        farm_result = _execute_farm_definition(request, definition)
        if farm_result:
            return farm_result
        # Farm failed - raise helpful error instead of falling through to local manifest
        scenario_id = request.dataset_id.replace("farm:", "")
        raise ValueError(
            f"Farm execution failed for definition '{request.definition_id}' "
            f"against scenario '{scenario_id}'. Check Farm logs or endpoint availability."
        )

    # NLQ test mode - local dataset with time_window support
    if request.dataset_id == "nlq_test":
        nlq_test_result = _execute_nlq_test_definition(request, definition)
        if nlq_test_result:
            return nlq_test_result
        # Fall through to generic local mode if definition not supported

    # Local demo mode
    manifest = _load_manifest(request.dataset_id)
    
    tables: dict[str, pd.DataFrame] = {}
    lineage: list[LineageReference] = []
    
    for source_ref in definition.sources:
        table_key = f"{source_ref.source_id}.{source_ref.table_id}"
        file_path = _get_table_path(manifest, source_ref.source_id, source_ref.table_id)
        
        if file_path:
            df = _load_table(file_path)
            available_cols = [c for c in source_ref.columns if c in df.columns]
            if available_cols:
                tables[table_key] = df
                lineage.append(LineageReference(
                    source_id=source_ref.source_id,
                    table_id=source_ref.table_id,
                    columns_used=available_cols,
                    row_contribution=len(df)
                ))
    
    if not tables:
        return _empty_response(request, definition, start_time)
    
    result_df = _execute_query(definition, tables, request)
    
    if request.filters:
        for f in request.filters:
            result_df = _apply_filter(result_df, f)
    
    if definition.default_filters:
        for f in definition.default_filters:
            result_df = _apply_filter(result_df, f)
    
    total_rows = len(result_df)
    full_population_df = result_df.copy()  # Keep full data for summary calculation

    # Apply definition-declared ordering when TopN limit is requested
    # PRODUCTION BOUNDARY: Ordering is defined in definition spec, not inferred by NLQ
    has_topn_limit = request.limit < 1000  # Default limit is 1000, anything less is TopN
    result_df = _apply_definition_ordering(result_df, definition, has_limit=has_topn_limit)

    # Apply limit AFTER sorting
    result_df = result_df.iloc[request.offset:request.offset + request.limit]

    # Compute summary with full population for accurate totals/share
    # Treat as top-N if user explicitly requested a small limit (not the default 1000)
    # This ensures share-of-total is computed even when limit >= total_rows
    applied_limit = request.limit if request.limit <= 100 else None

    # Check if this is an AGGREGATE definition (returns totals, not ranked lists)
    # AGGREGATE definitions like finops.arr should never generate "Top N" summaries
    is_aggregate_query = request.limit == 1000  # 1000 is the marker for aggregate queries
    defn_id_lower = definition.definition_id.lower()
    # Also check definition ID patterns for aggregate metrics
    if any(pattern in defn_id_lower for pattern in ['arr', 'burn_rate', 'total', 'aggregate']):
        is_aggregate_query = True

    summary = _compute_summary(result_df, definition, full_population_df, applied_limit, is_aggregate_query)
    
    result_df = result_df.fillna("")
    data = result_df.to_dict(orient="records")
    
    execution_time_ms = int((time.time() - start_time) * 1000)
    
    null_count = result_df.isna().sum().sum()
    total_cells = result_df.size if result_df.size > 0 else 1
    null_percentage = (null_count / total_cells) * 100
    
    schema = [
        ColumnSchema(name=col, dtype=str(result_df[col].dtype))
        for col in result_df.columns
    ]
    
    return ExecuteResponse(
        data=data,
        metadata=ExecuteMetadata(
            dataset_id=request.dataset_id,
            definition_id=request.definition_id,
            version=definition.version,
            executed_at=datetime.utcnow(),
            execution_time_ms=execution_time_ms,
            row_count=total_rows,
            result_schema=schema
        ),
        quality=QualityMetrics(
            completeness=1.0 - (null_percentage / 100),
            freshness_hours=0.0,
            row_count=total_rows,
            null_percentage=null_percentage
        ),
        lineage=lineage,
        summary=summary
    )


def _execute_query(definition: Definition, tables: dict[str, pd.DataFrame], request: ExecuteRequest) -> pd.DataFrame:
    if len(tables) == 1:
        return list(tables.values())[0]
    
    if definition.joins:
        result = None
        for join in definition.joins:
            left_key = f"{_find_source_for_table(definition, join.left_table)}.{join.left_table}"
            right_key = f"{_find_source_for_table(definition, join.right_table)}.{join.right_table}"
            
            left_df = tables.get(left_key)
            right_df = tables.get(right_key)
            
            if left_df is None or right_df is None:
                continue
            
            if result is None:
                result = left_df.copy()
            
            how = "inner" if join.join_type == "inner" else "left"
            
            if join.left_key in result.columns and join.right_key in right_df.columns:
                result = pd.merge(
                    result, right_df,
                    left_on=join.left_key,
                    right_on=join.right_key,
                    how=how,
                    suffixes=("", "_right")
                )
        
        return result if result is not None else pd.DataFrame()
    
    return pd.concat(list(tables.values()), ignore_index=True)


def _find_source_for_table(definition: Definition, table_id: str) -> str:
    for source_ref in definition.sources:
        if source_ref.table_id == table_id:
            return source_ref.source_id
    return ""


def _empty_response(request: ExecuteRequest, definition: Definition, start_time: float) -> ExecuteResponse:
    execution_time_ms = int((time.time() - start_time) * 1000)
    return ExecuteResponse(
        data=[],
        metadata=ExecuteMetadata(
            dataset_id=request.dataset_id,
            definition_id=request.definition_id,
            version=definition.version,
            executed_at=datetime.utcnow(),
            execution_time_ms=execution_time_ms,
            row_count=0,
            result_schema=definition.output_schema
        ),
        quality=QualityMetrics(
            completeness=0.0,
            freshness_hours=0.0,
            row_count=0,
            null_percentage=0.0
        ),
        lineage=[]
    )


def generate_proof(definition_id: str) -> ProofResponse:
    definition = get_definition(definition_id)
    if not definition:
        raise ValueError(f"Definition not found: {definition_id}")
    
    breadcrumbs: list[ProofBreadcrumb] = []
    
    breadcrumbs.append(ProofBreadcrumb(
        step=1,
        action="source_load",
        details={
            "sources": [
                {"source_id": s.source_id, "table_id": s.table_id, "columns": s.columns}
                for s in definition.sources
            ]
        }
    ))
    
    if definition.joins:
        breadcrumbs.append(ProofBreadcrumb(
            step=2,
            action="join",
            details={
                "joins": [
                    {
                        "left": f"{j.left_table}.{j.left_key}",
                        "right": f"{j.right_table}.{j.right_key}",
                        "type": j.join_type
                    }
                    for j in definition.joins
                ]
            }
        ))
    
    if definition.default_filters:
        breadcrumbs.append(ProofBreadcrumb(
            step=len(breadcrumbs) + 1,
            action="filter",
            details={
                "filters": [
                    {"column": f.column, "operator": f.operator, "value": f.value}
                    for f in definition.default_filters
                ]
            }
        ))
    
    breadcrumbs.append(ProofBreadcrumb(
        step=len(breadcrumbs) + 1,
        action="project",
        details={
            "output_columns": [c.name for c in definition.output_schema]
        }
    ))
    
    sql = _generate_sql_equivalent(definition)
    
    return ProofResponse(
        definition_id=definition_id,
        version=definition.version,
        generated_at=datetime.utcnow(),
        breadcrumbs=breadcrumbs,
        sql_equivalent=sql
    )


def _generate_sql_equivalent(definition: Definition) -> str:
    columns = ", ".join([c.name for c in definition.output_schema])
    
    if len(definition.sources) == 1:
        src = definition.sources[0]
        return f"SELECT {columns} FROM {src.source_id}.{src.table_id}"
    
    if definition.joins:
        first_src = definition.sources[0]
        sql = f"SELECT {columns}\nFROM {first_src.source_id}.{first_src.table_id}"
        
        for join in definition.joins:
            right_source = _find_source_for_table(definition, join.right_table)
            sql += f"\n{join.join_type.upper()} JOIN {right_source}.{join.right_table}"
            sql += f"\n  ON {join.left_table}.{join.left_key} = {join.right_table}.{join.right_key}"
        
        if definition.default_filters:
            conditions = []
            for f in definition.default_filters:
                if f.operator == "is_null":
                    conditions.append(f"{f.column} IS NULL")
                elif f.operator == "is_not_null":
                    conditions.append(f"{f.column} IS NOT NULL")
                else:
                    conditions.append(f"{f.column} {f.operator.upper()} {repr(f.value)}")
            sql += f"\nWHERE {' AND '.join(conditions)}"
        
        return sql
    
    return f"SELECT {columns} FROM (UNION of {len(definition.sources)} sources)"
