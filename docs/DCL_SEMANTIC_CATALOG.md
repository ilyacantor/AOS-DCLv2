# DCL Semantic Catalog (Demo Mode)

## Metrics (20)

| ID | Name | Pack | Unit | Allowed Dimensions |
|----|------|------|------|--------------------|
| arr | Annual Recurring Revenue | CFO | USD | customer, segment, product, region |
| mrr | Monthly Recurring Revenue | CFO | USD | customer, segment, product |
| revenue | Total Revenue | CFO | USD | customer, segment, service_line, region |
| services_revenue | Services Revenue | CFO | USD | customer, service_line, region |
| ar | Accounts Receivable | CFO | USD | customer, invoice, aging_bucket |
| dso | Days Sales Outstanding | CFO | days | customer, segment |
| burn_rate | Monthly Burn Rate | CFO | USD | cost_center, department |
| gross_margin | Gross Margin | CFO | percent | product, service_line |
| pipeline | Pipeline Value | CRO | USD | rep, stage, region, segment |
| win_rate | Win Rate | CRO | percent | rep, region, segment |
| churn_rate | Churn Rate | CRO | percent | segment, cohort, product |
| nrr | Net Revenue Retention | CRO | percent | segment, cohort |
| throughput | Throughput | COO | count | team, project, work_type |
| cycle_time | Cycle Time | COO | days | team, project, work_type, priority |
| sla_compliance | SLA Compliance Rate | COO | percent | team, service, priority |
| deploy_frequency | Deploy Frequency | CTO | count | service, team, environment |
| mttr | Mean Time to Recover | CTO | hours | service, team, severity |
| uptime | Uptime | CTO | percent | service, environment |
| slo_attainment | SLO Attainment | CTO | percent | service, team |
| cloud_spend | Cloud Spend | CTO | USD | service, team, resource_type, environment |

## Entities (22)

| ID | Name | Type | Aliases |
|----|------|------|---------|
| customer | Customer | dimension | client, account, buyer |
| segment | Segment | dimension | tier, customer_segment, market_segment |
| product | Product | dimension | sku, offering |
| region | Region | dimension | geo, geography, territory |
| service_line | Service Line | dimension | practice, business_unit |
| invoice | Invoice | dimension | bill, billing_document |
| aging_bucket | Aging Bucket | dimension | age_range, days_outstanding |
| cost_center | Cost Center | dimension | cc, department_code |
| department | Department | dimension | dept, org_unit |
| rep | Sales Rep | dimension | salesperson, account_exec, ae |
| stage | Stage | dimension | deal_stage, pipeline_stage |
| cohort | Cohort | dimension | signup_cohort, vintage |
| team | Team | dimension | squad, group |
| project | Project | dimension | initiative, program |
| work_type | Work Type | dimension | issue_type, task_type |
| priority | Priority | dimension | severity, urgency |
| service | Service | dimension | microservice, app, application |
| environment | Environment | dimension | env, deploy_target |
| severity | Severity | dimension | incident_severity, alert_level |
| resource_type | Resource Type | dimension | cloud_resource, infra_type |
| time | Time | time | date, period, timestamp |
| fiscal_period | Fiscal Period | time | quarter, fiscal_quarter, fy |

## Bindings (8 - Demo Mode Only)

| Source System | Canonical Event | Quality | Freshness | Dimensions Covered |
|---------------|-----------------|---------|-----------|-------------------|
| Salesforce CRM | deal_won | 0.95 | 0.98 | customer, rep, region, segment |
| NetSuite ERP | revenue_recognized | 0.92 | 0.95 | customer, service_line |
| NetSuite ERP | invoice_posted | 0.90 | 0.95 | customer, invoice, aging_bucket |
| Chargebee | subscription_started | 0.88 | 0.92 | customer, product, segment |
| Jira | work_item_completed | 0.85 | 0.90 | team, project, work_type, priority |
| GitHub Actions | deployment_completed | 0.90 | 0.98 | service, team, environment |
| PagerDuty | incident_resolved | 0.88 | 0.95 | service, team, severity |
| AWS Cost Explorer | cloud_cost_incurred | 0.92 | 0.85 | service, team, resource_type, environment |

## Persona Concepts

| Persona | Metrics |
|---------|---------|
| CFO | arr, mrr, revenue, services_revenue, ar, dso, burn_rate, gross_margin |
| CRO | pipeline, win_rate, churn_rate, nrr |
| COO | throughput, cycle_time, sla_compliance |
| CTO | deploy_frequency, mttr, uptime, slo_attainment, cloud_spend |

## Sample NLQ Queries

### CFO Pack
- What is our current ARR?
- Show me MRR trend by segment
- What's our monthly recurring revenue by customer?
- Break down revenue by service line
- Show services revenue by region
- What's our accounts receivable aging?
- What is DSO by customer segment?
- Show burn rate trend over last 6 months
- What's our gross margin by product?

### CRO Pack
- What's our current pipeline value?
- Show pipeline by rep and stage
- What's our win rate by region?
- Compare win rates across sales reps
- What is churn rate by customer segment?
- Show NRR by cohort
- Which customers have the highest churn risk?
- Break down net revenue retention by product

### COO Pack
- What is our average throughput by team?
- Show cycle time by project type
- What's our SLA compliance rate?
- Which teams have the lowest SLA attainment?
- Show throughput trend by work type
- Break down cycle time by priority

### CTO Pack
- What's our deploy frequency by service?
- Show MTTR by team
- What is current uptime by service?
- Which services have the lowest SLO attainment?
- What's our cloud spend by resource type?
- Break down cloud costs by team and environment
- Show deployment frequency trend
- Compare MTTR across severity levels

### Cross-Persona
- Show me all metrics for the enterprise segment
- What KPIs are trending down this quarter?
- Which data sources feed into revenue metrics?
- What's the data quality score for our Salesforce integration?
- Show freshness scores for all connected sources
