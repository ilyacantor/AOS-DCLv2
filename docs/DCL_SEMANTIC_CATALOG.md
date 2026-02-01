# DCL Semantic Catalog (Demo Mode)

## Metrics (20)

| ID | Name | Pack | Aliases | Allowed Dimensions |
|----|------|------|---------|-------------------|
| arr | Annual Recurring Revenue | CFO | ARR, annual recurring revenue, recurring revenue, annual revenue | customer, service_line, region, segment |
| mrr | Monthly Recurring Revenue | CFO | MRR, monthly recurring revenue, monthly revenue | customer, service_line, region, segment |
| revenue | Total Revenue | CFO | total revenue, sales, income, top line | customer, service_line, region, product, segment |
| services_revenue | Services Revenue | CFO | professional services, PS revenue, consulting revenue | customer, service_line, region, project |
| ar | Accounts Receivable | CFO | AR, accounts receivable, receivables, outstanding invoices, A/R | customer, invoice, aging_bucket |
| dso | Days Sales Outstanding | CFO | DSO, days sales outstanding, collection days, AR days | customer, segment, region |
| burn_rate | Burn Rate | CFO | burn, cash burn, monthly burn, spending rate | cost_center, category |
| gross_margin | Gross Margin | CFO | margin, GM, gross profit margin | product, service_line, segment |
| pipeline | Sales Pipeline | CRO | sales pipeline, open pipeline, pipeline value, opportunities | rep, stage, region, segment |
| win_rate | Win Rate | CRO | close rate, conversion rate, deal win rate | rep, segment, region, product |
| churn_rate | Churn Rate | CRO | churn, customer churn, revenue churn, attrition | segment, region, cohort |
| nrr | Net Revenue Retention | CRO | NRR, net retention, dollar retention, NDR | segment, region, cohort |
| throughput | Throughput | COO | velocity, output, completion rate | team, project, work_type |
| cycle_time | Cycle Time | COO | lead time, completion time, turnaround time | team, project, work_type, priority |
| sla_compliance | SLA Compliance | COO | SLA, service level, compliance rate | team, customer, sla_type |
| deploy_frequency | Deployment Frequency | CTO | deploys, release frequency, shipping velocity | team, service, environment |
| mttr | Mean Time to Recovery | CTO | MTTR, recovery time, incident recovery | team, service, severity |
| uptime | Uptime | CTO | availability, system uptime, service availability | service, environment, region |
| slo_attainment | SLO Attainment | CTO | SLO, SLO compliance, objective attainment | service, slo_type, team |
| cloud_spend | Cloud Spend | CTO | cloud cost, infrastructure cost, AWS spend, GCP cost, Azure cost | service, team, resource_type, environment |

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
