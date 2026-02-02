# DCL Semantic Catalog (Demo Mode)

## Metrics (20)

| ID | Name | Pack | Sample Value | Aliases | Allowed Dimensions |
|----|------|------|--------------|---------|-------------------|
| arr | Annual Recurring Revenue | CFO | $12.4M | ARR, annual recurring revenue, recurring revenue, annual revenue | customer, service_line, region, segment |
| mrr | Monthly Recurring Revenue | CFO | $1.03M | MRR, monthly recurring revenue, monthly revenue | customer, service_line, region, segment |
| revenue | Total Revenue | CFO | $14.8M | total revenue, sales, income, top line | customer, service_line, region, product, segment |
| services_revenue | Services Revenue | CFO | $2.4M | professional services, PS revenue, consulting revenue | customer, service_line, region, project |
| ar | Accounts Receivable | CFO | $1.8M | AR, accounts receivable, receivables, outstanding invoices, A/R | customer, invoice, aging_bucket |
| dso | Days Sales Outstanding | CFO | 42 days | DSO, days sales outstanding, collection days, AR days | customer, segment, region |
| burn_rate | Burn Rate | CFO | $890K/mo | burn, cash burn, monthly burn, spending rate | cost_center, category |
| gross_margin | Gross Margin | CFO | 72% | margin, GM, gross profit margin | product, service_line, segment |
| pipeline | Sales Pipeline | CRO | $8.2M | sales pipeline, open pipeline, pipeline value, opportunities | rep, stage, region, segment |
| win_rate | Win Rate | CRO | 28% | close rate, conversion rate, deal win rate | rep, segment, region, product |
| churn_rate | Churn Rate | CRO | 4.2% | churn, customer churn, revenue churn, attrition | segment, region, cohort |
| nrr | Net Revenue Retention | CRO | 112% | NRR, net retention, dollar retention, NDR | segment, region, cohort |
| throughput | Throughput | COO | 847 items/wk | velocity, output, completion rate | team, project, work_type |
| cycle_time | Cycle Time | COO | 6.3 days | lead time, completion time, turnaround time | team, project, work_type, priority |
| sla_compliance | SLA Compliance | COO | 94.2% | SLA, service level, compliance rate | team, customer, sla_type |
| deploy_frequency | Deployment Frequency | CTO | 18/week | deploys, release frequency, shipping velocity | team, service, environment |
| mttr | Mean Time to Recovery | CTO | 1.4 hrs | MTTR, recovery time, incident recovery | team, service, severity |
| uptime | Uptime | CTO | 99.94% | availability, system uptime, service availability | service, environment, region |
| slo_attainment | SLO Attainment | CTO | 97.8% | SLO, SLO compliance, objective attainment | service, slo_type, team |
| cloud_spend | Cloud Spend | CTO | $142K/mo | cloud cost, infrastructure cost, AWS spend, GCP cost, Azure cost | service, team, resource_type, environment |

## Entities (22)

| ID | Name | Type | Sample Values | Aliases |
|----|------|------|---------------|---------|
| customer | Customer | dimension | Acme Corp, TechFlow Inc, GlobalRetail | client, account, buyer |
| segment | Segment | dimension | Enterprise, Mid-Market, SMB | tier, customer_segment, market_segment |
| product | Product | dimension | Platform Pro, Analytics Suite, API Access | sku, offering |
| region | Region | dimension | North America, EMEA, APAC, LATAM | geo, geography, territory |
| service_line | Service Line | dimension | Implementation, Training, Support | practice, business_unit |
| invoice | Invoice | dimension | INV-2026-0142, INV-2026-0143 | bill, billing_document |
| aging_bucket | Aging Bucket | dimension | Current, 1-30 days, 31-60 days, 61-90 days, 90+ days | age_range, days_outstanding |
| cost_center | Cost Center | dimension | CC-ENG, CC-SALES, CC-G&A | cc, department_code |
| department | Department | dimension | Engineering, Sales, Marketing, Finance | dept, org_unit |
| rep | Sales Rep | dimension | Sarah Chen, Mike Johnson, Alex Rivera | salesperson, account_exec, ae |
| stage | Stage | dimension | Qualification, Discovery, Proposal, Negotiation, Closed Won | deal_stage, pipeline_stage |
| cohort | Cohort | dimension | Q1-2025, Q2-2025, Q3-2025, Q4-2025 | signup_cohort, vintage |
| team | Team | dimension | Platform, Growth, Infrastructure, Data | squad, group |
| project | Project | dimension | API v3, Dashboard Redesign, Mobile App | initiative, program |
| work_type | Work Type | dimension | Feature, Bug, Tech Debt, Incident | issue_type, task_type |
| priority | Priority | dimension | P0, P1, P2, P3 | severity, urgency |
| service | Service | dimension | api-gateway, auth-service, billing-engine | microservice, app, application |
| environment | Environment | dimension | Production, Staging, Development | env, deploy_target |
| severity | Severity | dimension | SEV1, SEV2, SEV3 | incident_severity, alert_level |
| resource_type | Resource Type | dimension | EC2, RDS, Lambda, S3 | cloud_resource, infra_type |
| time | Time | time | 2026-01-15, January 2026, Q4 2025 | date, period, timestamp |
| fiscal_period | Fiscal Period | time | FY26-Q1, FY26-Q2 | quarter, fiscal_quarter, fy |

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
- What is our current ARR? → $12.4M
- Show me MRR trend by segment → Enterprise: $620K, Mid-Market: $310K, SMB: $100K
- What's our accounts receivable aging? → Current: $1.2M, 1-30: $380K, 31-60: $140K, 61-90: $50K, 90+: $30K
- What is DSO by customer segment? → Enterprise: 52 days, Mid-Market: 38 days, SMB: 28 days
- What's our gross margin by product? → Platform Pro: 78%, Analytics: 68%, API: 82%

### CRO Pack
- What's our current pipeline value? → $8.2M
- Show pipeline by stage → Qualification: $2.1M, Discovery: $2.4M, Proposal: $1.8M, Negotiation: $1.9M
- What's our win rate by region? → NA: 32%, EMEA: 26%, APAC: 24%
- What is churn rate by segment? → Enterprise: 2.1%, Mid-Market: 4.8%, SMB: 8.4%
- Show NRR by cohort → Q1-2025: 118%, Q2-2025: 114%, Q3-2025: 108%

### COO Pack
- What is our average throughput by team? → Platform: 312/wk, Growth: 245/wk, Infra: 180/wk, Data: 110/wk
- Show cycle time by priority → P0: 0.8 days, P1: 2.4 days, P2: 5.8 days, P3: 12.1 days
- What's our SLA compliance by team? → Platform: 96%, Growth: 93%, Infra: 98%, Data: 91%

### CTO Pack
- What's our deploy frequency by service? → api-gateway: 24/wk, auth: 8/wk, billing: 6/wk
- Show MTTR by severity → SEV1: 0.4 hrs, SEV2: 1.8 hrs, SEV3: 4.2 hrs
- What's our cloud spend by resource type? → EC2: $68K, RDS: $42K, Lambda: $18K, S3: $14K
- Which services have the lowest uptime? → billing-engine: 99.82%, api-gateway: 99.96%, auth: 99.99%

### Cross-Persona
- Show me all metrics for Enterprise segment → ARR: $7.8M, Pipeline: $4.2M, Churn: 2.1%, NRR: 124%
- What's the data quality score for our Salesforce integration? → 0.95 (95%)
- Show freshness scores for all connected sources → GitHub: 98%, Salesforce: 98%, NetSuite: 95%, PagerDuty: 95%, Chargebee: 92%, Jira: 90%, AWS: 85%
