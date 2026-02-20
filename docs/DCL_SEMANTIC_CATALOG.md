# DCL Semantic Catalog

**Last Updated:** February 7, 2026
**Source:** Live data from `/api/dcl/semantic-export`

## Metrics (37)

### CFO Pack (9 metrics)

| ID | Name | Aliases | Allowed Dimensions | Grains | Measure | Direction |
|----|------|---------|--------------------|--------|---------|-----------|
| arr | Annual Recurring Revenue | ARR, annual recurring revenue, recurring revenue, annual revenue | customer, service_line, region, segment | month, quarter, year | point_in_time_sum | high |
| mrr | Monthly Recurring Revenue | MRR, monthly recurring revenue, monthly revenue | customer, service_line, region, segment | month, quarter | point_in_time_sum | high |
| revenue | Total Revenue | total revenue, sales, income, top line | customer, service_line, region, product, segment | day, week, month, quarter, year | sum | high |
| services_revenue | Services Revenue | professional services, PS revenue, consulting revenue | customer, service_line, region, project | month, quarter, year | sum | high |
| ar | Accounts Receivable | AR, accounts receivable, receivables, outstanding invoices, A/R | customer, invoice, aging_bucket | day, week, month | point_in_time_sum | high |
| dso | Days Sales Outstanding | DSO, days sales outstanding, collection days, AR days | customer, segment, region | month, quarter | avg_days_between | low |
| burn_rate | Burn Rate | burn, cash burn, monthly burn, spending rate | cost_center, category | month, quarter | sum | low |
| gross_margin | Gross Margin | margin, GM, gross profit margin | product, service_line, segment | month, quarter, year | ratio | high |
| ar_aging | Accounts Receivable Aging | AR aging, receivables aging, aged receivables | aging_bucket | month, quarter | sum | high |

### CRO Pack (8 metrics)

| ID | Name | Aliases | Allowed Dimensions | Grains | Measure | Direction |
|----|------|---------|--------------------|--------|---------|-----------|
| pipeline | Sales Pipeline | sales pipeline, open pipeline, pipeline value, opportunities | rep, stage, region, segment | week, month, quarter | point_in_time_sum | high |
| win_rate | Win Rate | close rate, conversion rate, deal win rate | rep, segment, region, product | month, quarter | ratio | high |
| quota_attainment | Quota Attainment | attainment, quota achievement, target attainment, sales attainment | rep, segment, region | month, quarter | ratio | high |
| churn_rate | Churn Rate | churn, customer churn, revenue churn | segment, region, cohort | month, quarter | ratio | low |
| nrr | Net Revenue Retention | NRR, net retention, dollar retention, NDR | segment, region, cohort | month, quarter, year | ratio | high |
| pipeline_value | Total Pipeline Value | pipeline, total pipeline, sales pipeline | rep, stage, region, segment | month, quarter | sum | high |
| churn_risk | Churn Risk Score | churn risk score, risk score | segment, customer | month, quarter | avg | low |
| nrr_by_cohort | NRR by Cohort | cohort NRR, retention by cohort | cohort, product | quarter, year | avg | high |

### COO Pack (3 metrics)

| ID | Name | Aliases | Allowed Dimensions | Grains | Measure | Direction |
|----|------|---------|--------------------|--------|---------|-----------|
| throughput | Work Throughput | work throughput, items completed, tickets resolved | team, work_type, priority | week, month, quarter | sum | high |
| cycle_time | Cycle Time | lead time, delivery time, time to complete | team, project_type, priority | week, month, quarter | avg | low |
| sla_compliance | SLA Compliance Rate | SLA attainment, SLA rate, service level compliance | team, tier, work_type | week, month, quarter | avg | high |

### CTO Pack (5 metrics)

| ID | Name | Aliases | Allowed Dimensions | Grains | Measure | Direction |
|----|------|---------|--------------------|--------|---------|-----------|
| deploy_frequency | Deployment Frequency | deployment rate, deploys per week, release frequency | service, team, environment | week, month | sum | high |
| mttr | Mean Time to Recovery | MTTR, recovery time, incident recovery | team, service, severity | week, month, quarter | avg | low |
| uptime | Service Uptime | availability, uptime percentage, service availability | service | month, quarter | avg | high |
| slo_attainment | SLO Attainment | SLO compliance, service level objective, SLO rate | service | month, quarter | avg | high |
| cloud_cost | Cloud Spend | cloud spend, infrastructure cost, AWS cost, cloud expenses | resource_type, team, environment | month, quarter | sum | low |

### CHRO Pack (12 metrics)

| ID | Name | Aliases | Allowed Dimensions | Grains | Measure | Direction |
|----|------|---------|--------------------|--------|---------|-----------|
| headcount | Headcount | total headcount, employee count, FTE, workforce size | department, level, location, tenure_band | month, quarter, year | point_in_time_sum | high |
| attrition | Attrition Rate | turnover, employee turnover, attrition rate, churn | department, level, tenure_band, location | month, quarter, year | ratio | low |
| time_to_fill | Time to Fill | days to fill, hiring speed, recruiting time | department, role, location | month, quarter | avg | low |
| engagement | Employee Engagement Score | engagement score, satisfaction, employee satisfaction | department, team, tenure_band, location | quarter, year | avg | high |
| compensation_ratio | Compensation Ratio | compa-ratio, pay ratio, salary competitiveness | department, level, location | quarter, year | avg | high |
| training_hours | Training Hours per Employee | learning hours, development hours, L&D hours | department, training_type, level | month, quarter, year | avg | high |
| dei_index | DEI Index | diversity index, inclusion score, diversity score | department, level, location | quarter, year | avg | high |
| absenteeism | Absenteeism Rate | absence rate, sick days, unplanned absence | department, team, location | month, quarter | ratio | low |
| offer_acceptance_rate | Offer Acceptance Rate | offer rate, acceptance rate, recruiting funnel completion, offer conversion | department, role, location | month, quarter, year | ratio | high |
| internal_mobility_rate | Internal Mobility Rate | internal mobility, career development, internal transfers, retention mobility | department, level, tenure_band | month, quarter, year | ratio | high |
| span_of_control | Span of Control | org structure health, manager ratio, direct reports, management span | department, level, location | quarter, year | avg | high |
| enps | Employee Net Promoter Score | eNPS, nps, employee NPS, employee net promoter, workplace recommendation score | department, team, tenure_band, location | month, quarter, year | avg | high |

---

## Entities (29)

| ID | Name | Aliases |
|----|------|---------|
| customer | Customer | account, company, client |
| service_line | Service Line | service type, professional service |
| region | Region | geography, geo, territory, area, location |
| segment | Segment | market segment, customer segment, tier, size |
| rep | Sales Representative | sales rep, account executive, AE, seller |
| team | Team | squad, group, department, unit |
| product | Product | SKU, item, offering |
| project | Project | initiative, program, engagement |
| invoice | Invoice | bill, statement |
| stage | Pipeline Stage | deal stage, opportunity stage, sales stage |
| cohort | Customer Cohort | customer cohort, signup cohort |
| cost_center | Cost Center | budget, department budget |
| aging_bucket | AR Aging Bucket | aging period, days outstanding bucket |
| project_type | Project Type | initiative type, work category |
| work_type | Work Type | task type, item type |
| priority | Priority Level | urgency, priority level |
| service | Service/Application | application, system, microservice |
| resource_type | Cloud Resource Type | infrastructure type, cloud resource |
| environment | Deployment Environment | env, stage |
| sla_type | SLA Type | SLA category |
| slo_type | SLO Type | objective type |
| severity | Incident Severity | incident level, priority |
| category | Category | type, class |
| department | Department | dept, division, org unit |
| level | Level | job_level, grade, band |
| tenure_band | Tenure Band | tenure, years_of_service |
| role | Role | job_title, position |
| training_type | Training Type | course_type, learning_category |
| location | Location | office, site, workplace |

---

## Bindings (13)

| Source System | Canonical Event | Quality | Freshness |
|---------------|-----------------|---------|-----------|
| Salesforce CRM | deal_won | 0.95 | 0.98 |
| NetSuite ERP | revenue_recognized | 0.92 | 0.95 |
| NetSuite ERP | invoice_posted | 0.90 | 0.95 |
| Chargebee | subscription_started | 0.88 | 0.92 |
| Jira | work_item_completed | 0.85 | 0.90 |
| GitHub Actions | deployment_completed | 0.90 | 0.98 |
| PagerDuty | incident_resolved | 0.88 | 0.95 |
| AWS Cost Explorer | cloud_cost_incurred | 0.92 | 0.85 |
| Workday | employee_hired | 0.92 | 0.95 |
| Workday | employee_terminated | 0.90 | 0.95 |
| Greenhouse | requisition_opened | 0.88 | 0.92 |
| Greenhouse | requisition_filled | 0.88 | 0.92 |
| Culture Amp | survey_completed | 0.85 | 0.90 |

---

## Persona Concepts

| Persona | Metrics |
|---------|---------|
| CFO | arr, mrr, revenue, services_revenue, ar, dso, burn_rate, gross_margin, ar_aging |
| CRO | pipeline, win_rate, quota_attainment, churn_rate, nrr, pipeline_value, churn_risk, nrr_by_cohort |
| COO | throughput, cycle_time, sla_compliance |
| CTO | deploy_frequency, mttr, uptime, slo_attainment, cloud_cost |
| CHRO | headcount, attrition, time_to_fill, engagement, compensation_ratio, training_hours, dei_index, absenteeism, offer_acceptance_rate, internal_mobility_rate, span_of_control, enps |
