/**
 * Static SE-mode data flow definitions.
 * Nodes and links representing the Semantic Engine pipeline.
 */

export type Domain = 'financial' | 'hr' | 'crm' | 'ops';
export type SELinkType = 'ingest' | 'internal' | 'mapping' | 'persona';

export interface SENodeDef {
  id: string;
  label: string;
  layer: 0 | 1 | 2 | 3;
  domain?: Domain;
  hoverContent?: string;
}

export interface SELinkDef {
  source: string;
  target: string;
  type: SELinkType;
  hoverContent: string;
}

export const SE_NODES: SENodeDef[] = [
  // L0 \u2014 Sources
  { id: 'crm', label: 'CRM', layer: 0, hoverContent: 'Salesforce / HubSpot CRM' },
  { id: 'erp_gl', label: 'ERP / GL', layer: 0, hoverContent: 'General ledger & ERP system' },
  { id: 'hcm', label: 'HCM', layer: 0, hoverContent: 'Human capital management' },
  { id: 'ipaas_fabric', label: 'iPaaS / Fabric', layer: 0, hoverContent: 'Integration platform & data fabric' },
  { id: 'service_catalog', label: 'Service Catalog', layer: 0, hoverContent: 'IT service catalog' },

  // L1 \u2014 DCL Processing (two equal peer nodes)
  { id: 'schema_norm', label: 'Schema Normalization', layer: 1, hoverContent: 'Steps 1\u20132: source ID normalization, semantic hint detection, ontology load, AAM edge fetch' },
  { id: 'semantic_map', label: 'Semantic Mapping', layer: 1, hoverContent: 'Step 3: tier 0 AAM edges \u2192 tier 1 heuristic \u2192 tier 2 LLM' },

  // L2 \u2014 Resolved Ontology Concepts (colored by domain)
  { id: 'revenue', label: 'revenue', layer: 2, domain: 'financial' },
  { id: 'cogs', label: 'cogs', layer: 2, domain: 'financial' },
  { id: 'opex', label: 'opex', layer: 2, domain: 'financial' },
  { id: 'cash_flow', label: 'cash_flow', layer: 2, domain: 'financial' },
  { id: 'asset', label: 'asset', layer: 2, domain: 'financial' },
  { id: 'chart_of_accounts', label: 'chart_of_accounts', layer: 2, domain: 'financial' },
  { id: 'employee', label: 'employee', layer: 2, domain: 'hr' },
  { id: 'customer', label: 'customer', layer: 2, domain: 'crm' },
  { id: 'vendor', label: 'vendor', layer: 2, domain: 'ops' },

  // L3 \u2014 Personas
  { id: 'cfo', label: 'CFO', layer: 3, hoverContent: 'Chief Financial Officer' },
  { id: 'cro', label: 'CRO', layer: 3, hoverContent: 'Chief Revenue Officer' },
  { id: 'coo', label: 'COO', layer: 3, hoverContent: 'Chief Operating Officer' },
  { id: 'chro', label: 'CHRO', layer: 3, hoverContent: 'Chief Human Resources Officer' },
  { id: 'cto', label: 'CTO', layer: 3, hoverContent: 'Chief Technology Officer' },
];

export const SE_LINKS: SELinkDef[] = [
  // L0 \u2192 L1a (Sources \u2192 Schema Normalization) \u2014 hover shows raw field names
  { source: 'crm', target: 'schema_norm', type: 'ingest', hoverContent: 'Amount, StageName, AccountId, IsWon' },
  { source: 'erp_gl', target: 'schema_norm', type: 'ingest', hoverContent: 'gl_account_code, revenue_amount, debit, credit' },
  { source: 'hcm', target: 'schema_norm', type: 'ingest', hoverContent: 'employee_id, department, salary, hire_date' },
  { source: 'ipaas_fabric', target: 'schema_norm', type: 'ingest', hoverContent: 'connector_id, flow_name, sync_status' },
  { source: 'service_catalog', target: 'schema_norm', type: 'ingest', hoverContent: 'service_name, owner, sla_tier, cost_center' },

  // L1a \u2192 L1b (internal handoff \u2014 thin, low opacity)
  { source: 'schema_norm', target: 'semantic_map', type: 'internal', hoverContent: 'Normalized fields \u2192 semantic resolution' },

  // L1b \u2192 L2 (Semantic Mapping \u2192 Concepts) \u2014 hover shows resolution tier + confidence
  { source: 'semantic_map', target: 'revenue', type: 'mapping', hoverContent: 'Tier 0: AAM edge \u00b7 confidence 0.97' },
  { source: 'semantic_map', target: 'cogs', type: 'mapping', hoverContent: 'Tier 1: heuristic \u00b7 confidence 0.91' },
  { source: 'semantic_map', target: 'opex', type: 'mapping', hoverContent: 'Tier 1: heuristic \u00b7 confidence 0.89' },
  { source: 'semantic_map', target: 'cash_flow', type: 'mapping', hoverContent: 'Tier 0: AAM edge \u00b7 confidence 0.95' },
  { source: 'semantic_map', target: 'asset', type: 'mapping', hoverContent: 'Tier 1: heuristic \u00b7 confidence 0.88' },
  { source: 'semantic_map', target: 'chart_of_accounts', type: 'mapping', hoverContent: 'Tier 0: AAM edge \u00b7 confidence 0.99' },
  { source: 'semantic_map', target: 'employee', type: 'mapping', hoverContent: 'Tier 2: LLM \u00b7 confidence 0.84' },
  { source: 'semantic_map', target: 'customer', type: 'mapping', hoverContent: 'Tier 1: heuristic \u00b7 confidence 0.92' },
  { source: 'semantic_map', target: 'vendor', type: 'mapping', hoverContent: 'Tier 2: LLM \u00b7 confidence 0.81' },

  // L2 \u2192 L3 (Concepts \u2192 Personas) \u2014 hover shows entity-tagged triple paths
  { source: 'revenue', target: 'cfo', type: 'persona', hoverContent: 'meridian.revenue.total' },
  { source: 'revenue', target: 'cro', type: 'persona', hoverContent: 'meridian.revenue.arr \u00b7 cascadia.revenue.services' },
  { source: 'cogs', target: 'cfo', type: 'persona', hoverContent: 'meridian.cogs.direct \u00b7 cascadia.cogs.labor' },
  { source: 'opex', target: 'cfo', type: 'persona', hoverContent: 'meridian.opex.sga \u00b7 cascadia.opex.facilities' },
  { source: 'opex', target: 'coo', type: 'persona', hoverContent: 'meridian.opex.operations \u00b7 cascadia.opex.logistics' },
  { source: 'cash_flow', target: 'cfo', type: 'persona', hoverContent: 'meridian.cash_flow.operating \u00b7 cascadia.cash_flow.investing' },
  { source: 'asset', target: 'cfo', type: 'persona', hoverContent: 'meridian.asset.current \u00b7 cascadia.asset.fixed' },
  { source: 'chart_of_accounts', target: 'cfo', type: 'persona', hoverContent: 'meridian.coa.gl_structure' },
  { source: 'employee', target: 'chro', type: 'persona', hoverContent: 'meridian.employee.headcount \u00b7 cascadia.employee.attrition' },
  { source: 'employee', target: 'coo', type: 'persona', hoverContent: 'meridian.employee.utilization' },
  { source: 'customer', target: 'cro', type: 'persona', hoverContent: 'meridian.customer.arr \u00b7 cascadia.customer.churn' },
  { source: 'customer', target: 'cto', type: 'persona', hoverContent: 'cascadia.customer.platform_usage' },
  { source: 'vendor', target: 'coo', type: 'persona', hoverContent: 'meridian.vendor.spend \u00b7 cascadia.vendor.contracts' },
  { source: 'vendor', target: 'cto', type: 'persona', hoverContent: 'cascadia.vendor.saas_licenses' },
];
