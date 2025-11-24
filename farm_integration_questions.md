# AOS-Farm API Integration Questions for DCL

**Context:** The DCL (Data Connectivity Layer) engine needs to ingest synthetic schemas from AOS-Farm and map them to a unified ontology. Currently using hardcoded mock data - need to connect to real Farm endpoints.

## 1. API Endpoints & Schema Discovery

**Q1.1:** What API endpoint(s) should DCL call to discover available data sources?
- Is there a single catalog endpoint like `GET /api/synthetic/catalog`?
- Or separate endpoints per domain (assets, business data, events)?

**Q1.2:** For each data source, what endpoint returns table/field schemas?
- Example: `GET /api/synthetic/sources/{source_id}/schema`?
- What does the response format look like (JSON schema structure)?

**Q1.3:** Which synthetic data domains are available via API?
- Enterprise assets (applications, services, hosts)?
- Business data (customers, invoices, subscriptions, transactions)?
- Time-series events (logs, network flows, auth events)?
- Mock CRM/ERP endpoints?

**Q1.4:** Do you provide sample data retrieval endpoints?
- If DCL needs a few sample records per table for analysis, what's the endpoint?
- Example: `GET /api/synthetic/data/{table_id}?limit=5`?

## 2. Authentication & Session Management

**Q2.1:** What authentication method should DCL use?
- API key in headers?
- Session cookie?
- Bearer token?
- No auth (public endpoints)?

**Q2.2:** How does tenant derivation work for external callers?
- You mentioned IP-based sessions - how does DCL specify/pass tenant context?
- Should we send a tenant ID in headers like `X-Tenant-Id`?
- Or does Farm auto-assign based on session?

**Q2.3:** Are there any auth setup steps required?
- Do we need to register DCL as a client first?
- Any credentials/secrets to configure?

## 3. Request/Response Contracts

**Q3.1:** For schema discovery endpoints, what are:
- Required request parameters (query params, headers)?
- Expected response structure (provide example JSON)?

**Q3.2:** What field metadata is included in schemas?
- Field name, type (string/int/float/date)?
- Semantic hints (is_id, is_foreign_key, category)?
- Nullable, distinct counts, sample values?

**Q3.3:** For tables with foreign key relationships (customers→invoices):
- How are FK relationships expressed in the schema response?
- Any relationship metadata we can use?

**Q3.4:** Pagination and limits:
- Are there default/max page sizes for data retrieval?
- Pagination format (offset/limit, cursor-based)?

## 4. Data Source Constraints & Environment

**Q4.1:** What are realistic scale expectations?
- How many data sources typically available?
- How many tables per source?
- Record count ranges?

**Q4.2:** Any rate limits or throttling?
- Requests per minute/hour?
- Concurrent connection limits?

**Q4.3:** Do chaos injections affect schema discovery calls?
- Should DCL expect random failures/latency?
- Or are schema endpoints stable?

## 5. Expected Integration Pattern

**Q5.1:** What's the recommended flow for external systems like DCL?
1. Call discovery endpoint to get source list
2. For each source, fetch schema
3. Optionally fetch sample data
4. Is this correct?

**Q5.2:** Should DCL cache schemas, or fetch fresh on every run?
- Do schemas change frequently?
- Any versioning or change notification mechanism?

---

**Target Outcome:** DCL will replace hardcoded `load_farm_schemas()` with HTTP calls to Farm, transforming your responses into our `SourceSystem → TableSchema → FieldSchema` model structure.

**Base URL:** https://autonomos.farm/

---

**Next Steps:**
1. Send these questions to the Farm Replit agent
2. Get API endpoint details and authentication requirements
3. Implement HTTP integration in `backend/engine/schema_loader.py`
4. Transform Farm responses into DCL's domain models
