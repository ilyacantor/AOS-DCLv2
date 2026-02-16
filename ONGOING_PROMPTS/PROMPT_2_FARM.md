# Farm Agent — Trifecta Flow Fixes

## Your Role

You are the Farm module. You are the hauler — you receive job orders from AAM and deliver data to DCL. You own two flows:

- **Path 3 — Content Path (Farm → DCL):** You extract data from external source systems (or generate simulated data) and push it to DCL's `/ingest` endpoint. You tag every push with the `pipe_id` from the Runner manifest so DCL can join it with the schema blueprint.
- **Path 4 — Verification Path (Farm ↔ DCL):** You inject known ground truth data, read it back from DCL, and compare. You are the test oracle.

---

## Change 1: Receive Runner Manifests from AAM (Path 2)

**NEW:** AAM will now send you JobManifest payloads instead of sending them directly to DCL. You need an intake endpoint to receive these.

**Action:** Create a `POST` endpoint (e.g., `/api/farm/manifest-intake`) that accepts a JobManifest, validates it, and queues it for execution.

### JobManifest Schema (what you receive from AAM)

```
JobManifest
├── manifest_version: "1.0"
├── run_id: string                    (AAM-generated, use as correlation key)
├── farm_verification: bool            (if true, run recon after push)
├── source: SourceSpec
│   ├── pipe_id: string               (THE canonical ID — use this everywhere)
│   ├── system: string                (vendor: salesforce, netsuite, etc.)
│   ├── adapter: string               (rest_api|jdbc|kafka|ipaas|webhook)
│   ├── endpoint_ref: dict            (connection details)
│   ├── credentials_ref: string?      (vault URI)
│   └── query: string?                (extraction filter)
├── transform: TransformSpec?
│   ├── schema_map: dict              (source → target field mapping)
│   ├── grain: string?
│   ├── period_field: string?
│   └── period_format: string?
├── target: TargetSpec
│   ├── dcl_url: string               (where YOU push data — DCL's /ingest)
│   ├── auth_token_ref: string?
│   ├── tenant_id: string?
│   └── snapshot_name: string?
├── provenance: dict
│   ├── run_timestamp: string
│   └── triggered_by: string
└── limits: RunLimits
    ├── max_rows: int
    ├── timeout_seconds: int
    └── retry_count: int
```

---

## Change 2: Use Manifest pipe_id in DCL Push (Path 3)

**CRITICAL:** When pushing data to DCL, the `x-pipe-id` header and any `pipe_id` in the payload **MUST** use the `pipe_id` from the manifest's `source.pipe_id`. This is the join key that DCL uses to match your data with AAM's schema blueprint. If these don't match, DCL cannot file your data.

### DCL Push Payload (what you send to DCL)

```
POST /api/dcl/ingest
Headers:
  x-run-id: string              (from manifest.run_id)
  x-pipe-id: string             (from manifest.source.pipe_id)
  x-schema-hash: string         (computed from your data schema)
  x-api-key: string
Body:
  source_system: string          (from manifest.source.system)
  tenant_id: string             (from manifest.target.tenant_id)
  snapshot_name: string          (from manifest.target.snapshot_name)
  run_timestamp: string          (from manifest.provenance.run_timestamp)
  schema_version: string         (first 16 chars of schema_hash)
  row_count: int
  rows: list[dict]              (actual data rows)
```

---

## Change 3: Handle New DCL Rejection Response

DCL will now return HTTP 422 with a structured `NO_MATCHING_PIPE` error if you push data for a `pipe_id` that has no schema blueprint. Your `push_results` handling should capture this distinctly from other errors.

### DCL Feedback Schemas

**Success (HTTP 200):**
```json
{
  "dcl_run_id": "uuid",
  "pipe_id": "string",
  "rows_accepted": 20,
  "schema_drift": false,
  "drift_fields": [],
  "matched_schema": true,
  "schema_fields": ["id", "email", "revenue"],
  "timestamp": "ISO string"
}
```

**Rejection (HTTP 422 — NEW):**
```json
{
  "error": "NO_MATCHING_PIPE",
  "pipe_id": "string",
  "message": "No schema blueprint found for this pipe_id.",
  "hint": "Ensure AAM has exported pipe definitions via /export-pipes",
  "timestamp": "ISO string"
}
```

**Failure (HTTP 4xx/5xx):**
```json
{
  "status_code": 500,
  "error": "string (first 500 chars)"
}
```

### Required Behaviors:

- **On NO_MATCHING_PIPE (422):** Log as CRITICAL error with the pipe_id. This means AAM's Structure Path and Farm's Content Path are misaligned. Do NOT retry — this is a configuration error, not a transient failure.
- **On schema_drift:** Log as WARNING with drift_fields. Continue processing but flag for operator review.
- **On success with farm_verification=true:** Trigger the recon function for this run_id.

---

## Change 4: Correlation Keys

To enable end-to-end traceability across all four paths, ensure these keys appear in your `push_results` and recon output:

| Key | Source | Purpose |
|-----|--------|---------|
| `run_id` | From JobManifest.run_id | Correlates AAM instruction to Farm execution |
| `pipe_id` | From JobManifest.source.pipe_id | Joins with AAM Export in DCL |
| `dcl_run_id` | From DCL push response | Correlates Farm push to DCL storage |
| `farm_run_id` | Your internal ID | Your execution trace |

---

## Unchanged: Your Ground Truth Output

Your existing ground truth schemas are well-structured and need no changes:

| Endpoint | Purpose | Key Fields |
|----------|---------|------------|
| `POST /api/business-data/generate` | Generate data + push to DCL | run_id, record_counts, push_results |
| `GET /api/business-data/profile/{run_id}` | Financial spine (56 fields/quarter) | ARR waterfall, P&L, SaaS metrics |
| `GET /api/business-data/ground-truth/{run_id}` | Answer key (90 metrics/quarter) | value, unit, primary_source, corroborating_source |
