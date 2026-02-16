# DCL Agent — Trifecta Flow Fixes

## Your Role

You are the DCL (Data Catalog Layer) module. You are the smart warehouse — you receive schema blueprints from AAM and data from Farm, and you unify them via a late-binding join on `pipe_id`. NLQ queries you for answers.

- **Path 1 — Structure Path (AAM → you):** AAM pushes pipe definitions via `/export-pipes`. This tells you what pipes exist, what fields they have, and how they're organized by fabric plane.
- **Path 3 — Content Path (Farm → you):** Farm pushes data rows via `/ingest`, tagged with a `pipe_id`. You JOIN this data with the schema blueprint from Path 1 on `pipe_id`.

**If content arrives without a matching schema, you MUST reject it with a structured error. Silent acceptance is the single worst failure mode in this architecture.**

---

## Change 1: Reject Ingest Without Matching Schema

**CURRENT BUG:** The `/ingest` endpoint accepts data even when no matching pipe schema exists from the Export. This causes silent data loss — rows land in DCL but can never be joined with their schema, so NLQ can never query them, and Farm's recon function gets meaningless results.

**FIX:** Before accepting any ingest payload, check whether a pipe definition exists for the incoming `pipe_id` (from the `x-pipe-id` header). If no match exists, return HTTP 422 with a structured error.

### Ingest Guard Logic

```python
# Pseudocode for /api/dcl/ingest

pipe_id = request.headers['x-pipe-id']

# Check if AAM has exported a schema for this pipe
pipe_schema = lookup_pipe_definition(pipe_id)

if pipe_schema is None:
    return Response(
        status=422,
        body={
            "error": "NO_MATCHING_PIPE",
            "pipe_id": pipe_id,
            "message": f"No schema blueprint exists for pipe_id: {pipe_id}.",
            "hint": "Ensure AAM has run /export-pipes and that the pipe_id matches between Export and Runner manifest.",
            "available_pipes": list_known_pipe_ids(),  # OPTIONAL: help diagnose mismatches
            "timestamp": now_iso()
        }
    )

# Schema exists — proceed with existing ingest logic
# (schema_drift detection, row acceptance, etc.)
```

---

## Change 2: Enrich Ingest Success Response

Your existing success response includes `rows_accepted`, `schema_drift`, and `drift_fields`. Add fields to confirm the join succeeded and what schema was matched:

### Success Response (HTTP 200)

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

New fields:
- `matched_schema` (bool): Confirms the join succeeded. Always `true` on 200 (since 422 handles the false case).
- `schema_fields` (list[string]): The fields from the Export blueprint for this pipe. Lets Farm verify DCL is filing data against the correct blueprint.

### Rejection Response (HTTP 422 — NEW)

```json
{
  "error": "NO_MATCHING_PIPE",
  "pipe_id": "the-pipe-id-that-was-sent",
  "message": "No schema blueprint found for this pipe_id.",
  "hint": "Ensure AAM has exported pipe definitions via /export-pipes",
  "available_pipes": ["sf-crm-001", "ns-erp-001", "..."],
  "timestamp": "ISO string"
}
```

The `available_pipes` field is optional but strongly recommended — it lets Farm and the operator immediately see whether this is a naming mismatch vs. a missing export.

---

## Inbound Schemas Reference

### From AAM — Export Payload (/export-pipes)

This is the schema blueprint. Store it and use it as the join target for ingest data.

```
DCLExportResponse
├── aod_run_id: string?
├── timestamp: string
├── source: "aam"
├── total_connections: int
└── fabric_planes: list
    ├── plane_type: string
    ├── vendor: string
    ├── connection_count: int
    ├── health: string
    └── connections: list
        ├── pipe_id: string          ← THE JOIN KEY
        ├── candidate_id: string     (provenance only, do not use for joining)
        ├── source_name: string
        ├── vendor: string
        ├── category: string
        ├── governance_status: string?
        ├── fields: list[string]
        ├── entity_scope: string?
        ├── identity_keys: list?
        ├── transport_kind: string?
        ├── modality: string?
        ├── change_semantics: string?
        ├── health: string
        ├── last_sync: string?
        ├── asset_key: string
        └── aod_asset_id: string?
```

### From Farm — Ingest Payload (/ingest)

This is the data content. Join with the Export schema on `pipe_id`.

```
POST /api/dcl/ingest
Headers:
  x-run-id: string              ← AAM run correlation
  x-pipe-id: string             ← THE JOIN KEY (must match Export connections[].pipe_id)
  x-schema-hash: string
  x-api-key: string
Body:
  source_system: string
  tenant_id: string
  snapshot_name: string
  run_timestamp: string
  schema_version: string
  row_count: int
  rows: list[dict]
```

---

## The Late-Binding Join

Your core architectural role is the Unification Engine. When NLQ queries for data, you join:

- **Structure (from Export):** pipe_id → fields, vendor, category, entity_scope, modality, transport_kind
- **Content (from Ingest):** pipe_id → actual data rows

This join MUST happen on `pipe_id`. If the `pipe_ids` don't match between Export and Ingest, the data is orphaned. The ingest guard (Change 1) prevents orphaned data from accumulating.

---

## Integration Test: How to Verify Your Changes Work

| Step | Action | Expected Result |
|------|--------|-----------------|
| 1 | AAM runs `/export-pipes` | You receive and store pipe schemas with real pipe_ids |
| 2 | Farm pushes data with matching pipe_id | You return 200 with `matched_schema: true` |
| 3 | Farm pushes data with WRONG pipe_id | You return 422 `NO_MATCHING_PIPE` |
| 4 | Farm runs recon | Your data is queryable and matches ground truth |
| 5 | NLQ queries a metric | You return joined structure + content |
