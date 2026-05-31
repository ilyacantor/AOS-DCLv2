# DCL Ingest Contract — `POST /api/dcl/ingest-records`

The **real fabric connect** server side (AAM Blueprint v3.1 §3.6 decision (c)).
AAM transports raw enterprise records + the pipe schema it inferred; DCL maps,
resolves identity, converts to triples, and persists — all inbound. AAM no longer
converts records to triples or runs its own resolver.

This contract is the seam AAM's transport builds to. Breaking changes require
coordination with the AAM repo.

## Request

```jsonc
POST /api/dcl/ingest-records?replace=true
{
  "tenant_id":   "<uuid>",            // required, UUID (I2 machine key)
  "dcl_ingest_id": "<uuid>",          // required, UUID — idempotency key (I1; alias "run_id" accepted)
  "entity_id":   "<string>",          // required, non-empty business key (I2)
  "snapshot_name": "<entity_id>-<4hex>" | null,  // canonical I5 or null (read-side derives)
  "source_run_tag": "<string>" | null,
  "source_farm_manifest_id": "<string>" | null,
  "run_mode": "Dev" | "Prod",         // default Dev
  "pipes": [
    {
      "pipe_id":        "<uuid>",      // required, UUID — provenance
      "source_system":  "NetSuite",   // required — canonicalized by DCL (normalize_source_id)
      "fabric_plane":   "ipaas",       // required — ipaas|api_gateway|warehouse|event_bus
      "fabric_product": "<string>" | null,
      "domain":         "customer" | null,   // the business concept this pipe carries
      "identity_key":   "company_name" | null, // field whose VALUE is the party identity
      "record_key_field": "customer_id" | null, // source natural key (default "id")
      "records": [ { "<field>": <value>, ... }, ... ]  // raw records, opaque field→value
    }
  ]
}
```

Query params: `?replace=true` (deactivate prior run for the entity, then ingest)
or `?append=true` (multi-batch under one `dcl_ingest_id`). Default: 409 if the run
already exists.

### Pipe semantics
- **`domain` + `identity_key` present** → DCL resolves that field's value to a
  canonical identity (4-tier: exact → alias → pattern → fuzzy → discovery) and
  stamps `canonical_id` / `resolution_method` / `resolution_confidence` on every
  triple built from the record. `concept` = `domain`; `property` = source field
  name. Use this for SE business records (customer / vendor / invoice / …).
- **No `domain`** → no resolution; DCL's Live Semantic Mapper classifies each
  field to a concept. Use this for metric pipes (e.g. cloud-spend). `identity_key`
  without `domain` is a 422.
- One `entity_id` per call; all pipes inherit it.

## Resolution → persistence vocabulary
The resolver's method maps to `semantic_triples.resolution_method`
(`deterministic` | `fuzzy` | `manual` | NULL):

| resolver method | confidence | triple resolution_method | HITL |
|---|---|---|---|
| exact / alias / pattern / discovery | 1.0 / .95 / .85 / .99 | `deterministic` | — |
| fuzzy (≥ auto 0.90) | score | `fuzzy` | `auto_applied` row (audit) |
| fuzzy ([0.65, 0.90)) | score | `fuzzy` | `pending` row (operator review) |
| rejected (no candidate, discovery off) | — | NULL + warning | — |

Operator approval of a `pending` row (`POST /api/dcl/resolver/hitl/{id}/decide`)
promotes the bound triples to `manual` @ 0.99.

## Response `201`
```jsonc
{
  "dcl_ingest_id", "tenant_id", "entity_id",
  "records_seen", "pipes",
  "triple_count", "triples_written", "concept_summary",
  "resolution_summary": { "discovery": 1, "fuzzy": 1, ... },  // by resolver method
  "hitl_queue_ids": ["..."],
  "warnings": [ { "type": "non_persona_concept|unmapped_field|identity_rejected",
                  "pipe_id", "field"?, "concept"?, "detail" } ]
}
```

`warnings` is the loud, non-silent record of fields DCL could not place (the Live
Mapper produced no concept, or the concept routes to no persona) and identities it
could not resolve. Nothing is dropped silently.

## Errors
- `422 ENTITY_ID_REQUIRED` — missing/empty `entity_id`.
- `422 RESOLVER_CONTRACT` — `identity_key` without `domain`.
- `422 PROVENANCE_INCOMPLETE` — pipe missing `fabric_plane`.
- `422 NO_TRIPLES_PRODUCED` — every field unmapped / non-persona (see `warnings`).
- `400` — non-UUID `tenant_id` / `dcl_ingest_id` / `pipe_id`, or empty `pipes`.
- `409 RUN_ALREADY_EXISTS` — run exists; use `?replace=true` or `?append=true`.

## Idempotency
Same `dcl_ingest_id` + `?replace=true` is a clean re-run: triples replaced
(entity-scoped), canonicals deduped on normalized value (`ON CONFLICT`), HITL rows
deduped on (tenant, domain, norm(left), norm(right), status). Note: a `pending`
match that later scores into the auto band produces a distinct `auto_applied` row
(different status) — both rows are retained by design.

## Resolver queue (operator surface)
- `GET  /api/dcl/resolver/hitl?tenant_id=…[&status=…&domain=…]` — pending +
  auto-applied (or filter by status).
- `POST /api/dcl/resolver/hitl/{hitl_queue_id}/decide` `{decision, decided_by}`.
