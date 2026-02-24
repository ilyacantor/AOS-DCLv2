# DCL Storage & Database Overview

> Comprehensive guide to how DCL stores, caches, and retrieves data.
> Written 2026-02-24. Covers the full backend storage stack.

---

## How Data Enters DCL

DCL receives data through two paths, both initiated by AAM:

1. **Structure Path** — AAM calls `POST /api/dcl/export-pipes` with pipe schemas (field definitions, governance metadata, schema hashes). This tells DCL "here's what the data will look like." Each pipe gets a UUID (`pipe_id`) that acts as the join key across the entire AOS chain.

2. **Content Path** — Farm (or any Runner) calls `POST /api/dcl/ingest` with actual row data. Each push carries the same `pipe_id` in the `x-pipe-id` header. DCL validates that the pipe was previously registered (schema-on-write), then accepts or rejects the payload.

These two paths meet at the **Ingest Guard**: if a pipe_id hasn't been registered via the Structure Path, the Content Path rejects it with `422 NO_MATCHING_PIPE`.

---

## Storage Layers (Top to Bottom)

DCL uses a four-tier storage hierarchy. Each tier serves a different purpose.

### Tier 1: PostgreSQL (Supabase) — Source of Truth

**What lives here:** Configuration and semantic data that must survive full redeploys.

| Table | What It Stores | Written By | Read By |
|-------|---------------|------------|---------|
| `pipe_definitions` | Pipe schemas from AAM (pipe_id, fields, category, vendor, governance) | export-pipes endpoint | Ingest Guard, graph builder |
| `pipe_export_receipts` | Audit trail of export-pipes calls (timestamps, pipe counts, snapshot names) | export-pipes endpoint | Admin/debugging |
| `field_concept_mappings` | Field-to-ontology mappings (e.g., `salesforce.Amount` → `finance.revenue`) | Batch mapping service | Graph builder, query engine |
| `ontology_concepts` | The 16 ontology concepts (id, name, cluster, description) | Config sync at startup | Mapping service, graph builder |
| `persona_profiles` | Persona definitions (CFO, CRO, COO, CTO, CHRO) | Config sync at startup | Persona view |
| `persona_concept_relevance` | Which concepts matter to which persona (with relevance scores) | Config sync at startup | Persona view, graph builder |
| `source_systems` | Registered data sources (type, vendor, trust score) | Mapping service | Schema loader |

**Connection pooling:** A single shared `psycopg2.pool.SimpleConnectionPool` in `backend/core/db.py`, min 2 / max 3 connections, 30-second connect timeout. All modules (MappingPersistence, PipeStore, SchemaLoader, PersonaView) borrow from this one pool via `get_connection()` context manager.

**When Postgres is unavailable:** The system falls back to Redis, then disk, then hardcoded defaults. Nothing crashes — Postgres is preferred but optional.

### Tier 2: Redis — Write-Through Cache

**What lives here:** Operational data that should survive backend restarts but doesn't need to live forever. All keys auto-expire after 24 hours.

| Key Pattern | Type | What It Stores |
|------------|------|---------------|
| `dcl:ingest:receipts` | Hash | Run receipt metadata (one entry per ingest push) |
| `dcl:ingest:receipt_order` | List | Ordered list of receipt keys (for FIFO eviction) |
| `dcl:ingest:rows:{run_id}:{pipe_id}` | String | Actual row data from each ingest push (JSON array) |
| `dcl:ingest:schemas` | Hash | Last-seen schema per pipe (for drift detection) |
| `dcl:ingest:drift_events` | String | Schema drift events (field additions/removals) |
| `dcl:ingest:activity_log` | String | 3-phase activity tracking (structure → dispatch → content) |
| `dcl:ingest:drop_log` | String | Rejected ingestion attempts with reasons |
| `dcl:ingest:materialized:{key}` | String | Pre-aggregated metric data points |
| `dcl:pipes:definitions` | Hash | Pipe definitions (mirrors Postgres) |
| `dcl:pipes:export_receipts` | String | Export receipts (mirrors Postgres) |

**Write pattern:** Every ingest and export-pipes call writes to Redis immediately after updating in-memory state. This is synchronous (not async) but fire-and-forget — if Redis fails, the write is skipped silently and the operation succeeds anyway.

**Multi-worker sync:** Each Uvicorn worker has its own in-memory state. Before serving any read endpoint, the worker calls `_sync_from_redis()` (3 Redis GETs, ~1ms) to pick up data written by other workers.

**When Redis is unavailable:** The system runs in-memory only. Data doesn't survive restarts but everything works. Disk cache provides partial backup.

### Tier 3: Disk — JSON Fallback

**What lives here:** Backup copies of operational data, used only when both Postgres and Redis are unavailable.

| File | What It Stores |
|------|---------------|
| `backend/cache/ingest_cache.json` | Receipts, schema registry, drift events, activity log, drop log, materialized points. **Raw rows are excluded** (too large). |
| `backend/cache/pipe_cache.json` | Pipe definitions and export receipts. |

**Write safety:** All disk writes use atomic operations (`tempfile.mkstemp()` + `os.replace()`) to prevent corruption from partial writes or crashes.

**When used:** Only during startup rehydration, and only if Redis returned nothing.

### Tier 4: In-Memory — Read Cache

**What lives here:** Everything. All reads are served from in-memory Python dictionaries for O(1) access. The other tiers exist only for durability.

**Memory limits (enforced via FIFO eviction):**

| Buffer | Max Size | What Happens When Full |
|--------|----------|----------------------|
| Run receipts | 500 | Oldest receipt evicted (along with its rows) |
| Row buffer | 50,000 total rows | Oldest receipt's rows evicted |
| Schema registry | 5,000 entries | Oldest schemas evicted |
| Drift events | 1,000 | Oldest events dropped |
| Activity log | 500 entries | Oldest entries dropped |
| Drop log | 500 entries | Oldest entries dropped |
| Materialized points | 50,000 | Oldest pipe's points evicted |
| Export receipts | 100 | Oldest receipts dropped |

**Thread safety:** `threading.Lock()` protects writes to IngestStore and PipeDefinitionStore. Cache reads for ontology concepts, mappings, and schemas use time-checked class variables (safe for read-mostly patterns).

---

## What Happens During Key Operations

### On Backend Startup

1. **Security check** — validate metadata-only mode, scan for payload write violations
2. **Config sync** — read `ontology_concepts.yaml` and `persona_profiles.yaml`, upsert into Postgres (idempotent)
3. **Semantic graph build** — load ontology, mappings, AAM edges into in-memory graph
4. **Pipe store rehydration** — load pipe definitions: try Postgres → try Redis → try disk
5. **Ingest store rehydration** — load receipts, schemas, activity: try Redis → try disk (rows are skipped if over memory limit)

### On `POST /api/dcl/export-pipes` (Structure Path)

1. Parse pipe definitions from request body
2. Write to in-memory `PipeDefinitionStore` (thread-locked)
3. Write to Postgres (`INSERT ... ON CONFLICT DO UPDATE`)
4. Write to Redis (`HSET` per definition + `SET` for receipts)
5. Write to disk (`pipe_cache.json`)
6. Record activity log entry (phase = "structure")

### On `POST /api/dcl/ingest` (Content Path)

1. Extract `x-pipe-id` header, validate against PipeDefinitionStore
2. If pipe_id not found → record Drop, return 422
3. Compute schema hash, compare to registry, detect drift
4. Tag every row with metadata (`_pipe_id`, `_source_system`, `_inserted_at`, etc.)
5. Store in-memory: receipt + rows + schema record (thread-locked)
6. Enforce memory limits (evict oldest if over 500 receipts or 50k rows)
7. Write-through to Redis (receipts, rows, schemas, drift, activity)
8. Write metadata to disk (rows excluded)
9. Optionally materialize metric data points

### On `POST /api/dcl/run` (Graph Build)

1. Load sources: from IngestStore receipts (preferred) or SchemaLoader (demo CSVs / AAM API)
2. Load mappings: from Postgres cache (60s TTL) or heuristic mapper
3. Load ontology: from Postgres cache (300s TTL) or YAML
4. Load persona relevance: from Postgres cache (300s TTL) or hardcoded defaults
5. Build 4-layer Sankey graph (L0 Pipeline → L1 Sources → L2 Ontology → L3 Personas)
6. Return `GraphSnapshot` — **no storage writes**

### On `POST /api/dcl/query` (Data Query)

1. Parse question, resolve metrics and dimensions
2. Search IngestStore rows first (fresh ingested data)
3. Fall back to `fact_base.json` if no matching rows
4. Return aggregated result with provenance — **no storage writes**

---

## Connection Pool Architecture

**One shared Postgres pool** in `backend/core/db.py`:

| Pool | Min | Max | Used By |
|------|-----|-----|---------|
| Shared pool | 2 | 3 | persist_mappings.py, pipe_store.py, schema_loader.py, persona_view.py |

Additionally, **two modules create standalone connections** (no pool):
- `config_sync.py` — one connection at startup, closed after sync
- `mapping_evaluator.py` — one connection for CLI evaluation, closed after use

**One shared Redis client** in `backend/core/redis_client.py` — used by IngestStore and PipeStore.

**Worst case:** 3 (shared pool) + 1 (config_sync) + 1 (evaluator) = **5 connections per worker process**. With 2 Uvicorn workers, that's up to 10 connections.

**Connection mode:** Transaction mode (port 6543) — Supabase pooler releases the upstream Postgres connection after each transaction, so app-side pool slots recycle quickly.

**Shutdown:** Pool is closed via FastAPI's lifespan handler on graceful shutdown, preventing abandoned connections on Supabase's pooler.

### Cache TTLs

| Cache | TTL | What It Caches |
|-------|-----|---------------|
| Ontology concepts | 300s (5 min) | The 16 ontology concepts from Postgres |
| Field mappings | 60s (1 min) | All field→concept mappings from Postgres |
| Schema loader (demo/stream) | 300s (5 min) | Parsed source schemas |
| Schema loader (AAM) | 120s (2 min) | Pipe schemas fetched from AAM API |
| Persona concepts | 300s (5 min) | Persona→concept relevance scores |
| Source normalizer | No TTL | Registry from Farm API (circuit breaker: 120s cooldown on failure) |
| Redis keys | 86,400s (24h) | All Redis data (auto-expiry) |

---

## External Services

| Service | Used For | Required? | What Happens Without It |
|---------|----------|-----------|------------------------|
| **Supabase Postgres** | Persistent storage of mappings, ontology, personas, pipes | No | Falls back to Redis → disk → hardcoded defaults |
| **Redis** | Write-through cache, multi-worker sync | No | Runs in-memory only; data lost on restart |
| **Pinecone** | Vector DB for semantic mapping lessons (RAG) | No | Heuristic-only mappings (no AI enhancement) |
| **OpenAI** | Embeddings for Pinecone; LLM mapping validation in Prod mode | No | Dev mode uses mock embeddings; Prod mode degrades |
| **Farm API** | Source registry for normalizer; runtime data generation | No | Uses local CSV schemas in Demo mode |
| **AAM API** | Pipe schemas and dispatch signals | No | Manual export-pipes calls or demo mode |

---

## Zero-Trust Design

DCL enforces a "metadata-only" policy — raw payload data is never written to disk. This is checked at startup:

- **MetadataEventBuffer** (`core/metadata_buffer.py`) — buffers only request IDs, schema metadata, and timing info. In-memory only, 300s TTL, max 10k entries.
- **FabricPointerBuffer** (`core/pointer_buffer.py`) — buffers Fabric Pointers (offsets/cursors), never actual payloads. Just-in-time fetch from Fabric when the semantic mapper needs data; payload discarded immediately after processing.
- The `ingest_cache.json` disk file intentionally **excludes row data** — only metadata (receipts, schemas, activity) is persisted.

---

## Optimizations Applied

### 1. Fixed N+1 Query in schema_loader (Done)

**Was:** `load_stream_sources()` held two connections simultaneously — one for `source_systems`, then a second per-source for `field_concept_mappings`. With N sources, this was N+1 queries and 2 concurrent connection holds.

**Now:** Single connection, two queries. The second uses `WHERE source_id = ANY(%s)` to fetch all mappings in one pass, then groups them in Python. Drops from N+1 to 2 queries and from 2 to 1 concurrent connection hold.

### 2. Added FastAPI Shutdown Hook (Done)

**Was:** No shutdown handler. Pooled connections were abandoned on redeploy, eating into Supabase's connection limit until they timed out.

**Now:** `main.py` uses a `lifespan` context manager that calls `MappingPersistence.close_pool()` and `PipeDefinitionStore.close_pool()` on graceful shutdown. Added `close_pool()` static method to PipeDefinitionStore.

### 3. Transaction Mode (Done)

Connection string switched from port 5432 (Session mode) to 6543 (Transaction mode). DCL's queries are all short-lived `SELECT` and `INSERT ... ON CONFLICT` with no session-level features — Transaction mode releases the upstream Postgres connection after each statement.

### 4. Reduced Pool Size to 3 (Done)

**Was:** `POOL_MAX_CONN` default was 5 per pool. With caching (60–300s TTLs), actual concurrent DB hits are rare.

**Now:** Default is 3. Worst case drops from 12 to 8 connections per worker. Override with `DCL_POOL_MAX_CONN` env var if needed.

---

### 5. Consolidated to Single Shared Postgres Pool (Done)

Created `backend/core/db.py` with one `SimpleConnectionPool`. Both MappingPersistence and PipeStore now borrow from it. Max connections per worker dropped from 6 to 3.

### 6. Shared Single Redis Client (Done)

Created `backend/core/redis_client.py`. IngestStore and PipeStore now share one connection. NarrationService no longer uses Redis at all (dead code removed).

### 7. Removed Dead NarrationService Redis Code (Done)

Removed `_get_redis()`, `_fetch_ingest_logs()`, and the `redis` import from `narration_service.py`. The service is now pure in-memory message storage as intended.

---

## Remaining Optimization Opportunities

### Eliminate Standalone Connection in config_sync.py (Low Effort)

`config_sync.py` creates its own direct `psycopg2.connect()` call, bypassing the shared pool. Having it borrow from `backend.core.db.get_connection()` would give it proper lifecycle management for free.

### Row Data Durability (Design Decision)

Ingested rows live in Redis for 24 hours, then auto-expire. Disk cache intentionally excludes rows (zero-trust policy). After 24h or a restart without Redis, raw row data is gone — materialized metric points survive longer. Consider a scheduled materialization job or external archival if long-term row retention matters.

---

## Summary: Where Everything Lives

```
                    ┌──────────────┐
                    │   Postgres   │  Source of truth
                    │  (Supabase)  │  Survives everything
                    │              │  Mappings, ontology, personas, pipes
                    └──────┬───────┘
                           │ Startup load + write-through
                    ┌──────▼───────┐
                    │    Redis     │  Fast cache, 24h TTL
                    │              │  Rows, receipts, activity, schemas
                    │              │  Multi-worker sync point
                    └──────┬───────┘
                           │ Fallback if Redis empty
                    ┌──────▼───────┐
                    │  Disk JSON   │  Crash recovery
                    │  (backend/   │  Metadata only (no rows)
                    │   cache/)    │  Atomic writes
                    └──────┬───────┘
                           │ Always populated
                    ┌──────▼───────┐
                    │  In-Memory   │  All reads served here
                    │  (Python     │  FIFO eviction at limits
                    │   dicts)     │  Lost on restart
                    └──────────────┘
```

---

## Reference: Environment Variables

| Variable | Default | Controls |
|----------|---------|----------|
| `DATABASE_URL` | (none) | Postgres connection string (port 6543 for Transaction mode) |
| `REDIS_URL` | (none) | Redis connection string |
| `DCL_POOL_MIN_CONN` | 2 | Minimum pooled Postgres connections |
| `DCL_POOL_MAX_CONN` | 3 | Maximum pooled Postgres connections |
| `DCL_DB_CONNECT_TIMEOUT` | 30 | Postgres connect timeout (seconds) |
| `DCL_POOL_RETRY_COOLDOWN` | 30 | Seconds to wait before retrying failed pool init |
| `DCL_ONTOLOGY_CACHE_TTL` | 300 | Ontology cache lifetime (seconds) |
| `DCL_MAPPINGS_CACHE_TTL` | 60 | Mappings cache lifetime (seconds) |
| `DCL_SCHEMA_CACHE_TTL` | 300 | Schema cache lifetime (seconds) |
| `DCL_CB_COOLDOWN` | 120 | Source normalizer circuit breaker cooldown (seconds) |
