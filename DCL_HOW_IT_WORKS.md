# DCL — How It Works

DCL is the semantic context layer of AutonomOS. It is a Postgres-backed triple store with a graph engine and a set of HTTP read APIs. The canonical, live path is the Farm-mode pipeline: Farm converts source data into triples, posts them to DCL, DCL validates and persists them under a per-tenant snapshot, and downstream services (Console, NLQ, Convergence) read from DCL to answer business questions.

## What DCL actually does (live Farm path)

- **Accepts pre-converted triples.** `POST /api/dcl/ingest-triples` is the canonical write endpoint. It validates UUID identity (`tenant_id`, `dcl_ingest_id`), checks every triple's concept against the ontology registry and persona-domain registry, persists into `semantic_triples`, then atomically swaps the active snapshot pointer in `tenant_runs`. Idempotent on `dcl_ingest_id`. Records every run in `ingest_log`. **No mapping, no LLM, no vector retrieval on this path.**
- **Resolves semantic questions for NLQ.** `/api/dcl/semantic-export*`, `/api/dcl/resolve`, `/api/dcl/graph/path` return concept-to-source maps, join paths, and confidence breakdowns. Backed by a structural metadata graph built once at startup from the ontology, contour map, and AAM-supplied semantic edges. Returns "where do I go to answer X," not the answer itself.
- **Builds Sankey graph snapshots.** `POST /api/dcl/run` (Farm mode) aggregates active triples by source/concept/period and returns the graph structure. Reads `semantic_triples` directly through indexed columns. Deterministic SQL aggregation.
- **Reconciles across systems.** Three endpoints. `/api/dcl/recon` runs five chain checks against Farm and AAM HTTP (counts match, domain completeness, persona coverage, source presence, ontology coverage). `/api/dcl/reconciliation` and `/api/dcl/reconciliation/cross-system` aggregate ingest receipts vs pipe definitions vs source-of-record lists and surface deltas.
- **Surfaces triple monitoring.** `/api/dcl/triples/*` endpoints (overview, runs, identity-checks, browse, browse-batch, engagement, resolution-summary, persona-stats, deactivate-run, dashboard-data, contextualization-summary) drive the Console operator surface.
- **Receives pipe definitions from AAM.** `POST /api/dcl/export-pipes` and `/dispatch` accept AAM's pipe blueprints and dispatch signals so DCL can validate ingests and surface pipe receipts.
- **Tracks temporal and provenance reads.** `/api/dcl/temporal/*`, `/api/dcl/provenance/{metric_id}`, `/api/dcl/persona-definitions/{metric_id}`.

## Technology

FastAPI + Python backend, React 18 + Vite operator UI (the Console surfaces it), Postgres on Supabase (single source of truth), Redis cache layer in front of hot reads. The schema sits behind a contract document (`SCHEMA_CONTRACT.md`) that gates breaking changes for cross-service consumers.

## Uniqueness

Triples carry first-class confidence (`exact / high / medium / low`), resolution method (`deterministic / fuzzy / manual`), canonical ID for entity resolution, and full provenance back to system, table, field, pipe, and run. Snapshot-named runs (`{entity_id}-{short_hash}`) live alongside one another in `semantic_triples`; the active snapshot is selected by an atomic pointer swap so readers never see a half-written graph. Ontology validation rejects any triple whose concept isn't registered.

## Governance and security (production posture)

- **Multi-tenancy is structural.** Every owned table (`semantic_triples`, `dimension_values_v2`, `tenant_runs`, `tenant_registry`) carries `tenant_id` (UUID); all composite indexes are leading-keyed on it.
- **Identity is enforced at the canonical write boundary.** `POST /api/dcl/ingest-triples` requires UUID `tenant_id` and rejects malformed input — no fallback to a default tenant on the live ingest path.
- **Audit by construction.** Every triple records source, confidence, resolution method, and run identifier. Every ingest is logged.
- **Schema contract.** Breaking changes to the four owned tables require coordination with downstream services before merge.
- **Read isolation across services.** Convergence holds direct SELECT access only and writes through the ingest API; DCL never reads or writes Convergence-owned tables.

## Use of AI

**None on the canonical Farm-mode read or write path.** Ingest is deterministic ontology validation. Query resolution is deterministic SQL. The semantic graph is built deterministically at startup. DCL invokes no LLM in any live request.

## Use of learning / RAG

**None on the canonical Farm-mode path.** DCL does not vectorize triples and does not retrieve by similarity for any user-facing query. DCL's contribution to enterprise learning is the ontology, confidence tiering, and canonical-ID resolution — the schema upstream services use to keep mapping decisions auditable.

## Speed and performance

- Indexed access on every read path: `(tenant_id, entity_id, concept)`, `(tenant_id, concept, period)`, `(canonical_id)` partial, `(tenant_id, is_active)` partial, plus a fabric-domain expression index on `split_part(concept, '.', 1)`.
- Latency targets are gated in CI: cold start under 5 s, warm reads under 200 ms. Reconciliation and ingest-stats endpoints are continuously monitored by `tests/test_latency.py`.
- Redis cache fronts hot read endpoints (recon, ingest stats, dashboards).
- Atomic swap-and-deactivate keeps readers unblocked during ingest.

## Enterprise-grade aspects

- Tenant isolation by composite key on every owned table; per-tenant snapshot pointers in `tenant_runs` support parallel runs across tenants and entities without contention.
- 13 ordered, reversible migrations.
- Snapshot naming per `(tenant, entity)` lets a single deployment serve many tenants with isolated histories and lossless rollback to any prior snapshot.
- Idempotent ingest API (run-ID keyed) and atomic pointer swap keep the store consistent under partial failures.

## Tested via Farm

- **Ground truth at runtime.** Reconciliation tests pull expected values from Farm's `/api/business-data/ground-truth/{tag}` endpoint at test time — no hardcoded expectations.
- **End-to-end pipeline tests** drive AOD → AAM → Farm → DCL ingest → query and assert tenant + entity identity at every stage.
- **Four fabric planes covered** (iPaaS, API gateway, data warehouse, event bus) — event-stream coverage alone produces 60+ triples per entity.
- **Latency suite** runs against the warm system and gates the build at the documented thresholds.
- **Frontend acceptance gate.** Every release passes Playwright tests that drive real operator flows in the browser.

---

## RACI delta — what disagrees with v8.6

This section records gaps between what DCL actually runs and what RACI says it owns. None of these affect the live Farm-mode path described above; all are either deprecated routes still wired up or boundary code that has drifted.

**Code present in DCL that RACI assigns elsewhere:**
- **Mapping pipeline.** `backend/engine/mapping_service.py`, `backend/engine/rag_service.py` (Pinecone + OpenAI embeddings), `backend/llm/mapping_validator.py`, `backend/eval/mapping_evaluator.py`, `backend/semantic_mapper/heuristic_mapper.py`. Reachable only via `POST /api/dcl/run` with `mode="AAM"` and `POST /api/dcl/batch-mapping`. The constitution says this old DCL pipe-ingest path is deprecated, but the routes are not 410'd. Mapping is Farm/AAM territory per RACI.
- **Farm proxy.** `backend/farm/routes.py` exposes `/api/farm/*` from DCL, proxying Farm v1 and v2 endpoints. Farm owns its own data; DCL hosting a proxy is boundary drift.
- **NLQ-style intent parsing.** `backend/nlq_client.py` and `backend/graph_resolver.py` are present but unreachable. Comment at the head of `nlq_client.py` admits it belongs in the NLQ repo.

**Capabilities RACI assigns to DCL but wired inconsistently:**
- **SE query resolution.** `POST /api/dcl/query` reads the in-memory `IngestStore` materialized buffer, not `semantic_triples`. After the triple ingest migration, this path does not see data written via `/api/dcl/ingest-triples`. The Sankey graph and triple-monitor endpoints read triples correctly; the metric query endpoint does not.
- **Default-tenant fallbacks.** `tenant_id="default"` is still accepted on `/mai/status`, `/api/dcl/semantic-export`, and `POST /api/dcl/query`. The canonical write path (`/api/dcl/ingest-triples`) does not allow this, but the read paths do.
- **Two graph structures.** The semantic resolver graph (built from ontology + contour map + AAM edges at startup) and the triple-derived Sankey graph (read on demand from `semantic_triples`) are not unified.

These are the deltas between RACI as written and what runs today. They are scoped here for visibility, not as a fix plan.
