# Convergence Transition Master

**Version:** v1.0
**Status:** Spec-in-progress. No code.
**Scope:** Convergence refactor from hardcoded Meridian/Cascadia combined-tenant to a generic two-AOS merge over any pair selected from Farm's catalog.
**Out of scope:** AWS migration, AOD/AAM internal changes, Mai Shape 4 event projection, Edge Agent work.

Supersedes: fixture-based Convergence sections of convergence_MA_spec_v7.4.1, ME v2 Blueprint v0.1–v0.3, Farm overlap.py, DCL Identity Gates UI, combined-tenant model, all hardcoded Meridian/Cascadia wiring.

Companion documents: convergence_blueprint_master (steady-state product architecture), mai_blueprint_master (Mai concierge), AOS_MASTER_RACI_v8.6.

---

## 0. Why this exists

Today's Convergence pipeline works because Farm generates Meridian and Cascadia deterministically, and `overlap.py` seeds the customers, vendors, and CoA lines that appear in both. DCL's Identity Gates UI shows resolution workspaces because Farm's seeding guarantees they exist. None of this is identity resolution — it's bookkeeping against a fixture.

This blueprint makes the following true:

- Convergence is a function over two independently-generated AOS tenants, selected by the operator from a Farm-produced AOS catalog.
- Meridian and Cascadia are eradicated from code entirely. No demotion, no catalog entries, no special-case references anywhere. A pre-commit hook enforces this permanently.
- Identity resolution becomes a real Convergence engine with real failure modes, real confidence scoring, and real human-in-the-loop confirmation.
- Convergence engagement state is persistent: resolver decisions, HITL confirmations, and adjustments persist across runs.

Two things this blueprint does not do: it does not extract identity resolution into a shared service (AOD keeps its own vendor-canonicalization resolver; Convergence builds its own business-record resolver; a thin shared primitives library is the only overlap), and it does not fold Convergence surfaces into DCL (DCL stays AOS-only per RACI v8.6 and se_triples_conversion_build_plan_v2.2 §0).

### 0.1 Surface ruling (reference)

AOS has four surface categories with non-overlapping jobs. This blueprint obeys the split throughout.

- **Convergence (port 3010).** Standalone product. One source of truth and one source of action for Convergence. Owns engagement creation, pair selection, Resolutions (HITL), COFA workspace, adjustments, deliverables.
- **Platform.** Mai's backend brain — constitution, classification, supervised execution, tool registry, `mai_memory`, API surface (`/api/mai/*`), guided tour / onboarding walkthrough. No user-facing operator controls for pipeline actions.
- **Console.** Read-only cross-product monitoring, including Mai monitoring (engagement ledger, run ledger, human review queue, classification decisions). Cross-module E2E chain execution. Narrow action exceptions: emergency controls (cancel/cycle) and triggering a pipeline run against an already-created Convergence engagement. Does not create engagements, does not resolve HITL, does not drive COFA.
- **Modules (AOD, AAM, DCL, Farm, NLQ).** Each owns its own operator UI for its own capabilities. No operator work for the AOS pipeline lives in Platform or Console.

---

## 1. AOS Output Contract

Every AOS tenant must emit a predictable shape for Convergence to consume. Today this is implicit because Farm generates both sides. Making it explicit is the AOS↔Convergence interface.

### 1.1 Namespace model

The namespace list is **open, not fixed.** Convergence reads all concept namespaces that exist in an AOS tenant's triple store. Each namespace carries a `namespace_type` property that tells the resolver how to process it:

| namespace_type | Meaning | Resolver behavior |
|---|---|---|
| `business_record` | Contains entities that can be matched across tenants (customers, vendors, employees, products, CoA lines, IT assets) | Resolver runs matching tiers. HITL for ambiguous matches. |
| `financial_fact` | Contains financial data that is combined, not resolved (revenue, cogs, opex, ebitda_adjustment, cash_flow, pnl, equity, asset) | Combining workflow aggregates by entity_id. No resolution. |

This model lets Farm add new domains (contract.\*, facility.\*, etc.) without requiring a blueprint revision. The resolver discovers what to resolve based on namespace_type, not a hardcoded list.

### 1.2 Required properties per business_record namespace

Each business-record namespace must emit on every concept, or explicitly emit `null` with provenance:

- `display_name` — human-readable name
- `normalized_name` — lowercased, punctuation-stripped, suffix-stripped, whitespace-collapsed
- `source_system` — which SoR this record came from
- `source_record_id` — the SoR's native ID
- `entity_id` — the AOS entity slug (business key)
- `tenant_id` — the AOS tenant UUID

### 1.3 Identifier fields (domain-specific)

Identifiers are the highest-signal evidence for the resolver. Each namespace declares which identifiers are meaningful:

| Namespace | Identifier priority (highest first) |
|---|---|
| customer | `tax_id`, `duns`, `domain`, `normalized_name` |
| vendor | `tax_id`, `duns`, `domain`, `normalized_name` |
| employee | `email`, `government_id` (hashed), `normalized_name + hire_date` |
| coa | `account_code` (within AOS only; Convergence resolves on normalized name + type) |
| product | `sku`, `normalized_name + category` |
| it_asset | `vendor_canonical_name + instance_id` |

### 1.4 Stability and completion

- `tenant_id` and `entity_id` are stable across runs of the same AOS tenant.
- `source_record_id` is stable within a source system.
- An AOS tenant is "ready for Convergence consumption" when it passes the contract check and has a current run pointer.
- If the AOS tenant refreshes after a Convergence engagement has started resolving it, the engagement's resolver state flags stale for any entity whose content hash changed. HITL re-confirms. No silent revalidation.

### 1.5 Contract enforcement

**Convergence owns the contract check.** The check runs inside `POST /api/convergence/engagements` (engagement creation). It verifies that every required namespace is present, every required property is populated (or explicitly null with reason), and identifier coverage meets a minimum threshold (configurable per domain, initial default 80% coverage on at least one identifier per record).

Failures block engagement creation. Console surfaces the failure to the operator with a per-domain diagnostic. Mai (concierge) can explain failures if asked but does not own the check. This is a Convergence pre-flight concern, not a Mai concern.

---

## 2. Convergence Engagement Model

Convergence engagements are persistent. An engagement is not a disposable recompute — it accumulates resolver decisions and adjustments across sessions.

### 2.1 Engagement identity

An engagement is keyed on `(acquirer_tenant_id, target_tenant_id)` and gets its own `engagement_id` (UUID, Convergence-minted). The pair is stored in metadata; the UUID is what every Convergence table foreign-keys to.

Deterministic hashing of the pair is rejected — the same pair may run with different configurations, resolver versions, or HITL operators, and each needs a distinct identity.

One pair can have many engagements. Convergence's engagement list shows existing engagements for a pair alongside a "new engagement" option.

### 2.2 Persisted state

Convergence owns a new schema, `convergence_engagement`:

| Table | Purpose |
|---|---|
| `engagements` | One row per engagement. engagement_id, acquirer_tenant_id, target_tenant_id, created_at, created_by, status, resolver_version, config_snapshot |
| `resolver_decisions` | One row per resolved pair across any domain. engagement_id, domain, acquirer_record_id, target_record_id, confidence, evidence_json, tier_matched, hitl_state, hitl_operator, hitl_timestamp, content_hash_acq, content_hash_tgt |
| `engagement_adjustments` | Accounting adjustments applied during the engagement. engagement_id, adjustment_type, payload_json, applied_at, applied_by, source (manual/auto) |
| `engagement_runs` | History of engagement run invocations. engagement_id, run_id, started_at, completed_at, resolver_stats, triggered_by |

### 2.3 AOS catalog discovery and pair selection

Engagement creation is a Convergence action per the surface ruling.

- `GET /api/farm/catalog` — lists AOS templates and instances Farm has generated.
- `GET /api/convergence/catalog` — Convergence view of the same, filtered to AOS tenants passing the §1.5 contract check, annotated with existing engagements.
- `POST /api/convergence/engagements` — creates an engagement. Body: `{acquirer_tenant_id, target_tenant_id, config}`. Returns `engagement_id`. This is the one creation endpoint.

Pair selection: two clicks on Convergence's Engagements page. Convergence enforces distinct tenant_ids and contract compliance.

Console's role: read-only — iframes or API-reads the engagements list for monitoring. Console does not call POST /api/convergence/engagements.

### 2.4 Interaction with DCL

DCL stays AOS-only. Convergence reads each AOS tenant's triples via DCL's HTTP API. Convergence triples (combined financials, resolved entities) live in `convergence_triples`, keyed on `engagement_id`. No writes to DCL from the Convergence path.

---

## 3. Resolver Contract

The resolver is the load-bearing Convergence engine. It takes two AOS snapshots and produces per-domain mapping tables with confidence and evidence.

### 3.1 Module location

`convergence/engine/identity_resolver_v2/` — a new module, not a fork of AOD's vendor resolver. Convergence owns it. AOD's resolver is specialized to source-system catalogs (closed-world); Convergence's resolver is specialized to business records (open-world). Sharing would produce cosmetic coupling with no leverage.

### 3.2 Shared primitives library

`convergence/lib/identity_primitives/` — functions both AOD and Convergence need:

- `normalize_name(s: str) -> str`
- `normalize_domain(s: str) -> str`
- `levenshtein(a: str, b: str) -> int`
- `token_sort_ratio(a: str, b: str) -> float`
- `embedding_distance(a: str, b: str, model: str) -> float` (stub in v1; lands in WP3.5)

Lives in Convergence for now. Extraction to `aos-common` deferred until a third consumer appears.

### 3.3 Resolver tiers

Per domain, first-match-wins, evidence accumulated:

1. **Identifier match.** Exact match on highest-priority identifier. Auto-accepted at confidence 1.0. Provenance: identifier type and value.
2. **Normalized name match.** Exact match on `normalized_name`. Auto-accepted at confidence 0.92 if no identifier conflict.
3. **Fuzzy name match.** Token-sort ratio and Levenshtein-normalized similarity above domain-specific threshold. Confidence 0.60–0.90. Flagged for HITL.
4. **Embedding similarity.** Vector distance on name + context features. Confidence 0.50–0.85. Flagged for HITL. **Deferred to WP3.5.** Scaffolded in v1, returns empty candidate set.
5. **No match.** Record is unique to one side.

### 3.4 HITL state machine

States: `auto_accepted` → terminal. `pending_hitl` → `confirmed` | `rejected` | `deferred`.

Decisions are persistent (resolver_decisions table) and keyed on both records' content hashes. If either record's content changes in a subsequent AOS run, the decision flags `stale` and re-enters `pending_hitl`.

Auto-accept threshold: 0.90 (configurable per domain). Auto-reject threshold: 0.40 (configurable per domain). In between: `pending_hitl`.

### 3.5 HITL surface

Lives in Convergence's frontend (port 3010), on the engagement detail page. Not Console — HITL confirmation is operator action against a Convergence-owned artifact.

Engagement detail page gains a "Resolutions" tab: pending decisions grouped by domain, evidence, both sides' display names, accept/reject/defer controls. Keyboard-first. Bulk actions for high-confidence clusters. Decisions propagate to resolver_decisions immediately; downstream output recomputes incrementally.

Console's monitoring view surfaces resolver progress counts (pending/confirmed/rejected/auto-accepted) as read-only metrics with deep-links to Convergence for action.

### 3.6 Output shape

Per domain:

```
domain: "customer"
mappings: [
  { acq_record_id, tgt_record_id, confidence, tier, hitl_state, evidence },
  ...
]
unmatched_acq: [acq_record_id, ...]
unmatched_tgt: [tgt_record_id, ...]
```

Downstream engines (combining, bridge, QofE, overlap, cross-sell) consume this shape exclusively. No engine re-implements matching.

---

## 4. Transition Plan

### 4.1 Hard deletions

- `farm/overlap.py` — deleted in WP2. Farm no longer generates combined runs.
- `convergence_triples` rows keyed on combined-tenant — truncated in WP2. All data is disposable synthetic.
- DCL Identity Gates UI — removed in WP5b once Convergence's Resolutions tab is functional.
- Meridian/Cascadia fixture wiring — **eradicated in WP2.** Full deletion from all code. Pre-commit hook rejects literal `meridian` or `cascadia` strings outside changelog/archive. YAMLs deleted, not demoted to catalog entries. No special-case references survive in Farm, Convergence, DCL, Console, NLQ, or any other repo.

### 4.2 Contract migration

- convergence_blueprint_master §4.4 flags Farm configs as transitional, superseded by this blueprint's WP2.
- AOS_MASTER_RACI_v8.6 updated to reflect Convergence ownership of engagement schema and resolver module.
- se_triples_conversion_build_plan_v2.2 unchanged. AOS output contract (§1 here) is additive.

### 4.3 Demo scripts and fixtures

Any script, test, or doc that references Meridian or Cascadia by name gets rewritten to pick-from-catalog. Demo becomes exploratory: operator picks two AOS tenants live, runs Convergence, walks Resolutions tab, walks reports. No scripted pair.

---

## 5. WP Sequence

| WP | Deliverable | Blocked by | CC agent scope |
|---|---|---|---|
| WP1 | This spec accepted; RACI v8.6 updated; contract interfaces frozen | — | Single pass |
| WP2 | Farm industry catalog generalization; Meridian/Cascadia eradication; `overlap.py` deletion; pre-commit hook; IT asset generator | WP1 | Farm repo only |
| WP3 | Resolver module (tiers 1–3), HITL state machine, `convergence_engagement` schema, resolver API | WP1, WP2 | Convergence repo only |
| WP4 | Convergence tenant overlay: engagement creation endpoint with AOS contract check, persistence | WP3 | Convergence repo only |
| WP5a | Convergence frontend: engagements list, pair-selector, engagement detail page, Resolutions tab with HITL controls | WP3, WP4 | Convergence repo only |
| WP5b | Console monitoring: read-only engagement list and progress view, deep-links to Convergence; DCL Identity Gates UI retirement; optional "trigger pipeline run on existing engagement" control | WP5a | Console + DCL |
| WP6 | Reports generic two-AOS comparator; existing engines rewired to resolver output; Convergence engagement detail surfaces reports inline | WP3, WP4 | Convergence |
| WP3.5 | Embedding-tier resolver, model selection, confidence calibration | WP3 + calibration data from first WP6 runs | Convergence |

### 5.1 Gate rules

- WP2 cannot begin until `farm_deferred_work.md` is current and Farm harness is green.
- WP3 cannot begin until at least two non-fixture AOS templates exist in Farm's catalog.
- WP5a is Playwright-gated (B17 rule) on Convergence's frontend harness.
- WP5b is Playwright-gated on Console's.
- Convergence remains standalone at port 3010. WP5a validated without Console.
- Each WP produces its own CC prompt against this spec. Pre-session deferred-work check required.

---

## 6. Open questions (tracked, not blocking WP1)

1. **Confidence thresholds per domain** are placeholder values. Real calibration requires the first cross-industry pair end-to-end. WP3.5 revisits.
2. **Embedding model choice** (WP3.5). Local `sentence-transformers/all-mpnet-base-v2` vs hosted. Deferred.
3. **Resolver version upgrade behavior.** Options: pin engagement to resolver version (stable, replayable); auto-re-resolve (churn). Lean: pin, with operator-triggered "re-resolve with new version" action.
4. **Cross-tenant data isolation** at DB layer for Convergence engagements belonging to different customers. Defer until Convergence has more than one real customer engagement.

---

## 7. Success criteria for WP1

- RACI v8.6 reflects the schemas, modules, and APIs in this blueprint.
- convergence_blueprint_master flags superseded sections with pointers to this document.
- No code written in WP1. The spec is the deliverable.
- Acceptance: Ilya reads end-to-end, open questions in §6 are the only items not decided.

---

End of blueprint.
