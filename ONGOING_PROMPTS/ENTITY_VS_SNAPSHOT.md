# Entity vs Snapshot: Definitive Architecture Reference

**This document is authoritative. If anything elsewhere contradicts this, this document wins.**

---

## Two Separate Worlds

There are two completely independent constructs in the AOS system. They share some infrastructure but have different purposes, different data sources, and different lifecycles. Agents must never conflate them.

### World 1: Showcase Entities (Meridian / Cascadia)

- **Purpose:** Demonstrate Convergence — dual-entity M&A comprehension in Reports, Dashboards, NLQ.
- **Entities:** `meridian` and `cascadia`. These are fixed strings. They do not change.
- **entity_id:** Always `"meridian"` or `"cascadia"`. Hardcoded. Not derived from snapshots, not derived from AOD, not derived from anything in the live pipeline.
- **Data source:** Farm generates financial data at ~$35M quarterly scale using the default config (`farm_config.yaml`). This is the ONLY config used by the pipeline. The same numbers appear for every snapshot. This is intentional — it tests idempotency and provides consistent showcase data.
- **Farm entity-specific configs:** `farm_config_meridian.yaml` ($5B) and `farm_config_cascadia.yaml` ($1B) exist but are NOT wired into the pipeline. They are separate constructs for potential future use. Do not attempt to connect them to manifest dispatch. Do not attempt to select configs based on entity_id.
- **NLQ default:** `_resolve_entity_id()` defaults to `"meridian"`. This is correct.
- **Where it surfaces:** Report portal Convergence demo, NLQ queries, dashboards. Maestra intel briefs reference Meridian as a $5B consultancy and Cascadia as a $1B BPM company — these are narrative constructs for the demo, not data the pipeline generates.

### World 2: Live Pipeline Snapshots

- **Purpose:** The actual production data flow. AOD discovers systems, AAM orchestrates, Farm generates, DCL ingests.
- **Identifiers:** `tenant_id` and `snapshot_name`. These are auto-generated session identifiers like `BlueFlow-8XHJ`, `NovaSystems-3NUY`, `BlueLogic-2OI9`, `CyberLabs-DXNX`. They have NO relation to entity names.
- **entity_id:** Does not originate in this pipeline. AOD does not know about entities. AAM does not assign entity_id. Farm receives manifests with no entity identity.
- **Data flow:** AOD → AAM → Farm → DCL. Farm uses the default config for all manifests. All pipeline data arrives at DCL at the same ~$35M scale regardless of which snapshot triggered it.
- **tenant_id in DCL:** Used to tag ingested data for namespace isolation. When DCL's ingest route receives data, it stamps `_entity_id = ingest_req.tenant_id` (per the fix at ingest.py:663). This means snapshot-originated data gets tagged with the snapshot's tenant_id, not with "meridian" or "cascadia".

---

## How They Coexist

The showcase and the pipeline share DCL as the data store and NLQ as the query surface. The connection point:

1. Farm generates data → pushes to DCL with a tenant_id (snapshot identifier)
2. DCL materializes points tagged with that tenant_id as `_entity_id`
3. NLQ defaults `entity_id` to `"meridian"` for showcase queries
4. For the showcase to work, DCL must have data tagged with `_entity_id = "meridian"`

**Current state:** Farm pushes all data through the pipeline using snapshot tenant_ids. The `FARM_DEFAULT_ENTITY_ID` env var defaults to `"meridian"`, which is how showcase data ends up tagged correctly. This is not elegant, but it works and is temporary.

---

## Anti-Patterns — Do NOT Do These

1. **Do not try to wire Farm entity-specific configs ($5B/$1B) to the pipeline.** The pipeline uses one config. The entity configs are separate.
2. **Do not map snapshots to business entities.** There is no mapping. Snapshots are session artifacts.
3. **Do not add entity_id to AAM manifests.** AAM orchestrates snapshots, not entities.
4. **Do not add entity awareness to AOD.** AOD discovers systems. It does not know what a business entity is.
5. **Do not treat $35M-scale numbers as wrong.** They are the correct showcase numbers from the default Farm config.
6. **Do not treat $124M-scale numbers as correct.** If you see ~$124M revenue, that is `fact_base.json` leaking through a silent fallback. This is being removed.
7. **Do not create config-selection logic in Farm keyed to entity_id.** Not the current design.
8. **Do not conflate Maestra intel brief numbers ($5B Meridian, $1B Cascadia) with pipeline output.** The briefs are narrative. The pipeline generates $35M-scale data.

---

## Quick Reference

| Attribute | Showcase (Meridian/Cascadia) | Live Pipeline (Snapshots) |
|-----------|------------------------------|---------------------------|
| Purpose | Convergence demo | Production data flow |
| Identifiers | `"meridian"`, `"cascadia"` (fixed) | `BlueFlow-8XHJ` etc. (auto-generated) |
| Origin | Hardcoded | AOD discovery |
| entity_id | Fixed strings | Not applicable |
| tenant_id | N/A | Snapshot session ID |
| Data scale | ~$35M quarterly (Farm default config) | ~$35M quarterly (same config) |
| Farm config | `farm_config.yaml` (default) | `farm_config.yaml` (default) |
| Where it shows | Reports, dashboards, NLQ, Maestra | DCL ingest activity, pipeline logs |

---

## For Future Reference

Connecting entity identity to the live pipeline (so AOD snapshots map to business entities and Farm selects entity-appropriate configs) is a real architectural need. It is not the current priority. When it becomes the priority, it requires a multi-module RACI decision touching AAM (manifest construction), Farm (config selection), and potentially AOD (entity assignment). Do not attempt this as a side-effect of another fix.
