# Sankey Graph — Logic Overview
> Session Date: March 2, 2026 | Module: DCL | RACI: A/R

---

## The Two Controls

The UI exposes two independent dropdowns in the top nav bar:

| Control | Options | What it governs |
|---------|---------|-----------------|
| **Data** (dataMode) | `Demo` · `Farm` · `AAM` | Where schemas come from — which systems/tables/fields feed the graph |
| **Mode** (runMode) | `Dev` · `Prod` | How mappings are validated — heuristic-only vs. heuristic + LLM refinement |

Both are sent together to `POST /api/dcl/run` as `mode` and `run_mode`.

---

## Data Mode: Where Schemas Come From

| Data Mode | Source | Caching | Failure Behavior |
|-----------|--------|---------|------------------|
| **Demo** | CSV files on disk (`schemas/schemas/salesforce/`, `sap/`, etc.) | 300s in-memory cache | Returns cached data |
| **Farm** | IngestStore (Redis) — Farm must have previously pushed data via `/api/dcl/ingest` | No cache (real-time) | Hard fail — HTTP 500 if no data |
| **AAM** | Live fetch from AAM microservice (`aam_client.get_pipes()`) | 120s cache; stale fallback if AAM unreachable | Returns stale cache, or empty |

**Key structural difference in the graph:**
- Demo & Farm → L1 nodes are individual source systems (Salesforce, SAP, NetSuite…)
- AAM with 30+ sources → L1 nodes are fabric planes (AWS, GCP, Azure…) that aggregate multiple pipes

---

## Run Mode: How Mappings Are Validated

| Run Mode | Mapping Behavior | Cost |
|----------|-----------------|------|
| **Dev** | Heuristic mapper only (pattern matching, ~1s). If AAM semantic edges exist, Tier 0 checks happen first. No LLM calls. | Free |
| **Prod** | Heuristic first, then LLM validation of low-confidence mappings via OpenAI. Also stores mapping lessons in Pinecone RAG. | LLM API costs |

Run mode does NOT change what data is loaded — it only changes whether the mapping pass includes LLM refinement after the heuristic pass.

---

## The 4-Layer Sankey Structure

```
L0 (Pipeline)  →  L1 (Sources/Fabrics)  →  L2 (Ontology Concepts)  →  L3 (Personas/BLL)
     schema              mapping                   consumption
```

---

## Layer-by-Layer Aggregation Logic

### L0 — Pipeline (Single Root Node)

One node per run. ID encodes the data source: `pipe_demo`, `pipe_farm`, or `pipe_aam`.

- **Metrics:** `source_count` (total source systems loaded)
- **Link to L1:** One link per source (or per fabric plane in AAM). Link `value` = number of tables in that source.
- No computation — purely a container node.

### L1 — Sources or Fabric Planes (Branching Logic)

**Demo / Farm path:** One L1 node per source system. Each node carries:
- `tables` count, `fields` count
- `trust_score`, `data_quality_score`, `vendor`, `category`
- `discovery_status`, `resolution_type`

**AAM path (30+ sources with fabric tags):** L1 nodes become fabric planes. Each fabric node aggregates all pipes in that plane:
- `pipe_count` — how many pipes live on this fabric
- `fields` — sum of all fields across all pipes on this plane
- `governed` / `ungoverned` counts
- `trust_score` — average across all pipes in the plane
- `sources` — list of source names rolled up under this fabric

This is a real aggregation boundary. In Demo/Farm, you see every source individually. In AAM with volume, individual source identity is lost at L1 — grouped by infrastructure plane.

### L1 → L2 — Mapping Links

#### Individual Source Mode (Demo / Farm): No Aggregation

Each field-level mapping becomes its own link. The atomic unit is one `Mapping` object produced by the heuristic mapper's triple-nested loop (`source → table → field`).

**Link ID construction:**
```
link_{source_system}_{ontology_concept}_{uuid_8chars}
```
The UUID suffix ensures every link is unique — there is no dedup key. If 5 fields from one source map to the same concept, that's 5 separate links.

**What each link carries:**

| Link Field | Set To | Meaning |
|-----------|--------|---------|
| `source` | e.g. `source_salesforce` | The L1 source node |
| `target` | e.g. `ontology_revenue` | The L2 concept node |
| `value` | `mapping.confidence` (0.0–1.0) | Not a count — it's the confidence score |
| `confidence` | Same as value | Redundant, but explicit |
| `flow_type` | `"mapping"` | Distinguishes from `"schema"` (L0→L1) and `"consumption"` (L2→L3) |
| `info_summary` | e.g. `"total_revenue → revenue (heuristic, 0.85)"` | Hover text |
| `mapping_detail` | `{source_field, source_table, target_concept, method, confidence}` | Full provenance |

**Concrete example — Salesforce fields mapping to `revenue`:**

| Field | Table | Confidence | Method |
|-------|-------|-----------|--------|
| `total_revenue` | `opportunities` | 0.95 | heuristic |
| `revenue_amount` | `opportunities` | 0.90 | heuristic |
| `annual_revenue` | `accounts` | 0.85 | heuristic |
| `mrr` | `subscriptions` | 0.70 | heuristic |
| `arr_value` | `subscriptions` | 0.65 | aam_edge |

Result: 5 separate `GraphLink` objects, all from `source_salesforce` → `ontology_revenue`, each with its own confidence as the `value`. In the frontend, D3-Sankey renders these as 5 parallel flowing bands of varying thickness — it does NOT merge them.

**Frontend rendering:**
- Each link becomes a separate SVG `<path>` element
- D3's layout stacks them vertically within the node's vertical space
- Stroke width is proportional to the link's `value` (confidence)
- Minimum stroke width enforced by `SANKEY_CONFIG.link.minStrokeWidth`
- On hover, each link shows its own tooltip with field-level provenance

#### Fabric Aggregation Mode (AAM with 30+ Sources)

Links are grouped by `(fabric_plane, ontology_concept)`. If the AWS plane has 8 fields across 3 pipes all mapping to `revenue`, that becomes 1 link:
- `value` = raw count (8)
- `confidence` = average of all 8 individual confidences

This is the **only place in the entire graph** where confidence gets averaged.

### L2 — Ontology Concepts (Accumulator)

L2 nodes are only rendered if they have at least 1 mapping. Each node accumulates:

- **`input_count`** — total number of individual field mappings. If 5 Salesforce fields + 3 SAP fields map to Revenue, `input_count = 8`. This is field count, not source count. No deduplication.
- **`contributing_fields`** — first 3 unique `table.field` names (for UI display)
- **`source_hierarchy`** — full nested structure for Monitor Panel drill-down:
  ```
  salesforce:
    opportunities:
      - {field: total_revenue, confidence: 0.95}
      - {field: revenue_amount, confidence: 0.90}
    accounts:
      - {field: annual_revenue, confidence: 0.85}
    subscriptions:
      - {field: mrr, confidence: 0.70}
      - {field: arr_value, confidence: 0.65}
  ```
- **`explanation`** — `"Derived from {N} field(s)"`

L2 does not transform or aggregate confidence values. It accumulates raw field mappings.

### L2 → L3 — Persona Consumption Links

One L3 node per selected persona (CFO, CRO, COO, CTO, CHRO).

For each persona:
1. Look up relevant ontology concepts (from `persona_profiles.yaml`)
2. For each relevant concept with `input_count > 0`, create one link
3. **Link value = the L2 node's `input_count`** — passed straight through, no further aggregation

If Revenue has `input_count = 8` and is relevant to both CFO and CRO, both get a link with `value = 8`. No weighting, no normalization. Binary relevance — a concept either matters to a persona or it doesn't.

---

## Relevance Filtering

Before any L1→L2 link is created, one gate applies:

```
if mapping.ontology_concept in relevant_concept_ids
```

A concept is "relevant" if at least one selected persona cares about it (from `persona_profiles.yaml`). If you deselect all personas, nothing maps. If you select only CFO, only finance-relevant concepts generate links.

---

## Visual Density & Virtualization

The no-aggregation design in Demo/Farm means a source with 50 fields mapping to 10 concepts produces 50 links. With 9 sources × 50 fields, you can hit 450 links. The frontend mitigates this with:
- **Link virtualization** at 100+ links (only renders links in the visible viewport)
- **Resize debounce** at 150ms
- **Minimum stroke width** so low-confidence links remain visible

---

## Aggregation Summary Table

| Layer Transition | Individual Mode (Demo/Farm) | Fabric Mode (AAM 30+) |
|-----------------|----------------------------|----------------------|
| **L0 → L1** | 1 link per source, value = table count | 1 link per fabric plane, value = pipe count |
| **L1 → L2** | 1 link per field mapping, value = confidence (no dedup) | 1 link per (plane, concept), value = count, confidence = average |
| **L2 node metrics** | `input_count` = raw field count across all sources | Same — raw field count across all planes |
| **L2 → L3** | 1 link per (concept, persona), value = input_count | Same |

---

## Architectural Implications

| Question | Answer |
|----------|--------|
| Can a source with many fields dominate the graph? | Yes. A source with 20 fields mapping to Revenue has 20x the link weight of a source with 1 field. No normalization. |
| Does fabric aggregation lose information? | Yes at L1 — you can't tell which pipe within AWS contributed which mapping. But `source_hierarchy` on L2 still preserves it. |
| Is confidence aggregated? | Only in fabric mode (averaged per plane-concept pair). Otherwise per-field, passed through raw. |
| Does persona relevance have weights? | No. Binary. The link value is just the concept's raw input count. |

---

## Auto-Load Behavior

On page mount, the frontend fires:
```
POST /api/dcl/run  { mode: "Demo", run_mode: "Dev", personas: [CFO, CRO, COO, CTO, CHRO] }
```
This populates the graph immediately. Subsequent "Run" clicks use whatever the dropdowns are set to.

---

## Key Files

| Concern | File |
|---------|------|
| Route handler + mode state | `backend/api/main.py` |
| Orchestration + `_build_graph()` | `backend/engine/dcl_engine.py` |
| Demo/AAM schema loading | `backend/engine/schema_loader.py` |
| Farm→SourceSystem conversion | `backend/farm/ingest_bridge.py` |
| Mode tracking singleton | `backend/core/mode_state.py` |
| Mapping model (`Mapping`) | `backend/domain/models.py` |
| Heuristic mapper (field→concept) | `backend/semantic_mapper/heuristic_mapper.py` |
| Frontend rendering (mode-agnostic) | `src/components/sankey/SankeyGraph.tsx` |
| Sankey constants & config | `src/components/sankey/constants.ts` |
| Link width / virtualization utils | `src/components/sankey/utils.ts` |
