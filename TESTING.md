# DCL Testing Guide

## Prerequisites

- **Python** >= 3.11
- **venv** set up: `.venv/` at repo root
- **httpx** installed (for live harness): `.venv/bin/pip install httpx`
- **Farm data generated** (for integration tests that reference Farm pipe shapes)

## Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install pytest httpx
```

## Running the Full Suite

```bash
pytest backend/tests/ -v
```

This discovers all `test_*.py` files under `backend/tests/`.

## Test Categories

### Unit Tests (no server, no external deps)

| File | What it covers |
|------|----------------|
| `backend/tests/test_graph_traversal.py` | Semantic graph traversal engine — all 8 resolution steps (concept location, dimension validity, join path discovery, filter resolution, confidence scoring, response assembly), graceful degradation, path caching, GraphStats |

Run unit tests only:

```bash
pytest backend/tests/test_graph_traversal.py -v
```

### Integration Tests (no server, synthetic data)

| File | What it covers |
|------|----------------|
| `backend/tests/test_tier0_aam_edges.py` | Tier 0 AAM edge classification — high/low confidence edges, fallthrough to Tier 1, transformed edges, alias resolution, EdgeIndex coverage stats, backward compatibility |

Run integration tests only:

```bash
pytest backend/tests/test_tier0_aam_edges.py -v
```

These tests can also run standalone (outside pytest):

```bash
python backend/tests/test_tier0_aam_edges.py
```

### Utilities (not tests)

| File | What it does |
|------|--------------|
| `backend/tests/farm_ground_truth.py` | Farm ground truth client — used by other tests to fetch/validate Farm data shapes |

### Live Harness (requires running DCL server)

| File | What it covers |
|------|----------------|
| `tests/harness/dcl_harness.py` | HTTP-only endpoint validation — tests 9 DCL API endpoints (combining IS, entity overlap, cross-sell, EBITDA bridge, QoE, CFO dashboard, what-if, cross-entity matches, conflicts). Tracks latency per request with min/max/avg/p95 stats. |

Run the live harness:

```bash
# Against local DCL (default http://localhost:8004)
python tests/harness/dcl_harness.py

# Against deployed DCL
python tests/harness/dcl_harness.py --url https://aos-dcl.onrender.com

# Verbose output (shows request details)
python tests/harness/dcl_harness.py --verbose

# Run a single test by ID
python tests/harness/dcl_harness.py --test ebitda-bridge
```

Exit codes:
- `0` — all tests passed
- `1` — one or more tests failed
- `2` — harness error (server unreachable, bad config)

Available test IDs: `combining-is`, `entity-overlap`, `cross-sell`, `ebitda-bridge`, `qoe`, `dashboard-cfo`, `what-if`, `cross-entity`, `conflicts`

## Running a Single Test File

```bash
pytest backend/tests/test_graph_traversal.py -v
pytest backend/tests/test_graph_traversal.py::test_step2_concept_location -v
```

## Running a Single Test Function

```bash
pytest backend/tests/test_graph_traversal.py::test_spec_example_revenue_by_division_for_cloud -v
```
