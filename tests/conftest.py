"""
Shared test fixtures — tenant_id, run_id, and ground truth from Farm.

No hardcoded UUIDs or financial values in test files. All tests read from
the manifest and fetch ground truth from Farm's API at runtime (B10).
"""

import json
import os
from pathlib import Path

import httpx
import pytest

_MANIFEST_PATH = Path(__file__).resolve().parent.parent / "data" / "seed_manifest.json"


def _load_manifest() -> dict:
    """Load seed_manifest.json. Fails loudly if missing."""
    if not _MANIFEST_PATH.exists():
        raise FileNotFoundError(
            f"seed_manifest.json not found at {_MANIFEST_PATH}. "
            f"Run the seed pipeline before executing tests."
        )
    with open(_MANIFEST_PATH) as f:
        return json.load(f)


_manifest = _load_manifest()

TENANT_ID: str = _manifest["tenant_id"]
RUN_ID: str = _manifest.get("dcl_ingest_id", _manifest.get("run_id", ""))
FARM_RUN_ID: str = _manifest["farm_run_id"]
ENTITIES: list[str] = _manifest.get("entities", [])


def _fetch_ground_truth() -> dict:
    """Fetch ground truth from Farm's API. Fails loudly if unavailable."""
    farm_url = os.environ.get("FARM_API_URL", "http://localhost:8003")
    url = f"{farm_url}/api/business-data/ground-truth/{FARM_RUN_ID}"
    try:
        response = httpx.get(url, timeout=30.0)
    except httpx.ConnectError as e:
        raise ConnectionError(
            f"Cannot reach Farm at {farm_url} — is Farm running? "
            f"Ground truth is required for test verification (B10). Error: {e}"
        ) from e
    if response.status_code != 200:
        raise ValueError(
            f"Ground truth not available from Farm at {url}: "
            f"HTTP {response.status_code} — {response.text[:200]}. "
            f"Ensure Farm is running and has data for {FARM_RUN_ID}."
        )
    return response.json()


# Session-scoped: fetch once, reuse across all tests
_ground_truth_cache = None


def _get_ground_truth() -> dict:
    global _ground_truth_cache
    if _ground_truth_cache is None:
        _ground_truth_cache = _fetch_ground_truth()
    return _ground_truth_cache


def gt_metric(entity: str, period: str, concept: str) -> float:
    """Look up a single ground truth value by entity/period/concept.

    Raises KeyError with a clear message if the value is not found.
    """
    gt = _get_ground_truth()
    tgt = gt.get("triple_ground_truth", {})
    entity_data = tgt.get(entity)
    if entity_data is None:
        raise KeyError(
            f"Entity '{entity}' not found in ground truth. "
            f"Available: {list(tgt.keys())}"
        )
    period_data = entity_data.get(period)
    if period_data is None:
        raise KeyError(
            f"Period '{period}' not found for entity '{entity}'. "
            f"Available: {sorted(entity_data.keys())}"
        )
    if concept not in period_data:
        raise KeyError(
            f"Concept '{concept}' not found for {entity}/{period}. "
            f"Available: {sorted(period_data.keys())}"
        )
    return period_data[concept]


def gt_overlap_count(category: str) -> int:
    """Look up overlap count by category (customer, vendor, employee)."""
    gt = _get_ground_truth()
    counts = gt.get("overlap_counts", {})
    if category not in counts:
        raise KeyError(
            f"Overlap category '{category}' not found. "
            f"Available: {list(counts.keys())}"
        )
    return counts[category]


def gt_atemporal(entity: str, concept: str, prop: str = "amount_current") -> float:
    """Look up an atemporal ground truth value (e.g., EBITDA adjustments).

    These are triples without a period — used for adjustments, service catalogs, etc.
    """
    gt = _get_ground_truth()
    agt = gt.get("atemporal_ground_truth", {})
    entity_data = agt.get(entity)
    if entity_data is None:
        raise KeyError(
            f"Entity '{entity}' not found in atemporal ground truth. "
            f"Available: {list(agt.keys())}"
        )
    concept_data = entity_data.get(concept)
    if concept_data is None:
        raise KeyError(
            f"Concept '{concept}' not found for entity '{entity}'. "
            f"Available: {sorted(k for k in entity_data.keys())}"
        )
    if prop not in concept_data:
        raise KeyError(
            f"Property '{prop}' not found for {entity}/{concept}. "
            f"Available: {sorted(concept_data.keys())}"
        )
    return concept_data[prop]


@pytest.fixture
def seed_tenant_id() -> str:
    """Tenant ID from seed_manifest.json."""
    return TENANT_ID


@pytest.fixture
def seed_run_id() -> str:
    """Run ID from seed_manifest.json."""
    return RUN_ID
