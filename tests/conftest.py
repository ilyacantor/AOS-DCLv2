"""
Shared test fixtures — tenant_id and run_id from seed_manifest.json.

No hardcoded UUIDs in test files. All tests read from the manifest
so they track the actual seed data.
"""

import json
from pathlib import Path

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
RUN_ID: str = _manifest["run_id"]


@pytest.fixture
def seed_tenant_id() -> str:
    """Tenant ID from seed_manifest.json."""
    return TENANT_ID


@pytest.fixture
def seed_run_id() -> str:
    """Run ID from seed_manifest.json."""
    return RUN_ID
