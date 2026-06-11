"""
Raw source-feed access for the demo — the "warehouse" side of the
before/after contrast, and the runtime ground-truth source for eval
scoring (B10: expected values come from the source system at run time,
never hardcoded).

The feeds are Farm's records-path export endpoints — the same endpoints
AAM's transports pull from when the platform ingests an entity:

    GET {FARM}/api/farm/financial-records?entity_id=&seed=
    GET {FARM}/api/farm/operational-records?entity_id=&seed=
    GET {FARM}/api/farm/ledger-records?entity_id=&seed=

Seed contract: the operator records-path run derives the seed as
uuid5(NAMESPACE_URL, "fin-seed:{entity_id}") % 2**31 (AAM
app/routers/operator_fabric.py) so an entity's numbers are stable across
re-runs. The same derivation is used here so a raw read returns exactly
the records the platform ingested. Verified live: CedarGrid-1823
pnl.net_income 2026-Q4 == feed net_income 2026-Q4 == 99.99.
"""

from __future__ import annotations

import os
import uuid
from typing import Any

import httpx

FARM_URL = os.environ.get("DEMO_FARM_URL", "http://localhost:8003")

FEED_PATHS = {
    "financial": "/api/farm/financial-records",
    "operational": "/api/farm/operational-records",
    "ledger": "/api/farm/ledger-records",
}


def derive_feed_seed(entity_id: str) -> int:
    """The records-path per-entity seed (mirrors AAM operator_fabric.py)."""
    return int(uuid.uuid5(uuid.NAMESPACE_URL, f"fin-seed:{entity_id}").int % (2**31))


def fetch_feed(feed: str, entity_id: str, timeout: float = 60.0) -> dict[str, Any]:
    """Fetch one raw feed. Raises loudly on any failure (A1)."""
    if feed not in FEED_PATHS:
        raise ValueError(f"unknown feed {feed!r}; valid: {sorted(FEED_PATHS)}")
    url = f"{FARM_URL}{FEED_PATHS[feed]}"
    params = {"entity_id": entity_id, "seed": derive_feed_seed(entity_id)}
    resp = httpx.get(url, params=params, timeout=timeout)
    if resp.status_code != 200:
        raise RuntimeError(
            f"Farm feed {feed} failed for entity_id={entity_id!r}: "
            f"GET {url} -> HTTP {resp.status_code}: {resp.text[:300]}"
        )
    body = resp.json()
    if not body.get("records"):
        raise RuntimeError(
            f"Farm feed {feed} returned no records for entity_id={entity_id!r} "
            f"(count={body.get('count')}) — cannot proceed (A1)."
        )
    return body


def latest_period(records: list[dict]) -> str:
    periods = sorted({r["period"] for r in records if r.get("period")})
    if not periods:
        raise RuntimeError("feed records carry no periods — cannot resolve 'latest'")
    return periods[-1]


def ground_truth_value(feed_body: dict, field: str, period: str) -> float:
    """Resolve one scalar ground-truth value from a fetched feed.

    period may be a literal like '2026-Q4' or the sentinel 'latest'.
    Raises loudly if the field is absent for that period — an absent
    ground-truth value means the question slot is mis-curated, not zero.
    """
    records = feed_body["records"]
    p = latest_period(records) if period == "latest" else period
    for rec in records:
        if rec.get("period") == p and rec.get(field) is not None:
            val = rec[field]
            if not isinstance(val, (int, float)):
                raise RuntimeError(
                    f"ground-truth field {field!r}@{p} is non-numeric: {type(val).__name__}"
                )
            return float(val)
    raise RuntimeError(
        f"ground-truth field {field!r} not found for period {p!r} in feed "
        f"(entity {feed_body.get('entity_id')!r}) — fix the question slot, "
        f"do not default (A1/B8)."
    )
