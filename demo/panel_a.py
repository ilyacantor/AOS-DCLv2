"""
Panel A — the BEFORE condition: a competent agent with direct raw access.

"Agent with raw warehouse access" is precisely the condition enterprises
run today, so it exists here as a reproducible condition: the same model
as Panel B, a decent prompt, full read access to the entity's raw source
feeds (Farm's records-path exports — the exact records the platform
ingested). Deliberately un-hobbled. No semantics, no provenance, no
conflict register, no audit trail — that absence is the point.

Containment: operator-gated CLI tool only. Not an API. Not importable as
a data path (backend/* never imports demo.* — test-enforced). Run it by
hand:

    python -m demo.panel_a --entity CedarGrid-1823 \
        --question "What was net income in the most recent quarter?"
"""

from __future__ import annotations

import argparse
import asyncio
import json

from demo import feeds
from demo.agent_common import DEFAULT_MODEL, emit, load_demo_env, run_agent_loop

SYSTEM = (
    "You are a senior data analyst agent for the company. You have direct "
    "read access to the raw data-warehouse exports for entity {entity_id}: "
    "three feeds of flat records — 'financial' (quarterly statements), "
    "'operational' (quarterly KPIs), 'ledger' (per-record GL/journal/invoice/"
    "AP/AR and observability detail). Dollar figures are in millions unless "
    "a field says otherwise; periods are quarters like 2026-Q4. Read what "
    "you need with the tools and answer the question precisely, with "
    "numbers and units. Be direct and confident; state the period you used."
)

TOOL_DEFS = [
    {
        "name": "list_feeds",
        "description": (
            "List the raw feeds available for the entity: record counts, "
            "field names, and the period range of each feed."
        ),
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "read_feed",
        "description": (
            "Read raw records from one feed. Optionally filter to one "
            "period (e.g. '2026-Q4') and project to a subset of fields. "
            "Large results are truncated — filter to keep them small."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "feed": {"type": "string", "enum": ["financial", "operational", "ledger"]},
                "period": {"type": "string", "description": "optional period filter, e.g. 2026-Q4"},
                "fields": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "optional projection — field names to keep (period is always kept)",
                },
            },
            "required": ["feed"],
        },
    },
]

_FEED_CACHE: dict[str, dict] = {}


def _get_feed(feed: str, entity_id: str) -> dict:
    if feed not in _FEED_CACHE:
        _FEED_CACHE[feed] = feeds.fetch_feed(feed, entity_id)
    return _FEED_CACHE[feed]


def _list_feeds(entity_id: str) -> str:
    out = {}
    for name in feeds.FEED_PATHS:
        body = _get_feed(name, entity_id)
        records = body["records"]
        fields: set[str] = set()
        for r in records:
            fields.update(r.keys())
        periods = sorted({r.get("period") for r in records if r.get("period")})
        out[name] = {
            "record_count": body["count"],
            "fields": sorted(fields),
            "period_range": [periods[0], periods[-1]] if periods else [],
        }
    return json.dumps(out)


def _read_feed(entity_id: str, feed: str, period: str | None, fields: list[str] | None) -> str:
    body = _get_feed(feed, entity_id)
    records = body["records"]
    if period:
        records = [r for r in records if r.get("period") == period]
    if fields:
        keep = set(fields) | {"period"}
        records = [{k: v for k, v in r.items() if k in keep} for r in records]
    payload = json.dumps({"feed": feed, "returned": len(records), "records": records})
    if len(payload) > 28000:
        payload = payload[:28000] + '"…TRUNCATED — narrow with period/fields filters"}'
    return payload


async def run_panel_a(entity_id: str, question: str, model: str = DEFAULT_MODEL) -> dict:
    _FEED_CACHE.clear()

    async def execute_tool(name: str, args: dict) -> str:
        if name == "list_feeds":
            return await asyncio.to_thread(_list_feeds, entity_id)
        if name == "read_feed":
            return await asyncio.to_thread(
                _read_feed, entity_id, args["feed"], args.get("period"), args.get("fields")
            )
        raise ValueError(f"unknown tool {name!r}")

    result = await run_agent_loop(
        model=model,
        system=SYSTEM.format(entity_id=entity_id),
        question=question,
        tool_defs=TOOL_DEFS,
        execute_tool=execute_tool,
    )
    result["panel"] = "a"
    result["access"] = "raw-feeds-direct"
    result["entity_id"] = entity_id
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Panel A — ungoverned raw-access agent (operator-gated CLI)")
    parser.add_argument("--entity", required=True)
    parser.add_argument("--question", required=True)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--json", action="store_true", help="emit the capture fragment as JSON")
    args = parser.parse_args()

    load_demo_env()
    result = asyncio.run(run_panel_a(args.entity, args.question, args.model))
    emit(result, args.json)


if __name__ == "__main__":
    main()
