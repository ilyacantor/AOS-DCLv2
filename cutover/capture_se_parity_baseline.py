#!/usr/bin/env python3
"""Capture the SE-path parity baseline for the richest SE entity (FluxEdge-TMZ8).

FROZEN CONTRACT. Before any flip of rich SE data from /api/dcl/ingest-triples to
/api/dcl/ingest-records (the SE-path cutover), the records-path
(source -> transport -> DCL-classify) MUST reproduce this exact concept+value set:
every concept present, under its CANONICAL name (dcl/SCHEMA_CONTRACT.md), values
matching. Parity is diffed against the files this writes, not eyeballed. See
SCHEMA_CONTRACT.md "SE-path cutover readiness".

Captured against the LIVE current SE output (Farm -> ingest-triples), so it includes
concepts that are currently dark in NLQ ONLY because of name drift (e.g.
infrastructure.cloud_spend.*, customer.count.total, support.tickets.total) — those are
real data and part of the contract; the cutover's records-path must emit them under the
canonical names the registry pins.

Run in the DCL venv:
  cd /home/ilyac/code/dcl && .venv/bin/python cutover/capture_se_parity_baseline.py
"""
import json
import os
from datetime import datetime, timezone

import psycopg2
from dotenv import load_dotenv

ENTITY = "FluxEdge-TMZ8"
TENANT = "69688df3-fc8e-51f8-a77c-9c13f9b3a784"
SCHEMA = "shared_gdbmdr"  # the dev-DCL schema the SE entities live in (:8104 / aos-dev)
HERE = os.path.dirname(os.path.abspath(__file__))


def _dsn() -> str:
    load_dotenv(os.path.join(HERE, "..", ".env.development"))
    dsn = os.environ.get("DATABASE_URL") or os.environ.get("SUPABASE_DB_URL")
    if not dsn:
        raise RuntimeError("No DATABASE_URL/SUPABASE_DB_URL in dcl/.env.development — cannot capture baseline.")
    return dsn


def main() -> None:
    conn = psycopg2.connect(_dsn())
    try:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT concept, property, value, period, unit, source_system, confidence_tier
            FROM {SCHEMA}.semantic_triples
            WHERE entity_id = %s AND tenant_id = %s AND is_active = true
            ORDER BY concept, property, period NULLS FIRST, value::text
            """,
            (ENTITY, TENANT),
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    triples = [
        {"concept": c, "property": p, "value": v, "period": per,
         "unit": u, "source_system": s, "confidence_tier": t}
        for (c, p, v, per, u, s, t) in rows
    ]

    # Full frozen set — sorted, stable: this is the diff target for parity.
    jsonl_path = os.path.join(HERE, f"se_parity_baseline__{ENTITY}.jsonl")
    with open(jsonl_path, "w") as f:
        for tr in triples:
            f.write(json.dumps(tr, sort_keys=True, default=str) + "\n")

    # Concept inventory — "every concept present", readable.
    inv: dict = {}
    for tr in triples:
        e = inv.setdefault(tr["concept"], {"triples": 0, "props": set(), "periods": set()})
        e["triples"] += 1
        e["props"].add(tr["property"])
        if tr["period"]:
            e["periods"].add(tr["period"])
    inventory = {
        c: {"triples": e["triples"], "distinct_properties": len(e["props"]),
            "distinct_periods": len(e["periods"]), "root": c.split(".")[0]}
        for c, e in sorted(inv.items())
    }
    roots: dict = {}
    for c, meta in inventory.items():
        r = roots.setdefault(meta["root"], {"concepts": 0, "triples": 0})
        r["concepts"] += 1
        r["triples"] += meta["triples"]

    summary = {
        "entity_id": ENTITY,
        "tenant_id": TENANT,
        "schema": SCHEMA,
        "captured_utc": datetime.now(timezone.utc).isoformat(),
        "source": "live SE output (Farm -> /api/dcl/ingest-triples)",
        "total_active_triples": len(triples),
        "distinct_concepts": len(inventory),
        "distinct_concept_roots": len(roots),
        "by_root": dict(sorted(roots.items())),
        "concept_inventory": inventory,
    }
    summary_path = os.path.join(HERE, f"se_parity_baseline__{ENTITY}.summary.json")
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2, sort_keys=True)

    print(f"  {len(triples)} active triples / {len(inventory)} concepts / {len(roots)} roots")
    print(f"  wrote {os.path.relpath(jsonl_path)}")
    print(f"  wrote {os.path.relpath(summary_path)}")


if __name__ == "__main__":
    main()
