#!/usr/bin/env python3
"""Verify the live dev store against the frozen FluxEdge-TMZ8 parity baseline.

Re-runs the exact capture SELECT (same 7 columns, same deterministic ORDER BY)
through the CURRENT liveness read (is_active = true) and byte-diffs the result
against the frozen jsonl. Zero diff = parity. Used pre- and post- the Gate 0
bi-temporal migration: identical output through the compatibility read proves
consumers see the same data.

Run: cd /home/ilyac/code/dcl && .venv/bin/python cutover/verify_se_parity_baseline.py
"""
import difflib
import json
import os
import sys

import psycopg2
from dotenv import load_dotenv

ENTITY = "FluxEdge-TMZ8"
TENANT = "69688df3-fc8e-51f8-a77c-9c13f9b3a784"
SCHEMA = "shared_gdbmdr"
HERE = os.path.dirname(os.path.abspath(__file__))
FROZEN = os.path.join(HERE, f"se_parity_baseline__{ENTITY}.jsonl")


def main() -> int:
    load_dotenv(os.path.join(HERE, "..", ".env.development"))
    dsn = os.environ.get("DATABASE_URL") or os.environ.get("SUPABASE_DB_URL")
    if not dsn:
        raise RuntimeError("No DATABASE_URL/SUPABASE_DB_URL in dcl/.env.development.")

    conn = psycopg2.connect(dsn)
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

    live_lines = [
        json.dumps(
            {"concept": c, "property": p, "value": v, "period": per,
             "unit": u, "source_system": s, "confidence_tier": t},
            sort_keys=True, default=str,
        )
        for (c, p, v, per, u, s, t) in rows
    ]
    frozen_lines = [l.rstrip("\n") for l in open(FROZEN)]

    concepts = len({json.loads(l)["concept"] for l in live_lines})
    print(f"live:   {len(live_lines)} active triples / {concepts} concepts")
    print(f"frozen: {len(frozen_lines)} triples")

    # The capture ORDER BY (concept, property, period, value::text) leaves
    # tie order among rows differing only in source_system nondeterministic
    # (insert-order dependent). The contract is the exact multiset of rows,
    # not tie order — compare sorted.
    live_lines = sorted(live_lines)
    frozen_lines = sorted(frozen_lines)

    if live_lines == frozen_lines:
        print("PARITY: row multiset identical to frozen baseline.")
        return 0

    diff = list(difflib.unified_diff(frozen_lines, live_lines, "frozen", "live", lineterm=""))
    print(f"PARITY FAILED: {len(diff)} diff lines; first 20:")
    for line in diff[:20]:
        print(f"  {line}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
