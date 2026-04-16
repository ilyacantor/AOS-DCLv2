"""Test helper — seed tenant_runs.updated_at to an old timestamp.

Used by the Ingest Refresh Playwright spec to force a set of tracked entities
to look "stale" to the Refresh endpoint so it picks them up as candidates
and Farm fans out concurrent multi-batch pushes per entity. This is the only
way to exercise the append_rows_for_entity race from an e2e test on a
live-but-idle system.

Commands (all via --command flag):
  list_tracked          → JSON list of (tenant_id, entity_id, updated_at)
  seed-stale EIDS...    → set updated_at = 2025-01-01Z for the named entities
  entity-count EID      → print current_triples COUNT(*) for an entity
  run-row-count EID     → print tenant_runs.run_row_count for an entity
  all-entity-counts     → JSON {entity_id: {run_row_count, current_count}}

All commands read DATABASE_URL from dcl/.env. Prints JSON or a plain integer
to stdout. Exits non-zero on any failure so Playwright surfaces DB issues
as test failures instead of silent passes.

Running this helper from a spec:

    import { execSync } from "child_process";
    const PY = "/home/ilyac/code/dcl/.venv/bin/python";
    const HELPER = "/home/ilyac/code/dcl/tests/e2e/helpers/seed_stale_tenant_runs.py";
    execSync(`${PY} ${HELPER} seed-stale VeloCorp-KY0F InfoWave-CTXD ...`);

The helper only writes to tenant_runs.updated_at. It reads current_triples
for count assertions. It does not touch any other table or column.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

# The DCL repo ships its DB URL in .env; the helper is designed to be
# invoked from a Playwright spec that has no shell env, so we load it
# explicitly rather than relying on a caller to set `set -a`.
_REPO = Path(__file__).resolve().parents[3]
_env_path = _REPO / ".env"
if _env_path.exists():
    for raw in _env_path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        # Only set if absent — caller's env wins.
        os.environ.setdefault(k.strip(), v.strip())

try:
    import psycopg2
except ImportError as e:  # pragma: no cover
    sys.stderr.write(
        "seed_stale_tenant_runs.py: psycopg2 is missing. "
        "Run via /home/ilyac/code/dcl/.venv/bin/python.\n"
    )
    raise SystemExit(2) from e


STALE_TIMESTAMP = "2025-01-01T00:00:00+00:00"


def _connect():
    url = os.environ.get("DATABASE_URL")
    if not url:
        sys.stderr.write(
            "seed_stale_tenant_runs.py: DATABASE_URL is not set. "
            "Expected it in /home/ilyac/code/dcl/.env.\n"
        )
        raise SystemExit(2)
    return psycopg2.connect(url)


def cmd_list_tracked() -> None:
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT tenant_id, entity_id, updated_at, run_row_count, current_run_id "
            "FROM tenant_runs ORDER BY entity_id"
        )
        rows = [
            {
                "tenant_id": str(r[0]),
                "entity_id": r[1],
                "updated_at": r[2].isoformat() if r[2] else None,
                "run_row_count": int(r[3]),
                "current_run_id": str(r[4]) if r[4] else None,
            }
            for r in cur.fetchall()
        ]
    json.dump(rows, sys.stdout)
    sys.stdout.write("\n")


def cmd_seed_stale(entity_ids: list[str]) -> None:
    if not entity_ids:
        sys.stderr.write("seed-stale requires at least one entity_id\n")
        raise SystemExit(2)
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE tenant_runs "
            "SET updated_at = %s::timestamptz "
            "WHERE entity_id = ANY(%s) "
            "RETURNING tenant_id, entity_id, updated_at",
            (STALE_TIMESTAMP, list(entity_ids)),
        )
        updated = [
            {
                "tenant_id": str(r[0]),
                "entity_id": r[1],
                "updated_at": r[2].isoformat(),
            }
            for r in cur.fetchall()
        ]
        conn.commit()
    if len(updated) != len(entity_ids):
        seen = {u["entity_id"] for u in updated}
        missing = [e for e in entity_ids if e not in seen]
        sys.stderr.write(
            f"seed-stale: entities not found in tenant_runs: {missing}\n"
        )
        raise SystemExit(1)
    json.dump({"seeded": updated, "stale_timestamp": STALE_TIMESTAMP}, sys.stdout)
    sys.stdout.write("\n")


def cmd_entity_count(entity_id: str) -> None:
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM current_triples WHERE entity_id = %s",
            (entity_id,),
        )
        row = cur.fetchone()
    if row is None:
        raise SystemExit(1)
    sys.stdout.write(f"{int(row[0])}\n")


def cmd_run_row_count(entity_id: str) -> None:
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT run_row_count FROM tenant_runs WHERE entity_id = %s",
            (entity_id,),
        )
        row = cur.fetchone()
    if row is None:
        sys.stderr.write(f"run-row-count: entity {entity_id} not in tenant_runs\n")
        raise SystemExit(1)
    sys.stdout.write(f"{int(row[0])}\n")


def cmd_all_entity_counts() -> None:
    """One round-trip compare — run_row_count per entity vs actual current_triples count.

    Returned JSON object keyed by entity_id with both numbers side by side.
    The spec asserts arithmetic equality per entity from this payload.
    """
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                tr.tenant_id,
                tr.entity_id,
                tr.run_row_count,
                COALESCE(ct.actual_count, 0) AS actual_count
            FROM tenant_runs tr
            LEFT JOIN (
                SELECT tenant_id, entity_id, COUNT(*) AS actual_count
                FROM current_triples
                GROUP BY tenant_id, entity_id
            ) ct
              ON ct.tenant_id = tr.tenant_id
             AND ct.entity_id = tr.entity_id
            ORDER BY tr.entity_id
            """
        )
        payload = {
            r[1]: {
                "tenant_id": str(r[0]),
                "run_row_count": int(r[2]),
                "current_count": int(r[3]),
            }
            for r in cur.fetchall()
        }
    json.dump(payload, sys.stdout)
    sys.stdout.write("\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", help="list-tracked | seed-stale | entity-count | run-row-count | all-entity-counts")
    parser.add_argument("args", nargs="*", help="command arguments (entity_ids or single entity_id)")
    ns = parser.parse_args()

    if ns.command == "list-tracked":
        cmd_list_tracked()
    elif ns.command == "seed-stale":
        cmd_seed_stale(ns.args)
    elif ns.command == "entity-count":
        if len(ns.args) != 1:
            parser.error("entity-count requires exactly one entity_id")
        cmd_entity_count(ns.args[0])
    elif ns.command == "run-row-count":
        if len(ns.args) != 1:
            parser.error("run-row-count requires exactly one entity_id")
        cmd_run_row_count(ns.args[0])
    elif ns.command == "all-entity-counts":
        cmd_all_entity_counts()
    else:
        parser.error(f"unknown command: {ns.command}")


if __name__ == "__main__":
    main()
