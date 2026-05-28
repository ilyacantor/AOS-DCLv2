"""Backfill non-canonical (UUID-shaped) snapshot_names in tenant_runs.

Closes dcl_deferred_work.md#39. Operates in two phases:
  1. AUDIT — enumerate polluted rows; write before/after to a JSON audit
     file in dcl/backfills/. Idempotent; safe to re-run.
  2. BACKFILL — UPDATE each polluted column to the canonical name derived
     via farm.services.identity.derive_run_name (single source of truth
     mirrored here to avoid a cross-repo Python import per B6).

Canonical I5 form: `{entity_id}-{ingest_uuid[:4]}` with hyphens stripped
from the UUID prefix to match Console's make_run_name pattern. Helper
mirror is verified byte-identical against farm/services/identity.py at
review time.

Usage:
    DATABASE_URL=postgresql://... python scripts/backfill_snapshot_names.py --audit-only
    DATABASE_URL=postgresql://... python scripts/backfill_snapshot_names.py --apply
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError:
    print("ERROR: psycopg2 not installed.", file=sys.stderr)
    sys.exit(1)


_UUID_PATTERN = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)


def is_canonical(entity_id: str, value: str) -> bool:
    """True if value matches the canonical I5 form for entity_id.

    Canonical: '<entity_id>-<4hex>'. Detects every shape that ISN'T
    canonical (UUIDs, cloudedge-*, cloud-spend-*, "cco_summary", any
    pre-Z legacy form), not just the UUID subclass.
    """
    pattern = re.compile(
        r"^" + re.escape(entity_id) + r"-[0-9a-f]{4}$",
        re.IGNORECASE,
    )
    return bool(pattern.match(value))


def derive_run_name(entity_id: str, ingest_uuid: str) -> str:
    """Mirror of farm.services.identity.derive_run_name.

    Keep byte-identical with the Farm implementation. See dcl#36 for the
    cross-repo helper-mirror design decision.
    """
    if not entity_id or not str(entity_id).strip():
        raise ValueError("derive_run_name requires entity_id (I2)")
    if not ingest_uuid or not str(ingest_uuid).strip():
        raise ValueError("derive_run_name requires ingest_uuid (I2)")
    short = str(ingest_uuid).replace("-", "")[:4]
    return f"{entity_id}-{short}"


def is_uuid_shaped(value: str | None) -> bool:
    """True if value matches the canonical UUID-with-hyphens text form."""
    if value is None:
        return False
    return bool(_UUID_PATTERN.match(value))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--audit-only", action="store_true",
        help="Enumerate + write audit file; do NOT update.",
    )
    parser.add_argument(
        "--apply", action="store_true",
        help="Run the backfill UPDATEs. Mutually exclusive with --audit-only.",
    )
    parser.add_argument(
        "--schema", default="shared_gdbmdr",
        help="Postgres schema housing tenant_runs (default: shared_gdbmdr).",
    )
    args = parser.parse_args()

    if args.audit_only == args.apply:
        print("ERROR: pass exactly one of --audit-only or --apply.", file=sys.stderr)
        return 2

    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("ERROR: DATABASE_URL not set.", file=sys.stderr)
        return 2

    audit_dir = Path(__file__).resolve().parent.parent / "backfills"
    audit_dir.mkdir(exist_ok=True)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    audit_path = audit_dir / f"{today}_snapshot_name_backfill.json"

    conn = psycopg2.connect(db_url)
    conn.autocommit = False
    rows_updated = 0
    polluted_rows: list[dict] = []
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"SELECT tenant_id, entity_id, current_run_id, "
                f"current_snapshot_name, previous_run_id, previous_snapshot_name, "
                f"updated_at FROM {args.schema}.tenant_runs "
                f"ORDER BY tenant_id, entity_id"
            )
            for row in cur.fetchall():
                cur_old = row["current_snapshot_name"]
                prev_old = row["previous_snapshot_name"]
                entity_id_row = row["entity_id"]
                # A row needs fixing if its name is non-NULL and non-canonical
                # for its entity_id — covers UUID-shape, cloudedge-*,
                # cloud-spend-*, "cco_summary", and any future non-I5 form.
                cur_needs_fix = (
                    cur_old is not None
                    and not is_canonical(entity_id_row, cur_old)
                )
                prev_needs_fix = (
                    prev_old is not None
                    and not is_canonical(entity_id_row, prev_old)
                )
                if not (cur_needs_fix or prev_needs_fix):
                    continue
                entity_id = row["entity_id"]
                cur_new = (
                    derive_run_name(entity_id, str(row["current_run_id"]))
                    if cur_needs_fix else None
                )
                prev_new = (
                    derive_run_name(entity_id, str(row["previous_run_id"]))
                    if prev_needs_fix and row["previous_run_id"] is not None
                    else None
                )
                polluted_rows.append({
                    "tenant_id": str(row["tenant_id"]),
                    "entity_id": entity_id,
                    "current_run_id": str(row["current_run_id"]),
                    "current_snapshot_name_old": cur_old,
                    "current_snapshot_name_new": cur_new,
                    "previous_run_id": (
                        str(row["previous_run_id"]) if row["previous_run_id"] else None
                    ),
                    "previous_snapshot_name_old": prev_old,
                    "previous_snapshot_name_new": prev_new,
                    "updated_at": row["updated_at"].isoformat(),
                })

        audit = {
            "audit_date": datetime.now(timezone.utc).isoformat(),
            "schema": args.schema,
            "phase": "apply" if args.apply else "audit_only",
            "polluted_row_count": len(polluted_rows),
            "rows": polluted_rows,
        }
        audit_path.write_text(json.dumps(audit, indent=2, sort_keys=False))
        print(f"Audit: {len(polluted_rows)} polluted row(s) → {audit_path}")

        if args.audit_only:
            return 0

        with conn.cursor() as cur:
            for r in polluted_rows:
                if r["current_snapshot_name_new"] is not None:
                    cur.execute(
                        f"UPDATE {args.schema}.tenant_runs "
                        f"SET current_snapshot_name = %s "
                        f"WHERE tenant_id = %s AND entity_id = %s "
                        f"AND current_snapshot_name = %s",
                        (
                            r["current_snapshot_name_new"],
                            r["tenant_id"], r["entity_id"],
                            r["current_snapshot_name_old"],
                        ),
                    )
                    rows_updated += cur.rowcount
                if r["previous_snapshot_name_new"] is not None:
                    cur.execute(
                        f"UPDATE {args.schema}.tenant_runs "
                        f"SET previous_snapshot_name = %s "
                        f"WHERE tenant_id = %s AND entity_id = %s "
                        f"AND previous_snapshot_name = %s",
                        (
                            r["previous_snapshot_name_new"],
                            r["tenant_id"], r["entity_id"],
                            r["previous_snapshot_name_old"],
                        ),
                    )
                    rows_updated += cur.rowcount

            # Residual check: any non-canonical row left? Loop in Python
            # because canonical depends on entity_id (per-row predicate).
            cur.execute(
                f"SELECT entity_id, current_snapshot_name, previous_snapshot_name "
                f"FROM {args.schema}.tenant_runs"
            )
            residual = 0
            for ent, cur_n, prev_n in cur.fetchall():
                if cur_n is not None and not is_canonical(ent, cur_n):
                    residual += 1
                if prev_n is not None and not is_canonical(ent, prev_n):
                    residual += 1

        if residual > 0:
            conn.rollback()
            print(
                f"ERROR: {residual} polluted row(s) remain after UPDATEs — "
                f"rolling back. Manual investigation required.",
                file=sys.stderr,
            )
            return 1

        conn.commit()
        print(
            f"Backfill applied: {rows_updated} UPDATE(s) committed. "
            f"0 polluted rows remain."
        )
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
