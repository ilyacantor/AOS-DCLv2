"""Apply migration 018 — record Farm's farm_run_id on tenant_runs.

Two steps in one transaction:
  1. Run 018_tenant_runs_last_farm_run_id.sql (adds column).
  2. Backfill `last_farm_run_id` by joining DCL's `tenant_runs.current_run_id`
     against Farm's `manifest_runs.dcl_run_id`. Rows with no match stay NULL
     (legacy or non-Farm ingest) — Refresh will treat them as candidates
     and re-ingest idempotently.

Matching against Farm requires a live Farm service at $FARM_API_URL.
"""

import os
import sys
import time

import httpx
import psycopg2
from dotenv import load_dotenv

_repo = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.insert(0, _repo)
load_dotenv(os.path.join(_repo, ".env"))

_SQL_PATH = os.path.join(_repo, "migrations", "018_tenant_runs_last_farm_run_id.sql")


def _column_exists(cur, table: str, column: str) -> bool:
    cur.execute(
        "SELECT 1 FROM information_schema.columns "
        "WHERE table_name = %s AND column_name = %s",
        (table, column),
    )
    return cur.fetchone() is not None


def _fetch_farm_manifest_runs(farm_url: str, page_size: int = 500) -> list[dict]:
    """Pull the full manifest_runs feed via Farm's paginated /api/runs."""
    url = f"{farm_url.rstrip('/')}/api/runs"
    all_rows: list[dict] = []
    offset = 0
    while True:
        resp = httpx.get(
            url,
            params={"limit": page_size, "offset": offset},
            timeout=60.0,
        )
        resp.raise_for_status()
        payload = resp.json()
        if not isinstance(payload, list):
            raise RuntimeError(
                f"Farm /api/runs returned non-list payload: {type(payload).__name__}"
            )
        all_rows.extend(payload)
        if len(payload) < page_size:
            break
        offset += page_size
    return all_rows


def main() -> None:
    url = os.environ["DATABASE_URL"]
    farm_url = os.environ.get("FARM_API_URL")
    if not farm_url:
        raise RuntimeError(
            "FARM_API_URL not set. Backfill requires a live Farm service to "
            "join tenant_runs.current_run_id against manifest_runs.dcl_run_id."
        )

    conn = psycopg2.connect(url, application_name="apply_mig018")
    conn.autocommit = False
    start = time.monotonic()
    try:
        cur = conn.cursor()
        cur.execute("SET statement_timeout = '600000'")

        pre = _column_exists(cur, "tenant_runs", "last_farm_run_id")
        print(f"[018] pre: last_farm_run_id column present = {pre}")

        with open(_SQL_PATH) as fh:
            sql = fh.read()
        cur.execute(sql)

        if not _column_exists(cur, "tenant_runs", "last_farm_run_id"):
            raise RuntimeError("mig018 failed: last_farm_run_id column missing")

        # Build {dcl_run_id: farm_run_id} from Farm's manifest_runs feed.
        farm_runs = _fetch_farm_manifest_runs(farm_url)
        mapping: dict[str, str] = {}
        for row in farm_runs:
            dcl_run_id = row.get("dcl_run_id")
            farm_run_id = row.get("farm_run_id")
            if dcl_run_id and farm_run_id:
                # Newer farm_run_id wins for a given dcl_run_id (shouldn't
                # collide in practice — dcl_run_id is unique per ingest).
                mapping[str(dcl_run_id)] = str(farm_run_id)

        print(f"[018] fetched {len(farm_runs)} manifest_runs; {len(mapping)} have dcl_run_id")

        # Backfill: for every tenant_runs row whose current_run_id appears in
        # Farm's manifest_runs, set last_farm_run_id.
        cur.execute(
            "SELECT tenant_id, entity_id, current_run_id FROM tenant_runs "
            "WHERE last_farm_run_id IS NULL AND current_run_id IS NOT NULL"
        )
        rows = cur.fetchall()
        matched = 0
        for tenant_id, entity_id, current_run_id in rows:
            farm_run_id = mapping.get(str(current_run_id))
            if farm_run_id is None:
                continue
            cur.execute(
                "UPDATE tenant_runs SET last_farm_run_id = %s "
                "WHERE tenant_id = %s AND entity_id = %s",
                (farm_run_id, tenant_id, entity_id),
            )
            matched += 1

        cur.execute(
            "SELECT COUNT(*) FILTER (WHERE last_farm_run_id IS NOT NULL), "
            "COUNT(*) FILTER (WHERE last_farm_run_id IS NULL) "
            "FROM tenant_runs"
        )
        with_id, without_id = cur.fetchone()
        print(
            f"[018] post: tenant_runs with last_farm_run_id={with_id} "
            f"without={without_id} (matched this pass={matched})"
        )

        conn.commit()
        print(
            f"[018] OK in {time.monotonic() - start:.1f}s — "
            f"last_farm_run_id populated via Farm join"
        )
    except Exception:
        conn.rollback()
        print(f"[018] ROLLED BACK after {time.monotonic() - start:.1f}s")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
