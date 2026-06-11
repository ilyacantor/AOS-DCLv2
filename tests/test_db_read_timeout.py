"""dcl ledger #62 — the dev read statement_timeout must actually bind.

The Supabase pooler ignores the connection-string options param on both
ports (verified 2026-06-11), so the pool-init `-c statement_timeout=...`
never reached dev connections and the 15s read ceiling was silently absent.
get_connection() now issues `SET LOCAL statement_timeout` per borrow — the
txn-safe mechanism the ingest paths already use. This test pins the binding:
the value visible INSIDE a borrow must be the configured ceiling, not the
role default (2min). Enforcement (pg_sleep cancel at ~15s) was proven live
on 2026-06-11 and is not re-run per suite — a 15s sleep per run buys no
additional signal over the binding assertion.
"""

from backend.core.constants import QUERY_STATEMENT_TIMEOUT_MS
from backend.core.db import get_connection


def test_read_statement_timeout_binds_inside_borrow():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SHOW statement_timeout")
            bound = cur.fetchone()[0]
    expected_s = QUERY_STATEMENT_TIMEOUT_MS // 1000
    assert bound == f"{expected_s}s", (
        f"statement_timeout inside a get_connection borrow is {bound!r}, "
        f"expected {expected_s}s — the SET LOCAL binding (ledger #62) is not "
        f"reaching the pooled connection; reads are riding the role default."
    )


def test_write_paths_keep_their_own_longer_budget():
    """A later SET LOCAL in the same transaction overrides the borrow-time
    ceiling — the ingest stores' longer budgets are unchanged."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SET LOCAL statement_timeout = 300000")
            cur.execute("SHOW statement_timeout")
            assert cur.fetchone()[0] == "5min"


def test_binding_covers_explicit_cursor_factories():
    """canonical_registry borrows RealDictCursor cursors explicitly — the
    binding must wrap whatever cursor class the caller requests, not only
    the default cursor."""
    from psycopg2.extras import RealDictCursor

    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SHOW statement_timeout")
            row = cur.fetchone()
    expected_s = QUERY_STATEMENT_TIMEOUT_MS // 1000
    assert row["statement_timeout"] == f"{expected_s}s", row
