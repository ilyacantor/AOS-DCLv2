"""Migration-runner ledger invariants (ledger #87).

The boot pass used to re-apply ALL migrations on every boot; on the large dev
store that exceeded the 30s boot budget and #82 fail-loud turned the timeout
into an aborted boot. The runner now records each applied file in
schema_migrations and skips already-applied, unchanged files. These tests pin
the ledger contract against the live aos-dev store (which is fully migrated and
backfilled).
"""

import glob
import os
import sys
from pathlib import Path

import psycopg2
import pytest

_repo = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_repo))
sys.path.insert(0, str(_repo / "migrations"))

from dotenv import load_dotenv
load_dotenv(_repo / ".env.development")

from run_migration import _checksum  # noqa: E402


def _conn():
    return psycopg2.connect(os.environ["DATABASE_URL"])


def test_checksum_is_deterministic_and_whitespace_normalized():
    base = "CREATE TABLE x (id int);\n"
    assert _checksum(base) == _checksum(base)
    # Trailing-whitespace / blank-line churn must NOT change the checksum
    # (a pure format touch should not force a re-run)...
    assert _checksum(base) == _checksum("CREATE TABLE x (id int);   \n\n")
    # ...but a real SQL change must.
    assert _checksum(base) != _checksum("CREATE TABLE x (id bigint);\n")


def test_ledger_covers_every_migration_file_on_dev():
    """Every migrations/*.sql is recorded as applied with a matching checksum.

    Proves the dev store is on the fast path: a boot re-run applies 0 and
    skips all — the condition that lets :8104 boot inside the 30s budget."""
    files = {os.path.basename(f): _checksum(open(f).read())
             for f in glob.glob(str(_repo / "migrations" / "*.sql"))}
    assert files, "no migration files found"

    conn = _conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT filename, checksum FROM schema_migrations")
        ledger = dict(cur.fetchall())
    finally:
        conn.close()

    missing = sorted(set(files) - set(ledger))
    assert not missing, f"migration files not recorded in ledger: {missing}"

    drifted = sorted(
        name for name, csum in files.items()
        if ledger.get(name) != csum
    )
    assert not drifted, (
        f"ledger checksum drift (these would re-run on next boot, risking the "
        f"30s budget): {drifted}"
    )
