"""Store-architecture invariants — post migrations 014/015.

These assertions encode the contract established by the store rebuild:
- semantic_triples holds only live-run rows (one per tenant_runs entry)
- current_triples is a byte-for-byte flat mirror of semantic_triples
- tenant_runs.run_row_count equals COUNT(*) per (tenant, entity)
- current_triples has no run_id column

If any invariant fails the store is in an unsafe state and writes must stop.
"""

import sys
from pathlib import Path

import pytest

_repo = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_repo))

from dotenv import load_dotenv
load_dotenv(_repo / ".env")

from backend.core.db import get_connection
from backend.db.triple_store import TENANT_RUNS_CAP


@pytest.fixture(scope="module")
def db_cursor():
    with get_connection() as conn:
        with conn.cursor() as cur:
            yield cur


def test_semantic_equals_sum_run_row_count(db_cursor):
    """COUNT(*) semantic_triples must equal SUM(run_row_count) tenant_runs."""
    db_cursor.execute("SELECT COUNT(*) FROM semantic_triples")
    semantic_count = db_cursor.fetchone()[0]

    db_cursor.execute("SELECT COALESCE(SUM(run_row_count), 0) FROM tenant_runs")
    sum_run_row_count = db_cursor.fetchone()[0]

    assert semantic_count == sum_run_row_count, (
        f"semantic_triples count ({semantic_count}) != "
        f"SUM(tenant_runs.run_row_count) ({sum_run_row_count}). "
        f"Drift means the pointer swap or backfill is broken."
    )


def test_current_equals_semantic(db_cursor):
    """COUNT(*) current_triples must equal COUNT(*) semantic_triples."""
    db_cursor.execute("SELECT COUNT(*) FROM semantic_triples")
    semantic_count = db_cursor.fetchone()[0]

    db_cursor.execute("SELECT COUNT(*) FROM current_triples")
    current_count = db_cursor.fetchone()[0]

    assert semantic_count == current_count, (
        f"current_triples ({current_count}) != semantic_triples ({semantic_count}). "
        f"The flat mirror has drifted from the live slice."
    )


def test_per_entity_count_matches_run_row_count(db_cursor):
    """Every tenant_runs row's run_row_count must equal its current_triples slice."""
    db_cursor.execute(
        "SELECT tenant_id, entity_id, run_row_count FROM tenant_runs"
    )
    rows = db_cursor.fetchall()
    assert rows, "tenant_runs is empty — cannot validate invariant"

    mismatches = []
    for tenant_id, entity_id, declared in rows:
        db_cursor.execute(
            "SELECT COUNT(*) FROM current_triples "
            "WHERE tenant_id = %s AND entity_id = %s",
            (tenant_id, entity_id),
        )
        actual = db_cursor.fetchone()[0]
        if actual != declared:
            mismatches.append((tenant_id, entity_id, declared, actual))

    assert not mismatches, (
        f"{len(mismatches)} (tenant, entity) pairs drifted between "
        f"tenant_runs.run_row_count and current_triples. First five: "
        f"{mismatches[:5]}"
    )


def test_current_triples_has_no_run_id(db_cursor):
    """current_triples must not carry a run_id column — it is a flat mirror."""
    db_cursor.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'current_triples'"
    )
    cols = {row[0] for row in db_cursor.fetchall()}
    assert "run_id" not in cols, (
        f"current_triples has a run_id column — it must be flat. Columns: {sorted(cols)}"
    )


def test_semantic_equals_current_per_entity(db_cursor):
    """Every (tenant, entity) in tenant_runs must have matching row counts
    in semantic_triples and current_triples. The write path keeps both in lockstep,
    so any divergence means the flat mirror has drifted from the live slice."""
    db_cursor.execute("SELECT tenant_id, entity_id FROM tenant_runs")
    rows = db_cursor.fetchall()
    assert rows, "tenant_runs is empty"

    drifted = []
    for tenant_id, entity_id in rows:
        db_cursor.execute(
            "SELECT COUNT(*) FROM semantic_triples "
            "WHERE tenant_id = %s AND entity_id = %s",
            (tenant_id, entity_id),
        )
        st = db_cursor.fetchone()[0]
        db_cursor.execute(
            "SELECT COUNT(*) FROM current_triples "
            "WHERE tenant_id = %s AND entity_id = %s",
            (tenant_id, entity_id),
        )
        ct = db_cursor.fetchone()[0]
        if st != ct:
            drifted.append((tenant_id, entity_id, st, ct))

    assert not drifted, (
        f"{len(drifted)} (tenant, entity) pairs diverged between "
        f"semantic_triples and current_triples. First five: {drifted[:5]}"
    )


def test_tenant_runs_per_tenant_cap(db_cursor):
    """Every tenant must have at most TENANT_RUNS_CAP entries in tenant_runs.

    Enforced atomically inside swap_and_delete; this test catches regressions
    where the cap-enforcement step is bypassed or skipped.
    """
    db_cursor.execute(
        "SELECT tenant_id, COUNT(*) FROM tenant_runs "
        "GROUP BY tenant_id HAVING COUNT(*) > %s",
        (TENANT_RUNS_CAP,),
    )
    over_cap = db_cursor.fetchall()
    assert not over_cap, (
        f"{len(over_cap)} tenant(s) exceed the per-tenant cap of "
        f"{TENANT_RUNS_CAP}: {over_cap}"
    )
