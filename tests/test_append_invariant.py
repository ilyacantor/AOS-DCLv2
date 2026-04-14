"""Write-path invariant tests — append_rows_for_entity must keep
semantic_triples, current_triples, and tenant_runs.run_row_count in
lockstep on every batch.

Regression guard for a drift bug where multi-batch append ingests left
current_triples stale (5000 rows) while semantic_triples grew to 20306.
The root cause was that append_rows_for_entity only COPYed into
semantic_triples and relied on a finalizing swap_and_delete call that
callers could omit.
"""

import sys
import uuid
from pathlib import Path

import pytest

_repo = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_repo))

from dotenv import load_dotenv
load_dotenv(_repo / ".env")

from backend.core.db import get_connection
from backend.db.triple_store import TripleStore


TEST_TENANT_ID = str(uuid.uuid5(uuid.NAMESPACE_DNS, "append-invariant-tenant"))
TEST_RUN_ID = str(uuid.uuid5(uuid.NAMESPACE_DNS, "append-invariant-run"))
TEST_OTHER_RUN_ID = str(uuid.uuid5(uuid.NAMESPACE_DNS, "append-invariant-run-other"))
TEST_ENTITY_ID = "AppendInvariant-A1"


def _make_rows(n: int, run_id: str = TEST_RUN_ID) -> list[dict]:
    return [
        {
            "tenant_id": TEST_TENANT_ID,
            "entity_id": TEST_ENTITY_ID,
            "concept": "revenue.total",
            "property": "amount",
            "value": i * 1000,
            "period": "2025-Q1",
            "currency": "USD",
            "unit": "dollars",
            "source_system": "test",
            "run_id": run_id,
            "confidence_score": 0.95,
            "confidence_tier": "high",
        }
        for i in range(n)
    ]


def _counts():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM semantic_triples "
                "WHERE tenant_id = %s AND entity_id = %s",
                (TEST_TENANT_ID, TEST_ENTITY_ID),
            )
            st = cur.fetchone()[0]
            cur.execute(
                "SELECT COUNT(*) FROM current_triples "
                "WHERE tenant_id = %s AND entity_id = %s",
                (TEST_TENANT_ID, TEST_ENTITY_ID),
            )
            ct = cur.fetchone()[0]
            cur.execute(
                "SELECT run_row_count FROM tenant_runs "
                "WHERE tenant_id = %s AND entity_id = %s",
                (TEST_TENANT_ID, TEST_ENTITY_ID),
            )
            row = cur.fetchone()
            rrc = row[0] if row else None
    return st, ct, rrc


@pytest.fixture(autouse=True)
def cleanup():
    def _clean():
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM semantic_triples WHERE tenant_id = %s",
                    (TEST_TENANT_ID,),
                )
                cur.execute(
                    "DELETE FROM current_triples WHERE tenant_id = %s",
                    (TEST_TENANT_ID,),
                )
                cur.execute(
                    "DELETE FROM tenant_runs WHERE tenant_id = %s",
                    (TEST_TENANT_ID,),
                )
                conn.commit()
    _clean()
    yield
    _clean()


def test_append_only_keeps_invariant_without_finalizing_swap():
    """Five append batches followed by zero swaps must still satisfy
    semantic_triples == current_triples == tenant_runs.run_row_count.

    This is the regression case that caused the SysEdge-IKK4 drift — the
    caller used append mode for every batch and never issued a finalizing
    swap_and_delete, and the old append path only wrote to semantic_triples.
    """
    store = TripleStore()
    batch_sizes = [100, 200, 50, 300, 150]

    for n in batch_sizes:
        store.append_rows_for_entity(
            tenant_id=TEST_TENANT_ID,
            entity_id=TEST_ENTITY_ID,
            new_run_id=TEST_RUN_ID,
            new_rows=_make_rows(n),
        )
        st, ct, rrc = _counts()
        assert st == ct == rrc, (
            f"invariant drift after batch of {n}: "
            f"semantic={st} current={ct} run_row_count={rrc}"
        )

    total = sum(batch_sizes)
    st, ct, rrc = _counts()
    assert st == ct == rrc == total, (
        f"final totals wrong: semantic={st} current={ct} run_row_count={rrc} expected={total}"
    )


def test_append_after_swap_still_keeps_invariant():
    """Mix the failure mode that actually hit SysEdge-IKK4: first batch
    goes through swap mode, subsequent batches through append mode. The
    fix must maintain the invariant regardless of which mode each batch used.
    """
    store = TripleStore()

    store.swap_and_delete(
        tenant_id=TEST_TENANT_ID,
        entity_id=TEST_ENTITY_ID,
        new_run_id=TEST_RUN_ID,
        snapshot_name=None,
        new_rows=_make_rows(500),
    )
    st, ct, rrc = _counts()
    assert st == ct == rrc == 500

    for n in (300, 200, 400):
        store.append_rows_for_entity(
            tenant_id=TEST_TENANT_ID,
            entity_id=TEST_ENTITY_ID,
            new_run_id=TEST_RUN_ID,
            new_rows=_make_rows(n),
        )
        st, ct, rrc = _counts()
        assert st == ct == rrc, (
            f"drift after append of {n} following swap: "
            f"semantic={st} current={ct} run_row_count={rrc}"
        )

    st, ct, rrc = _counts()
    assert st == ct == rrc == 1400


def test_append_rejects_run_id_transition():
    """append_rows_for_entity must not silently transition to a new run_id;
    that path requires archive and is owned by swap_and_delete."""
    store = TripleStore()

    store.swap_and_delete(
        tenant_id=TEST_TENANT_ID,
        entity_id=TEST_ENTITY_ID,
        new_run_id=TEST_RUN_ID,
        snapshot_name=None,
        new_rows=_make_rows(100),
    )

    with pytest.raises(ValueError, match="cannot transition"):
        store.append_rows_for_entity(
            tenant_id=TEST_TENANT_ID,
            entity_id=TEST_ENTITY_ID,
            new_run_id=TEST_OTHER_RUN_ID,
            new_rows=_make_rows(50, run_id=TEST_OTHER_RUN_ID),
        )
