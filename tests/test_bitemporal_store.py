"""Gate 0 acceptance: bi-temporal triple store (ContextOS_Blueprint_v1 §6/§15).

Operator-visible outcome under test: when a new ingest run for BitempCo-TEST
replaces revenue.total = 100.0 with revenue.total = 200.0, the live browse
returns exactly 200.0, an as-of read pinned between the two ingests returns
exactly 100.0, the run diff names revenue.total as changed 100.0 → 200.0 with
exact counts, and the store row total never decreases — supersession closes
knowledge windows, nothing is deleted.

Live-service integration tests: TestClient drives the real FastAPI app against
the aos-dev database (same path Farm/AAM hit over HTTP).
"""

import datetime
import sys
import time
import uuid
from pathlib import Path

import psycopg2
import pytest

_repo = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_repo))

from dotenv import load_dotenv
load_dotenv(_repo / ".env.development")

from fastapi.testclient import TestClient
from backend.api.main import app
from backend.core.db import get_connection

client = TestClient(app, raise_server_exceptions=False)

TEST_TENANT_ID = str(uuid.uuid5(uuid.NAMESPACE_DNS, "bitemporal-gate0-test"))
ENTITY = "BitempCo-TEST"
PIPE = "44444444-4444-4444-4444-444444444444"

CONCEPTS = [
    ("revenue.total", "amount", "2025-Q1"),
    ("revenue.total", "amount", "2025-Q2"),
    ("workforce.headcount.total", "count", "2025-Q1"),
]


def _triples(value: float):
    return [
        {
            "entity_id": ENTITY,
            "concept": concept,
            "property": prop,
            "value": value,
            "period": period,
            "source_system": "netsuite",
            "source_table": "bitemp_test",
            "source_field": prop,
            "pipe_id": PIPE,
            "confidence_score": 0.95,
            "confidence_tier": "exact",
            "fabric_plane": "ipaas",
        }
        for (concept, prop, period) in CONCEPTS
    ]


def _push(run_id: str, value: float, *, replace: bool = False):
    qs = "?replace=true" if replace else ""
    resp = client.post(
        f"/api/dcl/ingest-triples{qs}",
        json={
            "tenant_id": TEST_TENANT_ID,
            "dcl_ingest_id": run_id,
            "entity_id": ENTITY,
            "snapshot_name": f"{ENTITY}-{run_id.replace('-', '')[:4]}",
            "triples": _triples(value),
        },
    )
    assert resp.status_code == 201, f"ingest failed: {resp.status_code} {resp.text}"
    return resp.json()


def _entity_rows():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*),"
                "       count(*) FILTER (WHERE is_active),"
                "       count(*) FILTER (WHERE superseded_at IS NOT NULL) "
                "FROM semantic_triples WHERE tenant_id = %s AND entity_id = %s",
                (TEST_TENANT_ID, ENTITY),
            )
            return cur.fetchone()


def _browse(**params):
    q = {"tenant_id": TEST_TENANT_ID, "entity_id": ENTITY, "domain": "revenue",
         "limit": 50, **params}
    resp = client.get("/api/dcl/triples/browse", params=q)
    return resp


def _cleanup():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM semantic_triples WHERE tenant_id = %s", (TEST_TENANT_ID,)
            )
            cur.execute(
                "DELETE FROM tenant_runs WHERE tenant_id = %s", (TEST_TENANT_ID,)
            )
            conn.commit()


@pytest.fixture(scope="module", autouse=True)
def bitemp_runs():
    """Ingest run A (value 100.0), capture the between-instant, ingest run B
    (value 200.0). Module-scoped: every test reads the same timeline."""
    _cleanup()
    run_a, run_b = str(uuid.uuid4()), str(uuid.uuid4())
    _push(run_a, 100.0)
    time.sleep(1.1)  # ingested_at granularity guard around t_mid
    t_mid = datetime.datetime.now(datetime.timezone.utc).isoformat()
    time.sleep(1.1)
    _push(run_b, 200.0)
    yield {"run_a": run_a, "run_b": run_b, "t_mid": t_mid}
    _cleanup()


class TestSupersessionLifecycle:
    def test_01_current_view_returns_new_value_only(self, bitemp_runs):
        resp = _browse(period="2025-Q1")
        assert resp.status_code == 200, resp.text
        rows = resp.json()["triples"]
        assert len(rows) == 1, f"expected exactly 1 live revenue 2025-Q1 row, got {len(rows)}"
        assert float(rows[0]["value"]) == 200.0
        assert rows[0]["run_id"] == bitemp_runs["run_b"]

    def test_02_nothing_deleted_history_grows(self, bitemp_runs):
        total, active, superseded = _entity_rows()
        assert total == 6, f"expected 6 rows (3 per run, both retained), got {total}"
        assert active == 3, f"expected 3 live rows, got {active}"
        assert superseded == 3, f"expected 3 superseded rows, got {superseded}"

    def test_03_asof_returns_old_value(self, bitemp_runs):
        resp = _browse(period="2025-Q1", as_of=bitemp_runs["t_mid"])
        assert resp.status_code == 200, resp.text
        rows = resp.json()["triples"]
        assert len(rows) == 1, f"as-of t_mid expected 1 row, got {len(rows)}"
        assert float(rows[0]["value"]) == 100.0
        assert rows[0]["run_id"] == bitemp_runs["run_a"]

    def test_04_asof_before_first_ingest_is_empty(self, bitemp_runs):
        resp = _browse(as_of="2020-01-01T00:00:00+00:00")
        assert resp.status_code == 200
        assert resp.json()["triples"] == []
        assert resp.json()["total_count"] == 0

    def test_05_superseded_rows_carry_knowledge_window(self, bitemp_runs):
        resp = _browse(run_id=bitemp_runs["run_a"], period="2025-Q1")
        rows = resp.json()["triples"]
        assert len(rows) == 1
        row = rows[0]
        assert row["is_active"] is False
        assert row["superseded_at"] is not None
        assert row["ingested_at"] is not None
        assert row["ingested_at"] < row["superseded_at"]

    def test_06_replace_replay_is_idempotent_no_duplicate_history(self, bitemp_runs):
        _push(bitemp_runs["run_b"], 200.0, replace=True)
        total, active, superseded = _entity_rows()
        assert (total, active, superseded) == (6, 3, 3), (
            f"replay must not grow history: got total={total} active={active} "
            f"superseded={superseded}"
        )
        resp = _browse(period="2025-Q1")
        assert float(resp.json()["triples"][0]["value"]) == 200.0

    def test_07_generated_is_active_rejects_direct_writes(self):
        with get_connection() as conn:
            with conn.cursor() as cur:
                with pytest.raises(psycopg2.errors.GeneratedAlways):
                    cur.execute(
                        "UPDATE semantic_triples SET is_active = false "
                        "WHERE tenant_id = %s", (TEST_TENANT_ID,),
                    )
            conn.rollback()


class TestRunDiff:
    def test_08_diff_default_pair_names_the_change(self, bitemp_runs):
        resp = client.get(
            "/api/dcl/triples/runs/diff",
            params={"tenant_id": TEST_TENANT_ID, "entity_id": ENTITY},
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["tenant_id"] == TEST_TENANT_ID
        assert body["entity_id"] == ENTITY
        assert body["dcl_ingest_id_base"] == bitemp_runs["run_a"]
        assert body["dcl_ingest_id_compare"] == bitemp_runs["run_b"]
        assert body["counts"] == {"added": 0, "removed": 0, "changed": 3, "unchanged": 0}
        changed = {(s["concept"], s["period"]) for s in body["samples"]["changed"]}
        assert ("revenue.total", "2025-Q1") in changed
        rev_q1 = next(s for s in body["samples"]["changed"]
                      if (s["concept"], s["period"]) == ("revenue.total", "2025-Q1"))
        assert float(rev_q1["base_value"]) == 100.0
        assert float(rev_q1["compare_value"]) == 200.0
        assert "run_id" not in body, "bare run_id banned from response payloads (I1)"

    def test_09_diff_unknown_entity_is_readable_404(self):
        resp = client.get(
            "/api/dcl/triples/runs/diff",
            params={"tenant_id": TEST_TENANT_ID, "entity_id": "NoSuchCo-XX"},
        )
        assert resp.status_code == 404
        assert "no ingest history" in resp.json()["detail"].lower()

    def test_10_asof_malformed_is_readable_400(self):
        resp = _browse(as_of="yesterday-ish")
        assert resp.status_code == 400
        assert "ISO-8601" in resp.json()["detail"]


class TestDeterminism:
    def test_11_reads_twice_identical(self, bitemp_runs):
        a1 = _browse().json()
        a2 = _browse().json()
        assert a1 == a2, "live browse must be deterministic (B14)"
        b1 = _browse(as_of=bitemp_runs["t_mid"]).json()
        b2 = _browse(as_of=bitemp_runs["t_mid"]).json()
        assert b1 == b2, "as-of browse must be deterministic (B14)"
