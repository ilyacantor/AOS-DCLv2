"""ContextOS Stage 2 — semantic_triples_current canonical current-state surface.

Operator-visible outcome under test: "current state" has ONE definition — the
logical view semantic_triples_current (rows where superseded_at IS NULL, the
is_active generated column). After ingesting N triples for the test tenant, the
view's row count equals semantic_triples WHERE is_active=true equals N. A
surfacing read (the /api/dcl/triples/browse current-state default) returns the
current value (e.g. cloud_spend.summary.total_cost = 100.0 for one row). When a
coordinate is superseded — run1 value 100 replaced by run2 value 130 — the view
shows ONLY 130 (the 100 row left the current surface) while an as-of read at a
timestamp BEFORE the supersession still returns 100 (point-in-time history is
preserved on the same bi-temporal substrate). Re-delivering the same run_id with
?replace=true is idempotent (same current state); ?append=true adds to the run;
neither flag on an existing run → 409 RUN_ALREADY_EXISTS. A usd_thousands value
388.1088 surfaces in the view as the canonical base-USD 388108.80 (Stage 1
write-time normalization is visible through the canonical surface).

Live-service integration test: TestClient drives the real FastAPI app (real
ingest pipeline → real COPY into aos-dev → real supersession + as-of reads)
against the aos-dev database. No faked DB writes (B5). Dedicated test
tenant/entity (uuid5) so demo/other data is never touched.
"""

import sys
import uuid
from pathlib import Path

import pytest

_repo = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_repo))

from dotenv import load_dotenv
load_dotenv(_repo / ".env.development")

from fastapi.testclient import TestClient
from backend.api.main import app
from backend.core.db import get_connection

client = TestClient(app, raise_server_exceptions=False)

TEST_TENANT_ID = str(uuid.uuid5(uuid.NAMESPACE_DNS, "current-state-surface-stage2-test"))
ENTITY = "CurrentSurface-T1"
PIPE_A = "77777777-7777-4777-8777-777777777771"
PIPE_B = "77777777-7777-4777-8777-777777777772"
CONCEPT = "cloud_spend.summary"
PROP = "total_cost"
PERIOD = "2026-03"


# ---------------------------------------------------------------------------
# Helpers — real ingest path + direct dev-DB reads (view vs base table).
# ---------------------------------------------------------------------------

def _triple(source, pipe, value, *, concept=CONCEPT, prop=PROP, period=PERIOD,
            unit="usd", currency="USD"):
    t = {
        "entity_id": ENTITY, "concept": concept, "property": prop,
        "value": value, "period": period, "source_system": source,
        "source_table": "current_surface_probe", "source_field": prop,
        "pipe_id": pipe, "confidence_score": 0.95, "confidence_tier": "exact",
        "fabric_plane": "ipaas",
    }
    if unit is not None:
        t["unit"] = unit
    if currency is not None:
        t["currency"] = currency
    return t


def _ingest(run_id, triples, *, replace=False, append=False):
    """POST through the real ingest-triples path. Returns the Response (callers
    assert status — the 409 test expects a non-2xx)."""
    params = {}
    if replace:
        params["replace"] = "true"
    if append:
        params["append"] = "true"
    return client.post(
        "/api/dcl/ingest-triples",
        params=params,
        json={"tenant_id": TEST_TENANT_ID, "dcl_ingest_id": run_id,
              "entity_id": ENTITY,
              "snapshot_name": f"{ENTITY}-{run_id.replace('-', '')[:4]}",
              "triples": triples},
    )


def _ingest_ok(run_id, triples, *, replace=False, append=False):
    resp = _ingest(run_id, triples, replace=replace, append=append)
    assert resp.status_code == 201, f"ingest failed: {resp.status_code} {resp.text}"
    return resp.json()


def _db_now():
    """The dev DB's own clock — used to capture an as-of instant between two
    ingests without test-host/DB clock-skew error."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT now()")
            return cur.fetchone()[0]


def _view_count_for_tenant():
    """COUNT(*) from the canonical current-state VIEW for the test tenant."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM semantic_triples_current WHERE tenant_id = %s",
                (TEST_TENANT_ID,),
            )
            return cur.fetchone()[0]


def _is_active_count_for_tenant():
    """COUNT(*) from the base table with the is_active=true predicate the view
    formalizes — for the test tenant."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM semantic_triples "
                "WHERE tenant_id = %s AND is_active = true",
                (TEST_TENANT_ID,),
            )
            return cur.fetchone()[0]


def _view_values(concept=CONCEPT, prop=PROP, period=PERIOD):
    """The current value(s) for one coordinate, read straight from the VIEW."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT value FROM semantic_triples_current "
                "WHERE tenant_id = %s AND entity_id = %s AND concept = %s "
                "AND property = %s AND period = %s",
                (TEST_TENANT_ID, ENTITY, concept, prop, period),
            )
            return sorted(float(r[0]) for r in cur.fetchall())


def _asof_values(as_of, concept=CONCEPT, prop=PROP, period=PERIOD):
    """Point-in-time read against the BASE table with the bi-temporal as-of
    predicate (ingested_at <= T AND (superseded_at IS NULL OR superseded_at > T)).
    The view is deliberately bypassed — as-of history lives below the liveness
    flag."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT value FROM semantic_triples "
                "WHERE tenant_id = %s AND entity_id = %s AND concept = %s "
                "AND property = %s AND period = %s "
                "AND ingested_at <= %s "
                "AND (superseded_at IS NULL OR superseded_at > %s)",
                (TEST_TENANT_ID, ENTITY, concept, prop, period, as_of, as_of),
            )
            return sorted(float(r[0]) for r in cur.fetchall())


def _cleanup():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM conflict_dispositions WHERE tenant_id=%s", (TEST_TENANT_ID,))
            cur.execute("DELETE FROM conflict_register WHERE tenant_id=%s", (TEST_TENANT_ID,))
            cur.execute("DELETE FROM semantic_triples WHERE tenant_id=%s", (TEST_TENANT_ID,))
            cur.execute("DELETE FROM tenant_runs WHERE tenant_id=%s", (TEST_TENANT_ID,))
            cur.execute("DELETE FROM tenant_normalization_policy WHERE tenant_id=%s", (TEST_TENANT_ID,))
            conn.commit()


@pytest.fixture(autouse=True)
def _per_test_cleanup():
    """Each test starts and ends with a clean tenant — these tests assert exact
    row counts for the tenant, so they must not see each other's rows."""
    _cleanup()
    yield
    _cleanup()


# ---------------------------------------------------------------------------
# 1. The view IS the canonical current state: count == is_active == N.
# ---------------------------------------------------------------------------

def test_view_equals_is_active():
    run = str(uuid.uuid4())
    # Three distinct current-state coordinates (different periods → 3 rows).
    triples = [
        _triple("billing", PIPE_A, 100, period="2026-01"),
        _triple("billing", PIPE_A, 110, period="2026-02"),
        _triple("billing", PIPE_A, 120, period="2026-03"),
    ]
    n = len(triples)
    _ingest_ok(run, triples)

    view_count = _view_count_for_tenant()
    active_count = _is_active_count_for_tenant()

    assert view_count == n, (
        f"semantic_triples_current must hold exactly the {n} ingested current "
        f"rows for the tenant; got {view_count}"
    )
    assert view_count == active_count, (
        f"the view IS semantic_triples WHERE is_active=true — counts must be "
        f"identical: view={view_count}, is_active={active_count}"
    )


# ---------------------------------------------------------------------------
# 2. Surfacing reads the canonical surface: the browse current-state default
#    returns the current values, and they match the view.
# ---------------------------------------------------------------------------

def test_surfacing_reads_current_view():
    run = str(uuid.uuid4())
    _ingest_ok(run, [
        _triple("billing", PIPE_A, 100, period="2026-01"),
        _triple("billing", PIPE_A, 110, period="2026-02"),
    ])

    # /api/dcl/triples/browse with no as_of / no run_id = the current-state
    # default (is_active=true) — the surfacing read path. read-only GET.
    resp = client.get(
        "/api/dcl/triples/browse",
        params={"tenant_id": TEST_TENANT_ID, "entity_id": ENTITY,
                "domain": "cloud_spend"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()

    assert body["total_count"] == 2, (
        f"surfacing must show exactly the 2 current rows; got "
        f"{body['total_count']}"
    )
    surfaced = sorted(
        (t["period"], float(t["value"])) for t in body["triples"]
    )
    assert surfaced == [("2026-01", 100.0), ("2026-02", 110.0)], (
        f"surfacing must render the current values 100.0/110.0 at their "
        f"periods; got {surfaced}"
    )
    # And the surfaced current set equals the canonical view's set — the
    # surface reads the same definition the view formalizes.
    assert _view_values(period="2026-01") == [100.0]
    assert _view_values(period="2026-02") == [110.0]


# ---------------------------------------------------------------------------
# 3. Supersession leaves the current surface; as-of preserves the prior value.
# ---------------------------------------------------------------------------

def test_supersession_leaves_current_keeps_asof():
    run1 = str(uuid.uuid4())
    run2 = str(uuid.uuid4())

    # run1: value 100 at (ENTITY, CONCEPT, PROP, PERIOD).
    _ingest_ok(run1, [_triple("billing", PIPE_A, 100)])
    assert _view_values() == [100.0], "run1 value 100 must be current"

    # Capture an as-of instant AFTER run1 is live but BEFORE run2 supersedes it.
    t_before = _db_now()

    # run2 (?replace=true): value 130 at the SAME coordinate. replace supersedes
    # the tenant's prior live rows (SET superseded_at = now()) and installs run2.
    _ingest_ok(run2, [_triple("billing", PIPE_B, 130)], replace=True)

    # (a) Current view shows ONLY 130 — the 100 row left the current surface.
    assert _view_values() == [130.0], (
        f"after supersession the current view must show ONLY 130 for the "
        f"coordinate (the 100 row is superseded, gone from current); got "
        f"{_view_values()}"
    )

    # (b) As-of read at t_before still returns 100 — point-in-time preserved on
    #     the same substrate the view sits over.
    assert _asof_values(t_before) == [100.0], (
        f"an as-of read BEFORE the supersession must still return 100 "
        f"(history preserved); got {_asof_values(t_before)}"
    )

    # Coherence: current-view (130) and as-of-before (100) disagree by design —
    # one substrate, two timelines.
    assert _view_values() != _asof_values(t_before)


# ---------------------------------------------------------------------------
# 4. Idempotency of the write path is preserved (the view reflects it).
# ---------------------------------------------------------------------------

def test_idempotency_preserved():
    run = str(uuid.uuid4())
    _ingest_ok(run, [_triple("billing", PIPE_A, 100)])
    assert _view_values() == [100.0]

    # Same run_id re-delivered with ?replace=true → idempotent: the current
    # state is the same single 100 row (one current coordinate, not two).
    _ingest_ok(run, [_triple("billing", PIPE_A, 100)], replace=True)
    assert _view_values() == [100.0], (
        f"replace re-delivery must be idempotent — current state stays the "
        f"single value 100; got {_view_values()}"
    )
    assert _view_count_for_tenant() == 1, (
        f"replace re-delivery must not compound rows; expected 1 current row, "
        f"got {_view_count_for_tenant()}"
    )

    # ?append=true adds a NEW coordinate to the SAME run → both are current.
    _ingest_ok(run, [_triple("billing", PIPE_A, 200, period="2026-04")],
               append=True)
    assert _view_count_for_tenant() == 2, (
        f"append must add to the run — expected 2 current rows, got "
        f"{_view_count_for_tenant()}"
    )
    assert _view_values(period="2026-04") == [200.0]
    assert _view_values(period=PERIOD) == [100.0], "the original row stays current"

    # Neither flag on an existing run → 409 RUN_ALREADY_EXISTS (loud, no
    # silent overwrite or merge).
    resp = _ingest(run, [_triple("billing", PIPE_A, 999)])
    assert resp.status_code == 409, (
        f"re-ingesting an existing run with no replace/append flag must 409; "
        f"got {resp.status_code}: {resp.text}"
    )
    assert resp.json()["detail"]["error"] == "RUN_ALREADY_EXISTS", resp.text
    # And the store is untouched by the rejected ingest — still the 2 rows.
    assert _view_count_for_tenant() == 2
    assert _view_values(period=PERIOD) == [100.0]


# ---------------------------------------------------------------------------
# 5. Stage-1 write-time normalization is visible through the canonical surface.
# ---------------------------------------------------------------------------

def test_normalization_flows_to_view():
    run = str(uuid.uuid4())
    # general_ledger reports thousands-of-dollars: 388.1088 usd_thousands.
    # Stage-1 normalizes at ingest to base USD (388.1088 * 1000 = 388108.80).
    _ingest_ok(run, [
        _triple("general_ledger", PIPE_A, 388.10880, unit="usd_thousands"),
    ])

    # The view shows the CANONICAL base-USD value, not the raw 388.1088.
    assert _view_values() == [388108.80], (
        f"the canonical current-state view must surface the Stage-1 normalized "
        f"value 388108.80 (388.1088 usd_thousands → base usd), not the raw "
        f"388.1088; got {_view_values()}"
    )
