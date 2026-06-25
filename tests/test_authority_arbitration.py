"""ContextOS Stage 5 — authority arbitration + as-of (backend gate).

Operator-visible outcome under test:

  GATE 1 (decisive value + disclosure): the ContextOSDemo cloud_spend conflict
  for 2026-03 — aws_billing 409,974.93 vs netsuite_gl_allocation 388,829.94 —
  resolves to ONE decisive value, 409,974.93 from aws_billing (the provider
  invoice is authoritative over the GL allocation), with netsuite_gl_allocation
  disclosed at 388,829.94. The headcount engineering 2026-03 conflict —
  workday_hr 95 vs netsuite_finance_rollup 102 — resolves to 95 from workday_hr
  (HRIS is the system of record), disclosing netsuite_finance_rollup at 102.
  A conflict whose sources are covered by NO authority rule does NOT get a
  silent pick: it stays escalated with decisive_value None and every claim
  disclosed (A1).

  GATE 2 (as-of point-in-time): after a coordinate is superseded (run1 value 100
  replaced by run2 value 130), the live surface shows 130 while an as-of read
  through GET /api/dcl/triples/browse?as_of=<t before the supersession> still
  returns 100. Two timelines on one bi-temporal substrate.

Live-service integration test (B1/B5/B17 spirit): TestClient drives the real
FastAPI app against the aos-dev database. Gate-1 uses the already-ingested
ContextOSDemo entity (it is NOT re-ingested — the fixture only PUTs the demo
tenant's authority map and re-detects via the real endpoints). Gate-2 and the
escalate case use dedicated throwaway tenants (uuid5) ingested through the real
pipeline and cleaned up, so demo and other tenants are never touched.
"""

import os
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

# Refuse to run against prod (Dev/Prod DB separation; I6 env discipline).
_PROD_PROJECT_REF = "gdbmdrouocxjxiohpixr"
_DB_URL = os.environ.get("DATABASE_URL") or os.environ.get("SUPABASE_DB_URL") or ""

# Already-ingested ContextOS demo (do NOT re-ingest — fixture only seeds the
# authority map + re-detects).
DEMO_TENANT = "51aee6ec-15c3-4fb0-833a-a19bb4511296"
DEMO_ENTITY = "ContextOSDemo"
DEMO_INGEST = "e4ec6c50-2104-46e4-b173-74d579b1a136"

# The two authority decisions under test (provider invoice over GL allocation;
# HRIS over finance payroll rollup) plus workforce (same HRIS rule).
DEMO_AUTHORITY = {
    "cloud_spend": ["aws_billing", "netsuite_gl_allocation"],
    "headcount": ["workday_hr", "netsuite_finance_rollup"],
    "workforce": ["workday_hr", "netsuite_finance_rollup"],
}


@pytest.fixture(scope="module", autouse=True)
def _guard_dev_db():
    assert _PROD_PROJECT_REF not in _DB_URL, (
        f"REFUSING TO RUN: DATABASE_URL points at PROD {_PROD_PROJECT_REF!r}. "
        f"Authority-arbitration tests run ONLY against aos-dev."
    )
    yield


# ---------------------------------------------------------------------------
# Gate 1 — seed the demo authority map + re-detect through the real endpoints.
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def demo_conflicts(_guard_dev_db):
    """Seed the demo tenant's authority map (real PUT) and re-detect the demo
    entity (real POST). Returns {(concept, property, period): conflict} for the
    entity, each carrying the computed `resolved` field."""
    for prefix, ranked in DEMO_AUTHORITY.items():
        resp = client.put(
            "/api/dcl/conflicts/authority-map",
            json={"tenant_id": DEMO_TENANT, "concept_prefix": prefix,
                  "ranked_sources": ranked},
        )
        assert resp.status_code == 200, f"authority PUT {prefix} failed: {resp.text}"

    resp = client.post(
        "/api/dcl/conflicts/detect",
        json={"tenant_id": DEMO_TENANT, "entity_id": DEMO_ENTITY,
              "dcl_ingest_id": DEMO_INGEST},
    )
    assert resp.status_code == 200, f"detect failed: {resp.text}"

    # Read the register back through the list endpoint (this is where the
    # computed `resolved` field is attached) — entity-scoped (the demo tenant
    # holds multiple entities; the operator surface is always entity-scoped).
    resp = client.get(
        "/api/dcl/conflicts",
        params={"tenant_id": DEMO_TENANT, "entity_id": DEMO_ENTITY, "limit": "500"},
    )
    assert resp.status_code == 200, resp.text
    out = {}
    for c in resp.json()["conflicts"]:
        out[(c["concept"], c["property"], c.get("period"))] = c
    return out


def _claim_value(conflict, source):
    for cl in conflict["claims"]:
        if cl["source_system"] == source:
            return cl.get("value")
    raise AssertionError(f"source {source!r} not in claims of {conflict['conflict_id']}")


def test_cloud_conflict_resolves_to_billing(demo_conflicts):
    """cloud_spend.summary/total_cost/2026-03 → ONE decisive value from aws_billing
    (the provider invoice is authoritative over the GL allocation); the GL
    allocation is disclosed as the loser. Values are read from the conflict's OWN
    claims (Farm-derived at ingest), never hardcoded — the test verifies the
    arbitration LOGIC, robust to Farm regeneration (B8/B10)."""
    c = demo_conflicts[("cloud_spend.summary", "total_cost", "2026-03")]
    billing = _claim_value(c, "aws_billing")
    gl = _claim_value(c, "netsuite_gl_allocation")
    assert billing != gl, f"the conflict must be a real value disagreement; both are {billing}"

    r = c["resolved"]
    assert r["status"] == "resolved", f"expected resolved, got {r}"
    assert r["decisive_source"] == "aws_billing", r
    assert r["basis"] == "authority", r
    assert r["decisive_value"] == billing, (
        f"the provider invoice is authoritative — decisive value must be the "
        f"aws_billing claim {billing}; got {r['decisive_value']}"
    )

    # The loser is disclosed by source and value — and ONLY the loser.
    assert r["disclosed"] == [
        {"source_system": "netsuite_gl_allocation", "value": gl}
    ], f"loser must be disclosed as netsuite_gl_allocation {gl}; got {r['disclosed']}"

    # The disclosed spread between the two sources.
    assert abs(r["gap_abs"] - abs(billing - gl)) < 0.01, (
        f"gap_abs must be the spread |{billing} - {gl}|; got {r['gap_abs']}"
    )


def test_headcount_resolves_to_hr(demo_conflicts):
    """headcount.by_department/engineering/2026-03 → decisive value from workday_hr
    (HRIS system of record); netsuite_finance_rollup disclosed. Values read from the
    conflict's OWN claims, never hardcoded — verifies the arbitration LOGIC, robust
    to Farm regeneration (B8/B10)."""
    c = demo_conflicts[("headcount.by_department", "engineering", "2026-03")]
    hr = _claim_value(c, "workday_hr")
    fin = _claim_value(c, "netsuite_finance_rollup")
    assert hr != fin, f"the conflict must be a real disagreement; both are {hr}"

    r = c["resolved"]
    assert r["status"] == "resolved", f"expected resolved, got {r}"
    assert r["decisive_source"] == "workday_hr", r
    assert r["basis"] == "authority", r
    assert r["decisive_value"] == hr, (
        f"HRIS is authoritative — decisive headcount must be the workday_hr claim "
        f"{hr}; got {r['decisive_value']}"
    )
    assert r["disclosed"] == [
        {"source_system": "netsuite_finance_rollup", "value": fin}
    ], f"loser must be disclosed as netsuite_finance_rollup {fin}; got {r['disclosed']}"
    assert abs(r["gap_abs"] - abs(hr - fin)) < 0.01, (
        f"gap_abs must be the spread |{hr} - {fin}|; got {r['gap_abs']}"
    )


# ---------------------------------------------------------------------------
# Gate 1 (negative) — no authority match must escalate, never silently pick.
# ---------------------------------------------------------------------------

_ESC_TENANT = str(uuid.uuid5(uuid.NAMESPACE_DNS, "authority-arbitration-escalate-probe"))
_ESC_ENTITY = "EscalateProbe-T1"


def _esc_cleanup():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM conflict_dispositions WHERE tenant_id=%s", (_ESC_TENANT,))
            cur.execute("DELETE FROM conflict_register WHERE tenant_id=%s", (_ESC_TENANT,))
            cur.execute("DELETE FROM semantic_triples WHERE tenant_id=%s", (_ESC_TENANT,))
            cur.execute("DELETE FROM tenant_runs WHERE tenant_id=%s", (_ESC_TENANT,))
            cur.execute("DELETE FROM tenant_authority_map WHERE tenant_id=%s", (_ESC_TENANT,))
            conn.commit()


def test_no_authority_match_escalates_not_silently_picked():
    """Two sources (salesforce, hubspot) disagree on revenue.bookings — a concept
    no authority rule (tenant or '*') covers. Resolution must ESCALATE: no
    decisive value, every claim disclosed. A silent pick here is A1 cheating."""
    _esc_cleanup()
    try:
        run = str(uuid.uuid4())

        def _t(source, pipe, value):
            return {
                "entity_id": _ESC_ENTITY, "concept": "revenue.bookings",
                "property": "total", "value": value, "period": "2026-03",
                "source_system": source, "source_table": "esc_probe",
                "source_field": "total", "pipe_id": pipe,
                "confidence_score": 0.95, "confidence_tier": "exact",
                "fabric_plane": "ipaas", "unit": "usd", "currency": "USD",
            }

        # 1,000,000 vs 1,200,000 — a 20% gap, well over the 0.5% rel default →
        # a material VALUE conflict. salesforce/hubspot are in no authority map.
        resp = client.post(
            "/api/dcl/ingest-triples",
            json={"tenant_id": _ESC_TENANT, "dcl_ingest_id": run,
                  "entity_id": _ESC_ENTITY,
                  "snapshot_name": f"{_ESC_ENTITY}-{run.replace('-', '')[:4]}",
                  "triples": [
                      _t("salesforce", "88888888-8888-4888-8888-888888888881", 1000000),
                      _t("hubspot", "88888888-8888-4888-8888-888888888882", 1200000),
                  ]},
        )
        assert resp.status_code == 201, f"probe ingest failed: {resp.text}"

        resp = client.post(
            "/api/dcl/conflicts/detect",
            json={"tenant_id": _ESC_TENANT, "entity_id": _ESC_ENTITY,
                  "dcl_ingest_id": run},
        )
        assert resp.status_code == 200, resp.text

        resp = client.get(
            "/api/dcl/conflicts",
            params={"tenant_id": _ESC_TENANT, "entity_id": _ESC_ENTITY, "limit": "100"},
        )
        assert resp.status_code == 200, resp.text
        rows = {(c["concept"], c["property"], c.get("period")): c
                for c in resp.json()["conflicts"]}
        c = rows[("revenue.bookings", "total", "2026-03")]

        # It must be a material value conflict (so the surface is in scope).
        assert c["conflict_type"] == "value", (
            f"the 20% gap must register as a value conflict; got {c['conflict_type']}"
        )
        r = c["resolved"]
        assert r["status"] == "escalated", (
            f"no authority rule covers salesforce/hubspot on revenue.bookings — "
            f"resolution must escalate, not pick a winner; got {r}"
        )
        assert r["decisive_value"] is None, (
            f"escalated conflicts have NO decisive value (A1 — no silent pick); "
            f"got {r['decisive_value']}"
        )
        assert r["decisive_source"] is None, r
        # Every claim is disclosed when escalated — nothing hidden.
        disclosed = sorted((d["source_system"], d["value"]) for d in r["disclosed"])
        assert disclosed == [("hubspot", 1200000.0), ("salesforce", 1000000.0)], (
            f"escalated disclosure must list BOTH claims; got {disclosed}"
        )
    finally:
        _esc_cleanup()


# ---------------------------------------------------------------------------
# Gate 2 — as-of returns the point-in-time answer through the browse surface.
# ---------------------------------------------------------------------------

_ASOF_TENANT = str(uuid.uuid5(uuid.NAMESPACE_DNS, "authority-arbitration-asof-probe"))
_ASOF_ENTITY = "AsOfProbe-T1"
_ASOF_CONCEPT = "cloud_spend.summary"
_ASOF_PROP = "total_cost"
_ASOF_PERIOD = "2026-03"


def _asof_cleanup():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM conflict_dispositions WHERE tenant_id=%s", (_ASOF_TENANT,))
            cur.execute("DELETE FROM conflict_register WHERE tenant_id=%s", (_ASOF_TENANT,))
            cur.execute("DELETE FROM semantic_triples WHERE tenant_id=%s", (_ASOF_TENANT,))
            cur.execute("DELETE FROM tenant_runs WHERE tenant_id=%s", (_ASOF_TENANT,))
            conn.commit()


def _db_now():
    """The dev DB's own clock — capture an as-of instant without host/DB skew."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT now()")
            return cur.fetchone()[0]


def _browse_values(*, as_of=None):
    """Read total_cost/2026-03 for the probe entity through the real browse
    endpoint. With as_of → point-in-time; without → current surface."""
    params = {"tenant_id": _ASOF_TENANT, "entity_id": _ASOF_ENTITY,
              "domain": "cloud_spend", "period": _ASOF_PERIOD}
    if as_of is not None:
        params["as_of"] = as_of
    resp = client.get("/api/dcl/triples/browse", params=params)
    assert resp.status_code == 200, resp.text
    return sorted(
        float(t["value"]) for t in resp.json()["triples"]
        if t["concept"] == _ASOF_CONCEPT and t["property"] == _ASOF_PROP
    )


def test_as_of_returns_point_in_time():
    """run1 value 100, then run2 (?replace) value 130 at the SAME coordinate.
    The current surface shows 130; an as-of read BEFORE the supersession still
    returns 100 — proven through the browse endpoint, not just SQL."""
    _asof_cleanup()
    try:
        def _t(source, pipe, value):
            return {
                "entity_id": _ASOF_ENTITY, "concept": _ASOF_CONCEPT,
                "property": _ASOF_PROP, "value": value, "period": _ASOF_PERIOD,
                "source_system": source, "source_table": "asof_probe",
                "source_field": _ASOF_PROP, "pipe_id": pipe,
                "confidence_score": 0.95, "confidence_tier": "exact",
                "fabric_plane": "ipaas", "unit": "usd", "currency": "USD",
            }

        run1, run2 = str(uuid.uuid4()), str(uuid.uuid4())

        resp = client.post(
            "/api/dcl/ingest-triples",
            json={"tenant_id": _ASOF_TENANT, "dcl_ingest_id": run1,
                  "entity_id": _ASOF_ENTITY,
                  "snapshot_name": f"{_ASOF_ENTITY}-{run1.replace('-', '')[:4]}",
                  "triples": [_t("billing", "99999999-9999-4999-8999-999999999991", 100)]},
        )
        assert resp.status_code == 201, f"run1 ingest failed: {resp.text}"
        assert _browse_values() == [100.0], "run1 value 100 must be the current surface"

        # As-of instant AFTER run1 is live, BEFORE run2 supersedes it.
        t_before = _db_now().isoformat()

        resp = client.post(
            "/api/dcl/ingest-triples?replace=true",
            json={"tenant_id": _ASOF_TENANT, "dcl_ingest_id": run2,
                  "entity_id": _ASOF_ENTITY,
                  "snapshot_name": f"{_ASOF_ENTITY}-{run2.replace('-', '')[:4]}",
                  "triples": [_t("billing", "99999999-9999-4999-8999-999999999992", 130)]},
        )
        assert resp.status_code == 201, f"run2 replace ingest failed: {resp.text}"

        # Current surface: ONLY 130 (the 100 row left current).
        assert _browse_values() == [130.0], (
            f"after supersession the current surface must show ONLY 130; got "
            f"{_browse_values()}"
        )
        # As-of BEFORE the supersession: still 100 (point-in-time preserved).
        assert _browse_values(as_of=t_before) == [100.0], (
            f"an as-of read BEFORE the supersession must return 100 (history "
            f"preserved); got {_browse_values(as_of=t_before)}"
        )
        # The two timelines disagree by design — one substrate, two answers.
        assert _browse_values() != _browse_values(as_of=t_before)
    finally:
        _asof_cleanup()


def test_as_of_bad_timestamp_fails_loud():
    """A malformed as_of is rejected with a readable error, not a silent empty
    grid (A1). Paired negative for the as-of surface."""
    resp = client.get(
        "/api/dcl/triples/browse",
        params={"tenant_id": _ASOF_TENANT, "entity_id": _ASOF_ENTITY,
                "as_of": "not-a-timestamp"},
    )
    assert resp.status_code == 400, resp.text
    assert "ISO-8601" in resp.json()["detail"], resp.json()
