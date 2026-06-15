"""Acceptance suite for the "what's driving attrition" cross-source ingest
(ContextOS demo question a) on the real fabric-connect path:
POST /api/dcl/ingest-records maps the new HR exit-theme + comp-band fields and the
external market-benchmark feed into canonical concept triples, inbound.

Operator-visible outcome under test: when AAM transports a workday_hr workforce
pipe carrying the trailing-window exit cohort (engineering: compensation 8,
growth 2, management 1, work_life 1) and the internal comp band (engineering
$165,000), and SEPARATELY a radford_comp market pipe carrying the external market
median (software_engineering $190,000), DCL forms:
  - workforce.exit_theme.compensation.by_department, property=engineering, value 8
  - comp_band.median.by_department, property=engineering, value 165000,
    source_system=workday_hr
  - market_benchmark.median.by_job_family, property=software_engineering,
    value 190000, source_system=radford_comp
so the comp gap (internal 165k vs EXTERNAL market 190k, ~13% below market) and the
compensation-dominated engineering exits both resolve from real triples — the
cross-source beat (internal comp band vs external market median, two source
systems) the demo turns on.

These are live-service integration tests: TestClient drives the real FastAPI app
against the aos-dev database (the same path AAM's transport will hit over HTTP).
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
from backend.api.routes.ingest_triples import get_run_triples, delete_tenant_triples
from backend.core.db import get_connection

client = TestClient(app, raise_server_exceptions=False)

TEST_TENANT_ID = str(uuid.uuid5(uuid.NAMESPACE_DNS, "attrition-drivers-ingest-test"))
ENTITY = "AttritionDrive-TEST"

HR_PIPE = "aaaaaaaa-0000-0000-0000-0000000000a1"
MARKET_PIPE = "bbbbbbbb-0000-0000-0000-0000000000b1"

# ── The transported SOURCE records (the "fed" values) ────────────────────────
# These are exactly the flat source fields Farm serves and AAM transports — the
# workday_hr workforce feed (exit-theme cohort + internal comp band) and the
# radford_comp external market survey. Built from the locked demo calibration so
# the assertions check what the operator will see, not whatever the system emits.
# (B6: no cross-repo import of Farm's generator — the source shape is the contract.)
DEPTS = ("engineering", "sales", "customer_success", "g&a")

EXIT_COMPENSATION = {"engineering": 8, "sales": 1, "customer_success": 0, "g&a": 0}
EXIT_GROWTH = {"engineering": 2, "sales": 2, "customer_success": 1, "g&a": 1}
EXIT_MANAGEMENT = {"engineering": 1, "sales": 2, "customer_success": 2, "g&a": 1}
EXIT_WORKLIFE = {"engineering": 1, "sales": 1, "customer_success": 1, "g&a": 1}
COMP_BAND_MEDIAN = {"engineering": 165000, "sales": 145000,
                    "customer_success": 110000, "g&a": 130000}
MARKET_MEDIAN_BY_FAMILY = {"software_engineering": 190000, "sales": 150000,
                           "customer_success": 112000, "general_admin": 128000}

PERIOD = "2026-03"  # the demo headline month


def _new_run_id():
    return str(uuid.uuid4())


def _hr_pipe():
    """workday_hr workforce pipe (operations domain) carrying the new HR fields.
    No identity_key — domainless-of-identity operational metrics, so the
    operational records aggregator forms the concepts (record_converter routes
    domain==operations there). Headcount carried too, as the real feed does."""
    return {
        "pipe_id": HR_PIPE,
        "source_system": "workday_hr",
        "fabric_plane": "hcm",
        "fabric_product": "workday",
        "domain": "operations",
        "record_key_field": "period",
        "records": [{
            "period": PERIOD,
            "headcount_total": 210,
            "headcount_by_department": {"engineering": 96, "sales": 58,
                                        "customer_success": 34, "g&a": 22},
            "terminations": 4,
            "exit_compensation_by_department": dict(EXIT_COMPENSATION),
            "exit_growth_by_department": dict(EXIT_GROWTH),
            "exit_management_by_department": dict(EXIT_MANAGEMENT),
            "exit_worklife_by_department": dict(EXIT_WORKLIFE),
            "comp_band_median_by_department": dict(COMP_BAND_MEDIAN),
        }],
    }


def _market_pipe():
    """radford_comp external market-benchmark pipe (operations domain) — a
    SEPARATE source system from HR. market_median_by_job_family is keyed by job
    family (the survey's unit), not department."""
    return {
        "pipe_id": MARKET_PIPE,
        "source_system": "radford_comp",
        "fabric_plane": "hcm",
        "fabric_product": "radford",
        "domain": "operations",
        "record_key_field": "period",
        "records": [{
            "period": PERIOD,
            "market_median_by_job_family": dict(MARKET_MEDIAN_BY_FAMILY),
        }],
    }


def _post_records(pipes, *, run_id=None, replace=True, entity_id=ENTITY):
    run_id = run_id or _new_run_id()
    body = {
        "tenant_id": TEST_TENANT_ID,
        "dcl_ingest_id": run_id,
        "entity_id": entity_id,
        "run_mode": "Dev",
        "pipes": pipes,
    }
    resp = client.post(f"/api/dcl/ingest-records?replace={str(replace).lower()}", json=body)
    return run_id, resp


def _triples_for_run(run_id):
    return get_run_triples(TEST_TENANT_ID, run_id)


def _period_currency(run_id, concept, prop):
    """get_run_triples omits period/currency; read them directly for the one
    triple so the stamping is asserted (whitelisted read, scoped to this run)."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT period, currency FROM semantic_triples "
                "WHERE tenant_id=%s AND run_id=%s AND concept=%s AND property=%s",
                (TEST_TENANT_ID, run_id, concept, prop),
            )
            rows = cur.fetchall()
    assert len(rows) == 1, f"expected one {concept}/{prop}, got {rows}"
    return {"period": rows[0][0], "currency": rows[0][1]}


def _one(triples, concept, prop):
    """Exactly one triple for (concept, property) — fail loud otherwise."""
    hits = [t for t in triples if t["concept"] == concept and t["property"] == prop]
    assert len(hits) == 1, f"expected one {concept} property={prop}, got {hits}"
    return hits[0]


def _cleanup():
    delete_tenant_triples(TEST_TENANT_ID)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM tenant_runs WHERE tenant_id = %s", (TEST_TENANT_ID,))
            conn.commit()


@pytest.fixture(autouse=True)
def cleanup_around_each():
    _cleanup()
    yield
    _cleanup()


# ---------------------------------------------------------------------------
# Headline: exit themes + comp band + external market all resolve as triples
# ---------------------------------------------------------------------------

def test_exit_themes_comp_band_and_market_resolve_in_dcl():
    run_id, resp = _post_records([_hr_pipe(), _market_pipe()])
    assert resp.status_code == 201, resp.text
    triples = _triples_for_run(run_id)
    assert triples, "no triples written"

    # --- Exit theme: engineering compensation cohort == fed (8). ---
    t = _one(triples, "workforce.exit_theme.compensation.by_department", "engineering")
    assert float(t["value"]) == float(EXIT_COMPENSATION["engineering"]), t
    assert t["source_system"] == "workday_hr", t
    assert _period_currency(run_id, "workforce.exit_theme.compensation.by_department",
                            "engineering")["period"] == PERIOD

    # All four exit reasons formed their own concept, property=engineering == fed.
    for reason, fed in (("compensation", EXIT_COMPENSATION), ("growth", EXIT_GROWTH),
                        ("management", EXIT_MANAGEMENT), ("work_life", EXIT_WORKLIFE)):
        tr = _one(triples, f"workforce.exit_theme.{reason}.by_department", "engineering")
        assert float(tr["value"]) == float(fed["engineering"]), (reason, tr)

    # Engineering exits are compensation-dominated (>=60% of its cohort) — the
    # causal-coherence half, proven off the ingested triples (not Farm GT).
    eng_cohort = {
        r: float(_one(triples, f"workforce.exit_theme.{r}.by_department", "engineering")["value"])
        for r in ("compensation", "growth", "management", "work_life")
    }
    total = sum(eng_cohort.values())
    assert eng_cohort["compensation"] / total >= 0.60, eng_cohort

    # --- Internal comp band: engineering median == fed (165000), from workday_hr. ---
    cb = _one(triples, "comp_band.median.by_department", "engineering")
    assert float(cb["value"]) == float(COMP_BAND_MEDIAN["engineering"]), cb
    assert cb["source_system"] == "workday_hr", cb
    assert _period_currency(run_id, "comp_band.median.by_department",
                            "engineering")["currency"] == "USD"  # usd breakdown carries currency

    # --- External market median: software_engineering == fed (190000), from
    # radford_comp (the cross-source beat — a DIFFERENT source system). ---
    mk = _one(triples, "market_benchmark.median.by_job_family", "software_engineering")
    assert float(mk["value"]) == float(MARKET_MEDIAN_BY_FAMILY["software_engineering"]), mk
    assert mk["source_system"] == "radford_comp", mk
    assert _period_currency(run_id, "market_benchmark.median.by_job_family",
                            "software_engineering")["currency"] == "USD"

    # --- The comp gap resolves cross-source: internal 165k (workday_hr) vs
    # external 190k (radford_comp) → engineering ~13% below market. ---
    internal = float(cb["value"])
    external = float(mk["value"])
    gap_pct = (external - internal) / external * 100
    assert external > internal, (internal, external)
    assert 10.0 <= gap_pct <= 16.0, f"engineering gap {gap_pct:.1f}% off the ~13% target"
    assert cb["source_system"] != mk["source_system"], "comp gap must be cross-source"


def test_engineering_is_the_only_dept_with_gap_and_comp_dominated_exits():
    """Causal coherence end to end on ingested triples: engineering uniquely has
    BOTH the biggest comp gap AND compensation-dominated exits — no other dept
    does both. (Reads market by resolving job_family -> department.)"""
    run_id, resp = _post_records([_hr_pipe(), _market_pipe()])
    assert resp.status_code == 201, resp.text
    triples = _triples_for_run(run_id)

    fam_for_dept = {"engineering": "software_engineering", "sales": "sales",
                    "customer_success": "customer_success", "g&a": "general_admin"}

    gap_pct = {}
    comp_dominated = {}
    for dept in DEPTS:
        internal = float(_one(triples, "comp_band.median.by_department", dept)["value"])
        external = float(_one(triples, "market_benchmark.median.by_job_family",
                              fam_for_dept[dept])["value"])
        gap_pct[dept] = (external - internal) / external * 100
        cohort = {
            r: float(_one(triples, f"workforce.exit_theme.{r}.by_department", dept)["value"])
            for r in ("compensation", "growth", "management", "work_life")
        }
        tot = sum(cohort.values())
        dominant = max(cohort, key=lambda r: cohort[r])
        comp_dominated[dept] = dominant == "compensation" and (cohort["compensation"] / tot) >= 0.60

    # Engineering is the single biggest gap.
    assert max(gap_pct, key=lambda d: gap_pct[d]) == "engineering", gap_pct
    # g&a is at/above market (no gap) — gap_pct <= 0.
    assert gap_pct["g&a"] <= 0.0, gap_pct
    # Engineering is the ONLY comp-dominated-exit department.
    assert [d for d in DEPTS if comp_dominated[d]] == ["engineering"], comp_dominated


def test_every_new_triple_carries_full_provenance():
    run_id, resp = _post_records([_hr_pipe(), _market_pipe()])
    assert resp.status_code == 201, resp.text
    new_concepts = {
        "workforce.exit_theme.compensation.by_department",
        "workforce.exit_theme.growth.by_department",
        "workforce.exit_theme.management.by_department",
        "workforce.exit_theme.work_life.by_department",
        "comp_band.median.by_department",
        "market_benchmark.median.by_job_family",
    }
    new_triples = [t for t in _triples_for_run(run_id) if t["concept"] in new_concepts]
    assert new_triples, "none of the new concepts were written"
    for t in new_triples:
        assert t["source_system"] in ("workday_hr", "radford_comp"), t
        assert t["source_field"], t
        assert t["pipe_id"], t
        assert t["fabric_plane"] == "hcm", t
        assert t["property"], t            # the dept / job_family member
        assert t["confidence_score"] is not None, t
        assert t["canonical_id"] is None, t  # operational metrics: no identity resolution


def test_ingest_is_idempotent_on_replace():
    """B14/idempotency: re-ingesting the same records under the same run id with
    replace=true does not drift triple counts or values."""
    run_id, resp = _post_records([_hr_pipe(), _market_pipe()])
    assert resp.status_code == 201, resp.text
    first = _triples_for_run(run_id)
    first_eng = float(_one(first, "comp_band.median.by_department", "engineering")["value"])

    _, resp2 = _post_records([_hr_pipe(), _market_pipe()], run_id=run_id, replace=True)
    assert resp2.status_code == 201, resp2.text
    second = _triples_for_run(run_id)

    assert len(second) == len(first), f"triple count drifted: {len(first)} -> {len(second)}"
    assert float(_one(second, "comp_band.median.by_department", "engineering")["value"]) == first_eng


# ---------------------------------------------------------------------------
# Negative: an unmapped exit-theme-shaped field is dropped LOUDLY, not silently
# ---------------------------------------------------------------------------

def test_unmapped_field_warns_loud_not_silent():
    """A1/B4: a field with no concept mapping (a dict breakdown the aggregator
    does not know) is reported in warnings, never silently swallowed."""
    pipe = _hr_pipe()
    pipe["records"][0]["exit_relocation_by_department"] = {"engineering": 3}  # not in the map
    run_id, resp = _post_records([pipe, _market_pipe()])
    assert resp.status_code == 201, resp.text
    warnings = resp.json()["warnings"]
    assert any(w.get("field") == "exit_relocation_by_department" for w in warnings), warnings
    # And it produced no triple (it was dropped, loudly).
    assert not [t for t in _triples_for_run(run_id)
                if "exit_relocation" in t["concept"]], "unmapped field silently formed a concept"
