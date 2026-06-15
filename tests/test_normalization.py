"""Write-time value normalization layer (currency / unit-scale / date) — mig028.

Operator-visible outcome under test: when two sources report the same metric in
different unit-scales (billing 410194.49 in usd, general_ledger 388.1088 in
usd_thousands), DCL normalizes both to base USD at ingest BEFORE conflict
detection, so the registered conflict's materiality.abs_delta is the REAL gap
22085.69 — not the spurious raw gap 409806.38 it would be if 388.1088 were
compared against 410194.49 unconverted. The general_ledger triple stores 388108.80
with normalization_metadata.raw_value 388.1088 and scale_factor 1000. Two period
spellings of the same month ("2026-03" and "Mar-2026") canonicalize to one group
so the 100-vs-130 disagreement surfaces as ONE conflict with abs_delta 30.0. A EUR
value with a configured rate stores in USD; an unknown unit or a missing FX rate
fails the ingest loudly with the offending token in the message.

Live-service integration test: TestClient drives the real FastAPI app (the real
ingest pipeline → real COPY into aos-dev → real conflict detection) against the
aos-dev database. No faked DB writes (B5). Dedicated test tenant/entity so demo
data is never touched.
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

TEST_TENANT_ID = str(uuid.uuid5(uuid.NAMESPACE_DNS, "normalization-mig028-test"))
ENTITY = "NormProbe-T1"
PIPE_A = "66666666-6666-4666-8666-666666666661"
PIPE_B = "66666666-6666-4666-8666-666666666662"


# ---------------------------------------------------------------------------
# Helpers — real ingest path + direct store reads for stored-value assertions.
# ---------------------------------------------------------------------------

def _triple(source, pipe, value, *, concept="cloud_spend.summary",
            prop="total_cost", period="2026-03", unit="usd", currency="USD"):
    t = {
        "entity_id": ENTITY, "concept": concept, "property": prop,
        "value": value, "period": period, "source_system": source,
        "source_table": "norm_probe", "source_field": prop, "pipe_id": pipe,
        "confidence_score": 0.95, "confidence_tier": "exact",
        "fabric_plane": "ipaas",
    }
    if unit is not None:
        t["unit"] = unit
    if currency is not None:
        t["currency"] = currency
    return t


def _ingest(run_id, triples):
    """POST through the real ingest-triples path. Returns the Response (callers
    assert status — negative tests expect non-2xx)."""
    return client.post(
        "/api/dcl/ingest-triples",
        json={"tenant_id": TEST_TENANT_ID, "dcl_ingest_id": run_id,
              "entity_id": ENTITY,
              "snapshot_name": f"{ENTITY}-{run_id.replace('-', '')[:4]}",
              "triples": triples},
    )


def _ingest_ok(run_id, triples):
    resp = _ingest(run_id, triples)
    assert resp.status_code == 201, f"ingest failed: {resp.status_code} {resp.text}"
    return resp.json()


def _detect(run_id):
    resp = client.post("/api/dcl/conflicts/detect",
                       json={"entity_id": ENTITY, "tenant_id": TEST_TENANT_ID,
                             "dcl_ingest_id": run_id})
    assert resp.status_code == 200, resp.text
    return resp.json()


def _stored(run_id, source_system):
    """Read the stored row (canonical value + normalization_metadata) straight
    from aos-dev for the given run + source — proves what actually persisted."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT value, normalization_metadata, source_system, currency, unit "
                "FROM semantic_triples "
                "WHERE tenant_id=%s AND run_id=%s AND source_system=%s",
                (TEST_TENANT_ID, run_id, source_system),
            )
            rows = cur.fetchall()
    return rows


def _cleanup():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM conflict_dispositions WHERE tenant_id=%s", (TEST_TENANT_ID,))
            cur.execute("DELETE FROM conflict_register WHERE tenant_id=%s", (TEST_TENANT_ID,))
            cur.execute("DELETE FROM semantic_triples WHERE tenant_id=%s", (TEST_TENANT_ID,))
            cur.execute("DELETE FROM tenant_runs WHERE tenant_id=%s", (TEST_TENANT_ID,))
            cur.execute("DELETE FROM tenant_normalization_policy WHERE tenant_id=%s", (TEST_TENANT_ID,))
            conn.commit()


@pytest.fixture(scope="module", autouse=True)
def _module_cleanup():
    _cleanup()
    yield
    _cleanup()


# ---------------------------------------------------------------------------
# Unit-scale: the spurious gap vs the real gap.
# ---------------------------------------------------------------------------

class TestUnitScale:
    def test_unit_scale_wrong_then_right(self):
        run = str(uuid.uuid4())
        # billing reports dollars; general_ledger reports thousands-of-dollars.
        _ingest_ok(run, [
            _triple("billing", PIPE_A, 410194.49, unit="usd"),
            _triple("general_ledger", PIPE_B, 388.10880, unit="usd_thousands"),
        ])

        # General ledger row must be stored in BASE usd (388.1088 * 1000), with
        # the raw original preserved in normalization_metadata.
        gl_rows = _stored(run, "general_ledger")
        assert len(gl_rows) == 1, f"expected one GL row, got {len(gl_rows)}"
        gl_value, gl_meta, _src, gl_currency, gl_unit = gl_rows[0]
        assert float(gl_value) == 388108.80, (
            f"GL must store base-unit value 388108.80, got {gl_value}"
        )
        assert gl_meta is not None, "scaled row must carry normalization_metadata"
        assert float(gl_meta["raw_value"]) == 388.10880, gl_meta
        assert gl_meta["scale_factor"] == 1000, gl_meta
        assert gl_meta["raw_unit"] == "usd_thousands", gl_meta
        assert gl_unit == "usd", f"stored unit must be base 'usd', got {gl_unit!r}"

        # billing was already base usd — no metadata stamp (no-op).
        bill_rows = _stored(run, "billing")
        assert len(bill_rows) == 1
        bill_value, bill_meta, _s, _c, _u = bill_rows[0]
        assert float(bill_value) == 410194.49
        assert bill_meta is None, "already-base row must not get a metadata stamp"

        # The conflict's materiality must be the REAL gap, not the raw gap.
        body = _detect(run)
        value_conflicts = [c for c in body["conflicts"]
                           if c["conflict_type"] == "value"
                           and c["concept"] == "cloud_spend.summary"]
        assert len(value_conflicts) == 1, (
            f"expected one value conflict, got {len(value_conflicts)}: "
            f"{[(c['concept'], c['conflict_type']) for c in body['conflicts']]}"
        )
        abs_delta = value_conflicts[0]["materiality"]["abs_delta"]

        real_gap = round(abs(410194.49 - 388108.80), 2)        # 22085.69
        spurious_gap = round(abs(410194.49 - 388.10880), 2)    # 409806.38
        assert abs(abs_delta - real_gap) < 0.01, (
            f"normalized abs_delta must be the REAL gap {real_gap} "
            f"(billing 410194.49 − GL base 388108.80), got {abs_delta}. "
            f"The spurious raw gap (if 388.1088 were compared unconverted) "
            f"would have been {spurious_gap} — the normalization layer exists "
            f"to turn {spurious_gap} into {real_gap}."
        )
        # And the normalized delta must be materially different from the raw one.
        assert abs(abs_delta - spurious_gap) > 100000, (
            f"normalized delta {abs_delta} must differ materially from the raw "
            f"delta {spurious_gap}; if they were close, normalization did nothing."
        )


# ---------------------------------------------------------------------------
# Period: two spellings of the same month must collapse to one conflict group.
# ---------------------------------------------------------------------------

class TestPeriodCanon:
    def test_period_missed_then_detected(self):
        run = str(uuid.uuid4())
        # Same concept/property; period spelled two ways for the SAME month.
        # Without canonicalization these are two single-source groups and NO
        # conflict is found. Canonicalized, both become "2026-03" → one group.
        _ingest_ok(run, [
            _triple("billing", PIPE_A, 100, period="2026-03"),
            _triple("general_ledger", PIPE_B, 130, period="Mar-2026"),
        ])

        # Both rows must be stored under the canonical period "2026-03".
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT DISTINCT period FROM semantic_triples "
                    "WHERE tenant_id=%s AND run_id=%s AND concept='cloud_spend.summary'",
                    (TEST_TENANT_ID, run),
                )
                periods = sorted(r[0] for r in cur.fetchall())
        assert periods == ["2026-03"], (
            f"both period spellings must canonicalize to '2026-03', got {periods}"
        )

        body = _detect(run)
        value_conflicts = [c for c in body["conflicts"]
                           if c["conflict_type"] == "value"
                           and c["concept"] == "cloud_spend.summary"]
        assert len(value_conflicts) == 1, (
            f"the two spellings must form ONE conflict group, got "
            f"{len(value_conflicts)}"
        )
        assert value_conflicts[0]["period"] == "2026-03"
        abs_delta = value_conflicts[0]["materiality"]["abs_delta"]
        assert abs(abs_delta - 30.0) < 1e-9, (
            f"abs_delta must be 30 (130 − 100) at canonical period 2026-03, "
            f"got {abs_delta}"
        )


# ---------------------------------------------------------------------------
# Currency: EUR → USD at the configured rate, raw preserved.
# ---------------------------------------------------------------------------

class TestCurrency:
    def test_currency_eur_to_usd(self):
        # Configure a per-tenant FX book: 1 EUR = 1.10 USD.
        from backend.db.normalization_policy_store import NormalizationPolicyStore
        NormalizationPolicyStore().put_policy(
            TEST_TENANT_ID, canonical_currency="USD", fx_rates={"EUR": 1.10},
        )

        run = str(uuid.uuid4())
        _ingest_ok(run, [
            _triple("billing", PIPE_A, 100, currency="EUR", unit="usd"),
        ])

        rows = _stored(run, "billing")
        assert len(rows) == 1
        value, meta, _s, currency, _u = rows[0]
        assert float(value) == 110.0, (
            f"100 EUR at 1.10 must store as 110.0 USD, got {value}"
        )
        assert currency == "USD", f"stored currency must be canonical USD, got {currency!r}"
        assert meta is not None, "currency-converted row must carry metadata"
        assert float(meta["fx_rate"]) == 1.10, meta
        assert meta["raw_currency"] == "EUR", meta
        assert float(meta["raw_value"]) == 100, meta


# ---------------------------------------------------------------------------
# Negative: fail loud, no silent pass-through (A1).
# ---------------------------------------------------------------------------

class TestNegatives:
    def test_unknown_unit_fails_loud(self):
        run = str(uuid.uuid4())
        resp = _ingest(run, [
            _triple("billing", PIPE_A, 500.0, unit="furlongs"),
        ])
        assert resp.status_code >= 400 and resp.status_code < 500, (
            f"unknown unit must fail loud (4xx), got {resp.status_code}: {resp.text}"
        )
        msg = resp.text.lower()
        assert "furlongs" in msg, (
            f"the readable error must name the offending unit 'furlongs'; got: {resp.text}"
        )
        assert "unknown unit" in msg or "cannot normalize" in msg, resp.text
        # Nothing must have persisted for this run.
        assert _stored(run, "billing") == [], "failed ingest must not persist rows"

    def test_missing_fx_rate_fails_loud(self):
        # No FX policy configured for THIS run's currency → must refuse.
        run = str(uuid.uuid4())
        # Use a fresh tenant-less currency the default '*' policy cannot convert:
        # ensure this tenant has no GBP rate (the EUR test configured only EUR).
        resp = _ingest(run, [
            _triple("billing", PIPE_A, 100, currency="GBP", unit="usd"),
        ])
        assert resp.status_code >= 400 and resp.status_code < 500, (
            f"missing FX rate must fail loud (4xx), got {resp.status_code}: {resp.text}"
        )
        msg = resp.text
        assert "GBP" in msg, (
            f"the readable error must name the currency 'GBP'; got: {resp.text}"
        )
        assert "FX rate" in msg or "refusing to compare" in msg, resp.text
        assert _stored(run, "billing") == [], "failed ingest must not persist rows"


# ---------------------------------------------------------------------------
# Dimensional base units (days/hours/score/points/seconds/...) — measures, not
# magnitude scales: factor 1.0, unit preserved, no metadata stamp. Regression
# for the Stage-1 pipeline break where the records path's sales.cycle_days
# (unit "days") 422'd at AAM Transport -> DCL because the allowlist only knew
# currency + count/pct/ratio. The fix added the dimensional measures the live
# generators/aggregators stamp; this proves they pass through unchanged.
# ---------------------------------------------------------------------------

class TestDimensionalUnits:
    @pytest.mark.parametrize("unit", [
        "days", "days_outstanding", "hours", "seconds",
        "score", "points", "story_points", "messages_per_second",
    ])
    def test_dimensional_unit_passes_through(self, unit):
        run = str(uuid.uuid4())
        _ingest_ok(run, [
            _triple("salesforce_crm", PIPE_A, 87.0, concept="sales.cycle_days",
                    prop="days", unit=unit, currency=None),
        ])
        rows = _stored(run, "salesforce_crm")
        assert len(rows) == 1, f"unit {unit!r} must ingest exactly one row"
        value, meta, _s, _currency, stored_unit = rows[0]
        assert float(value) == 87.0, (
            f"dimensional unit {unit!r} stores the value unchanged (factor 1.0), got {value}"
        )
        assert stored_unit == unit, (
            f"the stored unit must be preserved as {unit!r}, got {stored_unit!r}"
        )
        assert meta is None, (
            f"a factor-1.0 dimensional unit must NOT stamp normalization_metadata, got {meta!r}"
        )

    def test_currency_scale_alias_still_fails_loud(self):
        # dollars_millions / millions_usd appear ONLY in stale shared-tenant
        # fixture artifacts ($M-scale, flagged broken in the constitution).
        # Adding a 1e6 scale would bless that fixture; refusing is the correct
        # A1 outcome. Guard the deliberate exclusion so a later edit cannot
        # silently scale it (which would corrupt every value 1,000,000x).
        run = str(uuid.uuid4())
        resp = _ingest(run, [
            _triple("sap", PIPE_A, 267.35, concept="customer.pipeline.lead",
                    prop="amount", unit="dollars_millions"),
        ])
        assert 400 <= resp.status_code < 500, (
            f"dollars_millions must still fail loud, got {resp.status_code}: {resp.text}"
        )
        assert "dollars_millions" in resp.text, resp.text
        assert _stored(run, "sap") == [], "failed ingest must not persist rows"


# ---------------------------------------------------------------------------
# Structural namespace markers ({ns}._meta / namespace_type) carry a non-numeric
# catalog value and the "_meta" period SENTINEL by ledger-aggregator protocol.
# They must bypass value normalization (not 422 on the unparseable sentinel) and
# store the sentinel unchanged — domain queries key on it. Regression for the
# second Stage-1 pipeline break (after the dimensional-units one).
# ---------------------------------------------------------------------------

class TestStructuralMarkers:
    def test_meta_marker_bypasses_normalization(self):
        run = str(uuid.uuid4())
        _ingest_ok(run, [
            _triple("netsuite_gl", PIPE_A, "financial_fact", concept="gl._meta",
                    prop="namespace_type", period="_meta", unit=None, currency=None),
        ])
        rows = _stored(run, "netsuite_gl")
        assert len(rows) == 1, "the _meta marker must ingest exactly one row"
        value, meta, _s, _currency, _unit = rows[0]
        assert str(value).strip('"') == "financial_fact", (
            f"marker's non-numeric value must store unchanged, got {value!r}"
        )
        assert meta is None, f"marker must not stamp normalization_metadata, got {meta!r}"
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT period FROM semantic_triples "
                    "WHERE tenant_id=%s AND run_id=%s AND concept=%s",
                    (TEST_TENANT_ID, run, "gl._meta"),
                )
                period = cur.fetchone()[0]
        assert period == "_meta", (
            f"the _meta period sentinel must be preserved (not canonicalized/nulled), got {period!r}"
        )

    def test_real_metric_bad_period_still_fails_loud(self):
        # The marker bypass must NOT weaken the guarantee for real metrics: a
        # genuine (non-marker) concept with an unparseable period still 422s, so
        # an unparsed period that should match another source can't silently
        # split a conflict group.
        run = str(uuid.uuid4())
        resp = _ingest(run, [
            _triple("netsuite", PIPE_A, 100.0, concept="revenue.total",
                    prop="amount", period="not-a-period"),
        ])
        assert 400 <= resp.status_code < 500, (
            f"a real metric with an unparseable period must fail loud, got {resp.status_code}: {resp.text}"
        )
        assert "unparseable period" in resp.text or "cannot canonicalize" in resp.text, resp.text
        assert _stored(run, "netsuite") == [], "failed ingest must not persist rows"


# ---------------------------------------------------------------------------
# Deferred #80: source_system casing split must collapse to one source.
# ---------------------------------------------------------------------------

class TestSourceSystem80:
    def test_source_system_80_canonical(self):
        run = str(uuid.uuid4())
        # The SAME physical source spelled two ways for the SAME coordinate.
        # normalize_source_id collapses "NetSuite" and "netsuite" to "netsuite",
        # so detection sees ONE source — no spurious conflict from a casing split.
        _ingest_ok(run, [
            _triple("NetSuite", PIPE_A, 100),
            _triple("netsuite", PIPE_B, 100),
        ])

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT DISTINCT source_system FROM semantic_triples "
                    "WHERE tenant_id=%s AND run_id=%s AND concept='cloud_spend.summary'",
                    (TEST_TENANT_ID, run),
                )
                sources = sorted(r[0] for r in cur.fetchall())
        assert sources == ["netsuite"], (
            f"both casings must store as the one canonical source 'netsuite', "
            f"got {sources}"
        )

        # Detection must see ONE source for the coordinate — so NO conflict
        # (value or structural) is registered for cloud_spend.summary.
        body = _detect(run)
        cs_conflicts = [c for c in body["conflicts"]
                        if c["concept"] == "cloud_spend.summary"]
        assert cs_conflicts == [], (
            f"one canonical source must yield no conflict (no casing split), "
            f"got {len(cs_conflicts)}: {cs_conflicts}"
        )
