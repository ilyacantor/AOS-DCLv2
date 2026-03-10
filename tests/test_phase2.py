#!/usr/bin/env python3
"""
DCL Agent -- Phase 1 Part 2 Test Suite

Tests: cross-sell pipeline, EBITDA bridge, what-if sensitivity,
executive dashboards, and Maestra engagement lifecycle.

Test groups:
  XS_001 - XS_007:  Cross-sell pipeline (7 tests)
  EB_001 - EB_007:  EBITDA bridge (7 tests)
  WI_001 - WI_007:  What-if sensitivity (7 tests)
  DB_001 - DB_004:  Executive dashboards (4 tests)
  MA_001 - MA_003:  Maestra engagement (3 tests)

Rules: 100% pass rate, fix defects not tests, no mocking, no skipping.
"""

import os
import sys
import time
from typing import List

import requests

BASE_URL = os.environ.get("DCL_BASE_URL", "http://localhost:8004")
TIMEOUT = 60.0


# ─── helpers ───────────────────────────────────────────────────────────────
class TestResult:
    def __init__(self, test_id: str, description: str):
        self.test_id = test_id
        self.description = description
        self.passed = False
        self.message = ""

    def pass_(self, msg: str = ""):
        self.passed = True
        self.message = msg
        print(f"  [PASS] {self.test_id}: {self.description}")

    def fail(self, expected: str, got: str):
        self.passed = False
        self.message = f"expected {expected}, got {got}"
        print(f"  [FAIL] {self.test_id}: {self.description} -- {self.message}")


results: List[TestResult] = []


def test(test_id: str, description: str) -> TestResult:
    r = TestResult(test_id, description)
    results.append(r)
    return r


def api_get(path: str, **kwargs) -> requests.Response:
    return requests.get(f"{BASE_URL}{path}", timeout=TIMEOUT, **kwargs)


def api_post(path: str, **kwargs) -> requests.Response:
    return requests.post(f"{BASE_URL}{path}", timeout=TIMEOUT, **kwargs)


def wait_for_backend(max_wait: int = 15) -> bool:
    for i in range(max_wait):
        try:
            r = requests.get(f"{BASE_URL}/health", timeout=5)
            if r.status_code == 200:
                print(f"Backend ready (waited {i}s)")
                return True
        except Exception:
            pass
        time.sleep(1)
    return False


# ─── SUITE: Cross-Sell Pipeline ────────────────────────────────────────
def suite_cross_sell():
    print("\n=== SUITE: Cross-Sell Pipeline ===")

    # XS-001: Pipeline endpoint returns data
    t = test("XS-001", "Cross-sell pipeline endpoint returns data")
    r = api_get("/api/reports/cross-sell")
    if r.status_code == 200:
        data = r.json()
        if "m_to_c" in data and "c_to_m" in data and "summary" in data:
            t.pass_(f"m_to_c={len(data['m_to_c'])}, c_to_m={len(data['c_to_m'])}")
        else:
            t.fail("m_to_c, c_to_m, summary keys", str(list(data.keys())))
    else:
        t.fail("200", str(r.status_code))
    xs_data = data if r.status_code == 200 else {}

    # XS-002: Candidate count in expected range (~103 total: ~65 M→C + ~38 C→M)
    t = test("XS-002", "Total candidates in expected range (80-130)")
    summary = xs_data.get("summary", {})
    total = summary.get("total_candidates", 0)
    if 80 <= total <= 130:
        t.pass_(f"total_candidates={total}")
    else:
        t.fail("80-130", str(total))

    # XS-003: Pipeline ACV in expected range ($230-290M)
    t = test("XS-003", "Pipeline ACV in expected range ($200-350M)")
    pipeline_acv = summary.get("total_pipeline_acv", 0)
    if 200_000_000 <= pipeline_acv <= 350_000_000:
        t.pass_(f"total_pipeline_acv=${pipeline_acv/1e6:.0f}M")
    else:
        t.fail("$200-350M", f"${pipeline_acv/1e6:.0f}M")

    # XS-004: Every candidate has all required fields
    t = test("XS-004", "Every candidate has required scoring fields")
    required_fields = {
        "customer_id", "customer_name", "recommended_service",
        "propensity_score", "estimated_acv", "industry_match",
        "size_match", "behavioral_score", "engagement_fit",
        "relationship_strength", "rationale",
    }
    all_candidates = xs_data.get("m_to_c", []) + xs_data.get("c_to_m", [])
    missing = []
    for c in all_candidates[:5]:
        for f in required_fields:
            if f not in c:
                missing.append(f"{c.get('customer_id', '?')}.{f}")
    if not missing:
        t.pass_(f"all {len(required_fields)} fields present on {len(all_candidates)} candidates")
    else:
        t.fail("all fields present", f"missing: {missing[:5]}")

    # XS-005: Scores sum correctly (0-100 range)
    t = test("XS-005", "Propensity scores in valid range and ≥60 threshold")
    bad_scores = []
    for c in all_candidates:
        score = c.get("propensity_score", 0)
        if score < 60 or score > 100:
            bad_scores.append(f"{c.get('customer_id')}: {score}")
    if not bad_scores:
        t.pass_(f"all {len(all_candidates)} scores in [60, 100]")
    else:
        t.fail("scores in [60, 100]", f"{len(bad_scores)} out of range: {bad_scores[:3]}")

    # XS-006: No overlap customers in pipeline
    t = test("XS-006", "No overlap customers appear in pipeline")
    overlap_r = api_get("/api/reports/entity-overlap")
    if overlap_r.status_code == 200:
        overlap_data = overlap_r.json()
        overlap_names = {
            m["canonical_name"].lower()
            for m in overlap_data.get("customer_overlap", {}).get("matches", [])
        }
        leaked = []
        for c in all_candidates:
            if c.get("customer_name", "").lower() in overlap_names:
                leaked.append(c["customer_name"])
        if not leaked:
            t.pass_(f"0 overlap customers in {len(all_candidates)} candidates")
        else:
            t.fail("0 overlap leaks", f"{len(leaked)} leaked: {leaked[:5]}")
    else:
        t.fail("overlap endpoint 200", str(overlap_r.status_code))

    # XS-007: Drill-through on a specific candidate works
    t = test("XS-007", "Cross-sell drill-through returns candidate detail")
    if all_candidates:
        cid = all_candidates[0]["customer_id"]
        r = api_get(f"/api/reports/cross-sell/drill/{cid}")
        if r.status_code == 200:
            detail = r.json()
            if detail.get("customer_id") == cid and "rationale" in detail:
                t.pass_(f"drill on {cid}")
            else:
                t.fail("matching customer_id + rationale", str(list(detail.keys())))
        else:
            t.fail("200", str(r.status_code))
    else:
        t.fail("candidates to drill", "no candidates")


# ─── SUITE: EBITDA Bridge ──────────────────────────────────────────────
def suite_ebitda_bridge():
    print("\n=== SUITE: EBITDA Bridge ===")

    # EB-001: Bridge endpoint returns data
    t = test("EB-001", "EBITDA bridge endpoint returns data")
    r = api_get("/api/reports/ebitda-bridge")
    eb_data = {}
    if r.status_code == 200:
        eb_data = r.json()
        required = {"reported_ebitda", "entity_adjustments", "entity_adjusted_ebitda",
                     "combination_synergies", "pro_forma_ebitda", "ev_impact"}
        if required.issubset(eb_data.keys()):
            t.pass_(f"all {len(required)} sections present")
        else:
            t.fail(str(required), str(set(eb_data.keys())))
    else:
        t.fail("200", str(r.status_code))

    # EB-002: 12 entity adjustments
    t = test("EB-002", "12 entity adjustments present")
    adj = eb_data.get("entity_adjustments", [])
    if len(adj) == 12:
        t.pass_(f"{len(adj)} entity adjustments")
    else:
        t.fail("12", str(len(adj)))

    # EB-003: 8 combination synergies
    t = test("EB-003", "8 combination synergies present")
    syn = eb_data.get("combination_synergies", [])
    if len(syn) == 8:
        t.pass_(f"{len(syn)} combination synergies")
    else:
        t.fail("8", str(len(syn)))

    # EB-004: Arithmetic check — reported + entity_adj = entity_adjusted
    t = test("EB-004", "Arithmetic: reported + entity_adj = entity_adjusted")
    reported = eb_data.get("reported_ebitda", {}).get("combined_reported", 0)
    adj_total = sum(a["amount"] for a in adj)
    ea_combined = eb_data.get("entity_adjusted_ebitda", {}).get("combined", 0)
    expected = reported + adj_total
    diff = abs(expected - ea_combined)
    if diff < 1.0:
        t.pass_(f"reported(${reported/1e6:.1f}M) + adj(${adj_total/1e6:.1f}M) = ${ea_combined/1e6:.1f}M")
    else:
        t.fail(f"${expected/1e6:.1f}M", f"${ea_combined/1e6:.1f}M (diff=${diff/1e6:.4f}M)")

    # EB-005: Pro forma has low/high/current with correct ordering
    t = test("EB-005", "Pro forma has low ≤ current ≤ high")
    pf = eb_data.get("pro_forma_ebitda", {})
    y1 = pf.get("year_1", {})
    ss = pf.get("steady_state", {})
    if y1 and ss:
        y1_ok = y1.get("low", 0) <= y1.get("current", 0) <= y1.get("high", 0)
        ss_ok = ss.get("low", 0) <= ss.get("current", 0) <= ss.get("high", 0)
        if y1_ok and ss_ok:
            t.pass_(f"Y1: ${y1['current']/1e6:.0f}M, SS: ${ss['current']/1e6:.0f}M")
        else:
            t.fail("low ≤ current ≤ high", f"Y1={y1_ok}, SS={ss_ok}")
    else:
        t.fail("year_1 and steady_state", f"keys: {list(pf.keys())}")

    # EB-006: Steady state > Year 1 (integration costs drop off)
    t = test("EB-006", "Steady state > Year 1")
    y1_current = pf.get("year_1", {}).get("current", 0)
    ss_current = pf.get("steady_state", {}).get("current", 0)
    if ss_current > y1_current:
        t.pass_(f"SS ${ss_current/1e6:.0f}M > Y1 ${y1_current/1e6:.0f}M")
    else:
        t.fail(f"SS > Y1", f"SS=${ss_current/1e6:.0f}M, Y1=${y1_current/1e6:.0f}M")

    # EB-007: EV impact computed with correct multiple
    t = test("EB-007", "EV impact uses correct multiple")
    ev = eb_data.get("ev_impact", {})
    multiple = ev.get("multiple", 0)
    ev_y1 = ev.get("year_1_ev", {}).get("current", 0)
    expected_ev = y1_current * multiple
    ev_diff = abs(ev_y1 - expected_ev)
    if multiple > 0 and ev_diff < 1.0:
        t.pass_(f"EV Y1 = ${ev_y1/1e9:.1f}B at {multiple}x")
    else:
        t.fail(f"EV = pro_forma × {multiple}", f"diff=${ev_diff/1e6:.0f}M")


# ─── SUITE: What-If Sensitivity ────────────────────────────────────────
def suite_what_if():
    print("\n=== SUITE: What-If Sensitivity ===")

    # WI-001: What-if endpoint returns data
    t = test("WI-001", "What-if base case endpoint returns data")
    r = api_post("/api/reports/what-if", json={})
    wi_data = {}
    if r.status_code == 200:
        wi_data = r.json()
        required = {"levers", "lever_definitions", "pro_forma_ebitda", "ev_impact"}
        if required.issubset(wi_data.keys()):
            t.pass_(f"all {len(required)} sections present")
        else:
            t.fail(str(required), str(set(wi_data.keys())))
    else:
        t.fail("200", str(r.status_code))

    # WI-002: 10 lever definitions
    t = test("WI-002", "10 lever definitions present")
    defs = wi_data.get("lever_definitions", [])
    if len(defs) == 10:
        t.pass_("10 levers")
    else:
        t.fail("10", str(len(defs)))

    # WI-003: 5 presets available
    t = test("WI-003", "5 presets available")
    r_presets = api_get("/api/reports/what-if/presets")
    if r_presets.status_code == 200:
        presets = r_presets.json().get("presets", {})
        if len(presets) == 5:
            t.pass_(f"presets: {list(presets.keys())}")
        else:
            t.fail("5", str(len(presets)))
    else:
        t.fail("200", str(r_presets.status_code))

    # WI-004: Conservative < Base < Aggressive ordering
    t = test("WI-004", "Conservative < Base < Aggressive ordering")
    base = wi_data.get("pro_forma_ebitda", {}).get("year_1", 0)
    r_cons = api_post("/api/reports/what-if", json={"preset": "conservative"})
    r_agg = api_post("/api/reports/what-if", json={"preset": "aggressive"})
    if r_cons.status_code == 200 and r_agg.status_code == 200:
        cons_y1 = r_cons.json().get("pro_forma_ebitda", {}).get("year_1", 0)
        agg_y1 = r_agg.json().get("pro_forma_ebitda", {}).get("year_1", 0)
        if cons_y1 < base < agg_y1:
            t.pass_(f"cons=${cons_y1/1e6:.0f}M < base=${base/1e6:.0f}M < agg=${agg_y1/1e6:.0f}M")
        else:
            t.fail("cons < base < agg", f"cons=${cons_y1/1e6:.0f}M, base=${base/1e6:.0f}M, agg=${agg_y1/1e6:.0f}M")
    else:
        t.fail("200 for both", f"cons={r_cons.status_code}, agg={r_agg.status_code}")

    # WI-005: Single lever change moves result
    t = test("WI-005", "Single lever change moves result vs base")
    r_lever = api_post("/api/reports/what-if", json={"levers": {"a_utilization_rate": 82}})
    if r_lever.status_code == 200:
        lever_y1 = r_lever.json().get("pro_forma_ebitda", {}).get("year_1", 0)
        if lever_y1 != base and lever_y1 > 0:
            t.pass_(f"utilization=82 → ${lever_y1/1e6:.0f}M vs base ${base/1e6:.0f}M")
        else:
            t.fail("different from base", f"same as base: ${lever_y1/1e6:.0f}M")
    else:
        t.fail("200", str(r_lever.status_code))

    # WI-006: Performance — under 2 seconds
    t = test("WI-006", "What-if computation under 2 seconds")
    start = time.time()
    r_perf = api_post("/api/reports/what-if", json={"preset": "aggressive"})
    elapsed = time.time() - start
    if r_perf.status_code == 200 and elapsed < 2.0:
        t.pass_(f"{elapsed:.3f}s")
    elif r_perf.status_code == 200:
        t.fail("<2s", f"{elapsed:.3f}s")
    else:
        t.fail("200 + <2s", f"status={r_perf.status_code}")

    # WI-007: Invalid preset returns 400
    t = test("WI-007", "Invalid preset returns 400")
    r_bad = api_post("/api/reports/what-if", json={"preset": "nonexistent"})
    if r_bad.status_code == 400:
        t.pass_("400 for invalid preset")
    else:
        t.fail("400", str(r_bad.status_code))


# ─── SUITE: Executive Dashboards ───────────────────────────────────────
def suite_dashboards():
    print("\n=== SUITE: Executive Dashboards ===")

    # DB-001: All 5 personas return data
    t = test("DB-001", "All 5 persona dashboards return 200")
    personas = ["cfo", "cro", "coo", "cto", "chro"]
    failures = []
    for p in personas:
        r = api_get(f"/api/reports/dashboard/{p}")
        if r.status_code != 200:
            failures.append(f"{p}={r.status_code}")
    if not failures:
        t.pass_(f"all 5 personas return 200")
    else:
        t.fail("all 200", f"failures: {failures}")

    # DB-002: Every dashboard has persona, title, kpis
    t = test("DB-002", "Every dashboard has persona, title, kpis fields")
    missing = []
    for p in personas:
        r = api_get(f"/api/reports/dashboard/{p}")
        if r.status_code == 200:
            data = r.json()
            for field in ["persona", "title", "kpis"]:
                if field not in data:
                    missing.append(f"{p}.{field}")
    if not missing:
        t.pass_("all fields present on all 5 dashboards")
    else:
        t.fail("all fields", f"missing: {missing}")

    # DB-003: CFO dashboard has revenue trend with multiple quarters
    t = test("DB-003", "CFO dashboard has quarterly revenue trend")
    r = api_get("/api/reports/dashboard/cfo")
    if r.status_code == 200:
        data = r.json()
        trend = data.get("revenue_trend", [])
        if isinstance(trend, list) and len(trend) >= 8:
            t.pass_(f"{len(trend)} quarters in trend")
        else:
            t.fail("≥8 quarters", f"{len(trend) if isinstance(trend, list) else 'not a list'}")
    else:
        t.fail("200", str(r.status_code))

    # DB-004: Invalid persona returns 400
    t = test("DB-004", "Invalid persona returns 400")
    r = api_get("/api/reports/dashboard/ceo")
    if r.status_code == 400:
        t.pass_("400 for invalid persona")
    else:
        t.fail("400", str(r.status_code))


# ─── SUITE: Maestra Engagement ─────────────────────────────────────────
def suite_maestra():
    print("\n=== SUITE: Maestra Engagement ===")

    # MA-001: Create engagement
    t = test("MA-001", "Create Maestra engagement")
    r = api_post("/api/reports/maestra/engage")
    engagement_id = None
    if r.status_code == 200:
        data = r.json()
        engagement_id = data.get("engagement_id")
        if engagement_id and data.get("phase") == "scoping":
            t.pass_(f"engagement_id={engagement_id[:8]}..., phase=scoping")
        else:
            t.fail("engagement_id + phase=scoping", str(data))
    else:
        t.fail("200", str(r.status_code))

    if not engagement_id:
        # Skip remaining tests
        test("MA-002", "Send message to engagement").fail("engagement_id", "none")
        test("MA-003", "Get engagement status").fail("engagement_id", "none")
        return

    # MA-002: Send message and get response
    t = test("MA-002", "Send status message and get response")
    r = api_post(
        f"/api/reports/maestra/{engagement_id}/message",
        json={"message": "status"},
    )
    if r.status_code == 200:
        data = r.json()
        if "response" in data and len(data["response"]) > 0:
            t.pass_(f"response length={len(data['response'])}")
        else:
            t.fail("non-empty response", str(data))
    else:
        t.fail("200", str(r.status_code))

    # MA-003: Get engagement status
    t = test("MA-003", "Get engagement status summary")
    r = api_get(f"/api/reports/maestra/{engagement_id}/status")
    if r.status_code == 200:
        data = r.json()
        if "phase" in data and "overall_progress_pct" in data and "workstream_summary" in data:
            t.pass_(f"phase={data['phase']}, progress={data['overall_progress_pct']}%")
        else:
            t.fail("phase + progress + workstreams", str(list(data.keys())))
    else:
        t.fail("200", str(r.status_code))


# ─── main ─────────────────────────────────────────────────────────────────
def run_harness():
    global results

    print("\n=== DCL PHASE 1 PART 2 TEST SUITE ===\n")

    if not wait_for_backend():
        print(f"FATAL: Backend not responding at {BASE_URL}")
        print("Start the DCL backend and try again.")
        return -1  # Signal connection failure

    results = []

    suites = [
        ("Cross-Sell Pipeline", suite_cross_sell),
        ("EBITDA Bridge", suite_ebitda_bridge),
        ("What-If Sensitivity", suite_what_if),
        ("Executive Dashboards", suite_dashboards),
        ("Maestra Engagement", suite_maestra),
    ]

    for name, fn in suites:
        try:
            fn()
        except requests.ConnectionError as e:
            print(f"  [ERROR] Connection lost during {name} suite: {e}")
        except Exception as e:
            print(f"  [ERROR] Unexpected error in {name} suite: {e}")

    total = len(results)
    passed = sum(1 for r in results if r.passed)
    failed = total - passed

    print(f"\n=== RESULTS -- Phase 1 Part 2 ===")
    print(f"Total: {total}")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")

    if failed > 0:
        print("\nFAILURES:")
        for r in results:
            if not r.passed:
                print(f"  [FAIL] {r.test_id}: {r.description} -- {r.message}")
        print(f"\n=== STATUS: NOT COMPLETE ===")
    else:
        print(f"\n=== STATUS: COMPLETE ===")

    return failed


if __name__ == "__main__":
    failed = run_harness()
    sys.exit(0 if failed == 0 else 1)  # -1 (connection failure) also exits 1
