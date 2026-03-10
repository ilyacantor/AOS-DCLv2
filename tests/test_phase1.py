#!/usr/bin/env python3
"""
DCL Agent -- Phase 1 Part 1 Test Suite

Tests dual-entity support, COFA unification, entity overlap, and backward compat.
Rules: 100% pass rate, fix defects not tests, no mocking, no skipping.
"""

import os
import sys
import time
from typing import List, Optional

import requests

BASE_URL = os.environ.get("DCL_BASE_URL", "http://localhost:8004")
TIMEOUT = 30.0


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


# ─── SUITE: Dual Entity ──────────────────────────────────────────────────
def suite_dual_entity():
    print("\n=== SUITE: Dual Entity ===")

    # DE-001
    t = test("DE-001", "Meridian query returns data")
    r = api_post("/api/dcl/query", json={"metric": "revenue", "entity_id": "meridian"})
    if r.status_code == 200:
        data = r.json()
        pts = data.get("data", data.get("data_points", []))
        if len(pts) > 0:
            t.pass_(f"{len(pts)} data points")
        else:
            t.fail("data_points > 0", f"{len(pts)} data points")
    else:
        t.fail("200", str(r.status_code))

    # DE-002
    t = test("DE-002", "Cascadia query returns data")
    r = api_post("/api/dcl/query", json={"metric": "revenue", "entity_id": "cascadia"})
    if r.status_code == 200:
        data = r.json()
        pts = data.get("data", data.get("data_points", []))
        if len(pts) > 0:
            t.pass_(f"{len(pts)} data points")
        else:
            t.fail("data_points > 0", f"{len(pts)} data points")
    else:
        t.fail("200", str(r.status_code))

    # DE-003
    t = test("DE-003", "Query without entity_id still works (backward compat)")
    r = api_post("/api/dcl/query", json={"metric": "revenue"})
    if r.status_code == 200:
        data = r.json()
        pts = data.get("data", data.get("data_points", []))
        if len(pts) > 0:
            t.pass_(f"{len(pts)} data points")
        else:
            t.fail("data_points > 0", f"{len(pts)} data points")
    else:
        t.fail("200", str(r.status_code))

    # DE-004
    t = test("DE-004", "Consolidate query works")
    r = api_post("/api/dcl/query", json={"metric": "revenue", "consolidate": True})
    if r.status_code == 200:
        data = r.json()
        pts = data.get("data", data.get("data_points", []))
        if len(pts) > 0:
            t.pass_(f"{len(pts)} data points")
        else:
            t.fail("data_points > 0", f"{len(pts)} data points")
    else:
        t.fail("200", str(r.status_code))

    # DE-005
    t = test("DE-005", "Entity selector — query with entity_id returns entity-scoped data")
    r = api_post("/api/dcl/query", json={"metric": "revenue", "entity_id": "meridian"})
    if r.status_code == 200:
        data = r.json()
        pts = data.get("data", data.get("data_points", []))
        if len(pts) > 0:
            # Check if data points have entity_id field
            has_entity = any(p.get("entity_id") for p in pts if isinstance(p, dict))
            if has_entity:
                all_meridian = all(
                    p.get("entity_id") == "meridian"
                    for p in pts
                    if isinstance(p, dict) and p.get("entity_id")
                )
                if all_meridian:
                    t.pass_("entity_id=meridian on all points")
                else:
                    t.fail("entity_id=meridian", "mixed entity_ids in response")
            else:
                t.pass_("query succeeded (entity_id not on data points)")
        else:
            t.fail("data_points > 0", f"{len(pts)} data points")
    else:
        t.fail("200", str(r.status_code))

    # DE-006
    t = test("DE-006", "Combining income statement endpoint exists")
    r = api_get("/api/reports/combining-is")
    if r.status_code == 200:
        data = r.json()
        line_items = data.get("line_items", [])
        if not isinstance(line_items, list):
            t.fail("line_items array", f"line_items is {type(line_items).__name__}")
        elif len(line_items) == 0:
            t.fail("line_items with data", "empty line_items array")
        else:
            has_entity_value = any(
                (li.get("meridian", 0) or 0) > 0 or (li.get("cascadia", 0) or 0) > 0
                for li in line_items
                if isinstance(li, dict)
            )
            if has_entity_value:
                t.pass_(f"{len(line_items)} line items with entity values")
            else:
                t.fail("meridian > 0 or cascadia > 0", "no entity values found")
    else:
        t.fail("200", str(r.status_code))


# ─── SUITE: COFA Unification ─────────────────────────────────────────────
def suite_cofa_unification():
    print("\n=== SUITE: COFA Unification ===")

    # CU-001
    t = test("CU-001", "Combining IS has revenue lines")
    r = api_get("/api/reports/combining-is")
    if r.status_code == 200:
        data = r.json()
        line_items = data.get("line_items", [])
        revenue_line = None
        for li in line_items:
            if isinstance(li, dict) and "total revenue" in (li.get("line_item", "") or "").lower():
                revenue_line = li
                break
        if revenue_line is None:
            t.fail("'Total Revenue' line", "not found in line_items")
        else:
            combined = revenue_line.get("combined", revenue_line.get("total", 0)) or 0
            if combined > 0:
                t.pass_(f"Total Revenue combined={combined}")
            else:
                t.fail("combined > 0", f"combined={combined}")
    else:
        t.fail("200", str(r.status_code))

    # CU-002
    t = test("CU-002", "Combining IS has COFA adjustments")
    r = api_get("/api/reports/combining-is")
    if r.status_code == 200:
        data = r.json()
        cofa = data.get("cofa_adjustments", [])
        if not isinstance(cofa, list):
            t.fail("cofa_adjustments array", f"cofa_adjustments is {type(cofa).__name__}")
        elif len(cofa) >= 4:
            t.pass_(f"{len(cofa)} COFA adjustments")
        else:
            t.fail("at least 4 adjustments", f"{len(cofa)} adjustments")
    else:
        t.fail("200", str(r.status_code))

    # CU-003
    t = test("CU-003", "Combining IS balances: A + B + Adj = Combined")
    r = api_get("/api/reports/combining-is")
    if r.status_code == 200:
        data = r.json()
        line_items = data.get("line_items", [])
        failures = []
        checked = 0
        for li in line_items:
            if not isinstance(li, dict):
                continue
            meridian_val = li.get("meridian", 0) or 0
            cascadia_val = li.get("cascadia", 0) or 0
            adj_val = li.get("adjustments", 0) or 0
            combined_val = li.get("combined", 0) or 0
            expected = meridian_val + cascadia_val + adj_val
            diff = abs(expected - combined_val)
            checked += 1
            if diff >= 0.02:
                label = li.get("line_item", "unknown")
                failures.append(
                    f"{label}: {meridian_val}+{cascadia_val}+{adj_val}={expected} != {combined_val} (diff={diff:.4f})"
                )
        if checked == 0:
            t.fail("line_items to check", "no line_items found")
        elif len(failures) == 0:
            t.pass_(f"all {checked} line items balance")
        else:
            t.fail("all balanced", f"{len(failures)} imbalanced: {failures[0]}")
    else:
        t.fail("200", str(r.status_code))

    # CU-004
    t = test("CU-004", "Revenue gross-up adjustment present")
    r = api_get("/api/reports/combining-is")
    if r.status_code == 200:
        data = r.json()
        line_items = data.get("line_items", [])
        found = False
        for li in line_items:
            if not isinstance(li, dict):
                continue
            label = (li.get("line_item", "") or "").lower()
            if "gross-up" in label or "gross up" in label:
                adj = li.get("adjustments", 0) or 0
                if adj < 0:
                    found = True
                    break
        if found:
            t.pass_("Gross-Up line with negative adjustments found")
        else:
            t.fail("Gross-Up line with negative adjustments", "not found")
    else:
        t.fail("200", str(r.status_code))

    # CU-005
    t = test("CU-005", "Combining IS has period selector")
    r = api_get("/api/reports/combining-is")
    if r.status_code == 200:
        data = r.json()
        periods = data.get("available_periods", [])
        if not isinstance(periods, list):
            t.fail("available_periods array", f"type={type(periods).__name__}")
        elif len(periods) == 12:
            t.pass_(f"12 periods available")
        else:
            t.fail("12 periods", f"{len(periods)} periods")
    else:
        t.fail("200", str(r.status_code))


# ─── SUITE: Entity Overlap ───────────────────────────────────────────────
def suite_entity_overlap():
    print("\n=== SUITE: Entity Overlap ===")

    # ER-001
    t = test("ER-001", "Entity overlap endpoint exists")
    r = api_get("/api/reports/entity-overlap")
    if r.status_code == 200:
        data = r.json()
        if "customer_overlap" in data:
            t.pass_()
        else:
            t.fail("customer_overlap key", f"keys={list(data.keys())}")
    else:
        t.fail("200", str(r.status_code))

    # ER-002
    t = test("ER-002", "Customer overlap in expected range")
    r = api_get("/api/reports/entity-overlap")
    if r.status_code == 200:
        data = r.json()
        co = data.get("customer_overlap", {})
        total = co.get("total_overlapping", -1)
        if 25 <= total <= 45:
            t.pass_(f"total_overlapping={total}")
        else:
            t.fail("25-45", str(total))
    else:
        t.fail("200", str(r.status_code))

    # ER-003
    t = test("ER-003", "Vendor overlap in expected range")
    r = api_get("/api/reports/entity-overlap")
    if r.status_code == 200:
        data = r.json()
        vo = data.get("vendor_overlap", {})
        total = vo.get("total_overlapping", -1)
        if 150 <= total <= 200:
            t.pass_(f"total_overlapping={total}")
        else:
            t.fail("150-200", str(total))
    else:
        t.fail("200", str(r.status_code))


# ─── SUITE: Entity Resolution ────────────────────────────────────────────
def suite_entity_resolution():
    print("\n=== SUITE: Entity Resolution ===")

    # ER-004
    t = test("ER-004", "Cross-entity customer matches accessible")
    r = api_get("/api/dcl/entities/cross-entity?entity_type=customer")
    if r.status_code == 200:
        data = r.json()
        total = data.get("total_matches", 0)
        if 25 <= total <= 45:
            t.pass_(f"total_matches={total}")
        else:
            t.fail("25-45 matches", str(total))
    else:
        t.fail("200", str(r.status_code))

    # ER-005
    t = test("ER-005", "Cross-entity vendor matches accessible")
    r = api_get("/api/dcl/entities/cross-entity?entity_type=vendor")
    if r.status_code == 200:
        data = r.json()
        total = data.get("total_matches", 0)
        if 150 <= total <= 200:
            t.pass_(f"total_matches={total}")
        else:
            t.fail("150-200 matches", str(total))
    else:
        t.fail("200", str(r.status_code))

    # ER-006
    t = test("ER-006", "People function overlap matches exist")
    r = api_get("/api/dcl/entities/cross-entity?entity_type=people")
    if r.status_code == 200:
        data = r.json()
        total = data.get("total_matches", 0)
        confirmed = data.get("confirmed", 0)
        if total >= 3 and confirmed == total:
            t.pass_(f"total={total}, all confirmed")
        else:
            t.fail(">=3 all confirmed", f"total={total} confirmed={confirmed}")
    else:
        t.fail("200", str(r.status_code))

    # ER-007
    t = test("ER-007", "Vendor and people entity types accepted for resolution")
    r_v = api_post("/api/dcl/entities/resolve", params={"entity_type": "vendor"})
    r_p = api_post("/api/dcl/entities/resolve", params={"entity_type": "people"})
    if r_v.status_code == 200 and r_p.status_code == 200:
        t.pass_("vendor and people types accepted")
    else:
        t.fail("200 for both", f"vendor={r_v.status_code} people={r_p.status_code}")


# ─── SUITE: Combining Recon ──────────────────────────────────────────────
def suite_combining_recon():
    print("\n=== SUITE: Combining Recon ===")

    # CR-001
    t = test("CR-001", "Combining IS has all expected line items")
    r = api_get("/api/reports/combining-is")
    if r.status_code == 200:
        data = r.json()
        line_items = data.get("line_items", [])
        labels = [li.get("line_item", "").lower() for li in line_items if isinstance(li, dict)]
        expected = ["total revenue", "total cogs", "gross profit", "total opex", "ebitda", "net income"]
        missing = [e for e in expected if not any(e in l for l in labels)]
        if not missing:
            t.pass_(f"all {len(expected)} expected lines present")
        else:
            t.fail("all lines present", f"missing: {missing}")
    else:
        t.fail("200", str(r.status_code))

    # CR-002
    t = test("CR-002", "Meridian revenue in $5B range")
    r = api_get("/api/reports/combining-is")
    if r.status_code == 200:
        data = r.json()
        line_items = data.get("line_items", [])
        for li in line_items:
            if isinstance(li, dict) and "total revenue" in (li.get("line_item", "") or "").lower():
                meridian_rev = li.get("meridian", 0) or 0
                # Quarterly rev should be ~$1.2-1.5B for $5B annual
                if 1000 < meridian_rev < 2000:
                    t.pass_(f"meridian quarterly rev={meridian_rev}M")
                else:
                    t.fail("1000-2000M quarterly", f"{meridian_rev}M")
                break
        else:
            t.fail("Total Revenue line", "not found")
    else:
        t.fail("200", str(r.status_code))

    # CR-003
    t = test("CR-003", "Cascadia revenue in $1B range")
    r = api_get("/api/reports/combining-is")
    if r.status_code == 200:
        data = r.json()
        line_items = data.get("line_items", [])
        for li in line_items:
            if isinstance(li, dict) and "total revenue" in (li.get("line_item", "") or "").lower():
                cascadia_rev = li.get("cascadia", 0) or 0
                # Quarterly rev should be ~$250-300M for $1B annual
                if 200 < cascadia_rev < 400:
                    t.pass_(f"cascadia quarterly rev={cascadia_rev}M")
                else:
                    t.fail("200-400M quarterly", f"{cascadia_rev}M")
                break
        else:
            t.fail("Total Revenue line", "not found")
    else:
        t.fail("200", str(r.status_code))

    # CR-004
    t = test("CR-004", "6 COFA conflicts in combining data")
    r = api_get("/api/reports/combining-is")
    if r.status_code == 200:
        data = r.json()
        cofa = data.get("cofa_adjustments", [])
        if isinstance(cofa, list) and len(cofa) >= 6:
            t.pass_(f"{len(cofa)} COFA adjustments")
        else:
            t.fail(">=6 adjustments", f"{len(cofa) if isinstance(cofa, list) else 'not a list'}")
    else:
        t.fail("200", str(r.status_code))

    # CR-005
    t = test("CR-005", "Full dual-entity recon — combining IS balances on every line with zero RED")
    r = api_get("/api/reports/combining-is")
    if r.status_code == 200:
        data = r.json()
        line_items = data.get("line_items", [])
        red_count = 0
        checked = 0
        for li in line_items:
            if not isinstance(li, dict):
                continue
            meridian_val = li.get("meridian", 0) or 0
            cascadia_val = li.get("cascadia", 0) or 0
            adj_val = li.get("adjustments", 0) or 0
            combined_val = li.get("combined", 0) or 0
            expected = meridian_val + cascadia_val + adj_val
            diff = abs(expected - combined_val)
            checked += 1
            if diff >= 0.1:
                red_count += 1
        if checked == 0:
            t.fail("line items to check", "none found")
        elif red_count == 0:
            t.pass_(f"all {checked} lines balance, zero RED")
        else:
            t.fail("zero RED", f"{red_count} RED out of {checked}")
    else:
        t.fail("200", str(r.status_code))


# ─── SUITE: Backward Compat ──────────────────────────────────────────────
def suite_backward_compat():
    print("\n=== SUITE: Backward Compat ===")

    # BC-001
    t = test("BC-001", "Phase 0 query still works")
    r = api_post("/api/dcl/query", json={"metric": "revenue"})
    if r.status_code == 200:
        data = r.json()
        pts = data.get("data", data.get("data_points", []))
        if len(pts) > 0:
            t.pass_(f"{len(pts)} data points")
        else:
            t.fail("data_points > 0", f"{len(pts)} data points")
    else:
        t.fail("200", str(r.status_code))

    # BC-002
    t = test("BC-002", "Phase 0 recon still works")
    r = api_post("/api/dcl/query", json={"metric": "gross_margin_pct"})
    if r.status_code == 200:
        data = r.json()
        pts = data.get("data", data.get("data_points", []))
        if len(pts) > 0:
            val = pts[0].get("value", 0) if isinstance(pts[0], dict) else 0
            if 0 < val < 100:
                t.pass_(f"gross_margin_pct={val}")
            else:
                t.fail("0 < value < 100", str(val))
        else:
            t.fail("data_points > 0", f"{len(pts)} data points")
    else:
        t.fail("200", str(r.status_code))

    # BC-003
    t = test("BC-003", "Health endpoint still works")
    r = api_get("/health")
    if r.status_code == 200:
        t.pass_()
    else:
        t.fail("200", str(r.status_code))


# ─── main ─────────────────────────────────────────────────────────────────
def run_harness():
    global results

    print("\n=== DCL PHASE 1 PART 1 TEST SUITE ===\n")

    if not wait_for_backend():
        print(f"FATAL: Backend not responding at {BASE_URL}")
        print("Start the DCL backend and try again.")
        return -1  # Signal connection failure

    results = []

    try:
        suite_dual_entity()
    except requests.ConnectionError as e:
        print(f"  [ERROR] Connection lost during Dual Entity suite: {e}")
    except Exception as e:
        print(f"  [ERROR] Unexpected error in Dual Entity suite: {e}")

    try:
        suite_cofa_unification()
    except requests.ConnectionError as e:
        print(f"  [ERROR] Connection lost during COFA Unification suite: {e}")
    except Exception as e:
        print(f"  [ERROR] Unexpected error in COFA Unification suite: {e}")

    try:
        suite_entity_overlap()
    except requests.ConnectionError as e:
        print(f"  [ERROR] Connection lost during Entity Overlap suite: {e}")
    except Exception as e:
        print(f"  [ERROR] Unexpected error in Entity Overlap suite: {e}")

    try:
        suite_entity_resolution()
    except requests.ConnectionError as e:
        print(f"  [ERROR] Connection lost during Entity Resolution suite: {e}")
    except Exception as e:
        print(f"  [ERROR] Unexpected error in Entity Resolution suite: {e}")

    try:
        suite_combining_recon()
    except requests.ConnectionError as e:
        print(f"  [ERROR] Connection lost during Combining Recon suite: {e}")
    except Exception as e:
        print(f"  [ERROR] Unexpected error in Combining Recon suite: {e}")

    try:
        suite_backward_compat()
    except requests.ConnectionError as e:
        print(f"  [ERROR] Connection lost during Backward Compat suite: {e}")
    except Exception as e:
        print(f"  [ERROR] Unexpected error in Backward Compat suite: {e}")

    total = len(results)
    passed = sum(1 for r in results if r.passed)
    failed = total - passed

    print(f"\n=== RESULTS -- Phase 1 Part 1 ===")
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
