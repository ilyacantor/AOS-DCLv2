#!/usr/bin/env python3
"""
DCL Agent -- Phase 1 Part 1 Test Suite (SE Only)

Tests backward compatibility for SE queries.
ME tests (dual-entity, COFA, entity overlap, entity resolution, combining)
removed — those live in the Convergence repo.
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
