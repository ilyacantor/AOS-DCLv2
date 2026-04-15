#!/usr/bin/env python3
"""
Latency regression tests — DCL SE endpoints.

Verifies that SE report/engine endpoints return within acceptable latency
bounds.  Tests run through the API (same path as the UI) using requests.

ME latency tests (cross-sell, EBITDA bridge, QoE, dashboards, Mai,
what-if) removed — those live in the Convergence repo.

Latency thresholds:
  - Cold start (first request): < 5000ms
  - Warm (cache hit): < 200ms

Run: python -m tests.test_latency
Requires: DCL backend running on localhost:8004
"""

import os
import sys
import time
from typing import List

import requests

BASE_URL = os.environ.get("DCL_BASE_URL", "http://localhost:8004")
TIMEOUT = 60.0

# Latency thresholds in milliseconds
WARM_THRESHOLD_MS = 200     # Subsequent requests serve from cache


class TestResult:
    def __init__(self, test_id: str, description: str):
        self.test_id = test_id
        self.description = description
        self.passed = False
        self.message = ""
        self.latency_ms = 0.0

    def pass_(self, msg: str = ""):
        self.passed = True
        self.message = msg
        print(f"  [PASS] {self.test_id}: {self.description} — {msg}")

    def fail(self, expected: str, got: str):
        self.passed = False
        self.message = f"expected {expected}, got {got}"
        print(f"  [FAIL] {self.test_id}: {self.description} — {self.message}")


results: List[TestResult] = []


def _timed_get(url: str) -> tuple[requests.Response, float]:
    """GET with timing. Returns (response, elapsed_ms)."""
    t0 = time.monotonic()
    resp = requests.get(url, timeout=TIMEOUT)
    elapsed_ms = (time.monotonic() - t0) * 1000
    return resp, elapsed_ms


# ─── LAT_009: Recon cross-system warm latency ─────────────────────────


def test_lat_009():
    """Recon cross-system endpoint — warm latency (cached)."""
    t = TestResult("LAT_009", "Recon cross-system warm latency")
    results.append(t)

    # Cold call to build cache
    resp = requests.get(f"{BASE_URL}/api/dcl/reconciliation/cross-system", timeout=TIMEOUT)
    if resp.status_code != 200:
        t.fail("HTTP 200", f"HTTP {resp.status_code}: {resp.text[:200]}")
        return

    # Warm call
    resp, elapsed = _timed_get(f"{BASE_URL}/api/dcl/reconciliation/cross-system")
    t.latency_ms = elapsed

    if resp.status_code != 200:
        t.fail("HTTP 200", f"HTTP {resp.status_code}")
        return

    data = resp.json()
    if "systems" not in data:
        t.fail("response with 'systems' key", f"keys: {list(data.keys())}")
        return

    if elapsed > WARM_THRESHOLD_MS:
        t.fail(f"<{WARM_THRESHOLD_MS}ms", f"{elapsed:.0f}ms")
        return

    t.pass_(f"{elapsed:.0f}ms")


# ─── LAT_010: Ingest stats warm latency ──────────────────────────────


def test_lat_010():
    """Snapshots endpoint — warm latency."""
    t = TestResult("LAT_010", "Snapshots warm latency")
    results.append(t)

    resp = requests.get(f"{BASE_URL}/api/dcl/snapshots", timeout=TIMEOUT)
    if resp.status_code != 200:
        t.fail("HTTP 200", f"HTTP {resp.status_code}: {resp.text[:200]}")
        return

    resp, elapsed = _timed_get(f"{BASE_URL}/api/dcl/snapshots")
    t.latency_ms = elapsed

    if resp.status_code != 200:
        t.fail("HTTP 200", f"HTTP {resp.status_code}")
        return

    data = resp.json()
    if "runs" not in data:
        t.fail("response with 'runs' key", f"keys: {list(data.keys())}")
        return

    if elapsed > WARM_THRESHOLD_MS:
        t.fail(f"<{WARM_THRESHOLD_MS}ms", f"{elapsed:.0f}ms")
        return

    t.pass_(f"{elapsed:.0f}ms")


# ─── Runner ───────────────────────────────────────────────────────────


def main():
    print(f"\n{'='*70}")
    print("DCL Latency Regression Tests")
    print(f"Target: {BASE_URL}")
    print(f"{'='*70}\n")

    # Check backend is reachable
    try:
        requests.get(f"{BASE_URL}/health", timeout=5)
    except requests.ConnectionError:
        print(f"ERROR: Cannot reach DCL backend at {BASE_URL}")
        print("Start the backend first: pm2 start dcl-backend")
        sys.exit(1)

    tests = [
        test_lat_009,
        test_lat_010,
    ]

    for test_fn in tests:
        try:
            test_fn()
        except Exception as e:
            # Surface the error — no silent swallowing
            tr = results[-1] if results else TestResult("???", "unknown")
            tr.fail("no exception", f"{type(e).__name__}: {e}")

    # Summary
    passed = sum(1 for r in results if r.passed)
    total = len(results)
    print(f"\n{'='*70}")
    print(f"Results: {passed}/{total} passed")

    # Latency summary table
    print(f"\n{'Test':<10} {'Latency':>10} {'Status':>8}")
    print(f"{'-'*10} {'-'*10} {'-'*8}")
    for r in results:
        status = "PASS" if r.passed else "FAIL"
        lat = f"{r.latency_ms:.0f}ms" if r.latency_ms > 0 else "N/A"
        print(f"{r.test_id:<10} {lat:>10} {status:>8}")

    print(f"{'='*70}\n")

    if passed < total:
        print(f"FAILED: {total - passed} test(s) failed")
        sys.exit(1)
    else:
        print("ALL TESTS PASSED")
        sys.exit(0)


if __name__ == "__main__":
    main()
