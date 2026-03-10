#!/usr/bin/env python3
"""
Latency regression tests — DCL report endpoints.

Verifies that all report/engine endpoints return within acceptable latency
bounds.  Tests run through the API (same path as the UI) using requests.

Latency thresholds:
  - Cold start (first request, cache build): < 5000ms
  - Warm (cache hit): < 200ms
  - Maestra message (engine-backed intent): < 500ms

These thresholds are intentionally generous — the point is to catch regressions
(e.g., per-request engine recomputation) not to micro-optimize.

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
COLD_THRESHOLD_MS = 5000    # First request builds the cache
WARM_THRESHOLD_MS = 200     # Subsequent requests serve from cache
MAESTRA_THRESHOLD_MS = 500  # Maestra messages with engine-backed responses


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


def _timed_post(url: str, json_body: dict) -> tuple[requests.Response, float]:
    """POST with timing. Returns (response, elapsed_ms)."""
    t0 = time.monotonic()
    resp = requests.post(url, json=json_body, timeout=TIMEOUT)
    elapsed_ms = (time.monotonic() - t0) * 1000
    return resp, elapsed_ms


# ─── LAT_001: Cross-sell cold start ──────────────────────────────────


def test_lat_001():
    """Cross-sell endpoint — cold start (first call builds cache)."""
    t = TestResult("LAT_001", "Cross-sell cold start latency")
    results.append(t)

    resp, elapsed = _timed_get(f"{BASE_URL}/api/reports/cross-sell")
    t.latency_ms = elapsed

    if resp.status_code != 200:
        t.fail("HTTP 200", f"HTTP {resp.status_code}: {resp.text[:200]}")
        return

    data = resp.json()
    if "summary" not in data:
        t.fail("response with 'summary' key", f"keys: {list(data.keys())}")
        return

    if elapsed > COLD_THRESHOLD_MS:
        t.fail(f"<{COLD_THRESHOLD_MS}ms", f"{elapsed:.0f}ms")
        return

    t.pass_(f"{elapsed:.0f}ms")


# ─── LAT_002: Cross-sell warm (cache hit) ─────────────────────────────


def test_lat_002():
    """Cross-sell endpoint — warm request (cache hit)."""
    t = TestResult("LAT_002", "Cross-sell warm latency")
    results.append(t)

    # Ensure cache is warm
    requests.get(f"{BASE_URL}/api/reports/cross-sell", timeout=TIMEOUT)

    resp, elapsed = _timed_get(f"{BASE_URL}/api/reports/cross-sell")
    t.latency_ms = elapsed

    if resp.status_code != 200:
        t.fail("HTTP 200", f"HTTP {resp.status_code}")
        return

    if elapsed > WARM_THRESHOLD_MS:
        t.fail(f"<{WARM_THRESHOLD_MS}ms", f"{elapsed:.0f}ms")
        return

    t.pass_(f"{elapsed:.0f}ms")


# ─── LAT_003: EBITDA bridge warm ──────────────────────────────────────


def test_lat_003():
    """EBITDA bridge endpoint — warm latency."""
    t = TestResult("LAT_003", "EBITDA bridge warm latency")
    results.append(t)

    # Ensure cache is warm
    requests.get(f"{BASE_URL}/api/reports/cross-sell", timeout=TIMEOUT)

    resp, elapsed = _timed_get(f"{BASE_URL}/api/reports/ebitda-bridge")
    t.latency_ms = elapsed

    if resp.status_code != 200:
        t.fail("HTTP 200", f"HTTP {resp.status_code}: {resp.text[:200]}")
        return

    data = resp.json()
    if "pro_forma_ebitda" not in data:
        t.fail("response with 'pro_forma_ebitda'", f"keys: {list(data.keys())}")
        return

    if elapsed > WARM_THRESHOLD_MS:
        t.fail(f"<{WARM_THRESHOLD_MS}ms", f"{elapsed:.0f}ms")
        return

    t.pass_(f"{elapsed:.0f}ms")


# ─── LAT_004: QofE warm ──────────────────────────────────────────────


def test_lat_004():
    """QofE endpoint — warm latency."""
    t = TestResult("LAT_004", "QofE warm latency")
    results.append(t)

    # Ensure cache is warm
    requests.get(f"{BASE_URL}/api/reports/cross-sell", timeout=TIMEOUT)

    resp, elapsed = _timed_get(f"{BASE_URL}/api/reports/qoe")
    t.latency_ms = elapsed

    if resp.status_code != 200:
        t.fail("HTTP 200", f"HTTP {resp.status_code}: {resp.text[:200]}")
        return

    if elapsed > WARM_THRESHOLD_MS:
        t.fail(f"<{WARM_THRESHOLD_MS}ms", f"{elapsed:.0f}ms")
        return

    t.pass_(f"{elapsed:.0f}ms")


# ─── LAT_005: Dashboard warm (all 5 personas) ─────────────────────────


def test_lat_005():
    """Dashboard endpoints — warm latency for all 5 personas."""
    t = TestResult("LAT_005", "Dashboard warm latency (all personas)")
    results.append(t)

    # Ensure cache is warm
    requests.get(f"{BASE_URL}/api/reports/cross-sell", timeout=TIMEOUT)

    worst_persona = ""
    worst_latency = 0.0

    for persona in ("cfo", "cro", "coo", "cto", "chro"):
        resp, elapsed = _timed_get(f"{BASE_URL}/api/reports/dashboard/{persona}")

        if resp.status_code != 200:
            t.fail(f"HTTP 200 for {persona}", f"HTTP {resp.status_code}: {resp.text[:200]}")
            return

        if elapsed > worst_latency:
            worst_latency = elapsed
            worst_persona = persona

    t.latency_ms = worst_latency

    if worst_latency > WARM_THRESHOLD_MS:
        t.fail(f"<{WARM_THRESHOLD_MS}ms", f"{worst_persona}={worst_latency:.0f}ms")
        return

    t.pass_(f"worst={worst_persona} at {worst_latency:.0f}ms")


# ─── LAT_006: Maestra engine-backed message ───────────────────────────


def test_lat_006():
    """Maestra overview message — engine-backed, should use cache."""
    t = TestResult("LAT_006", "Maestra engine-backed message latency")
    results.append(t)

    # Ensure cache is warm
    requests.get(f"{BASE_URL}/api/reports/cross-sell", timeout=TIMEOUT)

    # Create engagement
    resp = requests.post(f"{BASE_URL}/api/reports/maestra/engage", timeout=TIMEOUT)
    if resp.status_code != 200:
        t.fail("engagement creation HTTP 200", f"HTTP {resp.status_code}")
        return

    eid = resp.json()["engagement_id"]

    # Send engine-backed message
    resp, elapsed = _timed_post(
        f"{BASE_URL}/api/reports/maestra/{eid}/message",
        {"message": "good morning overview"},
    )
    t.latency_ms = elapsed

    if resp.status_code != 200:
        t.fail("HTTP 200", f"HTTP {resp.status_code}: {resp.text[:200]}")
        return

    data = resp.json()
    if "response" not in data:
        t.fail("response field present", f"keys: {list(data.keys())}")
        return

    if elapsed > MAESTRA_THRESHOLD_MS:
        t.fail(f"<{MAESTRA_THRESHOLD_MS}ms", f"{elapsed:.0f}ms")
        return

    t.pass_(f"{elapsed:.0f}ms")


# ─── LAT_007: Sequential dashboard requests show no degradation ──────


def test_lat_007():
    """10 sequential dashboard requests — no latency degradation."""
    t = TestResult("LAT_007", "Sequential dashboard requests — no degradation")
    results.append(t)

    # Ensure cache is warm
    requests.get(f"{BASE_URL}/api/reports/cross-sell", timeout=TIMEOUT)

    latencies = []
    for i in range(10):
        persona = ["cfo", "cro", "coo", "cto", "chro"][i % 5]
        resp, elapsed = _timed_get(f"{BASE_URL}/api/reports/dashboard/{persona}")
        if resp.status_code != 200:
            t.fail(f"HTTP 200 on request {i+1}", f"HTTP {resp.status_code}")
            return
        latencies.append(elapsed)

    t.latency_ms = max(latencies)
    avg = sum(latencies) / len(latencies)

    # No request should be more than 3x the average (would indicate cache thrashing)
    outliers = [lat for lat in latencies if lat > avg * 3 and lat > WARM_THRESHOLD_MS]
    if outliers:
        t.fail(
            f"no outliers >3x avg ({avg:.0f}ms)",
            f"{len(outliers)} outliers: {[f'{l:.0f}ms' for l in outliers]}",
        )
        return

    if max(latencies) > WARM_THRESHOLD_MS:
        t.fail(f"all <{WARM_THRESHOLD_MS}ms", f"max={max(latencies):.0f}ms")
        return

    t.pass_(f"avg={avg:.0f}ms, max={max(latencies):.0f}ms, min={min(latencies):.0f}ms")


# ─── LAT_008: What-if uses cached bridge ──────────────────────────────


def test_lat_008():
    """What-if endpoint — should use cached bridge, not recompute."""
    t = TestResult("LAT_008", "What-if warm latency")
    results.append(t)

    # Ensure cache is warm
    requests.get(f"{BASE_URL}/api/reports/cross-sell", timeout=TIMEOUT)

    resp, elapsed = _timed_post(
        f"{BASE_URL}/api/reports/what-if",
        {"preset": "base"},
    )
    t.latency_ms = elapsed

    if resp.status_code != 200:
        t.fail("HTTP 200", f"HTTP {resp.status_code}: {resp.text[:200]}")
        return

    if elapsed > WARM_THRESHOLD_MS:
        t.fail(f"<{WARM_THRESHOLD_MS}ms", f"{elapsed:.0f}ms")
        return

    t.pass_(f"{elapsed:.0f}ms")


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
    """Ingest stats endpoint — warm latency (cached)."""
    t = TestResult("LAT_010", "Ingest stats warm latency")
    results.append(t)

    # Cold call to build cache
    resp = requests.get(f"{BASE_URL}/api/dcl/ingest/stats", timeout=TIMEOUT)
    if resp.status_code != 200:
        t.fail("HTTP 200", f"HTTP {resp.status_code}: {resp.text[:200]}")
        return

    # Warm call
    resp, elapsed = _timed_get(f"{BASE_URL}/api/dcl/ingest/stats")
    t.latency_ms = elapsed

    if resp.status_code != 200:
        t.fail("HTTP 200", f"HTTP {resp.status_code}")
        return

    data = resp.json()
    if "total_runs" not in data:
        t.fail("response with 'total_runs' key", f"keys: {list(data.keys())}")
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
        test_lat_001,
        test_lat_002,
        test_lat_003,
        test_lat_004,
        test_lat_005,
        test_lat_006,
        test_lat_007,
        test_lat_008,
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
