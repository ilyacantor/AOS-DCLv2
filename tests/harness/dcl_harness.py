#!/usr/bin/env python3
"""
DCL Live Endpoint Harness
=========================

Verifies DCL report and entity endpoints via real HTTP requests against
a running DCL server. No Python imports from DCL — HTTP only.

RULES:
  1. HTTP only — every check goes through the DCL API
  2. No silent fallbacks — non-200 is a FAIL with status code and body
  3. Latency tracked — every request records elapsed time
  4. Exit codes: 0 = all pass, 1 = some fail, 2 = harness error

Usage:
    python tests/harness/dcl_harness.py
    python tests/harness/dcl_harness.py --url http://localhost:8004
    python tests/harness/dcl_harness.py --verbose
    python tests/harness/dcl_harness.py --test combining-is
"""

import argparse
import json
import statistics
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

try:
    import httpx
except ImportError:
    print(
        "HARNESS ERROR: httpx is required but not installed.\n"
        "  Install it:  pip install httpx\n"
        "  Or with venv: .venv/bin/pip install httpx"
    )
    sys.exit(2)


# =============================================================================
# Configuration
# =============================================================================

DEFAULT_BASE_URL = "http://localhost:8004"
HEALTH_ENDPOINT = "/health"
TIMEOUT = 30.0
SLOW_THRESHOLD_SEC = 5.0


# =============================================================================
# Data classes
# =============================================================================

@dataclass
class TestResult:
    test_id: str
    name: str
    passed: bool
    status_code: Optional[int] = None
    elapsed_sec: float = 0.0
    detail: str = ""
    slow: bool = False


@dataclass
class HarnessReport:
    results: List[TestResult] = field(default_factory=list)
    latencies: List[float] = field(default_factory=list)

    @property
    def passed(self) -> int:
        return sum(1 for r in self.results if r.passed)

    @property
    def failed(self) -> int:
        return sum(1 for r in self.results if not r.passed)

    @property
    def total(self) -> int:
        return len(self.results)

    @property
    def slow_requests(self) -> List[TestResult]:
        return [r for r in self.results if r.slow]


# =============================================================================
# Test definitions
# =============================================================================

def _check_keys(data: Any, required_keys: List[str], context: str) -> Optional[str]:
    """Return an error string if any required key is missing, else None."""
    if not isinstance(data, dict):
        return f"{context}: expected dict, got {type(data).__name__}"
    missing = [k for k in required_keys if k not in data]
    if missing:
        return f"{context}: missing keys {missing}"
    return None


class EndpointTest:
    """A single endpoint test specification."""

    def __init__(
        self,
        test_id: str,
        name: str,
        method: str,
        path: str,
        body: Optional[Dict] = None,
        validate: "callable" = None,
    ):
        self.test_id = test_id
        self.name = name
        self.method = method.upper()
        self.path = path
        self.body = body
        self.validate = validate


def _validate_combining_is(data: dict) -> Optional[str]:
    err = _check_keys(data, ["line_items", "available_periods"], "combining-is")
    if err:
        return err
    if not isinstance(data["line_items"], list):
        return "combining-is: line_items must be a list"
    if not isinstance(data["available_periods"], list):
        return "combining-is: available_periods must be a list"
    return None


def _validate_entity_overlap(data: dict) -> Optional[str]:
    err = _check_keys(
        data,
        ["customer_overlap", "vendor_overlap", "people_overlap"],
        "entity-overlap",
    )
    return err


def _validate_cross_sell(data: dict) -> Optional[str]:
    err = _check_keys(data, ["m_to_c", "c_to_m"], "cross-sell")
    if err:
        return err
    if not isinstance(data["m_to_c"], list):
        return "cross-sell: m_to_c must be a list"
    if not isinstance(data["c_to_m"], list):
        return "cross-sell: c_to_m must be a list"
    return None


def _validate_ebitda_bridge(data: dict) -> Optional[str]:
    return _check_keys(
        data,
        [
            "reported_ebitda",
            "entity_adjustments",
            "combination_synergies",
            "pro_forma_ebitda",
        ],
        "ebitda-bridge",
    )


def _validate_qoe(data: dict) -> Optional[str]:
    err = _check_keys(data, ["summary"], "qoe")
    if err:
        return err
    return _check_keys(
        data["summary"],
        ["sustainability_score", "sustainability_grade"],
        "qoe.summary",
    )


def _validate_dashboard_cfo(data: dict) -> Optional[str]:
    return _check_keys(data, ["kpis", "charts"], "dashboard/cfo")


def _validate_what_if(data: dict) -> Optional[str]:
    # what-if should return scenario results — at minimum a dict with keys
    if not isinstance(data, dict):
        return f"what-if: expected dict, got {type(data).__name__}"
    if len(data) == 0:
        return "what-if: response is empty dict"
    return None


def _validate_cross_entity(data: dict) -> Optional[str]:
    err = _check_keys(data, ["candidates"], "cross-entity")
    if err:
        return err
    if not isinstance(data["candidates"], list):
        return "cross-entity: candidates must be a list"
    return None


def _validate_conflicts(data: dict) -> Optional[str]:
    err = _check_keys(data, ["conflicts"], "conflicts")
    if err:
        return err
    if not isinstance(data["conflicts"], list):
        return "conflicts: conflicts must be a list"
    return None


# All tests in execution order
ALL_TESTS = [
    EndpointTest(
        test_id="combining-is",
        name="Combining Income Statement",
        method="GET",
        path="/api/reports/combining-is",
        validate=_validate_combining_is,
    ),
    EndpointTest(
        test_id="entity-overlap",
        name="Entity Overlap",
        method="GET",
        path="/api/reports/entity-overlap",
        validate=_validate_entity_overlap,
    ),
    EndpointTest(
        test_id="cross-sell",
        name="Cross-Sell Opportunities",
        method="GET",
        path="/api/reports/cross-sell",
        validate=_validate_cross_sell,
    ),
    EndpointTest(
        test_id="ebitda-bridge",
        name="EBITDA Bridge",
        method="GET",
        path="/api/reports/ebitda-bridge",
        validate=_validate_ebitda_bridge,
    ),
    EndpointTest(
        test_id="qoe",
        name="Quality of Earnings",
        method="GET",
        path="/api/reports/qoe",
        validate=_validate_qoe,
    ),
    EndpointTest(
        test_id="dashboard-cfo",
        name="CFO Dashboard",
        method="GET",
        path="/api/reports/dashboard/cfo",
        validate=_validate_dashboard_cfo,
    ),
    EndpointTest(
        test_id="what-if",
        name="What-If Scenario (base preset)",
        method="POST",
        path="/api/reports/what-if",
        body={"preset": "base"},
        validate=_validate_what_if,
    ),
    EndpointTest(
        test_id="cross-entity",
        name="Cross-Entity Matches",
        method="GET",
        path="/api/dcl/entities/cross-entity",
        validate=_validate_cross_entity,
    ),
    EndpointTest(
        test_id="conflicts",
        name="Active Conflicts",
        method="GET",
        path="/api/dcl/conflicts",
        validate=_validate_conflicts,
    ),
]


# =============================================================================
# Harness runner
# =============================================================================

class DCLHarness:
    """Execute endpoint tests against a live DCL server."""

    def __init__(self, base_url: str, verbose: bool = False):
        self.base_url = base_url.rstrip("/")
        self.verbose = verbose
        self.client = httpx.Client(timeout=TIMEOUT)
        self.report = HarnessReport()

    def _log(self, msg: str) -> None:
        """Print only in verbose mode."""
        if self.verbose:
            print(f"    {msg}")

    def check_health(self) -> bool:
        """Verify the DCL server is reachable. Returns False if not."""
        url = f"{self.base_url}{HEALTH_ENDPOINT}"
        try:
            resp = self.client.get(url)
            if resp.status_code == 200:
                self._log(f"Health check OK: {url}")
                return True
            else:
                print(
                    f"HARNESS ERROR: DCL health check returned {resp.status_code} "
                    f"at {url}\n  Body: {resp.text[:500]}"
                )
                return False
        except httpx.ConnectError as exc:
            print(
                f"HARNESS ERROR: Cannot reach DCL server at {url}\n"
                f"  Connection refused — is the DCL server running?\n"
                f"  Detail: {exc}"
            )
            return False
        except httpx.TimeoutException as exc:
            print(
                f"HARNESS ERROR: DCL health check timed out at {url} "
                f"after {TIMEOUT}s\n  Detail: {exc}"
            )
            return False

    def run_test(self, test: EndpointTest) -> TestResult:
        """Execute a single endpoint test and return the result."""
        url = f"{self.base_url}{test.path}"
        self._log(f"{test.method} {url}")

        try:
            t0 = time.perf_counter()
            if test.method == "GET":
                resp = self.client.get(url)
            elif test.method == "POST":
                resp = self.client.post(url, json=test.body or {})
            else:
                raise ValueError(f"Unsupported HTTP method: {test.method}")
            elapsed = time.perf_counter() - t0

        except httpx.ConnectError as exc:
            return TestResult(
                test_id=test.test_id,
                name=test.name,
                passed=False,
                elapsed_sec=0.0,
                detail=f"Connection refused: {exc}",
            )
        except httpx.TimeoutException as exc:
            return TestResult(
                test_id=test.test_id,
                name=test.name,
                passed=False,
                elapsed_sec=TIMEOUT,
                detail=f"Request timed out after {TIMEOUT}s: {exc}",
                slow=True,
            )

        self.report.latencies.append(elapsed)
        is_slow = elapsed > SLOW_THRESHOLD_SEC

        # Non-200 is always a FAIL
        if resp.status_code != 200:
            body_preview = resp.text[:500]
            return TestResult(
                test_id=test.test_id,
                name=test.name,
                passed=False,
                status_code=resp.status_code,
                elapsed_sec=elapsed,
                detail=f"HTTP {resp.status_code}: {body_preview}",
                slow=is_slow,
            )

        # Parse JSON
        try:
            data = resp.json()
        except (json.JSONDecodeError, ValueError) as exc:
            return TestResult(
                test_id=test.test_id,
                name=test.name,
                passed=False,
                status_code=200,
                elapsed_sec=elapsed,
                detail=f"Invalid JSON response: {exc}. Body: {resp.text[:300]}",
                slow=is_slow,
            )

        # Run validation
        if test.validate:
            err = test.validate(data)
            if err:
                self._log(f"Validation failed: {err}")
                return TestResult(
                    test_id=test.test_id,
                    name=test.name,
                    passed=False,
                    status_code=200,
                    elapsed_sec=elapsed,
                    detail=f"Validation: {err}",
                    slow=is_slow,
                )

        self._log(f"OK ({elapsed*1000:.0f}ms)")
        return TestResult(
            test_id=test.test_id,
            name=test.name,
            passed=True,
            status_code=200,
            elapsed_sec=elapsed,
            detail="",
            slow=is_slow,
        )

    def run(self, test_filter: Optional[str] = None) -> int:
        """Run the harness. Returns exit code (0/1/2)."""
        print("=" * 70)
        print("DCL Live Endpoint Harness")
        print(f"Target: {self.base_url}")
        print("=" * 70)

        # Health check first
        if not self.check_health():
            return 2

        # Filter tests if requested
        tests = ALL_TESTS
        if test_filter:
            tests = [t for t in ALL_TESTS if test_filter in t.test_id]
            if not tests:
                print(f"\nNo tests match filter '{test_filter}'. Available test IDs:")
                for t in ALL_TESTS:
                    print(f"  {t.test_id}")
                return 2

        print(f"\nRunning {len(tests)} endpoint tests...\n")

        for test in tests:
            result = self.run_test(test)
            self.report.results.append(result)

            status = "PASS" if result.passed else "FAIL"
            slow_tag = " [SLOW]" if result.slow else ""
            latency_str = f" ({result.elapsed_sec*1000:.0f}ms)" if result.elapsed_sec > 0 else ""

            print(f"  [{status}] {result.name}{latency_str}{slow_tag}")
            if not result.passed:
                print(f"         {result.detail}")

        # Summary
        self._print_summary()

        return 0 if self.report.failed == 0 else 1

    def _print_summary(self) -> None:
        """Print final summary with pass/fail counts and latency stats."""
        r = self.report
        print(f"\n{'=' * 70}")
        print(f"Results: {r.passed} passed, {r.failed} failed, {r.total} total")

        if r.latencies:
            sorted_lat = sorted(r.latencies)
            p95_idx = max(0, int(len(sorted_lat) * 0.95) - 1)
            print(
                f"Latency: min={min(sorted_lat)*1000:.0f}ms, "
                f"max={max(sorted_lat)*1000:.0f}ms, "
                f"avg={statistics.mean(sorted_lat)*1000:.0f}ms, "
                f"p95={sorted_lat[p95_idx]*1000:.0f}ms"
            )

        if r.slow_requests:
            print(f"\nSlow requests (>{SLOW_THRESHOLD_SEC}s):")
            for sr in r.slow_requests:
                print(f"  - {sr.name}: {sr.elapsed_sec*1000:.0f}ms")

        print("=" * 70)


# =============================================================================
# CLI
# =============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description="DCL Live Endpoint Harness — tests DCL API via HTTP"
    )
    parser.add_argument(
        "--url",
        default=DEFAULT_BASE_URL,
        help=f"DCL base URL (default: {DEFAULT_BASE_URL})",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print detailed request/response info",
    )
    parser.add_argument(
        "--test",
        default=None,
        help="Run only tests whose ID contains this string",
    )
    args = parser.parse_args()

    harness = DCLHarness(base_url=args.url, verbose=args.verbose)
    exit_code = harness.run(test_filter=args.test)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
