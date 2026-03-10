#!/usr/bin/env python3
"""
Unified test runner for all AOS-DCL test phases.

Usage:
    python -m tests.run_all                       # Run all phases
    python -m tests.run_all --phase 0             # Run only phase 0
    python -m tests.run_all --phase 1             # Run only phase 1
    python -m tests.run_all --phase 2             # Run only phase 2
    python -m tests.run_all --base-url http://localhost:9000

Exit codes:
    0 — all tests passed
    1 — one or more tests failed or backend unreachable
"""

import argparse
import os
import sys
import time

import requests


def check_backend(base_url: str, timeout: int = 10) -> bool:
    """Check if the backend is reachable. Returns True if healthy."""
    for i in range(timeout):
        try:
            r = requests.get(f"{base_url}/health", timeout=5)
            if r.status_code == 200:
                return True
        except Exception:
            pass
        if i < timeout - 1:
            time.sleep(1)
    return False


def run_phase(phase: int) -> dict:
    """
    Run a single phase and return results dict.

    Returns:
        {"phase": int, "passed": int, "failed": int, "error": str | None}
    """
    result = {"phase": phase, "passed": 0, "failed": 0, "error": None}

    try:
        if phase == 0:
            from tests.test_phase0_harness import run_phase0_harness, results as p0_results
            failed = run_phase0_harness()
            if failed == -1:
                result["error"] = "Backend not reachable"
                return result
            total = len(p0_results)
            result["passed"] = total - failed
            result["failed"] = failed

        elif phase == 1:
            from tests.test_phase1 import run_harness as run_p1, results as p1_results
            failed = run_p1()
            if failed == -1:
                result["error"] = "Backend not reachable"
                return result
            total = len(p1_results)
            result["passed"] = total - failed
            result["failed"] = failed

        elif phase == 2:
            from tests.test_phase2 import run_harness as run_p2, results as p2_results
            failed = run_p2()
            if failed == -1:
                result["error"] = "Backend not reachable"
                return result
            total = len(p2_results)
            result["passed"] = total - failed
            result["failed"] = failed

        else:
            result["error"] = f"Unknown phase: {phase}"

    except Exception as e:
        result["error"] = str(e)

    return result


def main():
    parser = argparse.ArgumentParser(
        description="Run AOS-DCL test harness (all phases or a single phase)."
    )
    parser.add_argument(
        "--phase",
        type=int,
        choices=[0, 1, 2],
        default=None,
        help="Run only the specified phase (0, 1, or 2). Default: run all.",
    )
    parser.add_argument(
        "--base-url",
        type=str,
        default=None,
        help="Backend base URL (default: http://localhost:8004).",
    )
    args = parser.parse_args()

    # Set base URL via environment so individual harnesses pick it up
    if args.base_url:
        os.environ["DCL_BASE_URL"] = args.base_url
    base_url = os.environ.get("DCL_BASE_URL", "http://localhost:8004")

    # ── Upfront connectivity check ──────────────────────────────────────
    print(f"\nChecking backend at {base_url} ...")
    if not check_backend(base_url):
        print(
            f"\n{'=' * 60}\n"
            f"  ERROR: Backend not reachable at {base_url}\n"
            f"{'=' * 60}\n"
            f"\n"
            f"  Make sure the DCL backend is running:\n"
            f"    python run_backend.py\n"
            f"\n"
            f"  Or specify a different URL:\n"
            f"    python -m tests.run_all --base-url http://host:port\n"
        )
        sys.exit(1)
    print(f"Backend is up at {base_url}\n")

    # ── Determine which phases to run ───────────────────────────────────
    phases = [args.phase] if args.phase is not None else [0, 1, 2]

    # ── Run phases ──────────────────────────────────────────────────────
    phase_results = []
    for phase in phases:
        result = run_phase(phase)
        phase_results.append(result)

    # ── Summary table ───────────────────────────────────────────────────
    total_passed = 0
    total_failed = 0
    any_error = False

    print(f"\n{'=' * 60}")
    print(f"  SUMMARY")
    print(f"{'=' * 60}")
    print(f"  {'Phase':<10} {'Passed':>8} {'Failed':>8} {'Status':>10}")
    print(f"  {'-' * 40}")

    for r in phase_results:
        if r["error"]:
            status = "ERROR"
            any_error = True
            print(f"  Phase {r['phase']:<6} {'—':>8} {'—':>8} {status:>10}")
            print(f"    Error: {r['error']}")
        else:
            status = "PASS" if r["failed"] == 0 else "FAIL"
            if r["failed"] > 0:
                any_error = True
            total_passed += r["passed"]
            total_failed += r["failed"]
            print(f"  Phase {r['phase']:<6} {r['passed']:>8} {r['failed']:>8} {status:>10}")

    print(f"  {'-' * 40}")
    print(f"  {'TOTAL':<10} {total_passed:>8} {total_failed:>8}")
    print(f"{'=' * 60}")

    all_ok = not any_error and total_failed == 0
    if all_ok:
        print(f"  ALL PHASES PASSED")
    else:
        print(f"  SOME TESTS FAILED — SEE ABOVE")
    print(f"{'=' * 60}\n")

    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
