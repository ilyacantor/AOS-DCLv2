"""
E2E Integration Test — NLQ → DCL Graph Traversal

Tests the full chain:
  Natural language → parse intent → graph resolve → provenance + confidence

Can be run as:
  python -m tests.test_e2e_integration
  pytest tests/test_e2e_integration.py -v
"""

import sys
import time
from pathlib import Path
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

# Ensure project root is on path
sys.path.insert(0, str(Path(__file__).parent.parent))

from backend.nlq_client import parse_intent, resolve_question
from backend.domain import QueryIntent, FilterClause


# ── Test case definitions ─────────────────────────────────────────────


@dataclass
class Expected:
    can_answer: bool
    concepts_found: List[str] = field(default_factory=list)
    dimensions_used: List[str] = field(default_factory=list)
    min_confidence: float = 0.0
    filters_resolved: Optional[List[Dict[str, Any]]] = None
    join_path_exists: bool = False
    reason_contains: Optional[str] = None
    primary_system: Optional[str] = None
    provenance_contains: Optional[str] = None
    management_overlay_used: bool = False
    warnings_contain: Optional[str] = None


@dataclass
class TestCase:
    id: int
    question: str
    expected: Expected
    setup: Optional[str] = None


TEST_CASES: List[TestCase] = [
    # 1. Basic concept lookup
    TestCase(
        id=1,
        question="What is total revenue?",
        expected=Expected(
            can_answer=True,
            concepts_found=["revenue"],
            min_confidence=0.7,
        ),
    ),
    # 2. Concept + single dimension
    TestCase(
        id=2,
        question="Show me revenue by region",
        expected=Expected(
            can_answer=True,
            concepts_found=["revenue"],
            dimensions_used=["region"],
            min_confidence=0.7,
        ),
    ),
    # 3. Cross-system join
    TestCase(
        id=3,
        question="What is revenue by cost center for the Cloud division?",
        expected=Expected(
            can_answer=True,
            concepts_found=["revenue"],
            dimensions_used=["cost_center"],
            filters_resolved=[
                {"dimension": "division", "value": "Cloud", "resolved_to": ["Cloud East", "Cloud West"]},
            ],
            join_path_exists=True,
            min_confidence=0.5,
        ),
    ),
    # 4. Invalid dimension combination
    TestCase(
        id=4,
        question="What is sprint velocity by profit center?",
        expected=Expected(
            can_answer=False,
            reason_contains="cannot be sliced",
        ),
    ),
    # 5. Hierarchy drill-down
    TestCase(
        id=5,
        question="Show me headcount for Engineering cost centers",
        expected=Expected(
            can_answer=True,
            concepts_found=["headcount"],
            dimensions_used=["cost_center"],
            filters_resolved=[
                {"dimension": "cost_center", "value": "Engineering", "resolved_to": ["Cloud Engineering", "Platform Engineering"]},
            ],
        ),
    ),
    # 6. Management overlay
    TestCase(
        id=6,
        question="Revenue by board segment",
        expected=Expected(
            can_answer=True,
            dimensions_used=[],  # board_segment may not be in allowed dims
            management_overlay_used=True,
        ),
    ),
    # 7. Multiple concepts
    TestCase(
        id=7,
        question="Compare revenue and headcount by department",
        expected=Expected(
            can_answer=True,
            concepts_found=["revenue", "headcount"],
            dimensions_used=["department"],
        ),
    ),
    # 8. SOR authority
    TestCase(
        id=8,
        question="Show me employees by department",
        expected=Expected(
            can_answer=True,
            primary_system="Workday",
            provenance_contains="authoritative",
        ),
    ),
    # 9. Graceful degradation
    TestCase(
        id=9,
        question="Revenue by cost center",
        expected=Expected(
            can_answer=True,
            min_confidence=0.5,
        ),
    ),
    # 10. Unknown concept
    TestCase(
        id=10,
        question="What is the florbatz by region?",
        expected=Expected(
            can_answer=False,
            reason_contains="not recognized",
        ),
    ),
]


# ── Test runner ───────────────────────────────────────────────────────


@dataclass
class TestResult:
    test_id: int
    question: str
    passed: bool
    failures: List[str] = field(default_factory=list)
    confidence: float = 0.0
    provenance_chain: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    resolution_path: str = ""
    elapsed_ms: float = 0.0


def run_test(tc: TestCase) -> TestResult:
    """Run a single E2E test case."""
    failures: List[str] = []

    start = time.perf_counter()
    result = resolve_question(tc.question)
    elapsed = (time.perf_counter() - start) * 1000

    res = result["resolution"]
    intent = result["intent"]

    # Check can_answer
    if res["can_answer"] != tc.expected.can_answer:
        failures.append(
            f"can_answer: expected {tc.expected.can_answer}, got {res['can_answer']}"
            + (f" (reason: {res.get('reason', 'N/A')})" if not res["can_answer"] else "")
        )

    # Check concepts_found
    if tc.expected.concepts_found:
        for concept in tc.expected.concepts_found:
            if concept not in res.get("concepts_found", []):
                failures.append(f"concept '{concept}' not found in {res.get('concepts_found', [])}")

    # Check dimensions_used
    if tc.expected.dimensions_used:
        for dim in tc.expected.dimensions_used:
            if dim not in res.get("dimensions_used", []):
                failures.append(f"dimension '{dim}' not in used dims {res.get('dimensions_used', [])}")

    # Check min_confidence
    if tc.expected.can_answer and tc.expected.min_confidence > 0:
        if res.get("confidence", 0) < tc.expected.min_confidence:
            failures.append(
                f"confidence {res.get('confidence', 0):.2f} < min {tc.expected.min_confidence}"
            )

    # Check filters_resolved
    if tc.expected.filters_resolved:
        actual_filters = res.get("filters_resolved", [])
        for exp_filter in tc.expected.filters_resolved:
            matched = False
            for af in actual_filters:
                if (af.get("dimension", "").lower() == exp_filter["dimension"].lower()
                        and af.get("value", "").lower() == exp_filter["value"].lower()):
                    # Check resolved_to
                    actual_resolved = set(af.get("resolved_to", []))
                    expected_resolved = set(exp_filter["resolved_to"])
                    if expected_resolved.issubset(actual_resolved):
                        matched = True
                    else:
                        failures.append(
                            f"filter {exp_filter['dimension']}={exp_filter['value']}: "
                            f"resolved_to {actual_resolved} missing expected {expected_resolved}"
                        )
                        matched = True  # Don't double-report
                    break
            if not matched:
                failures.append(
                    f"filter {exp_filter['dimension']}={exp_filter['value']} not found in resolved filters"
                )

    # Check join_path_exists
    if tc.expected.join_path_exists:
        if not res.get("join_paths"):
            failures.append("expected join paths but none found")

    # Check reason_contains
    if tc.expected.reason_contains:
        reason = res.get("reason", "") or ""
        all_text = reason + " ".join(res.get("warnings", []))
        if tc.expected.reason_contains.lower() not in all_text.lower():
            failures.append(
                f"reason/warnings don't contain '{tc.expected.reason_contains}', "
                f"got reason='{reason}', warnings={res.get('warnings', [])}"
            )

    # Check primary_system
    if tc.expected.primary_system:
        actual_primary = res.get("primary_system", "")
        if tc.expected.primary_system.lower() not in (actual_primary or "").lower():
            failures.append(
                f"primary_system: expected '{tc.expected.primary_system}', got '{actual_primary}'"
            )

    # Check management_overlay_used
    if tc.expected.management_overlay_used:
        if not res.get("management_overlay_used", False):
            failures.append("expected management overlay to be used")

    # Check warnings_contain
    if tc.expected.warnings_contain:
        all_warnings = " ".join(res.get("warnings", []))
        if tc.expected.warnings_contain.lower() not in all_warnings.lower():
            failures.append(
                f"warnings don't contain '{tc.expected.warnings_contain}', "
                f"got: {res.get('warnings', [])}"
            )

    # Build provenance chain summary
    provenance_chain: List[str] = []
    for step in res.get("provenance", []):
        provenance_chain.append(
            f"{step['concept']} <- {step['source_system']}.{step['table']}.{step['field']} "
            f"(conf={step['confidence']:.2f}, SOR={step['is_sor']})"
        )

    return TestResult(
        test_id=tc.id,
        question=tc.question,
        passed=len(failures) == 0,
        failures=failures,
        confidence=res.get("confidence", 0),
        provenance_chain=provenance_chain,
        warnings=res.get("warnings", []),
        resolution_path=result.get("resolution_path", ""),
        elapsed_ms=elapsed,
    )


def run_all_tests() -> List[TestResult]:
    """Run all E2E integration test cases."""
    results: List[TestResult] = []
    for tc in TEST_CASES:
        result = run_test(tc)
        results.append(result)
    return results


def print_report(results: List[TestResult]) -> None:
    """Print a detailed test report."""
    passed = sum(1 for r in results if r.passed)
    total = len(results)

    print("\n" + "=" * 78)
    print("E2E INTEGRATION TEST REPORT -- NLQ -> DCL Graph Traversal")
    print("=" * 78)
    print(f"\nOverall: {passed}/{total} passed ({passed/total*100:.0f}%)\n")

    for r in results:
        status = "PASS" if r.passed else "FAIL"
        print(f"  [{status}] #{r.test_id}: {r.question}")
        print(f"         Path: {r.resolution_path} | Confidence: {r.confidence:.2f} | Time: {r.elapsed_ms:.1f}ms")

        if r.provenance_chain:
            print(f"         Provenance:")
            for step in r.provenance_chain[:3]:
                print(f"           {step}")
            if len(r.provenance_chain) > 3:
                print(f"           ... and {len(r.provenance_chain) - 3} more")

        if r.warnings:
            print(f"         Warnings: {r.warnings}")

        if r.failures:
            print(f"         FAILURES:")
            for f in r.failures:
                print(f"           - {f}")

        print()

    print("=" * 78)
    if passed == total:
        print("ALL TESTS PASSED")
    else:
        print(f"{total - passed} TEST(S) FAILED")
    print("=" * 78)


# ── Pytest integration ────────────────────────────────────────────────

def test_e2e_all_queries():
    """Pytest entry point — runs all E2E tests and asserts each passes."""
    results = run_all_tests()
    failures = [r for r in results if not r.passed]
    if failures:
        msg_lines = [f"{len(failures)}/{len(results)} tests failed:"]
        for r in failures:
            msg_lines.append(f"  #{r.test_id} {r.question}: {r.failures}")
        raise AssertionError("\n".join(msg_lines))


if __name__ == "__main__":
    results = run_all_tests()
    print_report(results)
    sys.exit(0 if all(r.passed for r in results) else 1)
