#!/usr/bin/env python3
"""
NLQ Manual Smoke Test - Ad-hoc testing for NLQ quality debugging.

⚠️  NOTE: This is a MANUAL smoke test with hardcoded test cases.
    For CI/regression testing, use the generative harness instead:

    python -m backend.tests.nlq_harness --suite all

    The generative harness generates tests from the definitions registry
    and is the authoritative test suite for NLQ quality.

This script runs a curated set of test queries against DCL for manual debugging.
It evaluates:
1. Intent matching - did we match the right definition?
2. Summary quality - is the answer meaningful (not generic)?
3. Parameter extraction - did we extract limit, order_by correctly?
4. Data presence - did we get data back?

Usage:
    python tools/nlq_manual_smoke_test.py [--fix] [--verbose] [--direct]

Modes:
- Default: Hit DCL API at localhost:8000
- --direct: Import modules directly (no server needed)
"""
import sys
import os
from dataclasses import dataclass
from typing import Optional
from enum import Enum

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# DCL API base URL
BASE_URL = "http://localhost:8000"


class TestStatus(Enum):
    PASS = "PASS"
    FAIL = "FAIL"
    WARN = "WARN"


@dataclass
class TestCase:
    """A single NLQ test case."""
    question: str
    expected_definition: str  # e.g., "infra.slo_attainment"
    expected_limit: Optional[int] = None  # e.g., 5 for "top 5"
    min_rows: int = 0  # Expect at least this many rows (0 = don't check)
    max_rows: Optional[int] = None  # Expect at most this many rows
    summary_must_contain: list[str] = None  # Summary must contain these strings
    summary_must_not_contain: list[str] = None  # Summary must NOT contain these (generic answers)

    def __post_init__(self):
        if self.summary_must_contain is None:
            self.summary_must_contain = []
        if self.summary_must_not_contain is None:
            self.summary_must_not_contain = ["Retrieved", "records."]  # Generic answers = fail


@dataclass
class TestResult:
    """Result of running a test case."""
    test_case: TestCase
    status: TestStatus
    definition_matched: str
    confidence: float
    row_count: int
    summary: str
    errors: list[str]
    warnings: list[str]
    extracted_limit: Optional[int] = None


# =============================================================================
# Test Cases - Define expected behaviors
# =============================================================================

TEST_CASES = [
    # ARR / Revenue
    TestCase(
        question="What is our current ARR?",
        expected_definition="finops.arr",
        summary_must_contain=["ARR", "$"],
        summary_must_not_contain=[],
    ),

    # Burn Rate
    TestCase(
        question="What is our current burn rate?",
        expected_definition="finops.burn_rate",
        summary_must_contain=["burn", "$"],
        summary_must_not_contain=[],
    ),

    # Top Customers with limit
    TestCase(
        question="Show me top 5 customers by revenue",
        expected_definition="crm.top_customers",
        expected_limit=5,
        max_rows=5,
        summary_must_contain=["customer", "$"],
        summary_must_not_contain=[],
    ),

    TestCase(
        question="Who are our top 3 customers?",
        expected_definition="crm.top_customers",
        expected_limit=3,
        max_rows=3,
        summary_must_contain=["customer"],
        summary_must_not_contain=[],
    ),

    # SLO Attainment
    TestCase(
        question="How is our SLO attainment trending?",
        expected_definition="infra.slo_attainment",
        summary_must_contain=["SLO", "passing"],
        summary_must_not_contain=[],
    ),

    TestCase(
        question="What is our current SLO performance?",
        expected_definition="infra.slo_attainment",
        summary_must_contain=["SLO"],
        summary_must_not_contain=[],
    ),

    TestCase(
        question="Are we meeting our SLAs?",
        expected_definition="infra.slo_attainment",
        summary_must_contain=["SLO"],
        summary_must_not_contain=[],
    ),

    # DORA Metrics
    TestCase(
        question="What is our deployment frequency?",
        expected_definition="infra.deploy_frequency",
        summary_must_contain=["deploy"],
        summary_must_not_contain=[],
    ),

    TestCase(
        question="How often are we deploying?",
        expected_definition="infra.deploy_frequency",
        summary_must_contain=["deploy"],
        summary_must_not_contain=[],
    ),

    TestCase(
        question="What is our MTTR?",
        expected_definition="infra.mttr",
        summary_must_contain=["recovery", "minutes"],
        summary_must_not_contain=[],
    ),

    TestCase(
        question="What is our mean time to recovery?",
        expected_definition="infra.mttr",
        summary_must_contain=["recovery"],
        summary_must_not_contain=[],
    ),

    # Incidents
    TestCase(
        question="Show me recent incidents",
        expected_definition="infra.incidents",
        summary_must_contain=["incident"],
        summary_must_not_contain=[],
    ),

    TestCase(
        question="How many sev1 incidents did we have?",
        expected_definition="infra.incidents",
        summary_must_contain=["incident", "sev1"],
        summary_must_not_contain=[],
    ),

    # Zombie Resources
    TestCase(
        question="Show me zombie resources",
        expected_definition="aod.zombies_overview",
        summary_must_contain=["zombie", "idle"],
        summary_must_not_contain=[],
    ),

    TestCase(
        question="What idle resources are costing us money?",
        expected_definition="aod.zombies_overview",
        summary_must_contain=["idle", "zombie"],
        summary_must_not_contain=[],
    ),

    # Pipeline
    TestCase(
        question="What does our sales pipeline look like?",
        expected_definition="crm.pipeline",
        summary_must_contain=["pipeline", "deal"],
        summary_must_not_contain=[],
    ),

    # Cloud Spend
    TestCase(
        question="What is our total cloud spend?",
        expected_definition="finops.saas_spend",
        summary_must_contain=["spend", "$"],
        summary_must_not_contain=[],
    ),
]


# =============================================================================
# Test Runner
# =============================================================================

# Direct mode modules (loaded on demand)
_direct_modules_loaded = False
_match_question_to_definition = None
_extract_params = None
_apply_limit_clamp = None


def _load_direct_modules():
    """Load modules for direct testing (no API server needed)."""
    global _direct_modules_loaded, _match_question_to_definition, _extract_params, _apply_limit_clamp

    if _direct_modules_loaded:
        return

    # Import from standalone intent_matcher module (minimal dependencies)
    from backend.nlq.intent_matcher import match_question_to_definition as match_fn
    from backend.nlq.param_extractor import extract_params as extract_fn, apply_limit_clamp as clamp_fn

    _match_question_to_definition = match_fn
    _extract_params = extract_fn
    _apply_limit_clamp = clamp_fn
    _direct_modules_loaded = True


def run_nlq_ask_direct(question: str) -> dict:
    """Run NLQ matching directly without API."""
    _load_direct_modules()

    try:
        # Match question to definition
        definition_id, confidence, matched_keywords = _match_question_to_definition(question)

        # Extract params
        exec_args = _extract_params(question)
        if exec_args.limit:
            exec_args.limit = _apply_limit_clamp(exec_args.limit, max_limit=100)

        return {
            "definition_id": definition_id,
            "confidence_score": confidence,
            "matched_keywords": matched_keywords,
            "data": [],  # No actual execution in direct mode
            "summary": {"answer": f"[Direct mode - matched to {definition_id}]"},
            "execution_args": exec_args.to_dict(),
        }
    except Exception as e:
        return {"error": str(e)}


def run_extract_params_direct(question: str) -> dict:
    """Extract params directly without API."""
    _load_direct_modules()

    try:
        exec_args = _extract_params(question)
        if exec_args.limit:
            exec_args.limit = _apply_limit_clamp(exec_args.limit, max_limit=100)

        return {
            "limit": exec_args.limit,
            "order_by": exec_args.order_by,
            "time_window": exec_args.time_window,
        }
    except Exception as e:
        return {"error": str(e)}


def run_nlq_ask(question: str, direct: bool = False) -> dict:
    """Call the /api/nlq/ask endpoint or run directly."""
    if direct:
        return run_nlq_ask_direct(question)

    try:
        import requests
        # Don't pass dataset_id - let server use its default (Farm if FARM_SCENARIO_ID is set)
        resp = requests.post(
            f"{BASE_URL}/api/nlq/ask",
            json={"question": question},
            timeout=30
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        if "Connection" in str(e) or "refused" in str(e).lower():
            return {"error": "Connection failed - is DCL running on port 8000? Use --direct mode."}
        return {"error": str(e)}


def run_extract_params(question: str, direct: bool = False) -> dict:
    """Call the /api/nlq/extract_params endpoint or run directly."""
    if direct:
        return run_extract_params_direct(question)

    try:
        import requests
        resp = requests.post(
            f"{BASE_URL}/api/nlq/extract_params",
            json={"question": question},
            timeout=10
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        return {"error": str(e)}


def evaluate_test(tc: TestCase, ask_result: dict, params_result: dict, direct: bool = False) -> TestResult:
    """Evaluate a test case against actual results."""
    errors = []
    warnings = []

    # Check for API errors
    if "error" in ask_result:
        return TestResult(
            test_case=tc,
            status=TestStatus.FAIL,
            definition_matched="",
            confidence=0.0,
            row_count=0,
            summary="",
            errors=[f"API error: {ask_result['error']}"],
            warnings=[],
        )

    definition_matched = ask_result.get("definition_id", "")
    confidence = ask_result.get("confidence_score", 0.0)
    data = ask_result.get("data", [])
    row_count = len(data)
    summary_obj = ask_result.get("summary")
    summary = summary_obj.get("answer", "") if isinstance(summary_obj, dict) else str(summary_obj or "")
    extracted_limit = params_result.get("limit") if params_result else None

    # Check definition match (always check)
    if definition_matched != tc.expected_definition:
        errors.append(f"Definition mismatch: expected '{tc.expected_definition}', got '{definition_matched}'")

    # Check confidence
    if confidence < 0.3:
        warnings.append(f"Low confidence: {confidence:.2f}")

    # Check limit extraction (always check)
    if tc.expected_limit:
        if extracted_limit != tc.expected_limit:
            errors.append(f"Limit mismatch: expected {tc.expected_limit}, extracted {extracted_limit}")

    # Skip row count and summary checks in direct mode (no actual execution)
    if not direct:
        # Check row count
        if tc.min_rows > 0 and row_count < tc.min_rows:
            errors.append(f"Too few rows: expected >= {tc.min_rows}, got {row_count}")

        if tc.max_rows and row_count > tc.max_rows:
            errors.append(f"Too many rows: expected <= {tc.max_rows}, got {row_count}")

        # Check summary quality - must contain
        for phrase in tc.summary_must_contain:
            if phrase.lower() not in summary.lower():
                errors.append(f"Summary missing required phrase: '{phrase}'")

        # Check summary quality - must not contain (generic answers)
        for phrase in tc.summary_must_not_contain:
            if phrase.lower() in summary.lower():
                errors.append(f"Summary contains generic phrase: '{phrase}' - answer not meaningful")

    # Determine status
    if errors:
        status = TestStatus.FAIL
    elif warnings:
        status = TestStatus.WARN
    else:
        status = TestStatus.PASS

    return TestResult(
        test_case=tc,
        status=status,
        definition_matched=definition_matched,
        confidence=confidence,
        row_count=row_count,
        summary=summary,
        errors=errors,
        warnings=warnings,
        extracted_limit=extracted_limit,
    )


def run_all_tests(verbose: bool = False, direct: bool = False) -> list[TestResult]:
    """Run all test cases and return results."""
    results = []

    print("\n" + "=" * 70)
    print("NLQ TEST HARNESS" + (" (DIRECT MODE)" if direct else ""))
    print("=" * 70 + "\n")

    if direct:
        print("Running in direct mode - testing intent matching and param extraction only.\n")

    for i, tc in enumerate(TEST_CASES, 1):
        print(f"[{i}/{len(TEST_CASES)}] Testing: {tc.question[:50]}...")

        # Run both endpoints
        ask_result = run_nlq_ask(tc.question, direct=direct)
        params_result = run_extract_params(tc.question, direct=direct)

        # Evaluate
        result = evaluate_test(tc, ask_result, params_result, direct=direct)
        results.append(result)

        # Print status
        status_icon = {
            TestStatus.PASS: "✓",
            TestStatus.FAIL: "✗",
            TestStatus.WARN: "⚠",
        }[result.status]

        status_color = {
            TestStatus.PASS: "\033[92m",  # Green
            TestStatus.FAIL: "\033[91m",  # Red
            TestStatus.WARN: "\033[93m",  # Yellow
        }[result.status]

        print(f"  {status_color}{status_icon} {result.status.value}\033[0m", end="")
        print(f" | Matched: {result.definition_matched} ({result.confidence:.0%})")

        if verbose or result.status != TestStatus.PASS:
            print(f"     Summary: {result.summary[:80]}...")
            if result.extracted_limit:
                print(f"     Extracted limit: {result.extracted_limit}")
            for err in result.errors:
                print(f"     \033[91mERROR: {err}\033[0m")
            for warn in result.warnings:
                print(f"     \033[93mWARN: {warn}\033[0m")

        print()

    return results


def print_summary(results: list[TestResult]):
    """Print test summary."""
    passed = sum(1 for r in results if r.status == TestStatus.PASS)
    failed = sum(1 for r in results if r.status == TestStatus.FAIL)
    warned = sum(1 for r in results if r.status == TestStatus.WARN)
    total = len(results)

    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"\033[92mPASSED: {passed}/{total}\033[0m")
    print(f"\033[91mFAILED: {failed}/{total}\033[0m")
    print(f"\033[93mWARNED: {warned}/{total}\033[0m")

    if failed > 0:
        print("\n\033[91mFailed Tests:\033[0m")
        for r in results:
            if r.status == TestStatus.FAIL:
                print(f"  - {r.test_case.question}")
                for err in r.errors:
                    print(f"    → {err}")

    # Calculate pass rate
    pass_rate = (passed / total) * 100 if total > 0 else 0
    print(f"\nPass Rate: {pass_rate:.0f}%")

    if pass_rate >= 90:
        print("\033[92m✓ Target achieved: 90%+ intent understanding\033[0m")
    else:
        print(f"\033[93m⚠ Target not met: need {90 - pass_rate:.0f}% more to reach 90%\033[0m")

    return pass_rate


def generate_fix_report(results: list[TestResult]) -> str:
    """Generate a report of what needs fixing."""
    report = []

    # Group failures by type
    definition_mismatches = []
    limit_issues = []
    summary_issues = []
    no_data_issues = []

    for r in results:
        if r.status != TestStatus.FAIL:
            continue

        for err in r.errors:
            if "Definition mismatch" in err:
                definition_mismatches.append((r.test_case.question, r.test_case.expected_definition, r.definition_matched))
            elif "Limit mismatch" in err:
                limit_issues.append((r.test_case.question, r.test_case.expected_limit, r.extracted_limit))
            elif "Summary" in err:
                summary_issues.append((r.test_case.question, r.summary, err))
            elif "Too few rows" in err:
                no_data_issues.append((r.test_case.question, r.test_case.expected_definition))

    if definition_mismatches:
        report.append("\n## Definition Matching Issues")
        report.append("These questions matched the wrong definition:")
        for q, expected, got in definition_mismatches:
            report.append(f"  - \"{q}\"")
            report.append(f"    Expected: {expected}, Got: {got}")
            report.append(f"    FIX: Add keywords to {expected} definition")

    if limit_issues:
        report.append("\n## Limit Extraction Issues")
        report.append("These questions didn't extract the correct limit:")
        for q, expected, got in limit_issues:
            report.append(f"  - \"{q}\"")
            report.append(f"    Expected limit: {expected}, Extracted: {got}")
            report.append(f"    FIX: Improve limit extraction patterns in param_extractor.py")

    if summary_issues:
        report.append("\n## Summary Quality Issues")
        report.append("These queries returned generic/unhelpful summaries:")
        for q, summary, err in summary_issues:
            report.append(f"  - \"{q}\"")
            report.append(f"    Summary: {summary[:60]}...")
            report.append(f"    Issue: {err}")
            report.append(f"    FIX: Add specific summary handler in executor.py _compute_summary()")

    if no_data_issues:
        report.append("\n## Missing Data Issues")
        report.append("These queries returned no data:")
        for q, defn in no_data_issues:
            report.append(f"  - \"{q}\" ({defn})")
            report.append(f"    FIX: Add demo data for {defn} in dcl/demo/datasets/demo9/")

    return "\n".join(report)


if __name__ == "__main__":
    verbose = "--verbose" in sys.argv or "-v" in sys.argv
    direct = "--direct" in sys.argv or "-d" in sys.argv

    # Run tests
    results = run_all_tests(verbose=verbose, direct=direct)

    # Print summary
    pass_rate = print_summary(results)

    # Generate fix report if there are failures
    if any(r.status == TestStatus.FAIL for r in results):
        report = generate_fix_report(results)
        print(report)

        # Save report to file
        with open("/home/user/AOS-DCLv2/backend/tests/nlq_fix_report.txt", "w") as f:
            f.write(report)
        print("\n[Report saved to backend/tests/nlq_fix_report.txt]")

    # Exit with appropriate code
    sys.exit(0 if pass_rate >= 90 else 1)
