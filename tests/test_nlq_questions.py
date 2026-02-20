"""
Test harness for nlq_test_questions.json - 100 NLQ test questions.

Maps question categories to expected BLL definitions and validates routing.
"""
import json
import sys
from pathlib import Path
from typing import Dict, List, Set
from dataclasses import dataclass

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from backend.nlq.intent_matcher import match_question_with_details


# =============================================================================
# CATEGORY → DEFINITION MAPPING
# Maps question categories to acceptable BLL definition IDs
# =============================================================================
CATEGORY_TO_DEFINITIONS: Dict[str, Set[str]] = {
    # Revenue metrics - scalar totals
    "revenue_total": {"finops.total_revenue"},
    "revenue_period": {"finops.total_revenue"},
    "revenue_monthly": {"finops.total_revenue"},  # MoM queries
    "revenue_trend": {"finops.total_revenue", "finops.top_vendor_deltas_mom"},

    # Customer metrics
    "top_customers": {"crm.top_customers"},
    "customer_revenue": {"crm.top_customers", "finops.total_revenue"},
    "customer_concentration": {"crm.top_customers"},

    # Vendor/spend metrics
    "vendor_spend": {"finops.saas_spend"},
    "vendor_specific": {"finops.saas_spend"},
    "vendor_invoices": {"finops.saas_spend"},
    "vendor_average": {"finops.saas_spend"},

    # Resource health metrics
    "resource_health": {"aod.zombies_overview", "aod.identity_gap_financially_anchored"},
    "resource_zombie": {"aod.zombies_overview"},
    "resource_orphan": {"aod.identity_gap_financially_anchored"},
    "resource_ratio": {"aod.zombies_overview", "aod.identity_gap_financially_anchored"},

    # Complex queries - multiple valid definitions (including delta/trend definitions)
    "comparison": {"finops.total_revenue", "finops.saas_spend", "aod.zombies_overview",
                   "aod.identity_gap_financially_anchored", "crm.top_customers",
                   "finops.top_vendor_deltas_mom"},  # Growth comparisons
    "aggregation": {"finops.total_revenue", "finops.saas_spend", "crm.top_customers"},
    "ranking": {"crm.top_customers", "finops.saas_spend", "finops.total_revenue"},
    "filtering": {"crm.top_customers", "finops.saas_spend", "finops.total_revenue",
                  "finops.top_vendor_deltas_mom"},  # Growth filtering
    "yes_no": {"finops.total_revenue", "finops.saas_spend", "aod.zombies_overview",
               "aod.identity_gap_financially_anchored", "crm.top_customers",
               "finops.top_vendor_deltas_mom"},  # Biggest vendor queries
    "time_series": {"finops.total_revenue", "finops.top_vendor_deltas_mom"},
    "derived": {"finops.total_revenue", "finops.saas_spend", "crm.top_customers"},
    "entity_lookup": {"crm.top_customers", "finops.saas_spend", "finops.top_vendor_deltas_mom"},
}


@dataclass
class TestResult:
    """Result of a single test case."""
    id: int
    category: str
    question: str
    expected_definitions: Set[str]
    actual_definition: str
    confidence: float
    is_ambiguous: bool
    passed: bool
    reason: str


def run_test(question_data: dict) -> TestResult:
    """Run a single test case."""
    qid = question_data["id"]
    category = question_data["category"]
    question = question_data["question"]

    # Get expected definitions for this category
    expected = CATEGORY_TO_DEFINITIONS.get(category, set())

    # Run intent matcher
    result = match_question_with_details(question)
    actual = result.best_match
    confidence = result.confidence
    is_ambiguous = result.is_ambiguous

    # Check if passed
    if not expected:
        # No mapping defined - consider it a pass if we get any result
        passed = True
        reason = f"No expected mapping, got {actual}"
    elif actual in expected:
        passed = True
        reason = f"Matched {actual}"
    elif actual == "AMBIGUOUS" or actual == "UNKNOWN":
        passed = False
        reason = f"No match (expected one of {expected})"
    else:
        passed = False
        reason = f"Wrong: got {actual}, expected one of {expected}"

    return TestResult(
        id=qid,
        category=category,
        question=question,
        expected_definitions=expected,
        actual_definition=actual,
        confidence=confidence,
        is_ambiguous=is_ambiguous,
        passed=passed,
        reason=reason,
    )


def run_all_tests(verbose: bool = False) -> tuple:
    """Run all 100 test questions."""
    # Load questions
    questions_path = Path(__file__).parent.parent / "nlq_docs" / "nlq_test_questions.json"
    with open(questions_path) as f:
        data = json.load(f)

    questions = data["questions"]
    results = []
    failures = []

    for q in questions:
        result = run_test(q)
        results.append(result)
        if not result.passed:
            failures.append(result)

        if verbose:
            status = "✓" if result.passed else "✗"
            print(f"{status} [{result.id:3d}] {result.question[:50]}...")
            if not result.passed:
                print(f"      → {result.reason}")

    return results, failures


def print_summary(results: list, failures: list):
    """Print test summary."""
    total = len(results)
    passed = total - len(failures)
    pass_rate = (passed / total * 100) if total > 0 else 0

    print("\n" + "=" * 70)
    print("NLQ TEST QUESTIONS RESULTS (100 questions)")
    print("=" * 70)
    print(f"\nOverall: {passed}/{total} passed ({pass_rate:.1f}%)")

    # Group failures by category
    if failures:
        by_category = {}
        for f in failures:
            if f.category not in by_category:
                by_category[f.category] = []
            by_category[f.category].append(f)

        print(f"\nFailures by category:")
        for cat, fails in sorted(by_category.items()):
            print(f"  {cat}: {len(fails)} failures")

        print(f"\n{'=' * 70}")
        print(f"FAILURES ({len(failures)} total):")
        print("=" * 70)
        for i, f in enumerate(failures[:30]):
            print(f"\n{i+1}. [{f.id}] {f.question}")
            print(f"   Category: {f.category}")
            print(f"   Got: {f.actual_definition} (conf={f.confidence:.2f})")
            print(f"   Expected: {f.expected_definitions}")
            print(f"   Reason: {f.reason}")

        if len(failures) > 30:
            print(f"\n... and {len(failures) - 30} more failures")


if __name__ == "__main__":
    verbose = "-v" in sys.argv or "--verbose" in sys.argv
    results, failures = run_all_tests(verbose=verbose)
    print_summary(results, failures)
    sys.exit(0 if len(failures) == 0 else 1)
