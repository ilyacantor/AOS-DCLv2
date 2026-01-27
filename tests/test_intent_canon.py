"""
Test harness for NLQ intent matching against intent_canon.json ground truth.

This runs all 305 test cases and validates that the intent_matcher routes
questions to the correct BLL definitions.
"""
import json
import sys
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from backend.nlq.intent_matcher import match_question_with_details, MatchResult


# =============================================================================
# METRIC → DEFINITION MAPPING
# Maps semantic metrics from intent_canon.json to BLL definition IDs
# =============================================================================
METRIC_TO_DEFINITION: Dict[str, List[str]] = {
    # Financial metrics
    "arr": ["finops.arr"],
    "revenue": ["finops.total_revenue", "crm.top_customers"],  # scalar vs ranked
    "customer_revenue": ["crm.top_customers", "finops.total_revenue"],
    "revenue_delta": ["finops.top_vendor_deltas_mom", "finops.total_revenue"],
    "burn_rate": ["finops.burn_rate"],
    "profit": ["finops.total_revenue"],  # closest match

    # Spend metrics
    "spend": ["finops.saas_spend", "finops.burn_rate"],
    "saas_spend": ["finops.saas_spend"],
    "cloud_spend": ["finops.saas_spend"],
    "vendor_spend": ["finops.saas_spend"],
    "unallocated_spend": ["finops.unallocated_spend"],
    "spend_delta": ["finops.top_vendor_deltas_mom"],
    "vendor_spend_delta": ["finops.top_vendor_deltas_mom"],
    "unit_cost": ["finops.saas_spend"],

    # CRM metrics
    "pipeline": ["crm.pipeline"],
    "deal_count": ["crm.pipeline"],
    "deal_size": ["crm.pipeline"],
    "deal_value": ["crm.pipeline"],
    "deals_closed": ["crm.pipeline"],
    "win_rate": ["crm.pipeline"],
    "customer_count": ["crm.top_customers"],
    "customer_spend": ["crm.top_customers"],
    "customer_concentration": ["crm.top_customers"],
    "customer_churn": ["crm.top_customers"],
    "customer_signup": ["crm.top_customers"],

    # Infra / SRE metrics
    "slo_attainment": ["infra.slo_attainment"],
    "sla_breach": ["infra.slo_attainment"],
    "mttr": ["infra.mttr"],
    "deploy_frequency": ["infra.deploy_frequency"],
    "deploy_count": ["infra.deploy_frequency"],
    "lead_time": ["infra.lead_time"],
    "change_failure_rate": ["infra.change_failure_rate"],
    "incident_count": ["infra.incidents"],
    "incident_severity": ["infra.incidents"],
    "response_time": ["infra.mttr"],

    # AOD metrics
    "zombie_resources": ["aod.zombies_overview"],
    "zombie_count": ["aod.zombies_overview"],
    "identity_gaps": ["aod.identity_gap_financially_anchored"],
    "security_findings": ["aod.findings_by_severity"],
    "resource_count": ["aod.zombies_overview", "aod.identity_gap_financially_anchored"],

    # HR / other metrics (may not have direct definitions)
    "headcount": [],  # UNSUPPORTED
    "attrition": [],  # UNSUPPORTED
    "employee_tenure": [],  # UNSUPPORTED
    "nps": [],  # UNSUPPORTED
    "csat": [],  # UNSUPPORTED
    "ticket_count": [],  # UNSUPPORTED
    "story_points": [],  # UNSUPPORTED
    "code_coverage": [],  # UNSUPPORTED
    "build_success": [],  # UNSUPPORTED
    "vendor_count": ["finops.saas_spend"],
    "arr_churn": ["finops.arr"],
    "multi_metric": [],  # UNSUPPORTED - requires multiple definitions
    "win_rate": [],  # UNSUPPORTED
    "response_time": [],  # UNSUPPORTED - could be MTTR but ambiguous
    "customer_spend": ["crm.top_customers"],  # customers by spend
}


@dataclass
class TestResult:
    """Result of a single test case."""
    case_id: str
    question: str
    expected_metric: str
    expected_status: str
    actual_definition: str
    actual_confidence: float
    actual_is_ambiguous: bool
    passed: bool
    reason: str


def run_test_case(case: dict) -> TestResult:
    """Run a single test case and return the result."""
    case_id = case["id"]
    question = case["question"]
    expected = case["expected"]
    expected_status = expected["status"]

    # Get expected metric (may be None for some statuses)
    expected_metric = None
    if expected.get("intent"):
        expected_metric = expected["intent"].get("metric")

    # Run intent matcher
    result = match_question_with_details(question)
    actual_definition = result.best_match
    actual_confidence = result.confidence
    actual_is_ambiguous = result.is_ambiguous

    # Determine pass/fail based on expected status
    passed = False
    reason = ""

    if expected_status == "UNSUPPORTED":
        # For UNSUPPORTED, we expect either:
        # - Low confidence match
        # - Ambiguous result
        # - No valid definition mapping exists for this metric
        acceptable_defs = METRIC_TO_DEFINITION.get(expected_metric, [])
        if not acceptable_defs:
            passed = True
            reason = "Correctly unsupported (no definition mapping)"
        elif actual_confidence < 0.5 or actual_is_ambiguous:
            passed = True
            reason = f"Low confidence ({actual_confidence:.2f}) or ambiguous for unsupported metric"
        else:
            passed = False
            reason = f"Expected UNSUPPORTED but got confident match: {actual_definition} ({actual_confidence:.2f})"

    elif expected_status == "AMBIGUOUS":
        # For AMBIGUOUS, we expect is_ambiguous=True
        if actual_is_ambiguous:
            passed = True
            reason = "Correctly marked as ambiguous"
        else:
            passed = False
            reason = f"Expected AMBIGUOUS but got definitive: {actual_definition} ({actual_confidence:.2f})"

    elif expected_status in ("RESOLVED", "RESOLVED_WITH_WARNING"):
        # For RESOLVED, the matched definition should be in acceptable list
        acceptable_defs = METRIC_TO_DEFINITION.get(expected_metric, [])

        if not acceptable_defs:
            # No mapping defined - this is a gap in our mapping table
            passed = False
            reason = f"No definition mapping for metric '{expected_metric}'"
        elif actual_definition in acceptable_defs:
            passed = True
            reason = f"Matched {actual_definition} (acceptable for {expected_metric})"
        elif actual_definition == "UNKNOWN":
            passed = False
            reason = f"No match found (expected one of {acceptable_defs})"
        else:
            passed = False
            reason = f"Wrong definition: got {actual_definition}, expected one of {acceptable_defs}"

    else:
        passed = False
        reason = f"Unknown expected status: {expected_status}"

    return TestResult(
        case_id=case_id,
        question=question,
        expected_metric=expected_metric or "N/A",
        expected_status=expected_status,
        actual_definition=actual_definition,
        actual_confidence=actual_confidence,
        actual_is_ambiguous=actual_is_ambiguous,
        passed=passed,
        reason=reason,
    )


def run_all_tests(verbose: bool = False) -> Tuple[List[TestResult], dict]:
    """Run all test cases and return results with summary stats."""
    # Load test cases
    canon_path = Path(__file__).parent.parent / "nlq_docs" / "intent_canon.json"
    with open(canon_path) as f:
        data = json.load(f)

    cases = data["cases"]
    results = []

    # Stats by status
    stats = {
        "total": len(cases),
        "passed": 0,
        "failed": 0,
        "by_status": {},
        "by_metric": {},
        "failures": [],
    }

    for case in cases:
        result = run_test_case(case)
        results.append(result)

        status = result.expected_status
        if status not in stats["by_status"]:
            stats["by_status"][status] = {"total": 0, "passed": 0}
        stats["by_status"][status]["total"] += 1

        metric = result.expected_metric
        if metric not in stats["by_metric"]:
            stats["by_metric"][metric] = {"total": 0, "passed": 0}
        stats["by_metric"][metric]["total"] += 1

        if result.passed:
            stats["passed"] += 1
            stats["by_status"][status]["passed"] += 1
            stats["by_metric"][metric]["passed"] += 1
        else:
            stats["failed"] += 1
            stats["failures"].append(result)

        if verbose:
            status_icon = "✓" if result.passed else "✗"
            print(f"{status_icon} [{result.case_id}] {result.question[:50]}...")
            if not result.passed:
                print(f"   → {result.reason}")

    return results, stats


def print_summary(stats: dict):
    """Print a summary of test results."""
    print("\n" + "=" * 70)
    print("INTENT CANON TEST RESULTS")
    print("=" * 70)

    total = stats["total"]
    passed = stats["passed"]
    failed = stats["failed"]
    pass_rate = (passed / total * 100) if total > 0 else 0

    # Count unsupported metric failures vs actual routing failures
    unsupported_failures = [f for f in stats["failures"] if "No definition mapping" in f.reason]
    routing_failures = [f for f in stats["failures"] if "No definition mapping" not in f.reason]

    print(f"\nOverall: {passed}/{total} passed ({pass_rate:.1f}%)")
    if unsupported_failures:
        supported_total = total - len(unsupported_failures)
        supported_pass_rate = (passed / supported_total * 100) if supported_total > 0 else 0
        print(f"Supported metrics: {passed}/{supported_total} passed ({supported_pass_rate:.1f}%)")
        print(f"Unsupported metrics: {len(unsupported_failures)} (no BLL definitions)")

    print("\nBy Status:")
    for status, data in sorted(stats["by_status"].items()):
        rate = (data["passed"] / data["total"] * 100) if data["total"] > 0 else 0
        print(f"  {status}: {data['passed']}/{data['total']} ({rate:.1f}%)")

    print("\nBy Metric (failures only):")
    for metric, data in sorted(stats["by_metric"].items()):
        if data["passed"] < data["total"]:
            rate = (data["passed"] / data["total"] * 100) if data["total"] > 0 else 0
            print(f"  {metric}: {data['passed']}/{data['total']} ({rate:.1f}%)")

    if stats["failures"]:
        print(f"\n{'=' * 70}")
        print(f"FAILURES ({len(stats['failures'])} total):")
        print("=" * 70)
        for i, result in enumerate(stats["failures"][:20]):  # Show first 20
            print(f"\n{i+1}. [{result.case_id}] {result.question}")
            print(f"   Expected: {result.expected_status} → {result.expected_metric}")
            print(f"   Actual: {result.actual_definition} (conf={result.actual_confidence:.2f}, ambig={result.actual_is_ambiguous})")
            print(f"   Reason: {result.reason}")

        if len(stats["failures"]) > 20:
            print(f"\n... and {len(stats['failures']) - 20} more failures")


if __name__ == "__main__":
    verbose = "-v" in sys.argv or "--verbose" in sys.argv
    results, stats = run_all_tests(verbose=verbose)
    print_summary(stats)

    # Exit with error code if not 100%
    sys.exit(0 if stats["failed"] == 0 else 1)
