"""
Intent Evaluation Suite - Evaluates NLQ intent extraction against gold standard.

This module loads the gold intent canon and scores the current NLQ extraction
implementation against it using a well-defined rubric.

SCORING RUBRIC (100 points total):
- Time semantics correctness: 35
- Aggregation intent correctness: 20
- Metric correctness: 15
- Grouping correctness: 10
- Ranking / limit correctness: 10
- Restraint correctness (warn vs guess): 10

USAGE:
    python -m backend.tests.nlq_gold.intent_eval
"""
import os
import sys
import yaml
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path
from collections import defaultdict

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from backend.nlq.normalized_intent import (
    extract_normalized_intent,
    IntentResult,
    IntentStatus,
    NormalizedIntent,
    TimeMode,
    AggregationType,
    RankDirection,
)
from backend.nlq.intent_matcher import match_question_with_details


@dataclass
class CaseScore:
    """Score for a single test case."""
    case_id: str
    question: str
    total_score: float
    max_score: float = 100.0
    time_score: float = 0.0
    aggregation_score: float = 0.0
    metric_score: float = 0.0
    grouping_score: float = 0.0
    ranking_score: float = 0.0
    restraint_score: float = 0.0
    expected_status: str = ""
    actual_status: str = ""
    failures: List[str] = field(default_factory=list)
    expected: Dict[str, Any] = field(default_factory=dict)
    actual: Dict[str, Any] = field(default_factory=dict)


@dataclass
class EvalReport:
    """Full evaluation report."""
    total_cases: int
    total_score: float
    max_possible_score: float
    percentage: float
    case_scores: List[CaseScore]
    failure_buckets: Dict[str, int]
    worst_cases: List[CaseScore]
    primitive_scores: Dict[str, Tuple[float, float]]  # (actual, max)


def load_gold_cases(path: Optional[str] = None) -> List[Dict[str, Any]]:
    """Load gold test cases from YAML file."""
    if path is None:
        path = Path(__file__).parent / "intent_canon.yaml"

    with open(path, 'r') as f:
        data = yaml.safe_load(f)

    return data.get('cases', [])


def normalize_time_spec(time_dict: Dict[str, Any]) -> Tuple[str, str]:
    """Normalize time specification for comparison."""
    if not time_dict:
        return ("none", "current")

    mode = time_dict.get("mode", "none")
    spec = time_dict.get("spec", "current")

    return (mode, spec)


def compare_time(expected: Dict[str, Any], actual: Dict[str, Any]) -> float:
    """
    Compare time specifications.

    Returns score out of 35 points.
    - Mode match (calendar vs rolling vs none): 20 points
    - Spec match (specific time reference): 15 points
    """
    score = 0.0
    exp_mode, exp_spec = normalize_time_spec(expected)
    act_mode, act_spec = normalize_time_spec(actual)

    # Mode match
    if exp_mode == act_mode:
        score += 20.0
    elif exp_mode == "none" and act_mode == "none":
        score += 20.0
    elif (exp_mode in ("calendar", "rolling")) and (act_mode in ("calendar", "rolling")):
        # Partial credit for getting the right type of time reference
        score += 10.0

    # Spec match
    if exp_spec == act_spec:
        score += 15.0
    elif exp_spec.split(":")[0] == act_spec.split(":")[0]:
        # Same type of spec (e.g., both calendar_month)
        score += 7.5

    return score


def compare_aggregation(expected: str, actual: str) -> float:
    """
    Compare aggregation types.

    Returns score out of 20 points.
    """
    if expected == actual:
        return 20.0

    # Partial credit for related types
    related_groups = [
        {"total", "count", "inventory"},
        {"delta", "percent"},
        {"ranking"},
        {"trend", "health"},
    ]

    for group in related_groups:
        if expected in group and actual in group:
            return 10.0

    return 0.0


def compare_metric(expected: str, actual: str) -> float:
    """
    Compare metric names.

    Returns score out of 15 points.
    """
    if expected == actual:
        return 15.0

    # Partial credit for same domain
    domain_groups = {
        "finance": {"arr", "revenue", "spend", "saas_spend", "burn_rate",
                   "vendor_spend", "unallocated_spend", "cloud_spend",
                   "vendor_spend_delta", "revenue_delta", "spend_delta"},
        "crm": {"customer_revenue", "pipeline", "deal_value", "customer_count",
               "customer_concentration", "customer_churn", "deal_count"},
        "infra": {"slo_attainment", "deploy_frequency", "lead_time",
                 "change_failure_rate", "mttr", "incident_count"},
        "security": {"security_findings", "zombie_resources", "identity_gaps"},
    }

    for domain, metrics in domain_groups.items():
        if expected in metrics and actual in metrics:
            return 7.5

    return 0.0


def compare_grouping(expected: List[str], actual: List[str]) -> float:
    """
    Compare grouping dimensions.

    Returns score out of 10 points.
    """
    if not expected and not actual:
        return 10.0

    if set(expected) == set(actual):
        return 10.0

    # Partial credit for overlap
    if expected and actual:
        overlap = len(set(expected) & set(actual))
        total = len(set(expected) | set(actual))
        return 10.0 * (overlap / total)

    # One has groups, other doesn't
    return 0.0


def compare_ranking(
    expected_limit: Optional[int],
    actual_limit: Optional[int],
    expected_direction: Optional[str],
    actual_direction: Optional[str],
) -> float:
    """
    Compare ranking specifications.

    Returns score out of 10 points.
    - Limit correctness: 6 points
    - Direction correctness: 4 points
    """
    score = 0.0

    # Limit comparison
    if expected_limit == actual_limit:
        score += 6.0
    elif expected_limit is None and actual_limit is None:
        score += 6.0
    elif expected_limit is not None and actual_limit is not None:
        # Partial credit for being close
        ratio = min(expected_limit, actual_limit) / max(expected_limit, actual_limit)
        score += 6.0 * ratio

    # Direction comparison
    if expected_direction == actual_direction:
        score += 4.0
    elif expected_direction is None and actual_direction is None:
        score += 4.0

    return score


def compare_restraint(
    expected_status: str,
    actual_status: str,
    expected_warning: Optional[str],
    actual_warning: Optional[str],
) -> float:
    """
    Compare restraint (knowing when NOT to answer).

    Returns score out of 10 points.
    - Status match: 6 points
    - Warning appropriateness: 4 points
    """
    score = 0.0

    # Status match
    if expected_status == actual_status:
        score += 6.0
    elif expected_status in ("AMBIGUOUS", "UNSUPPORTED") and actual_status in ("AMBIGUOUS", "UNSUPPORTED"):
        # Both recognize it's problematic
        score += 4.0
    elif expected_status == "RESOLVED_WITH_WARNING" and actual_status == "RESOLVED":
        # Answered but missed the warning
        score += 3.0

    # Warning appropriateness
    if expected_status in ("AMBIGUOUS", "UNSUPPORTED"):
        if actual_status in ("AMBIGUOUS", "UNSUPPORTED"):
            score += 4.0  # Correctly refused to answer
        elif actual_status == "RESOLVED_WITH_WARNING":
            score += 2.0  # Answered with warning
        # RESOLVED = 0 points (guessed when shouldn't have)
    else:
        if expected_warning and actual_warning:
            score += 4.0
        elif not expected_warning and not actual_warning:
            score += 4.0
        elif expected_warning or actual_warning:
            score += 2.0  # Partial credit

    return score


def evaluate_case(case: Dict[str, Any]) -> CaseScore:
    """
    Evaluate a single test case.

    Returns CaseScore with detailed breakdown.
    """
    case_id = case.get("id", "unknown")
    question = case.get("question", "")
    expected = case.get("expected", {})

    failures = []

    # Get expected values
    exp_status = expected.get("status", "RESOLVED")
    exp_intent = expected.get("intent")
    exp_warning = expected.get("warning")

    # Run the actual NLQ extraction
    try:
        # First, match the question to a definition
        match_result = match_question_with_details(question)

        # Then extract normalized intent
        actual_result = extract_normalized_intent(
            question=question,
            matched_definition=match_result.best_match,
            confidence=match_result.confidence,
        )
    except Exception as e:
        # Handle extraction failure
        return CaseScore(
            case_id=case_id,
            question=question,
            total_score=0.0,
            expected_status=exp_status,
            actual_status="ERROR",
            failures=[f"Extraction error: {str(e)}"],
            expected=expected,
            actual={"error": str(e)},
        )

    # Get actual values
    act_status = actual_result.status.value
    act_intent = actual_result.intent.to_dict() if actual_result.intent else None
    act_warning = actual_result.warning

    # Calculate component scores
    time_score = 0.0
    aggregation_score = 0.0
    metric_score = 0.0
    grouping_score = 0.0
    ranking_score = 0.0

    if exp_intent and act_intent:
        # Time semantics (35 points)
        time_score = compare_time(
            exp_intent.get("time", {}),
            act_intent.get("time", {}),
        )
        if time_score < 35:
            failures.append(f"time: expected {exp_intent.get('time')}, got {act_intent.get('time')}")

        # Aggregation (20 points)
        aggregation_score = compare_aggregation(
            exp_intent.get("aggregation", "total"),
            act_intent.get("aggregation", "total"),
        )
        if aggregation_score < 20:
            failures.append(f"aggregation: expected {exp_intent.get('aggregation')}, got {act_intent.get('aggregation')}")

        # Metric (15 points)
        metric_score = compare_metric(
            exp_intent.get("metric", ""),
            act_intent.get("metric", ""),
        )
        if metric_score < 15:
            failures.append(f"metric: expected {exp_intent.get('metric')}, got {act_intent.get('metric')}")

        # Grouping (10 points)
        grouping_score = compare_grouping(
            exp_intent.get("group_by", []),
            act_intent.get("group_by", []),
        )
        if grouping_score < 10:
            failures.append(f"grouping: expected {exp_intent.get('group_by')}, got {act_intent.get('group_by')}")

        # Ranking (10 points)
        ranking_score = compare_ranking(
            exp_intent.get("limit"),
            act_intent.get("limit"),
            exp_intent.get("direction"),
            act_intent.get("direction"),
        )
        if ranking_score < 10:
            failures.append(f"ranking: expected limit={exp_intent.get('limit')}, got limit={act_intent.get('limit')}")

    elif exp_status in ("AMBIGUOUS", "UNSUPPORTED"):
        # For AMBIGUOUS/UNSUPPORTED, we don't expect an intent
        if act_status in ("AMBIGUOUS", "UNSUPPORTED"):
            # Correctly identified as problematic - give partial time/agg/metric scores
            time_score = 17.5  # Half credit
            aggregation_score = 10.0
            metric_score = 7.5
        # Don't penalize for not having intent when status is correct

    # Restraint (10 points)
    restraint_score = compare_restraint(exp_status, act_status, exp_warning, act_warning)
    if restraint_score < 10:
        failures.append(f"restraint: expected status={exp_status}, got status={act_status}")

    total_score = time_score + aggregation_score + metric_score + grouping_score + ranking_score + restraint_score

    return CaseScore(
        case_id=case_id,
        question=question,
        total_score=total_score,
        time_score=time_score,
        aggregation_score=aggregation_score,
        metric_score=metric_score,
        grouping_score=grouping_score,
        ranking_score=ranking_score,
        restraint_score=restraint_score,
        expected_status=exp_status,
        actual_status=act_status,
        failures=failures,
        expected=expected,
        actual=actual_result.to_dict() if actual_result else {},
    )


def run_evaluation(gold_path: Optional[str] = None) -> EvalReport:
    """
    Run full evaluation against gold standard.

    Returns EvalReport with scores and analysis.
    """
    cases = load_gold_cases(gold_path)
    case_scores: List[CaseScore] = []
    failure_buckets: Dict[str, int] = defaultdict(int)

    # Primitive totals for tracking
    primitive_totals = {
        "time": (0.0, 0.0),
        "aggregation": (0.0, 0.0),
        "metric": (0.0, 0.0),
        "grouping": (0.0, 0.0),
        "ranking": (0.0, 0.0),
        "restraint": (0.0, 0.0),
    }

    for case in cases:
        score = evaluate_case(case)
        case_scores.append(score)

        # Track failures by primitive
        for failure in score.failures:
            primitive = failure.split(":")[0]
            failure_buckets[primitive] += 1

        # Update primitive totals
        primitive_totals["time"] = (
            primitive_totals["time"][0] + score.time_score,
            primitive_totals["time"][1] + 35.0,
        )
        primitive_totals["aggregation"] = (
            primitive_totals["aggregation"][0] + score.aggregation_score,
            primitive_totals["aggregation"][1] + 20.0,
        )
        primitive_totals["metric"] = (
            primitive_totals["metric"][0] + score.metric_score,
            primitive_totals["metric"][1] + 15.0,
        )
        primitive_totals["grouping"] = (
            primitive_totals["grouping"][0] + score.grouping_score,
            primitive_totals["grouping"][1] + 10.0,
        )
        primitive_totals["ranking"] = (
            primitive_totals["ranking"][0] + score.ranking_score,
            primitive_totals["ranking"][1] + 10.0,
        )
        primitive_totals["restraint"] = (
            primitive_totals["restraint"][0] + score.restraint_score,
            primitive_totals["restraint"][1] + 10.0,
        )

    # Calculate totals
    total_score = sum(s.total_score for s in case_scores)
    max_score = len(case_scores) * 100.0
    percentage = (total_score / max_score * 100) if max_score > 0 else 0

    # Sort by score to find worst cases
    worst_cases = sorted(case_scores, key=lambda s: s.total_score)[:10]

    # Sort failure buckets
    failure_buckets = dict(sorted(failure_buckets.items(), key=lambda x: x[1], reverse=True))

    return EvalReport(
        total_cases=len(case_scores),
        total_score=total_score,
        max_possible_score=max_score,
        percentage=percentage,
        case_scores=case_scores,
        failure_buckets=failure_buckets,
        worst_cases=worst_cases,
        primitive_scores=primitive_totals,
    )


def print_report(report: EvalReport):
    """Print evaluation report to console."""
    print("\n" + "=" * 70)
    print("NLQ INTENT EVALUATION REPORT")
    print("=" * 70)

    print(f"\nTotal Cases: {report.total_cases}")
    print(f"Total Score: {report.total_score:.1f} / {report.max_possible_score:.1f}")
    print(f"Percentage:  {report.percentage:.1f}%")

    print("\n" + "-" * 70)
    print("PRIMITIVE SCORES")
    print("-" * 70)

    for primitive, (actual, max_val) in report.primitive_scores.items():
        pct = (actual / max_val * 100) if max_val > 0 else 0
        bar = "█" * int(pct / 5) + "░" * (20 - int(pct / 5))
        print(f"  {primitive:12} {bar} {pct:5.1f}% ({actual:.0f}/{max_val:.0f})")

    print("\n" + "-" * 70)
    print("TOP 5 FAILURE BUCKETS")
    print("-" * 70)

    for i, (bucket, count) in enumerate(list(report.failure_buckets.items())[:5]):
        print(f"  {i+1}. {bucket}: {count} failures")

    print("\n" + "-" * 70)
    print("WORST 10 CASES")
    print("-" * 70)

    for i, case in enumerate(report.worst_cases):
        status_match = "✓" if case.expected_status == case.actual_status else "✗"
        print(f"\n  {i+1}. [{case.case_id}] Score: {case.total_score:.1f}/100 {status_match}")
        print(f"     Q: \"{case.question[:60]}{'...' if len(case.question) > 60 else ''}\"")
        print(f"     Expected: {case.expected_status}, Got: {case.actual_status}")
        if case.failures:
            for failure in case.failures[:3]:
                print(f"     - {failure[:70]}")

    print("\n" + "=" * 70)


def main():
    """Run evaluation and print report."""
    print("Loading gold cases...")
    report = run_evaluation()
    print_report(report)

    # Return exit code based on score
    if report.percentage < 50:
        print("\n⚠️  SCORE BELOW 50% - INTENT EXTRACTION NEEDS IMPROVEMENT")
        return 1
    elif report.percentage < 75:
        print("\n⚡ SCORE BETWEEN 50-75% - ACCEPTABLE BUT ROOM FOR IMPROVEMENT")
        return 0
    else:
        print("\n✓ SCORE ABOVE 75% - GOOD INTENT EXTRACTION")
        return 0


if __name__ == "__main__":
    exit(main())
