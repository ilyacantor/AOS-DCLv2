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
    OutputShape,
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
    output_shape_score: float = 0.0  # NEW: Output shape validation
    expected_status: str = ""
    actual_status: str = ""
    failures: List[str] = field(default_factory=list)
    expected: Dict[str, Any] = field(default_factory=dict)
    actual: Dict[str, Any] = field(default_factory=dict)
    is_output_shape_violation: bool = False  # HARD FAIL marker
    # NEW: Metric confusion tracking
    expected_metric: str = ""
    actual_metric: str = ""
    is_revenue_as_arr: bool = False  # HARD GATE: revenue resolved as ARR
    is_arr_as_revenue: bool = False  # HARD GATE: ARR resolved as revenue


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
    output_shape_violations: int = 0  # HARD FAIL count (scalar→ranking)
    hallucination_count: int = 0  # Answered when should have refused
    # NEW: Metric confusion HARD GATES
    revenue_as_arr_count: int = 0  # HARD GATE: revenue queries resolved as ARR
    arr_as_revenue_count: int = 0  # HARD GATE: ARR queries resolved as revenue
    metric_confusion_matrix: Dict[str, Dict[str, int]] = field(default_factory=dict)  # [expected][actual] = count


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
    elif (exp_mode in ("none", "state")) and (act_mode in ("none", "state")):
        # "none" and "state" are semantically equivalent for current-state queries
        # Both mean "no time window, just current state"
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


def compare_output_shape(
    expected_shape: Optional[str],
    actual_shape: Optional[str],
    expected_aggregation: str,
    actual_aggregation: str,
    actual_limit: Optional[int],
    expected_limit: Optional[int] = None,
    expected_group_by: Optional[List[str]] = None,
) -> Tuple[float, bool]:
    """
    Compare output_shape and detect HARD FAIL violations.

    Returns (score, is_violation).

    HARD FAIL CONDITIONS:
    - Expected SCALAR but actual is RANKED → HARD FAIL (score = 0)
    - Expected SCALAR but actual has limit → HARD FAIL (score = 0)
    - Expected SCALAR but actual aggregation is 'ranking' → HARD FAIL (score = 0)

    Normal scoring (10 points):
    - Exact match: 10 points
    - Compatible shapes: 5 points
    - Mismatch: 0 points
    """
    expected_group_by = expected_group_by or []

    # If no expected output_shape, infer from aggregation AND limit/group_by
    if not expected_shape:
        # If there's a limit, it's ranked
        if expected_limit is not None and expected_limit < 100:
            expected_shape = "ranked"
        elif expected_aggregation == "ranking":
            expected_shape = "ranked"
        elif expected_aggregation in ("inventory", "breakdown"):
            expected_shape = "table"
        elif expected_aggregation == "health":
            expected_shape = "status"
        elif expected_aggregation == "percent" and expected_group_by:
            # Percent by group is table/ranked
            expected_shape = "table" if expected_limit is None else "ranked"
        elif expected_aggregation == "total" and not expected_group_by:
            expected_shape = "scalar"
        elif expected_group_by:
            # Has grouping but no limit = table
            expected_shape = "table"
        else:
            expected_shape = "scalar"

    # Normalize to lowercase
    exp_shape = str(expected_shape).lower()
    act_shape = str(actual_shape).lower() if actual_shape else "scalar"

    # HARD FAIL CHECKS for scalar intent violations
    if exp_shape == "scalar":
        violations = []

        # Check if actual shape is ranked
        if act_shape == "ranked":
            violations.append(f"scalar→ranked")

        # Check if ranking aggregation was applied
        if actual_aggregation == "ranking":
            violations.append(f"scalar but aggregation=ranking")

        # Check if limit was applied (implies ranking)
        if actual_limit is not None and actual_limit < 100:
            violations.append(f"scalar but limit={actual_limit}")

        if violations:
            # HARD FAIL - This is a critical violation
            return 0.0, True

    # Normal scoring
    if exp_shape == act_shape:
        return 10.0, False

    # Partial credit for compatible shapes
    compatible_pairs = [
        ("scalar", "status"),  # Status can be scalar-like
        ("table", "ranked"),   # Ranked is a type of table
    ]
    for pair in compatible_pairs:
        if (exp_shape, act_shape) in [pair, pair[::-1]]:
            return 5.0, False

    return 0.0, False


def evaluate_case(case: Dict[str, Any]) -> CaseScore:
    """
    Evaluate a single test case.

    SCORING RULES:
    - AMBIGUOUS expected + AMBIGUOUS actual = 100 (perfect restraint)
    - AMBIGUOUS expected + RESOLVED actual = 0 (hallucination - hard fail)
    - UNSUPPORTED expected + UNSUPPORTED actual = 100 (perfect restraint)
    - RESOLVED expected + AMBIGUOUS actual = 30 max (over-cautious)
    - RESOLVED cases: score based on intent primitives

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

    # SPECIAL CASE: AMBIGUOUS/UNSUPPORTED handling
    # These are RESTRAINT cases - correct identification = full score
    if exp_status in ("AMBIGUOUS", "UNSUPPORTED"):
        if act_status == exp_status:
            # Perfect restraint - correctly refused to answer
            return CaseScore(
                case_id=case_id,
                question=question,
                total_score=100.0,
                time_score=35.0,
                aggregation_score=20.0,
                metric_score=15.0,
                grouping_score=10.0,
                ranking_score=10.0,
                restraint_score=10.0,
                expected_status=exp_status,
                actual_status=act_status,
                failures=[],
                expected=expected,
                actual=actual_result.to_dict() if actual_result else {},
            )
        elif act_status in ("RESOLVED", "RESOLVED_WITH_WARNING"):
            # HALLUCINATION - answered when should have refused
            failures.append(f"HALLUCINATION: expected {exp_status}, but system answered")
            return CaseScore(
                case_id=case_id,
                question=question,
                total_score=0.0,
                time_score=0.0,
                aggregation_score=0.0,
                metric_score=0.0,
                grouping_score=0.0,
                ranking_score=0.0,
                restraint_score=0.0,
                expected_status=exp_status,
                actual_status=act_status,
                failures=failures,
                expected=expected,
                actual=actual_result.to_dict() if actual_result else {},
            )
        else:
            # Different non-answer status (e.g., AMBIGUOUS vs UNSUPPORTED)
            return CaseScore(
                case_id=case_id,
                question=question,
                total_score=80.0,  # Partial credit - at least refused
                time_score=28.0,
                aggregation_score=16.0,
                metric_score=12.0,
                grouping_score=8.0,
                ranking_score=8.0,
                restraint_score=8.0,
                expected_status=exp_status,
                actual_status=act_status,
                failures=[f"restraint: expected {exp_status}, got {act_status}"],
                expected=expected,
                actual=actual_result.to_dict() if actual_result else {},
            )

    # SPECIAL CASE: System refused when it should have answered
    if exp_status in ("RESOLVED", "RESOLVED_WITH_WARNING") and act_status in ("AMBIGUOUS", "UNSUPPORTED"):
        failures.append(f"OVER_CAUTIOUS: expected {exp_status}, but system refused")
        return CaseScore(
            case_id=case_id,
            question=question,
            total_score=30.0,  # Heavy penalty for over-caution
            time_score=10.0,
            aggregation_score=6.0,
            metric_score=5.0,
            grouping_score=3.0,
            ranking_score=3.0,
            restraint_score=3.0,
            expected_status=exp_status,
            actual_status=act_status,
            failures=failures,
            expected=expected,
            actual=actual_result.to_dict() if actual_result else {},
        )

    # NORMAL CASE: Both expected and actual are RESOLVED variants
    # Score based on intent primitives
    time_score = 0.0
    aggregation_score = 0.0
    metric_score = 0.0
    grouping_score = 0.0
    ranking_score = 0.0
    output_shape_score = 0.0
    is_output_shape_violation = False

    # Track metric confusion for HARD GATES
    expected_metric = exp_intent.get("metric", "") if exp_intent else ""
    actual_metric = act_intent.get("metric", "") if act_intent else ""
    is_revenue_as_arr = False
    is_arr_as_revenue = False

    # Detect revenue vs ARR confusion
    exp_metric_lower = expected_metric.lower()
    act_metric_lower = actual_metric.lower()

    # Revenue expected but ARR returned
    if ("revenue" in exp_metric_lower and "arr" not in exp_metric_lower) and \
       ("arr" in act_metric_lower or "recurring" in act_metric_lower):
        is_revenue_as_arr = True
        failures.insert(0, "METRIC_MISMATCH_REVENUE_VS_ARR: revenue requested but ARR returned")

    # ARR expected but revenue returned
    if ("arr" in exp_metric_lower or "recurring" in exp_metric_lower) and \
       ("revenue" in act_metric_lower and "arr" not in act_metric_lower and "recurring" not in act_metric_lower):
        is_arr_as_revenue = True
        failures.insert(0, "METRIC_MISMATCH_ARR_VS_REVENUE: ARR requested but revenue returned")

    if exp_intent and act_intent:
        # Time semantics (30 points - reduced from 35 to make room for output_shape)
        time_score = compare_time(
            exp_intent.get("time", {}),
            act_intent.get("time", {}),
        )
        # Scale to 30 points
        time_score = time_score * 30.0 / 35.0
        if time_score < 30:
            failures.append(f"time: expected {exp_intent.get('time')}, got {act_intent.get('time')}")

        # Aggregation (15 points - reduced from 20)
        aggregation_score = compare_aggregation(
            exp_intent.get("aggregation", "total"),
            act_intent.get("aggregation", "total"),
        )
        # Scale to 15 points
        aggregation_score = aggregation_score * 15.0 / 20.0
        if aggregation_score < 15:
            failures.append(f"aggregation: expected {exp_intent.get('aggregation')}, got {act_intent.get('aggregation')}")

        # Metric (15 points - unchanged)
        metric_score = compare_metric(
            exp_intent.get("metric", ""),
            act_intent.get("metric", ""),
        )
        if metric_score < 15:
            failures.append(f"metric: expected {exp_intent.get('metric')}, got {act_intent.get('metric')}")

        # Grouping (10 points - unchanged)
        grouping_score = compare_grouping(
            exp_intent.get("group_by", []),
            act_intent.get("group_by", []),
        )
        if grouping_score < 10:
            failures.append(f"grouping: expected {exp_intent.get('group_by')}, got {act_intent.get('group_by')}")

        # Ranking (10 points - unchanged)
        ranking_score = compare_ranking(
            exp_intent.get("limit"),
            act_intent.get("limit"),
            exp_intent.get("direction"),
            act_intent.get("direction"),
        )
        if ranking_score < 10:
            failures.append(f"ranking: expected limit={exp_intent.get('limit')}, got limit={act_intent.get('limit')}")

        # OUTPUT SHAPE (10 points - NEW)
        # This is a HARD FAIL category: scalar intent with ranking output = 0 total score
        output_shape_score, is_output_shape_violation = compare_output_shape(
            exp_intent.get("output_shape"),
            act_intent.get("output_shape"),
            exp_intent.get("aggregation", "total"),
            act_intent.get("aggregation", "total"),
            act_intent.get("limit"),
            expected_limit=exp_intent.get("limit"),
            expected_group_by=exp_intent.get("group_by", []),
        )
        if output_shape_score < 10:
            exp_shape = exp_intent.get("output_shape", "inferred")
            act_shape = act_intent.get("output_shape", "scalar")
            failures.append(f"output_shape: expected {exp_shape}, got {act_shape}")

        if is_output_shape_violation:
            failures.insert(0, f"HARD FAIL: scalar intent produced ranking output")

    # Restraint (10 points - unchanged)
    restraint_score = compare_restraint(exp_status, act_status, exp_warning, act_warning)
    if restraint_score < 10:
        failures.append(f"restraint: expected status={exp_status}, got status={act_status}")

    # Calculate total score
    # If HARD FAIL (output_shape violation), total score = 0
    if is_output_shape_violation:
        total_score = 0.0
    else:
        total_score = time_score + aggregation_score + metric_score + grouping_score + ranking_score + output_shape_score + restraint_score

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
        output_shape_score=output_shape_score,
        expected_status=exp_status,
        actual_status=act_status,
        failures=failures,
        expected=expected,
        actual=actual_result.to_dict() if actual_result else {},
        is_output_shape_violation=is_output_shape_violation,
        expected_metric=expected_metric,
        actual_metric=actual_metric,
        is_revenue_as_arr=is_revenue_as_arr,
        is_arr_as_revenue=is_arr_as_revenue,
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
        "output_shape": (0.0, 0.0),  # NEW
    }

    # Track HARD FAIL violations
    output_shape_violations = 0
    hallucination_count = 0
    revenue_as_arr_count = 0
    arr_as_revenue_count = 0

    # Confusion matrix for metrics
    # Key metrics to track: revenue, arr, spend, headcount, pipeline, tickets, apps
    tracked_metrics = ["revenue", "arr", "spend", "headcount", "pipeline", "tickets", "apps", "other"]
    metric_confusion_matrix = {m: {m2: 0 for m2 in tracked_metrics} for m in tracked_metrics}

    def categorize_metric(metric: str) -> str:
        """Categorize a metric into one of the tracked categories."""
        metric_lower = metric.lower() if metric else ""
        if "arr" in metric_lower or "recurring" in metric_lower:
            return "arr"
        elif "revenue" in metric_lower or "top_line" in metric_lower:
            return "revenue"
        elif "spend" in metric_lower or "cost" in metric_lower or "burn" in metric_lower:
            return "spend"
        elif "headcount" in metric_lower or "employee" in metric_lower or "staff" in metric_lower:
            return "headcount"
        elif "pipeline" in metric_lower or "deal" in metric_lower or "opportunity" in metric_lower:
            return "pipeline"
        elif "ticket" in metric_lower or "incident" in metric_lower or "issue" in metric_lower:
            return "tickets"
        elif "app" in metric_lower or "deploy" in metric_lower:
            return "apps"
        return "other"

    for case in cases:
        score = evaluate_case(case)
        case_scores.append(score)

        # Track violations
        if score.is_output_shape_violation:
            output_shape_violations += 1
        if any("HALLUCINATION" in f for f in score.failures):
            hallucination_count += 1

        # Track metric confusion
        if score.is_revenue_as_arr:
            revenue_as_arr_count += 1
        if score.is_arr_as_revenue:
            arr_as_revenue_count += 1

        # Update confusion matrix
        exp_cat = categorize_metric(score.expected_metric)
        act_cat = categorize_metric(score.actual_metric)
        metric_confusion_matrix[exp_cat][act_cat] += 1

        # Track failures by primitive
        for failure in score.failures:
            primitive = failure.split(":")[0]
            failure_buckets[primitive] += 1

        # Update primitive totals (max values updated for new scoring rubric)
        primitive_totals["time"] = (
            primitive_totals["time"][0] + score.time_score,
            primitive_totals["time"][1] + 30.0,  # Reduced from 35
        )
        primitive_totals["aggregation"] = (
            primitive_totals["aggregation"][0] + score.aggregation_score,
            primitive_totals["aggregation"][1] + 15.0,  # Reduced from 20
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
        primitive_totals["output_shape"] = (
            primitive_totals["output_shape"][0] + score.output_shape_score,
            primitive_totals["output_shape"][1] + 10.0,  # NEW: 10 points
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
        output_shape_violations=output_shape_violations,
        hallucination_count=hallucination_count,
        revenue_as_arr_count=revenue_as_arr_count,
        arr_as_revenue_count=arr_as_revenue_count,
        metric_confusion_matrix=metric_confusion_matrix,
    )


def print_report(report: EvalReport):
    """Print evaluation report to console."""
    print("\n" + "=" * 70)
    print("NLQ INTENT EVALUATION REPORT")
    print("=" * 70)

    print(f"\nTotal Cases: {report.total_cases}")
    print(f"Total Score: {report.total_score:.1f} / {report.max_possible_score:.1f}")
    print(f"Percentage:  {report.percentage:.1f}%")

    # HARD FAIL STATISTICS (HARD GATES)
    print("\n" + "-" * 70)
    print("HARD GATES (All must be 0 to PASS)")
    print("-" * 70)
    shape_status = "PASS" if report.output_shape_violations == 0 else "FAIL"
    halluc_status = "PASS" if report.hallucination_count == 0 else "FAIL"
    rev_arr_status = "PASS" if report.revenue_as_arr_count == 0 else "FAIL"
    arr_rev_status = "PASS" if report.arr_as_revenue_count == 0 else "FAIL"

    print(f"  scalar_shape_violations:   {report.output_shape_violations:3d} [{shape_status}]")
    print(f"  hallucinations_count:      {report.hallucination_count:3d} [{halluc_status}]")
    print(f"  revenue_as_arr_count:      {report.revenue_as_arr_count:3d} [{rev_arr_status}]")
    print(f"  arr_as_revenue_count:      {report.arr_as_revenue_count:3d} [{arr_rev_status}]")

    # Show which cases had violations
    if report.output_shape_violations > 0:
        print("\n  ⚠️  CRITICAL: Scalar queries returning ranked output!")
        violations = [s for s in report.case_scores if s.is_output_shape_violation]
        for v in violations[:5]:
            print(f"     - [{v.case_id}] {v.question[:50]}...")

    if report.revenue_as_arr_count > 0:
        print("\n  ⚠️  CRITICAL: Revenue queries resolved as ARR!")
        violations = [s for s in report.case_scores if s.is_revenue_as_arr]
        for v in violations[:5]:
            print(f"     - [{v.case_id}] {v.question[:50]}...")

    if report.arr_as_revenue_count > 0:
        print("\n  ⚠️  CRITICAL: ARR queries resolved as Revenue!")
        violations = [s for s in report.case_scores if s.is_arr_as_revenue]
        for v in violations[:5]:
            print(f"     - [{v.case_id}] {v.question[:50]}...")

    # METRIC CONFUSION MATRIX
    print("\n" + "-" * 70)
    print("METRIC CONFUSION MATRIX (Expected x Actual)")
    print("-" * 70)
    tracked = ["revenue", "arr", "spend", "pipeline", "tickets", "other"]
    # Header
    print("           ", end="")
    for m in tracked:
        print(f"{m[:6]:>7}", end="")
    print()
    # Rows
    for exp in tracked:
        row_total = sum(report.metric_confusion_matrix.get(exp, {}).values())
        if row_total > 0:
            print(f"  {exp:<8}", end="")
            for act in tracked:
                count = report.metric_confusion_matrix.get(exp, {}).get(act, 0)
                if count > 0:
                    if exp == act:
                        print(f"{count:>7}", end="")  # Correct
                    else:
                        print(f"{count:>6}*", end="")  # Confusion (marked)
                else:
                    print(f"{'·':>7}", end="")
            print()

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
        violation_mark = " [HARD FAIL]" if case.is_output_shape_violation else ""
        print(f"\n  {i+1}. [{case.case_id}] Score: {case.total_score:.1f}/100 {status_match}{violation_mark}")
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

    # HARD GATE CHECKS
    # These are MANDATORY pass conditions - ALL must be 0
    has_hard_fails = False

    print("\n" + "=" * 70)
    print("HARD GATE SUMMARY")
    print("=" * 70)

    # Check: Output shape violations MUST be 0
    if report.output_shape_violations > 0:
        print(f"❌ scalar_shape_violations: {report.output_shape_violations} (MUST BE 0)")
        has_hard_fails = True
    else:
        print(f"✅ scalar_shape_violations: 0")

    # Check: Hallucinations MUST be 0
    if report.hallucination_count > 0:
        print(f"❌ hallucinations_count: {report.hallucination_count} (MUST BE 0)")
        has_hard_fails = True
    else:
        print(f"✅ hallucinations_count: 0")

    # Check: Revenue as ARR MUST be 0
    if report.revenue_as_arr_count > 0:
        print(f"❌ revenue_as_arr_count: {report.revenue_as_arr_count} (MUST BE 0)")
        has_hard_fails = True
    else:
        print(f"✅ revenue_as_arr_count: 0")

    # Check: ARR as Revenue MUST be 0
    if report.arr_as_revenue_count > 0:
        print(f"❌ arr_as_revenue_count: {report.arr_as_revenue_count} (MUST BE 0)")
        has_hard_fails = True
    else:
        print(f"✅ arr_as_revenue_count: 0")

    # Check: Overall score threshold
    if report.percentage < 92:
        print(f"\n⚠️  WARNING: Score {report.percentage:.1f}% below target threshold of 92%")
        # This is a soft fail unless combined with hard fails

    if has_hard_fails:
        print("\n🚫 EVALUATION FAILED - HARD GATES NOT MET")
        return 1

    # Return exit code based on score
    if report.percentage < 50:
        print("\n⚠️  SCORE BELOW 50% - INTENT EXTRACTION NEEDS IMPROVEMENT")
        return 1
    elif report.percentage < 75:
        print("\n⚡ SCORE BETWEEN 50-75% - ACCEPTABLE BUT ROOM FOR IMPROVEMENT")
        return 0
    elif report.percentage < 92:
        print("\n⚡ SCORE BETWEEN 75-92% - GOOD BUT CAN IMPROVE")
        return 0
    else:
        print("\n✅ SCORE ABOVE 92% - EXCELLENT INTENT EXTRACTION")
        print(f"   Output shape violations: {report.output_shape_violations}")
        print(f"   Hallucinations: {report.hallucination_count}")
        return 0


if __name__ == "__main__":
    exit(main())
