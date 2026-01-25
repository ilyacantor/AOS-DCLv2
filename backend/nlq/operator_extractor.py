"""
Operator Extractor - Extracts temporal and comparison operators from NLQ queries.

This module is responsible for detecting:
- Temporal operators: MoM (month-over-month), QoQ (quarter-over-quarter), YoY (year-over-year)
- Comparison operators: change, delta, increase, decrease, growth
- Aggregation operators: total, sum, average, count

The key insight is that operators are INDEPENDENT of the entity/metric being queried.
"How did revenue change MoM?" and "How did costs change MoM?" both need delta capability,
regardless of whether they map to finops.arr or finops.spend.
"""
import re
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional, Set


class TemporalOperator(str, Enum):
    """Time-based comparison operators."""
    MOM = "mom"  # Month-over-month
    QOQ = "qoq"  # Quarter-over-quarter
    YOY = "yoy"  # Year-over-year
    WOW = "wow"  # Week-over-week
    DOD = "dod"  # Day-over-day


class ComparisonOperator(str, Enum):
    """Change/comparison operators."""
    CHANGE = "change"
    DELTA = "delta"
    INCREASE = "increase"
    DECREASE = "decrease"
    GROWTH = "growth"
    TREND = "trend"


class AggregationOperator(str, Enum):
    """Aggregation operators."""
    TOTAL = "total"
    SUM = "sum"
    AVERAGE = "average"
    COUNT = "count"
    TOP = "top"
    BOTTOM = "bottom"


@dataclass
class ExtractedOperators:
    """Result of operator extraction from a query."""
    temporal: Optional[TemporalOperator] = None
    comparison: Optional[ComparisonOperator] = None
    aggregation: Optional[AggregationOperator] = None
    requires_delta: bool = False  # True if query needs delta/change capability
    requires_trend: bool = False  # True if query needs trend capability


# Patterns for temporal operators
TEMPORAL_PATTERNS = {
    TemporalOperator.MOM: [
        r'\bmom\b',
        r'\bmonth[- ]over[- ]month\b',
        r'\bmonth to month\b',
        r'\bmonthly change\b',
        r'\blast month\b.*\bcompare\b',
        r'\bcompare\b.*\blast month\b',
        r'\bvs\.?\s*last month\b',
        r'\bfrom last month\b',
        r'\bsince last month\b',
    ],
    TemporalOperator.QOQ: [
        r'\bqoq\b',
        r'\bquarter[- ]over[- ]quarter\b',
        r'\bquarter to quarter\b',
        r'\bquarterly change\b',
        r'\blast quarter\b.*\bcompare\b',
        r'\bcompare\b.*\blast quarter\b',
        r'\bvs\.?\s*last quarter\b',
    ],
    TemporalOperator.YOY: [
        r'\byoy\b',
        r'\byear[- ]over[- ]year\b',
        r'\byear to year\b',
        r'\bannual change\b',
        r'\byearly change\b',
        r'\blast year\b.*\bcompare\b',
        r'\bcompare\b.*\blast year\b',
        r'\bvs\.?\s*last year\b',
    ],
    TemporalOperator.WOW: [
        r'\bwow\b',
        r'\bweek[- ]over[- ]week\b',
        r'\bweek to week\b',
        r'\bweekly change\b',
    ],
    TemporalOperator.DOD: [
        r'\bdod\b',
        r'\bday[- ]over[- ]day\b',
        r'\bday to day\b',
        r'\bdaily change\b',
    ],
}

# Patterns for comparison operators
COMPARISON_PATTERNS = {
    ComparisonOperator.CHANGE: [
        r'\bhow did .+ change\b',
        r'\bwhat changed\b',
        r'\bchanges? in\b',  # "change in" or "changes in"
        r'\bchanges? to\b',
        r'\bchanges? from\b',
        r'\bshow me changes\b',
    ],
    ComparisonOperator.DELTA: [
        r'\bdelta\b',
        r'\bdifference\b',
        r'\bvariance\b',
    ],
    ComparisonOperator.INCREASE: [
        r'\bincrease[ds]?\b',
        r'\bgrew\b',
        r'\bgrown\b',
        r'\bwent up\b',
        r'\brisen?\b',
    ],
    ComparisonOperator.DECREASE: [
        r'\bdecrease[ds]?\b',
        r'\bdeclined?\b',
        r'\bdropped?\b',
        r'\bwent down\b',
        r'\bfell\b',
        r'\bfallen\b',
    ],
    ComparisonOperator.GROWTH: [
        r'\bgrowth\b',
        r'\bgrowth rate\b',
    ],
    ComparisonOperator.TREND: [
        r'\btrend\b',
        r'\btrending\b',
        r'\bover time\b',
    ],
}

# Patterns for aggregation operators
AGGREGATION_PATTERNS = {
    AggregationOperator.TOTAL: [
        r'\btotal\b',
        r'\ball\b',
        r'\boverall\b',
    ],
    AggregationOperator.SUM: [
        r'\bsum\b',
        r'\bcombined\b',
    ],
    AggregationOperator.AVERAGE: [
        r'\baverage\b',
        r'\bavg\b',
        r'\bmean\b',
    ],
    AggregationOperator.COUNT: [
        r'\bcount\b',
        r'\bhow many\b',
        r'\bnumber of\b',
    ],
    AggregationOperator.TOP: [
        r'\btop\s*\d*\b',
        r'\bhighest\b',
        r'\blargest\b',
        r'\bbiggest\b',
        r'\bbest\b',
        r'\bmost\b',
    ],
    AggregationOperator.BOTTOM: [
        r'\bbottom\s*\d*\b',
        r'\blowest\b',
        r'\bsmallest\b',
        r'\bworst\b',
        r'\bleast\b',
    ],
}


def extract_operators(question: str) -> ExtractedOperators:
    """
    Extract temporal, comparison, and aggregation operators from a question.

    Args:
        question: The natural language question

    Returns:
        ExtractedOperators with detected operators and capability requirements
    """
    q_lower = question.lower()

    result = ExtractedOperators()

    # Extract temporal operator
    for op, patterns in TEMPORAL_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, q_lower):
                result.temporal = op
                result.requires_delta = True
                break
        if result.temporal:
            break

    # Extract comparison operator
    for op, patterns in COMPARISON_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, q_lower):
                result.comparison = op
                # Most comparison operators imply delta capability
                if op in (ComparisonOperator.CHANGE, ComparisonOperator.DELTA,
                          ComparisonOperator.INCREASE, ComparisonOperator.DECREASE,
                          ComparisonOperator.GROWTH):
                    result.requires_delta = True
                if op == ComparisonOperator.TREND:
                    result.requires_trend = True
                break
        if result.comparison:
            break

    # Extract aggregation operator
    for op, patterns in AGGREGATION_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, q_lower):
                result.aggregation = op
                break
        if result.aggregation:
            break

    return result


def get_required_capabilities(operators: ExtractedOperators) -> Set[str]:
    """
    Get the set of capability flags required based on extracted operators.

    Args:
        operators: Extracted operators from a question

    Returns:
        Set of capability flag names required (e.g., {"supports_delta", "supports_top_n"})
    """
    required = set()

    if operators.requires_delta:
        required.add("supports_delta")

    if operators.requires_trend:
        required.add("supports_trend")

    if operators.aggregation in (AggregationOperator.TOP, AggregationOperator.BOTTOM):
        required.add("supports_top_n")

    if operators.aggregation in (AggregationOperator.TOTAL, AggregationOperator.SUM,
                                  AggregationOperator.AVERAGE, AggregationOperator.COUNT):
        required.add("supports_aggregation")

    return required


def format_operator_description(operators: ExtractedOperators) -> str:
    """
    Format a human-readable description of the detected operators.

    Useful for debugging and logging.
    """
    parts = []

    if operators.temporal:
        parts.append(f"temporal={operators.temporal.value}")
    if operators.comparison:
        parts.append(f"comparison={operators.comparison.value}")
    if operators.aggregation:
        parts.append(f"aggregation={operators.aggregation.value}")

    if operators.requires_delta:
        parts.append("requires_delta")
    if operators.requires_trend:
        parts.append("requires_trend")

    return ", ".join(parts) if parts else "no operators"
