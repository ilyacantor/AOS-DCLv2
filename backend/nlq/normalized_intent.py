"""
Normalized Intent Model - Domain-agnostic intent representation.

This module defines the canonical intent schema that all NLQ questions
reduce to. The normalized intent is what gets evaluated against the
gold standard, not the final prose answer.

ARCHITECTURE:
- Intent primitives are domain-agnostic
- Time semantics distinguish calendar vs rolling
- Aggregation types are explicit
- Restraint is encoded (AMBIGUOUS, UNSUPPORTED are valid states)
"""
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, List, Any


class IntentStatus(str, Enum):
    """Status of intent resolution."""
    RESOLVED = "RESOLVED"
    RESOLVED_WITH_WARNING = "RESOLVED_WITH_WARNING"
    AMBIGUOUS = "AMBIGUOUS"
    UNSUPPORTED = "UNSUPPORTED"


class TimeMode(str, Enum):
    """Time scoping mode."""
    CALENDAR = "calendar"  # "last month", "Q2", "YTD"
    ROLLING = "rolling"    # "past 30 days", "last 7 days"
    NONE = "none"          # Current/no time scope


class AggregationType(str, Enum):
    """Type of aggregation requested."""
    TOTAL = "total"        # Sum/aggregate value
    COUNT = "count"        # Count of items
    DELTA = "delta"        # Change/growth
    PERCENT = "percent"    # Percentage/share
    RANKING = "ranking"    # Top/bottom N
    TREND = "trend"        # Over time
    INVENTORY = "inventory"  # List/enumeration
    HEALTH = "health"      # Status/health check


class RankDirection(str, Enum):
    """Direction for ranking queries."""
    TOP = "top"
    BOTTOM = "bottom"


@dataclass
class TimeSpec:
    """Time specification for the intent."""
    mode: TimeMode = TimeMode.NONE
    spec: str = "current"  # e.g., "calendar_month:last", "rolling_days:30"

    def to_dict(self) -> dict:
        return {
            "mode": self.mode.value,
            "spec": self.spec,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "TimeSpec":
        return cls(
            mode=TimeMode(d.get("mode", "none")),
            spec=d.get("spec", "current"),
        )


@dataclass
class NormalizedIntent:
    """
    Normalized intent extracted from a natural language question.

    This is the canonical representation that gets evaluated against
    the gold standard. It is domain-agnostic and uses primitive types.
    """
    metric: str  # Canonical metric name (arr, spend, slo_attainment, etc.)
    grain: str = "none"  # Time grain (day, week, month, quarter, year, none)
    time: TimeSpec = field(default_factory=TimeSpec)
    aggregation: AggregationType = AggregationType.TOTAL
    group_by: List[str] = field(default_factory=list)
    limit: Optional[int] = None
    direction: Optional[RankDirection] = None

    def to_dict(self) -> dict:
        return {
            "metric": self.metric,
            "grain": self.grain,
            "time": self.time.to_dict(),
            "aggregation": self.aggregation.value,
            "group_by": self.group_by,
            "limit": self.limit,
            "direction": self.direction.value if self.direction else None,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "NormalizedIntent":
        direction = None
        if d.get("direction"):
            direction = RankDirection(d["direction"])

        return cls(
            metric=d.get("metric", "unknown"),
            grain=d.get("grain", "none"),
            time=TimeSpec.from_dict(d.get("time", {})),
            aggregation=AggregationType(d.get("aggregation", "total")),
            group_by=d.get("group_by", []),
            limit=d.get("limit"),
            direction=direction,
        )


@dataclass
class IntentResult:
    """
    Full result of intent extraction including status and warnings.
    """
    status: IntentStatus
    intent: Optional[NormalizedIntent] = None
    warning: Optional[str] = None
    confidence: float = 0.0
    matched_definition: Optional[str] = None
    debug_info: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "status": self.status.value,
            "intent": self.intent.to_dict() if self.intent else None,
            "warning": self.warning,
            "confidence": self.confidence,
            "matched_definition": self.matched_definition,
            "debug_info": self.debug_info,
        }


# =============================================================================
# Metric Mapping - Definition ID to Canonical Metric
# =============================================================================

DEFINITION_TO_METRIC = {
    # Finance - Revenue
    "finops.arr": "arr",
    "finops.revenue": "revenue",

    # Finance - Spend
    "finops.saas_spend": "saas_spend",
    "finops.burn_rate": "burn_rate",
    "finops.top_vendor_deltas_mom": "vendor_spend_delta",
    "finops.unallocated_spend": "unallocated_spend",
    "finops.spend": "spend",
    "finops.cloud_spend": "cloud_spend",

    # CRM
    "crm.top_customers": "customer_revenue",
    "crm.pipeline": "pipeline",
    "crm.customers": "customer_revenue",
    "crm.deals": "deal_value",

    # Infrastructure
    "infra.slo_attainment": "slo_attainment",
    "infra.deploy_frequency": "deploy_frequency",
    "infra.lead_time": "lead_time",
    "infra.change_failure_rate": "change_failure_rate",
    "infra.mttr": "mttr",
    "infra.incidents": "incident_count",

    # Security/AOD
    "aod.security_findings": "security_findings",
    "aod.zombies_overview": "zombie_resources",
    "aod.identity_gap_financially_anchored": "identity_gaps",
}

# Reverse mapping
METRIC_TO_DEFINITION = {v: k for k, v in DEFINITION_TO_METRIC.items()}

# Question-to-metric patterns - ORDER MATTERS (more specific patterns first)
# These are checked in order, first match wins
QUESTION_METRIC_PATTERNS = [
    # Specific compound patterns first
    (r"\bannual\s+recurring\s+revenue\b", "arr"),
    (r"\barr\b", "arr"),
    (r"\bsaas\s+spend(?:ing)?\b", "saas_spend"),
    (r"\bcloud\s+(?:spend|cost)s?\b", "cloud_spend"),
    (r"\bunallocated\s+(?:cloud\s+)?spend\b", "unallocated_spend"),
    (r"\bunallocated\b", "unallocated_spend"),
    (r"\bburn\s*rate\b", "burn_rate"),
    (r"\bvendor[s]?\s+(?:by\s+)?spend\b", "vendor_spend"),
    (r"\bspend\s+by\s+vendor\b", "vendor_spend"),
    (r"\btop\s+\d*\s*vendor[s]?\b", "vendor_spend"),

    # Infrastructure - specific first
    (r"\b(?:mean\s+)?time\s+to\s+recover", "mttr"),
    (r"\bmttr\b", "mttr"),
    (r"\blead\s+time\b", "lead_time"),
    (r"\bchange\s+failure\s+rate\b", "change_failure_rate"),
    (r"\bdeploy(?:ment)?(?:s)?\s+frequency\b", "deploy_frequency"),
    (r"\bhow\s+often\s+(?:do\s+)?(?:we\s+)?deploy\b", "deploy_frequency"),
    (r"\bslo[s]?\b", "slo_attainment"),
    (r"\buptime\b", "slo_attainment"),
    (r"\bavailability\b", "slo_attainment"),
    (r"\bservice\s+level\b", "slo_attainment"),
    (r"\bincident[s]?\b", "incident_count"),
    (r"\boutage[s]?\b", "incident_count"),

    # Security
    (r"\bsecurity\s+finding[s]?\b", "security_findings"),
    (r"\bzombie[s]?\s+resource[s]?\b", "zombie_resources"),
    (r"\bzombie[s]?\b", "zombie_resources"),
    (r"\bidle\s+resource[s]?\b", "zombie_resources"),
    (r"\bidle\b", "zombie_resources"),
    (r"\bidentity\s+gap[s]?\b", "identity_gaps"),
    (r"\bunowned\s+spend\b", "identity_gaps"),
    (r"\bresource[s]?\s+without\s+owner", "identity_gaps"),

    # CRM - specific patterns
    (r"\btop\s+\d*\s*customer[s]?\b", "customer_revenue"),
    (r"\bcustomer[s]?\s+by\s+revenue\b", "customer_revenue"),
    (r"\bcustomer\s+concentration\b", "customer_concentration"),
    (r"\bpipeline\b", "pipeline"),
    (r"\bdeal[s]?\s+(?:by\s+)?value\b", "deal_value"),
    (r"\bdeal[s]?\s+in\s+(?:the\s+)?pipeline\b", "pipeline"),
    (r"\btop\s+\d*\s*deal[s]?\b", "deal_value"),
    (r"\bdeals?\s+closing\b", "pipeline"),

    # Generic patterns last (these are catch-alls)
    (r"\brevenue\b", "revenue"),
    (r"\bspend(?:ing)?\b", "spend"),
    (r"\bcost[s]?\b", "spend"),
    (r"\bcustomer[s]?\b", "customer_revenue"),
    (r"\bvendor[s]?\b", "vendor_spend"),
    (r"\bdeal[s]?\b", "deal_value"),
    (r"\bdeploy(?:ment)?(?:s)?\b", "deploy_frequency"),
]


# =============================================================================
# Time Pattern Detection
# =============================================================================

import re

# Calendar time patterns
CALENDAR_PATTERNS = {
    r"\blast\s+month\b": ("calendar", "calendar_month:last", "month"),
    r"\bthis\s+month\b": ("calendar", "calendar_month:current", "month"),
    r"\blast\s+quarter\b": ("calendar", "calendar_quarter:last", "quarter"),
    r"\bthis\s+quarter\b": ("calendar", "calendar_quarter:current", "quarter"),
    r"\blast\s+year\b": ("calendar", "calendar_year:last", "year"),
    r"\bthis\s+year\b": ("calendar", "calendar_year:current", "year"),
    r"\blast\s+week\b": ("calendar", "calendar_week:last", "week"),
    r"\bthis\s+week\b": ("calendar", "calendar_week:current", "week"),
    r"\bytd\b": ("calendar", "calendar_ytd", "year"),
    r"\byear\s+to\s+date\b": ("calendar", "calendar_ytd", "year"),
    r"\bq([1-4])\b": ("calendar", "calendar_quarter:Q\\1", "quarter"),
    r"\bmom\b": ("calendar", "calendar_month:mom", "month"),
    r"\bmonth\s+over\s+month\b": ("calendar", "calendar_month:mom", "month"),
    r"\bqoq\b": ("calendar", "calendar_quarter:qoq", "quarter"),
    r"\bquarter\s+over\s+quarter\b": ("calendar", "calendar_quarter:qoq", "quarter"),
    r"\byoy\b": ("calendar", "calendar_year:yoy", "year"),
    r"\byear\s+over\s+year\b": ("calendar", "calendar_year:yoy", "year"),
}

# Rolling time patterns
ROLLING_PATTERNS = {
    r"\bpast\s+(\d+)\s+days?\b": ("rolling", "rolling_days:{}", "day"),
    r"\blast\s+(\d+)\s+days?\b": ("rolling", "rolling_days:{}", "day"),
    r"\bpast\s+(\d+)\s+hours?\b": ("rolling", "rolling_hours:{}", "day"),
    r"\blast\s+(\d+)\s+hours?\b": ("rolling", "rolling_hours:{}", "day"),
    r"\bpast\s+(\d+)\s+weeks?\b": ("rolling", "rolling_weeks:{}", "week"),
    r"\blast\s+(\d+)\s+weeks?\b": ("rolling", "rolling_weeks:{}", "week"),
}


def extract_time_spec(question: str) -> tuple[TimeSpec, str]:
    """
    Extract time specification from question.

    Returns (TimeSpec, grain).
    Distinguishes calendar vs rolling time.
    """
    question_lower = question.lower()

    # Check calendar patterns first
    for pattern, (mode, spec, grain) in CALENDAR_PATTERNS.items():
        match = re.search(pattern, question_lower)
        if match:
            # Handle Q1-Q4 substitution
            if "\\1" in spec:
                spec = spec.replace("\\1", match.group(1))
            return TimeSpec(mode=TimeMode.CALENDAR, spec=spec), grain

    # Check rolling patterns
    for pattern, (mode, spec_template, grain) in ROLLING_PATTERNS.items():
        match = re.search(pattern, question_lower)
        if match:
            num = match.group(1)
            spec = spec_template.format(num)
            return TimeSpec(mode=TimeMode.ROLLING, spec=spec), grain

    # Default: no time scope
    return TimeSpec(mode=TimeMode.NONE, spec="current"), "none"


# =============================================================================
# Aggregation Detection
# =============================================================================

# Aggregation patterns - ORDER MATTERS (check in sequence)
# More specific patterns should be checked first
AGGREGATION_PATTERNS_ORDERED = [
    # Ranking - explicit
    (r"\btop\s+\d+\b", AggregationType.RANKING),
    (r"\bbottom\s+\d+\b", AggregationType.RANKING),
    (r"\bfirst\s+\d+\b", AggregationType.RANKING),
    (r"\bwho\s+(?:are|is)\s+(?:our\s+)?top\b", AggregationType.RANKING),  # "Who are our top customers?"
    (r"\btop\s+(?:customers?|vendors?|deals?|services?)\b", AggregationType.RANKING),

    # Inventory - before count patterns
    (r"\bshow\s+(?:me\s+)?(?:all|the)\s+", AggregationType.INVENTORY),
    (r"\blist\s+(?:all\s+)?", AggregationType.INVENTORY),
    (r"\binventory\b", AggregationType.INVENTORY),
    (r"\bshow\s+me\s+(?:security\s+)?findings?\b", AggregationType.INVENTORY),  # security findings = inventory

    # Count - explicit
    (r"\bhow\s+many\b", AggregationType.COUNT),
    (r"\bnumber\s+of\b", AggregationType.COUNT),
    (r"\btotal\s+(?:deal\s+)?count\b", AggregationType.COUNT),
    (r"\b(?:new|lost)\s+customers?\b", AggregationType.COUNT),  # "new customers", "lost customers"
    (r"\bincident[s]?\s+(?:last|this|past)\b", AggregationType.COUNT),  # "incidents last week"
    (r"\bopen\s+incidents?\b", AggregationType.COUNT),  # "open incidents"

    # Health - before generic "how are"
    (r"\bslo\s+(?:attainment\s+)?by\s+service\b", AggregationType.HEALTH),
    (r"\bhow\s+(?:are|is)\s+(?:our\s+)?slo", AggregationType.HEALTH),
    (r"\bservice\s+health\b", AggregationType.HEALTH),
    (r"\bsystem\s+status\b", AggregationType.HEALTH),
    (r"\buptime\b", AggregationType.HEALTH),
    (r"\bat\s+risk\b", AggregationType.HEALTH),
    (r"\bpipeline\s+health\b", AggregationType.HEALTH),
    (r"\bare\s+there\s+any\s+issues\b", AggregationType.HEALTH),

    # Delta/Change - specific patterns (exclude "change failure rate", "lead time for changes")
    (r"\bchange[ds]?\s+mom\b", AggregationType.DELTA),
    (r"\bchange[ds]?\s+month\s+over\s+month\b", AggregationType.DELTA),
    (r"\bchange[ds]?\s+qoq\b", AggregationType.DELTA),
    (r"\bchange[ds]?\s+yoy\b", AggregationType.DELTA),
    (r"\bgrowth\b", AggregationType.DELTA),
    (r"\bincrease[ds]?\b", AggregationType.DELTA),
    (r"\bdecrease[ds]?\b", AggregationType.DELTA),
    (r"\bdelta\b", AggregationType.DELTA),
    (r"\bmom\b", AggregationType.DELTA),
    (r"\bqoq\b", AggregationType.DELTA),
    (r"\byoy\b", AggregationType.DELTA),
    (r"\bvs\s+(?:last|prior|previous)\b", AggregationType.DELTA),
    (r"\bcomparison\b", AggregationType.DELTA),
    (r"\bhow\s+has\s+.*\s+changed\b", AggregationType.DELTA),
    (r"\bhas\s+changed\b", AggregationType.DELTA),

    # Trend
    (r"\btrend(?:ing)?\b", AggregationType.TREND),
    (r"\bover\s+time\b", AggregationType.TREND),
    (r"\bhow's?\s+.*\s+trending\b", AggregationType.TREND),

    # Percent - specific patterns only (not generic "rate")
    (r"\bpercent(?:age)?\b", AggregationType.PERCENT),
    (r"\bshare\s+of\b", AggregationType.PERCENT),
    (r"\bconcentration\b", AggregationType.PERCENT),
    (r"\bwhat\s+percent(?:age)?\b", AggregationType.PERCENT),
    (r"\bbreach\s+rate\b", AggregationType.PERCENT),  # SLO breach rate

    # Ranking - superlatives (check after more specific patterns)
    (r"\blargest\b", AggregationType.RANKING),
    (r"\bsmallest\b", AggregationType.RANKING),
    (r"\bhighest\b", AggregationType.RANKING),
    (r"\blowest\b", AggregationType.RANKING),
    (r"\bworst\b", AggregationType.RANKING),
    (r"\bbest\b", AggregationType.RANKING),
    (r"\bbiggest\b", AggregationType.RANKING),
    (r"\bwhat's?\s+eating\b", AggregationType.RANKING),  # "what's eating up our budget?"
]

# Legacy dict for backward compatibility (not used directly)
AGGREGATION_PATTERNS = {p: t for p, t in AGGREGATION_PATTERNS_ORDERED}


def extract_aggregation(question: str) -> AggregationType:
    """Extract aggregation type from question.

    Uses ordered patterns - first match wins.
    More specific patterns are checked first.
    """
    question_lower = question.lower()

    for pattern, agg_type in AGGREGATION_PATTERNS_ORDERED:
        if re.search(pattern, question_lower):
            return agg_type

    return AggregationType.TOTAL


# =============================================================================
# Limit Extraction
# =============================================================================

LIMIT_PATTERNS = [
    r"\btop\s+(\d+)\b",
    r"\bbottom\s+(\d+)\b",
    r"\bfirst\s+(\d+)\b",
    r"\blast\s+(\d+)\b(?!\s+(?:day|week|month|quarter|year|hour))",
]

IMPLICIT_LIMIT_PATTERNS = {
    r"\bbiggest\b": (1, RankDirection.TOP),
    r"\blargest\b": (1, RankDirection.TOP),
    r"\bsmallest\b": (1, RankDirection.BOTTOM),
    r"\bhighest\b": (1, RankDirection.TOP),
    r"\blowest\b": (1, RankDirection.BOTTOM),
    r"\bworst\b": (1, RankDirection.BOTTOM),
    r"\bbest\b": (1, RankDirection.TOP),
}


def extract_limit_and_direction(question: str) -> tuple[Optional[int], Optional[RankDirection]]:
    """
    Extract limit and direction from question.

    Returns (limit, direction).
    """
    question_lower = question.lower()

    # Check for explicit limit
    for pattern in LIMIT_PATTERNS:
        match = re.search(pattern, question_lower)
        if match:
            limit = int(match.group(1))
            # Determine direction
            if "bottom" in pattern or "last" in pattern:
                return limit, RankDirection.BOTTOM
            return limit, RankDirection.TOP

    # Check for implicit single-item patterns
    for pattern, (limit, direction) in IMPLICIT_LIMIT_PATTERNS.items():
        if re.search(pattern, question_lower):
            return limit, direction

    # Check for "top" without number (implicit limit)
    if re.search(r"\btop\s+(?:customers?|vendors?|deals?|services?)\b", question_lower):
        return None, RankDirection.TOP  # Limit will be defaulted with warning

    return None, None


# =============================================================================
# Grouping Detection
# =============================================================================

GROUP_PATTERNS = {
    r"\bby\s+customer\b": "customer",
    r"\bby\s+vendor\b": "vendor",
    r"\bby\s+service\b": "service",
    r"\bby\s+team\b": "team",
    r"\bby\s+region\b": "region",
    r"\bby\s+severity\b": "severity",
    r"\bby\s+stage\b": "stage",
    r"\bper\s+customer\b": "customer",
    r"\bper\s+vendor\b": "vendor",
    r"\bper\s+service\b": "service",
    r"\bbreakdown\s+by\b": None,  # Marker for grouping
}

ENTITY_PATTERNS = {
    r"\bcustomers?\b": "customer",
    r"\bvendors?\b": "vendor",
    r"\bservices?\b": "service",
    r"\bdeals?\b": "deal",
    r"\bresources?\b": "resource",
}


def extract_grouping(question: str, aggregation: AggregationType) -> List[str]:
    """Extract grouping dimensions from question."""
    question_lower = question.lower()
    groups = []

    # Check explicit "by X" patterns
    for pattern, group in GROUP_PATTERNS.items():
        if group and re.search(pattern, question_lower):
            groups.append(group)

    # For ranking queries, infer grouping from entity
    if aggregation == AggregationType.RANKING and not groups:
        for pattern, group in ENTITY_PATTERNS.items():
            if re.search(pattern, question_lower):
                groups.append(group)
                break

    return groups


# =============================================================================
# Ambiguity Detection
# =============================================================================

AMBIGUOUS_PATTERNS = [
    (r"^what\s+changed\??$", "METRIC_UNDERSPECIFIED: What changed? (revenue, spend, headcount, etc.)"),
    (r"^what\s+changed\s+(?:mom|month\s+over\s+month|qoq|quarter\s+over\s+quarter|yoy|year\s+over\s+year)\??$", "METRIC_UNDERSPECIFIED: What changed? (revenue, spend, headcount, etc.)"),
    (r"^show\s+me\s+the\s+numbers\??$", "METRIC_UNDERSPECIFIED: Which numbers? (revenue, spend, customers, etc.)"),
    (r"^how\s+are\s+we\s+doing\??$", "METRIC_UNDERSPECIFIED: Which aspect? (revenue, SLOs, pipeline, etc.)"),
    (r"^what's?\s+the\s+trend\??$", "METRIC_UNDERSPECIFIED: Trend of what? (revenue, spend, incidents, etc.)"),
    (r"^give\s+me\s+a\s+summary\??$", "METRIC_UNDERSPECIFIED: Summary of what? (financial, operational, etc.)"),
    (r"^top\s+performers?\??$", "METRIC_UNDERSPECIFIED: Top performers in what? (customers, vendors, services, etc.)"),
    (r"^what's?\s+happening\??$", "METRIC_UNDERSPECIFIED: Cannot determine metric or domain"),
    (r"^status\s+update\??$", "METRIC_UNDERSPECIFIED: Status of what? (incidents, SLOs, pipeline, etc.)"),
    (r"^compare\s+(?:this|last)?\s*(?:month|quarter|year)?\s+to\s+", "METRIC_UNDERSPECIFIED: Compare what? (revenue, spend, incidents, etc.)"),
    (r"^what's?\s+at\s+risk\??$", "METRIC_UNDERSPECIFIED: What's at risk? (SLOs, deals, customers, etc.)"),
    (r"\bdora\s+metrics?\b", "METRIC_UNDERSPECIFIED: Which DORA metric? (deploy frequency, lead time, MTTR, change failure rate)"),
    (r"\borphan\s+resources?\b", "METRIC_UNDERSPECIFIED: Do you mean unused resources (zombies) or resources without owners (identity gaps)?"),
]

UNSUPPORTED_PATTERNS = [
    (r"\bpredict\b", "FORECASTING_UNSUPPORTED: DCL does not support predictions"),
    (r"\bforecast\b", "FORECASTING_UNSUPPORTED: DCL does not support predictions"),
    (r"\bwhy\s+did\b", "CAUSAL_ANALYSIS_UNSUPPORTED: DCL cannot determine causation"),
    (r"\bwhat\s+should\b", "RECOMMENDATION_UNSUPPORTED: DCL provides data, not recommendations"),
    (r"\bdelete\s+(?:the\s+)?(?:old\s+)?", "WRITE_OPERATION_UNSUPPORTED: DCL is read-only"),
    (r"\bupdate\s+(?:customer|record|data)", "WRITE_OPERATION_UNSUPPORTED: DCL is read-only"),
    (r"\bsend\s+me\b", "SCHEDULING_UNSUPPORTED: DCL does not support scheduled reports"),
    (r"\bemail\b", "COMMUNICATION_UNSUPPORTED: DCL cannot send emails"),
    (r"\b(?:over|under)\s+budget\b", "BUDGET_COMPARISON_UNSUPPORTED: Budget targets not available"),
    (r"\bbudget\b.*\b(?:over|under|exceed)\b", "BUDGET_COMPARISON_UNSUPPORTED: Budget targets not available"),
    (r"^are\s+we\s+over\s+budget\??$", "BUDGET_COMPARISON_UNSUPPORTED: Budget targets not available"),
    (r"^show\s+me\s+everything\b", "SCOPE_TOO_BROAD: Cannot show all data; please specify a metric"),
    (r"^compare\s+everything\b", "SCOPE_TOO_BROAD: Cannot compare without specific metrics"),
]


def check_ambiguity(question: str) -> Optional[str]:
    """Check if question is ambiguous and return warning if so."""
    question_lower = question.lower().strip()

    for pattern, warning in AMBIGUOUS_PATTERNS:
        if re.search(pattern, question_lower):
            return warning

    return None


def check_unsupported(question: str) -> Optional[str]:
    """Check if question is unsupported and return warning if so."""
    question_lower = question.lower().strip()

    # Empty query
    if not question_lower:
        return "EMPTY_QUERY: No question provided"

    # Very short/invalid query
    if len(question_lower) <= 2 and not question_lower.isalnum():
        return "INVALID_QUERY: Cannot parse question"

    for pattern, warning in UNSUPPORTED_PATTERNS:
        if re.search(pattern, question_lower):
            return warning

    return None


# =============================================================================
# Main Intent Extraction Function
# =============================================================================

def extract_normalized_intent(
    question: str,
    matched_definition: Optional[str] = None,
    confidence: float = 0.0,
) -> IntentResult:
    """
    Extract normalized intent from a natural language question.

    This is the main entry point for intent normalization.
    It returns an IntentResult with status, intent, and warnings.

    Args:
        question: The natural language question
        matched_definition: The BLL definition matched (from intent_matcher)
        confidence: Match confidence (from intent_matcher)

    Returns:
        IntentResult with normalized intent
    """
    # Check for unsupported queries first
    unsupported_warning = check_unsupported(question)
    if unsupported_warning:
        return IntentResult(
            status=IntentStatus.UNSUPPORTED,
            warning=unsupported_warning,
            confidence=0.0,
            matched_definition=matched_definition,
            debug_info={"question": question, "reason": "unsupported_pattern"},
        )

    # Check for ambiguous queries
    ambiguous_warning = check_ambiguity(question)
    if ambiguous_warning:
        return IntentResult(
            status=IntentStatus.AMBIGUOUS,
            warning=ambiguous_warning,
            confidence=confidence,
            matched_definition=matched_definition,
            debug_info={"question": question, "reason": "ambiguous_pattern"},
        )

    # Extract time specification
    time_spec, grain = extract_time_spec(question)

    # Extract aggregation type
    aggregation = extract_aggregation(question)

    # Extract limit and direction
    limit, direction = extract_limit_and_direction(question)

    # Extract grouping
    group_by = extract_grouping(question, aggregation)

    # First, try to extract metric directly from the question
    # This is more reliable than relying on definition matching
    question_lower = question.lower()
    metric_from_question = None

    # Check question patterns first - prioritize explicit metric mentions
    # QUESTION_METRIC_PATTERNS is ordered by specificity (most specific first)
    for pattern, metric_name in QUESTION_METRIC_PATTERNS:
        if re.search(pattern, question_lower):
            metric_from_question = metric_name
            break

    # Use definition mapping as fallback
    metric_from_definition = DEFINITION_TO_METRIC.get(matched_definition, "unknown")

    # Decision logic:
    # - If question explicitly mentions a metric, use that
    # - Otherwise use definition-based metric
    metric = metric_from_question if metric_from_question else metric_from_definition

    # Special cases where question and definition both have info
    # Use more specific metric when available
    if metric_from_question and metric_from_definition != "unknown":
        # If question says "revenue" and definition is "arr", use "revenue" (more generic)
        # If question says "arr" and definition is "arr", use "arr" (specific)
        pass  # metric_from_question takes precedence

    # Fallback: infer from definition ID
    if metric == "unknown" and matched_definition:
        if "arr" in matched_definition or "revenue" in matched_definition:
            metric = "arr"
        elif "spend" in matched_definition or "cost" in matched_definition:
            metric = "spend"
        elif "slo" in matched_definition:
            metric = "slo_attainment"
        elif "customer" in matched_definition:
            metric = "customer_revenue"
        elif "incident" in matched_definition:
            metric = "incident_count"
        elif "deploy" in matched_definition:
            metric = "deploy_frequency"
        elif "finding" in matched_definition or "security" in matched_definition:
            metric = "security_findings"
        elif "zombie" in matched_definition or "idle" in matched_definition:
            metric = "zombie_resources"
        elif "identity" in matched_definition or "gap" in matched_definition:
            metric = "identity_gaps"

    # Build normalized intent
    intent = NormalizedIntent(
        metric=metric,
        grain=grain,
        time=time_spec,
        aggregation=aggregation,
        group_by=group_by,
        limit=limit,
        direction=direction,
    )

    # Determine status and warnings
    status = IntentStatus.RESOLVED
    warning = None

    # Check for implied limits (ranking without explicit limit)
    if aggregation == AggregationType.RANKING and limit is None and direction:
        intent.limit = 10  # Default
        warning = "LIMIT_DEFAULTED: No limit specified, defaulting to top 10"
        status = IntentStatus.RESOLVED_WITH_WARNING

    # Check for delta queries on definitions that may not support it
    if aggregation == AggregationType.DELTA:
        delta_definitions = {"finops.top_vendor_deltas_mom"}
        if matched_definition and matched_definition not in delta_definitions:
            # The matched definition doesn't support delta, but user asked for it
            if not any(d in (matched_definition or "") for d in ["delta", "change", "mom"]):
                warning = "PERIOD_ASSUMED: Defaulting to month-over-month comparison"
                status = IntentStatus.RESOLVED_WITH_WARNING

    return IntentResult(
        status=status,
        intent=intent,
        warning=warning,
        confidence=confidence,
        matched_definition=matched_definition,
        debug_info={
            "question": question,
            "extracted_time": time_spec.to_dict(),
            "extracted_aggregation": aggregation.value,
            "extracted_limit": limit,
            "extracted_direction": direction.value if direction else None,
            "extracted_group_by": group_by,
        },
    )
