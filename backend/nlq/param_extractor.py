"""
NLQ Parameter Extractor - Extracts execution parameters from natural language questions.

PRODUCTION BOUNDARY:
- NLQ = compiler. It emits TopN(limit) intent, NOT ordering decisions.
- Ordering is defined in Definition.capabilities.default_order_by (concrete columns).
- Executor applies ordering based on definition spec, not NLQ inference.

This module provides deterministic, regex-based parameter extraction for:
- TopN limit extraction ("top 5", "first 10")
- Time window extraction ("last month", "YTD")

No LLM required - all extraction uses pattern matching.
"""
import re
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, field


@dataclass
class ExecutionArgs:
    """Extracted execution arguments from a question."""
    limit: Optional[int] = None
    time_window: Optional[str] = None
    filters: Optional[Dict[str, Any]] = None
    # NOTE: order_by removed - ordering is handled by definition spec, not NLQ inference
    
    def to_dict(self) -> Dict[str, Any]:
        result = {}
        if self.limit is not None:
            result["limit"] = self.limit
        if self.time_window:
            result["time_window"] = self.time_window
        if self.filters:
            result["filters"] = self.filters
        return result
    
    def has_params(self) -> bool:
        return any([self.limit, self.time_window, self.filters])


# Allowed parameters per definition (order_by removed - handled by definition spec)
DEFAULT_ALLOWED_PARAMS = ["limit", "time_window", "filters"]


def extract_limit(question: str) -> Optional[int]:
    """
    Extract limit/top-N from question.
    
    Patterns:
    - "top 5 customers"
    - "top five customers" 
    - "first 10 results"
    - "show me 3 vendors"
    - "give me the 5 largest"
    
    Returns None if no limit found (use definition default).
    """
    question_lower = question.lower()
    
    # Pattern: "top N" or "top <word-number>"
    top_pattern = r'\btop\s+(\d+)\b'
    match = re.search(top_pattern, question_lower)
    if match:
        return int(match.group(1))
    
    # Pattern: word numbers
    word_numbers = {
        'one': 1, 'two': 2, 'three': 3, 'four': 4, 'five': 5,
        'six': 6, 'seven': 7, 'eight': 8, 'nine': 9, 'ten': 10,
        'twenty': 20, 'fifty': 50, 'hundred': 100
    }
    for word, num in word_numbers.items():
        if re.search(rf'\btop\s+{word}\b', question_lower):
            return num
    
    # Pattern: "first N"
    first_pattern = r'\bfirst\s+(\d+)\b'
    match = re.search(first_pattern, question_lower)
    if match:
        return int(match.group(1))
    
    # Pattern: "show/give me N <noun>"
    show_pattern = r'\b(?:show|give)\s+(?:me\s+)?(?:the\s+)?(\d+)\s+'
    match = re.search(show_pattern, question_lower)
    if match:
        return int(match.group(1))
    
    # Pattern: "N largest/biggest/highest"
    largest_pattern = r'\b(\d+)\s+(?:largest|biggest|highest|top)\b'
    match = re.search(largest_pattern, question_lower)
    if match:
        return int(match.group(1))
    
    return None


def extract_time_window(question: str) -> Optional[str]:
    """
    Extract time window from question.

    Patterns:
    - "last month" / "this month"
    - "last quarter" / "this quarter"
    - "Q1" / "Q2" / "Q3" / "Q4" (specific quarters)
    - "YTD" / "year to date"
    - "last year" / "this year"
    - "in 2024" / "2025 revenue" (specific years)
    - "last 30 days"
    """
    question_lower = question.lower()

    # Specific year patterns - check first (e.g., "in 2024", "2024 revenue", "2025's")
    year_patterns = [
        r'\b(?:in\s+)?(\d{4})\b(?:\s+revenue|\s+sales)?',  # "in 2024", "2024 revenue"
        r"(\d{4})(?:'s)?\s+(?:revenue|sales)",  # "2024's revenue"
    ]
    for pattern in year_patterns:
        match = re.search(pattern, question_lower)
        if match:
            year = match.group(1)
            if 2020 <= int(year) <= 2030:  # Reasonable year range
                return year

    # Specific quarter patterns (Q1, Q2, Q3, Q4)
    quarter_match = re.search(r'\b[qQ]([1-4])\b', question_lower)
    if quarter_match:
        return f"q{quarter_match.group(1)}"

    # Relative time patterns
    time_patterns = {
        r'\blast\s+month\b': 'last_month',
        r'\bthis\s+month\b': 'this_month',
        r'\blast\s+quarter\b': 'last_quarter',
        r'\bthis\s+quarter\b': 'this_quarter',
        r'\b(?:ytd|year[\s-]to[\s-]date)\b': 'ytd',
        r'\blast\s+year\b': 'last_year',
        r'\bthis\s+year\b': 'this_year',
        r'\blast\s+(\d+)\s+days?\b': 'last_n_days',
        r'\blast\s+week\b': 'last_week',
        r'\bthis\s+week\b': 'this_week',
    }

    for pattern, window in time_patterns.items():
        match = re.search(pattern, question_lower)
        if match:
            if window == 'last_n_days':
                days = match.group(1)
                return f"last_{days}_days"
            return window

    return None


def extract_params(
    question: str, 
    allowed_params: Optional[List[str]] = None
) -> ExecutionArgs:
    """
    Extract execution parameters from a question.
    
    PRODUCTION BOUNDARY: NLQ extracts TopN(limit) intent only.
    Ordering is determined by definition.capabilities.default_order_by.
    
    Args:
        question: Natural language question
        allowed_params: List of allowed parameter types (default: all)
    
    Returns:
        ExecutionArgs with extracted parameters (limit, time_window only)
    """
    allowed = allowed_params or DEFAULT_ALLOWED_PARAMS
    args = ExecutionArgs()
    
    if "limit" in allowed:
        args.limit = extract_limit(question)
    
    if "time_window" in allowed:
        args.time_window = extract_time_window(question)
    
    return args


def apply_limit_clamp(limit: Optional[int], max_limit: int = 100) -> Optional[int]:
    """Clamp limit to a maximum value."""
    if limit is None:
        return None
    return min(limit, max_limit)
