"""
NLQ Parameter Extractor - Extracts execution parameters from natural language questions.

This module provides deterministic, regex-based parameter extraction for common
query modifiers like "top N", "by revenue", "last month", etc.

No LLM required - all extraction uses pattern matching.
"""
import re
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, field


@dataclass
class ExecutionArgs:
    """Extracted execution arguments from a question."""
    limit: Optional[int] = None
    order_by: Optional[List[Dict[str, str]]] = None
    time_window: Optional[str] = None
    filters: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        result = {}
        if self.limit is not None:
            result["limit"] = self.limit
        if self.order_by:
            result["order_by"] = self.order_by
        if self.time_window:
            result["time_window"] = self.time_window
        if self.filters:
            result["filters"] = self.filters
        return result
    
    def has_params(self) -> bool:
        return any([self.limit, self.order_by, self.time_window, self.filters])


# Allowed parameters per definition (can be extended per-definition)
DEFAULT_ALLOWED_PARAMS = ["limit", "order_by", "time_window", "filters"]

# Order-by field mappings
ORDER_FIELD_MAPPINGS = {
    "revenue": ["revenue", "annual_revenue", "total_revenue", "amount", "AnnualRevenue"],
    "cost": ["monthly_cost", "cost", "amount", "total_cost"],
    "date": ["close_date", "created_date", "date", "CloseDate"],
    "name": ["name", "account_name", "Name", "AccountName"],
    "count": ["count", "total", "quantity"],
}


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


def extract_order_by(question: str) -> Optional[List[Dict[str, str]]]:
    """
    Extract order-by from question.
    
    Patterns:
    - "by revenue"
    - "sorted by cost"
    - "ordered by name"
    - "highest revenue" (implies desc)
    - "lowest cost" (implies asc)
    """
    question_lower = question.lower()
    
    # Check for explicit "by X" patterns
    by_pattern = r'\b(?:by|sorted\s+by|ordered\s+by)\s+(\w+)'
    match = re.search(by_pattern, question_lower)
    if match:
        field_hint = match.group(1)
        direction = "desc"  # Default to descending for "top" queries
        
        # Check for ascending indicators
        if any(word in question_lower for word in ['lowest', 'smallest', 'least', 'ascending', 'asc']):
            direction = "asc"
        
        # Map hint to actual field
        for canonical, variants in ORDER_FIELD_MAPPINGS.items():
            if field_hint in variants or field_hint == canonical:
                return [{"field": canonical, "direction": direction}]
        
        # Use hint directly if no mapping found
        return [{"field": field_hint, "direction": direction}]
    
    # Check for implicit ordering from superlatives
    if any(word in question_lower for word in ['highest', 'largest', 'biggest', 'most', 'top']):
        # Try to infer field from context
        if 'revenue' in question_lower:
            return [{"field": "revenue", "direction": "desc"}]
        if 'cost' in question_lower or 'spend' in question_lower:
            return [{"field": "cost", "direction": "desc"}]
    
    if any(word in question_lower for word in ['lowest', 'smallest', 'least']):
        if 'cost' in question_lower or 'spend' in question_lower:
            return [{"field": "cost", "direction": "asc"}]
    
    return None


def extract_time_window(question: str) -> Optional[str]:
    """
    Extract time window from question.
    
    Patterns:
    - "last month"
    - "this quarter"
    - "YTD" / "year to date"
    - "last 30 days"
    """
    question_lower = question.lower()
    
    time_patterns = {
        r'\blast\s+month\b': 'last_month',
        r'\bthis\s+month\b': 'current_month',
        r'\blast\s+quarter\b': 'last_quarter',
        r'\bthis\s+quarter\b': 'current_quarter',
        r'\b(?:ytd|year\s+to\s+date)\b': 'ytd',
        r'\blast\s+year\b': 'last_year',
        r'\bthis\s+year\b': 'current_year',
        r'\blast\s+(\d+)\s+days?\b': 'last_n_days',
        r'\blast\s+week\b': 'last_week',
        r'\bthis\s+week\b': 'current_week',
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
    Extract all parameters from a question.
    
    Args:
        question: Natural language question
        allowed_params: List of allowed parameter types (default: all)
    
    Returns:
        ExecutionArgs with extracted parameters
    """
    allowed = allowed_params or DEFAULT_ALLOWED_PARAMS
    args = ExecutionArgs()
    
    if "limit" in allowed:
        args.limit = extract_limit(question)
    
    if "order_by" in allowed:
        args.order_by = extract_order_by(question)
    
    if "time_window" in allowed:
        args.time_window = extract_time_window(question)
    
    return args


def apply_limit_clamp(limit: Optional[int], max_limit: int = 100) -> Optional[int]:
    """Clamp limit to a maximum value."""
    if limit is None:
        return None
    return min(limit, max_limit)
