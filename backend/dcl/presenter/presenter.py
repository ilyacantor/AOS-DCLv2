"""
DCL Presenter - Generates mechanical data summaries.

The presenter is responsible for generating data_summary strings that:
1. Are purely mechanical (based on data, not LLM)
2. Respect the parameters used (limit, filters, etc.)
3. Include computed aggregations in human-readable form
4. Never over-claim (e.g., don't say "top 5" if limit wasn't applied)
"""
from typing import Dict, Any, List, Optional


def _format_currency(amount: float) -> str:
    """Format a number as currency."""
    if abs(amount) >= 1_000_000:
        return f"${amount/1_000_000:,.2f}M"
    elif abs(amount) >= 1_000:
        return f"${amount/1_000:,.1f}K"
    else:
        return f"${amount:,.2f}"


def _format_number(num: float) -> str:
    """Format a number for display."""
    if abs(num) >= 1_000_000:
        return f"{num/1_000_000:,.1f}M"
    elif abs(num) >= 1_000:
        return f"{num/1_000:,.1f}K"
    else:
        return f"{num:,.0f}"


def generate_data_summary(
    definition_id: str,
    rows: List[Dict[str, Any]],
    aggregations: Dict[str, Any],
    limit_applied: Optional[int] = None,
    entity_type: str = "items",
    primary_metric: str = "amount",
    warnings: Optional[List[str]] = None,
) -> str:
    """
    Generate a mechanical data summary for query results.

    Args:
        definition_id: The definition that was executed
        rows: The result rows
        aggregations: Computed aggregations (population_total, topn_total, etc.)
        limit_applied: The limit that was applied (None if no limit)
        entity_type: What the rows represent (e.g., "customer", "vendor")
        primary_metric: The primary metric name (e.g., "revenue", "cost")
        warnings: Any warnings from execution

    Returns:
        Human-readable data summary string
    """
    row_count = len(rows)

    # Extract standard aggregations
    pop_total = aggregations.get("population_total", 0)
    pop_count = aggregations.get("population_count", row_count)
    topn_total = aggregations.get("topn_total", aggregations.get("shown_total", 0))
    share_pct = aggregations.get("share_of_total_pct", 100.0)

    # Pluralize entity type
    entity_plural = entity_type + "s" if not entity_type.endswith("s") else entity_type

    # Determine if this is a monetary metric
    is_monetary = primary_metric in ["revenue", "cost", "spend", "amount", "arr"]
    format_value = _format_currency if is_monetary else _format_number

    # Check if we have a missing limit warning
    has_missing_limit = warnings and any("MISSING_LIMIT" in str(w) for w in warnings)

    parts = []

    if limit_applied and limit_applied < pop_count and not has_missing_limit:
        # Top-N summary with share of total
        if pop_total > 0 and topn_total > 0:
            parts.append(
                f"Top {row_count} {entity_plural} total {format_value(topn_total)} "
                f"({share_pct:.0f}% of {format_value(pop_total)} across {pop_count} {entity_plural})."
            )
        else:
            parts.append(f"Top {row_count} {entity_plural}.")
    else:
        # Full dataset or no limit
        if pop_total > 0:
            parts.append(f"Showing {row_count} of {pop_count} {entity_plural}, total {format_value(pop_total)}.")
        else:
            parts.append(f"Showing {row_count} {entity_plural}.")

    # Add any warnings as caveats
    if has_missing_limit:
        parts.append("(No limit specified - returning full dataset)")

    return " ".join(parts)
