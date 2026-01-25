"""
DCL Unified Executor - Executes definitions with proper aggregation semantics.

Key features:
1. Computes population_total and population_count on FULL population before limiting
2. Computes topn_total and share_of_total_pct for limited results
3. Generates warnings when limit is missing for ranked-list definitions
4. Returns normalized response shape for both NLQ and structured execution
"""
import time
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from enum import Enum

from backend.bll.executor import execute_definition as bll_execute
from backend.bll.models import ExecuteRequest as BLLExecuteRequest
from backend.bll.definitions import get_definition as get_bll_definition
from backend.dcl.definitions.registry import DefinitionRegistry


class WarningType(str, Enum):
    MISSING_LIMIT = "MISSING_LIMIT"
    LIMIT_EXCEEDS_POPULATION = "LIMIT_EXCEEDS_POPULATION"
    NO_DATA = "NO_DATA"
    STALE_DATA = "STALE_DATA"


@dataclass
class Warning:
    """Typed warning for execution issues."""
    type: WarningType
    message: str

    def to_dict(self) -> Dict[str, str]:
        return {"type": self.type.value, "message": self.message}


@dataclass
class ExecuteRequest:
    """Unified execution request."""
    definition_id: str
    dataset_id: str = "demo9"
    limit: Optional[int] = None
    offset: int = 0
    filters: Optional[Dict[str, Any]] = None
    time_range: Optional[Dict[str, str]] = None


@dataclass
class ExecuteResponse:
    """Unified execution response with proper aggregations."""
    rows: List[Dict[str, Any]]
    aggregations: Dict[str, Any]
    warnings: List[Warning]
    data_summary: str
    narrative_answer: Optional[str] = None
    debug: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "rows": self.rows,
            "aggregations": self.aggregations,
            "warnings": [w.to_dict() for w in self.warnings],
            "data_summary": self.data_summary,
            "narrative_answer": self.narrative_answer,
            "debug": self.debug,
        }


def _format_currency(amount: float) -> str:
    """Format a number as currency."""
    if abs(amount) >= 1_000_000:
        return f"${amount/1_000_000:,.2f}M"
    elif abs(amount) >= 1_000:
        return f"${amount/1_000:,.1f}K"
    else:
        return f"${amount:,.2f}"


def execute_query(request: ExecuteRequest) -> ExecuteResponse:
    """
    Execute a definition query with proper aggregation semantics.

    For ranked-list definitions:
    1. Compute population_total and population_count on FULL population
    2. Apply sort + limit to get rows
    3. Compute topn_total and share_of_total_pct
    4. Generate mechanical data_summary
    """
    start_time = time.time()
    warnings: List[Warning] = []

    # Check if this is a ranked-list definition
    is_ranked_list = DefinitionRegistry.is_ranked_list(request.definition_id)
    default_limit = DefinitionRegistry.get_default_limit(request.definition_id)

    # Handle missing limit for ranked-list definitions
    effective_limit = request.limit
    if is_ranked_list and request.limit is None:
        if default_limit:
            # Apply default limit from definition metadata
            effective_limit = default_limit
        else:
            # No limit specified and no default - add warning
            warnings.append(Warning(
                type=WarningType.MISSING_LIMIT,
                message="No limit specified for ranked list query; returning full dataset. Data summary will not claim 'top N'."
            ))
            effective_limit = 1000  # Fallback high limit

    # Execute via BLL executor
    bll_request = BLLExecuteRequest(
        dataset_id=request.dataset_id,
        definition_id=request.definition_id,
        limit=effective_limit or 1000,
        offset=request.offset,
    )

    bll_result = bll_execute(bll_request)

    # Extract aggregations from BLL result
    aggregations = {}
    if bll_result.summary:
        aggregations = bll_result.summary.aggregations.copy()

    # Ensure required aggregation fields exist
    if "population_total" not in aggregations:
        aggregations["population_total"] = aggregations.get("shown_total", 0)
    if "population_count" not in aggregations:
        aggregations["population_count"] = bll_result.metadata.row_count

    # For ranked-list with limit, compute topn_total if not present
    rows = bll_result.data
    if is_ranked_list and effective_limit and effective_limit < 1000:
        if "topn_total" not in aggregations:
            aggregations["topn_total"] = aggregations.get("shown_total", 0)

        # Compute share_of_total_pct if not present
        if "share_of_total_pct" not in aggregations:
            pop_total = aggregations.get("population_total", 0)
            topn_total = aggregations.get("topn_total", 0)
            if pop_total > 0:
                aggregations["share_of_total_pct"] = round((topn_total / pop_total) * 100, 2)
            else:
                aggregations["share_of_total_pct"] = 100.0

    # Generate mechanical data summary
    data_summary = _generate_data_summary(
        definition_id=request.definition_id,
        rows=rows,
        aggregations=aggregations,
        limit=effective_limit if request.limit else None,
        warnings=warnings,
    )

    execution_time_ms = int((time.time() - start_time) * 1000)

    return ExecuteResponse(
        rows=rows,
        aggregations=aggregations,
        warnings=warnings,
        data_summary=data_summary,
        narrative_answer=bll_result.summary.answer if bll_result.summary else None,
        debug={
            "definition_id": request.definition_id,
            "dataset_id": request.dataset_id,
            "effective_limit": effective_limit,
            "execution_time_ms": execution_time_ms,
            "row_count": len(rows),
            "is_ranked_list": is_ranked_list,
        },
    )


def _generate_data_summary(
    definition_id: str,
    rows: List[Dict[str, Any]],
    aggregations: Dict[str, Any],
    limit: Optional[int],
    warnings: List[Warning],
) -> str:
    """
    Generate mechanical data summary that respects parameters.

    Rules:
    - If limit was applied: "Top N items total $X (Y% of $Z across M items)"
    - If no limit/warning: Do NOT claim "top N" - describe full result
    - Be precise about what was computed
    """
    row_count = len(rows)
    pop_total = aggregations.get("population_total", 0)
    pop_count = aggregations.get("population_count", row_count)
    topn_total = aggregations.get("topn_total", aggregations.get("shown_total", 0))
    share_pct = aggregations.get("share_of_total_pct", 100)

    # Get definition metadata for entity type
    meta = DefinitionRegistry.get_metadata(definition_id)
    entity_type = meta.entity_type if meta else "items"
    entity_plural = entity_type + "s" if not entity_type.endswith("s") else entity_type

    # Check if we have a MISSING_LIMIT warning
    has_missing_limit_warning = any(w.type == WarningType.MISSING_LIMIT for w in warnings)

    if limit and limit < 1000 and not has_missing_limit_warning:
        # Proper top-N summary
        if pop_total > 0:
            summary = f"Top {row_count} {entity_plural} total {_format_currency(topn_total)} ({share_pct:.0f}% of {_format_currency(pop_total)} across {pop_count} {entity_plural})."
        else:
            summary = f"Top {row_count} {entity_plural} returned."
    else:
        # No limit applied or missing limit warning - don't claim top-N
        if pop_total > 0:
            summary = f"Showing {row_count} of {pop_count} {entity_plural}, total {_format_currency(pop_total)}."
        else:
            summary = f"Showing {row_count} {entity_plural}."

    return summary
