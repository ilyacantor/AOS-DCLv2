"""
SQL Compiler and Filter DSL for NLQ Semantic Layer.

Provides:
- FilterDSL: Interprets filter expressions into SQL conditions
- SQLCompiler: Generates executable SQL from definition specs
- ProofResolver: Resolves proof hook templates to actual URLs
"""

from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, timedelta
from functools import lru_cache
import hashlib
import json
from backend.utils.log_utils import get_logger
from backend.nlq.models import (
    DefinitionVersionSpec,
    DefinitionVersion,
    CompiledPlan,
    ProofHook,
)
from backend.nlq.persistence import NLQPersistence

logger = get_logger(__name__)


# =============================================================================
# Filter DSL Interpreter
# =============================================================================

class FilterDSL:
    """
    Interprets filter expressions from definition specs into SQL conditions.

    Supports operators:
    - eq: equals
    - neq: not equals
    - in: in list
    - not_in: not in list
    - gt, gte, lt, lte: comparisons
    - between: range
    - like: pattern match
    - is_null, is_not_null: null checks
    """

    @staticmethod
    def interpret(filter_spec: Dict[str, Any], field_name: str) -> Tuple[str, List[Any]]:
        """
        Interpret a filter spec into SQL condition and parameters.

        Args:
            filter_spec: Filter specification with 'op' and value(s)
            field_name: The field to filter on

        Returns:
            Tuple of (sql_condition, parameters)
        """
        if not filter_spec or not isinstance(filter_spec, dict):
            return "", []

        op = filter_spec.get("op", "eq")

        if op == "eq":
            return f"{field_name} = %s", [filter_spec.get("value")]

        elif op == "neq":
            return f"{field_name} != %s", [filter_spec.get("value")]

        elif op == "in":
            values = filter_spec.get("values", [])
            if not values:
                return "1=0", []  # No values = no matches
            placeholders = ", ".join(["%s"] * len(values))
            return f"{field_name} IN ({placeholders})", values

        elif op == "not_in":
            values = filter_spec.get("values", [])
            if not values:
                return "1=1", []  # No values = all match
            placeholders = ", ".join(["%s"] * len(values))
            return f"{field_name} NOT IN ({placeholders})", values

        elif op == "gt":
            return f"{field_name} > %s", [filter_spec.get("value")]

        elif op == "gte":
            return f"{field_name} >= %s", [filter_spec.get("value")]

        elif op == "lt":
            return f"{field_name} < %s", [filter_spec.get("value")]

        elif op == "lte":
            return f"{field_name} <= %s", [filter_spec.get("value")]

        elif op == "between":
            return f"{field_name} BETWEEN %s AND %s", [
                filter_spec.get("min"),
                filter_spec.get("max")
            ]

        elif op == "like":
            pattern = filter_spec.get("pattern", "%")
            return f"{field_name} LIKE %s", [pattern]

        elif op == "ilike":
            pattern = filter_spec.get("pattern", "%")
            return f"{field_name} ILIKE %s", [pattern]

        elif op == "is_null":
            return f"{field_name} IS NULL", []

        elif op == "is_not_null":
            return f"{field_name} IS NOT NULL", []

        else:
            logger.warning(f"Unknown filter operator: {op}")
            return "", []

    @staticmethod
    def interpret_all(filters: Dict[str, Any]) -> Tuple[str, List[Any]]:
        """
        Interpret all filters into a combined SQL WHERE clause.

        Args:
            filters: Dict mapping field names to filter specs

        Returns:
            Tuple of (combined_sql_condition, all_parameters)
        """
        if not filters:
            return "1=1", []

        conditions = []
        all_params = []

        for field_name, filter_spec in filters.items():
            condition, params = FilterDSL.interpret(filter_spec, field_name)
            if condition:
                conditions.append(condition)
                all_params.extend(params)

        if not conditions:
            return "1=1", []

        return " AND ".join(conditions), all_params


# =============================================================================
# Time Window Interpreter
# =============================================================================

class TimeWindowInterpreter:
    """
    Interprets time window specifications into SQL date conditions.
    """

    @staticmethod
    def get_window_dates(window: str, reference_date: Optional[datetime] = None) -> Tuple[datetime, datetime]:
        """
        Get start and end dates for a time window.

        Args:
            window: Time window (QoQ, YoY, MTD, QTD, YTD, etc.)
            reference_date: Reference date (defaults to now)

        Returns:
            Tuple of (start_date, end_date)
        """
        ref = reference_date or datetime.now()

        if window.upper() == "MTD":
            start = ref.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            end = ref

        elif window.upper() == "QTD":
            quarter_month = ((ref.month - 1) // 3) * 3 + 1
            start = ref.replace(month=quarter_month, day=1, hour=0, minute=0, second=0, microsecond=0)
            end = ref

        elif window.upper() == "YTD":
            start = ref.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
            end = ref

        elif window.upper() == "QOQ":
            # Current quarter vs previous quarter
            quarter_month = ((ref.month - 1) // 3) * 3 + 1
            start = ref.replace(month=quarter_month, day=1) - timedelta(days=90)
            end = ref

        elif window.upper() == "YOY":
            # Current period vs same period last year
            start = ref.replace(year=ref.year - 1)
            end = ref

        elif window.upper() == "MOM":
            # Current month vs previous month
            start = (ref.replace(day=1) - timedelta(days=1)).replace(day=1)
            end = ref

        else:
            # Default to last 30 days
            start = ref - timedelta(days=30)
            end = ref

        return start, end

    @staticmethod
    def build_time_condition(
        time_field: str,
        window: str,
        reference_date: Optional[datetime] = None
    ) -> Tuple[str, List[Any]]:
        """
        Build SQL time condition for a window.

        Returns:
            Tuple of (sql_condition, parameters)
        """
        start, end = TimeWindowInterpreter.get_window_dates(window, reference_date)
        return f"{time_field} BETWEEN %s AND %s", [start, end]


# =============================================================================
# SQL Compiler
# =============================================================================

class SQLCompiler:
    """
    Compiles definition specs into executable SQL queries.

    Generates parameterized SQL with:
    - Proper aggregation from measure spec
    - Filter conditions from filter DSL
    - Time window conditions
    - Dimension grouping
    - Event type filtering
    """

    # Mapping of measure operations to SQL
    MEASURE_OPS = {
        "sum": "SUM",
        "count": "COUNT",
        "avg": "AVG",
        "min": "MIN",
        "max": "MAX",
        "count_distinct": "COUNT(DISTINCT",
        "annualized_sum": "SUM",  # Post-processing needed
        "avg_days_between": "AVG",  # Special handling
    }

    def __init__(self, persistence: Optional[NLQPersistence] = None):
        self.persistence = persistence or NLQPersistence()

    def compile(
        self,
        spec: DefinitionVersionSpec,
        definition_id: str,
        requested_dims: Optional[List[str]] = None,
        time_window: Optional[str] = None,
        additional_filters: Optional[Dict[str, Any]] = None,
        tenant_id: str = "default",
    ) -> Tuple[str, List[Any], Dict[str, Any]]:
        """
        Compile a definition spec into SQL.

        Args:
            spec: The definition version spec
            definition_id: Definition ID for context
            requested_dims: Dimensions to group by
            time_window: Time window (QoQ, YoY, etc.)
            additional_filters: Additional runtime filters
            tenant_id: Tenant ID

        Returns:
            Tuple of (sql_query, parameters, metadata)
        """
        requested_dims = requested_dims or []
        additional_filters = additional_filters or {}

        # Build SELECT clause
        select_clause = self._build_select(spec, requested_dims)

        # Build FROM clause (with event joins if needed)
        from_clause, from_params = self._build_from(spec, tenant_id)

        # Build WHERE clause
        where_clause, where_params = self._build_where(
            spec, time_window, additional_filters
        )

        # Build GROUP BY clause
        group_by_clause = self._build_group_by(requested_dims)

        # Build ORDER BY clause
        order_by_clause = self._build_order_by(spec)

        # Combine into full query
        sql = f"""
SELECT
    {select_clause}
FROM {from_clause}
WHERE {where_clause}
{group_by_clause}
{order_by_clause}
""".strip()

        params = from_params + where_params

        metadata = {
            "definition_id": definition_id,
            "time_window": time_window,
            "dimensions": requested_dims,
            "events": spec.required_events,
            "measure": spec.measure,
        }

        return sql, params, metadata

    def _build_select(self, spec: DefinitionVersionSpec, dims: List[str]) -> str:
        """Build SELECT clause with dimensions and measure."""
        parts = []

        # Add dimensions
        for dim in dims:
            parts.append(f"e.{dim}")

        # Add measure
        measure = spec.measure
        op = measure.get("op", "sum")
        field = measure.get("field", "amount")

        sql_op = self.MEASURE_OPS.get(op, "SUM")

        if op == "count_distinct":
            parts.append(f"{sql_op} e.{field})) AS metric_value")
        elif op == "avg_days_between":
            # Special case: calculate days between two date fields
            fields = field if isinstance(field, list) else [field, field]
            parts.append(f"AVG(EXTRACT(EPOCH FROM (e.{fields[1]} - e.{fields[0]})) / 86400) AS metric_value")
        else:
            parts.append(f"{sql_op}(e.{field}) AS metric_value")

        return ",\n    ".join(parts)

    def _build_from(self, spec: DefinitionVersionSpec, tenant_id: str) -> Tuple[str, List[Any]]:
        """Build FROM clause with event table references."""
        events = spec.required_events

        if len(events) == 1:
            return f"events_{events[0]} e", []

        # Multiple events - need to join
        # For now, use UNION approach
        unions = []
        for event in events:
            unions.append(f"SELECT * FROM events_{event}")

        return f"({' UNION ALL '.join(unions)}) e", []

    def _build_where(
        self,
        spec: DefinitionVersionSpec,
        time_window: Optional[str],
        additional_filters: Dict[str, Any]
    ) -> Tuple[str, List[Any]]:
        """Build WHERE clause with all filters."""
        conditions = []
        params = []

        # Add spec filters
        if spec.filters:
            spec_condition, spec_params = FilterDSL.interpret_all(spec.filters)
            if spec_condition and spec_condition != "1=1":
                conditions.append(f"({spec_condition})")
                params.extend(spec_params)

        # Add time window filter
        if time_window and spec.time_field:
            time_condition, time_params = TimeWindowInterpreter.build_time_condition(
                f"e.{spec.time_field}", time_window
            )
            conditions.append(f"({time_condition})")
            params.extend(time_params)

        # Add additional runtime filters
        if additional_filters:
            add_condition, add_params = FilterDSL.interpret_all(additional_filters)
            if add_condition and add_condition != "1=1":
                conditions.append(f"({add_condition})")
                params.extend(add_params)

        if not conditions:
            return "1=1", []

        return " AND ".join(conditions), params

    def _build_group_by(self, dims: List[str]) -> str:
        """Build GROUP BY clause."""
        if not dims:
            return ""

        cols = [f"e.{dim}" for dim in dims]
        return f"GROUP BY {', '.join(cols)}"

    def _build_order_by(self, spec: DefinitionVersionSpec) -> str:
        """Build ORDER BY clause."""
        return "ORDER BY metric_value DESC"

    def generate_query_hash(self, sql: str, params: List[Any]) -> str:
        """Generate a deterministic hash for the query."""
        content = json.dumps({"sql": sql, "params": [str(p) for p in params]}, sort_keys=True)
        return f"sha256:{hashlib.sha256(content.encode()).hexdigest()[:16]}"


# =============================================================================
# Proof Hook Resolver
# =============================================================================

class ProofResolver:
    """
    Resolves proof hook templates into actual source system URLs/references.
    """

    # Source system URL patterns
    SOURCE_PATTERNS = {
        "NetSuite": {
            "saved_search": "https://system.netsuite.com/app/common/search/searchresults.nl?searchid={search_id}",
            "record": "https://system.netsuite.com/app/common/entity/entity.nl?id={record_id}",
            "report": "https://system.netsuite.com/app/reporting/reportrunner.nl?reportid={report_id}",
        },
        "Salesforce": {
            "report": "https://org.lightning.force.com/lightning/r/Report/{report_id}/view",
            "record": "https://org.lightning.force.com/lightning/r/{object}/{record_id}/view",
            "dashboard": "https://org.lightning.force.com/lightning/r/Dashboard/{dashboard_id}/view",
        },
        "PSA": {
            "report": "https://psa.example.com/reports/{report_id}",
            "project": "https://psa.example.com/projects/{project_id}",
        },
        "Snowflake": {
            "query": "https://app.snowflake.com/org/account/#/query?query_id={query_id}",
            "worksheet": "https://app.snowflake.com/org/account/#/worksheet/{worksheet_id}",
        },
    }

    def __init__(self, persistence: Optional[NLQPersistence] = None):
        self.persistence = persistence or NLQPersistence()

    def resolve(
        self,
        proof_hook: ProofHook,
        context: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Resolve a proof hook template into actual URLs.

        Args:
            proof_hook: The proof hook with template
            context: Context values for template substitution

        Returns:
            Dict with resolved proof information
        """
        template = proof_hook.pointer_template_json
        system = template.get("system", "Unknown")
        pointer_type = template.get("type", "record")
        ref_template = template.get("ref_template", "")

        # Resolve the reference template
        resolved_ref = self._substitute_template(ref_template, context)

        # Generate URL if pattern exists
        url = None
        if system in self.SOURCE_PATTERNS:
            system_patterns = self.SOURCE_PATTERNS[system]
            if pointer_type in system_patterns:
                url_template = system_patterns[pointer_type]
                url = self._substitute_template(url_template, context)

        return {
            "system": system,
            "type": pointer_type,
            "ref": resolved_ref,
            "url": url,
            "availability_score": proof_hook.availability_score,
        }

    def resolve_all(
        self,
        definition_id: str,
        context: Dict[str, Any],
        tenant_id: str = "default",
    ) -> List[Dict[str, Any]]:
        """
        Resolve all proof hooks for a definition.

        Returns:
            List of resolved proof information dicts
        """
        hooks = self.persistence.get_proof_hooks_for_definition(definition_id, tenant_id)
        return [self.resolve(hook, context) for hook in hooks]

    def _substitute_template(self, template: str, context: Dict[str, Any]) -> str:
        """Substitute context values into a template string."""
        result = template
        for key, value in context.items():
            result = result.replace(f"{{{key}}}", str(value))
        return result


# =============================================================================
# Cached Validator Wrapper
# =============================================================================

class CachedValidationMixin:
    """
    Mixin that adds caching to validation results.
    """

    _validation_cache: Dict[str, Any] = {}
    _cache_ttl_seconds: int = 300  # 5 minutes

    def _cache_key(self, definition_id: str, version: str, dims: List[str], tenant_id: str) -> str:
        """Generate cache key for validation."""
        dims_str = ",".join(sorted(dims)) if dims else ""
        return f"{tenant_id}:{definition_id}:{version}:{dims_str}"

    def get_cached_validation(self, cache_key: str):
        """Get cached validation if still valid."""
        if cache_key in self._validation_cache:
            entry = self._validation_cache[cache_key]
            if datetime.now().timestamp() - entry["timestamp"] < self._cache_ttl_seconds:
                return entry["result"]
            else:
                del self._validation_cache[cache_key]
        return None

    def set_cached_validation(self, cache_key: str, result: Any):
        """Cache a validation result."""
        self._validation_cache[cache_key] = {
            "result": result,
            "timestamp": datetime.now().timestamp(),
        }

    def clear_validation_cache(self):
        """Clear all cached validations."""
        self._validation_cache.clear()
