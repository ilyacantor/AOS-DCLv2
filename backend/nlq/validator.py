"""
Definition Validator and Compiler for NLQ Semantic Layer.

Provides deterministic validation of definitions without executing queries:
- DefinitionValidator: Validates definition answerability
- DefinitionCompiler: Compiles definition to query plan template

These are pure functions that operate on metadata only.
"""

from typing import List, Optional, Dict, Any
from backend.utils.log_utils import get_logger
from backend.nlq.models import (
    ValidationResult,
    WeakBinding,
    CompiledPlan,
    DefinitionVersion,
)
from backend.nlq.persistence import NLQPersistence

logger = get_logger(__name__)


class DefinitionValidator:
    """
    Validates definition answerability based on metadata.

    Returns detailed information about:
    - Missing events (required events not bound)
    - Missing dimensions (requested dims not covered)
    - Weak bindings (low quality or freshness)
    - Coverage, freshness, and proof scores
    """

    # Thresholds for weak binding detection
    QUALITY_THRESHOLD = 0.70
    FRESHNESS_THRESHOLD = 0.70

    def __init__(self, persistence: Optional[NLQPersistence] = None):
        """
        Initialize the validator.

        Args:
            persistence: NLQPersistence instance. Creates default if not provided.
        """
        self.persistence = persistence or NLQPersistence()

    def validate(
        self,
        definition_id: str,
        version: str = "v1",
        requested_dims: Optional[List[str]] = None,
        time_window: Optional[str] = None,
        tenant_id: str = "default",
    ) -> ValidationResult:
        """
        Validate a definition's answerability.

        Args:
            definition_id: The definition to validate
            version: Definition version (default: v1)
            requested_dims: Dimensions the user wants to drill into
            time_window: Time window requested (QoQ, YoY, etc.)
            tenant_id: Tenant ID

        Returns:
            ValidationResult with ok, missing_events, missing_dims, weak_bindings,
            coverage_score, freshness_score, proof_score
        """
        # Get the definition version
        def_version = self.persistence.get_definition_version(
            definition_id, version, tenant_id
        )

        if not def_version:
            logger.warning(f"Definition version not found: {definition_id} {version}")
            return ValidationResult(
                ok=False,
                missing_events=[],
                missing_dims=[],
                weak_bindings=[],
                coverage_score=0.0,
                freshness_score=0.0,
                proof_score=0.0,
            )

        spec = def_version.spec
        requested_dims = requested_dims or []

        # Check required events
        missing_events = []
        bound_events = []
        for event_id in spec.required_events:
            bindings = self.persistence.get_bindings_for_event(event_id, tenant_id)
            if not bindings:
                missing_events.append(event_id)
            else:
                bound_events.append(event_id)

        # Check requested dims against allowed dims
        missing_dims = []
        for dim in requested_dims:
            if dim not in spec.allowed_dims:
                missing_dims.append(dim)
            else:
                # Check if dim is covered by bindings
                dim_available = False
                for event_id in bound_events:
                    coverage = self.persistence.get_dims_coverage(event_id, tenant_id)
                    if coverage.get(dim, False):
                        dim_available = True
                        break
                if not dim_available:
                    missing_dims.append(dim)

        # Find weak bindings
        weak_bindings = []
        for event_id in bound_events:
            bindings = self.persistence.get_bindings_for_event(event_id, tenant_id)
            for binding in bindings:
                if (binding.quality_score < self.QUALITY_THRESHOLD or
                    binding.freshness_score < self.FRESHNESS_THRESHOLD):
                    # Find which dims are missing
                    dims_missing = [
                        dim for dim, covered in binding.dims_coverage_json.items()
                        if not covered and dim in requested_dims
                    ]
                    weak_bindings.append(WeakBinding(
                        source_system=binding.source_system,
                        canonical_event_id=binding.canonical_event_id,
                        dims_missing=dims_missing,
                        quality_score=binding.quality_score,
                        freshness_score=binding.freshness_score,
                    ))

        # Calculate scores
        coverage_score = self._calculate_coverage_score(
            bound_events, spec.required_events, requested_dims, spec.allowed_dims, tenant_id
        )
        freshness_score = self._calculate_freshness_score(bound_events, tenant_id)
        proof_score = self.persistence.get_proof_availability(definition_id, tenant_id)

        # Determine if validation passed
        ok = (
            len(missing_events) == 0 and
            len(missing_dims) == 0 and
            coverage_score >= 0.5
        )

        return ValidationResult(
            ok=ok,
            missing_events=missing_events,
            missing_dims=missing_dims,
            weak_bindings=weak_bindings,
            coverage_score=coverage_score,
            freshness_score=freshness_score,
            proof_score=proof_score,
        )

    def _calculate_coverage_score(
        self,
        bound_events: List[str],
        required_events: List[str],
        requested_dims: List[str],
        allowed_dims: List[str],
        tenant_id: str,
    ) -> float:
        """
        Calculate coverage score as weighted average of:
        - Event binding coverage (50%)
        - Dims coverage (30%)
        - Binding quality (20%)
        """
        # Event coverage
        if not required_events:
            event_coverage = 1.0
        else:
            event_coverage = len(bound_events) / len(required_events)

        # Dims coverage
        if not requested_dims:
            dims_coverage = 1.0
        else:
            covered_dims = 0
            for dim in requested_dims:
                if dim in allowed_dims:
                    for event_id in bound_events:
                        coverage = self.persistence.get_dims_coverage(event_id, tenant_id)
                        if coverage.get(dim, False):
                            covered_dims += 1
                            break
            dims_coverage = covered_dims / len(requested_dims)

        # Average binding quality
        if not bound_events:
            quality_avg = 0.0
        else:
            total_quality = sum(
                self.persistence.get_binding_quality(e, tenant_id)
                for e in bound_events
            )
            quality_avg = total_quality / len(bound_events)

        # Weighted average
        coverage_score = (
            0.50 * event_coverage +
            0.30 * dims_coverage +
            0.20 * quality_avg
        )

        return min(1.0, max(0.0, coverage_score))

    def _calculate_freshness_score(
        self,
        bound_events: List[str],
        tenant_id: str,
    ) -> float:
        """Calculate average freshness across all bindings."""
        if not bound_events:
            return 0.0

        total_freshness = sum(
            self.persistence.get_binding_freshness(e, tenant_id)
            for e in bound_events
        )
        return total_freshness / len(bound_events)


class DefinitionCompiler:
    """
    Compiles a definition to a query plan template.

    Does NOT execute SQL. Returns a deterministic plan that can be
    used for:
    - UI display of what would be queried
    - Proof hash generation
    - Downstream query execution (not in this MVP)
    """

    def __init__(self, persistence: Optional[NLQPersistence] = None):
        """
        Initialize the compiler.

        Args:
            persistence: NLQPersistence instance. Creates default if not provided.
        """
        self.persistence = persistence or NLQPersistence()

    def compile(
        self,
        definition_id: str,
        version: str = "v1",
        requested_dims: Optional[List[str]] = None,
        time_window: Optional[str] = None,
        filters: Optional[Dict[str, Any]] = None,
        tenant_id: str = "default",
    ) -> CompiledPlan:
        """
        Compile a definition to a query plan template.

        Args:
            definition_id: The definition to compile
            version: Definition version (default: v1)
            requested_dims: Dimensions to include in the query
            time_window: Time window (QoQ, YoY, etc.)
            filters: Additional filters to apply
            tenant_id: Tenant ID

        Returns:
            CompiledPlan with sql_template, params_schema, required_events,
            required_dims, time_semantics, proof_hook
        """
        # Get the definition version
        def_version = self.persistence.get_definition_version(
            definition_id, version, tenant_id
        )

        if not def_version:
            logger.warning(f"Definition version not found: {definition_id} {version}")
            return CompiledPlan()

        spec = def_version.spec
        requested_dims = requested_dims or []
        filters = filters or {}

        # Get the definition for time semantics
        definition = self.persistence.get_definition(definition_id, tenant_id)

        # Build SQL template (stub for MVP)
        sql_template = self._build_sql_template(spec, requested_dims, time_window)

        # Build params schema
        params_schema = self._build_params_schema(spec, requested_dims, time_window, filters)

        # Get time semantics
        time_semantics = {}
        if definition and definition.default_time_semantics_json:
            time_semantics = definition.default_time_semantics_json.copy()
        if time_window:
            time_semantics["window"] = time_window

        # Get proof hook
        proof_hook = None
        hooks = self.persistence.get_proof_hooks_for_definition(definition_id, tenant_id)
        if hooks:
            proof_hook = hooks[0].pointer_template_json

        return CompiledPlan(
            sql_template=sql_template,
            params_schema=params_schema,
            required_events=spec.required_events,
            required_dims=requested_dims or spec.allowed_dims,
            time_semantics=time_semantics,
            proof_hook=proof_hook,
        )

    def _build_sql_template(
        self,
        spec,
        requested_dims: List[str],
        time_window: Optional[str],
    ) -> str:
        """Build a SQL template (stub for MVP)."""
        # This is a simplified stub - real implementation would build proper SQL
        measure = spec.measure
        op = measure.get("op", "sum")
        field = measure.get("field", "amount")

        dims_clause = ", ".join(requested_dims) if requested_dims else "*"
        events_clause = ", ".join(spec.required_events)

        time_filter = ""
        if time_window:
            time_filter = f"-- Time window: {time_window}"

        template = f"""
-- Definition: {{definition_id}}
-- Version: {{version}}
{time_filter}
SELECT
    {dims_clause},
    {op.upper()}({field}) as metric_value
FROM canonical_events
WHERE event_type IN ('{events_clause}')
    AND {{time_filter}}
    AND {{dimension_filters}}
GROUP BY {dims_clause if dims_clause != "*" else "1"}
ORDER BY metric_value DESC
""".strip()

        return template

    def _build_params_schema(
        self,
        spec,
        requested_dims: List[str],
        time_window: Optional[str],
        filters: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Build the params schema for the query."""
        schema = {
            "type": "object",
            "properties": {
                "definition_id": {"type": "string"},
                "version": {"type": "string"},
                "time_filter": {
                    "type": "object",
                    "properties": {
                        "window": {"type": "string", "enum": ["QoQ", "YoY", "MTD", "QTD", "YTD"]},
                        "start_date": {"type": "string", "format": "date"},
                        "end_date": {"type": "string", "format": "date"},
                    }
                },
                "dimension_filters": {
                    "type": "object",
                    "properties": {}
                }
            }
        }

        # Add dimension filter schemas
        for dim in requested_dims:
            schema["properties"]["dimension_filters"]["properties"][dim] = {
                "type": "array",
                "items": {"type": "string"}
            }

        return schema
