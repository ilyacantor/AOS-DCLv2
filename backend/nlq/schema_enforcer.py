"""
Schema Enforcement Validator for NLQ Semantic Layer.

Validates:
- Event schema_json fields match expected types
- Binding mapping_json covers required event fields
- Time semantics are properly configured
- Definition specs have valid structure
"""

from typing import List, Dict, Any, Optional, Set, Tuple
from dataclasses import dataclass, field
from enum import Enum

from backend.utils.log_utils import get_logger
from backend.nlq.persistence import NLQPersistence
from backend.nlq.models import (
    CanonicalEvent,
    Binding,
    DefinitionVersion,
    DefinitionVersionSpec,
)

logger = get_logger(__name__)


class FieldType(Enum):
    """Valid field types for event schemas."""
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    DECIMAL = "decimal"
    BOOLEAN = "boolean"
    TIMESTAMP = "timestamp"
    DATE = "date"
    ARRAY = "array"
    OBJECT = "object"


class MeasureOp(Enum):
    """Valid measure operations."""
    SUM = "sum"
    COUNT = "count"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    COUNT_DISTINCT = "count_distinct"
    RATIO = "ratio"
    POINT_IN_TIME_SUM = "point_in_time_sum"
    COHORT_RETENTION = "cohort_retention"
    EVENT_SOURCED_BALANCE = "event_sourced_balance"
    AVG_DAYS_BETWEEN = "avg_days_between"
    PERIOD_OVER_PERIOD_GROWTH = "period_over_period_growth"
    NET_COUNT = "net_count"
    DIFFERENCE = "difference"


@dataclass
class SchemaViolation:
    """A single schema violation."""
    object_type: str
    object_id: str
    field: str
    message: str
    expected: Optional[str] = None
    actual: Optional[str] = None
    severity: str = "error"  # error, warning

    def to_dict(self) -> Dict[str, Any]:
        return {
            "object_type": self.object_type,
            "object_id": self.object_id,
            "field": self.field,
            "message": self.message,
            "expected": self.expected,
            "actual": self.actual,
            "severity": self.severity,
        }


@dataclass
class SchemaValidationResult:
    """Result of schema validation."""
    valid: bool
    violations: List[SchemaViolation] = field(default_factory=list)
    errors: int = 0
    warnings: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "valid": self.valid,
            "violations": [v.to_dict() for v in self.violations],
            "errors": self.errors,
            "warnings": self.warnings,
        }


class SchemaEnforcer:
    """
    Enforces schema rules across the semantic layer.

    Validates:
    1. Event schemas have valid structure and types
    2. Bindings map all required fields
    3. Definition specs are well-formed
    4. Time semantics are consistent
    """

    # Required fields for events
    REQUIRED_EVENT_FIELDS = {"event_id", "occurred_at"}

    # Recommended time fields
    TIME_FIELDS = {"occurred_at", "effective_at", "created_at", "updated_at"}

    # Valid filter operators
    VALID_FILTER_OPS = {
        "eq", "neq", "in", "not_in", "gt", "gte", "lt", "lte",
        "between", "like", "ilike", "is_null", "is_not_null"
    }

    # Valid grains
    VALID_GRAINS = {"day", "week", "month", "quarter", "year", "sprint", "hour"}

    def __init__(self, persistence: Optional[NLQPersistence] = None):
        """
        Initialize the schema enforcer.

        Args:
            persistence: NLQPersistence instance. Creates default if not provided.
        """
        self.persistence = persistence or NLQPersistence()

    def validate_all(self, tenant_id: str = "default") -> SchemaValidationResult:
        """
        Validate all schema rules for a tenant.

        Args:
            tenant_id: Tenant ID

        Returns:
            SchemaValidationResult with all violations
        """
        violations = []

        # Validate events
        events = self.persistence.get_events(tenant_id)
        for event in events:
            violations.extend(self.validate_event_schema(event))

        # Validate bindings
        bindings = self.persistence.get_bindings(tenant_id)
        for binding in bindings:
            event = self.persistence.get_event(binding.canonical_event_id, tenant_id)
            if event:
                violations.extend(self.validate_binding_mapping(binding, event))

        # Validate definition versions
        versions = self.persistence.get_definition_versions(tenant_id)
        for version in versions:
            violations.extend(self.validate_definition_spec(version))

        errors = len([v for v in violations if v.severity == "error"])
        warnings = len([v for v in violations if v.severity == "warning"])

        return SchemaValidationResult(
            valid=errors == 0,
            violations=violations,
            errors=errors,
            warnings=warnings,
        )

    def validate_event_schema(self, event: CanonicalEvent) -> List[SchemaViolation]:
        """
        Validate an event's schema.

        Checks:
        1. schema_json has 'fields' array
        2. Each field has name and type
        3. Types are valid FieldType values
        4. Required fields are present
        5. Time semantics are valid
        """
        violations = []

        # Check schema_json structure
        schema = event.schema_json
        if not schema:
            violations.append(SchemaViolation(
                object_type="event",
                object_id=event.id,
                field="schema_json",
                message="Event has no schema defined",
                severity="warning",
            ))
            return violations

        if "fields" not in schema:
            violations.append(SchemaViolation(
                object_type="event",
                object_id=event.id,
                field="schema_json.fields",
                message="Schema missing 'fields' array",
                severity="error",
            ))
            return violations

        fields = schema.get("fields", [])
        if not isinstance(fields, list):
            violations.append(SchemaViolation(
                object_type="event",
                object_id=event.id,
                field="schema_json.fields",
                message="'fields' must be an array",
                expected="array",
                actual=type(fields).__name__,
                severity="error",
            ))
            return violations

        # Track field names for duplicate detection
        field_names: Set[str] = set()

        for i, field_def in enumerate(fields):
            if not isinstance(field_def, dict):
                violations.append(SchemaViolation(
                    object_type="event",
                    object_id=event.id,
                    field=f"schema_json.fields[{i}]",
                    message="Field definition must be an object",
                    severity="error",
                ))
                continue

            # Check name
            if "name" not in field_def:
                violations.append(SchemaViolation(
                    object_type="event",
                    object_id=event.id,
                    field=f"schema_json.fields[{i}]",
                    message="Field missing 'name' property",
                    severity="error",
                ))
            else:
                name = field_def["name"]
                if name in field_names:
                    violations.append(SchemaViolation(
                        object_type="event",
                        object_id=event.id,
                        field=f"schema_json.fields[{i}].name",
                        message=f"Duplicate field name: {name}",
                        severity="error",
                    ))
                field_names.add(name)

            # Check type
            if "type" not in field_def:
                violations.append(SchemaViolation(
                    object_type="event",
                    object_id=event.id,
                    field=f"schema_json.fields[{i}]",
                    message="Field missing 'type' property",
                    severity="error",
                ))
            else:
                field_type = field_def["type"]
                valid_types = {t.value for t in FieldType}
                if field_type not in valid_types:
                    violations.append(SchemaViolation(
                        object_type="event",
                        object_id=event.id,
                        field=f"schema_json.fields[{i}].type",
                        message=f"Invalid field type: {field_type}",
                        expected=f"one of {valid_types}",
                        actual=field_type,
                        severity="warning",
                    ))

        # Check required fields
        for required_field in self.REQUIRED_EVENT_FIELDS:
            if required_field not in field_names:
                violations.append(SchemaViolation(
                    object_type="event",
                    object_id=event.id,
                    field="schema_json.fields",
                    message=f"Missing required field: {required_field}",
                    severity="warning",
                ))

        # Validate time semantics
        time_semantics = event.time_semantics_json
        if time_semantics:
            violations.extend(self._validate_time_semantics(event.id, time_semantics, field_names))

        return violations

    def _validate_time_semantics(
        self,
        event_id: str,
        time_semantics: Dict[str, Any],
        field_names: Set[str]
    ) -> List[SchemaViolation]:
        """Validate time semantics configuration."""
        violations = []

        # Check occurred_at reference
        if "occurred_at" in time_semantics:
            ref_field = time_semantics["occurred_at"]
            if ref_field not in field_names and ref_field not in {"created_timestamp", "system_timestamp"}:
                violations.append(SchemaViolation(
                    object_type="event",
                    object_id=event_id,
                    field="time_semantics_json.occurred_at",
                    message=f"Referenced field '{ref_field}' not in schema",
                    severity="warning",
                ))

        # Check effective_at reference
        if "effective_at" in time_semantics:
            ref_field = time_semantics["effective_at"]
            if ref_field not in field_names and not ref_field.endswith("_date"):
                violations.append(SchemaViolation(
                    object_type="event",
                    object_id=event_id,
                    field="time_semantics_json.effective_at",
                    message=f"Referenced field '{ref_field}' not in schema",
                    severity="warning",
                ))

        # Check calendar type
        if "calendar" in time_semantics:
            calendar = time_semantics["calendar"]
            valid_calendars = {"fiscal", "calendar", "iso", "custom"}
            if calendar not in valid_calendars:
                violations.append(SchemaViolation(
                    object_type="event",
                    object_id=event_id,
                    field="time_semantics_json.calendar",
                    message=f"Invalid calendar type: {calendar}",
                    expected=f"one of {valid_calendars}",
                    actual=calendar,
                    severity="warning",
                ))

        return violations

    def validate_binding_mapping(
        self,
        binding: Binding,
        event: CanonicalEvent
    ) -> List[SchemaViolation]:
        """
        Validate a binding's field mapping against the event schema.

        Checks:
        1. Mapped fields exist in event schema
        2. Required event fields are mapped
        3. Time fields are properly mapped
        """
        violations = []

        # Get event field names
        event_fields: Set[str] = set()
        if event.schema_json and "fields" in event.schema_json:
            event_fields = {f["name"] for f in event.schema_json.get("fields", [])}

        # Check mapping targets
        mapping = binding.mapping_json
        for source_field, target_field in mapping.items():
            if target_field not in event_fields:
                violations.append(SchemaViolation(
                    object_type="binding",
                    object_id=binding.id,
                    field=f"mapping_json.{source_field}",
                    message=f"Target field '{target_field}' not in event schema",
                    severity="warning",
                ))

        # Check required fields are mapped
        mapped_targets = set(mapping.values())
        for required_field in self.REQUIRED_EVENT_FIELDS:
            if required_field in event_fields and required_field not in mapped_targets:
                violations.append(SchemaViolation(
                    object_type="binding",
                    object_id=binding.id,
                    field="mapping_json",
                    message=f"Required field '{required_field}' is not mapped",
                    severity="warning",
                ))

        # Check time fields
        has_time_mapping = any(t in mapped_targets for t in self.TIME_FIELDS)
        if not has_time_mapping:
            violations.append(SchemaViolation(
                object_type="binding",
                object_id=binding.id,
                field="mapping_json",
                message="No time field is mapped (occurred_at, effective_at, etc.)",
                severity="warning",
            ))

        return violations

    def validate_definition_spec(self, version: DefinitionVersion) -> List[SchemaViolation]:
        """
        Validate a definition version's spec.

        Checks:
        1. required_events is non-empty
        2. measure has valid op and field
        3. filters use valid operators
        4. allowed_grains are valid
        5. time_field is specified
        """
        violations = []
        spec = version.spec

        # Check required_events
        if not spec.required_events:
            violations.append(SchemaViolation(
                object_type="definition_version",
                object_id=version.id,
                field="spec.required_events",
                message="Definition has no required events",
                severity="warning",
            ))

        # Check measure
        measure = spec.measure
        if not measure:
            violations.append(SchemaViolation(
                object_type="definition_version",
                object_id=version.id,
                field="spec.measure",
                message="Definition has no measure defined",
                severity="error",
            ))
        else:
            # Check op
            if "op" not in measure:
                violations.append(SchemaViolation(
                    object_type="definition_version",
                    object_id=version.id,
                    field="spec.measure.op",
                    message="Measure missing 'op' (operation)",
                    severity="error",
                ))
            else:
                op = measure["op"]
                valid_ops = {m.value for m in MeasureOp}
                if op not in valid_ops:
                    violations.append(SchemaViolation(
                        object_type="definition_version",
                        object_id=version.id,
                        field="spec.measure.op",
                        message=f"Invalid measure operation: {op}",
                        expected=f"one of {valid_ops}",
                        actual=op,
                        severity="warning",
                    ))

            # Check field
            if "field" not in measure:
                violations.append(SchemaViolation(
                    object_type="definition_version",
                    object_id=version.id,
                    field="spec.measure.field",
                    message="Measure missing 'field'",
                    severity="warning",
                ))

        # Check filters
        filters = spec.filters
        if filters:
            for field_name, filter_spec in filters.items():
                if isinstance(filter_spec, dict) and "op" in filter_spec:
                    op = filter_spec["op"]
                    if op not in self.VALID_FILTER_OPS:
                        violations.append(SchemaViolation(
                            object_type="definition_version",
                            object_id=version.id,
                            field=f"spec.filters.{field_name}.op",
                            message=f"Invalid filter operation: {op}",
                            expected=f"one of {self.VALID_FILTER_OPS}",
                            actual=op,
                            severity="warning",
                        ))

        # Check allowed_grains
        for grain in spec.allowed_grains:
            if grain not in self.VALID_GRAINS:
                violations.append(SchemaViolation(
                    object_type="definition_version",
                    object_id=version.id,
                    field="spec.allowed_grains",
                    message=f"Invalid grain: {grain}",
                    expected=f"one of {self.VALID_GRAINS}",
                    actual=grain,
                    severity="warning",
                ))

        # Check time_field
        if not spec.time_field:
            violations.append(SchemaViolation(
                object_type="definition_version",
                object_id=version.id,
                field="spec.time_field",
                message="Definition has no time_field specified",
                severity="warning",
            ))

        return violations

    def validate_event(
        self,
        event_id: str,
        schema_json: Dict[str, Any],
        time_semantics_json: Dict[str, Any]
    ) -> Tuple[bool, List[str]]:
        """
        Validate event schema before creation.

        Args:
            event_id: Proposed event ID
            schema_json: Event schema
            time_semantics_json: Time semantics

        Returns:
            Tuple of (is_valid, list of error messages)
        """
        # Create temporary event for validation
        temp_event = CanonicalEvent(
            id=event_id,
            tenant_id="temp",
            schema_json=schema_json,
            time_semantics_json=time_semantics_json,
        )

        violations = self.validate_event_schema(temp_event)
        errors = [v.message for v in violations if v.severity == "error"]

        return len(errors) == 0, errors

    def validate_binding(
        self,
        binding_id: str,
        canonical_event_id: str,
        mapping_json: Dict[str, str],
        tenant_id: str = "default"
    ) -> Tuple[bool, List[str]]:
        """
        Validate binding before creation.

        Args:
            binding_id: Proposed binding ID
            canonical_event_id: Target event
            mapping_json: Field mappings
            tenant_id: Tenant ID

        Returns:
            Tuple of (is_valid, list of error messages)
        """
        event = self.persistence.get_event(canonical_event_id, tenant_id)
        if not event:
            return False, [f"Event '{canonical_event_id}' not found"]

        temp_binding = Binding(
            id=binding_id,
            tenant_id=tenant_id,
            source_system="temp",
            canonical_event_id=canonical_event_id,
            mapping_json=mapping_json,
        )

        violations = self.validate_binding_mapping(temp_binding, event)
        errors = [v.message for v in violations if v.severity == "error"]

        return len(errors) == 0, errors

    def validate_definition_spec_dict(
        self,
        definition_id: str,
        spec: Dict[str, Any]
    ) -> Tuple[bool, List[str]]:
        """
        Validate definition spec before creation.

        Args:
            definition_id: Proposed definition ID
            spec: Spec dictionary

        Returns:
            Tuple of (is_valid, list of error messages)
        """
        temp_spec = DefinitionVersionSpec(**spec)
        temp_version = DefinitionVersion(
            id=f"{definition_id}_temp",
            tenant_id="temp",
            definition_id=definition_id,
            version="temp",
            status="draft",
            spec=temp_spec,
        )

        violations = self.validate_definition_spec(temp_version)
        errors = [v.message for v in violations if v.severity == "error"]

        return len(errors) == 0, errors

    def suggest_schema_improvements(
        self,
        event_id: str,
        tenant_id: str = "default"
    ) -> List[Dict[str, Any]]:
        """
        Suggest improvements to an event schema.

        Args:
            event_id: Event ID
            tenant_id: Tenant ID

        Returns:
            List of improvement suggestions
        """
        suggestions = []

        event = self.persistence.get_event(event_id, tenant_id)
        if not event:
            return suggestions

        schema = event.schema_json or {}
        fields = schema.get("fields", [])
        field_names = {f.get("name") for f in fields if f.get("name")}

        # Suggest effective_at if not present
        if "effective_at" not in field_names and "occurred_at" in field_names:
            suggestions.append({
                "type": "add_field",
                "field": "effective_at",
                "field_type": "timestamp",
                "reason": "Consider adding effective_at for business-date semantics separate from occurred_at",
            })

        # Suggest primary identifier if missing
        common_id_fields = {"id", "event_id", f"{event_id}_id"}
        if not any(f in field_names for f in common_id_fields):
            suggestions.append({
                "type": "add_field",
                "field": "event_id",
                "field_type": "string",
                "reason": "Add a unique identifier field for traceability",
            })

        # Suggest currency field for monetary events
        monetary_indicators = {"amount", "total", "price", "cost", "revenue"}
        has_monetary = any(ind in f.lower() for f in field_names for ind in monetary_indicators)
        if has_monetary and "currency" not in field_names:
            suggestions.append({
                "type": "add_field",
                "field": "currency",
                "field_type": "string",
                "reason": "Add currency field for multi-currency support",
            })

        return suggestions
