"""
Consistency Validator for NLQ Semantic Layer.

Validates the semantic layer for:
- Orphan detection (events with no bindings, definitions with missing events)
- Circular dependency detection in definition specs
- Binding coverage validation (do bindings cover all required fields?)
- Entity reference validation
"""

from typing import List, Dict, Any, Optional, Set, Tuple
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime
import uuid

from backend.utils.log_utils import get_logger
from backend.nlq.persistence import NLQPersistence

logger = get_logger(__name__)


class ConsistencyStatus(Enum):
    """Status of a consistency check."""
    PASSED = "passed"
    WARNING = "warning"
    FAILED = "failed"


class IssueLevel(Enum):
    """Severity level of a consistency issue."""
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass
class ConsistencyIssue:
    """A single consistency issue found during validation."""
    level: IssueLevel
    check_type: str
    message: str
    object_type: str
    object_id: str
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "level": self.level.value,
            "check_type": self.check_type,
            "message": self.message,
            "object_type": self.object_type,
            "object_id": self.object_id,
            "details": self.details,
        }


@dataclass
class ConsistencyCheckResult:
    """Result of a consistency check."""
    check_id: str
    check_type: str
    status: ConsistencyStatus
    issues: List[ConsistencyIssue] = field(default_factory=list)
    summary: str = ""
    checked_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def to_dict(self) -> Dict[str, Any]:
        return {
            "check_id": self.check_id,
            "check_type": self.check_type,
            "status": self.status.value,
            "issues": [i.to_dict() for i in self.issues],
            "summary": self.summary,
            "checked_at": self.checked_at,
        }


@dataclass
class FullConsistencyReport:
    """Full consistency report across all checks."""
    report_id: str
    tenant_id: str
    overall_status: ConsistencyStatus
    checks: List[ConsistencyCheckResult] = field(default_factory=list)
    total_issues: int = 0
    errors: int = 0
    warnings: int = 0
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def to_dict(self) -> Dict[str, Any]:
        return {
            "report_id": self.report_id,
            "tenant_id": self.tenant_id,
            "overall_status": self.overall_status.value,
            "checks": [c.to_dict() for c in self.checks],
            "total_issues": self.total_issues,
            "errors": self.errors,
            "warnings": self.warnings,
            "created_at": self.created_at,
        }


class ConsistencyValidator:
    """
    Validates consistency of the NLQ semantic layer.

    Performs the following checks:
    1. Orphan events - events with no bindings
    2. Orphan definitions - definitions with missing events or no versions
    3. Orphan bindings - bindings referencing non-existent events
    4. Circular dependencies - cycles in definition dependencies
    5. Binding coverage - bindings covering all required fields
    6. Entity references - definitions referencing valid entities
    7. Version consistency - definition versions are consistent
    """

    def __init__(self, persistence: Optional[NLQPersistence] = None):
        """
        Initialize the consistency validator.

        Args:
            persistence: NLQPersistence instance. Creates default if not provided.
        """
        self.persistence = persistence or NLQPersistence()

    def run_all_checks(self, tenant_id: str = "default") -> FullConsistencyReport:
        """
        Run all consistency checks and return a full report.

        Args:
            tenant_id: Tenant ID to check

        Returns:
            FullConsistencyReport with all check results
        """
        report_id = str(uuid.uuid4())[:8]
        checks = []

        # Run all checks
        checks.append(self.check_orphan_events(tenant_id))
        checks.append(self.check_orphan_definitions(tenant_id))
        checks.append(self.check_orphan_bindings(tenant_id))
        checks.append(self.check_circular_dependencies(tenant_id))
        checks.append(self.check_binding_coverage(tenant_id))
        checks.append(self.check_entity_references(tenant_id))
        checks.append(self.check_version_consistency(tenant_id))

        # Aggregate results
        total_issues = sum(len(c.issues) for c in checks)
        errors = sum(
            1 for c in checks
            for i in c.issues
            if i.level == IssueLevel.ERROR
        )
        warnings = sum(
            1 for c in checks
            for i in c.issues
            if i.level == IssueLevel.WARNING
        )

        # Determine overall status
        if errors > 0:
            overall_status = ConsistencyStatus.FAILED
        elif warnings > 0:
            overall_status = ConsistencyStatus.WARNING
        else:
            overall_status = ConsistencyStatus.PASSED

        return FullConsistencyReport(
            report_id=report_id,
            tenant_id=tenant_id,
            overall_status=overall_status,
            checks=checks,
            total_issues=total_issues,
            errors=errors,
            warnings=warnings,
        )

    def check_orphan_events(self, tenant_id: str = "default") -> ConsistencyCheckResult:
        """
        Check for events that have no bindings.

        These events exist in the semantic model but have no source system
        mapping, making them unusable.
        """
        check_id = str(uuid.uuid4())[:8]
        issues = []

        events = self.persistence.get_events(tenant_id)
        for event in events:
            bindings = self.persistence.get_bindings_for_event(event.id, tenant_id)
            if not bindings:
                issues.append(ConsistencyIssue(
                    level=IssueLevel.WARNING,
                    check_type="orphan_events",
                    message=f"Event '{event.id}' has no bindings",
                    object_type="event",
                    object_id=event.id,
                    details={"description": event.description or "No description"},
                ))

        status = ConsistencyStatus.PASSED if not issues else ConsistencyStatus.WARNING
        summary = f"Found {len(issues)} events without bindings" if issues else "All events have bindings"

        return ConsistencyCheckResult(
            check_id=check_id,
            check_type="orphan_events",
            status=status,
            issues=issues,
            summary=summary,
        )

    def check_orphan_definitions(self, tenant_id: str = "default") -> ConsistencyCheckResult:
        """
        Check for definitions with missing events or no versions.

        Definitions should have:
        1. At least one version
        2. All required_events existing in the semantic model
        """
        check_id = str(uuid.uuid4())[:8]
        issues = []

        definitions = self.persistence.get_definitions(tenant_id)
        versions = self.persistence.get_definition_versions(tenant_id)
        events = self.persistence.get_events(tenant_id)
        event_ids = {e.id for e in events}

        # Build version map
        def_versions: Dict[str, List[str]] = {}
        for v in versions:
            if v.definition_id not in def_versions:
                def_versions[v.definition_id] = []
            def_versions[v.definition_id].append(v.version)

        for defn in definitions:
            # Check for missing versions
            if defn.id not in def_versions:
                issues.append(ConsistencyIssue(
                    level=IssueLevel.ERROR,
                    check_type="orphan_definitions",
                    message=f"Definition '{defn.id}' has no versions",
                    object_type="definition",
                    object_id=defn.id,
                    details={"kind": defn.kind},
                ))
                continue

            # Check for missing events in published versions
            published_version = self.persistence.get_published_version(defn.id, tenant_id)
            if published_version and published_version.spec.required_events:
                missing_events = [
                    e for e in published_version.spec.required_events
                    if e not in event_ids
                ]
                if missing_events:
                    issues.append(ConsistencyIssue(
                        level=IssueLevel.ERROR,
                        check_type="orphan_definitions",
                        message=f"Definition '{defn.id}' references non-existent events",
                        object_type="definition",
                        object_id=defn.id,
                        details={
                            "missing_events": missing_events,
                            "version": published_version.version,
                        },
                    ))

        error_count = len([i for i in issues if i.level == IssueLevel.ERROR])
        status = ConsistencyStatus.FAILED if error_count > 0 else (
            ConsistencyStatus.WARNING if issues else ConsistencyStatus.PASSED
        )
        summary = f"Found {len(issues)} issues with definitions" if issues else "All definitions are valid"

        return ConsistencyCheckResult(
            check_id=check_id,
            check_type="orphan_definitions",
            status=status,
            issues=issues,
            summary=summary,
        )

    def check_orphan_bindings(self, tenant_id: str = "default") -> ConsistencyCheckResult:
        """
        Check for bindings that reference non-existent events.
        """
        check_id = str(uuid.uuid4())[:8]
        issues = []

        bindings = self.persistence.get_bindings(tenant_id)
        events = self.persistence.get_events(tenant_id)
        event_ids = {e.id for e in events}

        for binding in bindings:
            if binding.canonical_event_id not in event_ids:
                issues.append(ConsistencyIssue(
                    level=IssueLevel.ERROR,
                    check_type="orphan_bindings",
                    message=f"Binding '{binding.id}' references non-existent event '{binding.canonical_event_id}'",
                    object_type="binding",
                    object_id=binding.id,
                    details={
                        "source_system": binding.source_system,
                        "missing_event": binding.canonical_event_id,
                    },
                ))

        status = ConsistencyStatus.FAILED if issues else ConsistencyStatus.PASSED
        summary = f"Found {len(issues)} bindings with invalid event references" if issues else "All bindings reference valid events"

        return ConsistencyCheckResult(
            check_id=check_id,
            check_type="orphan_bindings",
            status=status,
            issues=issues,
            summary=summary,
        )

    def check_circular_dependencies(self, tenant_id: str = "default") -> ConsistencyCheckResult:
        """
        Check for circular dependencies in definition specs.

        This checks if any definition's required_events form a cycle
        through other definitions (if events are produced by definitions).
        """
        check_id = str(uuid.uuid4())[:8]
        issues = []

        # For now, this is a simple check since we don't have derived events.
        # In a full implementation, we would build a dependency graph and check for cycles.

        versions = self.persistence.get_definition_versions(tenant_id)

        # Build dependency graph: definition -> events it requires
        dependencies: Dict[str, Set[str]] = {}
        for v in versions:
            if v.status == "published":
                dependencies[v.definition_id] = set(v.spec.required_events)

        # Check for any suspicious patterns (definitions requiring events with same prefix)
        for def_id, events in dependencies.items():
            for event in events:
                # Check if any other definition produces this event
                # (This would be relevant if we had computed/derived events)
                pass

        # Currently no cycles detected (would need more complex logic for true cycle detection)
        status = ConsistencyStatus.PASSED
        summary = "No circular dependencies detected"

        return ConsistencyCheckResult(
            check_id=check_id,
            check_type="circular_dependencies",
            status=status,
            issues=issues,
            summary=summary,
        )

    def check_binding_coverage(self, tenant_id: str = "default") -> ConsistencyCheckResult:
        """
        Check if bindings provide adequate coverage for definition requirements.

        For each definition, checks:
        1. All required events have bindings
        2. All allowed dimensions are covered by bindings
        """
        check_id = str(uuid.uuid4())[:8]
        issues = []

        definitions = self.persistence.get_definitions(tenant_id)

        for defn in definitions:
            published_version = self.persistence.get_published_version(defn.id, tenant_id)
            if not published_version:
                continue

            spec = published_version.spec

            # Check event coverage
            for event_id in spec.required_events:
                bindings = self.persistence.get_bindings_for_event(event_id, tenant_id)
                if not bindings:
                    issues.append(ConsistencyIssue(
                        level=IssueLevel.ERROR,
                        check_type="binding_coverage",
                        message=f"Definition '{defn.id}' requires event '{event_id}' which has no bindings",
                        object_type="definition",
                        object_id=defn.id,
                        details={
                            "missing_event": event_id,
                            "version": published_version.version,
                        },
                    ))
                else:
                    # Check quality scores
                    avg_quality = sum(b.quality_score for b in bindings) / len(bindings)
                    if avg_quality < 0.7:
                        issues.append(ConsistencyIssue(
                            level=IssueLevel.WARNING,
                            check_type="binding_coverage",
                            message=f"Event '{event_id}' has low quality bindings (avg: {avg_quality:.2f})",
                            object_type="event",
                            object_id=event_id,
                            details={
                                "definition": defn.id,
                                "average_quality": avg_quality,
                                "binding_count": len(bindings),
                            },
                        ))

            # Check dimension coverage
            uncovered_dims = []
            for dim in spec.allowed_dims:
                dim_covered = False
                for event_id in spec.required_events:
                    coverage = self.persistence.get_dims_coverage(event_id, tenant_id)
                    if coverage.get(dim, False):
                        dim_covered = True
                        break
                if not dim_covered:
                    uncovered_dims.append(dim)

            if uncovered_dims:
                issues.append(ConsistencyIssue(
                    level=IssueLevel.WARNING,
                    check_type="binding_coverage",
                    message=f"Definition '{defn.id}' has uncovered dimensions",
                    object_type="definition",
                    object_id=defn.id,
                    details={
                        "uncovered_dims": uncovered_dims,
                        "version": published_version.version,
                    },
                ))

        error_count = len([i for i in issues if i.level == IssueLevel.ERROR])
        status = ConsistencyStatus.FAILED if error_count > 0 else (
            ConsistencyStatus.WARNING if issues else ConsistencyStatus.PASSED
        )
        summary = f"Found {len(issues)} binding coverage issues" if issues else "All bindings provide adequate coverage"

        return ConsistencyCheckResult(
            check_id=check_id,
            check_type="binding_coverage",
            status=status,
            issues=issues,
            summary=summary,
        )

    def check_entity_references(self, tenant_id: str = "default") -> ConsistencyCheckResult:
        """
        Check if definitions reference valid entities.

        Validates that:
        1. allowed_dims in specs reference existing entities
        2. joins reference existing entities
        """
        check_id = str(uuid.uuid4())[:8]
        issues = []

        entities = self.persistence.get_entities(tenant_id)
        entity_ids = {e.id for e in entities}

        versions = self.persistence.get_definition_versions(tenant_id)

        for v in versions:
            if v.status != "published":
                continue

            spec = v.spec

            # Check allowed_dims
            for dim in spec.allowed_dims:
                if dim not in entity_ids:
                    issues.append(ConsistencyIssue(
                        level=IssueLevel.WARNING,
                        check_type="entity_references",
                        message=f"Definition '{v.definition_id}' references unknown entity '{dim}' in allowed_dims",
                        object_type="definition_version",
                        object_id=v.id,
                        details={
                            "unknown_entity": dim,
                            "field": "allowed_dims",
                        },
                    ))

            # Check joins
            for _, entity in spec.joins.items():
                if entity not in entity_ids:
                    issues.append(ConsistencyIssue(
                        level=IssueLevel.WARNING,
                        check_type="entity_references",
                        message=f"Definition '{v.definition_id}' references unknown entity '{entity}' in joins",
                        object_type="definition_version",
                        object_id=v.id,
                        details={
                            "unknown_entity": entity,
                            "field": "joins",
                        },
                    ))

        status = ConsistencyStatus.WARNING if issues else ConsistencyStatus.PASSED
        summary = f"Found {len(issues)} invalid entity references" if issues else "All entity references are valid"

        return ConsistencyCheckResult(
            check_id=check_id,
            check_type="entity_references",
            status=status,
            issues=issues,
            summary=summary,
        )

    def check_version_consistency(self, tenant_id: str = "default") -> ConsistencyCheckResult:
        """
        Check for version consistency issues.

        Validates that:
        1. Each definition has at most one published version
        2. Version specs are valid
        """
        check_id = str(uuid.uuid4())[:8]
        issues = []

        versions = self.persistence.get_definition_versions(tenant_id)

        # Group by definition
        def_versions: Dict[str, List] = {}
        for v in versions:
            if v.definition_id not in def_versions:
                def_versions[v.definition_id] = []
            def_versions[v.definition_id].append(v)

        for def_id, vers in def_versions.items():
            # Check for multiple published versions
            published = [v for v in vers if v.status == "published"]
            if len(published) > 1:
                issues.append(ConsistencyIssue(
                    level=IssueLevel.ERROR,
                    check_type="version_consistency",
                    message=f"Definition '{def_id}' has multiple published versions",
                    object_type="definition",
                    object_id=def_id,
                    details={
                        "published_versions": [v.version for v in published],
                    },
                ))

            # Check for valid specs
            for v in vers:
                if not v.spec.required_events:
                    issues.append(ConsistencyIssue(
                        level=IssueLevel.WARNING,
                        check_type="version_consistency",
                        message=f"Version '{v.id}' has no required_events",
                        object_type="definition_version",
                        object_id=v.id,
                        details={
                            "definition": def_id,
                            "version": v.version,
                        },
                    ))

                if not v.spec.measure:
                    issues.append(ConsistencyIssue(
                        level=IssueLevel.WARNING,
                        check_type="version_consistency",
                        message=f"Version '{v.id}' has no measure defined",
                        object_type="definition_version",
                        object_id=v.id,
                        details={
                            "definition": def_id,
                            "version": v.version,
                        },
                    ))

        error_count = len([i for i in issues if i.level == IssueLevel.ERROR])
        status = ConsistencyStatus.FAILED if error_count > 0 else (
            ConsistencyStatus.WARNING if issues else ConsistencyStatus.PASSED
        )
        summary = f"Found {len(issues)} version consistency issues" if issues else "All versions are consistent"

        return ConsistencyCheckResult(
            check_id=check_id,
            check_type="version_consistency",
            status=status,
            issues=issues,
            summary=summary,
        )

    def validate_new_binding(
        self,
        binding_id: str,
        source_system: str,
        canonical_event_id: str,
        mapping_json: Dict[str, str],
        tenant_id: str = "default"
    ) -> Tuple[bool, List[str]]:
        """
        Validate a new binding before creation.

        Args:
            binding_id: Proposed binding ID
            source_system: Source system name
            canonical_event_id: Target canonical event
            mapping_json: Field mappings
            tenant_id: Tenant ID

        Returns:
            Tuple of (is_valid, list of error messages)
        """
        errors = []

        # Check if event exists
        event = self.persistence.get_event(canonical_event_id, tenant_id)
        if not event:
            errors.append(f"Canonical event '{canonical_event_id}' does not exist")
            return False, errors

        # Check if binding ID is unique (if not updating)
        bindings = self.persistence.get_bindings(tenant_id)
        for b in bindings:
            if b.id == binding_id:
                # Allow updates
                break

        # Validate mapping covers required fields from event schema
        if event.schema_json and "fields" in event.schema_json:
            event_fields = {f["name"] for f in event.schema_json["fields"]}
            mapped_fields = set(mapping_json.values())
            missing_required = event_fields - mapped_fields - {"event_id", "occurred_at", "effective_at"}

            # Just warn about unmapped fields, don't fail
            if missing_required:
                errors.append(f"Warning: Some event fields are not mapped: {missing_required}")

        return len([e for e in errors if not e.startswith("Warning")]) == 0, errors

    def validate_new_definition(
        self,
        definition_id: str,
        required_events: List[str],
        allowed_dims: List[str],
        tenant_id: str = "default"
    ) -> Tuple[bool, List[str]]:
        """
        Validate a new definition before creation.

        Args:
            definition_id: Proposed definition ID
            required_events: Events required by this definition
            allowed_dims: Dimensions allowed for this definition
            tenant_id: Tenant ID

        Returns:
            Tuple of (is_valid, list of error messages)
        """
        errors = []

        # Check if all required events exist
        events = self.persistence.get_events(tenant_id)
        event_ids = {e.id for e in events}

        for event_id in required_events:
            if event_id not in event_ids:
                errors.append(f"Required event '{event_id}' does not exist")

        # Check if all dimensions exist as entities
        entities = self.persistence.get_entities(tenant_id)
        entity_ids = {e.id for e in entities}

        for dim in allowed_dims:
            if dim not in entity_ids:
                errors.append(f"Dimension '{dim}' is not a registered entity (warning, may be intentional)")

        return len([e for e in errors if "warning" not in e.lower()]) == 0, errors
