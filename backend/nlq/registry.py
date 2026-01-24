"""
Definition Registry Service for NLQ Semantic Layer.

Provides:
- Admin API to list/search/manage definitions
- Definition discovery and search
- Publish/deprecate workflow
- Catalog management
"""

from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

from backend.utils.log_utils import get_logger
from backend.nlq.persistence import NLQPersistence
from backend.nlq.models import (
    CanonicalEvent,
    Entity,
    Binding,
    Definition,
    DefinitionVersion,
    DefinitionVersionSpec,
    ProofHook,
)
from backend.nlq.consistency import ConsistencyValidator
from backend.nlq.schema_enforcer import SchemaEnforcer
from backend.nlq.lineage import LineageService

logger = get_logger(__name__)


class DefinitionStatus(Enum):
    """Status of a definition in the registry."""
    DRAFT = "draft"
    PUBLISHED = "published"
    DEPRECATED = "deprecated"


@dataclass
class DefinitionSummary:
    """Summary view of a definition for listing."""
    id: str
    kind: str
    pack: Optional[str]
    description: Optional[str]
    status: str
    version: Optional[str]
    event_count: int
    dim_count: int
    binding_coverage: float
    updated_at: Optional[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "kind": self.kind,
            "pack": self.pack,
            "description": self.description,
            "status": self.status,
            "version": self.version,
            "event_count": self.event_count,
            "dim_count": self.dim_count,
            "binding_coverage": self.binding_coverage,
            "updated_at": self.updated_at,
        }


@dataclass
class DefinitionDetail:
    """Detailed view of a definition."""
    definition: Definition
    published_version: Optional[DefinitionVersion]
    all_versions: List[DefinitionVersion]
    events: List[CanonicalEvent]
    bindings: List[Binding]
    proof_hooks: List[ProofHook]
    lineage: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "definition": {
                "id": self.definition.id,
                "kind": self.definition.kind,
                "description": self.definition.description,
                "default_time_semantics": self.definition.default_time_semantics_json,
            },
            "published_version": {
                "id": self.published_version.id,
                "version": self.published_version.version,
                "status": self.published_version.status,
                "spec": self.published_version.spec.model_dump() if self.published_version.spec else {},
            } if self.published_version else None,
            "all_versions": [
                {"id": v.id, "version": v.version, "status": v.status}
                for v in self.all_versions
            ],
            "events": [
                {"id": e.id, "description": e.description}
                for e in self.events
            ],
            "bindings": [
                {
                    "id": b.id,
                    "source_system": b.source_system,
                    "quality_score": b.quality_score,
                }
                for b in self.bindings
            ],
            "proof_hooks": [
                {"id": h.id, "availability_score": h.availability_score}
                for h in self.proof_hooks
            ],
            "lineage": self.lineage,
        }


@dataclass
class CatalogStats:
    """Statistics about the definition catalog."""
    total_definitions: int
    published_definitions: int
    draft_definitions: int
    deprecated_definitions: int
    definitions_by_pack: Dict[str, int]
    definitions_by_kind: Dict[str, int]
    total_events: int
    total_bindings: int
    total_entities: int
    avg_binding_coverage: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_definitions": self.total_definitions,
            "published_definitions": self.published_definitions,
            "draft_definitions": self.draft_definitions,
            "deprecated_definitions": self.deprecated_definitions,
            "definitions_by_pack": self.definitions_by_pack,
            "definitions_by_kind": self.definitions_by_kind,
            "total_events": self.total_events,
            "total_bindings": self.total_bindings,
            "total_entities": self.total_entities,
            "avg_binding_coverage": self.avg_binding_coverage,
        }


class DefinitionRegistry:
    """
    Registry service for managing semantic layer definitions.

    Provides:
    - List/search definitions with filtering
    - Get definition details with full context
    - Publish/deprecate workflow
    - Validation before registration
    - Catalog statistics
    """

    def __init__(self, persistence: Optional[NLQPersistence] = None):
        """
        Initialize the definition registry.

        Args:
            persistence: NLQPersistence instance. Creates default if not provided.
        """
        self.persistence = persistence or NLQPersistence()
        self.consistency = ConsistencyValidator(self.persistence)
        self.schema_enforcer = SchemaEnforcer(self.persistence)
        self.lineage = LineageService(self.persistence)

    # =========================================================================
    # List and Search
    # =========================================================================

    def list_definitions(
        self,
        tenant_id: str = "default",
        pack: Optional[str] = None,
        kind: Optional[str] = None,
        status: Optional[str] = None,
        search: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Tuple[List[DefinitionSummary], int]:
        """
        List definitions with optional filtering.

        Args:
            tenant_id: Tenant ID
            pack: Filter by pack (cfo, cto, coo, ceo)
            kind: Filter by kind (metric, view)
            status: Filter by status (draft, published, deprecated)
            search: Search string for ID or description
            limit: Max results to return
            offset: Results offset for pagination

        Returns:
            Tuple of (list of summaries, total count)
        """
        definitions = self.persistence.get_definitions(tenant_id)
        versions = self.persistence.get_definition_versions(tenant_id)

        # Build version map
        version_map: Dict[str, List[DefinitionVersion]] = {}
        for v in versions:
            if v.definition_id not in version_map:
                version_map[v.definition_id] = []
            version_map[v.definition_id].append(v)

        # Filter and build summaries
        summaries = []
        for defn in definitions:
            # Get pack from definition if available
            defn_pack = getattr(defn, 'pack', None)

            # Apply filters
            if pack and defn_pack != pack:
                continue
            if kind and defn.kind != kind:
                continue

            # Get published version
            def_versions = version_map.get(defn.id, [])
            published = next((v for v in def_versions if v.status == "published"), None)

            # Determine status
            if published:
                def_status = "published"
            elif def_versions:
                def_status = def_versions[0].status
            else:
                def_status = "no_version"

            if status and def_status != status:
                continue

            # Search filter
            if search:
                search_lower = search.lower()
                if (search_lower not in defn.id.lower() and
                    (not defn.description or search_lower not in defn.description.lower())):
                    continue

            # Calculate binding coverage
            coverage = 0.0
            event_count = 0
            dim_count = 0

            if published and published.spec:
                event_count = len(published.spec.required_events)
                dim_count = len(published.spec.allowed_dims)

                # Check binding coverage
                covered_events = 0
                for event_id in published.spec.required_events:
                    bindings = self.persistence.get_bindings_for_event(event_id, tenant_id)
                    if bindings:
                        covered_events += 1

                if event_count > 0:
                    coverage = covered_events / event_count

            summaries.append(DefinitionSummary(
                id=defn.id,
                kind=defn.kind,
                pack=defn_pack,
                description=defn.description,
                status=def_status,
                version=published.version if published else None,
                event_count=event_count,
                dim_count=dim_count,
                binding_coverage=coverage,
                updated_at=defn.updated_at,
            ))

        # Sort by ID
        summaries.sort(key=lambda s: s.id)

        # Pagination
        total = len(summaries)
        summaries = summaries[offset:offset + limit]

        return summaries, total

    def search_definitions(
        self,
        query: str,
        tenant_id: str = "default",
        limit: int = 10
    ) -> List[DefinitionSummary]:
        """
        Search definitions by query string.

        Searches in:
        - Definition ID
        - Description
        - Required events
        - Allowed dimensions

        Args:
            query: Search query
            tenant_id: Tenant ID
            limit: Max results

        Returns:
            List of matching definition summaries
        """
        query_lower = query.lower()
        definitions = self.persistence.get_definitions(tenant_id)
        versions = self.persistence.get_definition_versions(tenant_id)

        # Build version map
        version_map: Dict[str, DefinitionVersion] = {}
        for v in versions:
            if v.status == "published":
                version_map[v.definition_id] = v

        matches = []
        for defn in definitions:
            score = 0

            # Score by ID match
            if query_lower in defn.id.lower():
                score += 10
            if defn.id.lower().startswith(query_lower):
                score += 5

            # Score by description match
            if defn.description and query_lower in defn.description.lower():
                score += 3

            # Score by events/dims match
            published = version_map.get(defn.id)
            if published and published.spec:
                for event in published.spec.required_events:
                    if query_lower in event.lower():
                        score += 2
                for dim in published.spec.allowed_dims:
                    if query_lower in dim.lower():
                        score += 1

            if score > 0:
                matches.append((score, defn, published))

        # Sort by score descending
        matches.sort(key=lambda m: m[0], reverse=True)

        # Build summaries
        summaries = []
        for _, defn, published in matches[:limit]:
            summaries.append(DefinitionSummary(
                id=defn.id,
                kind=defn.kind,
                pack=getattr(defn, 'pack', None),
                description=defn.description,
                status="published" if published else "draft",
                version=published.version if published else None,
                event_count=len(published.spec.required_events) if published else 0,
                dim_count=len(published.spec.allowed_dims) if published else 0,
                binding_coverage=0.0,
                updated_at=defn.updated_at,
            ))

        return summaries

    # =========================================================================
    # Get Details
    # =========================================================================

    def get_definition_detail(
        self,
        definition_id: str,
        tenant_id: str = "default"
    ) -> Optional[DefinitionDetail]:
        """
        Get full details for a definition.

        Args:
            definition_id: Definition ID
            tenant_id: Tenant ID

        Returns:
            DefinitionDetail with full context, or None if not found
        """
        definition = self.persistence.get_definition(definition_id, tenant_id)
        if not definition:
            return None

        # Get all versions
        all_versions = self.persistence.get_definition_versions(tenant_id)
        def_versions = [v for v in all_versions if v.definition_id == definition_id]

        # Get published version
        published = next((v for v in def_versions if v.status == "published"), None)

        # Get events used by this definition
        events = []
        bindings = []
        if published and published.spec:
            for event_id in published.spec.required_events:
                event = self.persistence.get_event(event_id, tenant_id)
                if event:
                    events.append(event)
                    bindings.extend(self.persistence.get_bindings_for_event(event_id, tenant_id))

        # Get proof hooks
        proof_hooks = self.persistence.get_proof_hooks_for_definition(definition_id, tenant_id)

        # Get lineage
        lineage = self.lineage.get_definition_lineage(definition_id, tenant_id)

        return DefinitionDetail(
            definition=definition,
            published_version=published,
            all_versions=def_versions,
            events=events,
            bindings=bindings,
            proof_hooks=proof_hooks,
            lineage=lineage,
        )

    # =========================================================================
    # Create/Update
    # =========================================================================

    def create_definition(
        self,
        definition_id: str,
        kind: str = "metric",
        pack: Optional[str] = None,
        description: Optional[str] = None,
        default_time_semantics: Optional[Dict[str, Any]] = None,
        spec: Optional[Dict[str, Any]] = None,
        tenant_id: str = "default",
        validate: bool = True
    ) -> Tuple[Optional[Definition], List[str]]:
        """
        Create a new definition with optional initial version.

        Args:
            definition_id: Definition ID
            kind: Definition kind (metric, view)
            pack: Optional pack (cfo, cto, coo, ceo)
            description: Description
            default_time_semantics: Time semantics config
            spec: Optional initial version spec
            tenant_id: Tenant ID
            validate: Whether to validate before creating

        Returns:
            Tuple of (Definition or None, list of errors)
        """
        errors = []

        # Check if already exists
        existing = self.persistence.get_definition(definition_id, tenant_id)
        if existing:
            errors.append(f"Definition '{definition_id}' already exists")
            return None, errors

        # Validate spec if provided
        if spec and validate:
            valid, spec_errors = self.schema_enforcer.validate_definition_spec_dict(
                definition_id, spec
            )
            if not valid:
                errors.extend(spec_errors)
                return None, errors

            # Validate events and dims
            valid, consistency_errors = self.consistency.validate_new_definition(
                definition_id,
                spec.get("required_events", []),
                spec.get("allowed_dims", []),
                tenant_id
            )
            if not valid:
                errors.extend(consistency_errors)
                return None, errors

        # Create definition
        definition = Definition(
            id=definition_id,
            tenant_id=tenant_id,
            kind=kind,
            description=description,
            default_time_semantics_json=default_time_semantics or {},
        )

        # Add pack attribute if provided
        if pack:
            definition = Definition(
                id=definition_id,
                tenant_id=tenant_id,
                kind=kind,
                description=description,
                default_time_semantics_json=default_time_semantics or {},
            )

        created = self.persistence.register_definition(definition)

        # Create initial version if spec provided
        if spec:
            version = DefinitionVersion(
                id=f"{definition_id}_v1",
                tenant_id=tenant_id,
                definition_id=definition_id,
                version="v1",
                status="draft",
                spec=DefinitionVersionSpec(**spec),
            )
            self.persistence.register_definition_version(version)

        return created, errors

    def create_version(
        self,
        definition_id: str,
        version: str,
        spec: Dict[str, Any],
        tenant_id: str = "default",
        validate: bool = True
    ) -> Tuple[Optional[DefinitionVersion], List[str]]:
        """
        Create a new version for a definition.

        Args:
            definition_id: Definition ID
            version: Version string (v1, v2, etc.)
            spec: Version spec
            tenant_id: Tenant ID
            validate: Whether to validate

        Returns:
            Tuple of (DefinitionVersion or None, list of errors)
        """
        errors = []

        # Check definition exists
        definition = self.persistence.get_definition(definition_id, tenant_id)
        if not definition:
            errors.append(f"Definition '{definition_id}' not found")
            return None, errors

        # Check version doesn't exist
        existing = self.persistence.get_definition_version(definition_id, version, tenant_id)
        if existing:
            errors.append(f"Version '{version}' already exists for definition '{definition_id}'")
            return None, errors

        # Validate spec
        if validate:
            valid, spec_errors = self.schema_enforcer.validate_definition_spec_dict(
                definition_id, spec
            )
            if not valid:
                errors.extend(spec_errors)
                return None, errors

        # Create version
        version_obj = DefinitionVersion(
            id=f"{definition_id}_{version}",
            tenant_id=tenant_id,
            definition_id=definition_id,
            version=version,
            status="draft",
            spec=DefinitionVersionSpec(**spec),
        )

        created = self.persistence.register_definition_version(version_obj)
        return created, errors

    # =========================================================================
    # Publish/Deprecate Workflow
    # =========================================================================

    def publish_definition(
        self,
        definition_id: str,
        version: str,
        tenant_id: str = "default",
        validate: bool = True
    ) -> Tuple[bool, List[str]]:
        """
        Publish a definition version.

        This will:
        1. Validate the version
        2. Deprecate any previously published version
        3. Set the specified version to published

        Args:
            definition_id: Definition ID
            version: Version to publish
            tenant_id: Tenant ID
            validate: Whether to validate before publishing

        Returns:
            Tuple of (success, list of errors)
        """
        errors = []

        # Get version
        def_version = self.persistence.get_definition_version(definition_id, version, tenant_id)
        if not def_version:
            errors.append(f"Version '{version}' not found for definition '{definition_id}'")
            return False, errors

        if def_version.status == "published":
            errors.append(f"Version '{version}' is already published")
            return False, errors

        # Validate
        if validate:
            spec_violations = self.schema_enforcer.validate_definition_spec(def_version)
            spec_errors = [v.message for v in spec_violations if v.severity == "error"]
            if spec_errors:
                errors.extend(spec_errors)
                return False, errors

            # Check binding coverage
            for event_id in def_version.spec.required_events:
                bindings = self.persistence.get_bindings_for_event(event_id, tenant_id)
                if not bindings:
                    errors.append(f"Event '{event_id}' has no bindings - definition may not be answerable")

        # Deprecate existing published version
        versions = self.persistence.get_definition_versions(tenant_id)
        for v in versions:
            if v.definition_id == definition_id and v.status == "published":
                v.status = "deprecated"
                self.persistence.register_definition_version(v)

        # Publish new version
        def_version.status = "published"
        def_version.published_at = datetime.utcnow().isoformat()
        self.persistence.register_definition_version(def_version)

        return True, errors

    def deprecate_definition(
        self,
        definition_id: str,
        version: str,
        tenant_id: str = "default"
    ) -> Tuple[bool, List[str]]:
        """
        Deprecate a definition version.

        Args:
            definition_id: Definition ID
            version: Version to deprecate
            tenant_id: Tenant ID

        Returns:
            Tuple of (success, list of errors/warnings)
        """
        errors = []

        # Get version
        def_version = self.persistence.get_definition_version(definition_id, version, tenant_id)
        if not def_version:
            errors.append(f"Version '{version}' not found for definition '{definition_id}'")
            return False, errors

        if def_version.status == "deprecated":
            errors.append(f"Version '{version}' is already deprecated")
            return False, errors

        # Check impact
        impact = self.lineage.analyze_impact("definition", definition_id, tenant_id)
        if impact.severity in ["high", "critical"]:
            errors.append(f"Warning: Deprecating this definition affects {impact.total_affected} objects")

        # Deprecate
        def_version.status = "deprecated"
        self.persistence.register_definition_version(def_version)

        return True, errors

    # =========================================================================
    # Catalog Statistics
    # =========================================================================

    def get_catalog_stats(self, tenant_id: str = "default") -> CatalogStats:
        """
        Get statistics about the definition catalog.

        Args:
            tenant_id: Tenant ID

        Returns:
            CatalogStats with catalog metrics
        """
        definitions = self.persistence.get_definitions(tenant_id)
        versions = self.persistence.get_definition_versions(tenant_id)
        events = self.persistence.get_events(tenant_id)
        bindings = self.persistence.get_bindings(tenant_id)
        entities = self.persistence.get_entities(tenant_id)

        # Build version map
        version_map: Dict[str, List[DefinitionVersion]] = {}
        for v in versions:
            if v.definition_id not in version_map:
                version_map[v.definition_id] = []
            version_map[v.definition_id].append(v)

        # Count by status
        published = 0
        draft = 0
        deprecated = 0

        for defn in definitions:
            def_versions = version_map.get(defn.id, [])
            if any(v.status == "published" for v in def_versions):
                published += 1
            elif any(v.status == "draft" for v in def_versions):
                draft += 1
            else:
                deprecated += 1

        # Count by pack
        by_pack: Dict[str, int] = {}
        for defn in definitions:
            pack = getattr(defn, 'pack', None) or "unassigned"
            by_pack[pack] = by_pack.get(pack, 0) + 1

        # Count by kind
        by_kind: Dict[str, int] = {}
        for defn in definitions:
            by_kind[defn.kind] = by_kind.get(defn.kind, 0) + 1

        # Calculate average binding coverage
        total_coverage = 0.0
        coverage_count = 0
        for defn in definitions:
            published_version = next(
                (v for v in version_map.get(defn.id, []) if v.status == "published"),
                None
            )
            if published_version and published_version.spec.required_events:
                covered = sum(
                    1 for e in published_version.spec.required_events
                    if any(b.canonical_event_id == e for b in bindings)
                )
                total_coverage += covered / len(published_version.spec.required_events)
                coverage_count += 1

        avg_coverage = total_coverage / coverage_count if coverage_count > 0 else 0.0

        return CatalogStats(
            total_definitions=len(definitions),
            published_definitions=published,
            draft_definitions=draft,
            deprecated_definitions=deprecated,
            definitions_by_pack=by_pack,
            definitions_by_kind=by_kind,
            total_events=len(events),
            total_bindings=len(bindings),
            total_entities=len(entities),
            avg_binding_coverage=avg_coverage,
        )

    def get_packs(self, tenant_id: str = "default") -> List[Dict[str, Any]]:
        """
        Get all packs with their definition counts.

        Args:
            tenant_id: Tenant ID

        Returns:
            List of pack info dicts
        """
        definitions = self.persistence.get_definitions(tenant_id)

        pack_counts: Dict[str, int] = {}
        for defn in definitions:
            pack = getattr(defn, 'pack', None) or "unassigned"
            pack_counts[pack] = pack_counts.get(pack, 0) + 1

        packs = [
            {"pack": pack, "definition_count": count}
            for pack, count in sorted(pack_counts.items())
        ]

        return packs
