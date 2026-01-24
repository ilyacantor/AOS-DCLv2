"""
Persistence layer for NLQ semantic metadata.

Provides access to:
- canonical_events: Event types in the semantic model
- entities: Business entities (dimensions)
- bindings: Source system to semantic model mappings
- definitions: Metric/KPI definitions
- proof_hooks: Source system proof pointers

Uses JSON fixtures by default, with optional PostgreSQL backend.
"""

import os
import json
from pathlib import Path
from typing import Dict, List, Optional, Any
from backend.utils.log_utils import get_logger
from backend.nlq.models import (
    CanonicalEvent,
    Entity,
    Binding,
    Definition,
    DefinitionVersion,
    DefinitionVersionSpec,
    ProofHook,
)

logger = get_logger(__name__)


# Default fixture path
FIXTURES_DIR = Path(__file__).parent / "fixtures"


class NLQPersistence:
    """
    Persistence layer for NLQ semantic metadata.

    Loads from JSON fixtures by default. Can be configured to use PostgreSQL.
    """

    def __init__(self, fixtures_dir: Optional[Path] = None, use_db: bool = False):
        """
        Initialize the persistence layer.

        Args:
            fixtures_dir: Path to JSON fixtures directory. Defaults to ./fixtures/
            use_db: If True, use PostgreSQL. If False, use JSON fixtures.
        """
        self.fixtures_dir = fixtures_dir or FIXTURES_DIR
        self.use_db = use_db
        self._cache: Dict[str, Any] = {}

        # Ensure fixtures directory exists
        self.fixtures_dir.mkdir(parents=True, exist_ok=True)

    def _load_fixture(self, name: str) -> List[Dict[str, Any]]:
        """Load a JSON fixture file."""
        if name in self._cache:
            return self._cache[name]

        fixture_path = self.fixtures_dir / f"{name}.json"
        if not fixture_path.exists():
            logger.warning(f"Fixture not found: {fixture_path}")
            return []

        try:
            with open(fixture_path, "r") as f:
                data = json.load(f)
                self._cache[name] = data
                return data
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in {fixture_path}: {e}")
            return []

    def clear_cache(self) -> None:
        """Clear the in-memory cache."""
        self._cache.clear()

    # =========================================================================
    # Canonical Events
    # =========================================================================

    def get_events(self, tenant_id: str = "default") -> List[CanonicalEvent]:
        """Get all canonical events for a tenant."""
        data = self._load_fixture("canonical_events")
        return [CanonicalEvent(**item) for item in data]

    def get_event(self, event_id: str, tenant_id: str = "default") -> Optional[CanonicalEvent]:
        """Get a specific canonical event by ID."""
        events = self.get_events(tenant_id)
        for event in events:
            if event.id == event_id:
                return event
        return None

    def event_exists(self, event_id: str, tenant_id: str = "default") -> bool:
        """Check if an event exists."""
        return self.get_event(event_id, tenant_id) is not None

    # =========================================================================
    # Entities (Dimensions)
    # =========================================================================

    def get_entities(self, tenant_id: str = "default") -> List[Entity]:
        """Get all entities for a tenant."""
        data = self._load_fixture("entities")
        return [Entity(**item) for item in data]

    def get_entity(self, entity_id: str, tenant_id: str = "default") -> Optional[Entity]:
        """Get a specific entity by ID."""
        entities = self.get_entities(tenant_id)
        for entity in entities:
            if entity.id == entity_id:
                return entity
        return None

    def entity_exists(self, entity_id: str, tenant_id: str = "default") -> bool:
        """Check if an entity exists."""
        return self.get_entity(entity_id, tenant_id) is not None

    # =========================================================================
    # Bindings
    # =========================================================================

    def get_bindings(self, tenant_id: str = "default") -> List[Binding]:
        """Get all bindings for a tenant."""
        data = self._load_fixture("bindings")
        return [Binding(**item) for item in data]

    def get_bindings_for_event(self, event_id: str, tenant_id: str = "default") -> List[Binding]:
        """Get bindings that map to a specific canonical event."""
        bindings = self.get_bindings(tenant_id)
        return [b for b in bindings if b.canonical_event_id == event_id]

    def get_binding_quality(self, event_id: str, tenant_id: str = "default") -> float:
        """Get the average binding quality for an event."""
        bindings = self.get_bindings_for_event(event_id, tenant_id)
        if not bindings:
            return 0.0
        return sum(b.quality_score for b in bindings) / len(bindings)

    def get_binding_freshness(self, event_id: str, tenant_id: str = "default") -> float:
        """Get the average binding freshness for an event."""
        bindings = self.get_bindings_for_event(event_id, tenant_id)
        if not bindings:
            return 0.0
        return sum(b.freshness_score for b in bindings) / len(bindings)

    def get_available_dims(self, event_id: str, tenant_id: str = "default") -> List[str]:
        """Get all available dimensions for an event across bindings."""
        bindings = self.get_bindings_for_event(event_id, tenant_id)
        dims = set()
        for b in bindings:
            # Use dims_coverage_json which maps dim -> bool
            for dim, covered in b.dims_coverage_json.items():
                if covered:
                    dims.add(dim)
        return list(dims)

    def get_dims_coverage(self, event_id: str, tenant_id: str = "default") -> Dict[str, bool]:
        """Get dimension coverage across all bindings for an event."""
        bindings = self.get_bindings_for_event(event_id, tenant_id)
        coverage: Dict[str, bool] = {}
        for b in bindings:
            for dim, covered in b.dims_coverage_json.items():
                # If any binding covers this dim, mark as covered
                if covered:
                    coverage[dim] = True
                elif dim not in coverage:
                    coverage[dim] = False
        return coverage

    # =========================================================================
    # Definitions
    # =========================================================================

    def get_definitions(self, tenant_id: str = "default") -> List[Definition]:
        """Get all definitions for a tenant."""
        data = self._load_fixture("definitions")
        return [Definition(**item) for item in data]

    def get_definition(self, definition_id: str, tenant_id: str = "default") -> Optional[Definition]:
        """Get a specific definition by ID."""
        definitions = self.get_definitions(tenant_id)
        for defn in definitions:
            if defn.id == definition_id:
                return defn
        return None

    def definition_exists(self, definition_id: str, tenant_id: str = "default") -> bool:
        """Check if a definition exists."""
        return self.get_definition(definition_id, tenant_id) is not None

    # =========================================================================
    # Definition Versions
    # =========================================================================

    def get_definition_versions(self, tenant_id: str = "default") -> List[DefinitionVersion]:
        """Get all definition versions for a tenant."""
        data = self._load_fixture("definition_versions")
        versions = []
        for item in data:
            # Parse the spec if it exists
            spec_data = item.get("spec", {})
            spec = DefinitionVersionSpec(**spec_data)
            versions.append(DefinitionVersion(
                id=item["id"],
                definition_id=item["definition_id"],
                version=item.get("version", "v1"),
                status=item.get("status", "published"),
                spec=spec,
                published_at=item.get("published_at"),
            ))
        return versions

    def get_definition_version(
        self,
        definition_id: str,
        version: str = "v1",
        tenant_id: str = "default"
    ) -> Optional[DefinitionVersion]:
        """Get a specific definition version."""
        versions = self.get_definition_versions(tenant_id)
        for v in versions:
            if v.definition_id == definition_id and v.version == version:
                return v
        return None

    def get_published_version(
        self,
        definition_id: str,
        tenant_id: str = "default"
    ) -> Optional[DefinitionVersion]:
        """Get the published version of a definition."""
        versions = self.get_definition_versions(tenant_id)
        for v in versions:
            if v.definition_id == definition_id and v.status == "published":
                return v
        return None

    # =========================================================================
    # Proof Hooks
    # =========================================================================

    def get_proof_hooks(self, tenant_id: str = "default") -> List[ProofHook]:
        """Get all proof hooks for a tenant."""
        data = self._load_fixture("proof_hooks")
        return [ProofHook(**item) for item in data]

    def get_proof_hooks_for_definition(
        self, definition_id: str, tenant_id: str = "default"
    ) -> List[ProofHook]:
        """Get proof hooks for a specific definition."""
        hooks = self.get_proof_hooks(tenant_id)
        return [h for h in hooks if h.definition_id == definition_id]

    def get_proof_availability(self, definition_id: str, tenant_id: str = "default") -> float:
        """Get the average proof availability for a definition."""
        hooks = self.get_proof_hooks_for_definition(definition_id, tenant_id)
        if not hooks:
            return 0.0
        return sum(h.availability_score for h in hooks) / len(hooks)

    # =========================================================================
    # Semantic Query Helpers
    # =========================================================================

    def resolve_definition(
        self, metric_hint: Optional[str] = None, keywords: Optional[List[str]] = None, tenant_id: str = "default"
    ) -> Optional[Definition]:
        """
        Resolve a definition from hints or keywords.

        Args:
            metric_hint: Direct hint like "services_revenue"
            keywords: Keywords from the question like ["services", "revenue"]
            tenant_id: Tenant ID

        Returns:
            Matched definition or None
        """
        definitions = self.get_definitions(tenant_id)

        # Direct hint match
        if metric_hint:
            for defn in definitions:
                if defn.id == metric_hint or defn.id == metric_hint.lower().replace(" ", "_"):
                    return defn

        # Keyword matching
        if keywords:
            best_match = None
            best_score = 0
            for defn in definitions:
                score = 0
                defn_words = set(defn.id.lower().replace("_", " ").split())
                for kw in keywords:
                    if kw.lower() in defn_words or kw.lower() in defn.id.lower():
                        score += 1
                if score > best_score:
                    best_score = score
                    best_match = defn
            if best_match and best_score > 0:
                return best_match

        return None

    def check_event_binding(
        self, event_ids: List[str], tenant_id: str = "default"
    ) -> Dict[str, bool]:
        """
        Check which events have bindings.

        Returns:
            Dict mapping event_id -> is_bound
        """
        result = {}
        for event_id in event_ids:
            bindings = self.get_bindings_for_event(event_id, tenant_id)
            result[event_id] = len(bindings) > 0
        return result

    def check_dims_available(
        self, dim_ids: List[str], event_ids: List[str], tenant_id: str = "default"
    ) -> Dict[str, bool]:
        """
        Check which dimensions are available for the given events.

        Returns:
            Dict mapping dim_id -> is_available
        """
        available_dims = set()
        for event_id in event_ids:
            available_dims.update(self.get_available_dims(event_id, tenant_id))

        result = {}
        for dim_id in dim_ids:
            result[dim_id] = dim_id in available_dims
        return result

    def get_dims_missing_for_events(
        self, requested_dims: List[str], event_ids: List[str], tenant_id: str = "default"
    ) -> List[str]:
        """
        Get list of dimensions that are missing coverage for the given events.

        Returns:
            List of dimension IDs that are not covered by any binding
        """
        dims_check = self.check_dims_available(requested_dims, event_ids, tenant_id)
        return [dim for dim, available in dims_check.items() if not available]
