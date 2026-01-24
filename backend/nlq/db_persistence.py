"""
Database persistence layer for NLQ Semantic Layer.

Provides PostgreSQL persistence with:
- Row-level tenant isolation
- Session management
- CRUD operations for all semantic layer objects
"""

import os
from contextlib import contextmanager
from typing import Dict, List, Optional, Any, Generator
from datetime import datetime

from sqlalchemy import create_engine, and_, or_
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import IntegrityError

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
from backend.nlq.db_models import (
    Base,
    CanonicalEventDB,
    EntityDB,
    BindingDB,
    DefinitionDB,
    DefinitionVersionDB,
    ProofHookDB,
    LineageEdgeDB,
    QueryExecutionDB,
    ConsistencyCheckDB,
)

logger = get_logger(__name__)


def get_database_url() -> str:
    """Get database URL from environment."""
    return os.environ.get(
        "NLQ_DATABASE_URL",
        "postgresql://localhost/dcl_nlq"
    )


class DatabasePersistence:
    """
    Database persistence layer for NLQ semantic metadata.

    Provides row-level tenant isolation and full CRUD operations.
    """

    def __init__(self, database_url: Optional[str] = None, echo: bool = False):
        """
        Initialize database persistence.

        Args:
            database_url: PostgreSQL connection URL
            echo: If True, log all SQL statements
        """
        self.database_url = database_url or get_database_url()
        self.engine = create_engine(self.database_url, echo=echo, pool_pre_ping=True)
        self.SessionLocal = sessionmaker(bind=self.engine, autocommit=False, autoflush=False)

    def create_tables(self) -> None:
        """Create all tables (for testing/development)."""
        Base.metadata.create_all(self.engine)

    def drop_tables(self) -> None:
        """Drop all tables (for testing only)."""
        Base.metadata.drop_all(self.engine)

    @contextmanager
    def session_scope(self) -> Generator[Session, None, None]:
        """Provide a transactional scope around a series of operations."""
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    # =========================================================================
    # Canonical Events
    # =========================================================================

    def get_events(self, tenant_id: str = "default") -> List[CanonicalEvent]:
        """Get all canonical events for a tenant."""
        with self.session_scope() as session:
            rows = session.query(CanonicalEventDB).filter(
                CanonicalEventDB.tenant_id == tenant_id
            ).all()
            return [self._event_from_db(row) for row in rows]

    def get_event(self, event_id: str, tenant_id: str = "default") -> Optional[CanonicalEvent]:
        """Get a specific canonical event by ID."""
        with self.session_scope() as session:
            row = session.query(CanonicalEventDB).filter(
                and_(
                    CanonicalEventDB.id == event_id,
                    CanonicalEventDB.tenant_id == tenant_id
                )
            ).first()
            return self._event_from_db(row) if row else None

    def event_exists(self, event_id: str, tenant_id: str = "default") -> bool:
        """Check if an event exists."""
        return self.get_event(event_id, tenant_id) is not None

    def register_event(self, event: CanonicalEvent) -> CanonicalEvent:
        """Register or update a canonical event."""
        with self.session_scope() as session:
            row = session.query(CanonicalEventDB).filter(
                and_(
                    CanonicalEventDB.id == event.id,
                    CanonicalEventDB.tenant_id == event.tenant_id
                )
            ).first()

            if row:
                row.description = event.description
                row.schema_json = event.schema_json
                row.time_semantics_json = event.time_semantics_json
                row.updated_at = datetime.utcnow()
            else:
                row = CanonicalEventDB(
                    id=event.id,
                    tenant_id=event.tenant_id,
                    description=event.description,
                    schema_json=event.schema_json,
                    time_semantics_json=event.time_semantics_json,
                )
                session.add(row)

            session.flush()
            return self._event_from_db(row)

    def delete_event(self, event_id: str, tenant_id: str = "default") -> bool:
        """Delete a canonical event."""
        with self.session_scope() as session:
            deleted = session.query(CanonicalEventDB).filter(
                and_(
                    CanonicalEventDB.id == event_id,
                    CanonicalEventDB.tenant_id == tenant_id
                )
            ).delete()
            return deleted > 0

    def _event_from_db(self, row: CanonicalEventDB) -> CanonicalEvent:
        """Convert database row to Pydantic model."""
        return CanonicalEvent(
            id=row.id,
            tenant_id=row.tenant_id,
            description=row.description,
            schema_json=row.schema_json or {},
            time_semantics_json=row.time_semantics_json or {},
            created_at=row.created_at.isoformat() if row.created_at else None,
            updated_at=row.updated_at.isoformat() if row.updated_at else None,
        )

    # =========================================================================
    # Entities
    # =========================================================================

    def get_entities(self, tenant_id: str = "default") -> List[Entity]:
        """Get all entities for a tenant."""
        with self.session_scope() as session:
            rows = session.query(EntityDB).filter(
                EntityDB.tenant_id == tenant_id
            ).all()
            return [self._entity_from_db(row) for row in rows]

    def get_entity(self, entity_id: str, tenant_id: str = "default") -> Optional[Entity]:
        """Get a specific entity by ID."""
        with self.session_scope() as session:
            row = session.query(EntityDB).filter(
                and_(
                    EntityDB.id == entity_id,
                    EntityDB.tenant_id == tenant_id
                )
            ).first()
            return self._entity_from_db(row) if row else None

    def entity_exists(self, entity_id: str, tenant_id: str = "default") -> bool:
        """Check if an entity exists."""
        return self.get_entity(entity_id, tenant_id) is not None

    def register_entity(self, entity: Entity) -> Entity:
        """Register or update an entity."""
        with self.session_scope() as session:
            row = session.query(EntityDB).filter(
                and_(
                    EntityDB.id == entity.id,
                    EntityDB.tenant_id == entity.tenant_id
                )
            ).first()

            if row:
                row.description = entity.description
                row.identifiers_json = entity.identifiers_json
                row.updated_at = datetime.utcnow()
            else:
                row = EntityDB(
                    id=entity.id,
                    tenant_id=entity.tenant_id,
                    description=entity.description,
                    identifiers_json=entity.identifiers_json,
                )
                session.add(row)

            session.flush()
            return self._entity_from_db(row)

    def delete_entity(self, entity_id: str, tenant_id: str = "default") -> bool:
        """Delete an entity."""
        with self.session_scope() as session:
            deleted = session.query(EntityDB).filter(
                and_(
                    EntityDB.id == entity_id,
                    EntityDB.tenant_id == tenant_id
                )
            ).delete()
            return deleted > 0

    def _entity_from_db(self, row: EntityDB) -> Entity:
        """Convert database row to Pydantic model."""
        return Entity(
            id=row.id,
            tenant_id=row.tenant_id,
            description=row.description,
            identifiers_json=row.identifiers_json or {},
        )

    # =========================================================================
    # Bindings
    # =========================================================================

    def get_bindings(self, tenant_id: str = "default") -> List[Binding]:
        """Get all bindings for a tenant."""
        with self.session_scope() as session:
            rows = session.query(BindingDB).filter(
                BindingDB.tenant_id == tenant_id
            ).all()
            return [self._binding_from_db(row) for row in rows]

    def get_bindings_for_event(self, event_id: str, tenant_id: str = "default") -> List[Binding]:
        """Get bindings that map to a specific canonical event."""
        with self.session_scope() as session:
            rows = session.query(BindingDB).filter(
                and_(
                    BindingDB.canonical_event_id == event_id,
                    BindingDB.tenant_id == tenant_id
                )
            ).all()
            return [self._binding_from_db(row) for row in rows]

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
                if covered:
                    coverage[dim] = True
                elif dim not in coverage:
                    coverage[dim] = False
        return coverage

    def register_binding(self, binding: Binding) -> Binding:
        """Register or update a binding."""
        with self.session_scope() as session:
            row = session.query(BindingDB).filter(
                and_(
                    BindingDB.id == binding.id,
                    BindingDB.tenant_id == binding.tenant_id
                )
            ).first()

            if row:
                row.source_system = binding.source_system
                row.canonical_event_id = binding.canonical_event_id
                row.mapping_json = binding.mapping_json
                row.dims_coverage_json = binding.dims_coverage_json
                row.quality_score = binding.quality_score
                row.freshness_score = binding.freshness_score
                row.updated_at = datetime.utcnow()
            else:
                row = BindingDB(
                    id=binding.id,
                    tenant_id=binding.tenant_id,
                    source_system=binding.source_system,
                    canonical_event_id=binding.canonical_event_id,
                    mapping_json=binding.mapping_json,
                    dims_coverage_json=binding.dims_coverage_json,
                    quality_score=binding.quality_score,
                    freshness_score=binding.freshness_score,
                )
                session.add(row)

            session.flush()
            return self._binding_from_db(row)

    def delete_binding(self, binding_id: str, tenant_id: str = "default") -> bool:
        """Delete a binding."""
        with self.session_scope() as session:
            deleted = session.query(BindingDB).filter(
                and_(
                    BindingDB.id == binding_id,
                    BindingDB.tenant_id == tenant_id
                )
            ).delete()
            return deleted > 0

    def _binding_from_db(self, row: BindingDB) -> Binding:
        """Convert database row to Pydantic model."""
        return Binding(
            id=row.id,
            tenant_id=row.tenant_id,
            source_system=row.source_system,
            canonical_event_id=row.canonical_event_id,
            mapping_json=row.mapping_json or {},
            dims_coverage_json=row.dims_coverage_json or {},
            quality_score=row.quality_score,
            freshness_score=row.freshness_score,
            updated_at=row.updated_at.isoformat() if row.updated_at else None,
        )

    # =========================================================================
    # Definitions
    # =========================================================================

    def get_definitions(self, tenant_id: str = "default", pack: Optional[str] = None) -> List[Definition]:
        """Get all definitions for a tenant, optionally filtered by pack."""
        with self.session_scope() as session:
            query = session.query(DefinitionDB).filter(
                DefinitionDB.tenant_id == tenant_id
            )
            if pack:
                query = query.filter(DefinitionDB.pack == pack)
            rows = query.all()
            return [self._definition_from_db(row) for row in rows]

    def get_definition(self, definition_id: str, tenant_id: str = "default") -> Optional[Definition]:
        """Get a specific definition by ID."""
        with self.session_scope() as session:
            row = session.query(DefinitionDB).filter(
                and_(
                    DefinitionDB.id == definition_id,
                    DefinitionDB.tenant_id == tenant_id
                )
            ).first()
            return self._definition_from_db(row) if row else None

    def definition_exists(self, definition_id: str, tenant_id: str = "default") -> bool:
        """Check if a definition exists."""
        return self.get_definition(definition_id, tenant_id) is not None

    def register_definition(self, definition: Definition) -> Definition:
        """Register or update a definition."""
        with self.session_scope() as session:
            row = session.query(DefinitionDB).filter(
                and_(
                    DefinitionDB.id == definition.id,
                    DefinitionDB.tenant_id == definition.tenant_id
                )
            ).first()

            # Get pack from definition if available
            pack = getattr(definition, 'pack', None)

            if row:
                row.kind = definition.kind
                row.pack = pack
                row.description = definition.description
                row.default_time_semantics_json = definition.default_time_semantics_json
                row.updated_at = datetime.utcnow()
            else:
                row = DefinitionDB(
                    id=definition.id,
                    tenant_id=definition.tenant_id,
                    kind=definition.kind,
                    pack=pack,
                    description=definition.description,
                    default_time_semantics_json=definition.default_time_semantics_json,
                )
                session.add(row)

            session.flush()
            return self._definition_from_db(row)

    def delete_definition(self, definition_id: str, tenant_id: str = "default") -> bool:
        """Delete a definition and its versions."""
        with self.session_scope() as session:
            deleted = session.query(DefinitionDB).filter(
                and_(
                    DefinitionDB.id == definition_id,
                    DefinitionDB.tenant_id == tenant_id
                )
            ).delete()
            return deleted > 0

    def _definition_from_db(self, row: DefinitionDB) -> Definition:
        """Convert database row to Pydantic model."""
        return Definition(
            id=row.id,
            tenant_id=row.tenant_id,
            kind=row.kind,
            description=row.description,
            default_time_semantics_json=row.default_time_semantics_json or {},
            created_at=row.created_at.isoformat() if row.created_at else None,
            updated_at=row.updated_at.isoformat() if row.updated_at else None,
        )

    # =========================================================================
    # Definition Versions
    # =========================================================================

    def get_definition_versions(self, tenant_id: str = "default") -> List[DefinitionVersion]:
        """Get all definition versions for a tenant."""
        with self.session_scope() as session:
            rows = session.query(DefinitionVersionDB).filter(
                DefinitionVersionDB.tenant_id == tenant_id
            ).all()
            return [self._version_from_db(row) for row in rows]

    def get_definition_version(
        self,
        definition_id: str,
        version: str = "v1",
        tenant_id: str = "default"
    ) -> Optional[DefinitionVersion]:
        """Get a specific definition version."""
        with self.session_scope() as session:
            row = session.query(DefinitionVersionDB).filter(
                and_(
                    DefinitionVersionDB.definition_id == definition_id,
                    DefinitionVersionDB.version == version,
                    DefinitionVersionDB.tenant_id == tenant_id
                )
            ).first()
            return self._version_from_db(row) if row else None

    def get_published_version(
        self,
        definition_id: str,
        tenant_id: str = "default"
    ) -> Optional[DefinitionVersion]:
        """Get the published version of a definition."""
        with self.session_scope() as session:
            row = session.query(DefinitionVersionDB).filter(
                and_(
                    DefinitionVersionDB.definition_id == definition_id,
                    DefinitionVersionDB.status == "published",
                    DefinitionVersionDB.tenant_id == tenant_id
                )
            ).first()
            return self._version_from_db(row) if row else None

    def register_definition_version(self, version: DefinitionVersion) -> DefinitionVersion:
        """Register or update a definition version."""
        with self.session_scope() as session:
            row = session.query(DefinitionVersionDB).filter(
                and_(
                    DefinitionVersionDB.id == version.id,
                    DefinitionVersionDB.tenant_id == version.tenant_id
                )
            ).first()

            spec_dict = version.spec.model_dump() if version.spec else {}

            if row:
                row.definition_id = version.definition_id
                row.version = version.version
                row.status = version.status
                row.spec_json = spec_dict
                row.published_at = datetime.fromisoformat(version.published_at) if version.published_at else None
                row.updated_at = datetime.utcnow()
            else:
                row = DefinitionVersionDB(
                    id=version.id,
                    tenant_id=version.tenant_id,
                    definition_id=version.definition_id,
                    version=version.version,
                    status=version.status,
                    spec_json=spec_dict,
                    published_at=datetime.fromisoformat(version.published_at) if version.published_at else None,
                )
                session.add(row)

            session.flush()
            return self._version_from_db(row)

    def publish_version(
        self,
        definition_id: str,
        version: str,
        tenant_id: str = "default"
    ) -> Optional[DefinitionVersion]:
        """Publish a definition version (deprecates previous published versions)."""
        with self.session_scope() as session:
            # Deprecate existing published versions
            session.query(DefinitionVersionDB).filter(
                and_(
                    DefinitionVersionDB.definition_id == definition_id,
                    DefinitionVersionDB.status == "published",
                    DefinitionVersionDB.tenant_id == tenant_id
                )
            ).update({"status": "deprecated", "updated_at": datetime.utcnow()})

            # Publish the new version
            row = session.query(DefinitionVersionDB).filter(
                and_(
                    DefinitionVersionDB.definition_id == definition_id,
                    DefinitionVersionDB.version == version,
                    DefinitionVersionDB.tenant_id == tenant_id
                )
            ).first()

            if row:
                row.status = "published"
                row.published_at = datetime.utcnow()
                row.updated_at = datetime.utcnow()
                session.flush()
                return self._version_from_db(row)

            return None

    def _version_from_db(self, row: DefinitionVersionDB) -> DefinitionVersion:
        """Convert database row to Pydantic model."""
        spec_data = row.spec_json or {}
        spec = DefinitionVersionSpec(**spec_data)
        return DefinitionVersion(
            id=row.id,
            tenant_id=row.tenant_id,
            definition_id=row.definition_id,
            version=row.version,
            status=row.status,
            spec=spec,
            published_at=row.published_at.isoformat() if row.published_at else None,
        )

    # =========================================================================
    # Proof Hooks
    # =========================================================================

    def get_proof_hooks(self, tenant_id: str = "default") -> List[ProofHook]:
        """Get all proof hooks for a tenant."""
        with self.session_scope() as session:
            rows = session.query(ProofHookDB).filter(
                ProofHookDB.tenant_id == tenant_id
            ).all()
            return [self._proof_hook_from_db(row) for row in rows]

    def get_proof_hooks_for_definition(
        self, definition_id: str, tenant_id: str = "default"
    ) -> List[ProofHook]:
        """Get proof hooks for a specific definition."""
        with self.session_scope() as session:
            rows = session.query(ProofHookDB).filter(
                and_(
                    ProofHookDB.definition_id == definition_id,
                    ProofHookDB.tenant_id == tenant_id
                )
            ).all()
            return [self._proof_hook_from_db(row) for row in rows]

    def get_proof_availability(self, definition_id: str, tenant_id: str = "default") -> float:
        """Get the average proof availability for a definition."""
        hooks = self.get_proof_hooks_for_definition(definition_id, tenant_id)
        if not hooks:
            return 0.0
        return sum(h.availability_score for h in hooks) / len(hooks)

    def register_proof_hook(self, hook: ProofHook) -> ProofHook:
        """Register or update a proof hook."""
        with self.session_scope() as session:
            row = session.query(ProofHookDB).filter(
                and_(
                    ProofHookDB.id == hook.id,
                    ProofHookDB.tenant_id == hook.tenant_id
                )
            ).first()

            if row:
                row.definition_id = hook.definition_id
                row.pointer_template_json = hook.pointer_template_json
                row.availability_score = hook.availability_score
                row.updated_at = datetime.utcnow()
            else:
                row = ProofHookDB(
                    id=hook.id,
                    tenant_id=hook.tenant_id,
                    definition_id=hook.definition_id,
                    pointer_template_json=hook.pointer_template_json,
                    availability_score=hook.availability_score,
                )
                session.add(row)

            session.flush()
            return self._proof_hook_from_db(row)

    def delete_proof_hook(self, hook_id: str, tenant_id: str = "default") -> bool:
        """Delete a proof hook."""
        with self.session_scope() as session:
            deleted = session.query(ProofHookDB).filter(
                and_(
                    ProofHookDB.id == hook_id,
                    ProofHookDB.tenant_id == tenant_id
                )
            ).delete()
            return deleted > 0

    def _proof_hook_from_db(self, row: ProofHookDB) -> ProofHook:
        """Convert database row to Pydantic model."""
        return ProofHook(
            id=row.id,
            tenant_id=row.tenant_id,
            definition_id=row.definition_id,
            pointer_template_json=row.pointer_template_json or {},
            availability_score=row.availability_score,
        )

    # =========================================================================
    # Semantic Query Helpers
    # =========================================================================

    def resolve_definition(
        self, metric_hint: Optional[str] = None, keywords: Optional[List[str]] = None, tenant_id: str = "default"
    ) -> Optional[Definition]:
        """Resolve a definition from hints or keywords."""
        definitions = self.get_definitions(tenant_id)

        if metric_hint:
            for defn in definitions:
                if defn.id == metric_hint or defn.id == metric_hint.lower().replace(" ", "_"):
                    return defn

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
        """Check which events have bindings."""
        result = {}
        for event_id in event_ids:
            bindings = self.get_bindings_for_event(event_id, tenant_id)
            result[event_id] = len(bindings) > 0
        return result

    def check_dims_available(
        self, dim_ids: List[str], event_ids: List[str], tenant_id: str = "default"
    ) -> Dict[str, bool]:
        """Check which dimensions are available for the given events."""
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
        """Get list of dimensions that are missing coverage for the given events."""
        dims_check = self.check_dims_available(requested_dims, event_ids, tenant_id)
        return [dim for dim, available in dims_check.items() if not available]
