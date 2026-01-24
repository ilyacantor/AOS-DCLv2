"""
Unit tests for NLQ Registration API.

Tests binding, event, entity, definition, and proof hook registration.
"""

import pytest
from backend.nlq.persistence import NLQPersistence
from backend.nlq.models import (
    Binding,
    CanonicalEvent,
    Entity,
    Definition,
    DefinitionVersion,
    DefinitionVersionSpec,
    ProofHook,
)


class TestBindingRegistration:
    """Tests for binding registration."""

    def setup_method(self):
        """Set up persistence with default fixtures."""
        self.persistence = NLQPersistence()

    def test_register_new_binding(self):
        """Should register a new binding."""
        binding = Binding(
            id="test_binding_new",
            tenant_id="test_tenant",
            source_system="TestSystem",
            canonical_event_id="revenue_recognized",
            mapping_json={"field1": "canonical_field1"},
            dims_coverage_json={"customer": True},
            quality_score=0.85,
            freshness_score=0.90,
        )

        result = self.persistence.register_binding(binding)
        assert result.id == "test_binding_new"

        # Verify it was saved
        bindings = self.persistence.get_bindings("test_tenant")
        ids = [b.id for b in bindings]
        assert "test_binding_new" in ids

    def test_update_existing_binding(self):
        """Should update an existing binding."""
        # First create
        binding = Binding(
            id="test_binding_update",
            tenant_id="test_tenant",
            source_system="TestSystem",
            canonical_event_id="revenue_recognized",
            quality_score=0.80,
            freshness_score=0.80,
        )
        self.persistence.register_binding(binding)

        # Then update
        binding.quality_score = 0.95
        self.persistence.register_binding(binding)

        # Verify update
        bindings = self.persistence.get_bindings("test_tenant")
        updated = next(b for b in bindings if b.id == "test_binding_update")
        assert updated.quality_score == 0.95

    def test_delete_binding(self):
        """Should delete a binding."""
        binding = Binding(
            id="test_binding_delete",
            tenant_id="test_tenant",
            source_system="TestSystem",
            canonical_event_id="revenue_recognized",
        )
        self.persistence.register_binding(binding)

        # Delete
        result = self.persistence.delete_binding("test_binding_delete", "test_tenant")
        assert result is True

        # Verify deletion
        bindings = self.persistence.get_bindings("test_tenant")
        ids = [b.id for b in bindings]
        assert "test_binding_delete" not in ids

    def test_delete_nonexistent_binding(self):
        """Should return False for nonexistent binding."""
        result = self.persistence.delete_binding("nonexistent", "test_tenant")
        assert result is False


class TestEventRegistration:
    """Tests for canonical event registration."""

    def setup_method(self):
        """Set up persistence with default fixtures."""
        self.persistence = NLQPersistence()

    def test_register_new_event(self):
        """Should register a new canonical event."""
        event = CanonicalEvent(
            id="test_event_new",
            tenant_id="test_tenant",
            schema_json={"fields": [{"name": "amount", "type": "decimal"}]},
            time_semantics_json={"event_time": "created_at"},
            description="Test event",
        )

        result = self.persistence.register_event(event)
        assert result.id == "test_event_new"

        # Verify it was saved
        events = self.persistence.get_events("test_tenant")
        ids = [e.id for e in events]
        assert "test_event_new" in ids


class TestEntityRegistration:
    """Tests for entity registration."""

    def setup_method(self):
        """Set up persistence with default fixtures."""
        self.persistence = NLQPersistence()

    def test_register_new_entity(self):
        """Should register a new entity."""
        entity = Entity(
            id="test_entity_new",
            tenant_id="test_tenant",
            identifiers_json={"primary": "entity_id"},
            description="Test entity",
        )

        result = self.persistence.register_entity(entity)
        assert result.id == "test_entity_new"

        # Verify it was saved
        entities = self.persistence.get_entities("test_tenant")
        ids = [e.id for e in entities]
        assert "test_entity_new" in ids


class TestDefinitionRegistration:
    """Tests for definition registration."""

    def setup_method(self):
        """Set up persistence with default fixtures."""
        self.persistence = NLQPersistence()

    def test_register_new_definition(self):
        """Should register a new definition."""
        definition = Definition(
            id="test_definition_new",
            tenant_id="test_tenant",
            kind="metric",
            description="Test metric",
            default_time_semantics_json={"event": "revenue_recognized"},
        )

        result = self.persistence.register_definition(definition)
        assert result.id == "test_definition_new"

        # Verify it was saved
        definitions = self.persistence.get_definitions("test_tenant")
        ids = [d.id for d in definitions]
        assert "test_definition_new" in ids


class TestDefinitionVersionRegistration:
    """Tests for definition version registration."""

    def setup_method(self):
        """Set up persistence with default fixtures."""
        self.persistence = NLQPersistence()

    def test_register_new_definition_version(self):
        """Should register a new definition version."""
        version = DefinitionVersion(
            id="test_definition_v1",
            tenant_id="test_tenant",
            definition_id="test_definition",
            version="v1",
            status="draft",
            spec=DefinitionVersionSpec(
                required_events=["revenue_recognized"],
                measure={"op": "sum", "field": "amount"},
                allowed_dims=["customer"],
            ),
        )

        result = self.persistence.register_definition_version(version)
        assert result.id == "test_definition_v1"

        # Verify it was saved
        versions = self.persistence.get_definition_versions("test_tenant")
        ids = [v.id for v in versions]
        assert "test_definition_v1" in ids


class TestProofHookRegistration:
    """Tests for proof hook registration."""

    def setup_method(self):
        """Set up persistence with default fixtures."""
        self.persistence = NLQPersistence()

    def test_register_new_proof_hook(self):
        """Should register a new proof hook."""
        hook = ProofHook(
            id="test_proof_hook_new",
            tenant_id="test_tenant",
            definition_id="test_definition",
            pointer_template_json={
                "system": "TestSystem",
                "type": "report",
                "ref_template": "report:{id}",
            },
            availability_score=0.85,
        )

        result = self.persistence.register_proof_hook(hook)
        assert result.id == "test_proof_hook_new"

        # Verify it was saved
        hooks = self.persistence.get_proof_hooks("test_tenant")
        ids = [h.id for h in hooks]
        assert "test_proof_hook_new" in ids


class TestTenantIsolation:
    """Tests for tenant isolation in persistence."""

    def setup_method(self):
        """Set up persistence with default fixtures."""
        self.persistence = NLQPersistence()

    def test_bindings_isolated_by_tenant(self):
        """Bindings should be isolated by tenant."""
        # Create binding for tenant1
        binding1 = Binding(
            id="isolated_binding",
            tenant_id="tenant1",
            source_system="System1",
            canonical_event_id="revenue_recognized",
        )
        self.persistence.register_binding(binding1)

        # Create binding for tenant2
        binding2 = Binding(
            id="isolated_binding",
            tenant_id="tenant2",
            source_system="System2",
            canonical_event_id="invoice_posted",
        )
        self.persistence.register_binding(binding2)

        # Verify isolation
        tenant1_bindings = self.persistence.get_bindings("tenant1")
        tenant2_bindings = self.persistence.get_bindings("tenant2")

        tenant1_ids = [b.id for b in tenant1_bindings]
        tenant2_ids = [b.id for b in tenant2_bindings]

        # Both should have the binding but with different data
        assert "isolated_binding" in tenant1_ids
        assert "isolated_binding" in tenant2_ids

        # Verify data is different
        t1_binding = next(b for b in tenant1_bindings if b.id == "isolated_binding")
        t2_binding = next(b for b in tenant2_bindings if b.id == "isolated_binding")
        assert t1_binding.source_system == "System1"
        assert t2_binding.source_system == "System2"

    def test_default_tenant_fixtures_exist(self):
        """Default tenant should have fixture data."""
        events = self.persistence.get_events("default")
        assert len(events) > 0

        bindings = self.persistence.get_bindings("default")
        assert len(bindings) > 0

        definitions = self.persistence.get_definitions("default")
        assert len(definitions) > 0
