"""
Unit tests for NLQ Persistence layer.

Tests fixture loading and semantic query helpers.
"""

import pytest
from pathlib import Path
from backend.nlq.persistence import NLQPersistence


class TestNLQPersistence:
    """Tests for NLQPersistence with JSON fixtures."""

    def setup_method(self):
        """Set up persistence with default fixtures."""
        self.persistence = NLQPersistence()

    def test_get_events(self):
        """Should load canonical events from fixtures."""
        events = self.persistence.get_events()
        assert len(events) > 0

        # Check for expected events
        event_ids = [e.id for e in events]
        assert "revenue_recognized" in event_ids
        assert "invoice_posted" in event_ids
        assert "mapping_changed" in event_ids

    def test_get_event_by_id(self):
        """Should retrieve specific event by ID."""
        event = self.persistence.get_event("revenue_recognized")
        assert event is not None
        assert event.id == "revenue_recognized"
        assert event.time_semantics is not None

    def test_get_event_not_found(self):
        """Should return None for non-existent event."""
        event = self.persistence.get_event("nonexistent_event")
        assert event is None

    def test_event_exists(self):
        """Should check event existence correctly."""
        assert self.persistence.event_exists("revenue_recognized") is True
        assert self.persistence.event_exists("nonexistent") is False

    def test_get_entities(self):
        """Should load entities from fixtures."""
        entities = self.persistence.get_entities()
        assert len(entities) > 0

        entity_ids = [e.id for e in entities]
        assert "customer" in entity_ids
        assert "service_line" in entity_ids
        assert "region" in entity_ids

    def test_get_entity_by_id(self):
        """Should retrieve specific entity by ID."""
        entity = self.persistence.get_entity("customer")
        assert entity is not None
        assert entity.id == "customer"
        assert "primary" in entity.identifiers_json

    def test_get_bindings(self):
        """Should load bindings from fixtures."""
        bindings = self.persistence.get_bindings()
        assert len(bindings) > 0

        # Check binding properties
        for binding in bindings:
            assert binding.source_system
            assert binding.maps_to
            assert 0 <= binding.quality_score <= 1

    def test_get_bindings_for_event(self):
        """Should filter bindings by event ID."""
        bindings = self.persistence.get_bindings_for_event("revenue_recognized")
        assert len(bindings) > 0

        for binding in bindings:
            assert binding.maps_to == "revenue_recognized"
            assert binding.binding_type == "event"

    def test_get_binding_quality(self):
        """Should calculate average binding quality for event."""
        quality = self.persistence.get_binding_quality("revenue_recognized")
        assert 0 <= quality <= 1
        assert quality > 0  # Should have at least one binding

    def test_get_binding_quality_no_bindings(self):
        """Should return 0 for event with no bindings."""
        quality = self.persistence.get_binding_quality("nonexistent_event")
        assert quality == 0.0

    def test_get_available_dims(self):
        """Should get available dimensions for event."""
        dims = self.persistence.get_available_dims("revenue_recognized")
        assert len(dims) > 0
        assert "customer" in dims
        assert "service_line" in dims

    def test_get_definitions(self):
        """Should load definitions from fixtures."""
        definitions = self.persistence.get_definitions()
        assert len(definitions) > 0

        def_ids = [d.id for d in definitions]
        assert "services_revenue" in def_ids

    def test_get_definition_by_id(self):
        """Should retrieve specific definition by ID."""
        definition = self.persistence.get_definition("services_revenue")
        assert definition is not None
        assert definition.id == "services_revenue"
        assert definition.quality_score > 0

    def test_definition_has_inputs(self):
        """Definition should have input specification."""
        definition = self.persistence.get_definition("services_revenue")
        assert definition is not None
        assert "events" in definition.inputs_json

    def test_get_proof_hooks(self):
        """Should load proof hooks from fixtures."""
        hooks = self.persistence.get_proof_hooks()
        assert len(hooks) > 0

        for hook in hooks:
            assert hook.definition_id
            assert 0 <= hook.availability_score <= 1

    def test_get_proof_hooks_for_definition(self):
        """Should filter proof hooks by definition ID."""
        hooks = self.persistence.get_proof_hooks_for_definition("services_revenue")
        assert len(hooks) > 0

        for hook in hooks:
            assert hook.definition_id == "services_revenue"

    def test_get_proof_availability(self):
        """Should calculate proof availability for definition."""
        availability = self.persistence.get_proof_availability("services_revenue")
        assert 0 <= availability <= 1
        assert availability > 0  # Should have at least one proof hook

    def test_resolve_definition_by_hint(self):
        """Should resolve definition from metric hint."""
        definition = self.persistence.resolve_definition(
            metric_hint="services_revenue"
        )
        assert definition is not None
        assert definition.id == "services_revenue"

    def test_resolve_definition_by_keywords(self):
        """Should resolve definition from keywords."""
        definition = self.persistence.resolve_definition(
            keywords=["services", "revenue"]
        )
        assert definition is not None
        assert definition.id == "services_revenue"

    def test_resolve_definition_no_match(self):
        """Should return None when no definition matches."""
        definition = self.persistence.resolve_definition(
            keywords=["nonexistent", "metric"]
        )
        assert definition is None

    def test_check_event_binding(self):
        """Should check which events have bindings."""
        result = self.persistence.check_event_binding([
            "revenue_recognized",
            "invoice_posted",
            "nonexistent_event",
        ])

        assert result["revenue_recognized"] is True
        assert result["invoice_posted"] is True
        assert result["nonexistent_event"] is False

    def test_check_dims_available(self):
        """Should check which dims are available for events."""
        result = self.persistence.check_dims_available(
            dim_ids=["customer", "service_line", "nonexistent_dim"],
            event_ids=["revenue_recognized"],
        )

        assert result["customer"] is True
        assert result["service_line"] is True
        assert result["nonexistent_dim"] is False

    def test_cache_clearing(self):
        """Should clear cache correctly."""
        # Load to populate cache
        self.persistence.get_events()
        assert len(self.persistence._cache) > 0

        # Clear cache
        self.persistence.clear_cache()
        assert len(self.persistence._cache) == 0


class TestNLQPersistenceMissingFixtures:
    """Tests for persistence with missing fixtures."""

    def test_missing_fixture_returns_empty_list(self):
        """Should return empty list for missing fixture file."""
        persistence = NLQPersistence(
            fixtures_dir=Path("/nonexistent/path")
        )

        events = persistence.get_events()
        assert events == []
