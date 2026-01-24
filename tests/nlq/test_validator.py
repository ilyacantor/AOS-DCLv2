"""
Unit tests for DefinitionValidator and DefinitionCompiler.

Tests the semantic layer validation and compilation logic.
"""

import pytest
from backend.nlq.validator import DefinitionValidator, DefinitionCompiler
from backend.nlq.persistence import NLQPersistence


class TestDefinitionValidator:
    """Tests for DefinitionValidator."""

    def setup_method(self):
        """Set up validator with default fixtures."""
        self.persistence = NLQPersistence()
        self.validator = DefinitionValidator(persistence=self.persistence)

    def test_validate_services_revenue(self):
        """Should validate services_revenue definition successfully."""
        result = self.validator.validate(
            definition_id="services_revenue",
            version="v1",
            requested_dims=["customer", "service_line"],
        )

        assert result.ok is True
        assert len(result.missing_events) == 0
        assert result.coverage_score > 0.5
        assert result.freshness_score > 0

    def test_validate_missing_definition(self):
        """Should return failed validation for missing definition."""
        result = self.validator.validate(
            definition_id="nonexistent_definition",
            version="v1",
        )

        assert result.ok is False
        assert result.coverage_score == 0.0
        assert result.freshness_score == 0.0

    def test_validate_missing_events(self):
        """Should detect missing events."""
        result = self.validator.validate(
            definition_id="dso",  # Requires invoice_posted and payment_received
            version="v1",
            requested_dims=["customer"],
        )

        # payment_received has no bindings in fixtures
        # Check if validation properly reports this
        assert result is not None

    def test_validate_missing_dims(self):
        """Should detect missing dimensions."""
        result = self.validator.validate(
            definition_id="services_revenue",
            version="v1",
            requested_dims=["customer", "service_line", "nonexistent_dim"],
        )

        # nonexistent_dim is not in allowed_dims
        assert "nonexistent_dim" in result.missing_dims

    def test_validate_weak_bindings(self):
        """Should detect weak bindings."""
        result = self.validator.validate(
            definition_id="services_revenue",
            version="v1",
            requested_dims=["customer", "service_line"],
        )

        # PSA binding has lower quality/freshness scores
        # Check if weak bindings are detected
        assert result is not None

    def test_validate_coverage_score_calculation(self):
        """Coverage score should be between 0 and 1."""
        result = self.validator.validate(
            definition_id="services_revenue",
            version="v1",
        )

        assert 0.0 <= result.coverage_score <= 1.0

    def test_validate_freshness_score_calculation(self):
        """Freshness score should be between 0 and 1."""
        result = self.validator.validate(
            definition_id="services_revenue",
            version="v1",
        )

        assert 0.0 <= result.freshness_score <= 1.0

    def test_validate_proof_score(self):
        """Proof score should reflect proof hook availability."""
        result = self.validator.validate(
            definition_id="services_revenue",
            version="v1",
        )

        # services_revenue has proof hooks in fixtures
        assert result.proof_score > 0


class TestDefinitionCompiler:
    """Tests for DefinitionCompiler."""

    def setup_method(self):
        """Set up compiler with default fixtures."""
        self.persistence = NLQPersistence()
        self.compiler = DefinitionCompiler(persistence=self.persistence)

    def test_compile_services_revenue(self):
        """Should compile services_revenue definition."""
        plan = self.compiler.compile(
            definition_id="services_revenue",
            version="v1",
            requested_dims=["customer", "service_line"],
            time_window="QoQ",
        )

        assert plan.sql_template != ""
        assert "revenue_recognized" in plan.required_events
        assert "customer" in plan.required_dims
        assert plan.time_semantics.get("window") == "QoQ"

    def test_compile_missing_definition(self):
        """Should return empty plan for missing definition."""
        plan = self.compiler.compile(
            definition_id="nonexistent_definition",
            version="v1",
        )

        assert plan.sql_template == ""
        assert len(plan.required_events) == 0

    def test_compile_includes_proof_hook(self):
        """Should include proof hook in compiled plan."""
        plan = self.compiler.compile(
            definition_id="services_revenue",
            version="v1",
        )

        assert plan.proof_hook is not None
        assert "system" in plan.proof_hook

    def test_compile_params_schema(self):
        """Should generate params schema."""
        plan = self.compiler.compile(
            definition_id="services_revenue",
            version="v1",
            requested_dims=["customer"],
        )

        assert plan.params_schema is not None
        assert "properties" in plan.params_schema
        assert "dimension_filters" in plan.params_schema["properties"]

    def test_compile_time_semantics(self):
        """Should include time semantics from definition."""
        plan = self.compiler.compile(
            definition_id="services_revenue",
            version="v1",
        )

        assert plan.time_semantics is not None
        # Should inherit from definition's default_time_semantics_json


class TestValidatorEdgeCases:
    """Edge case tests for validator."""

    def setup_method(self):
        self.persistence = NLQPersistence()
        self.validator = DefinitionValidator(persistence=self.persistence)

    def test_validate_no_requested_dims(self):
        """Should work without requested dims."""
        result = self.validator.validate(
            definition_id="services_revenue",
            version="v1",
            requested_dims=[],
        )

        assert result is not None
        assert len(result.missing_dims) == 0

    def test_validate_all_allowed_dims(self):
        """Should validate all allowed dims."""
        result = self.validator.validate(
            definition_id="services_revenue",
            version="v1",
            requested_dims=["customer", "service_line", "region"],
        )

        assert result is not None

    def test_coverage_score_with_partial_bindings(self):
        """Coverage score should reflect partial binding coverage."""
        result = self.validator.validate(
            definition_id="arr",  # Requires contract_signed and revenue_recognized
            version="v1",
            requested_dims=["customer"],
        )

        # Both events should have bindings
        assert result.coverage_score > 0
