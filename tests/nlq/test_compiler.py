"""
Unit tests for SQL Compiler and Filter DSL.

Tests filter interpretation, SQL generation, and proof resolution.
"""

import pytest
from backend.nlq.compiler import (
    FilterDSL,
    TimeWindowInterpreter,
    SQLCompiler,
    ProofResolver,
    CachedValidationMixin,
)
from backend.nlq.persistence import NLQPersistence
from backend.nlq.models import DefinitionVersionSpec


class TestFilterDSL:
    """Tests for FilterDSL interpreter."""

    def test_eq_operator(self):
        """Should generate equality condition."""
        filter_spec = {"op": "eq", "value": "Professional Services"}
        sql, params = FilterDSL.interpret(filter_spec, "service_line")
        assert "service_line = %s" in sql
        assert params == ["Professional Services"]

    def test_neq_operator(self):
        """Should generate not-equal condition."""
        filter_spec = {"op": "neq", "value": "Internal"}
        sql, params = FilterDSL.interpret(filter_spec, "service_line")
        assert "service_line != %s" in sql
        assert params == ["Internal"]

    def test_in_operator(self):
        """Should generate IN clause."""
        filter_spec = {"op": "in", "values": ["A", "B", "C"]}
        sql, params = FilterDSL.interpret(filter_spec, "category")
        assert "category IN" in sql
        assert params == ["A", "B", "C"]

    def test_not_in_operator(self):
        """Should generate NOT IN clause."""
        filter_spec = {"op": "not_in", "values": ["X", "Y"]}
        sql, params = FilterDSL.interpret(filter_spec, "category")
        assert "category NOT IN" in sql
        assert params == ["X", "Y"]

    def test_gt_operator(self):
        """Should generate greater-than condition."""
        filter_spec = {"op": "gt", "value": 100}
        sql, params = FilterDSL.interpret(filter_spec, "amount")
        assert "amount > %s" in sql
        assert params == [100]

    def test_gte_operator(self):
        """Should generate greater-than-or-equal condition."""
        filter_spec = {"op": "gte", "value": 100}
        sql, params = FilterDSL.interpret(filter_spec, "amount")
        assert "amount >= %s" in sql
        assert params == [100]

    def test_lt_operator(self):
        """Should generate less-than condition."""
        filter_spec = {"op": "lt", "value": 50}
        sql, params = FilterDSL.interpret(filter_spec, "amount")
        assert "amount < %s" in sql
        assert params == [50]

    def test_lte_operator(self):
        """Should generate less-than-or-equal condition."""
        filter_spec = {"op": "lte", "value": 50}
        sql, params = FilterDSL.interpret(filter_spec, "amount")
        assert "amount <= %s" in sql
        assert params == [50]

    def test_between_operator(self):
        """Should generate BETWEEN clause."""
        filter_spec = {"op": "between", "min": 10, "max": 100}
        sql, params = FilterDSL.interpret(filter_spec, "amount")
        assert "amount BETWEEN %s AND %s" in sql
        assert params == [10, 100]

    def test_like_operator(self):
        """Should generate LIKE clause."""
        filter_spec = {"op": "like", "pattern": "%Services%"}
        sql, params = FilterDSL.interpret(filter_spec, "description")
        assert "description LIKE %s" in sql
        assert params == ["%Services%"]

    def test_is_null_operator(self):
        """Should generate IS NULL condition."""
        filter_spec = {"op": "is_null"}
        sql, params = FilterDSL.interpret(filter_spec, "optional_field")
        assert "optional_field IS NULL" in sql
        assert params == []

    def test_is_not_null_operator(self):
        """Should generate IS NOT NULL condition."""
        filter_spec = {"op": "is_not_null"}
        sql, params = FilterDSL.interpret(filter_spec, "required_field")
        assert "required_field IS NOT NULL" in sql
        assert params == []

    def test_unknown_operator_raises(self):
        """Should raise ValueError for unknown operator."""
        filter_spec = {"op": "unknown_op"}
        with pytest.raises(ValueError):
            FilterDSL.interpret(filter_spec, "field")


class TestTimeWindowInterpreter:
    """Tests for TimeWindowInterpreter."""

    def test_qoq_window(self):
        """Should generate QoQ time conditions."""
        sql, params = TimeWindowInterpreter.interpret("QoQ", "recognized_at")
        assert "recognized_at" in sql
        # QoQ should have date comparison
        assert len(params) > 0

    def test_yoy_window(self):
        """Should generate YoY time conditions."""
        sql, params = TimeWindowInterpreter.interpret("YoY", "recognized_at")
        assert "recognized_at" in sql
        assert len(params) > 0

    def test_mtd_window(self):
        """Should generate MTD time conditions."""
        sql, params = TimeWindowInterpreter.interpret("MTD", "recognized_at")
        assert "recognized_at" in sql
        assert len(params) > 0

    def test_qtd_window(self):
        """Should generate QTD time conditions."""
        sql, params = TimeWindowInterpreter.interpret("QTD", "recognized_at")
        assert "recognized_at" in sql

    def test_ytd_window(self):
        """Should generate YTD time conditions."""
        sql, params = TimeWindowInterpreter.interpret("YTD", "recognized_at")
        assert "recognized_at" in sql

    def test_unknown_window_returns_true(self):
        """Unknown window should return TRUE (no filter)."""
        sql, params = TimeWindowInterpreter.interpret("unknown", "recognized_at")
        assert sql == "TRUE"
        assert params == []


class TestSQLCompiler:
    """Tests for SQLCompiler."""

    def setup_method(self):
        """Set up compiler with default fixtures."""
        self.persistence = NLQPersistence()
        self.compiler = SQLCompiler(persistence=self.persistence)

    def test_compile_services_revenue(self):
        """Should compile services_revenue definition."""
        version = self.persistence.get_definition_version("services_revenue", "v1")
        sql, params, metadata = self.compiler.compile(
            spec=version.spec,
            definition_id="services_revenue",
            requested_dims=["customer", "service_line"],
            time_window="QoQ",
        )

        assert sql != ""
        assert "SELECT" in sql
        assert "SUM" in sql
        assert "GROUP BY" in sql

    def test_compile_with_filters(self):
        """Should include filter conditions."""
        version = self.persistence.get_definition_version("services_revenue", "v1")
        sql, params, metadata = self.compiler.compile(
            spec=version.spec,
            definition_id="services_revenue",
        )

        # services_revenue has service_line filter
        assert "service_line IN" in sql

    def test_compile_with_additional_filters(self):
        """Should include additional filters."""
        version = self.persistence.get_definition_version("total_revenue", "v1")
        sql, params, metadata = self.compiler.compile(
            spec=version.spec,
            definition_id="total_revenue",
            additional_filters={"region": {"op": "eq", "value": "US"}},
        )

        assert "region = %s" in sql
        assert "US" in params

    def test_compile_returns_metadata(self):
        """Should return metadata about the query."""
        version = self.persistence.get_definition_version("services_revenue", "v1")
        sql, params, metadata = self.compiler.compile(
            spec=version.spec,
            definition_id="services_revenue",
        )

        assert "required_events" in metadata
        assert "requested_dims" in metadata

    def test_compile_missing_definition_returns_empty(self):
        """Should return empty for missing definition."""
        empty_spec = DefinitionVersionSpec()
        sql, params, metadata = self.compiler.compile(
            spec=empty_spec,
            definition_id="nonexistent",
        )

        assert sql == ""
        assert params == []


class TestProofResolver:
    """Tests for ProofResolver."""

    def setup_method(self):
        """Set up resolver with default fixtures."""
        self.persistence = NLQPersistence()
        self.resolver = ProofResolver(persistence=self.persistence)

    def test_resolve_netsuite_proof(self):
        """Should resolve NetSuite proof hook."""
        hooks = self.persistence.get_proof_hooks_for_definition("services_revenue")
        netsuite_hook = next(h for h in hooks if "netsuite" in h.id.lower())

        result = self.resolver.resolve(
            proof_hook=netsuite_hook,
            context={
                "period": "2024Q1",
                "search_id": "123",
                "hash": "abc123",
            },
        )

        assert result["system"] == "NetSuite"
        assert result["type"] == "saved_search"
        assert "url" in result

    def test_resolve_psa_proof(self):
        """Should resolve PSA proof hook."""
        hooks = self.persistence.get_proof_hooks_for_definition("services_revenue")
        psa_hook = next(h for h in hooks if "psa" in h.id.lower())

        result = self.resolver.resolve(
            proof_hook=psa_hook,
            context={"period": "2024Q1"},
        )

        assert result["system"] == "PSA"
        assert result["type"] == "report"


class TestCachedValidationMixin:
    """Tests for validation caching."""

    def test_cache_key_generation(self):
        """Should generate unique cache keys."""
        key1 = CachedValidationMixin._cache_key(
            "def1", "v1", ["dim1"], "QoQ", "tenant1"
        )
        key2 = CachedValidationMixin._cache_key(
            "def1", "v1", ["dim1"], "YoY", "tenant1"
        )
        key3 = CachedValidationMixin._cache_key(
            "def1", "v1", ["dim1", "dim2"], "QoQ", "tenant1"
        )

        assert key1 != key2
        assert key1 != key3

    def test_cache_key_same_for_same_inputs(self):
        """Same inputs should generate same cache key."""
        key1 = CachedValidationMixin._cache_key(
            "def1", "v1", ["dim1", "dim2"], "QoQ", "tenant1"
        )
        key2 = CachedValidationMixin._cache_key(
            "def1", "v1", ["dim1", "dim2"], "QoQ", "tenant1"
        )

        assert key1 == key2
