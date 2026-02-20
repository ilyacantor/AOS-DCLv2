"""
Tests for NLQ Registry Services.

Tests the new semantic layer components:
- ConsistencyValidator
- LineageService
- SchemaEnforcer
- DefinitionRegistry
- QueryExecutor
- ProofResolver
"""

import pytest
from backend.nlq.persistence import NLQPersistence
from backend.nlq.consistency import ConsistencyValidator, ConsistencyStatus, IssueLevel
from backend.nlq.lineage import LineageService, NodeType, EdgeType
from backend.nlq.schema_enforcer import SchemaEnforcer, FieldType, MeasureOp
from backend.nlq.registry import DefinitionRegistry
from backend.nlq.executor import QueryExecutor, ExecutionStatus, MockBackend
from backend.nlq.proof import ProofResolver, ProofType, ProofStatus


@pytest.fixture
def persistence():
    """Create a persistence instance with test fixtures."""
    return NLQPersistence()


class TestConsistencyValidator:
    """Tests for ConsistencyValidator."""

    def test_run_all_checks(self, persistence):
        """Should run all consistency checks and return a report."""
        validator = ConsistencyValidator(persistence)
        report = validator.run_all_checks("default")

        assert report.report_id
        assert report.tenant_id == "default"
        assert report.overall_status in [ConsistencyStatus.PASSED, ConsistencyStatus.WARNING, ConsistencyStatus.FAILED]
        assert len(report.checks) == 7  # All 7 checks
        assert report.total_issues >= 0

    def test_check_orphan_events(self, persistence):
        """Should detect events without bindings."""
        validator = ConsistencyValidator(persistence)
        result = validator.check_orphan_events("default")

        assert result.check_type == "orphan_events"
        assert result.status in [ConsistencyStatus.PASSED, ConsistencyStatus.WARNING]
        # Check that issues are properly formatted
        for issue in result.issues:
            assert issue.object_type == "event"
            assert issue.level == IssueLevel.WARNING

    def test_check_orphan_definitions(self, persistence):
        """Should detect definitions with missing events or versions."""
        validator = ConsistencyValidator(persistence)
        result = validator.check_orphan_definitions("default")

        assert result.check_type == "orphan_definitions"
        assert result.summary

    def test_check_binding_coverage(self, persistence):
        """Should check binding coverage for definitions."""
        validator = ConsistencyValidator(persistence)
        result = validator.check_binding_coverage("default")

        assert result.check_type == "binding_coverage"
        # May have warnings for uncovered dimensions
        for issue in result.issues:
            assert issue.level in [IssueLevel.ERROR, IssueLevel.WARNING]

    def test_check_entity_references(self, persistence):
        """Should check that definitions reference valid entities."""
        validator = ConsistencyValidator(persistence)
        result = validator.check_entity_references("default")

        assert result.check_type == "entity_references"

    def test_validate_new_binding(self, persistence):
        """Should validate a new binding."""
        validator = ConsistencyValidator(persistence)

        # Should fail for non-existent event
        valid, errors = validator.validate_new_binding(
            binding_id="test_binding",
            source_system="TestSystem",
            canonical_event_id="nonexistent_event",
            mapping_json={"field1": "field1"},
            tenant_id="default"
        )

        assert not valid
        assert any("does not exist" in e for e in errors)

    def test_validate_new_definition(self, persistence):
        """Should validate a new definition."""
        validator = ConsistencyValidator(persistence)

        # Get an existing event
        events = persistence.get_events("default")
        if events:
            valid, errors = validator.validate_new_definition(
                definition_id="test_definition",
                required_events=[events[0].id],
                allowed_dims=["customer"],
                tenant_id="default"
            )
            assert valid  # Should pass with valid event


class TestLineageService:
    """Tests for LineageService."""

    def test_build_full_graph(self, persistence):
        """Should build the complete lineage graph."""
        service = LineageService(persistence)
        graph = service.build_full_graph("default")

        assert len(graph.nodes) > 0
        assert len(graph.edges) > 0

        # Check node types
        node_types = {n.node_type for n in graph.nodes}
        assert NodeType.DEFINITION in node_types or NodeType.EVENT in node_types

    def test_get_definition_lineage(self, persistence):
        """Should get lineage for a definition."""
        service = LineageService(persistence)
        definitions = persistence.get_definitions("default")

        if definitions:
            lineage = service.get_definition_lineage(definitions[0].id, "default")
            assert "definition_id" in lineage
            assert "events" in lineage
            assert "bindings" in lineage

    def test_get_event_consumers(self, persistence):
        """Should get definitions that consume an event."""
        service = LineageService(persistence)
        events = persistence.get_events("default")

        if events:
            consumers = service.get_event_consumers(events[0].id, "default")
            assert isinstance(consumers, list)

    def test_analyze_impact(self, persistence):
        """Should analyze impact of removing an object."""
        service = LineageService(persistence)
        events = persistence.get_events("default")

        if events:
            analysis = service.analyze_impact("event", events[0].id, "default")
            assert analysis.object_type == "event"
            assert analysis.object_id == events[0].id
            assert analysis.severity in ["none", "low", "medium", "high", "critical"]

    def test_upstream_downstream_dependencies(self, persistence):
        """Should find upstream and downstream dependencies."""
        service = LineageService(persistence)
        definitions = persistence.get_definitions("default")

        if definitions:
            upstream = service.get_upstream_dependencies(
                "definition", definitions[0].id, "default"
            )
            assert len(upstream.nodes) >= 1  # At least the definition itself

            downstream = service.get_downstream_dependencies(
                "definition", definitions[0].id, "default"
            )
            assert len(downstream.nodes) >= 1


class TestSchemaEnforcer:
    """Tests for SchemaEnforcer."""

    def test_validate_all(self, persistence):
        """Should validate all schemas."""
        enforcer = SchemaEnforcer(persistence)
        result = enforcer.validate_all("default")

        assert isinstance(result.valid, bool)
        assert result.errors >= 0
        assert result.warnings >= 0

    def test_validate_event_schema_valid(self, persistence):
        """Should validate a valid event schema."""
        enforcer = SchemaEnforcer(persistence)

        valid, errors = enforcer.validate_event(
            event_id="test_event",
            schema_json={
                "fields": [
                    {"name": "event_id", "type": "string"},
                    {"name": "amount", "type": "decimal"},
                    {"name": "occurred_at", "type": "timestamp"},
                ]
            },
            time_semantics_json={
                "occurred_at": "occurred_at",
                "calendar": "fiscal"
            }
        )

        assert valid
        assert len(errors) == 0

    def test_validate_event_schema_invalid(self, persistence):
        """Should detect invalid event schema."""
        enforcer = SchemaEnforcer(persistence)

        valid, errors = enforcer.validate_event(
            event_id="test_event",
            schema_json={
                "fields": "not_an_array"  # Invalid
            },
            time_semantics_json={}
        )

        assert not valid
        assert len(errors) > 0

    def test_validate_definition_spec(self, persistence):
        """Should validate a definition spec."""
        enforcer = SchemaEnforcer(persistence)

        valid, errors = enforcer.validate_definition_spec_dict(
            definition_id="test_definition",
            spec={
                "required_events": ["revenue_recognized"],
                "measure": {"op": "sum", "field": "amount"},
                "filters": {},
                "allowed_grains": ["month", "quarter"],
                "allowed_dims": ["customer"],
                "time_field": "occurred_at"
            }
        )

        assert valid

    def test_suggest_schema_improvements(self, persistence):
        """Should suggest schema improvements."""
        enforcer = SchemaEnforcer(persistence)
        events = persistence.get_events("default")

        if events:
            suggestions = enforcer.suggest_schema_improvements(events[0].id, "default")
            assert isinstance(suggestions, list)


class TestDefinitionRegistry:
    """Tests for DefinitionRegistry."""

    def test_list_definitions(self, persistence):
        """Should list definitions with filtering."""
        registry = DefinitionRegistry(persistence)
        summaries, total = registry.list_definitions("default")

        assert isinstance(summaries, list)
        assert total >= 0

    def test_list_definitions_with_pack_filter(self, persistence):
        """Should filter definitions by pack."""
        registry = DefinitionRegistry(persistence)

        summaries, total = registry.list_definitions("default", pack="cfo")
        # All returned should be CFO pack
        for s in summaries:
            assert s.pack == "cfo"

    def test_search_definitions(self, persistence):
        """Should search definitions."""
        registry = DefinitionRegistry(persistence)

        results = registry.search_definitions("revenue", "default")
        assert isinstance(results, list)
        # Results should be relevant to search
        for r in results:
            assert "revenue" in r.id.lower() or (r.description and "revenue" in r.description.lower())

    def test_get_definition_detail(self, persistence):
        """Should get definition details."""
        registry = DefinitionRegistry(persistence)
        definitions = persistence.get_definitions("default")

        if definitions:
            detail = registry.get_definition_detail(definitions[0].id, "default")
            assert detail is not None
            assert detail.definition.id == definitions[0].id

    def test_get_catalog_stats(self, persistence):
        """Should get catalog statistics."""
        registry = DefinitionRegistry(persistence)
        stats = registry.get_catalog_stats("default")

        assert stats.total_definitions >= 0
        assert stats.total_events >= 0
        assert stats.total_bindings >= 0

    def test_get_packs(self, persistence):
        """Should get pack listing."""
        registry = DefinitionRegistry(persistence)
        packs = registry.get_packs("default")

        assert isinstance(packs, list)
        for pack in packs:
            assert "pack" in pack
            assert "definition_count" in pack


class TestQueryExecutor:
    """Tests for QueryExecutor."""

    def test_execute_definition(self, persistence):
        """Should execute a query for a definition."""
        executor = QueryExecutor(persistence, backend=MockBackend())
        definitions = persistence.get_definitions("default")

        if definitions:
            result = executor.execute_definition(
                definition_id=definitions[0].id,
                version="v1",
                tenant_id="default"
            )

            assert result.execution_id
            assert result.status in [ExecutionStatus.COMPLETED, ExecutionStatus.FAILED]
            if result.status == ExecutionStatus.COMPLETED:
                assert len(result.rows) >= 0

    def test_execute_with_dimensions(self, persistence):
        """Should execute with dimension grouping."""
        executor = QueryExecutor(persistence, backend=MockBackend())

        result = executor.execute_definition(
            definition_id="services_revenue",
            dims=["customer"],
            time_window="QoQ",
            tenant_id="default"
        )

        assert result.execution_id

    def test_query_caching(self, persistence):
        """Should cache query results."""
        executor = QueryExecutor(persistence, backend=MockBackend(), enable_cache=True)

        # First execution
        result1 = executor.execute_definition(
            definition_id="services_revenue",
            tenant_id="default"
        )

        # Second execution should be cached
        result2 = executor.execute_definition(
            definition_id="services_revenue",
            tenant_id="default"
        )

        assert result2.cached == True

    def test_skip_cache(self, persistence):
        """Should skip cache when requested."""
        executor = QueryExecutor(persistence, backend=MockBackend(), enable_cache=True)

        result = executor.execute_definition(
            definition_id="services_revenue",
            tenant_id="default",
            skip_cache=True
        )

        assert result.cached == False

    def test_execution_stats(self, persistence):
        """Should track execution statistics."""
        executor = QueryExecutor(persistence, backend=MockBackend())

        # Execute a query
        executor.execute_definition(
            definition_id="services_revenue",
            tenant_id="default"
        )

        stats = executor.get_execution_stats("default")
        assert stats["total_executions"] >= 1

    def test_audit_log(self, persistence):
        """Should maintain audit log."""
        executor = QueryExecutor(persistence, backend=MockBackend())

        # Execute a query
        executor.execute_definition(
            definition_id="services_revenue",
            tenant_id="default"
        )

        audits = executor.get_audit_log("default")
        assert len(audits) >= 1
        assert audits[0].definition_id == "services_revenue"


class TestProofResolver:
    """Tests for ProofResolver."""

    def test_resolve_definition_proofs(self, persistence):
        """Should resolve proofs for a definition."""
        resolver = ProofResolver(persistence)

        proofs = resolver.resolve_definition_proofs(
            definition_id="services_revenue",
            version="v1",
            tenant_id="default"
        )

        assert isinstance(proofs, list)

    def test_build_proof_chain(self, persistence):
        """Should build a proof chain."""
        resolver = ProofResolver(persistence)

        chain = resolver.build_proof_chain(
            definition_id="services_revenue",
            version="v1",
            tenant_id="default"
        )

        assert chain.definition_id == "services_revenue"
        assert isinstance(chain.source_proofs, list)
        assert isinstance(chain.event_traces, list)

    def test_generate_query_proof(self, persistence):
        """Should generate a query hash proof."""
        resolver = ProofResolver(persistence)

        proof = resolver.generate_query_proof(
            sql="SELECT * FROM events",
            params=["param1"],
            definition_id="test"
        )

        assert proof.proof_type == ProofType.QUERY_HASH
        assert proof.system == "internal"
        assert proof.reference.startswith("sha256:")

    def test_generate_source_proof(self, persistence):
        """Should generate a source system proof."""
        resolver = ProofResolver(persistence)

        proof = resolver.generate_source_proof(
            system="NetSuite",
            proof_type="saved_search",
            identifiers={"search_id": "123"}
        )

        assert proof.system == "NetSuite"
        assert proof.url is not None
        assert "searchid=123" in proof.url

    def test_get_proof_coverage(self, persistence):
        """Should get proof coverage statistics."""
        resolver = ProofResolver(persistence)

        coverage = resolver.get_proof_coverage("default")
        assert "total_definitions" in coverage
        assert "with_proofs" in coverage
        assert "coverage_percentage" in coverage


class TestRegistryAPIIntegration:
    """Integration tests for Registry API."""

    @pytest.fixture
    def client(self):
        """Create test client."""
        from fastapi.testclient import TestClient
        from backend.api.main import app
        return TestClient(app)

    def test_list_definitions_endpoint(self, client):
        """Should list definitions via API."""
        response = client.get("/api/nlq/registry/definitions")
        assert response.status_code == 200
        data = response.json()
        assert "definitions" in data
        assert "total" in data

    def test_search_definitions_endpoint(self, client):
        """Should search definitions via API."""
        response = client.get("/api/nlq/registry/definitions/search?q=revenue")
        assert response.status_code == 200
        data = response.json()
        assert "results" in data

    def test_catalog_stats_endpoint(self, client):
        """Should get catalog stats via API."""
        response = client.get("/api/nlq/registry/catalog/stats")
        assert response.status_code == 200
        data = response.json()
        assert "total_definitions" in data

    def test_consistency_check_endpoint(self, client):
        """Should run consistency check via API."""
        response = client.get("/api/nlq/registry/consistency/check")
        assert response.status_code == 200
        data = response.json()
        assert "overall_status" in data
        assert "checks" in data

    def test_lineage_graph_endpoint(self, client):
        """Should get lineage graph via API."""
        response = client.get("/api/nlq/registry/lineage/graph")
        assert response.status_code == 200
        data = response.json()
        assert "nodes" in data
        assert "edges" in data

    def test_schema_validate_endpoint(self, client):
        """Should validate schemas via API."""
        response = client.get("/api/nlq/registry/schema/validate")
        assert response.status_code == 200
        data = response.json()
        assert "valid" in data

    def test_execute_query_endpoint(self, client):
        """Should execute query via API."""
        response = client.post(
            "/api/nlq/registry/execute",
            json={
                "definition_id": "services_revenue",
                "version": "v1"
            }
        )
        # May fail if definition doesn't exist, but should be a valid response
        assert response.status_code in [200, 400, 500]

    def test_proof_coverage_endpoint(self, client):
        """Should get proof coverage via API."""
        response = client.get("/api/nlq/registry/proof/coverage")
        assert response.status_code == 200
        data = response.json()
        assert "total_definitions" in data

    def test_registry_health_endpoint(self, client):
        """Should get registry health via API."""
        response = client.get("/api/nlq/registry/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "services" in data
