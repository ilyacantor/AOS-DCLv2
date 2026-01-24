"""
BLL Contract Tests - Validates BLL consumption contract endpoints.
"""
import pytest
from fastapi.testclient import TestClient
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.api.main import app

client = TestClient(app)


class TestDefinitionsEndpoint:
    def test_list_definitions_returns_list(self):
        response = client.get("/api/bll/definitions")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) >= 6
    
    def test_list_definitions_has_required_fields(self):
        response = client.get("/api/bll/definitions")
        data = response.json()
        for item in data:
            assert "definition_id" in item
            assert "name" in item
            assert "category" in item
            assert "version" in item
            assert "description" in item
    
    def test_get_definition_by_id(self):
        response = client.get("/api/bll/definitions/finops.saas_spend")
        assert response.status_code == 200
        data = response.json()
        assert data["definition_id"] == "finops.saas_spend"
        assert data["category"] == "finops"
        assert "output_schema" in data
        assert "sources" in data
    
    def test_get_definition_not_found(self):
        response = client.get("/api/bll/definitions/nonexistent.definition")
        assert response.status_code == 404


class TestExecuteEndpoint:
    def test_execute_saas_spend(self):
        response = client.post("/api/bll/execute", json={
            "dataset_id": "demo9",
            "definition_id": "finops.saas_spend"
        })
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert "metadata" in data
        assert "quality" in data
        assert "lineage" in data
    
    def test_execute_returns_metadata(self):
        response = client.post("/api/bll/execute", json={
            "dataset_id": "demo9",
            "definition_id": "finops.saas_spend"
        })
        data = response.json()
        metadata = data["metadata"]
        assert metadata["dataset_id"] == "demo9"
        assert metadata["definition_id"] == "finops.saas_spend"
        assert "executed_at" in metadata
        assert "execution_time_ms" in metadata
        assert "row_count" in metadata
    
    def test_execute_returns_quality_metrics(self):
        response = client.post("/api/bll/execute", json={
            "dataset_id": "demo9",
            "definition_id": "finops.saas_spend"
        })
        data = response.json()
        quality = data["quality"]
        assert "completeness" in quality
        assert "freshness_hours" in quality
        assert "row_count" in quality
        assert "null_percentage" in quality
        assert 0.0 <= quality["completeness"] <= 1.0
    
    def test_execute_returns_lineage(self):
        response = client.post("/api/bll/execute", json={
            "dataset_id": "demo9",
            "definition_id": "finops.saas_spend"
        })
        data = response.json()
        lineage = data["lineage"]
        assert isinstance(lineage, list)
        for item in lineage:
            assert "source_id" in item
            assert "table_id" in item
            assert "columns_used" in item
            assert "row_contribution" in item
    
    def test_execute_with_limit(self):
        response = client.post("/api/bll/execute", json={
            "dataset_id": "demo9",
            "definition_id": "finops.saas_spend",
            "limit": 5
        })
        assert response.status_code == 200
        data = response.json()
        assert len(data["data"]) <= 5
    
    def test_execute_invalid_definition(self):
        response = client.post("/api/bll/execute", json={
            "dataset_id": "demo9",
            "definition_id": "nonexistent.definition"
        })
        assert response.status_code == 400
    
    def test_execute_all_definitions(self):
        definitions_response = client.get("/api/bll/definitions")
        definitions = definitions_response.json()
        
        for defn in definitions:
            response = client.post("/api/bll/execute", json={
                "dataset_id": "demo9",
                "definition_id": defn["definition_id"]
            })
            assert response.status_code == 200, f"Failed for {defn['definition_id']}"


class TestProofEndpoint:
    def test_proof_returns_breadcrumbs(self):
        response = client.get("/api/bll/proof/finops.saas_spend")
        assert response.status_code == 200
        data = response.json()
        assert "breadcrumbs" in data
        assert isinstance(data["breadcrumbs"], list)
        assert len(data["breadcrumbs"]) > 0
    
    def test_proof_has_source_load_step(self):
        response = client.get("/api/bll/proof/finops.saas_spend")
        data = response.json()
        breadcrumbs = data["breadcrumbs"]
        source_load = next((b for b in breadcrumbs if b["action"] == "source_load"), None)
        assert source_load is not None
        assert "sources" in source_load["details"]
    
    def test_proof_includes_sql_equivalent(self):
        response = client.get("/api/bll/proof/finops.saas_spend")
        data = response.json()
        assert "sql_equivalent" in data
    
    def test_proof_not_found(self):
        response = client.get("/api/bll/proof/nonexistent.definition")
        assert response.status_code == 404


class TestDatasetEndpoint:
    def test_get_dataset_info(self):
        response = client.get("/api/bll/dataset")
        assert response.status_code == 200
        data = response.json()
        assert data["dataset_id"] == "demo9"
        assert data["env_var"] == "DCL_DATASET_ID"


class TestFinOpsDefinitions:
    def test_saas_spend_has_data(self):
        response = client.post("/api/bll/execute", json={
            "dataset_id": "demo9",
            "definition_id": "finops.saas_spend"
        })
        data = response.json()
        assert data["metadata"]["row_count"] > 0
    
    def test_top_vendor_deltas_has_data(self):
        response = client.post("/api/bll/execute", json={
            "dataset_id": "demo9",
            "definition_id": "finops.top_vendor_deltas_mom"
        })
        data = response.json()
        assert response.status_code == 200
    
    def test_unallocated_spend_has_data(self):
        response = client.post("/api/bll/execute", json={
            "dataset_id": "demo9",
            "definition_id": "finops.unallocated_spend"
        })
        data = response.json()
        assert response.status_code == 200


class TestAODDefinitions:
    def test_findings_by_severity(self):
        response = client.post("/api/bll/execute", json={
            "dataset_id": "demo9",
            "definition_id": "aod.findings_by_severity"
        })
        assert response.status_code == 200
    
    def test_identity_gap(self):
        response = client.post("/api/bll/execute", json={
            "dataset_id": "demo9",
            "definition_id": "aod.identity_gap_financially_anchored"
        })
        assert response.status_code == 200
    
    def test_zombies_overview(self):
        response = client.post("/api/bll/execute", json={
            "dataset_id": "demo9",
            "definition_id": "aod.zombies_overview"
        })
        assert response.status_code == 200


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
