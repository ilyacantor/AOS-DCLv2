"""
Stage 3B Harness — Entity Resolution Persistence
Tests workspace creation from triple overlap and decision persistence.
"""
import pytest
import uuid
from backend.engine.entity_resolution_v2 import EntityResolutionV2
from backend.core.db import get_connection

TENANT_ID = "400aa910-a6b4-5d44-ab9f-e6aecde37721"
RUN_ID = "6754a9d7-387a-553f-8c4c-978bfbbfca13"

CUSTOMER_OVERLAP = 34
VENDOR_OVERLAP = 170
EMPLOYEE_OVERLAP = 10
TOTAL_OVERLAP = 214


@pytest.fixture(autouse=True)
def clean_workspaces():
    """Delete all resolution_workspaces_v2 rows for test tenant/run before each test."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM resolution_workspaces_v2 WHERE tenant_id = %s AND run_id = %s",
                (TENANT_ID, RUN_ID),
            )
            # Also reset canonical_id on triples that tests may have set
            cur.execute(
                "UPDATE semantic_triples SET canonical_id = NULL, resolution_method = NULL, "
                "resolution_confidence = NULL WHERE tenant_id = %s AND run_id = %s "
                "AND canonical_id IS NOT NULL",
                (TENANT_ID, RUN_ID),
            )
            conn.commit()
    yield


@pytest.fixture
def resolver():
    return EntityResolutionV2(TENANT_ID, RUN_ID)


# --- Test 1: Workspace creation from overlap ---
def test_create_workspaces(resolver):
    result = resolver.create_workspaces_from_overlap()
    assert result["created"] == TOTAL_OVERLAP
    assert result["by_domain"]["customer"] == CUSTOMER_OVERLAP
    assert result["by_domain"]["vendor"] == VENDOR_OVERLAP
    assert result["by_domain"]["employee"] == EMPLOYEE_OVERLAP

# --- Test 2: Idempotency ---
def test_create_workspaces_idempotent(resolver):
    resolver.create_workspaces_from_overlap()
    result2 = resolver.create_workspaces_from_overlap()
    assert result2["created"] == 0  # no new workspaces on second call

# --- Test 3: List workspaces ---
def test_list_all_workspaces(resolver):
    resolver.create_workspaces_from_overlap()
    workspaces = resolver.list_workspaces()
    assert len(workspaces) == TOTAL_OVERLAP

def test_list_customer_workspaces(resolver):
    resolver.create_workspaces_from_overlap()
    workspaces = resolver.list_workspaces(domain="customer")
    assert len(workspaces) == CUSTOMER_OVERLAP

def test_list_pending_workspaces(resolver):
    resolver.create_workspaces_from_overlap()
    workspaces = resolver.list_workspaces(status="pending")
    assert len(workspaces) == TOTAL_OVERLAP  # all start as pending

# --- Test 4: Confirm match ---
def test_confirm_match(resolver):
    resolver.create_workspaces_from_overlap()
    workspaces = resolver.list_workspaces(domain="customer")
    ws = workspaces[0]
    canonical = f"canonical-{uuid.uuid4().hex[:8]}"
    result = resolver.confirm_match(ws["workspace_id"], canonical)
    assert result["status"] == "confirmed"
    assert result["canonical_id"] == canonical

# --- Test 5: Reject match ---
def test_reject_match(resolver):
    resolver.create_workspaces_from_overlap()
    workspaces = resolver.list_workspaces(domain="vendor")
    ws = workspaces[0]
    result = resolver.reject_match(ws["workspace_id"])
    assert result["status"] == "rejected"

# --- Test 6: Escalate ---
def test_escalate(resolver):
    resolver.create_workspaces_from_overlap()
    workspaces = resolver.list_workspaces(domain="employee")
    ws = workspaces[0]
    result = resolver.escalate(ws["workspace_id"], reason="Ambiguous name match")
    assert result["status"] == "escalated"

# --- Test 7: Undo decision ---
def test_undo_confirm(resolver):
    resolver.create_workspaces_from_overlap()
    workspaces = resolver.list_workspaces(domain="customer")
    ws = workspaces[0]
    canonical = f"canonical-{uuid.uuid4().hex[:8]}"
    resolver.confirm_match(ws["workspace_id"], canonical)
    result = resolver.undo_decision(ws["workspace_id"])
    assert result["status"] == "pending"

# --- Test 8: Decision persists across instances ---
def test_decision_persists(resolver):
    resolver.create_workspaces_from_overlap()
    workspaces = resolver.list_workspaces(domain="customer")
    ws = workspaces[0]
    canonical = f"canonical-{uuid.uuid4().hex[:8]}"
    resolver.confirm_match(ws["workspace_id"], canonical)

    # Create a new instance — decision should persist
    resolver2 = EntityResolutionV2(TENANT_ID, RUN_ID)
    ws2 = resolver2.get_workspace(ws["workspace_id"])
    assert ws2["status"] == "confirmed"
    assert ws2["canonical_id"] == canonical

# --- Test 9: Stats ---
def test_resolution_stats(resolver):
    resolver.create_workspaces_from_overlap()
    stats = resolver.get_resolution_stats()
    assert stats["total"] == TOTAL_OVERLAP
    assert stats["pending"] == TOTAL_OVERLAP
    assert stats["confirmed"] == 0
    assert stats["rejected"] == 0

# --- Test 10: Missing workspace raises ---
def test_missing_workspace_raises(resolver):
    with pytest.raises(ValueError, match="not found"):
        resolver.get_workspace("nonexistent-workspace-id")

# --- Test 11: Filter by status after decisions ---
def test_filter_confirmed(resolver):
    resolver.create_workspaces_from_overlap()
    workspaces = resolver.list_workspaces(domain="customer")
    resolver.confirm_match(workspaces[0]["workspace_id"], "canon-1")
    resolver.confirm_match(workspaces[1]["workspace_id"], "canon-2")
    confirmed = resolver.list_workspaces(status="confirmed")
    assert len(confirmed) == 2

# --- Test 12: Undo restores pending count ---
def test_undo_restores_stats(resolver):
    resolver.create_workspaces_from_overlap()
    workspaces = resolver.list_workspaces(domain="vendor")
    resolver.confirm_match(workspaces[0]["workspace_id"], "canon-x")
    assert resolver.get_resolution_stats()["confirmed"] == 1
    resolver.undo_decision(workspaces[0]["workspace_id"])
    assert resolver.get_resolution_stats()["confirmed"] == 0
    assert resolver.get_resolution_stats()["pending"] == TOTAL_OVERLAP
