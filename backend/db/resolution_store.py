"""
ResolutionStore — CRUD for resolution_workspaces table.

Sync psycopg2, parameterized queries, no business logic.
"""

from backend.core.db import get_connection
from backend.utils.log_utils import get_logger
import json

logger = get_logger(__name__)


class ResolutionStore:

    def create_workspace(self, workspace: dict) -> dict:
        """Insert a resolution workspace. Returns the created row."""
        sql = (
            "INSERT INTO resolution_workspaces "
            "(tenant_id, workspace_type, status, candidates, evidence, "
            " decision, decided_by, decided_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
            "RETURNING *"
        )
        params = (
            workspace["tenant_id"],
            workspace["workspace_type"],
            workspace.get("status", "pending"),
            json.dumps(workspace["candidates"]),
            json.dumps(workspace["evidence"]),
            json.dumps(workspace["decision"]) if workspace.get("decision") else None,
            workspace.get("decided_by"),
            workspace.get("decided_at"),
        )
        with get_connection() as conn:
            if conn is None:
                raise RuntimeError(
                    "ResolutionStore.create_workspace failed: database connection unavailable."
                )
            with conn.cursor() as cur:
                cur.execute(sql, params)
                conn.commit()
                columns = [desc[0] for desc in cur.description]
                return dict(zip(columns, cur.fetchone()))

    def get_workspace(self, workspace_id: str) -> dict | None:
        """Get a workspace by ID."""
        sql = "SELECT * FROM resolution_workspaces WHERE id = %s"
        with get_connection() as conn:
            if conn is None:
                raise RuntimeError(
                    "ResolutionStore.get_workspace failed: database connection unavailable."
                )
            with conn.cursor() as cur:
                cur.execute(sql, (workspace_id,))
                row = cur.fetchone()
                if row is None:
                    return None
                columns = [desc[0] for desc in cur.description]
                return dict(zip(columns, row))

    def update_status(self, workspace_id: str, status: str, decided_by: str | None = None, decision: dict | None = None) -> dict | None:
        """Update workspace status and optionally record a decision."""
        sql = (
            "UPDATE resolution_workspaces "
            "SET status = %s, decided_by = %s, decision = %s, "
            "    decided_at = CASE WHEN %s IN ('resolved', 'escalated') THEN now() ELSE decided_at END "
            "WHERE id = %s RETURNING *"
        )
        params = (
            status,
            decided_by,
            json.dumps(decision) if decision else None,
            status,
            workspace_id,
        )
        with get_connection() as conn:
            if conn is None:
                raise RuntimeError(
                    "ResolutionStore.update_status failed: database connection unavailable."
                )
            with conn.cursor() as cur:
                cur.execute(sql, params)
                conn.commit()
                row = cur.fetchone()
                if row is None:
                    return None
                columns = [desc[0] for desc in cur.description]
                return dict(zip(columns, row))

    def list_workspaces(self, tenant_id: str, status: str | None = None, workspace_type: str | None = None) -> list[dict]:
        """List workspaces with optional filters."""
        clauses = ["tenant_id = %s"]
        params: list = [tenant_id]
        if status:
            clauses.append("status = %s")
            params.append(status)
        if workspace_type:
            clauses.append("workspace_type = %s")
            params.append(workspace_type)

        where = " AND ".join(clauses)
        sql = f"SELECT * FROM resolution_workspaces WHERE {where} ORDER BY created_at DESC"

        with get_connection() as conn:
            if conn is None:
                raise RuntimeError(
                    "ResolutionStore.list_workspaces failed: database connection unavailable."
                )
            with conn.cursor() as cur:
                cur.execute(sql, params)
                columns = [desc[0] for desc in cur.description]
                return [dict(zip(columns, row)) for row in cur.fetchall()]

    def delete_workspace(self, workspace_id: str) -> int:
        """Hard-delete a workspace (test cleanup only)."""
        sql = "DELETE FROM resolution_workspaces WHERE id = %s"
        with get_connection() as conn:
            if conn is None:
                raise RuntimeError(
                    "ResolutionStore.delete_workspace failed: database connection unavailable."
                )
            with conn.cursor() as cur:
                cur.execute(sql, (workspace_id,))
                conn.commit()
                return cur.rowcount
