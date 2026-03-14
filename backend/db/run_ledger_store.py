"""
RunLedgerStore — CRUD for run_ledger table.

Sync psycopg2, parameterized queries, no business logic.
"""

from backend.core.db import get_connection
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)


class RunLedgerStore:

    def create_step(self, step: dict) -> dict:
        """Insert a run ledger step. Returns the created row."""
        sql = (
            "INSERT INTO run_ledger "
            "(tenant_id, engagement_id, step_name, status, idempotency_key, "
            " attempt, inputs_hash, outputs_ref, upstream_deps, "
            " started_at, completed_at, error) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
            "RETURNING *"
        )
        params = (
            step["tenant_id"],
            step["engagement_id"],
            step["step_name"],
            step.get("status", "pending"),
            step["idempotency_key"],
            step.get("attempt", 1),
            step.get("inputs_hash"),
            step.get("outputs_ref"),
            step.get("upstream_deps"),
            step.get("started_at"),
            step.get("completed_at"),
            step.get("error"),
        )
        with get_connection() as conn:
            if conn is None:
                raise RuntimeError(
                    "RunLedgerStore.create_step failed: database connection unavailable."
                )
            with conn.cursor() as cur:
                cur.execute(sql, params)
                conn.commit()
                columns = [desc[0] for desc in cur.description]
                return dict(zip(columns, cur.fetchone()))

    def get_step(self, step_id: str) -> dict | None:
        """Get a step by ID."""
        sql = "SELECT * FROM run_ledger WHERE id = %s"
        with get_connection() as conn:
            if conn is None:
                raise RuntimeError(
                    "RunLedgerStore.get_step failed: database connection unavailable."
                )
            with conn.cursor() as cur:
                cur.execute(sql, (step_id,))
                row = cur.fetchone()
                if row is None:
                    return None
                columns = [desc[0] for desc in cur.description]
                return dict(zip(columns, row))

    def get_by_idempotency_key(self, key: str) -> dict | None:
        """Get step by idempotency key."""
        sql = "SELECT * FROM run_ledger WHERE idempotency_key = %s"
        with get_connection() as conn:
            if conn is None:
                raise RuntimeError(
                    "RunLedgerStore.get_by_idempotency_key failed: database connection unavailable."
                )
            with conn.cursor() as cur:
                cur.execute(sql, (key,))
                row = cur.fetchone()
                if row is None:
                    return None
                columns = [desc[0] for desc in cur.description]
                return dict(zip(columns, row))

    def update_status(self, step_id: str, status: str, error: str | None = None, outputs_ref: str | None = None) -> dict | None:
        """Update step status."""
        sql = (
            "UPDATE run_ledger SET status = %s, error = %s, outputs_ref = COALESCE(%s, outputs_ref), "
            "completed_at = CASE WHEN %s IN ('complete', 'failed') THEN now() ELSE completed_at END "
            "WHERE id = %s RETURNING *"
        )
        with get_connection() as conn:
            if conn is None:
                raise RuntimeError(
                    "RunLedgerStore.update_status failed: database connection unavailable."
                )
            with conn.cursor() as cur:
                cur.execute(sql, (status, error, outputs_ref, status, step_id))
                conn.commit()
                row = cur.fetchone()
                if row is None:
                    return None
                columns = [desc[0] for desc in cur.description]
                return dict(zip(columns, row))

    def list_steps(self, tenant_id: str, engagement_id: str | None = None) -> list[dict]:
        """List steps for a tenant, optionally filtered by engagement."""
        clauses = ["tenant_id = %s"]
        params: list = [tenant_id]
        if engagement_id:
            clauses.append("engagement_id = %s")
            params.append(engagement_id)

        where = " AND ".join(clauses)
        sql = f"SELECT * FROM run_ledger WHERE {where} ORDER BY created_at"

        with get_connection() as conn:
            if conn is None:
                raise RuntimeError(
                    "RunLedgerStore.list_steps failed: database connection unavailable."
                )
            with conn.cursor() as cur:
                cur.execute(sql, params)
                columns = [desc[0] for desc in cur.description]
                return [dict(zip(columns, row)) for row in cur.fetchall()]

    def find_downstream(self, upstream_dep: str) -> list[dict]:
        """Find steps where upstream_deps array contains the given value."""
        sql = "SELECT * FROM run_ledger WHERE %s = ANY(upstream_deps) ORDER BY created_at"
        with get_connection() as conn:
            if conn is None:
                raise RuntimeError(
                    "RunLedgerStore.find_downstream failed: database connection unavailable."
                )
            with conn.cursor() as cur:
                cur.execute(sql, (upstream_dep,))
                columns = [desc[0] for desc in cur.description]
                return [dict(zip(columns, row)) for row in cur.fetchall()]

    def delete_by_engagement(self, engagement_id: str) -> int:
        """Hard-delete all steps for an engagement (test cleanup only)."""
        sql = "DELETE FROM run_ledger WHERE engagement_id = %s"
        with get_connection() as conn:
            if conn is None:
                raise RuntimeError(
                    "RunLedgerStore.delete_by_engagement failed: database connection unavailable."
                )
            with conn.cursor() as cur:
                cur.execute(sql, (engagement_id,))
                conn.commit()
                return cur.rowcount

    def delete_by_idempotency_key(self, key: str) -> int:
        """Hard-delete a step by idempotency key (test cleanup only)."""
        sql = "DELETE FROM run_ledger WHERE idempotency_key = %s"
        with get_connection() as conn:
            if conn is None:
                raise RuntimeError(
                    "RunLedgerStore.delete_by_idempotency_key failed: database connection unavailable."
                )
            with conn.cursor() as cur:
                cur.execute(sql, (key,))
                conn.commit()
                return cur.rowcount
