"""
EngagementStore — CRUD for engagement_state table.

Sync psycopg2, parameterized queries, no business logic.
"""

import json
from backend.core.db import get_connection
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)


class EngagementStore:

    def create_engagement(self, engagement: dict) -> dict:
        """Insert an engagement. Returns the created row."""
        sql = (
            "INSERT INTO engagement_state "
            "(tenant_id, engagement_id, entity_a_id, entity_b_id, status, config) "
            "VALUES (%s, %s, %s, %s, %s, %s) "
            "RETURNING *"
        )
        params = (
            engagement["tenant_id"],
            engagement["engagement_id"],
            engagement["entity_a_id"],
            engagement.get("entity_b_id"),
            engagement.get("status", "active"),
            json.dumps(engagement.get("config", {})),
        )
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                conn.commit()
                columns = [desc[0] for desc in cur.description]
                return dict(zip(columns, cur.fetchone()))

    def get_by_engagement_id(self, engagement_id: str) -> dict | None:
        """Get engagement by engagement_id."""
        sql = "SELECT * FROM engagement_state WHERE engagement_id = %s"
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (engagement_id,))
                row = cur.fetchone()
                if row is None:
                    return None
                columns = [desc[0] for desc in cur.description]
                return dict(zip(columns, row))

    def update_status(self, engagement_id: str, status: str) -> dict | None:
        """Update engagement status."""
        sql = (
            "UPDATE engagement_state SET status = %s, updated_at = now() "
            "WHERE engagement_id = %s RETURNING *"
        )
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (status, engagement_id))
                conn.commit()
                row = cur.fetchone()
                if row is None:
                    return None
                columns = [desc[0] for desc in cur.description]
                return dict(zip(columns, row))

    def list_engagements(self, tenant_id: str) -> list[dict]:
        """List all engagements for a tenant."""
        sql = "SELECT * FROM engagement_state WHERE tenant_id = %s ORDER BY created_at DESC"
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (tenant_id,))
                columns = [desc[0] for desc in cur.description]
                return [dict(zip(columns, row)) for row in cur.fetchall()]

    def delete_engagement(self, engagement_id: str) -> int:
        """Hard-delete an engagement (test cleanup only)."""
        sql = "DELETE FROM engagement_state WHERE engagement_id = %s"
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (engagement_id,))
                conn.commit()
                return cur.rowcount
