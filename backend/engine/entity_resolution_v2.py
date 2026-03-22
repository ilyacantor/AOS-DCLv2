"""
EntityResolutionV2 — PG-backed entity resolution derived from triple overlap.

Workspaces are created from concepts that appear under both entity_ids
in semantic_triples (customer.*, vendor.*, employee.* domains).

Decisions (confirm/reject/escalate) are persisted to resolution_workspaces_v2
table and survive restarts.
"""

import uuid as _uuid_mod

from backend.core.db import get_connection
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

_ALLOWED_DOMAINS = ("customer", "vendor", "employee")


import re as _re

_UUID_RE = _re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    _re.IGNORECASE,
)


def _is_valid_uuid(val: str) -> bool:
    """Check if a string is a valid UUID."""
    return isinstance(val, str) and bool(_UUID_RE.match(val))


class EntityResolutionV2:
    """
    PG-backed entity resolution derived from triple overlap.

    Workspaces are created from concepts that appear under both entity_ids
    in semantic_triples (customer.*, vendor.*, employee.* domains).

    Decisions (confirm/reject/escalate) are persisted to resolution_workspaces_v2
    table and survive restarts.
    """

    def __init__(self, tenant_id: str, run_id: str):
        """Store tenant/run context."""
        self.tenant_id = tenant_id
        self.run_id = run_id

    def _get_conn(self):
        """Get a database connection or raise."""
        conn_ctx = get_connection()
        conn = conn_ctx.__enter__()
        return conn, conn_ctx

    def _query(self, sql: str, params: list | tuple) -> list[dict]:
        """Execute a parameterized query and return rows as dicts."""
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                columns = [desc[0] for desc in cur.description]
                return [dict(zip(columns, row)) for row in cur.fetchall()]

    def _execute(self, sql: str, params: list | tuple) -> list[dict]:
        """Execute a write query, commit, and return rows from RETURNING clause."""
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                conn.commit()
                if cur.description:
                    columns = [desc[0] for desc in cur.description]
                    return [dict(zip(columns, row)) for row in cur.fetchall()]
                return []

    def _row_to_workspace(self, row: dict) -> dict:
        """Convert a DB row to the workspace dict format."""
        return {
            "workspace_id": str(row["id"]),
            "tenant_id": str(row["tenant_id"]),
            "run_id": str(row["run_id"]),
            "concept": row["concept"],
            "domain": row["domain"],
            "status": row["status"],
            "canonical_id": row["canonical_id"],
            "decided_by": row["decided_by"],
            "decided_at": str(row["decided_at"]) if row["decided_at"] else None,
            "escalation_reason": row["escalation_reason"],
            "created_at": str(row["created_at"]) if row["created_at"] else None,
            "updated_at": str(row["updated_at"]) if row["updated_at"] else None,
        }

    def create_workspaces_from_overlap(self) -> dict:
        """
        Scan semantic_triples for concepts in customer.*, vendor.*, employee.*
        that appear under both entity_ids. Create one workspace per overlapping
        concept. Returns {"created": int, "by_domain": {"customer": int, ...}}.

        Idempotent: if workspace already exists for a concept, skip it.
        """
        by_domain: dict[str, int] = {}
        total_created = 0

        for domain in _ALLOWED_DOMAINS:
            overlapping = self._find_overlapping_concepts(domain)
            created_in_domain = self._batch_create_workspaces(overlapping, domain)
            by_domain[domain] = created_in_domain
            total_created += created_in_domain

        logger.info(
            f"EntityResolutionV2: created {total_created} workspaces from overlap "
            f"(by_domain={by_domain}) for tenant={self.tenant_id}, run={self.run_id}"
        )

        return {"created": total_created, "by_domain": by_domain}

    def _find_overlapping_concepts(self, domain: str) -> list[str]:
        """Find concepts in a domain that appear under both entity_ids."""
        sql = """
            SELECT concept
            FROM semantic_triples
            WHERE tenant_id = %s AND is_active = true
              AND concept LIKE %s
              AND entity_id != 'combined'
            GROUP BY concept
            HAVING COUNT(DISTINCT entity_id) > 1
            ORDER BY concept
        """
        rows = self._query(sql, [self.tenant_id, f"{domain}.%"])
        return [r["concept"] for r in rows]

    def _batch_create_workspaces(self, concepts: list[str], domain: str) -> int:
        """Batch insert workspaces for a domain. Returns count actually created."""
        if not concepts:
            return 0

        # Build a single INSERT with multiple value tuples
        value_placeholders = []
        params: list = []
        for concept in concepts:
            value_placeholders.append("(%s, %s, %s, %s, 'pending')")
            params.extend([self.tenant_id, self.run_id, concept, domain])

        sql = (
            "INSERT INTO resolution_workspaces_v2 "
            "(tenant_id, run_id, concept, domain, status) VALUES "
            + ", ".join(value_placeholders)
            + " ON CONFLICT (tenant_id, run_id, concept) DO NOTHING"
        )

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                conn.commit()
                return cur.rowcount

    def list_workspaces(self, domain: str | None = None,
                        status: str | None = None) -> list[dict]:
        """
        List resolution workspaces. Optional filters by domain and status.
        Status: "pending" | "confirmed" | "rejected" | "escalated".
        Returns list of workspace dicts with concept, domain, status, decision metadata.
        """
        clauses = ["tenant_id = %s", "run_id = %s"]
        params: list = [self.tenant_id, self.run_id]

        if domain is not None:
            clauses.append("domain = %s")
            params.append(domain)
        if status is not None:
            clauses.append("status = %s")
            params.append(status)

        where = " AND ".join(clauses)
        sql = f"SELECT * FROM resolution_workspaces_v2 WHERE {where} ORDER BY domain, concept"

        rows = self._query(sql, params)
        return [self._row_to_workspace(r) for r in rows]

    def get_workspace(self, workspace_id: str) -> dict:
        """Get a single workspace by ID. Raises ValueError if not found."""
        if not _is_valid_uuid(workspace_id):
            raise ValueError(
                f"Workspace not found: workspace_id='{workspace_id}' "
                f"for tenant_id='{self.tenant_id}', run_id='{self.run_id}'"
            )

        sql = """
            SELECT * FROM resolution_workspaces_v2
            WHERE id = %s AND tenant_id = %s AND run_id = %s
        """
        rows = self._query(sql, [workspace_id, self.tenant_id, self.run_id])
        if not rows:
            raise ValueError(
                f"Workspace not found: workspace_id='{workspace_id}' "
                f"for tenant_id='{self.tenant_id}', run_id='{self.run_id}'"
            )
        return self._row_to_workspace(rows[0])

    def confirm_match(self, workspace_id: str, canonical_id: str,
                      decided_by: str = "system") -> dict:
        """
        Confirm that the overlapping concept is the same real-world entity.
        Sets canonical_id on the semantic_triples for this concept.
        Returns the updated workspace.
        """
        # Get workspace first to find concept
        ws = self.get_workspace(workspace_id)

        # Update workspace status
        sql = """
            UPDATE resolution_workspaces_v2
            SET status = 'confirmed', canonical_id = %s,
                decided_by = %s, decided_at = now(), updated_at = now()
            WHERE id = %s
            RETURNING *
        """
        rows = self._execute(sql, [canonical_id, decided_by, workspace_id])
        if not rows:
            raise ValueError(
                f"Failed to update workspace: workspace_id='{workspace_id}'"
            )

        # Set canonical_id on semantic_triples for this concept
        self._set_canonical_on_triples(ws["concept"], canonical_id)

        return self._row_to_workspace(rows[0])

    def reject_match(self, workspace_id: str,
                     decided_by: str = "system") -> dict:
        """
        Reject the match — concepts are different entities despite name overlap.
        Returns the updated workspace.
        """
        sql = """
            UPDATE resolution_workspaces_v2
            SET status = 'rejected', decided_by = %s,
                decided_at = now(), updated_at = now()
            WHERE id = %s AND tenant_id = %s AND run_id = %s
            RETURNING *
        """
        rows = self._execute(sql, [decided_by, workspace_id, self.tenant_id, self.run_id])
        if not rows:
            raise ValueError(
                f"Workspace not found: workspace_id='{workspace_id}' "
                f"for tenant_id='{self.tenant_id}', run_id='{self.run_id}'"
            )
        return self._row_to_workspace(rows[0])

    def escalate(self, workspace_id: str, reason: str,
                 decided_by: str = "system") -> dict:
        """
        Escalate for human review. Returns the updated workspace.
        """
        sql = """
            UPDATE resolution_workspaces_v2
            SET status = 'escalated', decided_by = %s,
                decided_at = now(), escalation_reason = %s, updated_at = now()
            WHERE id = %s AND tenant_id = %s AND run_id = %s
            RETURNING *
        """
        rows = self._execute(sql, [decided_by, reason, workspace_id, self.tenant_id, self.run_id])
        if not rows:
            raise ValueError(
                f"Workspace not found: workspace_id='{workspace_id}' "
                f"for tenant_id='{self.tenant_id}', run_id='{self.run_id}'"
            )
        return self._row_to_workspace(rows[0])

    def undo_decision(self, workspace_id: str) -> dict:
        """
        Undo a confirm/reject/escalate — reset to pending.
        If canonical_id was set on triples, remove it.
        Returns the updated workspace.
        """
        # Get workspace to check if canonical_id was set
        ws = self.get_workspace(workspace_id)

        if ws["status"] == "confirmed" and ws["canonical_id"]:
            self._clear_canonical_on_triples(ws["concept"])

        sql = """
            UPDATE resolution_workspaces_v2
            SET status = 'pending', canonical_id = NULL, decided_by = NULL,
                decided_at = NULL, escalation_reason = NULL, updated_at = now()
            WHERE id = %s AND tenant_id = %s AND run_id = %s
            RETURNING *
        """
        rows = self._execute(sql, [workspace_id, self.tenant_id, self.run_id])
        if not rows:
            raise ValueError(
                f"Workspace not found: workspace_id='{workspace_id}' "
                f"for tenant_id='{self.tenant_id}', run_id='{self.run_id}'"
            )
        return self._row_to_workspace(rows[0])

    def get_resolution_stats(self) -> dict:
        """
        Returns: {"total": int, "pending": int, "confirmed": int,
                  "rejected": int, "escalated": int, "by_domain": {...}}.
        """
        # Status counts
        sql = """
            SELECT status, COUNT(*) as cnt
            FROM resolution_workspaces_v2
            WHERE tenant_id = %s AND run_id = %s
            GROUP BY status
        """
        status_rows = self._query(sql, [self.tenant_id, self.run_id])
        status_counts = {r["status"]: r["cnt"] for r in status_rows}

        # Domain counts
        sql_domain = """
            SELECT domain, COUNT(*) as cnt
            FROM resolution_workspaces_v2
            WHERE tenant_id = %s AND run_id = %s
            GROUP BY domain
        """
        domain_rows = self._query(sql_domain, [self.tenant_id, self.run_id])
        by_domain = {r["domain"]: r["cnt"] for r in domain_rows}

        total = sum(status_counts.values())

        # Distinct entity_ids from triples (for NLQ EntityRegistry discovery).
        # Not filtered by run_id — entities may span multiple pipeline runs.
        # Excludes synthetic 'combined' aggregate.
        sql_entities = """
            SELECT DISTINCT entity_id
            FROM semantic_triples
            WHERE tenant_id = %s AND is_active = true
              AND entity_id IS NOT NULL
              AND entity_id != 'combined'
            ORDER BY entity_id
        """
        entity_rows = self._query(sql_entities, [self.tenant_id])
        entities = [r["entity_id"] for r in entity_rows]

        return {
            "total": total,
            "pending": status_counts.get("pending", 0),
            "confirmed": status_counts.get("confirmed", 0),
            "rejected": status_counts.get("rejected", 0),
            "escalated": status_counts.get("escalated", 0),
            "by_domain": by_domain,
            "entities": entities,
        }

    def _set_canonical_on_triples(self, concept: str, canonical_id: str) -> int:
        """Set canonical_id on semantic_triples for a given concept.

        semantic_triples.canonical_id is UUID type. If canonical_id is not a
        valid UUID, generate a deterministic UUID v5 from it so the link is
        still traceable.
        """
        if _is_valid_uuid(canonical_id):
            uuid_val = canonical_id
        else:
            uuid_val = str(_uuid_mod.uuid5(_uuid_mod.NAMESPACE_DNS, canonical_id))

        sql = """
            UPDATE semantic_triples
            SET canonical_id = %s, resolution_method = 'manual',
                resolution_confidence = 1.0, updated_at = now()
            WHERE tenant_id = %s AND concept = %s AND is_active = true
        """
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, [uuid_val, self.tenant_id, concept])
                conn.commit()
                return cur.rowcount

    def _clear_canonical_on_triples(self, concept: str) -> int:
        """Remove canonical_id from semantic_triples for a given concept."""
        sql = """
            UPDATE semantic_triples
            SET canonical_id = NULL, resolution_method = NULL,
                resolution_confidence = NULL, updated_at = now()
            WHERE tenant_id = %s AND concept = %s AND is_active = true
        """
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, [self.tenant_id, concept])
                conn.commit()
                return cur.rowcount
