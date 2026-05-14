"""
mai_mcp_audit writes — one row per MCP tool invocation (Plan B WP5, §11.4).

Outcome values: 'success' | 'error' | 'rate_limited' | 'unauthorized'.
Append-only. Failures to write audit rows are logged but do not abort the
tool call response (the user's request is already complete by that point).
"""

from __future__ import annotations

import hashlib
import json
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterator

from backend.core.db import get_connection
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)


def hash_arguments(arguments: dict[str, Any] | None) -> str:
    """Stable SHA256 hex of the arguments dict for audit lookups."""
    if not arguments:
        return ""
    blob = json.dumps(arguments, separators=(",", ":"), sort_keys=True, default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


@dataclass
class AuditRow:
    tenant_id: str
    tool_name: str
    caller_token_id: str
    arguments_hash: str
    latency_ms: int
    outcome: str
    error_summary: str | None = None
    transport: str | None = None


def write_audit(row: AuditRow) -> None:
    """Insert one audit row. Catches DB errors and logs, never raises."""
    sql = (
        "INSERT INTO mai_mcp_audit "
        "(tenant_id, tool_name, caller_token_id, arguments_hash, "
        " latency_ms, outcome, error_summary, transport) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    )
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (
                        row.tenant_id,
                        row.tool_name,
                        row.caller_token_id,
                        row.arguments_hash or None,
                        int(row.latency_ms),
                        row.outcome,
                        row.error_summary,
                        row.transport,
                    ),
                )
                conn.commit()
    except Exception as exc:
        # Audit write failure is operationally severe but must not block
        # the tool response. Log with full detail per A1's "informative
        # error messages" rule.
        logger.error(
            "mai_mcp_audit INSERT failed — tenant_id=%s tool=%s outcome=%s "
            "err=%s",
            row.tenant_id, row.tool_name, row.outcome, exc,
        )


@contextmanager
def time_call() -> Iterator[dict]:
    """Context manager that captures wall-clock latency in milliseconds."""
    start = time.perf_counter()
    holder: dict[str, int] = {"latency_ms": 0}
    try:
        yield holder
    finally:
        holder["latency_ms"] = int((time.perf_counter() - start) * 1000)
