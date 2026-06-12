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


def summarize_result(result: Any) -> dict[str, Any]:
    """Compact, structured summary of a tool result for the audit ledger
    (migration 020 go-forward enrichment). NEVER the full payload:
      list  -> {"rows": N}
      dict  -> {"keys": [...top-level keys...]} plus "rows"/"total_count"
               when the dict carries an obvious row list / count
      other -> {"type": <python type name>}
    """
    if isinstance(result, list):
        return {"rows": len(result)}
    if isinstance(result, dict):
        summary: dict[str, Any] = {"keys": sorted(str(k) for k in result.keys())}
        if isinstance(result.get("total_count"), int):
            summary["total_count"] = result["total_count"]
        for list_key in ("traces", "conflicts", "edges", "nodes"):
            if isinstance(result.get(list_key), list):
                summary["rows"] = len(result[list_key])
                break
        return summary
    return {"type": type(result).__name__}


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
    # Migration 020 go-forward enrichment (nullable; historical rows stay NULL):
    entity_id: str | None = None          # entity business key from tool args, when present
    arguments: dict[str, Any] | None = None       # full tool-call arguments
    result_summary: dict[str, Any] | None = None  # summarize_result() output, success only


def write_audit(row: AuditRow) -> None:
    """Insert one audit row. Catches DB errors and logs, never raises."""
    sql = (
        "INSERT INTO mai_mcp_audit "
        "(tenant_id, tool_name, caller_token_id, arguments_hash, "
        " latency_ms, outcome, error_summary, transport, "
        " entity_id, arguments, result_summary) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb)"
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
                        row.entity_id,
                        json.dumps(row.arguments, default=str) if row.arguments is not None else None,
                        json.dumps(row.result_summary, default=str) if row.result_summary is not None else None,
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
