"""
Real wire-protocol MCP server for DCL (Plan B WP5, §11.4).

Uses the Anthropic `mcp` SDK. Two transports:
  - stdio: launched via `python -m backend.api.mcp_stdio`
  - HTTP+SSE: mounted on FastAPI at /api/mcp/sse and /api/mcp/messages

Tool surface: PUBLIC_TOOLS in `backend/engine/mcp_tools.py` (read-only,
tenant-scoped). The tool bodies live there and are shared with the legacy
HTTP path.

Auth: each session is bound to a tenant_id derived from the caller's
verified token. tenant_id is never an argument. Per-tenant rate limit
applies. Every call is written to mai_mcp_audit.
"""

from __future__ import annotations

import contextvars
import json
from typing import Any

from mcp import types
from mcp.server import NotificationOptions, Server
from mcp.server.models import InitializationOptions

from backend.api.mcp_audit import (
    AuditRow,
    hash_arguments,
    summarize_result,
    time_call,
    write_audit,
)
from backend.api.mcp_auth import TokenError, VerifiedToken, verify_token
from backend.api.mcp_identity_registry import (
    IdentityRegistryError,
    UnknownIdentityError,
    get_effective_identity,
    intersect_scope,
)
from backend.api.mcp_rate_limit import global_limiter
from backend.engine.mcp_tools import (
    PUBLIC_TOOLS,
    TOOL_SCHEMAS,
    MCPToolError,
    dispatch,
)
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)


# Per-session context: token info + transport label. anyio task locals.
_current_token: contextvars.ContextVar[VerifiedToken | None] = contextvars.ContextVar(
    "_current_token", default=None
)
_current_transport: contextvars.ContextVar[str] = contextvars.ContextVar(
    "_current_transport", default="stdio"
)


SERVER_NAME = "dcl-mcp"
SERVER_VERSION = "1.0.0"


def _audit_enrichment(arguments: dict[str, Any] | None) -> dict[str, Any]:
    """Migration 020 go-forward enrichment for every audit row: the full
    tool-call arguments and the entity business key when the call names one."""
    args = arguments if isinstance(arguments, dict) else None
    entity_id = None
    if args is not None:
        raw = args.get("entity_id")
        if isinstance(raw, str) and raw.strip():
            entity_id = raw.strip()
    return {"entity_id": entity_id, "arguments": args}


def _rbac_denial_enrichment(
    arguments: dict[str, Any] | None,
    identity_label: str,
    axis: str,
    denied_value: str,
) -> dict[str, Any]:
    """Gate 3C D1: enrich the audit arguments with RBAC denial context so the
    identity and denied resource appear in the decision_traces VIEW payload.
    The error_summary carries the human reason; payload carries the structured
    context for machine queries on the trace axis."""
    orig = dict(arguments or {})
    entity_id = None
    raw = orig.get("entity_id")
    if isinstance(raw, str) and raw.strip():
        entity_id = raw.strip()
    enriched = {
        **orig,
        "_rbac_denied": {"identity": identity_label, "axis": axis, "denied": denied_value},
    }
    return {"entity_id": entity_id, "arguments": enriched}


def build_server() -> Server:
    """Build a fresh MCP Server instance with the PUBLIC_TOOLS surface wired."""
    server = Server(SERVER_NAME)

    @server.list_tools()
    async def _list_tools() -> list[types.Tool]:
        return [
            types.Tool(
                name=name,
                description=schema["description"],
                inputSchema=schema["inputSchema"],
            )
            for name, schema in TOOL_SCHEMAS.items()
        ]

    @server.call_tool()
    async def _call_tool(name: str, arguments: dict[str, Any]) -> list[types.TextContent]:
        token = _current_token.get()
        transport = _current_transport.get()

        # Guard: tool must be in the public surface.
        if name not in PUBLIC_TOOLS:
            audit = AuditRow(
                tenant_id=(token.tenant_id if token else "00000000-0000-0000-0000-000000000000"),
                tool_name=name,
                caller_token_id=(token.token_id if token else "anonymous"),
                identity=(token.identity if token else None),
                arguments_hash=hash_arguments(arguments),
                latency_ms=0,
                outcome="error",
                error_summary=f"Unknown tool: {name}",
                transport=transport,
                **_audit_enrichment(arguments),
            )
            write_audit(audit)
            raise MCPToolError(f"Unknown tool: {name}")

        # Token must exist.
        if token is None:
            audit = AuditRow(
                tenant_id="00000000-0000-0000-0000-000000000000",
                tool_name=name,
                caller_token_id="anonymous",
                identity=None,
                arguments_hash=hash_arguments(arguments),
                latency_ms=0,
                outcome="unauthorized",
                error_summary="No MCP token bound to this session.",
                transport=transport,
                **_audit_enrichment(arguments),
            )
            write_audit(audit)
            raise MCPToolError(
                "MCP session is not authenticated — no tenant-scoped token."
            )

        # --- Scope checks (Gate 3C D1): tool, domain, persona axes. ---
        # Back-compat: an empty scope axis means UNRESTRICTED on that axis.
        # Only non-empty scope restricts access. Mirrors the existing tool check.

        identity_label = token.identity or token.token_id

        # --- Live registry enforcement (Gate 3C D2): token ∩ live registry. ---
        # Consulted ONLY when the token carries an identity. Legacy/identity-less
        # tokens skip this entirely and behave EXACTLY as before — a minted token
        # keeps its embedded scope (back-compat). When an identity IS present the
        # registry is the live source of truth: an operator can NARROW or REVOKE
        # it and the NEXT call (within the ~5s TTL) enforces it, with no
        # HMAC-secret rotation and no waiting for token expiry. The registry can
        # only NARROW — the token-only checks below remain the floor, so the
        # registry can never widen access beyond what the token already grants.
        effective_domain_for_dispatch = token.domain_scope
        if token.identity:

            def _deny_registry(reason: str, axis: str, denied: str) -> None:
                write_audit(AuditRow(
                    tenant_id=token.tenant_id,
                    tool_name=name,
                    caller_token_id=token.token_id,
                    identity=token.identity,
                    arguments_hash=hash_arguments(arguments),
                    latency_ms=0,
                    outcome="unauthorized",
                    error_summary=reason,
                    transport=transport,
                    **_rbac_denial_enrichment(arguments, identity_label, axis, denied),
                ))
                raise MCPToolError(reason)

            try:
                reg = get_effective_identity(token.tenant_id, token.identity)
            except UnknownIdentityError as exc:
                # Fail-closed: a token naming an identity the registry does not
                # have is denied, never treated as unrestricted.
                _deny_registry(
                    f"identity {token.identity!r} is not present in the live "
                    f"agent-identity registry — denied (fail-closed). {exc}",
                    axis="unknown_identity", denied=token.identity,
                )
            except IdentityRegistryError as exc:
                # System failure reading the registry — fail closed and loud,
                # recorded as outcome='error' (not an authorization denial).
                write_audit(AuditRow(
                    tenant_id=token.tenant_id,
                    tool_name=name,
                    caller_token_id=token.token_id,
                    identity=token.identity,
                    arguments_hash=hash_arguments(arguments),
                    latency_ms=0,
                    outcome="error",
                    error_summary=str(exc),
                    transport=transport,
                    **_audit_enrichment(arguments),
                ))
                raise MCPToolError(str(exc)) from exc

            # Revocation is a hard deny on every axis — checked before scopes.
            if reg.revoked_at is not None:
                _deny_registry(
                    f"identity {identity_label!r} has been REVOKED "
                    f"(revoked_at={reg.revoked_at}) — all MCP access denied. "
                    f"A new token does NOT restore access; an operator must "
                    f"un-revoke the identity in mcp_agent_identities.",
                    axis="revoked", denied=token.identity,
                )

            # Registry tool floor — live narrowing of the tool axis.
            if reg.tool_scope and name not in reg.tool_scope:
                _deny_registry(
                    f"identity {identity_label!r} is not scoped for tool "
                    f"{name!r} by the live registry — denied (current allowed "
                    f"tools: {list(reg.tool_scope)})",
                    axis="tool", denied=name,
                )

            # Registry persona floor — explicit out-of-scope persona arg.
            reg_persona: str | None = (arguments or {}).get("persona")
            if reg.persona_scope and reg_persona and reg_persona not in reg.persona_scope:
                _deny_registry(
                    f"identity {identity_label!r} is not scoped for persona "
                    f"{reg_persona!r} by the live registry — denied (current "
                    f"allowed personas: {list(reg.persona_scope)})",
                    axis="persona", denied=reg_persona,
                )

            # Registry domain floor — explicit domain arg and domain-qualified
            # concept root (mirrors the token-floor checks below).
            if reg.domain_scope:
                reg_domain: str | None = (arguments or {}).get("domain")
                if reg_domain and reg_domain not in reg.domain_scope:
                    _deny_registry(
                        f"identity {identity_label!r} is not scoped for domain "
                        f"{reg_domain!r} by the live registry — denied (current "
                        f"allowed domains: {list(reg.domain_scope)})",
                        axis="domain", denied=reg_domain,
                    )
                reg_concept: str | None = (arguments or {}).get("concept")
                if reg_concept and "." in reg_concept:
                    reg_root = reg_concept.split(".", 1)[0]
                    if reg_root not in reg.domain_scope:
                        _deny_registry(
                            f"identity {identity_label!r} is not scoped for "
                            f"domain {reg_root!r} (from concept {reg_concept!r}) "
                            f"by the live registry — denied (current allowed "
                            f"domains: {list(reg.domain_scope)})",
                            axis="domain", denied=reg_root,
                        )

            # Effective domain scope for BROAD reads = token ∩ registry. A
            # genuinely empty intersection (both sides restrict, no overlap) is a
            # deny-all — NOT unrestricted — so a broad read can never leak the
            # whole tenant when the registry has narrowed the identity to a
            # domain the token never carried.
            eff_domains, deny_all = intersect_scope(
                token.domain_scope, reg.domain_scope
            )
            if deny_all:
                _deny_registry(
                    f"identity {identity_label!r} has an empty effective domain "
                    f"scope: token domains {list(token.domain_scope)} ∩ live "
                    f"registry domains {list(reg.domain_scope)} = none in "
                    f"common. No domain is readable — denied.",
                    axis="domain", denied="(empty token∩registry intersection)",
                )
            effective_domain_for_dispatch = eff_domains

        # Tool axis (pre-existing, extended with identity label in reason).
        if token.scope and name not in token.scope:
            reason = (
                f"identity {identity_label!r} is not scoped for tool {name!r} "
                f"— denied (allowed tools: {list(token.scope)})"
            )
            audit = AuditRow(
                tenant_id=token.tenant_id,
                tool_name=name,
                caller_token_id=token.token_id,
                identity=token.identity,
                arguments_hash=hash_arguments(arguments),
                latency_ms=0,
                outcome="unauthorized",
                error_summary=reason,
                transport=transport,
                **_rbac_denial_enrichment(arguments, identity_label, "tool", name),
            )
            write_audit(audit)
            raise MCPToolError(reason)

        # Domain axis: if token has domain_scope, check explicit domain/concept args.
        if token.domain_scope:
            call_domain: str | None = (arguments or {}).get("domain")
            if call_domain and call_domain not in token.domain_scope:
                reason = (
                    f"identity {identity_label!r} is not scoped for domain "
                    f"{call_domain!r} — denied (allowed domains: {list(token.domain_scope)})"
                )
                audit = AuditRow(
                    tenant_id=token.tenant_id,
                    tool_name=name,
                    caller_token_id=token.token_id,
                    identity=token.identity,
                    arguments_hash=hash_arguments(arguments),
                    latency_ms=0,
                    outcome="unauthorized",
                    error_summary=reason,
                    transport=transport,
                    **_rbac_denial_enrichment(arguments, identity_label, "domain", call_domain),
                )
                write_audit(audit)
                raise MCPToolError(reason)
            # Also deny a domain-qualified concept whose root is out of scope.
            call_concept: str | None = (arguments or {}).get("concept")
            if call_concept and "." in call_concept:
                concept_root = call_concept.split(".", 1)[0]
                if concept_root not in token.domain_scope:
                    reason = (
                        f"identity {identity_label!r} is not scoped for domain "
                        f"{concept_root!r} (from concept {call_concept!r}) "
                        f"— denied (allowed domains: {list(token.domain_scope)})"
                    )
                    audit = AuditRow(
                        tenant_id=token.tenant_id,
                        tool_name=name,
                        caller_token_id=token.token_id,
                        identity=token.identity,
                        arguments_hash=hash_arguments(arguments),
                        latency_ms=0,
                        outcome="unauthorized",
                        error_summary=reason,
                        transport=transport,
                        **_rbac_denial_enrichment(
                            arguments, identity_label, "domain", concept_root
                        ),
                    )
                    write_audit(audit)
                    raise MCPToolError(reason)

        # Persona axis: if token has persona_scope, deny any out-of-scope persona arg.
        if token.persona_scope:
            call_persona: str | None = (arguments or {}).get("persona")
            if call_persona and call_persona not in token.persona_scope:
                reason = (
                    f"identity {identity_label!r} is not scoped for persona "
                    f"{call_persona!r} — denied (allowed personas: {list(token.persona_scope)})"
                )
                audit = AuditRow(
                    tenant_id=token.tenant_id,
                    tool_name=name,
                    caller_token_id=token.token_id,
                    identity=token.identity,
                    arguments_hash=hash_arguments(arguments),
                    latency_ms=0,
                    outcome="unauthorized",
                    error_summary=reason,
                    transport=transport,
                    **_rbac_denial_enrichment(
                        arguments, identity_label, "persona", call_persona
                    ),
                )
                write_audit(audit)
                raise MCPToolError(reason)

        # Rate limit check.
        decision = global_limiter().check(token.tenant_id)
        if not decision.allowed:
            audit = AuditRow(
                tenant_id=token.tenant_id,
                tool_name=name,
                caller_token_id=token.token_id,
                identity=token.identity,
                arguments_hash=hash_arguments(arguments),
                latency_ms=0,
                outcome="rate_limited",
                error_summary=(
                    f"Tenant rpm exceeded; retry after "
                    f"{decision.retry_after_seconds:.1f}s."
                ),
                transport=transport,
                **_audit_enrichment(arguments),
            )
            write_audit(audit)
            raise MCPToolError(
                f"Rate limit exceeded for tenant — retry after "
                f"{decision.retry_after_seconds:.1f}s."
            )

        # Dispatch — pass domain_scope so broad reads filter to in-scope domains.
        with time_call() as t:
            try:
                result = dispatch(
                    token.tenant_id, name, arguments,
                    effective_domain_scope=effective_domain_for_dispatch,
                )
                outcome = "success"
                error_summary: str | None = None
            except MCPToolError as exc:
                outcome = "error"
                error_summary = str(exc)
                audit = AuditRow(
                    tenant_id=token.tenant_id,
                    tool_name=name,
                    caller_token_id=token.token_id,
                    identity=token.identity,
                    arguments_hash=hash_arguments(arguments),
                    latency_ms=t["latency_ms"],
                    outcome=outcome,
                    error_summary=error_summary,
                    transport=transport,
                    **_audit_enrichment(arguments),
                )
                write_audit(audit)
                raise
            except Exception as exc:
                outcome = "error"
                error_summary = f"{type(exc).__name__}: {exc}"
                audit = AuditRow(
                    tenant_id=token.tenant_id,
                    tool_name=name,
                    caller_token_id=token.token_id,
                    identity=token.identity,
                    arguments_hash=hash_arguments(arguments),
                    latency_ms=t["latency_ms"],
                    outcome=outcome,
                    error_summary=error_summary,
                    transport=transport,
                    **_audit_enrichment(arguments),
                )
                write_audit(audit)
                raise MCPToolError(error_summary) from exc

        # Success audit.
        audit = AuditRow(
            tenant_id=token.tenant_id,
            tool_name=name,
            caller_token_id=token.token_id,
            identity=token.identity,
            arguments_hash=hash_arguments(arguments),
            latency_ms=t["latency_ms"],
            outcome=outcome,
            error_summary=None,
            transport=transport,
            result_summary=summarize_result(result),
            **_audit_enrichment(arguments),
        )
        write_audit(audit)

        # MCP expects TextContent[] back — JSON-encode the result.
        body = json.dumps(result, default=str)
        return [types.TextContent(type="text", text=body)]

    return server


def build_init_options(server: Server) -> InitializationOptions:
    """Build InitializationOptions for an already-built Server.

    Capabilities reflect the handlers registered on this server instance.
    """
    return InitializationOptions(
        server_name=SERVER_NAME,
        server_version=SERVER_VERSION,
        capabilities=server.get_capabilities(
            notification_options=NotificationOptions(),
            experimental_capabilities={},
        ),
    )


def bind_token_to_session(token: VerifiedToken) -> contextvars.Token:
    """Bind a verified token to the current async task. Returns the
    contextvars reset token; pass it to release_token when the session
    ends."""
    return _current_token.set(token)


def release_token(reset_token: contextvars.Token) -> None:
    _current_token.reset(reset_token)


def bind_transport(label: str) -> contextvars.Token:
    return _current_transport.set(label)


def release_transport(reset_token: contextvars.Token) -> None:
    _current_transport.reset(reset_token)


def verify_bearer(header_value: str | None) -> VerifiedToken:
    """Parse an Authorization: Bearer <token> header and verify the token."""
    if not header_value:
        raise TokenError("Authorization header is required for HTTP+SSE transport.")
    parts = header_value.split(None, 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise TokenError(
            "Authorization header must be 'Bearer <token>'."
        )
    return verify_token(parts[1].strip())
