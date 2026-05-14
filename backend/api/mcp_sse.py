"""
HTTP+SSE FastAPI mount for the DCL MCP server (Plan B WP5, §11.4).

Endpoints:
  GET  /api/mcp/sse                  — open SSE stream (Authorization: Bearer)
  POST /api/mcp/messages/            — client → server JSONRPC messages

The MCP SDK's SseServerTransport supplies the protocol-level read/write
streams. We bridge them with the shared MCP `Server` from mcp_server_real,
and bind the tenant_id (from the bearer token) to the session contextvars.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from starlette.responses import Response

from mcp.server.sse import SseServerTransport

from backend.api.mcp_auth import TokenError
from backend.api.mcp_server_real import (
    bind_token_to_session,
    bind_transport,
    build_init_options,
    build_server,
    release_token,
    release_transport,
    verify_bearer,
)
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)


_TRANSPORT_ENDPOINT = "/api/mcp/messages/"

# One shared transport instance per process — sessions are keyed by
# session_id inside the SDK.
_transport = SseServerTransport(_TRANSPORT_ENDPOINT)


router = APIRouter(prefix="/api/mcp", tags=["MCP (real)"])


@router.get("/sse")
async def mcp_sse(request: Request) -> Response:
    """Open an SSE stream. Requires Authorization: Bearer <token>.
    Each session is bound to the token's tenant_id."""
    try:
        token = verify_bearer(request.headers.get("authorization"))
    except TokenError as exc:
        # Per A1: respond with informative error, never 200.
        raise HTTPException(status_code=401, detail=str(exc))

    server = build_server()
    init_options = build_init_options(server)

    async with _transport.connect_sse(
        request.scope, request.receive, request._send
    ) as (read_stream, write_stream):
        reset_token = bind_token_to_session(token)
        reset_transport = bind_transport("http+sse")
        try:
            await server.run(read_stream, write_stream, init_options)
        finally:
            release_token(reset_token)
            release_transport(reset_transport)
    return Response(status_code=200)


@router.post("/messages/")
async def mcp_messages(request: Request) -> Response:
    """Client → server JSONRPC messages, addressed by session_id query
    parameter. The SDK transport handles session lookup; we only forward.
    Auth is enforced at the SSE-open step; the session_id mapping is the
    binding mechanism thereafter."""
    await _transport.handle_post_message(
        request.scope, request.receive, request._send
    )
    # Starlette's Response is what handle_post_message expects to manage,
    # but FastAPI wants us to return something. Use an empty response —
    # handle_post_message has already written the body.
    return Response(status_code=202)
