"""
stdio entry point for the DCL MCP server (Plan B WP5, §11.4).

Usage:
  DCL_MCP_TOKEN=<token> python -m backend.api.mcp_stdio

The token is read once on startup and bound to the session. Per §11.4, every
stdio session is bound to a single tenant_id (the token's tenant_id).
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys

# CRITICAL: MCP stdio requires that ONLY JSON-RPC be written to stdout.
# Any log output on stdout corrupts the protocol stream. Reroute the root
# logger to stderr BEFORE importing DCL modules (their loggers initialize
# on import in some cases).
_root = logging.getLogger()
for _h in list(_root.handlers):
    _root.removeHandler(_h)
_stderr_handler = logging.StreamHandler(sys.stderr)
_stderr_handler.setFormatter(
    logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s"
    )
)
_root.addHandler(_stderr_handler)
_root.setLevel(logging.WARNING)
# Mark the DCL logger as initialized so setup_logging() does not re-add a
# stdout handler.
import backend.utils.log_utils as _log_utils  # noqa: E402
_log_utils._initialized = True

from mcp.server.stdio import stdio_server  # noqa: E402

from backend.api.mcp_auth import TokenError, verify_token  # noqa: E402
from backend.api.mcp_server_real import (  # noqa: E402
    bind_token_to_session,
    bind_transport,
    build_init_options,
    build_server,
    release_token,
    release_transport,
)


async def _run() -> int:
    token_str = os.environ.get("DCL_MCP_TOKEN")
    if not token_str:
        print(
            "DCL_MCP_TOKEN environment variable is required to start the "
            "stdio MCP server. The token must be issued by Mai's token "
            "minting endpoint (or `python -m backend.api.mcp_mint` in v1).",
            file=sys.stderr,
        )
        return 2
    try:
        token = verify_token(token_str)
    except TokenError as exc:
        print(f"DCL_MCP_TOKEN invalid: {exc}", file=sys.stderr)
        return 2

    reset_token = bind_token_to_session(token)
    reset_transport = bind_transport("stdio")
    try:
        server = build_server()
        init = build_init_options(server)
        async with stdio_server() as (read_stream, write_stream):
            await server.run(read_stream, write_stream, init)
    finally:
        release_token(reset_token)
        release_transport(reset_transport)
    return 0


def main() -> int:
    return asyncio.run(_run())


if __name__ == "__main__":
    raise SystemExit(main())
