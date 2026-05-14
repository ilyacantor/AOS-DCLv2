"""
v1 token minting CLI (Plan B WP5, §11.4).

In v2, this responsibility moves to Platform (POST /api/mai/mcp-tokens/issue).
For v1, operators mint tokens locally:

  DCL_MCP_TOKEN_SECRET=<secret> python -m backend.api.mcp_mint <tenant_id> [ttl_seconds]

Prints a JSON line with the token, expiry, and short token_id (the latter
is what appears in mai_mcp_audit.caller_token_id).
"""

from __future__ import annotations

import json
import sys

from backend.api.mcp_auth import TokenError, mint_token


def main() -> int:
    if len(sys.argv) < 2:
        print(
            "usage: python -m backend.api.mcp_mint <tenant_id> [ttl_seconds]",
            file=sys.stderr,
        )
        return 2
    tenant_id = sys.argv[1]
    ttl = int(sys.argv[2]) if len(sys.argv) >= 3 else 86400
    try:
        out = mint_token(tenant_id, ttl_seconds=ttl)
    except TokenError as exc:
        print(f"mint failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
