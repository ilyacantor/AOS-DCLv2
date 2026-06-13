#!/usr/bin/env python3
"""CLI minter for DCL MCP tokens (Gate 3C D1 — scoped identity).

Usage:
  python scripts/mcp_mint.py --tenant-id <UUID> [options]

Options:
  --tenant-id TEXT      Required. Tenant UUID to bind the token to.
  --ttl SECONDS         Token TTL in seconds (default: 86400 = 24h).
  --identity TEXT       Declared agent-identity name (e.g. finops-readonly).
                        If --identity is given without explicit scope args,
                        this script looks up the identity in mcp_agent_identities
                        and uses its registered scopes.
  --tools TEXT...       Tool names to allow (repeatable). Empty = all tools.
  --domains TEXT...     Concept-root domains to allow (repeatable). Empty = all.
  --personas TEXT...    Persona keys to allow (repeatable). Empty = all.
  --no-expiry           Use a 10-year TTL (for testing only).
  --verify              Verify and print the decoded payload of an existing token
                        passed via --token.
  --token TEXT          Token string (used with --verify).

Examples:
  # Mint a token for finops-readonly: only query_triples, only cloud_spend domain, only CFO persona
  python scripts/mcp_mint.py --tenant-id <UUID> \\
    --identity finops-readonly \\
    --tools query_triples \\
    --domains cloud_spend \\
    --personas CFO

  # Mint an unrestricted legacy token (no scope = full access)
  python scripts/mcp_mint.py --tenant-id <UUID>

  # Look up a registered identity's scopes and mint accordingly
  python scripts/mcp_mint.py --tenant-id <UUID> --identity finops-readonly

  # Verify an existing token
  python scripts/mcp_mint.py --verify --token <TOKEN_STRING>
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

# Ensure backend/ is importable when run from repo root.
_repo = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_repo))

from dotenv import load_dotenv

# Load live env for DCL_MCP_TOKEN_SECRET (mint_token needs it).
load_dotenv(_repo / ".env")


def _resolve_identity_scopes(tenant_id: str, identity_name: str) -> dict:
    """Look up identity in mcp_agent_identities and return its 3-axis scope."""
    from backend.core.db import get_connection

    sql = (
        "SELECT tool_scope, domain_scope, persona_scope "
        "FROM mcp_agent_identities "
        "WHERE tenant_id = %s AND identity_name = %s"
    )
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (tenant_id, identity_name))
            row = cur.fetchone()
            if row is None:
                print(
                    f"ERROR: identity {identity_name!r} not found in "
                    f"mcp_agent_identities for tenant {tenant_id}.\n"
                    "Register it first, or pass explicit --tools/--domains/--personas.",
                    file=sys.stderr,
                )
                sys.exit(1)
            return {
                "tools": list(row[0] or []),
                "domains": list(row[1] or []),
                "personas": list(row[2] or []),
            }


def _mint(args: argparse.Namespace) -> None:
    from backend.api.mcp_auth import mint_token, TokenError

    tenant_id = args.tenant_id
    ttl = 10 * 365 * 24 * 3600 if args.no_expiry else (args.ttl or 86400)

    tools = list(args.tools or [])
    domains = list(args.domains or [])
    personas = list(args.personas or [])
    identity = args.identity

    # If identity given without explicit scopes, resolve from registry.
    if identity and not (tools or domains or personas):
        scopes = _resolve_identity_scopes(tenant_id, identity)
        tools = scopes["tools"]
        domains = scopes["domains"]
        personas = scopes["personas"]
        print(
            f"Resolved identity {identity!r} scopes from registry:\n"
            f"  tools:   {tools or '(all)'}\n"
            f"  domains: {domains or '(all)'}\n"
            f"  personas:{personas or '(all)'}",
        )

    try:
        result = mint_token(
            tenant_id,
            ttl_seconds=ttl,
            scope=tools or None,
            identity=identity,
            domain_scope=domains or None,
            persona_scope=personas or None,
        )
    except TokenError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    print(json.dumps(result, indent=2))


def _verify(args: argparse.Namespace) -> None:
    from backend.api.mcp_auth import verify_token, TokenError

    if not args.token:
        print("ERROR: --token is required with --verify", file=sys.stderr)
        sys.exit(1)
    try:
        tok = verify_token(args.token)
    except TokenError as e:
        print(f"INVALID: {e}", file=sys.stderr)
        sys.exit(1)

    print("VALID token payload:")
    print(f"  tenant_id:    {tok.tenant_id}")
    print(f"  token_id:     {tok.token_id}")
    print(f"  expires_at:   {tok.expires_at}")
    print(f"  identity:     {tok.identity or '(none — legacy token)'}")
    print(f"  tool_scope:   {list(tok.scope) or '(all tools — empty scope)'}")
    print(f"  domain_scope: {list(tok.domain_scope) or '(all domains — unrestricted)'}")
    print(f"  persona_scope:{list(tok.persona_scope) or '(all personas — unrestricted)'}")


def main() -> None:
    parser = argparse.ArgumentParser(description="DCL MCP token minter (Gate 3C D1)")
    parser.add_argument("--tenant-id", help="Tenant UUID")
    parser.add_argument("--ttl", type=int, default=86400, help="TTL in seconds (default 86400)")
    parser.add_argument("--identity", help="Agent-identity name")
    parser.add_argument("--tools", nargs="*", default=[], help="Allowed tool names")
    parser.add_argument("--domains", nargs="*", default=[], help="Allowed concept-root domains")
    parser.add_argument("--personas", nargs="*", default=[], help="Allowed persona keys")
    parser.add_argument("--no-expiry", action="store_true", help="10-year TTL (testing only)")
    parser.add_argument("--verify", action="store_true", help="Verify --token instead of minting")
    parser.add_argument("--token", help="Token string to verify")

    args = parser.parse_args()

    if args.verify:
        _verify(args)
    else:
        if not args.tenant_id:
            parser.error("--tenant-id is required for minting")
        _mint(args)


if __name__ == "__main__":
    main()
