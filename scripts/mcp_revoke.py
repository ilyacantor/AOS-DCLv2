#!/usr/bin/env python3
"""Operator revoke / narrow surface for live MCP agent identities (Gate 3C D2).

Narrow OR revoke a LIVE agent-identity's scope in mcp_agent_identities. The MCP
boundary reads this registry on every call (TTL-cached, ~5s), so the change
takes effect on the NEXT call within the TTL window — no HMAC-secret rotation,
no waiting for token expiry, no re-mint.

Usage:
  python scripts/mcp_revoke.py --tenant <UUID> --identity <name> --revoke
  python scripts/mcp_revoke.py --tenant <UUID> --identity <name> --restore
  python scripts/mcp_revoke.py --tenant <UUID> --identity <name> \\
      --set-domains cloud_spend revenue
  python scripts/mcp_revoke.py --tenant <UUID> --identity <name> --set-domains
      (no domains = clear the narrowing → identity is unrestricted on the domain
       axis again, bounded by whatever the token still carries)

Exactly one action is required: --revoke | --restore | --set-domains.

Safety: aos-dev ONLY. This is a WRITE to a shared table; it refuses to run when
DATABASE_URL points at a known production project ref (fail-closed, A1). It is a
CLI only — no HTTP/UI surface (that is a later phase).
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

_repo = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_repo))

from dotenv import load_dotenv

# Target aos-dev. load_dotenv does NOT override an already-exported DATABASE_URL,
# so an operator who has explicitly exported one keeps it — and the prod guard
# below still catches it.
load_dotenv(_repo / ".env.development")

# Known production project-ref prefixes — never write to these from this tool.
# These are matched against the EXTRACTED Supabase project ref (not a blind
# substring over the whole URL), so a dev search_path like 'shared_gdbmdr' can
# never be mistaken for the prod project ref 'gdbmdr...'.
_PROD_REFS = ("gdbmdr", "yuxrdo", "jhvxtl")


def _project_ref(database_url: str) -> str | None:
    """Best-effort Supabase project ref from a DATABASE_URL.
      pooler: username 'role.<ref>' @ '<region>.pooler.supabase.com' -> <ref>
      direct: host 'db.<ref>.supabase.co'                            -> <ref>
    Returns None when it cannot be determined (caller fails closed)."""
    from urllib.parse import urlparse

    p = urlparse(database_url)
    user = p.username or ""
    host = p.hostname or ""
    if "pooler.supabase.com" in host and "." in user:
        return user.rsplit(".", 1)[-1]
    if host.startswith("db.") and host.endswith(".supabase.co"):
        return host.split(".")[1]
    return None


def _assert_dev_target() -> str:
    """Return the active DATABASE_URL after asserting it is NOT a prod project.
    Fails loudly (A1) — never silently retargets or proceeds on a prod ref, and
    never proceeds when the ref cannot be determined (fail-closed)."""
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print(
            "ERROR: DATABASE_URL is not set. Load .env.development (aos-dev) "
            "before running this tool.",
            file=sys.stderr,
        )
        sys.exit(1)
    ref = _project_ref(database_url)
    if ref is None:
        print(
            "REFUSED: could not determine the Supabase project ref from "
            "DATABASE_URL — refusing fail-closed. scripts/mcp_revoke.py is "
            "aos-dev ONLY.",
            file=sys.stderr,
        )
        sys.exit(2)
    lowered_ref = ref.lower()
    for prod in _PROD_REFS:
        if lowered_ref.startswith(prod):
            print(
                f"REFUSED: DATABASE_URL project ref {ref!r} is a PRODUCTION "
                f"project ({prod}...). scripts/mcp_revoke.py writes to "
                f"mcp_agent_identities and is aos-dev ONLY. Aborting without any "
                f"write.",
                file=sys.stderr,
            )
            sys.exit(2)
    return database_url


def _print_state(cur, tenant_id: str, identity: str) -> None:
    cur.execute(
        "SELECT identity_name, tool_scope, domain_scope, persona_scope, revoked_at "
        "FROM mcp_agent_identities WHERE tenant_id = %s AND identity_name = %s",
        (tenant_id, identity),
    )
    row = cur.fetchone()
    if row is None:
        print(f"  (no row for identity {identity!r})")
        return
    name, tools, domains, personas, revoked_at = row
    print(f"  identity:      {name}")
    print(f"  tool_scope:    {list(tools or []) or '(all tools)'}")
    print(f"  domain_scope:  {list(domains or []) or '(all domains)'}")
    print(f"  persona_scope: {list(personas or []) or '(all personas)'}")
    print(f"  revoked_at:    {revoked_at if revoked_at is not None else '(active)'}")


def _apply(tenant_id: str, identity: str, set_sql: str, params: tuple) -> None:
    """Run an UPDATE on the one identity row, loudly verifying it existed."""
    from backend.core.db import get_connection

    with get_connection() as conn:
        with conn.cursor() as cur:
            print(f"Before — tenant={tenant_id} identity={identity!r}:")
            _print_state(cur, tenant_id, identity)

            cur.execute(
                f"UPDATE mcp_agent_identities SET {set_sql} "
                f"WHERE tenant_id = %s AND identity_name = %s",
                (*params, tenant_id, identity),
            )
            if cur.rowcount != 1:
                # No silent no-op: an unknown identity is a loud failure, rolled
                # back. The operator must register it (mcp_mint / console) first.
                conn.rollback()
                print(
                    f"\nERROR: expected to update exactly 1 row, updated "
                    f"{cur.rowcount}. identity {identity!r} is not registered for "
                    f"tenant {tenant_id} in mcp_agent_identities — nothing "
                    f"changed (rolled back).",
                    file=sys.stderr,
                )
                sys.exit(3)
            conn.commit()

            print(f"\nAfter:")
            _print_state(cur, tenant_id, identity)

    print(
        "\nDone. The MCP boundary enforces this on the NEXT call within the "
        "registry TTL window (~5s) — no re-mint, no secret rotation."
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Operator revoke / narrow surface for live MCP agent identities (aos-dev only)."
    )
    parser.add_argument("--tenant", required=True, help="Tenant UUID")
    parser.add_argument("--identity", required=True, help="Agent-identity name (mcp_agent_identities.identity_name)")
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--revoke", action="store_true", help="Set revoked_at = now() (deny all access)")
    action.add_argument("--restore", action="store_true", help="Clear revoked_at (re-activate)")
    action.add_argument(
        "--set-domains", nargs="*", metavar="DOMAIN",
        help="Replace domain_scope with these domains (narrowing). No domains = clear narrowing.",
    )

    args = parser.parse_args()
    _assert_dev_target()

    if args.revoke:
        print(f"REVOKING identity {args.identity!r} for tenant {args.tenant} ...\n")
        _apply(args.tenant, args.identity, "revoked_at = now()", ())
    elif args.restore:
        print(f"RESTORING identity {args.identity!r} for tenant {args.tenant} ...\n")
        _apply(args.tenant, args.identity, "revoked_at = NULL", ())
    else:
        # --set-domains (may be an empty list = clear the narrowing).
        domains = list(args.set_domains or [])
        shown = domains or "(cleared — unrestricted on the domain axis)"
        print(
            f"NARROWING identity {args.identity!r} for tenant {args.tenant} "
            f"to domain_scope = {shown} ...\n"
        )
        _apply(args.tenant, args.identity, "domain_scope = %s", (domains,))


if __name__ == "__main__":
    main()
