#!/usr/bin/env python3
"""Register (upsert) an MCP agent identity into mcp_agent_identities (aos-dev only).

Post-migration 030 the MCP boundary enforces token∩registry on EVERY call for any
token that carries an identity — so an identity must be provisioned here before
its token is honored (an unregistered identity is denied fail-closed). This is the
create/provision step paired with:
  - scripts/mcp_mint.py    — mint a token from a registered identity's scopes
  - scripts/mcp_revoke.py  — narrow / revoke a live identity

Usage:
  python scripts/mcp_register_identity.py --tenant <UUID> --identity finops-rightsizing \\
      --tools query_triples traverse_graph list_domains --domains cloud_spend
  # an EMPTY axis = unrestricted on that axis (mirrors the token/registry semantics)

Safety: aos-dev ONLY — refuses a production project ref (fail-closed), reusing the
exact guard in scripts/mcp_revoke.py. Upsert: re-registering the same
(tenant, identity) replaces its scopes AND clears any prior revoked_at — an
explicit re-register re-activates the identity.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_repo = Path(__file__).resolve().parent.parent
_scripts = Path(__file__).resolve().parent
sys.path.insert(0, str(_repo))
sys.path.insert(0, str(_scripts))

# Reuse the aos-dev guard + state printer from the revoke tool (single source of
# truth for the prod-ref refusal — no duplicated guard to drift).
from mcp_revoke import _assert_dev_target, _print_state  # noqa: E402


def _register(
    tenant_id: str, identity: str,
    tools: list[str], domains: list[str], personas: list[str],
) -> None:
    from backend.core.db import get_connection

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO mcp_agent_identities "
                "(tenant_id, identity_name, tool_scope, domain_scope, persona_scope, revoked_at) "
                "VALUES (%s, %s, %s, %s, %s, NULL) "
                "ON CONFLICT (tenant_id, identity_name) DO UPDATE SET "
                "  tool_scope    = EXCLUDED.tool_scope, "
                "  domain_scope  = EXCLUDED.domain_scope, "
                "  persona_scope = EXCLUDED.persona_scope, "
                "  revoked_at    = NULL",
                (tenant_id, identity, tools, domains, personas),
            )
            if cur.rowcount < 1:
                conn.rollback()
                print(
                    f"\nERROR: upsert affected {cur.rowcount} rows — nothing "
                    f"registered (rolled back).",
                    file=sys.stderr,
                )
                sys.exit(3)
            conn.commit()
            print(f"Registered — tenant={tenant_id} identity={identity!r}:")
            _print_state(cur, tenant_id, identity)

    print(
        "\nDone. The identity is provisioned; its token is honored on the next "
        "MCP call (token∩registry enforced at the boundary)."
    )


def main() -> None:
    p = argparse.ArgumentParser(
        description="Register/upsert an MCP agent identity (aos-dev only)."
    )
    p.add_argument("--tenant", required=True, help="Tenant UUID")
    p.add_argument("--identity", required=True, help="Agent-identity name (stable string key)")
    p.add_argument("--tools", nargs="*", default=[], metavar="TOOL",
                   help="Allowed tool names. Empty = all PUBLIC_TOOLS.")
    p.add_argument("--domains", nargs="*", default=[], metavar="DOMAIN",
                   help="Allowed concept-root domains. Empty = all domains.")
    p.add_argument("--personas", nargs="*", default=[], metavar="PERSONA",
                   help="Allowed persona keys. Empty = all personas.")
    args = p.parse_args()

    _assert_dev_target()
    print(f"REGISTERING identity {args.identity!r} for tenant {args.tenant} ...\n")
    _register(
        args.tenant, args.identity,
        list(args.tools), list(args.domains), list(args.personas),
    )


if __name__ == "__main__":
    main()
