#!/usr/bin/env bash
# Launcher for the long-running DCL dev process (dcl-dev under pm2).
#
# Why a second DCL process: prod dcl runs on .env (prod Supabase). Test work
# that needs dev data has accidentally written to prod three times because
# operators kept restarting the prod dcl onto .env.development for a test
# and forgetting to restore. The fix is two processes, never the same.
#
#   dcl       — port 8004 — .env       (prod Supabase gdbmdrouocxjxiohpixr)
#   dcl-dev   — port 8104 — .env.development (aos-dev glmeqbnuahlkkbolkent)
#
# pm2 launches this script for dcl-dev. The script sources the dev env,
# activates the venv, and execs uvicorn so the env-var set survives. The
# DCL_MCP_TOKEN_SECRET is required for the MCP server; it lives in .env so
# we also source that to pull it in (a dev token is fine for dev DCL).
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

if [[ ! -f .env.development ]]; then
  echo "FATAL: $REPO_ROOT/.env.development missing — dcl-dev cannot start." >&2
  exit 1
fi

set -a
# Source prod first only for DCL_MCP_TOKEN_SECRET (single shared dev token);
# then dev overrides DATABASE_URL/SUPABASE_DB_URL so writes land in dev.
[[ -f .env ]] && source .env
source .env.development
set +a

source .venv/bin/activate

# --reload-dir backend: only watch the served code. Default --reload watches the
# whole repo cwd, so (a) WatchFiles walks .venv/node_modules/.git (~777MB) at
# startup, and (b) ANY .py write under tests/, scripts/, demo/, migrations/ —
# including edits from other agent sessions — restarts the worker and drops :8104
# mid-reboot. Scoping to backend/ keeps hot-reload for real backend edits while
# the boot stays ~9s and the service stays up. (ledger: dcl-dev --reload thrash.)
exec python -m uvicorn backend.api.main:app \
    --host 0.0.0.0 \
    --port "${PORT:-8104}" \
    --reload \
    --reload-dir backend
