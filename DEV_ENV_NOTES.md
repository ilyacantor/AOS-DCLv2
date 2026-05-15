# DCL — dev/prod runtime split

Two long-running DCL processes under pm2. **Never** mode-switch one between envs.

| pm2 name      | Port | Env file          | Supabase project        | When to use |
|---------------|------|-------------------|-------------------------|-------------|
| `dcl-backend` | 8004 | `.env`            | `gdbmdrouocxjxiohpixr` (prod DCL) | All prod ops; default operator surface; runtime path for FinOps, Console, Mai. |
| `dcl-dev`     | 8104 | `.env.development` | `glmeqbnuahlkkbolkent` (aos-dev) | Any test that writes data, regenerates seed_manifest, exercises ingest, runs the pytest suite that reads `.env.development`. |

## Why two processes

Three CC sessions wrote test data to prod because operators restarted
`dcl-backend` onto `.env.development` for a test, then forgot to restore.
The fix is structural: a second permanent process on dev, addressed by port.

- **Future test prompts that need dev DB** → hit `http://localhost:8104`.
- **Future prod ops** → hit `http://localhost:8004`.
- Pytest reads `.env.development` (since Plan A WP-fix; see `tests/conftest.py`),
  so the test suite always lands on the same dev DB the `dcl-dev` runtime serves.

## Running

`dcl-dev` is launched by `scripts/run_dcl_dev.sh` under pm2:

```bash
pm2 start scripts/run_dcl_dev.sh --name dcl-dev --cwd ~/code/dcl
```

The script sources `.env` first (for shared secrets like
`DCL_MCP_TOKEN_SECRET`), then `.env.development` (which overrides
`DATABASE_URL` / `SUPABASE_DB_URL` to the dev pooler). Activates the
venv. Execs `uvicorn ... --port 8104 --reload`.

If `scripts/run_dcl_dev.sh` fails to start, the log message names the
missing file. Do not hand-restart `dcl-backend` onto `.env.development`
as a workaround — that's the failure mode this split exists to prevent.

## Sanity check

```bash
for p in $(pgrep -f "uvicorn.*backend.api.main"); do
  port=$(ss -ntlp 2>/dev/null | grep "pid=$p" | grep -oE ":(8004|8104)" | head -1)
  url=$(cat /proc/$p/environ 2>/dev/null | tr '\0' '\n' | grep '^DATABASE_URL=' | head -1)
  echo "$port → $url"
done
```

Expected:
- `:8004 → DATABASE_URL=postgresql://postgres:...@db.gdbmdrouocxjxiohpixr.supabase.co:5432/postgres`
- `:8104 → DATABASE_URL=postgresql://dev_user.glmeqbnuahlkkbolkent:...@aws-1-us-east-1.pooler.supabase.com:5432/postgres?options=-c%20search_path%3Dshared_gdbmdr`

Mismatch = stop and fix; do not run tests.
