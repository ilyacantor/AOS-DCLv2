#!/usr/bin/env python3
"""
demo/finops_arc.py — headless, re-runnable sequencer for the criterion-#12
agent-context arc, executed as REAL platform operations against the LIVE
aos-dev stack (DCL :8104 + finops :5000). It is dev/debug tooling, not a demo
script: every beat is a real op against the running services, each beat carries
a HARD assertion whose EXPECTED value is pulled from the live system at runtime
(B8/B10 — nothing hardcoded), and the process exits 0 iff all six beats pass.

It is idempotent (B14): it asserts only on THIS run's correlation_ids / token id
(the append-only logs may grow), and it always restores the finops-rightsizing
identity (domain_scope=[cloud_spend], un-narrowed) and the finops autonomous-mode
flag to their starting state in a finally block. Two back-to-back runs produce
identical PASS verdicts and identical headline values.

THE SIX BEATS
  1. AUTH       — mint the scoped finops-rightsizing token, open an MCP session,
                  call list_domains; assert :8104 accepts it and the call lands
                  in mai_mcp_audit under identity='finops-rightsizing' (an
                  identity, not a persona).
  2. SHALLOW    — billing-only read (cloud_spend.summary + by_service): a total +
                  a top service, but NO per-team attribution and NO output/efficiency.
  3. TRAVERSE   — scoped cost->team->output read across sources; worst-efficiency
                  team (data_sci_apac, $/deploy) computed from the live rows, both
                  source systems (aws_cost_explorer + deploy_analytics) cited via
                  provenance.
  4. ACT        — drive finops :5000 in BOTH modes (HITL approve + autonomous);
                  assert each wrote a finops_agent_actions row carrying
                  correlation_id + identity + cross-source basis + mode.
  5. BOUNDARY   — join who-asked (mai_mcp_audit.identity) -> what-resolved (the
                  efficiency answer) -> what-action (finops_agent_actions) by
                  identity + correlation_id; assert a READ ties to an ACTION.
  6. REVOKE     — narrow finops-rightsizing OFF cloud_spend at the registry, wait
                  the registry TTL, re-run beat 3's read with the SAME token; assert
                  it is DENIED at query time (no re-mint); restore and re-confirm.

RUN COMMAND (aos-dev; the dev stack must be up — pm2 dcl-dev-backend :8104 +
finops :5000):

    cd /home/ilyac/code/dcl
    .venv/bin/python -m demo.finops_arc --stamp "$(date -u +%Y%m%dT%H%M%SZ)"

Env is resolved exactly like the running dev stack: DATABASE_URL + AOS_TENANT_ID
from .env.development (aos-dev), and the single shared DCL_MCP_TOKEN_SECRET pulled
BY NAME from .env (the prod DB creds in .env are never loaded). --stamp is REQUIRED
and supplied by the caller so the capture filename is deterministic input, not a
datetime() side effect.

aos-dev ONLY. Every target is guarded: DCL must be :8104 (refuses :8004/prod),
finops must be :5000, and DATABASE_URL must resolve to the aos-dev project ref
(glmeqbnu) — a prod ref (gdbmdr/yuxrdo/jhvxtl) refuses fail-closed (A1).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
import traceback
from pathlib import Path
from urllib.parse import urlparse

REPO_ROOT = Path(__file__).resolve().parent.parent

# --- targets: aos-dev ONLY (prod-guarded below) --------------------------------
DCL_URL = "http://localhost:8104"
FINOPS_URL = "http://localhost:5000"
_PROD_DCL_PORT = "8004"
_DEV_DB_REF = "glmeqbnu"          # aos-dev Supabase project ref
_PROD_DB_REFS = ("gdbmdr", "yuxrdo", "jhvxtl")

# --- the cloud_spend estate (live values are fetched at runtime; these are just
#     coordinates) ------------------------------------------------------------
TENANT_ID = "69688df3-fc8e-51f8-a77c-9c13f9b3a784"
ENTITY_ID = "CloudFleet-USE1-7f3a"
IDENTITY = "finops-rightsizing"
DOMAIN = "cloud_spend"
# Tool scope must be a subset of what mcp_agent_identities grants finops-rightsizing.
TOOL_SCOPE = ["query_triples", "list_domains", "provenance", "traverse_graph"]
TOKEN_TTL = 900  # 15 min — comfortably covers one run incl. the registry waits

C_SUMMARY = "cloud_spend.summary"
C_BY_SERVICE = "cloud_spend.by_service"
C_BY_TEAM = "cloud_spend.by_team"
C_OUTPUT = "cloud_spend.output_by_team"
C_EFF = "cloud_spend.efficiency_by_team"

# The server-side identity-registry cache TTL is ~5s; wait past it so a
# narrow/restore is enforced on the next call (no re-mint, no secret rotation).
REGISTRY_TTL_WAIT = 7.0

CAPTURE_DIR = REPO_ROOT / "public" / "demo-captures"


# =============================================================================
# env + prod guards
# =============================================================================
def load_env() -> None:
    """Load the dev env the same way the running stack does: .env.development for
    the aos-dev DB + AOS_TENANT_ID (override=False — a shell export still wins),
    plus DCL_MCP_TOKEN_SECRET pulled BY NAME from .env. The prod DB creds in .env
    are never loaded into this process. Fail loud if either required secret is
    missing (A1)."""
    from dotenv import dotenv_values, load_dotenv

    dev_path = REPO_ROOT / ".env.development"
    if not dev_path.exists():
        raise RuntimeError(
            f"{dev_path} not found — finops_arc runs against the aos-dev stack only."
        )
    load_dotenv(dev_path, override=False)

    if not os.environ.get("DCL_MCP_TOKEN_SECRET"):
        prod_path = REPO_ROOT / ".env"
        vals = dotenv_values(prod_path) if prod_path.exists() else {}
        if vals.get("DCL_MCP_TOKEN_SECRET"):
            os.environ["DCL_MCP_TOKEN_SECRET"] = vals["DCL_MCP_TOKEN_SECRET"]

    if not os.environ.get("DATABASE_URL"):
        raise RuntimeError(
            "DATABASE_URL unresolved — not in the environment and not in "
            ".env.development. finops_arc reads finops_agent_actions / "
            "mai_mcp_audit directly and needs the aos-dev DB. No fallback (A1)."
        )
    if not os.environ.get("DCL_MCP_TOKEN_SECRET"):
        raise RuntimeError(
            "DCL_MCP_TOKEN_SECRET unresolved — not in the environment, not in "
            ".env.development, and not present in .env. The dev DCL backend mints "
            "and validates MCP tokens with this shared dev secret; set it where "
            "the dev stack expects, or export it. No fallback (A1)."
        )


def _project_ref(database_url: str) -> str | None:
    """Best-effort Supabase project ref from a DATABASE_URL (mirrors
    scripts/mcp_revoke._project_ref — the canonical guard the revoke subprocess
    also enforces). Returns None when undeterminable (caller fails closed)."""
    p = urlparse(database_url)
    user = p.username or ""
    host = p.hostname or ""
    if "pooler.supabase.com" in host and "." in user:
        return user.rsplit(".", 1)[-1]
    if host.startswith("db.") and host.endswith(".supabase.co"):
        return host.split(".")[1]
    return None


def assert_dev_only() -> str:
    """Refuse anything but the aos-dev stack. Returns the resolved DB project ref.
    Fail-closed (A1): an undeterminable or prod ref aborts without any op."""
    if _PROD_DCL_PORT in DCL_URL:
        raise RuntimeError(
            f"DCL target {DCL_URL!r} points at the PROD DCL port :{_PROD_DCL_PORT}. "
            f"finops_arc runs against the dev stack (:8104) ONLY."
        )
    if not urlparse(DCL_URL).netloc.endswith(":8104"):
        raise RuntimeError(f"DCL target {DCL_URL!r} is not the dev backend :8104.")
    if not urlparse(FINOPS_URL).netloc.endswith(":5000"):
        raise RuntimeError(f"finops target {FINOPS_URL!r} is not the dev finops :5000.")

    ref = _project_ref(os.environ["DATABASE_URL"])
    if ref is None:
        raise RuntimeError(
            "REFUSED fail-closed: could not determine the Supabase project ref "
            "from DATABASE_URL. finops_arc is aos-dev ONLY."
        )
    low = ref.lower()
    for prod in _PROD_DB_REFS:
        if low.startswith(prod):
            raise RuntimeError(
                f"REFUSED: DATABASE_URL project ref {ref!r} is a PRODUCTION "
                f"project ({prod}...). finops_arc is aos-dev ONLY — aborting "
                f"without any op."
            )
    if not low.startswith(_DEV_DB_REF):
        raise RuntimeError(
            f"REFUSED: DATABASE_URL project ref {ref!r} is not the expected "
            f"aos-dev project ({_DEV_DB_REF}...). Refusing fail-closed (A1)."
        )
    return ref


# =============================================================================
# small clients (HTTP / DB / MCP)
# =============================================================================
def http_json(method: str, url: str, body: dict | None = None, timeout: int = 60):
    """Minimal JSON HTTP. Returns (status, parsed). Does NOT raise on 4xx/5xx —
    the caller inspects the status (so a beat can assert it loudly)."""
    import urllib.error
    import urllib.request

    data = json.dumps(body).encode("utf-8") if body is not None else None
    headers = {"Content-Type": "application/json"} if data is not None else {}
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read().decode("utf-8")
            return r.status, (json.loads(raw) if raw.strip() else None)
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8")
        try:
            parsed = json.loads(raw)
        except Exception:
            parsed = {"_raw": raw}
        return e.code, parsed


def db_query(sql: str, params: tuple = ()):
    """Run one read-only query against the aos-dev DB via DCL's own connection
    pool (search_path resolves to the same schema the services write to)."""
    from backend.core.db import get_connection

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [d[0] for d in cur.description] if cur.description else []
            rows = cur.fetchall() if cur.description else []
        conn.rollback()  # read-only: never hold a txn open
    return [dict(zip(cols, r)) for r in rows]


def mint_token() -> dict:
    """Mint the scoped finops-rightsizing bearer token in-process via the
    platform's own auth library — the same operation as the operator mint flow."""
    from backend.api.mcp_auth import mint_token as _mint

    return _mint(
        TENANT_ID,
        ttl_seconds=TOKEN_TTL,
        scope=TOOL_SCOPE,
        identity=IDENTITY,
        domain_scope=[DOMAIN],
    )


async def _mcp_call(token: str, tool: str, args: dict) -> dict:
    """One real MCP tool call over the wire-protocol HTTP+SSE path with the
    scoped bearer token. Returns {ok, data, error}: a tool denial / error comes
    back as ok=False with the readable error (surfaced loudly, never swallowed)."""
    from mcp import ClientSession
    from mcp.client.sse import sse_client

    sse_url = f"{DCL_URL}/api/mcp/sse"
    headers = {"Authorization": f"Bearer {token}"}
    async with sse_client(sse_url, headers=headers) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            res = await session.call_tool(tool, args)
            text = "\n".join(getattr(c, "text", "") or "" for c in res.content)
            if res.isError:
                return {"ok": False, "data": None, "error": text}
            try:
                data = json.loads(text)
            except Exception:
                data = text
            return {"ok": True, "data": data, "error": None}


def mcp_call(token: str, tool: str, args: dict) -> dict:
    return asyncio.run(_mcp_call(token, tool, args))


def run_revoke_cli(*cli_args: str) -> subprocess.CompletedProcess:
    """Invoke scripts/mcp_revoke.py (the operator narrow/restore surface) as a
    real subprocess. It loads .env.development and re-enforces the aos-dev guard
    itself (single source of truth for the prod-ref refusal)."""
    cmd = [
        sys.executable,
        str(REPO_ROOT / "scripts" / "mcp_revoke.py"),
        "--tenant", TENANT_ID,
        "--identity", IDENTITY,
        *cli_args,
    ]
    return subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True)


def _num(v) -> float | None:
    try:
        f = float(str(v))
        return f
    except (TypeError, ValueError):
        return None


# =============================================================================
# beat result accumulator
# =============================================================================
class Beat:
    def __init__(self, num: int, name: str):
        self.num = num
        self.name = name
        self.checks: list[dict] = []
        self.values: dict = {}
        self.error: str | None = None

    def add(self, label: str, ok: bool, expected, got) -> bool:
        self.checks.append(
            {"label": label, "pass": bool(ok), "expected": expected, "got": got}
        )
        return bool(ok)

    def fail_exc(self, exc: Exception) -> None:
        self.error = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        self.add(
            "beat executed without raising (real op succeeded)",
            False,
            "no exception",
            f"{type(exc).__name__}: {exc}",
        )

    @property
    def passed(self) -> bool:
        return len(self.checks) > 0 and all(c["pass"] for c in self.checks)

    def to_dict(self) -> dict:
        return {
            "beat": self.num,
            "name": self.name,
            "status": "PASS" if self.passed else "FAIL",
            "checks": self.checks,
            "values": self.values,
            "error": self.error,
        }


# =============================================================================
# ground truth (independent of the path under test — B8/B10)
# =============================================================================
def fetch_ground_truth() -> dict:
    """Pull the cloud_spend estate from DCL's read-only triples/browse endpoint
    (the source-system ground truth) and reduce it to the expected values the
    beats assert against. Never hardcoded."""
    url = (
        f"{DCL_URL}/api/dcl/triples/browse?tenant_id={TENANT_ID}"
        f"&entity_id={ENTITY_ID}&domain={DOMAIN}&limit=500"
    )
    status, body = http_json("GET", url)
    if status != 200 or not isinstance(body, dict) or "triples" not in body:
        raise RuntimeError(
            f"ground-truth fetch failed: GET {url} -> HTTP {status} body={str(body)[:200]}"
        )
    rows = body["triples"]
    by_concept: dict[str, list[dict]] = {}
    for r in rows:
        by_concept.setdefault(str(r.get("concept")), []).append(r)

    def fam(concept: str) -> dict[str, dict]:
        out = {}
        for r in by_concept.get(concept, []):
            out[str(r.get("property"))] = {
                "value": _num(r.get("value")),
                "source": r.get("source_system"),
                "triple_id": r.get("triple_id") or r.get("id"),
                "unit": r.get("unit"),
            }
        return out

    summary = fam(C_SUMMARY)
    by_service = fam(C_BY_SERVICE)
    cost = fam(C_BY_TEAM)
    output = fam(C_OUTPUT)
    eff = fam(C_EFF)

    top_service = None
    if by_service:
        top_service = max(by_service.items(), key=lambda kv: kv[1]["value"] or 0)

    worst_team = None
    if eff:
        worst_team = max(eff.items(), key=lambda kv: kv[1]["value"] or float("-inf"))

    return {
        "total_cost": summary.get("total_cost", {}).get("value"),
        "top_service_name": top_service[0] if top_service else None,
        "top_service_cost": top_service[1]["value"] if top_service else None,
        "teams": sorted(set(cost) | set(output)),
        "cost": cost,
        "output": output,
        "eff": eff,
        "worst_team": worst_team[0] if worst_team else None,
        "worst_usd_per_deploy": worst_team[1]["value"] if worst_team else None,
        "expected_sources": {"aws_cost_explorer", "deploy_analytics"},
    }


# =============================================================================
# beats
# =============================================================================
def beat1_auth(token: str, minted: dict, run_started, state: dict) -> Beat:
    b = Beat(1, "AUTH (scoped identity, logged)")
    try:
        # Real op: open an MCP session under the scoped token and call list_domains.
        res = mcp_call(token, "list_domains", {"entity_id": ENTITY_ID})
        domains = [d.get("domain") for d in res["data"]] if res["ok"] and isinstance(res["data"], list) else []
        b.values["list_domains_result"] = res["data"] if res["ok"] else res["error"]
        b.add(":8104 ACCEPTS the scoped token's list_domains call", res["ok"], "accepted (no error)",
              "accepted" if res["ok"] else f"DENIED: {res['error']}")
        b.add("domain in result is the in-scope cloud_spend", DOMAIN in domains,
              f"{DOMAIN} present", domains)

        # Externality proof via the public REST audit ledger (by caller_token_id).
        tok_id = minted["token_id"]
        status, audit = http_json(
            "GET",
            f"{DCL_URL}/api/dcl/mcp/audit?tenant_id={TENANT_ID}"
            f"&caller_token_id={tok_id}&tool_name=list_domains&limit=20",
        )
        entries = (audit or {}).get("entries", []) if status == 200 else []
        rest_success = any(e.get("outcome") == "success" for e in entries)
        b.add("call is visible in the REST audit ledger (success)", rest_success,
              ">=1 list_domains row, outcome=success", f"{len(entries)} entries, status={status}")

        # Identity read-back: mai_mcp_audit carries the agent-identity NAME (the
        # REST projection does not), so read it directly and assert it.
        rows = db_query(
            "SELECT identity, outcome FROM mai_mcp_audit "
            "WHERE tenant_id=%s AND caller_token_id=%s AND tool_name='list_domains' "
            "AND created_at >= %s ORDER BY created_at DESC LIMIT 1",
            (TENANT_ID, tok_id, run_started),
        )
        audit_identity = rows[0]["identity"] if rows else None
        audit_outcome = rows[0]["outcome"] if rows else None
        b.values["audit_identity"] = audit_identity
        b.add("audit row identity == finops-rightsizing", audit_identity == IDENTITY,
              IDENTITY, audit_identity)
        b.add("audit row outcome == success", audit_outcome == "success", "success", audit_outcome)

        # It is an IDENTITY, not a persona: the token carries an identity name +
        # empty persona_scope, and a live registry row backs it.
        persona_scope = minted.get("persona_scope") or []
        reg = db_query(
            "SELECT domain_scope, persona_scope, revoked_at FROM mcp_agent_identities "
            "WHERE tenant_id=%s AND identity_name=%s",
            (TENANT_ID, IDENTITY),
        )
        registered = bool(reg) and reg[0]["revoked_at"] is None
        b.add("scoped by IDENTITY, not persona (empty persona_scope)", persona_scope == [],
              "persona_scope == []", persona_scope)
        b.add("identity is provisioned + active in mcp_agent_identities", registered,
              "1 active registry row", reg[0] if reg else None)
        state["caller_token_id"] = tok_id
    except Exception as e:
        b.fail_exc(e)
    return b


def beat2_shallow(token: str, gt: dict, state: dict) -> Beat:
    b = Beat(2, "SINGLE-SYSTEM SHALLOW (the before)")
    try:
        # Real op: read ONLY the billing concepts a cost/billing system exposes.
        summ = mcp_call(token, "query_triples", {"concept": C_SUMMARY, "entity_id": ENTITY_ID})
        svc = mcp_call(token, "query_triples", {"concept": C_BY_SERVICE, "entity_id": ENTITY_ID})
        if not summ["ok"] or not svc["ok"]:
            raise RuntimeError(f"billing read failed: summary={summ['error']} by_service={svc['error']}")

        summ_rows = summ["data"]
        svc_rows = svc["data"]
        billing_rows = summ_rows + svc_rows

        # total + top service present (a billing system yields these).
        total = next((_num(r.get("value")) for r in summ_rows if r.get("property") == "total_cost"), None)
        top = max(svc_rows, key=lambda r: _num(r.get("value")) or 0) if svc_rows else None
        top_name = top.get("property") if top else None
        b.values["total_cost"] = total
        b.values["top_service"] = {"name": top_name, "cost": _num(top.get("value")) if top else None}
        b.add("billing total_cost matches live ground truth", total is not None and gt["total_cost"] is not None
              and round(total, 2) == round(gt["total_cost"], 2), gt["total_cost"], total)
        b.add("billing top service matches live ground truth", top_name == gt["top_service_name"],
              gt["top_service_name"], top_name)

        # NO per-team attribution: none of the billing properties is a team key.
        team_keys = set(gt["teams"])
        billing_props = {str(r.get("property")) for r in billing_rows}
        team_leak = sorted(billing_props & team_keys)
        b.add("NO per-team attribution in the billing view", team_leak == [],
              "no team property in {summary, by_service}", team_leak)

        # NO output / efficiency: the billing read carries zero output/efficiency rows.
        out_eff = [r for r in billing_rows if str(r.get("concept")) in (C_OUTPUT, C_EFF)]
        b.add("NO output / efficiency in the billing view", out_eff == [],
              "0 output_by_team/efficiency_by_team rows", len(out_eff))

        # A billing system alone: every billing row is from a single cost system.
        srcs = sorted({str(r.get("source_system")) for r in billing_rows})
        b.add("billing view is a single source system (cannot relate to output)",
              srcs == ["aws_cost_explorer"], ["aws_cost_explorer"], srcs)
    except Exception as e:
        b.fail_exc(e)
    return b


def beat3_traverse(token: str, gt: dict, state: dict) -> Beat:
    b = Beat(3, "TRAVERSE cost->team->output (the cross-source answer)")
    try:
        # Real op: one scoped root-domain read; split the three cross-source
        # families client-side (the cloud_spend estate has no graph edges, so the
        # traversal is the client-side cost<->output join — same as finops does).
        res = mcp_call(token, "query_triples", {"domain": DOMAIN, "entity_id": ENTITY_ID, "limit": 1000})
        if not res["ok"]:
            raise RuntimeError(f"scoped cloud_spend read DENIED/failed: {res['error']}")
        rows = res["data"]

        def fam(concept):
            return {str(r.get("property")): r for r in rows if str(r.get("concept")) == concept}

        cost, output, eff = fam(C_BY_TEAM), fam(C_OUTPUT), fam(C_EFF)
        b.add("all three families present (cost, output, efficiency)",
              len(cost) > 0 and len(output) > 0 and len(eff) > 0,
              "by_team>0 AND output_by_team>0 AND efficiency_by_team>0",
              {"by_team": len(cost), "output": len(output), "eff": len(eff)})

        # Compute the worst-efficiency team from the LIVE rows.
        ranked = []
        for team in set(cost) | set(output):
            e = _num(eff[team].get("value")) if team in eff else None
            c = _num(cost[team].get("value")) if team in cost else None
            o = _num(output[team].get("value")) if team in output else None
            score = e if e is not None else (round(c / o, 2) if c and o else None)
            if score is not None:
                ranked.append((team, score, c, o))
        ranked.sort(key=lambda t: t[1], reverse=True)
        worst = ranked[0]
        worst_team, worst_eff = worst[0], worst[1]
        b.values["worst_team"] = worst_team
        b.values["worst_usd_per_deploy"] = worst_eff
        b.values["worst_cost"] = worst[2]
        b.values["worst_output"] = worst[3]

        b.add("worst-efficiency team matches live ground truth", worst_team == gt["worst_team"],
              gt["worst_team"], worst_team)
        b.add("worst $/deploy matches live ground truth",
              gt["worst_usd_per_deploy"] is not None and round(worst_eff, 2) == round(gt["worst_usd_per_deploy"], 2),
              gt["worst_usd_per_deploy"], worst_eff)

        # cost<->output related across the two source systems for the worst team.
        worst_cost_row = cost.get(worst_team)
        worst_out_row = output.get(worst_team)
        related = bool(worst_cost_row) and bool(worst_out_row)
        b.add("cost<->output related for the worst team (the cross-source join)", related,
              "worst team has BOTH a cost fact and an output fact",
              {"has_cost": bool(worst_cost_row), "has_output": bool(worst_out_row)})

        # Cite BOTH source systems via provenance on the actual triples.
        cost_tid = (worst_cost_row or {}).get("triple_id") or (worst_cost_row or {}).get("id")
        out_tid = (worst_out_row or {}).get("triple_id") or (worst_out_row or {}).get("id")
        cost_prov = mcp_call(token, "provenance", {"triple_id": cost_tid}) if cost_tid else {"ok": False, "error": "no cost triple", "data": None}
        out_prov = mcp_call(token, "provenance", {"triple_id": out_tid}) if out_tid else {"ok": False, "error": "no output triple", "data": None}
        cost_src = (cost_prov["data"] or {}).get("source_system") if cost_prov["ok"] else None
        out_src = (out_prov["data"] or {}).get("source_system") if out_prov["ok"] else None
        b.values["provenance"] = {"cost_source": cost_src, "output_source": out_src}
        b.add("cost provenance cites aws_cost_explorer", cost_src == "aws_cost_explorer",
              "aws_cost_explorer", cost_src)
        b.add("output provenance cites deploy_analytics", out_src == "deploy_analytics",
              "deploy_analytics", out_src)
        b.add("the answer draws on BOTH source systems", {cost_src, out_src} == gt["expected_sources"],
              sorted(gt["expected_sources"]), sorted(s for s in {cost_src, out_src} if s))

        state["worst_team"] = worst_team
        state["worst_usd_per_deploy"] = worst_eff
        # Row count an in-scope read returns — the "before" for beat 6.
        state["scoped_read_rows"] = len(cost) + len(output) + len(eff)
    except Exception as e:
        b.fail_exc(e)
    return b


def beat4_act(initial_autonomous: bool, state: dict) -> Beat:
    b = Beat(4, "ACT — the hero, BOTH modes (HITL + autonomous)")
    try:
        # --- HITL: decide -> execute with an approver ---
        status_h, exec_h = http_json(
            "POST", f"{FINOPS_URL}/api/agent/rightsizing/execute",
            {"mode": "hitl", "approver": "arc-operator"},
        )
        corr_hitl = (exec_h or {}).get("correlationId")
        b.add("HITL execute committed (HTTP 200)", status_h == 200 and (exec_h or {}).get("status") == "committed",
              "200 / status=committed", f"{status_h} / {(exec_h or {}).get('status')} ({(exec_h or {}).get('reason')})")

        # --- AUTONOMOUS: flip mode ON, then execute autonomously ---
        status_on, cfg_on = http_json(
            "POST", f"{FINOPS_URL}/api/agent-config/autonomous-mode",
            {"enabled": True, "updatedBy": "finops_arc"},
        )
        b.add("autonomous mode flipped ON", status_on == 200 and (cfg_on or {}).get("autonomousMode") is True,
              "200 / autonomousMode=true", f"{status_on} / {(cfg_on or {}).get('autonomousMode')}")
        status_a, exec_a = http_json(
            "POST", f"{FINOPS_URL}/api/agent/rightsizing/execute", {"mode": "autonomous"},
        )
        corr_auto = (exec_a or {}).get("correlationId")
        b.add("autonomous execute committed (HTTP 200)", status_a == 200 and (exec_a or {}).get("status") == "committed",
              "200 / status=committed", f"{status_a} / {(exec_a or {}).get('status')} ({(exec_a or {}).get('reason')})")

        # Restore the flag immediately (cleanup() re-asserts this as a safety net).
        http_json("POST", f"{FINOPS_URL}/api/agent-config/autonomous-mode",
                  {"enabled": bool(initial_autonomous), "updatedBy": "finops_arc"})

        b.values["correlation_ids"] = {"hitl": corr_hitl, "autonomous": corr_auto}
        state["correlation_ids"] = {"hitl": corr_hitl, "autonomous": corr_auto}

        # Read the REAL rows THIS run wrote (assert on this run's ids only — the
        # log is append-only, so this stays deterministic across re-runs).
        for mode, corr in (("hitl", corr_hitl), ("autonomous", corr_auto)):
            if not corr:
                b.add(f"{mode} produced a correlation_id", False, "a correlation id", None)
                continue
            rows = db_query(
                "SELECT identity, tenant_id, entity_id, action_type, target, basis, "
                "mode, approver, outcome FROM finops_agent_actions "
                "WHERE tenant_id=%s AND correlation_id=%s",
                (TENANT_ID, corr),
            )
            b.add(f"{mode}: exactly one finops_agent_actions row for this correlation_id",
                  len(rows) == 1, "1 row", len(rows))
            if len(rows) != 1:
                continue
            row = rows[0]
            basis = row["basis"] if isinstance(row["basis"], dict) else json.loads(row["basis"] or "{}")
            cost_src = (basis.get("cost") or {}).get("source")
            out_src = (basis.get("output") or {}).get("source")
            b.add(f"{mode}: row identity == finops-rightsizing", row["identity"] == IDENTITY, IDENTITY, row["identity"])
            b.add(f"{mode}: row mode == {mode}", row["mode"] == mode, mode, row["mode"])
            b.add(f"{mode}: outcome == success", row["outcome"] == "success", "success", row["outcome"])
            b.add(f"{mode}: target == worst-efficiency team", row["target"] == state.get("worst_team"),
                  state.get("worst_team"), row["target"])
            b.add(f"{mode}: basis carries the cross-source reads (both systems)",
                  cost_src == "aws_cost_explorer" and out_src == "deploy_analytics",
                  {"cost": "aws_cost_explorer", "output": "deploy_analytics"},
                  {"cost": cost_src, "output": out_src})
            b.values[f"{mode}_row"] = {
                "target": row["target"], "mode": row["mode"], "approver": row["approver"],
                "outcome": row["outcome"], "basis_sources": basis.get("sources"),
            }
    except Exception as e:
        b.fail_exc(e)
    return b


def beat5_boundary(run_started, state: dict) -> Beat:
    b = Beat(5, "GOVERNANCE BOUNDARY RECORD (read -> action, joined)")
    try:
        corr = state.get("correlation_ids") or {}
        corr_ids = [c for c in (corr.get("hitl"), corr.get("autonomous")) if c]
        if len(corr_ids) != 2:
            raise RuntimeError(f"beat 5 needs beat 4's two correlation_ids; got {corr_ids}")

        # The literal join: who-asked (mai_mcp_audit reads under the identity) tied
        # to what-action (finops_agent_actions rows) by identity + correlation_id.
        joined = db_query(
            "SELECT a.identity AS identity, "
            "       COUNT(DISTINCT a.audit_id) AS reads, "
            "       COUNT(DISTINCT f.correlation_id) AS actions "
            "FROM mai_mcp_audit a "
            "JOIN finops_agent_actions f "
            # mai_mcp_audit.tenant_id is uuid, finops_agent_actions.tenant_id is
            # varchar (separate services own each table) — cast to text to join.
            "  ON a.identity = f.identity AND a.tenant_id::text = f.tenant_id "
            "WHERE a.identity=%s AND a.tenant_id=%s AND a.created_at >= %s "
            "  AND a.outcome='success' AND f.correlation_id = ANY(%s) "
            "GROUP BY a.identity",
            (IDENTITY, TENANT_ID, run_started, corr_ids),
        )
        b.add("the read<->action join yields exactly one identity group", len(joined) == 1,
              "1 group keyed by identity", len(joined))
        if joined:
            g = joined[0]
            b.add("who-asked identity == finops-rightsizing", g["identity"] == IDENTITY, IDENTITY, g["identity"])
            b.add("at least one READ ties to the ACTIONS through the identity join", g["reads"] >= 1,
                  ">=1 successful MCP read under the identity", g["reads"])
            b.add("both this-run ACTIONS are joined (correlation_id)", g["actions"] == 2,
                  "2 actions", g["actions"])
            b.values["boundary_record"] = {
                "who_asked": g["identity"],
                "reads_under_identity": g["reads"],
                "what_resolved": {
                    "worst_efficiency_team": state.get("worst_team"),
                    "usd_per_deploy": state.get("worst_usd_per_deploy"),
                },
                "what_action_correlation_ids": corr_ids,
                "joined_by": ["identity", "correlation_id"],
            }
    except Exception as e:
        b.fail_exc(e)
    return b


def beat6_revoke(token: str, state: dict) -> Beat:
    b = Beat(6, "REVOKE + RE-RUN (live, query-time enforcement)")
    try:
        # BEFORE: the same token reads cloud_spend successfully.
        before = mcp_call(token, "query_triples", {"domain": DOMAIN, "entity_id": ENTITY_ID, "limit": 1000})
        before_rows = len(before["data"]) if before["ok"] and isinstance(before["data"], list) else 0
        b.add("BEFORE narrow: scoped read is allowed", before["ok"] and before_rows > 0,
              "allowed, rows>0", f"ok={before['ok']} rows={before_rows}")

        # NARROW the live identity OFF cloud_spend (to a disjoint domain) at the
        # registry — a query-time write, no token change.
        narrowed = run_revoke_cli("--set-domains", "revenue")
        b.add("registry narrow applied (mcp_revoke.py exit 0)", narrowed.returncode == 0,
              "exit 0", f"exit {narrowed.returncode}: {narrowed.stderr.strip()[:120]}")

        # Wait past the server-side registry cache TTL, then re-run beat 3's read
        # with the SAME token. No re-mint.
        import time as _t
        _t.sleep(REGISTRY_TTL_WAIT)
        after = mcp_call(token, "query_triples", {"domain": DOMAIN, "entity_id": ENTITY_ID, "limit": 1000})
        after_rows = len(after["data"]) if after["ok"] and isinstance(after["data"], list) else 0
        denied = (not after["ok"]) or after_rows == 0
        b.add("AFTER narrow: the SAME token is DENIED at query time (returns less)",
              denied and after_rows < before_rows, "denied / fewer rows than before",
              f"ok={after['ok']} rows={after_rows} err={(after['error'] or '')[:80]}")

        # RESTORE: re-widen to cloud_spend; wait the TTL; confirm access is live again.
        restored = run_revoke_cli("--set-domains", DOMAIN)
        b.add("registry restore applied (mcp_revoke.py exit 0)", restored.returncode == 0,
              "exit 0", f"exit {restored.returncode}: {restored.stderr.strip()[:120]}")
        _t.sleep(REGISTRY_TTL_WAIT)
        confirm = mcp_call(token, "query_triples", {"domain": DOMAIN, "entity_id": ENTITY_ID, "limit": 1000})
        confirm_rows = len(confirm["data"]) if confirm["ok"] and isinstance(confirm["data"], list) else 0
        b.add("RESTORE: access is live again with the SAME token", confirm["ok"] and confirm_rows == before_rows,
              f"allowed, rows=={before_rows}", f"ok={confirm['ok']} rows={confirm_rows}")

        # Registry row is back to the baseline (idempotent for the next run).
        reg = db_query(
            "SELECT domain_scope, revoked_at FROM mcp_agent_identities "
            "WHERE tenant_id=%s AND identity_name=%s", (TENANT_ID, IDENTITY),
        )
        reg_domains = list(reg[0]["domain_scope"]) if reg else None
        b.add("registry restored to baseline domain_scope=[cloud_spend]",
              reg_domains == [DOMAIN] and reg and reg[0]["revoked_at"] is None,
              [DOMAIN], reg_domains)

        b.values["revoke"] = {
            "before_domains": [DOMAIN],
            "before_rows": before_rows,
            "after_narrow": "denied" if denied else f"{after_rows} rows",
            "after_rows": after_rows,
            "restored_domains": reg_domains,
            "confirm_rows": confirm_rows,
        }
    except Exception as e:
        b.fail_exc(e)
    return b


# =============================================================================
# cleanup (always restores baseline — idempotency / B14)
# =============================================================================
def cleanup(initial_autonomous: bool | None) -> list[str]:
    notes = []
    try:
        r = run_revoke_cli("--set-domains", DOMAIN)
        notes.append(f"registry->[cloud_spend] exit={r.returncode}")
    except Exception as e:
        notes.append(f"registry restore error: {e}")
    if initial_autonomous is not None:
        try:
            s, _ = http_json("POST", f"{FINOPS_URL}/api/agent-config/autonomous-mode",
                             {"enabled": bool(initial_autonomous), "updatedBy": "finops_arc"})
            notes.append(f"autonomous->{initial_autonomous} http={s}")
        except Exception as e:
            notes.append(f"autonomous restore error: {e}")
    return notes


# =============================================================================
# reporting
# =============================================================================
def print_report(beats: list[Beat], db_ref: str) -> None:
    print("=" * 78)
    print("finops_arc :: criterion-12 agent-context arc (REAL ops, aos-dev)")
    print(f"target: DCL={DCL_URL}  finops={FINOPS_URL}  db={db_ref}...  "
          f"tenant={TENANT_ID[:8]}...  entity={ENTITY_ID}")
    print("=" * 78)
    for b in beats:
        verdict = "PASS" if b.passed else "FAIL"
        print(f"[{verdict}] Beat {b.num}: {b.name}")
        for c in b.checks:
            mark = "ok" if c["pass"] else "XX"
            line = f"    [{mark}] {c['label']}"
            if not c["pass"]:
                line += f"  | expected={c['expected']!r} got={c['got']!r}"
            print(line)
        if b.error:
            first = b.error.strip().splitlines()[-1]
            print(f"    -> exception: {first}")
    passed = sum(1 for b in beats if b.passed)
    print("-" * 78)
    print(f"OVERALL: {'PASS' if passed == len(beats) else 'FAIL'} ({passed}/{len(beats)} beats)")


# =============================================================================
# main
# =============================================================================
def main() -> int:
    ap = argparse.ArgumentParser(description="Headless criterion-12 agent-context arc (aos-dev, REAL ops).")
    ap.add_argument("--stamp", required=True,
                    help="Deterministic capture stamp (caller-supplied, e.g. $(date -u +%Y%m%dT%H%M%SZ)).")
    ap.add_argument("--out", default=None, help="Override capture path (default: public/demo-captures/finops_arc__<stamp>.json).")
    args = ap.parse_args()

    load_env()
    db_ref = assert_dev_only()

    # Run-start instant from the DB (lower bound for this run's audit reads).
    run_started = db_query("SELECT now() AS now")[0]["now"]

    minted = mint_token()
    token = minted["token"]
    gt = fetch_ground_truth()

    # Capture the finops autonomous flag BEFORE any mutation so cleanup can
    # always restore it (even if a beat raises).
    s_cfg, cfg = http_json("GET", f"{FINOPS_URL}/api/agent-config")
    initial_autonomous = bool((cfg or {}).get("autonomousMode")) if s_cfg == 200 else None

    state: dict = {}
    beats: list[Beat] = []
    cleanup_notes: list[str] = []
    try:
        beats.append(beat1_auth(token, minted, run_started, state))
        beats.append(beat2_shallow(token, gt, state))
        beats.append(beat3_traverse(token, gt, state))
        beats.append(beat4_act(initial_autonomous, state))
        beats.append(beat5_boundary(run_started, state))
        beats.append(beat6_revoke(token, state))
    finally:
        cleanup_notes = cleanup(initial_autonomous)

    all_pass = all(b.passed for b in beats) and len(beats) == 6

    capture = {
        "arc": "criterion-12 agent-context (finops cloud_spend, REAL ops)",
        "stamp": args.stamp,
        "overall": "PASS" if all_pass else "FAIL",
        "target": {
            "dcl": DCL_URL, "finops": FINOPS_URL, "db_ref": db_ref,
            "tenant_id": TENANT_ID, "entity_id": ENTITY_ID, "identity": IDENTITY,
        },
        "run_started_at": run_started.isoformat() if hasattr(run_started, "isoformat") else str(run_started),
        "caller_token_id": minted["token_id"],
        "ground_truth": {
            "total_cost": gt["total_cost"],
            "top_service": gt["top_service_name"],
            "worst_efficiency_team": gt["worst_team"],
            "worst_usd_per_deploy": gt["worst_usd_per_deploy"],
        },
        "headline": {
            "worst_efficiency_team": state.get("worst_team"),
            "usd_per_deploy": state.get("worst_usd_per_deploy"),
            "action_correlation_ids": state.get("correlation_ids"),
            "revoke": next((b.values.get("revoke") for b in beats if b.num == 6), None),
            "boundary_record": next((b.values.get("boundary_record") for b in beats if b.num == 5), None),
        },
        "beats": [b.to_dict() for b in beats],
        "cleanup": cleanup_notes,
    }

    out_path = Path(args.out) if args.out else CAPTURE_DIR / f"finops_arc__{args.stamp}.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(capture, indent=2, default=str))

    print_report(beats, db_ref)
    print(f"capture: {out_path}")
    print(f"cleanup: {cleanup_notes}")
    return 0 if all_pass else 1


if __name__ == "__main__":
    sys.exit(main())
