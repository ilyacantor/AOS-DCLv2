"""
The SEQUENCE layer — orders real platform operations into the §13
before/after run. Headless by design; the same artifact CI runs
(headless run = regression run). The wrapper only renders what this
writes.

Every step here is a documented, manually-runnable platform operation
(demo/OPERATIONS.md). Pending slots (Gate 1A scenario beats) are recorded
as pending with their reason — output is never faked.

Run:
    DCL_MCP_TOKEN_SECRET=… python -m demo.sequence --entity CedarGrid-1823
Exit codes: 0 all non-pending beats pass; 1 any beat failed; 2 preflight failed.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import httpx
import yaml

from demo import feeds, scoring
from demo.agent_common import DEFAULT_MODEL, REPO_ROOT, dcl_dev_url, load_demo_env
from demo.panel_a import run_panel_a
from demo.panel_b import run_panel_b

# Live dev backend (:8104), prod-guarded — no per-run --dcl-url override needed.
DCL_URL = dcl_dev_url()
QUESTIONS_PATH = Path(__file__).parent / "questions.yaml"
DEFAULT_OUT_DIR = REPO_ROOT / "public" / "demo-captures"


def _fail(msg: str, code: int = 2) -> None:
    print(f"[FAIL] {msg}")
    sys.exit(code)


def preflight(dcl_url: str, entity_id: str) -> dict:
    """Health + entity presence + auth-secret presence. Loud on any miss."""
    checks: dict = {}
    for name, url in (("dcl", f"{dcl_url}/health"), ("farm", f"{feeds.FARM_URL}/api/health")):
        try:
            r = httpx.get(url, timeout=10)
            checks[name] = {"url": url, "status": r.status_code}
            if r.status_code != 200:
                _fail(f"preflight: {name} unhealthy — GET {url} -> {r.status_code}")
        except Exception as exc:
            _fail(f"preflight: {name} unreachable — GET {url} -> {type(exc).__name__}: {exc}")

    if not os.environ.get("DCL_MCP_TOKEN_SECRET"):
        _fail("preflight: DCL_MCP_TOKEN_SECRET not set — Panel B cannot mint/verify tokens")

    r = httpx.get(f"{dcl_url}/api/dcl/snapshots", params={"limit": 50}, timeout=30)
    if r.status_code != 200:
        _fail(f"preflight: snapshots read failed — {r.status_code}: {r.text[:200]}")
    snap = next((s for s in r.json().get("snapshots", []) if s.get("entity_id") == entity_id), None)
    if snap is None:
        _fail(f"preflight: entity {entity_id!r} not present in current snapshots — "
              f"run the records-path pipeline for it first (B15)")
    checks["entity_snapshot"] = snap
    print(f"[PASS] preflight — dcl+farm healthy; {entity_id} current "
          f"(dcl_ingest_id {snap['dcl_ingest_id']}, {snap['total_rows']} rows)")
    return checks


def probe_register(dcl_url: str, entity_id: str) -> tuple[str | None, list[dict], dict]:
    """Conflict Register probe — also resolves the entity's tenant_id.
    A non-200 means the conflicts capability is absent on this build:
    conflict slots then degrade to pending honestly."""
    r = httpx.get(f"{dcl_url}/api/dcl/conflicts",
                  params={"entity_id": entity_id, "limit": 100}, timeout=30)
    if r.status_code != 200:
        return None, [], {"available": False, "status": r.status_code, "detail": r.text[:300]}
    body = r.json()
    return body["tenant_id"], body.get("conflicts", []), {
        "available": True,
        "open_conflicts": body.get("total_count", len(body.get("conflicts", []))),
    }


def ingest_reject_beat(dcl_url: str, entity_id: str, tenant_id: str) -> dict:
    """Real-condition beat: a malformed/partial ingest envelope must be
    rejected loudly at the platform boundary — 422 with an informative
    error, no silent acceptance, no write."""
    import uuid as _uuid
    envelope = {
        "tenant_id": tenant_id,
        "dcl_ingest_id": str(_uuid.uuid4()),
        "entity_id": entity_id,
        "snapshot_name": f"{entity_id}-demo",
        "pipes": [],  # partial input: no pipes — nothing to ingest
    }
    r = httpx.post(f"{dcl_url}/api/dcl/ingest-records", json=envelope, timeout=30)
    detail = r.text[:500]
    # Contract: 400 VALIDATION_FAILED (domain validation; 422 is the
    # identity/I2 class) with an informative message. Anything else —
    # especially a 2xx — fails the beat.
    passed = r.status_code == 400 and "VALIDATION_FAILED" in detail
    status = "PASS" if passed else "FAIL"
    print(f"[{status}] ingest-reject beat — POST /api/dcl/ingest-records (empty pipes) "
          f"-> {r.status_code}; expected 400 VALIDATION_FAILED")
    return {"request": envelope, "status_code": r.status_code,
            "response_excerpt": detail, "passed": passed}


def audit_proof_beat(dcl_url: str, tenant_id: str, panel_b_runs: list[dict]) -> dict:
    """Externality proof: every MCP call Panel B made is visible in the
    audit ledger under its token id. Equality is the assertion — a missing
    audit row is a real failure, not noise."""
    proofs = []
    all_pass = True
    for run in panel_b_runs:
        token_id = run["mcp"]["caller_token_id"]
        expected = len(run["tool_calls"])
        r = httpx.get(f"{dcl_url}/api/dcl/mcp/audit",
                      params={"tenant_id": tenant_id, "caller_token_id": token_id,
                              "limit": 500}, timeout=30)
        if r.status_code != 200:
            proofs.append({"caller_token_id": token_id, "passed": False,
                           "error": f"audit read -> {r.status_code}: {r.text[:200]}"})
            all_pass = False
            continue
        body = r.json()
        got = body["total_count"]
        ok = got == expected
        all_pass &= ok
        proofs.append({
            "question_id": run.get("question_id"),
            "caller_token_id": token_id,
            "tool_calls_made": expected,
            "audit_rows": got,
            "tools_audited": sorted({e["tool_name"] for e in body["entries"]}),
            "transport": sorted({e["transport"] for e in body["entries"] if e["transport"]}),
            "passed": ok,
        })
    status = "PASS" if all_pass else "FAIL"
    print(f"[{status}] audit-proof beat — {len(proofs)} Panel-B tokens checked "
          f"against /api/dcl/mcp/audit (rows must equal calls)")
    return {"per_token": proofs, "passed": all_pass}


def run_sequence(entity_id: str, tenant_override: str | None, dcl_url: str,
                 model: str, out_dir: Path) -> int:
    spec = yaml.safe_load(QUESTIONS_PATH.read_text())
    rel_tol = float(spec["meta"].get("rel_tolerance", 0.02))
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    checks = preflight(dcl_url, entity_id)
    tenant_id, register_conflicts, register_meta = probe_register(dcl_url, entity_id)
    if tenant_override:
        tenant_id = tenant_override
    if not tenant_id:
        _fail("tenant_id unresolved — conflicts capability absent on this build; "
              "pass --tenant explicitly")
    print(f"[PASS] register probe — conflicts capability: {register_meta}")

    beats = {"preflight": checks, "register_probe": register_meta,
             "ingest_reject": ingest_reject_beat(dcl_url, entity_id, tenant_id)}

    feed_cache: dict[str, dict] = {}

    def gt_for(slot: dict) -> float:
        gt = slot["ground_truth"]
        if gt["feed"] not in feed_cache:
            feed_cache[gt["feed"]] = feeds.fetch_feed(gt["feed"], entity_id)
        return feeds.ground_truth_value(feed_cache[gt["feed"]], gt["field"], gt["period"])

    slot_results: list[dict] = []
    panel_b_runs: list[dict] = []
    any_failed = not beats["ingest_reject"]["passed"]

    for slot in spec["slots"]:
        record = {"id": slot["id"], "question": slot["question"],
                  "status": slot["status"], "kind": slot["kind"]}
        if slot["status"] != "live":
            record["pending_reason"] = slot.get("pending_reason", "")
            print(f"[PENDING] {slot['id']} — {slot.get('pending_reason', '')[:80]}…")
            slot_results.append(record)
            continue

        gt_value = gt_for(slot) if slot["kind"] == "numeric" else None
        if gt_value is not None:
            record["ground_truth_resolved"] = {**slot["ground_truth"], "value": gt_value}

        print(f"[RUN ] {slot['id']} — both panels…")
        try:
            cap_a = asyncio.run(run_panel_a(entity_id, slot["question"], model))
            cap_b = asyncio.run(run_panel_b(entity_id, tenant_id, slot["question"], model, dcl_url))
        except Exception as exc:
            record["error"] = f"{type(exc).__name__}: {exc}"
            record["passed"] = False
            any_failed = True
            print(f"[FAIL] {slot['id']} — panel run error: {record['error'][:200]}")
            slot_results.append(record)
            continue

        cap_b["question_id"] = slot["id"]
        panel_b_runs.append(cap_b)
        record["panel_a"] = cap_a
        record["panel_b"] = cap_b
        record["scores"] = scoring.score_slot(
            slot, gt_value, {"a": cap_a, "b": cap_b}, register_conflicts, rel_tol
        )

        b = record["scores"]["b"]
        b_ok = (
            b.get("correctness", {}).get("passed",
            b.get("no_data_honesty", {}).get("passed",
            b.get("conflict", {}).get("passed", False)))
        )
        record["passed"] = bool(b_ok)
        if not b_ok:
            any_failed = True
        gt_str = f" gt={gt_value}" if gt_value is not None else ""
        print(f"[{'PASS' if b_ok else 'FAIL'}] {slot['id']}{gt_str} — "
              f"A correct={record['scores']['a'].get('correctness', {}).get('passed', 'n/a')} "
              f"B correct={b.get('correctness', {}).get('passed', 'n/a')} "
              f"B prov={b['provenance']['present']}")
        slot_results.append(record)

    beats["audit_proof"] = audit_proof_beat(dcl_url, tenant_id, panel_b_runs)
    if not beats["audit_proof"]["passed"]:
        any_failed = True

    capture = {
        "meta": {
            "stamp": stamp,
            "entity_id": entity_id,
            "tenant_id": tenant_id,
            "model": model,
            "dcl_url": dcl_url,
            "farm_url": feeds.FARM_URL,
            "dcl_ingest_id": checks["entity_snapshot"]["dcl_ingest_id"],
            "snapshot_name": checks["entity_snapshot"].get("snapshot_name"),
            "register": register_meta,
        },
        "beats": beats,
        "slots": slot_results,
        "summary": scoring.summarize(slot_results),
        "sequence_passed": not any_failed,
    }

    out_dir.mkdir(parents=True, exist_ok=True)
    stamped = out_dir / f"{stamp}__{entity_id}.json"
    stamped.write_text(json.dumps(capture, indent=2, default=str))
    (out_dir / "latest.json").write_text(json.dumps(capture, indent=2, default=str))
    print(f"\ncapture: {stamped}")
    print(f"summary: {json.dumps(capture['summary'])}")
    print(f"sequence: {'PASS' if not any_failed else 'FAIL'}")
    return 0 if not any_failed else 1


def main() -> None:
    parser = argparse.ArgumentParser(description="Grounded-demo headless sequence (§13)")
    parser.add_argument("--entity", default=None)
    parser.add_argument("--tenant", default=None, help="override tenant resolution")
    parser.add_argument("--dcl-url", default=DCL_URL)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    args = parser.parse_args()

    load_demo_env()
    spec = yaml.safe_load(QUESTIONS_PATH.read_text())
    entity = args.entity or spec["meta"]["entity_default"]
    sys.exit(run_sequence(entity, args.tenant, args.dcl_url, args.model, Path(args.out_dir)))


if __name__ == "__main__":
    main()
