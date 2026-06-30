#!/usr/bin/env python3
"""Seed the cross-source cloud_spend ESTATE on aos-dev (idempotent).

WHAT IT BUILDS
  ONE entity, ONE ingest run, carrying — via the canonical AAM records-path
  (NOT the Farm→/ingest-triples direct push) — per-resource cost detail
  (warehouse) + per-team OUTPUT (delivery analytics), so DCL aggregates, for the
  SAME entity:
    cloud_spend.by_team.<team>          (cost, $/mo)        — from aws_cost_explorer
    cloud_spend.output_by_team.<team>   (deploys/mo)        — from deploy_analytics
    cloud_spend.efficiency_by_team.<team> = round(cost / output, 2)  (derived)

HOW
  POSTs the AAM estate trigger (POST {AAM}/api/aam/cloud-spend/estate). AAM pulls
  both Farm sources and hands the two pipes to DCL /api/dcl/ingest-records in ONE
  envelope under one dcl_ingest_id with replace=true — idempotent (re-running
  replaces this run's rows, no accumulation). The dcl_ingest_id is derived from
  (tenant, entity) so re-runs are the SAME run (B14).

  AAM must be the aos-dev instance (:8002) configured for DCL :8104, FARM :8003,
  CLOUD_SPEND_SOURCE_URL=/sources/cloud_spend, TEAM_OUTPUT_SOURCE_URL=
  /sources/team_output (see aam/.env.development). Farm + AAM + DCL must be up.

ENV SAFETY (HARD)
  aos-dev DCL is :8104 (Supabase ref glmeqbnu). PROD is :8004 (ref gdbmdr). This
  script REFUSES any :8004 target. The orchestrator must additionally confirm the
  DCL DB ref is glmeqbnu before running. NEVER seed prod.

RUN (orchestrator — not run here)
  python dcl/scripts/seed_cloud_spend_demo.py \
      --aam-url http://localhost:8002 --dcl-url http://localhost:8104 \
      --tenant 69688df3-fc8e-51f8-a77c-9c13f9b3a784 \
      --entity CloudFleet-USE1-7f3a --verify

VERIFY (the ONE query — agent-context read over DCL-MCP, persona CTO/CFO)
  query_triples(tenant_id=<T>, entity_id="CloudFleet-USE1-7f3a", domain="cloud_spend")
  → rows for cloud_spend.by_team.<team> (cost), cloud_spend.output_by_team.<team>
    (deploys), cloud_spend.efficiency_by_team.<team> (usd_per_deploy) — cost,
    output, AND efficiency for one entity from one run.
  REST equivalent (no token needed):
  curl "http://localhost:8104/api/dcl/triples/browse?tenant_id=<T>\
&entity_id=CloudFleet-USE1-7f3a&domain=cloud_spend&limit=500"
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.parse
import urllib.request
import uuid

# aos-dev defaults. The tenant is the estate's fixed dev tenant (given); the
# entity follows I5 ({entity_id}-{short_hash}). Both are CLI-overridable.
_DEFAULT_TENANT = "69688df3-fc8e-51f8-a77c-9c13f9b3a784"
_DEFAULT_ENTITY = "CloudFleet-USE1-7f3a"
_DEFAULT_AAM = "http://localhost:8002"
_DEFAULT_DCL = "http://localhost:8104"

# The three concept roots the estate must produce for the demo to be coherent.
_REQUIRED_CONCEPTS = (
    "cloud_spend.by_team",
    "cloud_spend.output_by_team",
    "cloud_spend.efficiency_by_team",
)


def _estate_run_id(tenant: str, entity: str) -> str:
    """Deterministic ingest id per (tenant, entity) → idempotent re-runs."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"cloud_spend_estate:{tenant}:{entity}"))


def _guard_dev(*urls: str) -> None:
    """Refuse prod (:8004). Loud-fail — never seed prod (fireable)."""
    for u in urls:
        if ":8004" in u:
            raise SystemExit(
                f"REFUSING: {u!r} targets the PROD port :8004. This seed is "
                f"aos-dev ONLY (:8104, Supabase ref glmeqbnu). Aborting."
            )


def _post_json(url: str, body: dict, timeout: float = 180.0) -> dict:
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST",
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", "replace")[:1000]
        raise SystemExit(f"POST {url} -> HTTP {exc.code}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise SystemExit(
            f"POST {url} failed: {exc.reason}. Is AAM up at this URL and "
            f"configured for the Farm sources + DCL :8104? No fallback (A1)."
        ) from exc


def _get_json(url: str, timeout: float = 60.0):
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return json.loads(r.read())
    except urllib.error.URLError as exc:
        raise SystemExit(f"GET {url} failed: {getattr(exc, 'reason', exc)}") from exc


def seed(*, aam_url: str, tenant: str, entity: str, run_id: str) -> dict:
    body = {"tenant_id": tenant, "entity_id": entity, "dcl_ingest_id": run_id}
    print(f"[seed] POST {aam_url}/api/aam/cloud-spend/estate  tenant={tenant} entity={entity}")
    ack = _post_json(f"{aam_url}/api/aam/cloud-spend/estate", body)
    print(f"[seed] dcl_ingest_id={ack.get('dcl_ingest_id')} "
          f"cost_records={ack.get('cost_records')} output_records={ack.get('output_records')} "
          f"attempts={ack.get('attempts')}")
    return ack


def verify(*, dcl_url: str, tenant: str, entity: str) -> bool:
    """The ONE verification query: browse cloud_spend.* for the entity and assert
    cost + output + efficiency are all present. Returns True on success."""
    url = (f"{dcl_url}/api/dcl/triples/browse?tenant_id={tenant}"
           f"&entity_id={urllib.parse.quote(entity)}&domain=cloud_spend&limit=500")
    payload = _get_json(url)
    rows = payload if isinstance(payload, list) else (
        payload.get("triples") or payload.get("rows") or payload.get("data") or [])
    by_concept: dict[str, list] = {}
    for row in rows:
        by_concept.setdefault(str(row.get("concept")), []).append(row)

    ok = True
    for concept in _REQUIRED_CONCEPTS:
        hits = by_concept.get(concept, [])
        mark = "OK " if hits else "MISSING"
        sample = ""
        if hits:
            h = hits[0]
            sample = f"  e.g. {h.get('property')}={h.get('value')} ({h.get('unit')})"
        print(f"[verify] {mark} {concept}: {len(hits)} team rows{sample}")
        if not hits:
            ok = False
    if not ok:
        print("[verify] FAILED — estate is not coherent (cost/output/efficiency missing).")
    else:
        print("[verify] PASS — one entity carries cost, output, AND efficiency from one run.")
    return ok


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Seed the cloud_spend estate on aos-dev (idempotent).")
    p.add_argument("--aam-url", default=_DEFAULT_AAM)
    p.add_argument("--dcl-url", default=_DEFAULT_DCL)
    p.add_argument("--tenant", default=_DEFAULT_TENANT)
    p.add_argument("--entity", default=_DEFAULT_ENTITY)
    p.add_argument("--dcl-ingest-id", default=None,
                   help="Override the derived idempotent run id (default: uuid5 of tenant+entity).")
    p.add_argument("--verify", action="store_true", help="Run the verification query after seeding.")
    p.add_argument("--verify-only", action="store_true", help="Skip seeding; only run the verification query.")
    args = p.parse_args(argv)

    _guard_dev(args.aam_url, args.dcl_url)
    run_id = args.dcl_ingest_id or _estate_run_id(args.tenant, args.entity)

    if not args.verify_only:
        seed(aam_url=args.aam_url, tenant=args.tenant, entity=args.entity, run_id=run_id)

    if args.verify or args.verify_only:
        return 0 if verify(dcl_url=args.dcl_url, tenant=args.tenant, entity=args.entity) else 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
