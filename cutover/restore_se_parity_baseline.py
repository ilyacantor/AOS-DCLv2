#!/usr/bin/env python3
"""Restore the FluxEdge-TMZ8 SE-parity baseline into the dev store.

The 2026-06-10 dev prune (dcl_deferred_work.md#56) deleted FluxEdge-TMZ8's
rows; the frozen parity contract (se_parity_baseline__FluxEdge-TMZ8.jsonl,
24,165 triples / 259 concepts) survived as files. This pushes the frozen
set back through the REAL ingest path (POST /api/dcl/ingest-triples on
dcl-dev :8104) so Gate 0's pre/post-migration parity is measured on the
exact contract data. No store backdoor — every row passes the live
ontology/persona/provenance/identity gates.

Restore provenance is explicit, not simulated: the baseline file is the
source of THIS ingest, so source_table names the file and source_field
names the jsonl key that carried the value. source_system / unit / period /
confidence_tier are the frozen per-row values. fabric_plane is stamped from
the Farm SE source→plane map observed in the live dev store (netsuite-class
ERP feeds arrive on ipaas, observability on api_gateway, streams on
event_bus, cost on data_warehouse); fabric_product is left NULL — the
original per-product attribution was not captured and is not invented.

Run: cd /home/ilyac/code/dcl && .venv/bin/python cutover/restore_se_parity_baseline.py
"""
import json
import os
import sys
import time
import uuid
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
BASELINE = os.path.join(HERE, "se_parity_baseline__FluxEdge-TMZ8.jsonl")

DCL_DEV = os.environ.get("DCL_DEV_URL", "http://localhost:8104")
ENTITY = "FluxEdge-TMZ8"
TENANT = "69688df3-fc8e-51f8-a77c-9c13f9b3a784"  # shared SE tenant the baseline was captured on
BATCH = 1000

TIER_SCORE = {"exact": 0.95, "high": 0.85, "medium": 0.70, "low": 0.50}

# Farm SE source→plane map as observed in the live dev store (gl-bearing
# entities, 2026-06-11). An unmapped source is a loud failure, not a default.
SOURCE_PLANE = {
    "sap": "ipaas", "netsuite": "ipaas", "quickbooks": "ipaas",
    "salesforce": "ipaas", "workday": "ipaas", "stripe": "ipaas",
    "datadog": "api_gateway", "jira": "api_gateway", "pagerduty": "api_gateway",
    "github_actions": "api_gateway", "zendesk": "api_gateway",
    "kafka": "event_bus", "confluent": "event_bus",
    "aws_cost_explorer": "data_warehouse", "snowflake": "data_warehouse",
}


def _post(path: str, body: dict) -> dict:
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        f"{DCL_DEV}{path}", data=data, method="POST",
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        return json.loads(resp.read())


def main() -> None:
    rows = [json.loads(l) for l in open(BASELINE)]
    print(f"baseline rows: {len(rows)}")

    sources = sorted({r["source_system"] for r in rows})
    unmapped = [s for s in sources if s not in SOURCE_PLANE]
    if unmapped:
        raise RuntimeError(
            f"source_systems {unmapped} have no fabric_plane mapping — "
            f"extend SOURCE_PLANE before restoring; refusing to default."
        )
    print(f"distinct sources: {sources}")

    ingest_id = str(uuid.uuid4())
    pipe_id = str(uuid.uuid4())  # one explicit restore pipe for the whole push
    snapshot_name = f"{ENTITY}-{ingest_id.replace('-', '')[:4]}"
    print(f"dcl_ingest_id={ingest_id} snapshot_name={snapshot_name} pipe_id={pipe_id}")

    triples = [
        {
            "entity_id": ENTITY,
            "concept": r["concept"],
            "property": r["property"],
            "value": r["value"],
            "period": r["period"],
            "unit": r["unit"],
            "source_system": r["source_system"],
            "source_table": os.path.basename(BASELINE),
            "source_field": "value",
            "pipe_id": pipe_id,
            "confidence_score": TIER_SCORE[r["confidence_tier"]],
            "confidence_tier": r["confidence_tier"],
            "fabric_plane": SOURCE_PLANE[r["source_system"]],
            "fabric_product": None,
        }
        for r in rows
    ]

    total = 0
    for i in range(0, len(triples), BATCH):
        chunk = triples[i:i + BATCH]
        body = {
            "tenant_id": TENANT,
            "dcl_ingest_id": ingest_id,
            "entity_id": ENTITY,
            "snapshot_name": snapshot_name if i == 0 else None,
            "triples": chunk,
        }
        qs = "" if i == 0 else "?append=true"
        for attempt in (1, 2):
            try:
                resp = _post(f"/api/dcl/ingest-triples{qs}", body)
                break
            except Exception as e:
                if attempt == 2:
                    raise
                print(f"  batch {i // BATCH}: attempt 1 failed ({e}); retrying in 3s")
                time.sleep(3)
        total += len(chunk)
        print(f"  batch {i // BATCH}: {len(chunk)} sent, run total reported={resp['triples_written']}")

    print(f"restore push complete: {total} triples sent, dcl_ingest_id={ingest_id}")


if __name__ == "__main__":
    sys.exit(main())
