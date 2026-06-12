"""Playwright acceptance: Conflict Register operator drill (Gate 1A, B17).

Operator-visible outcome under test: with ConflictUI-T1 selected on the Context
tab, the Conflict Register header reads "1 open"; drilling shows one value
conflict row "revenue.total.amount · 2025-Q1" naming salesforce=110 vs sap=100;
expanding it shows both claims with confidence 0.95 (exact) and the
concept-metadata root cause; after typing a rationale and clicking "Accept sap"
the row's status chip renders "dispositioned", the supersession note appears,
and the header badge drops to "0 open". Expected values are fetched from the
register API (read-only GET) at test time, not hardcoded.

The entity is seeded as a PRECONDITION through the real ingest pipeline
(B5 — data must exist via the real path); the feature under test — drill +
disposition — is driven exclusively by clicks and typing.
"""

import os
import uuid

import httpx
import pytest
from playwright.sync_api import Page, expect

DCL_URL = os.environ.get("DCL_FRONTEND_URL", "http://localhost:3004")
DCL_BACKEND = os.environ.get("DCL_BACKEND_URL", "http://localhost:8104")

TENANT = str(uuid.uuid5(uuid.NAMESPACE_DNS, "conflict-register-ui-test"))
ENTITY = "ConflictUI-T1"


def _triple(source: str, pipe: str, value: float):
    return {
        "entity_id": ENTITY, "concept": "revenue.total", "property": "amount",
        "value": value, "period": "2025-Q1", "source_system": source,
        "source_table": "conflict_ui_probe", "source_field": "amount",
        "pipe_id": pipe, "confidence_score": 0.95, "confidence_tier": "exact",
        "fabric_plane": "ipaas",
    }


def _cleanup_tenant():
    import psycopg2
    from dotenv import load_dotenv
    from pathlib import Path
    load_dotenv(Path(__file__).resolve().parents[2] / ".env.development")
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    try:
        cur = conn.cursor()
        for sql in ("DELETE FROM conflict_dispositions WHERE tenant_id = %s",
                    "DELETE FROM conflict_register WHERE tenant_id = %s",
                    "DELETE FROM semantic_triples WHERE tenant_id = %s",
                    "DELETE FROM tenant_runs WHERE tenant_id = %s"):
            cur.execute(sql, (TENANT,))
        conn.commit()
    finally:
        conn.close()


@pytest.fixture(scope="module", autouse=True)
def seeded_conflict():
    """Precondition via the real pipeline: one ingest carrying the two-source
    disagreement. Cleaned before seeding (idempotent reruns) and after."""
    health = httpx.get(f"{DCL_BACKEND}/api/health", timeout=10.0)
    assert health.status_code == 200, f"DCL backend not healthy at {DCL_BACKEND}"
    _cleanup_tenant()

    run_id = str(uuid.uuid4())
    resp = httpx.post(
        f"{DCL_BACKEND}/api/dcl/ingest-triples",
        json={
            "tenant_id": TENANT, "dcl_ingest_id": run_id, "entity_id": ENTITY,
            "snapshot_name": f"{ENTITY}-{run_id.replace('-', '')[:4]}",
            "triples": [
                _triple("sap", "66666666-6666-4666-8666-666666666661", 100.0),
                _triple("salesforce", "66666666-6666-4666-8666-666666666662", 110.0),
            ],
        },
        timeout=60.0,
    )
    assert resp.status_code == 201, f"seed ingest failed: {resp.status_code} {resp.text}"
    assert resp.json()["conflicts_detected"] == 1

    yield {"dcl_ingest_id": run_id}

    _cleanup_tenant()


def test_conflict_register_drill_and_disposition(page: Page, seeded_conflict):
    # Ground truth from the source system at test time (read-only GET).
    truth = httpx.get(
        f"{DCL_BACKEND}/api/dcl/conflicts",
        params={"entity_id": ENTITY, "status": "open"}, timeout=30.0,
    ).json()
    assert truth["total_count"] == 1
    claim_values = {
        c["source_system"]: float(c["value"])
        for c in truth["conflicts"][0]["claims"]
    }

    page.set_viewport_size({"width": 1400, "height": 900})
    page.goto(DCL_URL, wait_until="domcontentloaded")

    # Operator path: Context tab → entity snapshot → Conflict Register drill.
    page.get_by_role("button", name="Context").click()
    page.locator("#snapshot-selector").select_option(value=seeded_conflict["dcl_ingest_id"])

    panel = page.get_by_test_id("conflicts-panel")
    expect(panel).to_contain_text("1 open", timeout=15000)

    # Settle gate: the Change Proposals panel (Gate 3A) mounts beside the register and
    # re-renders the tab as its fetches land; interact only once it has
    # loaded ("N pending", never '…') so the toggle click cannot be lost to
    # a mid-load re-render — the operator clicks a settled page (the #60
    # condition-wait discipline, not a fixed sleep).
    expect(page.get_by_test_id("proposals-pending-count")).to_contain_text(
        "pending", timeout=15000
    )

    page.get_by_test_id("conflicts-toggle").click()
    row = page.get_by_test_id("conflict-row-revenue.total-2025-Q1")
    expect(row).to_contain_text("revenue.total.amount · 2025-Q1", timeout=15000)
    expect(row).to_contain_text(f"salesforce={claim_values['salesforce']:g}")
    expect(row).to_contain_text(f"sap={claim_values['sap']:g}")
    expect(row).to_contain_text("value")
    expect(row).to_contain_text("open")

    # Drill to claims + provenance.
    row.get_by_role("button").first.click()
    detail = page.get_by_test_id("conflict-detail")
    expect(detail).to_contain_text("sap")
    expect(detail).to_contain_text("salesforce")
    expect(detail).to_contain_text("0.95 (exact)")
    expect(detail).to_contain_text("conflict_ui_probe.amount")
    expect(detail).to_contain_text("Concept 'revenue.total'")

    page.screenshot(path="test-results/conflict_register_drill.png")

    # Disposition: rationale + Accept sap (real clicks; before/after capture).
    page.get_by_test_id("rationale").fill("sap is the ERP of record for recognized revenue")
    page.get_by_test_id("accept-sap").click()

    # After dispositioning, the entry leaves the open-only list and the badge
    # drops to 0 (the contextualization summary refetch).
    expect(panel).to_contain_text("0 open", timeout=15000)

    # Flip to the full register: the same entry now renders dispositioned,
    # with the supersession note and no action buttons.
    panel.get_by_role("checkbox").check()
    expect(row).to_contain_text("dispositioned", timeout=15000)
    row.get_by_role("button").first.click()
    expect(page.get_by_test_id("dispositioned-note")).to_contain_text(
        "losing claims superseded"
    )
    expect(page.get_by_test_id("accept-sap")).to_have_count(0)

    page.screenshot(path="test-results/conflict_register_dispositioned.png")
