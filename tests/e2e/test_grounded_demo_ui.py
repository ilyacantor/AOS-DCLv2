# Operator-visible outcome: the DCL "Demo" tab renders the latest captured
# Semantics-vs-contextOS tier run for the capture's entity (e.g. ContextOSDemo):
# the net-income slot shows the contextOS panel's answer containing the run-time
# ground-truth value (e.g. 119.05) with a provenance chip naming a real source
# system (e.g. netsuite); the eNPS slot shows the green "honest no-data" chip;
# the conflict slots disclose the Register conflict naming the disagreeing
# sources; the ingest-reject beat shows "HTTP 400"; the audit beat shows
# per-token rows == calls. All expected values come from the capture artifact
# (itself ground-truthed against Farm feeds at sequence run time) fetched via
# read-only GET — nothing is hardcoded here.
"""
B17 acceptance for the grounded-demo WRAPPER: the operator opens the DCL
frontend, clicks the Demo tab, steps through slots by clicking them — real
UI events only. Prerequisite (B15-style): a real headless sequence run has
written public/demo-captures/latest.json (python -m demo.sequence). Panels:
Semantics (base tier, capture key 'semantics') + contextOS (premium tier,
capture key 'contextos').
"""

import json
import os
import re

import httpx
import pytest
from playwright.sync_api import Page, expect

DCL_URL = os.environ.get("DCL_FRONTEND_URL", "http://localhost:3004")
DCL_BACKEND = os.environ.get("DCL_BACKEND_URL", "http://localhost:8104")

SCREENSHOT_DIR = os.path.join(os.path.dirname(__file__), "screenshots")


@pytest.fixture(scope="session", autouse=True)
def verify_backends():
    try:
        resp = httpx.get(f"{DCL_BACKEND}/health", timeout=5.0)
        assert resp.status_code == 200, f"DCL backend returned {resp.status_code}"
    except httpx.ConnectError:
        pytest.fail(f"DCL backend not reachable at {DCL_BACKEND}")


@pytest.fixture(scope="session")
def capture() -> dict:
    """The artifact of record — read-only GET for expected values."""
    resp = httpx.get(f"{DCL_URL}/demo-captures/latest.json", timeout=10.0)
    if resp.status_code != 200:
        pytest.fail(
            f"no demo capture at {DCL_URL}/demo-captures/latest.json "
            f"({resp.status_code}) — run `python -m demo.sequence` first (B15)"
        )
    return resp.json()


def _open_demo_tab(page: Page) -> None:
    page.goto(DCL_URL, wait_until="domcontentloaded")
    page.get_by_role("button", name="Demo", exact=True).click()
    expect(page.get_by_test_id("grounded-demo")).to_be_visible(timeout=15000)


def test_demo_tab_renders_capture_meta_and_summary(page: Page, capture: dict):
    _open_demo_tab(page)
    meta = page.get_by_test_id("demo-meta")
    expect(meta).to_contain_text(capture["meta"]["snapshot_name"])
    expect(meta).to_contain_text(capture["meta"]["dcl_ingest_id"])
    s = capture["summary"]["contextos"]
    expect(page.get_by_test_id("demo-summary")).to_contain_text(
        f"contextOS correct {s['numeric_correct']}/{s['numeric_total']}"
    )
    page.screenshot(path=os.path.join(SCREENSHOT_DIR, "grounded_demo_overview.png"))


def test_net_income_slot_shows_grounded_answer_and_provenance(page: Page, capture: dict):
    slot = next(s for s in capture["slots"] if s["id"] == "q_net_income")
    gt_value = slot["ground_truth_resolved"]["value"]
    sources = slot["scores"]["contextos"]["provenance"]["cited_source_systems"]

    _open_demo_tab(page)
    page.get_by_test_id("demo-slot-q_net_income").click()
    expect(page.get_by_test_id("demo-question-text")).to_contain_text(slot["question"])

    # The contextOS panel's rendered answer carries the run-time ground-truth number.
    after_card = page.get_by_text("contextOS — premium tier").locator("..").locator("..")
    expect(after_card).to_contain_text(str(gt_value))
    expect(after_card).to_contain_text("correct vs source ground truth")
    if sources:
        expect(after_card).to_contain_text(sources[0])

    # The ground-truth line states the resolved source value verbatim.
    expect(page.get_by_test_id("demo-ground-truth")).to_contain_text(str(gt_value))
    page.screenshot(path=os.path.join(SCREENSHOT_DIR, "grounded_demo_net_income.png"))


def test_no_data_slot_renders_honestly(page: Page, capture: dict):
    slot = next(s for s in capture["slots"] if s["id"] == "q_enps_no_data")
    assert slot["scores"]["contextos"]["no_data_honesty"]["passed"], (
        "capture itself failed the no-data beat — fix the sequence, not the UI test"
    )
    _open_demo_tab(page)
    page.get_by_test_id("demo-slot-q_enps_no_data").click()
    after_card = page.get_by_text("contextOS — premium tier").locator("..").locator("..")
    expect(after_card).to_contain_text("honest no-data")
    page.screenshot(path=os.path.join(SCREENSHOT_DIR, "grounded_demo_no_data.png"))


def test_gate1a_headline_slots_render_live_conflicts(page: Page, capture: dict):
    """q1/q2 flipped pending→live (#66 RESOLVED, commit b3387e5): clicking
    each renders the grounded After card disclosing the Register conflict and
    naming the scenario-feed sources from the capture's own scores — and the
    Gate 1A pending tile no longer renders for them (count 0 IS the spec:
    a live slot has no pending state)."""
    _open_demo_tab(page)
    for slot_id in ("q1_attrition_headline", "q2_cloud_conflict"):
        slot = next(s for s in capture["slots"] if s["id"] == slot_id)
        conflict_score = slot["scores"]["contextos"]["conflict"]
        if conflict_score["expected_conflicts"] == 0:
            # Register empty at capture time — the honest expectation flips.
            assert conflict_score["passed"], (
                f"{slot_id}: B failed to state the no-conflict case"
            )
            continue
        page.get_by_test_id(f"demo-slot-{slot_id}").click()
        expect(page.get_by_test_id("demo-pending-tile")).to_have_count(0)
        after_card = page.get_by_text("contextOS — premium tier").locator("..").locator("..")
        for src in conflict_score["sources_named_in_answer"]:
            expect(after_card).to_contain_text(src)
        expect(after_card).to_contain_text("conflict disclosed")
    page.screenshot(path=os.path.join(SCREENSHOT_DIR, "grounded_demo_headline_live.png"))


def test_conflict_slot_discloses_register_sources(page: Page, capture: dict):
    slot = next(s for s in capture["slots"] if s["id"] == "q_conflict_live")
    conflict_score = slot["scores"]["contextos"]["conflict"]
    if conflict_score["expected_conflicts"] == 0:
        # Register empty at capture time — the honest expectation flips.
        assert conflict_score["passed"], "B failed to state the no-conflict case"
        return
    _open_demo_tab(page)
    page.get_by_test_id("demo-slot-q_conflict_live").click()
    after_card = page.get_by_text("contextOS — premium tier").locator("..").locator("..")
    for src in conflict_score["sources_named_in_answer"]:
        expect(after_card).to_contain_text(src)
    expect(after_card).to_contain_text("conflict disclosed")
    page.screenshot(path=os.path.join(SCREENSHOT_DIR, "grounded_demo_conflict.png"))


def test_real_condition_beats_render(page: Page, capture: dict):
    _open_demo_tab(page)
    reject = page.get_by_test_id("beat-ingest-reject")
    expect(reject).to_contain_text(f"HTTP {capture['beats']['ingest_reject']['status_code']}")
    expect(reject).to_contain_text("VALIDATION_FAILED")
    audit = page.get_by_test_id("beat-audit-proof")
    per_token = capture["beats"]["audit_proof"]["per_token"]
    assert per_token, "capture has no audit-proof entries — sequence did not run the contextOS panel"
    expect(audit).to_contain_text(f"{per_token[0]['audit_rows']}/{per_token[0]['tool_calls_made']}")
    expect(audit).to_contain_text(per_token[0]["caller_token_id"])


def test_deep_link_opens_demo_directly(page: Page, capture: dict):
    """The Console launch contract: /?view=demo lands on the demo surface."""
    page.goto(f"{DCL_URL}/?view=demo", wait_until="domcontentloaded")
    expect(page.get_by_test_id("grounded-demo")).to_be_visible(timeout=15000)
    expect(page.get_by_test_id("demo-meta")).to_contain_text(capture["meta"]["snapshot_name"])


def test_deep_link_entity_mismatch_is_honest(page: Page, capture: dict):
    """Asking for an entity the capture doesn't hold renders the honest
    mismatch banner naming both entities — never someone else's numbers
    presented as the requested entity's."""
    other = "NotARealEntity-0000"
    assert other != capture["meta"]["entity_id"]
    page.goto(f"{DCL_URL}/?view=demo&entity_id={other}", wait_until="domcontentloaded")
    banner = page.get_by_test_id("demo-entity-mismatch")
    expect(banner).to_contain_text(other)
    expect(banner).to_contain_text(capture["meta"]["entity_id"])
