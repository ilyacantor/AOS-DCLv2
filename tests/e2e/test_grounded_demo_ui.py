# Operator-visible outcome: the DCL "Demo" tab renders the Semantics-vs-Context
# tier demo for the capture's entity (e.g. ContextOSDemo). Top of screen: the
# thesis carries "+80.4%" (Context Lab lift), the two definitions, the seam, and
# the contrast table. Live proof, by capability class:
#   - CONNECTION (hero): the Context panel is graph-grounded — its chip names a
#     concentration node from the capture (e.g. "platform"); the Semantics panel
#     shows the "no relationship graph" capability-gap chip.
#   - CONFLICT: the Context panel chip reads "decided · <sources>" naming the
#     disagreeing source systems from the capture; the Semantics panel shows
#     "sees both values · cannot arbitrate".
#   - FACT: the Semantics (base) panel itself shows "correct vs source ground
#     truth" + "resolved · provenanced · sufficient" — base succeeds; the
#     ground-truth line carries the run-time resolved value.
#   - ABSENCE: the eNPS slot shows "honest no-data".
#   - TIME: the as-of slot renders the honest pending tile (never simulated).
# Trust strip: ingest-reject "HTTP 400"; audit ledger pass. All expected values
# come from the capture artifact (itself ground-truthed against Farm feeds at
# sequence run time) fetched via read-only GET — nothing is hardcoded here.
"""
B17 acceptance for the Semantics-vs-Context demo WRAPPER: the operator opens the
DCL frontend, clicks the Demo tab, and steps through slots by clicking them —
real UI events only. Prerequisite (B15): a real headless sequence run has written
public/demo-captures/latest.json (python -m demo.sequence). Panels: Semantics
(base tier, capture key 'semantics') + Context (premium tier, key 'contextos').
"""

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


def _slot(capture: dict, slot_id: str) -> dict:
    return next(s for s in capture["slots"] if s["id"] == slot_id)


def _open_demo_tab(page: Page) -> None:
    page.goto(DCL_URL, wait_until="domcontentloaded")
    page.get_by_role("button", name="Demo", exact=True).click()
    expect(page.get_by_test_id("grounded-demo")).to_be_visible(timeout=15000)


def _premium(page: Page):
    return page.get_by_text("Context — premium tier", exact=True).locator("..").locator("..")


def _base(page: Page):
    return page.get_by_text("Semantics — base tier", exact=True).locator("..").locator("..")


def test_thesis_definitions_seam_table_render(page: Page, capture: dict):
    """Top of screen: the reframe — lift thesis, two definitions, the seam, the table."""
    _open_demo_tab(page)
    expect(page.get_by_test_id("demo-thesis")).to_contain_text("80.4")
    defs = page.get_by_test_id("demo-definitions")
    expect(defs).to_contain_text("Semantics")
    expect(defs).to_contain_text("Context")
    expect(page.get_by_test_id("demo-seam")).to_contain_text(re.compile("built on semantics", re.I))
    expect(page.get_by_test_id("demo-table")).to_contain_text(re.compile("arbitration", re.I))
    page.screenshot(path=os.path.join(SCREENSHOT_DIR, "grounded_demo_overview.png"))


def test_connection_slot_is_graph_grounded(page: Page, capture: dict):
    """HERO: Context answers the relationship question by traversing the graph;
    Semantics cannot — it shows the capability-gap chip."""
    slot = _slot(capture, "q_connection_attrition")
    conn = slot["scores"]["contextos"]["connection"]
    assert conn["passed"], (
        "capture's Connection slot is NOT graph-grounded — fix the sequence/prompt, "
        f"not the UI test: {conn}"
    )
    nodes = conn.get("cited_concentration_nodes") or []
    assert nodes, f"capture cited no concentration nodes: {conn}"

    _open_demo_tab(page)
    page.get_by_test_id("demo-slot-q_connection_attrition").click()
    expect(page.get_by_test_id("demo-question-text")).to_contain_text(slot["question"])
    # Context panel is graph-grounded and its chip names the concentration node.
    prem = _premium(page)
    expect(prem).to_contain_text("graph-grounded")
    expect(prem).to_contain_text(re.compile(re.escape(nodes[0]), re.I))
    # Semantics panel shows the honest capability gap — no relationship graph.
    expect(_base(page)).to_contain_text(re.compile("no relationship graph", re.I))
    page.screenshot(path=os.path.join(SCREENSHOT_DIR, "grounded_demo_connection.png"))


def test_conflict_slot_arbitrates_to_decisive_value(page: Page, capture: dict):
    """Context names the disagreeing sources and returns the decisive value;
    Semantics sees both and cannot arbitrate."""
    slot = _slot(capture, "q_conflict_attrition_rate")
    conf = slot["scores"]["contextos"]["conflict"]
    assert conf["passed"], (
        f"capture's Conflict slot did not disclose+decide — fix the sequence: {conf}"
    )
    sources = conf.get("sources_named_in_answer") or []
    assert sources, f"capture named no disagreeing sources: {conf}"

    _open_demo_tab(page)
    page.get_by_test_id("demo-slot-q_conflict_attrition_rate").click()
    prem = _premium(page)
    expect(prem).to_contain_text("decided")
    for src in sources:
        expect(prem).to_contain_text(src)  # source ids are carried verbatim in the chip
    expect(_base(page)).to_contain_text(re.compile("cannot arbitrate", re.I))
    page.screenshot(path=os.path.join(SCREENSHOT_DIR, "grounded_demo_conflict.png"))


def test_fact_slot_base_tier_succeeds(page: Page, capture: dict):
    """The seam from the base side: Semantics returns the resolved fact correctly,
    with provenance — base is sufficient here."""
    slot = _slot(capture, "q_fact_revenue")
    correctness = slot["scores"]["semantics"]["correctness"]
    assert correctness["passed"], (
        f"capture: base tier did not answer the fact correctly — fix the sequence: {correctness}"
    )
    gt_value = slot["ground_truth_resolved"]["value"]

    _open_demo_tab(page)
    page.get_by_test_id("demo-slot-q_fact_revenue").click()
    base = _base(page)
    expect(base).to_contain_text("correct vs source ground truth")
    expect(base).to_contain_text(re.compile("resolved .* provenanced .* sufficient", re.I))
    # The ground-truth line states the live-resolved source value verbatim.
    expect(page.get_by_test_id("demo-ground-truth")).to_contain_text(str(gt_value))
    page.screenshot(path=os.path.join(SCREENSHOT_DIR, "grounded_demo_fact.png"))


def test_no_data_slot_renders_honestly(page: Page, capture: dict):
    slot = _slot(capture, "q_no_data_enps")
    assert slot["scores"]["contextos"]["no_data_honesty"]["passed"], (
        "capture itself failed the no-data beat — fix the sequence, not the UI test"
    )
    _open_demo_tab(page)
    page.get_by_test_id("demo-slot-q_no_data_enps").click()
    expect(_premium(page)).to_contain_text("honest no-data")
    page.screenshot(path=os.path.join(SCREENSHOT_DIR, "grounded_demo_no_data.png"))


def test_time_slot_is_honest_pending_not_simulated(page: Page, capture: dict):
    """Negative/honesty surface: the as-of slot has no scenario data yet, so it
    renders the pending tile — never a fabricated answer."""
    slot = _slot(capture, "q_time_asof")
    assert slot["status"] == "pending", f"time slot unexpectedly live: {slot['status']}"
    _open_demo_tab(page)
    page.get_by_test_id("demo-slot-q_time_asof").click()
    tile = page.get_by_test_id("demo-pending-tile")
    expect(tile).to_be_visible()
    expect(tile).to_contain_text(re.compile("never simulated", re.I))
    # No answer panels render for a pending slot.
    expect(page.get_by_text("Context — premium tier", exact=True)).to_have_count(0)
    page.screenshot(path=os.path.join(SCREENSHOT_DIR, "grounded_demo_pending.png"))


def test_trust_strip_real_conditions(page: Page, capture: dict):
    _open_demo_tab(page)
    reject = page.get_by_test_id("beat-ingest-reject")
    expect(reject).to_contain_text(f"HTTP {capture['beats']['ingest_reject']['status_code']}")
    audit = page.get_by_test_id("beat-audit-proof")
    expect(audit).to_contain_text(re.compile("audit ledger", re.I))


def test_deep_link_opens_demo_directly(page: Page, capture: dict):
    """The Console launch contract: /?view=demo lands on the demo surface."""
    page.goto(f"{DCL_URL}/?view=demo", wait_until="domcontentloaded")
    expect(page.get_by_test_id("grounded-demo")).to_be_visible(timeout=15000)
    expect(page.get_by_test_id("demo-summary")).to_contain_text(capture["meta"]["snapshot_name"])


def test_deep_link_entity_mismatch_is_honest(page: Page, capture: dict):
    """Paired negative test: asking for an entity the capture doesn't hold renders
    the readable mismatch banner naming both entities — never someone else's
    numbers presented as the requested entity's."""
    other = "NotARealEntity-0000"
    assert other != capture["meta"]["entity_id"]
    page.goto(f"{DCL_URL}/?view=demo&entity_id={other}", wait_until="domcontentloaded")
    banner = page.get_by_test_id("demo-entity-mismatch")
    expect(banner).to_contain_text(other)
    expect(banner).to_contain_text(capture["meta"]["entity_id"])
