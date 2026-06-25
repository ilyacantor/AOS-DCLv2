# Operator-visible outcome: the DCL "Demo" tab renders a compact comparison GRID
# (one row per question, Semantics vs Context), summarized at a glance from the
# real scored capture. The Relationship (hero) row reads "lists only — can't
# connect" on the Semantics side and "<team> · below market" on the Context side;
# the Conflict row reads "shows both — no decision" vs "decided · <sources>"; the
# Fact rows read the same correct value + ✓ on BOTH sides (base suffices). Clicking
# a row opens the full grounded answer from each tier. No conceptual header block,
# no CLI text. All expected values come from the capture (Farm-truthed at run time)
# via read-only GET — nothing hardcoded.
"""
B17 acceptance for the Semantics-vs-Context demo GRID. The operator opens the DCL
frontend, clicks Demo, scans the grid, and clicks a row to drill in — real UI
events only. Prerequisite (B15): a real headless run wrote latest.json.
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
    resp = httpx.get(f"{DCL_URL}/demo-captures/latest.json", timeout=10.0)
    if resp.status_code != 200:
        pytest.fail(f"no demo capture ({resp.status_code}) — run `python -m demo.sequence` first (B15)")
    return resp.json()


def _slot(capture: dict, slot_id: str) -> dict:
    return next(s for s in capture["slots"] if s["id"] == slot_id)


def _open_demo_tab(page: Page) -> None:
    page.goto(DCL_URL, wait_until="domcontentloaded")
    page.get_by_role("button", name="Demo", exact=True).click()
    expect(page.get_by_test_id("grounded-demo")).to_be_visible(timeout=15000)


def _row(page: Page, slot_id: str):
    return page.get_by_test_id(f"demo-row-{slot_id}")


def test_grid_renders_with_a_row_per_slot(page: Page, capture: dict):
    _open_demo_tab(page)
    expect(page.get_by_test_id("demo-compare-grid")).to_be_visible()
    expect(page.get_by_test_id("demo-lift")).to_contain_text("80.4")
    for s in capture["slots"]:
        expect(_row(page, s["id"])).to_be_visible()
    page.screenshot(path=os.path.join(SCREENSHOT_DIR, "grounded_demo_overview.png"))


def test_connection_row_premium_drives_base_cannot(page: Page, capture: dict):
    """HERO at a glance: Semantics 'can't connect', Context names the concentration
    + below-market driver — the answer changes across the row."""
    slot = _slot(capture, "q_connection_attrition")
    conn = slot["scores"]["contextos"]["connection"]
    assert conn["passed"], f"capture's connection row is not graph-grounded: {conn}"
    node = (conn.get("cited_concentration_nodes") or [None])[0]
    assert node, f"capture cited no concentration node: {conn}"

    _open_demo_tab(page)
    row = _row(page, "q_connection_attrition")
    expect(row).to_contain_text(re.compile("can.?t connect", re.I))          # Semantics cell
    expect(row).to_contain_text(re.compile(re.escape(node), re.I))           # Context names the team
    expect(row).to_contain_text(re.compile("below market", re.I))            # Context driver
    page.screenshot(path=os.path.join(SCREENSHOT_DIR, "grounded_demo_connection.png"))


def test_conflict_row_premium_decides_base_shows_both(page: Page, capture: dict):
    slot = _slot(capture, "q_conflict_attrition_rate")
    conf = slot["scores"]["contextos"]["conflict"]
    assert conf["passed"], f"capture's conflict row did not decide: {conf}"
    sources = conf.get("sources_named_in_answer") or []
    assert sources, f"capture named no sources: {conf}"

    _open_demo_tab(page)
    row = _row(page, "q_conflict_attrition_rate")
    expect(row).to_contain_text(re.compile("no decision", re.I))             # Semantics cell
    expect(row).to_contain_text("decided")                                   # Context cell
    for src in sources:
        expect(row).to_contain_text(src)                                     # named in the chip
    page.screenshot(path=os.path.join(SCREENSHOT_DIR, "grounded_demo_conflict.png"))


def test_fact_row_both_tiers_correct(page: Page, capture: dict):
    """The seam from the base side: on a resolved fact BOTH tiers show the correct
    value with a check — base is sufficient, you don't pay premium for this."""
    slot = _slot(capture, "q_fact_revenue")
    assert slot["scores"]["semantics"]["correctness"]["passed"], "capture: base tier wrong on the fact"
    assert slot["scores"]["contextos"]["correctness"]["passed"], "capture: premium wrong on the fact"
    _open_demo_tab(page)
    text = _row(page, "q_fact_revenue").text_content() or ""
    # Both tier cells carry a check; the row is a visual tie (base == premium here).
    assert text.count("✓") == 2, f"both tiers must show the correct-fact check; row text: {text!r}"
    page.screenshot(path=os.path.join(SCREENSHOT_DIR, "grounded_demo_fact.png"))


def test_no_data_and_time_rows(page: Page, capture: dict):
    _open_demo_tab(page)
    expect(_row(page, "q_no_data_enps")).to_contain_text(re.compile("no data", re.I))
    pending = _slot(capture, "q_time_asof")
    assert pending["status"] == "pending"
    expect(_row(page, "q_time_asof")).to_contain_text(re.compile("scenario data", re.I))


def test_row_click_opens_full_grounded_answer(page: Page, capture: dict):
    """Clicking a row reveals the real agent answers (the proof behind the
    summary) — both tiers, the premium one naming the graph-grounded driver."""
    slot = _slot(capture, "q_connection_attrition")
    nodes = slot["scores"]["contextos"]["connection"]["cited_concentration_nodes"]
    _open_demo_tab(page)
    _row(page, "q_connection_attrition").click()
    detail = page.get_by_test_id("demo-detail")
    expect(detail).to_be_visible()
    expect(detail).to_contain_text("Context — premium tier")
    expect(detail).to_contain_text("Semantics — base tier")
    # The full grounded answer names at least one of the graph nodes the score
    # cited (prose uses spaces, the node_key uses underscores — normalize).
    text = detail.text_content() or ""
    assert any(re.search(re.escape(n.replace("_", " ")), text, re.I) for n in nodes), (
        f"premium detail must name a graph-grounded org node {nodes}; got: {text[:200]!r}"
    )
    page.screenshot(path=os.path.join(SCREENSHOT_DIR, "grounded_demo_detail.png"))


def test_trust_strip_real_conditions(page: Page, capture: dict):
    _open_demo_tab(page)
    expect(page.get_by_test_id("beat-ingest-reject")).to_contain_text(
        str(capture["beats"]["ingest_reject"]["status_code"]))
    expect(page.get_by_test_id("beat-audit-proof")).to_contain_text(re.compile("audit ledger", re.I))


def test_deep_link_entity_mismatch_is_honest(page: Page, capture: dict):
    """Paired negative test: asking for an entity the capture doesn't hold renders
    the readable mismatch banner naming both entities, never someone else's data."""
    other = "NotARealEntity-0000"
    assert other != capture["meta"]["entity_id"]
    page.goto(f"{DCL_URL}/?view=demo&entity_id={other}", wait_until="domcontentloaded")
    banner = page.get_by_test_id("demo-entity-mismatch")
    expect(banner).to_contain_text(other)
    expect(banner).to_contain_text(capture["meta"]["entity_id"])
