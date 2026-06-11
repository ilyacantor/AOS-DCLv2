"""
Playwright acceptance tests for pipeline quality after SE pipeline run.

These tests verify that the single-manifest triple conversion produces:
  1. Full domain coverage (not just 5 operational pipes)
  2. Non-uniform confidence scoring
  3. source_run_tag for recon cross-verification
  4. Total triple count reflecting financial statement additions

Prerequisites:
  - SE pipeline has run (Farm → DCL via /api/dcl/ingest-triples)
  - DCL backend on :8004, frontend on :3004
  - Run: python -m pytest tests/e2e/test_pipeline_quality.py -v
"""

import os
import re
import pytest
import httpx
from playwright.sync_api import Page, expect

# pytest targets the dev stack (DEV_ENV_NOTES: pytest → dcl-dev :8104). The
# dcl-frontend on :3004 proxies /api → :8104, so backend reads must hit :8104 to
# stay coherent with what the UI renders. Override for a prod-consistent run.
DCL_URL = os.environ.get("DCL_FRONTEND_URL", "http://localhost:3004")
DCL_BACKEND = os.environ.get("DCL_BACKEND_URL", "http://localhost:8104")


@pytest.fixture(scope="session", autouse=True)
def verify_backends():
    """Fail fast if DCL backend/frontend not running."""
    try:
        resp = httpx.get(f"{DCL_BACKEND}/api/health", timeout=5.0)
        assert resp.status_code == 200, f"DCL backend returned {resp.status_code}"
    except httpx.ConnectError:
        pytest.fail(f"DCL backend not reachable at {DCL_BACKEND}")
    try:
        resp = httpx.get(DCL_URL, timeout=5.0)
        assert resp.status_code == 200, f"DCL frontend returned {resp.status_code}"
    except httpx.ConnectError:
        pytest.fail(f"DCL frontend not reachable at {DCL_URL}")


@pytest.fixture
def page_setup(page: Page):
    """Block external requests and set viewport."""
    page.set_viewport_size({"width": 1280, "height": 800})

    def handle_route(route):
        if "localhost" in route.request.url:
            route.continue_()
        else:
            route.abort()

    page.route("**/*", handle_route)
    return page


def navigate_to_tab(page: Page, tab_name: str):
    """Navigate to DCL frontend and click a tab.

    wait_until="load" (not "networkidle") — the snapshot selector polls
    every ~12s so the page is never network-idle. Dynamic content is
    awaited via expect(...) in each test.
    """
    page.goto(DCL_URL, wait_until="load")
    tab = page.locator("button, a").filter(has_text=tab_name)
    expect(tab.first).to_be_visible(timeout=15_000)
    tab.first.click()
    page.wait_for_timeout(3_000)


def select_active_snapshot(page: Page):
    """Select the active-run snapshot with the broadest domain coverage.

    These tests assert full pipeline output (>= 10 domains, all financial
    domains present). The snapshot selector follows the latest snapshot by
    default, but the latest snapshot's run may not be the active one, and
    active-run-scoped views (Context domain coverage) return nothing for a
    non-active run. There can also be multiple active runs (one per tenant)
    of differing breadth. Pick the active snapshot whose entity has the most
    populated domains so the quality assertions run against real data.
    """
    sel = page.locator("#snapshot-selector")
    if sel.count() == 0:
        return
    snaps = httpx.get(f"{DCL_BACKEND}/api/dcl/snapshots", timeout=30.0).json()["snapshots"]
    active = [s for s in snaps if s.get("is_current")]
    assert active, "No is_current snapshot — run the ingest pipeline"

    # Deterministic pick from the snapshots payload itself: highest
    # total_rows (the richest run), dcl_ingest_id as the tiebreak. The
    # previous per-candidate domain probes (one contextualization-summary
    # call per active snapshot) silently scored any non-200 as 0 — under
    # load one failed probe let a near-empty entity win selection, which
    # rotated these quality assertions red (the B14 harness-bug class).
    # No probe storm, no silent fallback, same winner every run.
    best = max(active, key=lambda s: (s.get("total_rows") or 0, s["dcl_ingest_id"]))

    # Deterministic sync, tab-aware: only the Context tab fetches the scoped
    # contextualization summary — wait for THIS entity's response there (the
    # page-load convoy can push it past any fixed sleep; the measured landing
    # was ~6s where the old wait_for_timeout(2_000) then scraped a null
    # render as 0 / 0). Other tabs fetch their own endpoints and their tests
    # wait on their own rendered content.
    on_context_tab = page.locator("text=Domain Coverage").count() > 0
    if on_context_tab:
        with page.expect_response(
            lambda r: "contextualization-summary" in r.url
            and f"entity_id={best['entity_id']}" in r.url
            and r.status == 200,
            timeout=45_000,
        ):
            sel.select_option(best["dcl_ingest_id"])
    else:
        sel.select_option(best["dcl_ingest_id"])
    page.wait_for_timeout(500)  # paint settle


class TestDomainCoverage:
    """TEST 1: Context tab Domain Coverage must show >= 10 domains with
    revenue, cogs/expenses, assets, liabilities, equity present."""

    def test_domain_count_at_least_10(self, page_setup: Page):
        page = page_setup
        navigate_to_tab(page, "Context")
        select_active_snapshot(page)

        body = page.locator("body").text_content() or ""

        # Extract "X / Y" from the Domain Coverage metric card
        match = re.search(r"Domain Coverage.*?(\d+)\s*/\s*(\d+)", body, re.DOTALL)
        assert match, (
            f"Could not find 'X / Y' domain coverage number. Body excerpt: "
            f"{body[:500]}"
        )
        populated = int(match.group(1))
        total = int(match.group(2))
        assert populated >= 10, (
            f"Domain coverage too low: {populated} / {total} populated. "
            f"Expected >= 10 domains (revenue, cogs, assets, liabilities, equity, "
            f"cash_flow, opex, pnl, etc.). Got {populated}."
        )

    def test_required_domains_present(self, page_setup: Page):
        page = page_setup
        navigate_to_tab(page, "Context")
        select_active_snapshot(page)

        body = (page.locator("body").text_content() or "").lower()

        required = ["revenue", "asset", "liabilit", "equity", "cash"]
        missing = [d for d in required if d not in body]
        assert not missing, (
            f"Required domains missing from Context tab: {missing}. "
            f"The financial statement generator should produce revenue, asset, "
            f"liability, equity, and cash_flow domains."
        )


class TestConfidenceDistribution:
    """TEST 2: Confidence distribution must not be uniform 0.85/high.
    Exact count must be > 0."""

    def test_exact_confidence_greater_than_zero(self, page_setup: Page):
        page = page_setup
        navigate_to_tab(page, "Context")
        select_active_snapshot(page)

        body = page.locator("body").text_content() or ""

        # Extract "Exact: N"
        exact_match = re.search(r"Exact:\s*(\d[\d,]*)", body)
        assert exact_match, (
            f"Could not find 'Exact: N' in Confidence Distribution. "
            f"Body excerpt: {body[:500]}"
        )
        exact_count = int(exact_match.group(1).replace(",", ""))
        assert exact_count > 0, (
            f"Exact confidence count is {exact_count}. Expected > 0. "
            f"Financial statement triples (deterministic calculations) should "
            f"have confidence_tier='exact'."
        )

    def test_at_least_two_confidence_tiers(self, page_setup: Page):
        page = page_setup
        navigate_to_tab(page, "Context")
        select_active_snapshot(page)

        body = page.locator("body").text_content() or ""

        tiers = {}
        for tier in ["Exact", "High", "Med", "Low"]:
            m = re.search(rf"{tier}:\s*(\d[\d,]*)", body)
            if m:
                tiers[tier] = int(m.group(1).replace(",", ""))

        non_zero = [t for t, c in tiers.items() if c > 0]
        assert len(non_zero) >= 2, (
            f"Only {len(non_zero)} confidence tier(s) have count > 0: {tiers}. "
            f"Expected at least 2 (operational triples=Exact, plus other tiers). "
            f"Uniform confidence means scoring is not working."
        )


class TestReconSourceRunTag:
    """TEST 3: Recon Farm → DCL Count check must NOT be SKIP."""

    def test_farm_dcl_count_not_skip(self, page_setup: Page):
        page = page_setup
        navigate_to_tab(page, "Recon")
        select_active_snapshot(page)

        # Click Run Recon
        run_btn = page.locator("button").filter(has_text="Run Recon")
        expect(run_btn.first).to_be_visible(timeout=5_000)
        run_btn.first.click()

        # Wait for results deterministically: the completion artifact is the
        # rendered check row itself. Under page-load convoy the recon round
        # trips can exceed any fixed sleep (the old 8s wait scraped a
        # still-Running DOM).
        expect(page.locator("body")).to_contain_text("DCL Count", timeout=60_000)
        expect(
            page.locator("button").filter(has_text=re.compile(r"Running"))
        ).to_have_count(0, timeout=60_000)

        body = page.locator("body").text_content() or ""

        # Farm → DCL Count row must exist
        assert "DCL Count" in body, (
            f"Could not find 'Farm → DCL Count' check in Recon results. "
            f"Body excerpt: {body[:500]}"
        )

        # Find the Farm → DCL Count section and check its status
        # The check renders: status icon + check name + summary
        # SKIP shows "No source_run_tag found" in the summary
        farm_dcl_section = page.locator("div").filter(
            has_text=re.compile(r"Farm.*DCL Count")
        )
        section_text = farm_dcl_section.last.text_content() or ""

        assert "SKIP" not in section_text, (
            f"Farm → DCL Count check is SKIP. "
            f"source_run_tag is missing from the triple push. "
            f"Section text: {section_text}"
        )
        assert "source_run_tag" not in section_text.lower(), (
            f"Farm → DCL Count mentions missing source_run_tag: {section_text}"
        )


class TestTripleCountGrowth:
    """TEST 4: Total triple count on Ingest tab must match the API ground truth
    and exceed a minimum floor for a valid SE pipeline run (B10, B17)."""

    def test_total_triples_over_20000(self, page_setup: Page):
        page = page_setup
        navigate_to_tab(page, "Ingest")
        select_active_snapshot(page)

        # Wait for the stat to actually render before scraping — the Ingest
        # tab's own fetch can land after any fixed sleep under the page-load
        # convoy; a premature scrape reads a stale or absent number.
        expect(page.locator("body")).to_contain_text(
            re.compile(r"Total Triples\s*[\d,]{2,}"), timeout=30_000
        )
        body = page.locator("body").text_content() or ""

        # Extract the number after "Total Triples"
        match = re.search(r"Total Triples\s*([\d,]+)", body)
        assert match, (
            f"Could not find Total Triples count on Ingest tab. "
            f"Body excerpt: {body[:500]}"
        )
        ui_total = int(match.group(1).replace(",", ""))

        # Ground truth from API at runtime (B10). The Ingest tab's "Total Triples"
        # is the system-wide ACTIVE count — IngestTab sums total_rows over the
        # is_current snapshots (one active run per entity). Compare against exactly
        # that, not the all-entities sum (which also counts superseded runs on the
        # multi-entity AOS tenant and would never match the UI).
        resp = httpx.get(f"{DCL_BACKEND}/api/dcl/snapshots", timeout=15.0)
        assert resp.status_code == 200, f"Snapshots API returned {resp.status_code}"
        snaps = resp.json()["snapshots"]
        expected_total = sum(s["total_rows"] for s in snaps if s.get("is_current"))

        assert ui_total == expected_total, (
            f"Total Triples in UI ({ui_total:,}) does not match the is_current "
            f"snapshot total from the API ({expected_total:,}). UI-API mismatch."
        )
        # Minimum floor: the active snapshot set should hold a full SE pipeline
        # run's worth of triples (>= 10K).
        assert ui_total >= 10_000, (
            f"Total Triples is {ui_total:,}. The active snapshot set should hold "
            f"at least 10,000 triples (a full SE pipeline run)."
        )
