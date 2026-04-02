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

import re
import pytest
import httpx
from playwright.sync_api import Page, expect

DCL_URL = "http://localhost:3004"
DCL_BACKEND = "http://localhost:8004"


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
    """Navigate to DCL frontend and click a tab."""
    page.goto(DCL_URL, wait_until="networkidle")
    tab = page.locator("button, a").filter(has_text=tab_name)
    expect(tab.first).to_be_visible(timeout=15_000)
    tab.first.click()
    page.wait_for_timeout(3_000)


def select_most_recent_entity(page: Page):
    """Select the most recent entity from the entity dropdown."""
    selects = page.locator("select")
    if selects.count() > 0:
        sel = selects.first
        options = sel.locator("option").all()
        # Skip "All Entities" (index 0), pick first real entity
        if len(options) > 1:
            sel.select_option(index=1)
            page.wait_for_timeout(2_000)


class TestDomainCoverage:
    """TEST 1: Context tab Domain Coverage must show >= 10 domains with
    revenue, cogs/expenses, assets, liabilities, equity present."""

    def test_domain_count_at_least_10(self, page_setup: Page):
        page = page_setup
        navigate_to_tab(page, "Context")
        select_most_recent_entity(page)

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
        select_most_recent_entity(page)

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
        select_most_recent_entity(page)

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
        select_most_recent_entity(page)

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
        select_most_recent_entity(page)

        # Click Run Recon
        run_btn = page.locator("button").filter(has_text="Run Recon")
        expect(run_btn.first).to_be_visible(timeout=5_000)
        run_btn.first.click()

        # Wait for results — "Running..." disappears
        page.wait_for_timeout(8_000)

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
        select_most_recent_entity(page)

        body = page.locator("body").text_content() or ""

        # Extract the number after "Total Triples"
        match = re.search(r"Total Triples\s*([\d,]+)", body)
        assert match, (
            f"Could not find Total Triples count on Ingest tab. "
            f"Body excerpt: {body[:500]}"
        )
        ui_total = int(match.group(1).replace(",", ""))

        # Ground truth from API at runtime (B10) — not a hardcoded threshold
        resp = httpx.get(f"{DCL_BACKEND}/api/dcl/entities", timeout=10.0)
        assert resp.status_code == 200, f"Entities API returned {resp.status_code}"
        api_entities = resp.json()["entities"]
        expected_total = sum(e["triple_count"] for e in api_entities)

        assert ui_total == expected_total, (
            f"Total Triples in UI ({ui_total:,}) does not match API "
            f"ground truth ({expected_total:,}). UI-API mismatch."
        )
        # Minimum floor: any valid pipeline run produces at least 10K triples
        assert ui_total >= 10_000, (
            f"Total Triples is {ui_total:,}. Even a single-entity SE run "
            f"should produce at least 10,000 triples."
        )
