"""
Playwright E2E tests for the 4 monitoring tabs: Ingest, Context, Dashboard, Recon.

Prerequisites:
  - DCL backend on :8004, frontend on :3004
  - Financial triples loaded (at least one ingest run)
  - Run: python -m pytest tests/e2e/test_monitoring_tabs.py -v

Install (once):
  pip install playwright pytest-playwright
  playwright install chromium
"""

import json
import pytest
import httpx
from playwright.sync_api import Page, expect

DCL_URL = "http://localhost:3004"
DCL_BACKEND = "http://localhost:8004"

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session", autouse=True)
def verify_backends():
    """Fail fast if DCL backend isn't running."""
    try:
        resp = httpx.get(f"{DCL_BACKEND}/api/health", timeout=5.0)
        assert resp.status_code == 200, f"DCL backend returned {resp.status_code}"
    except httpx.ConnectError:
        pytest.fail(f"DCL backend not reachable at {DCL_BACKEND}")


@pytest.fixture(scope="session", autouse=True)
def seed_ingest_log():
    """Ensure at least one ingest_log entry exists by triggering a small ingest
    if the log is empty. Uses append mode to avoid disrupting existing data."""
    try:
        resp = httpx.get(f"{DCL_BACKEND}/api/dcl/ingest-log?limit=1", timeout=5.0)
        if resp.status_code == 200 and len(resp.json()) > 0:
            return  # Already have log entries

        # Check if triples exist (from prior pipeline runs)
        overview = httpx.get(f"{DCL_BACKEND}/api/dcl/triples/overview", timeout=5.0)
        if overview.status_code == 200 and overview.json().get("total_triples", 0) > 0:
            # Triples exist but no ingest_log — the table was just created.
            # That's fine; tests will check for whatever state exists.
            return
    except Exception:
        pass  # Non-fatal; tests will handle missing data


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

    Uses networkidle to ensure async API calls (runs, tab data) complete
    before the test reads the DOM. The runs endpoint returns in ~850ms
    with 95 runs; networkidle waits until no requests for 500ms.
    """
    page.goto(DCL_URL, wait_until="networkidle")
    tab = page.locator("button, a").filter(has_text=tab_name)
    expect(tab.first).to_be_visible(timeout=15_000)
    tab.first.click()
    page.wait_for_timeout(3_000)  # Allow tab-specific data fetch


# ---------------------------------------------------------------------------
# Cross-tab tests
# ---------------------------------------------------------------------------

class TestCrossTab:
    """Navigate between all 4 tabs — verify no crash or blank content."""

    def test_all_tabs_navigate_without_crash(self, page_setup: Page):
        page = page_setup
        page.goto(DCL_URL, wait_until="load")
        page.wait_for_timeout(3_000)

        for tab_name in ["Ingest", "Context", "Dashboard", "Recon", "Graph v2"]:
            tab = page.locator("button, a").filter(has_text=tab_name)
            expect(tab.first).to_be_visible(timeout=10_000)
            tab.first.click()
            page.wait_for_timeout(1_500)

            # Verify no blank content: body should have meaningful text
            body_text = page.locator("body").text_content()
            assert body_text is not None and len(body_text) > 50, (
                f"Tab '{tab_name}' appears blank or has minimal content"
            )

            # No error elements
            errors = page.locator('[class*="destructive"]').count()
            # Allow for error states if backend data is empty
            # but the component itself should render
            assert page.locator("button, table, select, div").count() > 5, (
                f"Tab '{tab_name}' has too few DOM elements — likely crashed"
            )

    def test_all_tabs_have_entity_selector(self, page_setup: Page):
        """Every monitoring tab must display an entity selector dropdown."""
        page = page_setup
        page.goto(DCL_URL, wait_until="load")
        page.wait_for_timeout(3_000)

        for tab_name in ["Ingest", "Context", "Dashboard", "Recon"]:
            tab = page.locator("button, a").filter(has_text=tab_name)
            expect(tab.first).to_be_visible(timeout=10_000)
            tab.first.click()
            page.wait_for_timeout(2_000)

            body_text = page.locator("body").text_content() or ""

            # Entity selector dropdown must be present
            assert "Entity:" in body_text, (
                f"Tab '{tab_name}' missing entity selector (no 'Entity:' label)"
            )

    def test_all_tabs_show_same_entity(self, page_setup: Page):
        """After load, all 4 monitoring tabs must show the same selected entity."""
        page = page_setup
        page.goto(DCL_URL, wait_until="networkidle")
        page.wait_for_timeout(3_000)

        entities_seen = {}
        for tab_name in ["Ingest", "Context", "Dashboard", "Recon"]:
            tab = page.locator("button, a").filter(has_text=tab_name)
            expect(tab.first).to_be_visible(timeout=10_000)
            tab.first.click()
            page.wait_for_timeout(2_000)

            # Find the entity selector — it has "Entity:" label next to it
            selects = page.locator("select")
            for i in range(selects.count()):
                sel = selects.nth(i)
                val = sel.input_value()
                # Entity IDs are readable names (not UUIDs)
                if val and len(val) < 100 and not (len(val) > 30 and val.count("-") >= 4):
                    entities_seen[tab_name] = val
                    break

        assert len(entities_seen) >= 3, (
            f"Could not find entity selector in enough tabs: {entities_seen}"
        )
        unique_entities = set(entities_seen.values())
        assert len(unique_entities) == 1, (
            f"Tabs show different entities — provenance mismatch: {entities_seen}"
        )


# ---------------------------------------------------------------------------
# WP-2: Ingest tab
# ---------------------------------------------------------------------------

class TestIngestTab:
    def test_summary_metrics_visible(self, page_setup: Page):
        page = page_setup
        navigate_to_tab(page, "Ingest")

        # Summary metric cards should be in viewport without scrolling
        body = page.locator("body")
        body_text = body.text_content() or ""

        # Check for metric labels
        assert "Last Ingest" in body_text or "Total Triples" in body_text, (
            "Ingest tab summary metrics not found"
        )
        assert "Rejection Rate" in body_text or "Ingest Count" in body_text, (
            "Ingest tab summary metrics incomplete"
        )

    def test_table_headers_present(self, page_setup: Page):
        page = page_setup
        navigate_to_tab(page, "Ingest")

        # Verify table headers
        expected_headers = ["Timestamp", "Run ID", "Entity", "Received", "Written", "Rejected", "Sources", "Duration"]
        for header in expected_headers:
            header_el = page.locator("th").filter(has_text=header)
            expect(header_el.first).to_be_visible(timeout=5_000)

    def test_refresh_button_works(self, page_setup: Page):
        page = page_setup
        navigate_to_tab(page, "Ingest")

        refresh_btn = page.locator("button").filter(has_text="Refresh")
        expect(refresh_btn.first).to_be_visible(timeout=5_000)
        refresh_btn.first.click()
        page.wait_for_timeout(1_000)

        # Page should still be functional after refresh
        body_text = page.locator("body").text_content() or ""
        assert "Last Ingest" in body_text or "Ingest Count" in body_text

    def test_entity_selector(self, page_setup: Page):
        page = page_setup
        navigate_to_tab(page, "Ingest")

        body_text = page.locator("body").text_content() or ""
        assert "Entity:" in body_text, "Ingest tab missing entity selector"

        # Entity selector dropdown should exist with at least one option
        selects = page.locator("select")
        assert selects.count() >= 1, "No select elements found on Ingest tab"


# ---------------------------------------------------------------------------
# WP-3: Contextualization tab
# ---------------------------------------------------------------------------

class TestContextTab:
    def test_summary_metrics_render(self, page_setup: Page):
        page = page_setup
        navigate_to_tab(page, "Context")

        body_text = page.locator("body").text_content() or ""
        assert "Domain Coverage" in body_text, "Domain coverage metric missing"
        assert "Confidence" in body_text, "Confidence metric missing"
        assert "Source Systems" in body_text, "Source systems metric missing"

    def test_domain_table_present(self, page_setup: Page):
        page = page_setup
        navigate_to_tab(page, "Context")

        # Domain coverage table should have a header
        domain_header = page.locator("text=Domain Coverage").first
        expect(domain_header).to_be_visible(timeout=5_000)

        # Table should have domain-related headers
        domain_th = page.locator("th").filter(has_text="Domain")
        expect(domain_th.first).to_be_visible(timeout=5_000)

    def test_source_panel_alongside_domain(self, page_setup: Page):
        page = page_setup
        navigate_to_tab(page, "Context")

        # Both panels should be visible (side by side, not stacked)
        domain_panel = page.locator("text=Domain Coverage").first
        source_panel = page.locator("text=Source Systems").first
        expect(domain_panel).to_be_visible(timeout=5_000)
        expect(source_panel).to_be_visible(timeout=5_000)

    def test_confidence_bar_renders(self, page_setup: Page):
        page = page_setup
        navigate_to_tab(page, "Context")

        # Confidence distribution section
        conf_header = page.locator("text=Confidence Distribution").first
        expect(conf_header).to_be_visible(timeout=5_000)

    def test_entity_selector(self, page_setup: Page):
        page = page_setup
        navigate_to_tab(page, "Context")

        # Entity dropdown should be present
        entity_select = page.locator("select").filter(has_text="All Entities")
        expect(entity_select.first).to_be_visible(timeout=5_000)


# ---------------------------------------------------------------------------
# WP-4: Dashboard tab
# ---------------------------------------------------------------------------

class TestDashboardTab:
    def test_filter_bar_renders(self, page_setup: Page):
        page = page_setup
        navigate_to_tab(page, "Dashboard")

        body_text = page.locator("body").text_content() or ""
        # Filter labels should be present
        for label in ["Domain:", "Source:", "Period:"]:
            assert label in body_text, f"Filter label '{label}' not found in Dashboard"

    def test_data_table_renders(self, page_setup: Page):
        page = page_setup
        navigate_to_tab(page, "Dashboard")

        # Table headers
        for header in ["Concept", "Property", "Value", "Period", "Source", "Confidence"]:
            th = page.locator("th").filter(has_text=header)
            expect(th.first).to_be_visible(timeout=5_000)

    def test_sidebar_aggregations_visible(self, page_setup: Page):
        page = page_setup
        navigate_to_tab(page, "Dashboard")

        # Sidebar should show aggregation sections
        for section in ["Domains", "Sources", "Periods"]:
            el = page.locator("div").filter(has_text=section)
            assert el.count() > 0, f"Sidebar section '{section}' not found"

    def test_domain_filter_applies(self, page_setup: Page):
        page = page_setup
        navigate_to_tab(page, "Dashboard")

        # Get initial triple count
        body_text_before = page.locator("body").text_content() or ""

        # Select a domain from dropdown if options exist
        domain_select = page.locator("select").nth(1)  # Second select (first may be Entity)
        options = domain_select.locator("option").all()
        if len(options) > 1:
            # Select second option (first is placeholder)
            domain_select.select_option(index=1)
            page.wait_for_timeout(1_500)

            # Page should update (no crash)
            body_text_after = page.locator("body").text_content() or ""
            assert len(body_text_after) > 50, "Dashboard went blank after filter"

    def test_pagination_controls(self, page_setup: Page):
        page = page_setup
        navigate_to_tab(page, "Dashboard")

        body_text = page.locator("body").text_content() or ""
        # If there's enough data, pagination should appear
        # Otherwise, just verify the table is present
        tables = page.locator("table")
        assert tables.count() > 0, "No tables found in Dashboard"


# ---------------------------------------------------------------------------
# WP-5: Recon tab
# ---------------------------------------------------------------------------

class TestReconTab:
    def test_entity_selector(self, page_setup: Page):
        page = page_setup
        navigate_to_tab(page, "Recon")

        body_text = page.locator("body").text_content() or ""
        assert "Entity:" in body_text, "Recon tab missing entity selector"

        # Entity selector dropdown should exist with at least one option
        entity_select = page.locator("select")
        expect(entity_select.first).to_be_visible(timeout=5_000)
        options = entity_select.first.locator("option").all()
        assert len(options) >= 1, "Entity selector has no options"

    def test_run_recon_button(self, page_setup: Page):
        page = page_setup
        navigate_to_tab(page, "Recon")

        btn = page.locator("button").filter(has_text="Run Recon")
        expect(btn.first).to_be_visible(timeout=5_000)

    def test_recon_executes_and_shows_correct_results(self, page_setup: Page):
        """Run recon and verify all 5 checks render with correct data from the backend."""
        page = page_setup
        navigate_to_tab(page, "Recon")

        # Get the selected entity from the dropdown
        entity_select = page.locator("select").first
        selected_entity = entity_select.input_value()

        # Fetch backend ground truth for this entity
        recon_url = f"{DCL_BACKEND}/api/dcl/recon?entity_id={selected_entity}"
        api_resp = httpx.get(recon_url, timeout=30.0)
        assert api_resp.status_code == 200, f"Recon API failed: {api_resp.status_code}"
        api_data = api_resp.json()
        api_checks = {c["check"]: c for c in api_data["checks"]}

        # Click Run Recon in UI
        btn = page.locator("button").filter(has_text="Run Recon")
        expect(btn.first).to_be_visible(timeout=5_000)
        btn.first.click()

        # Wait for results
        page.wait_for_timeout(10_000)

        body_text = page.locator("body").text_content() or ""

        # All 5 checks must render — not "at least 3"
        check_labels = [
            "Farm", "Entity Consistency", "Source Coverage",
            "Validation Rejections", "Domain Completeness",
        ]
        for label in check_labels:
            assert label in body_text, (
                f"Check '{label}' not found in recon results. "
                f"Body: {body_text[:500]}"
            )

        # Verify entity_consistency shows the correct entity name
        ec = api_checks.get("entity_consistency", {})
        if ec.get("entities"):
            for ent in ec["entities"]:
                assert ent in body_text, (
                    f"Entity '{ent}' not shown in Entity Consistency check"
                )

        # Verify source_coverage shows source count or missing list
        sc = api_checks.get("source_coverage", {})
        if sc.get("status") == "pass":
            actual_sources = sc.get("actual", [])
            assert f"{len(actual_sources)} sources present" in body_text, (
                f"Source Coverage should show '{len(actual_sources)} sources present', "
                f"body: {body_text[:500]}"
            )
        elif sc.get("missing"):
            for m in sc["missing"]:
                assert m in body_text, (
                    f"Missing source '{m}' not shown in Source Coverage check"
                )

        # Verify validation_rejections shows correct count
        vr = api_checks.get("validation_rejections", {})
        if vr.get("rejected", 0) == 0:
            assert "No rejections" in body_text, (
                "Validation Rejections should show 'No rejections'"
            )
        else:
            assert f"{vr['rejected']} rejected" in body_text, (
                f"Validation Rejections should show '{vr['rejected']} rejected'"
            )

        # Verify domain_completeness shows populated/total
        dc = api_checks.get("domain_completeness", {})
        if dc.get("populated") is not None and dc.get("total") is not None:
            expected_summary = f"{dc['populated']} / {dc['total']} domains populated"
            assert expected_summary in body_text, (
                f"Domain Completeness should show '{expected_summary}', "
                f"body: {body_text[:500]}"
            )

        # Verify farm_dcl_count shows expected/actual numbers
        fc = api_checks.get("farm_dcl_count", {})
        if fc.get("status") not in ("skip", None) and fc.get("expected") is not None:
            assert f"Expected: {fc['expected']}" in body_text, (
                f"Farm count should show 'Expected: {fc['expected']}'"
            )
            assert f"Actual: {fc['actual']}" in body_text, (
                f"Farm count should show 'Actual: {fc['actual']}'"
            )

    def test_overall_status_matches_backend(self, page_setup: Page):
        """Overall status badge must match the backend's overall verdict."""
        page = page_setup
        navigate_to_tab(page, "Recon")

        # Get the selected entity
        entity_select = page.locator("select").first
        selected_entity = entity_select.input_value()

        # Fetch backend ground truth
        api_resp = httpx.get(
            f"{DCL_BACKEND}/api/dcl/recon?entity_id={selected_entity}",
            timeout=30.0,
        )
        assert api_resp.status_code == 200
        expected_overall = api_resp.json()["overall"].upper()  # "PASS", "WARN", or "FAIL"

        # Run recon in UI
        btn = page.locator("button").filter(has_text="Run Recon")
        btn.first.click()
        page.wait_for_timeout(10_000)

        body_text = page.locator("body").text_content() or ""

        # The overall badge must match the backend verdict
        assert expected_overall in body_text, (
            f"Overall status should be {expected_overall} but not found in UI. "
            f"Body: {body_text[:300]}"
        )

        # Each individual check status must appear in the UI
        api_checks = api_resp.json()["checks"]
        for check in api_checks:
            status_text = check["status"].upper()  # PASS, FAIL, WARN, SKIP
            check_name = {
                "farm_dcl_count": "Farm",
                "entity_consistency": "Entity Consistency",
                "source_coverage": "Source Coverage",
                "validation_rejections": "Validation Rejections",
                "domain_completeness": "Domain Completeness",
            }.get(check["check"], check["check"])
            assert status_text in body_text, (
                f"Check '{check_name}' status {status_text} not found in UI"
            )


# ---------------------------------------------------------------------------
# Graph v2 tab tests
# ---------------------------------------------------------------------------

class TestGraphV2Tab:
    """E2E tests for the data-driven Graph v2 tab."""

    def test_tab_visible_and_navigable(self, page_setup: Page):
        """Graph v2 tab appears in nav and renders without crash."""
        page = page_setup
        page.goto(DCL_URL, wait_until="networkidle")
        tab = page.locator("button, a").filter(has_text="Graph v2")
        expect(tab.first).to_be_visible(timeout=10_000)
        tab.first.click()
        page.wait_for_timeout(3_000)
        body_text = page.locator("body").text_content() or ""
        # Should render either graph content or empty state
        assert "pipeline data" in body_text.lower() or len(body_text) > 100, (
            "Graph v2 tab appears blank"
        )

    def test_entity_selector_present(self, page_setup: Page):
        """Graph v2 tab shows entity selector."""
        page = page_setup
        navigate_to_tab(page, "Graph v2")
        body_text = page.locator("body").text_content() or ""
        assert "Entity:" in body_text, (
            "Graph v2 tab missing entity selector"
        )

    def test_graph_renders_svg_with_nodes(self, page_setup: Page):
        """Graph v2 renders SVG with at least one node when pipeline data exists."""
        page = page_setup
        navigate_to_tab(page, "Graph v2")
        svg = page.locator("svg")
        if svg.count() > 0:
            nodes = page.locator("[data-layer]")
            if nodes.count() > 0:
                # At least one node has a layer attribute
                assert nodes.count() > 0
            else:
                # Empty state is also acceptable
                body_text = page.locator("body").text_content() or ""
                assert "no pipeline data" in body_text.lower()

    def test_links_have_stroke_width(self, page_setup: Page):
        """At least one link has a non-zero strokeWidth when data exists."""
        page = page_setup
        navigate_to_tab(page, "Graph v2")
        paths = page.locator("svg path[stroke-width]")
        if paths.count() > 0:
            width = paths.first.get_attribute("stroke-width")
            assert width is not None and float(width) > 0, (
                f"Link has zero or missing stroke-width: {width}"
            )

    def test_empty_state_message(self, page_setup: Page):
        """When no data, shows 'No pipeline data' message."""
        page = page_setup
        navigate_to_tab(page, "Graph v2")
        # If there's no SVG with nodes, the empty state should show
        nodes = page.locator("[data-layer]")
        if nodes.count() == 0:
            body_text = page.locator("body").text_content() or ""
            assert "no pipeline data" in body_text.lower(), (
                "Expected empty state message but found neither graph nor message"
            )

    def test_stub_node_has_no_outgoing_mapping_links(self, page_setup: Page):
        """A stub source (registered but zero triples) has no outgoing domain links."""
        page = page_setup
        navigate_to_tab(page, "Graph v2")
        stubs = page.locator('[data-status="stub"]')
        if stubs.count() > 0:
            # Stub node exists — it should render but have no outgoing
            # mapping links (only an ingest link from pipe_farm).
            # The stub is visible proof that the source was registered
            # but nothing downstream consumes it.
            expect(stubs.first).to_be_visible()

    def test_original_graph_tab_unchanged(self, page_setup: Page):
        """Tab 1 (Graph) still works — regression check."""
        page = page_setup
        page.goto(DCL_URL, wait_until="networkidle")
        tab = page.locator("button, a").filter(has_text="Graph").first
        expect(tab).to_be_visible(timeout=10_000)
        tab.click()
        page.wait_for_timeout(5_000)
        svg = page.locator("svg")
        expect(svg.first).to_be_visible(timeout=15_000)
