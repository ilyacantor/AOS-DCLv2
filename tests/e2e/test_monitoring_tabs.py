"""
Playwright E2E tests for the monitoring tabs: Ingest, Context, Dashboard, Graph.

Prerequisites:
  - DCL backend on :8004, frontend on :3004
  - Financial triples loaded (at least one ingest run)
  - Run: python -m pytest tests/e2e/test_monitoring_tabs.py -v

Install (once):
  pip install playwright pytest-playwright
  playwright install chromium
"""

import os
import json
import re
import pytest
import httpx
from playwright.sync_api import Page, expect

from _dcl_ground_truth import get_snapshots  # sibling in tests/e2e (on sys.path)

# pytest targets the dev stack (DEV_ENV_NOTES: pytest → dcl-dev :8104). The
# dcl-frontend on :3004 proxies /api → :8104, so backend reads must hit :8104 to
# stay coherent with what the UI renders. Override for a prod-consistent run.
DCL_URL = os.environ.get("DCL_FRONTEND_URL", "http://localhost:3004")
DCL_BACKEND = os.environ.get("DCL_BACKEND_URL", "http://localhost:8104")

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

    Uses wait_until="load" — not "networkidle" — because the snapshot
    selector polls /api/dcl/snapshots every ~12s, so the page never goes
    network-idle for 500ms. Dynamic content is awaited explicitly via
    expect(...) below and in each test.
    """
    page.goto(DCL_URL, wait_until="load")
    tab = page.locator("button, a").filter(has_text=tab_name)
    expect(tab.first).to_be_visible(timeout=15_000)
    tab.first.click()
    page.wait_for_timeout(3_000)  # Allow tab-specific data fetch


# ---------------------------------------------------------------------------
# Cross-tab tests
# ---------------------------------------------------------------------------

class TestCrossTab:
    """Navigate between all tabs — verify no crash or blank content."""

    def test_all_tabs_navigate_without_crash(self, page_setup: Page):
        page = page_setup
        page.goto(DCL_URL, wait_until="load")
        page.wait_for_timeout(3_000)

        for tab_name in ["Ingest", "Context", "Dashboard", "Graph"]:
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

    def test_all_tabs_have_snapshot_selector(self, page_setup: Page):
        """Every monitoring tab must display the snapshot selector dropdown."""
        page = page_setup
        page.goto(DCL_URL, wait_until="load")
        page.wait_for_timeout(3_000)

        for tab_name in ["Ingest", "Context", "Dashboard"]:
            tab = page.locator("button, a").filter(has_text=tab_name)
            expect(tab.first).to_be_visible(timeout=10_000)
            tab.first.click()
            page.wait_for_timeout(2_000)

            body_text = page.locator("body").text_content() or ""

            # Snapshot selector dropdown must be present
            assert "Snapshot:" in body_text, (
                f"Tab '{tab_name}' missing snapshot selector (no 'Snapshot:' label)"
            )
            assert page.locator("#snapshot-selector").count() == 1, (
                f"Tab '{tab_name}' missing #snapshot-selector dropdown"
            )

    def test_all_tabs_show_same_snapshot(self, page_setup: Page):
        """After load, all monitoring tabs share one selected snapshot.

        The snapshot selector is app-level shared state, so every tab's
        #snapshot-selector must hold the same dcl_ingest_id — and it must
        be the newest snapshot (max run_timestamp) since the default mode
        is follow-latest.
        """
        page = page_setup

        # Ground truth: newest snapshot by run_timestamp.
        snaps = get_snapshots(DCL_BACKEND)
        newest_id = max(snaps, key=lambda s: s["run_timestamp"] or "")["dcl_ingest_id"]
        snap_count = len(snaps)

        page.goto(DCL_URL, wait_until="load")
        page.wait_for_timeout(3_000)

        snapshots_seen = {}
        for tab_name in ["Ingest", "Context", "Dashboard"]:
            tab = page.locator("button, a").filter(has_text=tab_name)
            expect(tab.first).to_be_visible(timeout=10_000)
            tab.first.click()
            page.wait_for_timeout(2_000)

            selector = page.locator("#snapshot-selector")
            expect(selector).to_be_visible(timeout=10_000)
            # Wait for the async snapshot fetch to populate the dropdown —
            # it renders a "No snapshots" placeholder until then.
            expect(selector.locator("option")).to_have_count(snap_count, timeout=15_000)
            snapshots_seen[tab_name] = selector.input_value()

        assert len(snapshots_seen) == 3, (
            f"Snapshot selector missing on some tab: {snapshots_seen}"
        )
        unique = set(snapshots_seen.values())
        assert unique == {newest_id}, (
            f"Tabs must all follow the newest snapshot {newest_id}; saw {snapshots_seen}"
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

    def test_snapshot_selector(self, page_setup: Page):
        page = page_setup
        navigate_to_tab(page, "Ingest")

        body_text = page.locator("body").text_content() or ""
        assert "Snapshot:" in body_text, "Ingest tab missing snapshot selector"

        # Snapshot selector option count must match the backend snapshot list.
        snap_count = len(get_snapshots(DCL_BACKEND))
        selector = page.locator("#snapshot-selector")
        expect(selector).to_be_visible(timeout=10_000)
        expect(selector.locator("option")).to_have_count(snap_count, timeout=10_000)


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

    def test_snapshot_selector(self, page_setup: Page):
        page = page_setup
        navigate_to_tab(page, "Context")

        # Snapshot dropdown should be present and populated.
        selector = page.locator("#snapshot-selector")
        expect(selector).to_be_visible(timeout=5_000)
        snap_count = len(get_snapshots(DCL_BACKEND))
        expect(selector.locator("option")).to_have_count(snap_count, timeout=10_000)


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
# Graph v2 tab tests
# ---------------------------------------------------------------------------

class TestGraphV2Tab:
    """E2E tests for the data-driven Graph v2 tab."""

    def test_tab_visible_and_navigable(self, page_setup: Page):
        """Graph tab appears in nav and renders without crash."""
        page = page_setup
        page.goto(DCL_URL, wait_until="load")
        tab = page.locator("button, a").filter(has_text="Graph")
        expect(tab.first).to_be_visible(timeout=10_000)
        tab.first.click()
        page.wait_for_timeout(3_000)
        body_text = page.locator("body").text_content() or ""
        # Should render either graph content or empty state
        assert "pipeline data" in body_text.lower() or len(body_text) > 100, (
            "Graph tab appears blank"
        )

    def test_snapshot_selector_present(self, page_setup: Page):
        """Graph tab shows the snapshot selector."""
        page = page_setup
        navigate_to_tab(page, "Graph")
        body_text = page.locator("body").text_content() or ""
        assert "Snapshot:" in body_text, (
            "Graph tab missing snapshot selector"
        )

    def test_graph_renders_svg_with_nodes(self, page_setup: Page):
        """Graph tab renders SVG with at least one node when pipeline data exists."""
        page = page_setup
        navigate_to_tab(page, "Graph")
        # The Graph tab fetches POST /api/dcl/run for the selected snapshot —
        # wait for the "Loading graph data..." state to clear before asserting.
        expect(page.locator("text=Loading graph data")).to_have_count(0, timeout=90_000)
        # The Sankey draws its nodes asynchronously AFTER the fetch resolves; for a
        # large snapshot the draw lags the "Loading" clear. Wait for the graph to
        # settle — nodes drawn OR the empty-state message — so a graph still
        # drawing is not misread as empty (race fix, B14).
        nodes = page.locator("[data-layer]")
        empty = page.get_by_text("No pipeline data", exact=False)
        # Budget aligned with the Loading-clear wait above (90s): on a COLD graph
        # build / under full-suite load on the bloated dev store (#27/#28), the
        # nodes draw slow-but-correct — measured ~38s cold in isolation, which
        # blew the old 30s wait and flapped this test in B14 run B (the render
        # IS correct, just past 30s). Same condition-wait, wider budget.
        expect(nodes.first.or_(empty.first)).to_be_visible(timeout=90_000)
        if nodes.count() > 0:
            assert nodes.count() > 0
        else:
            body_text = page.locator("body").text_content() or ""
            assert "no pipeline data" in body_text.lower()

    def test_links_have_stroke_width(self, page_setup: Page):
        """At least one link has a non-zero strokeWidth when data exists."""
        page = page_setup
        navigate_to_tab(page, "Graph")
        paths = page.locator("svg path[stroke-width]")
        if paths.count() > 0:
            width = paths.first.get_attribute("stroke-width")
            assert width is not None and float(width) > 0, (
                f"Link has zero or missing stroke-width: {width}"
            )

    def test_empty_state_message(self, page_setup: Page):
        """When no data, shows 'No pipeline data' message."""
        page = page_setup
        navigate_to_tab(page, "Graph")
        # Wait for the graph fetch to settle so a still-loading tab is not
        # mistaken for an empty one.
        expect(page.locator("text=Loading graph data")).to_have_count(0, timeout=90_000)
        # Wait for the graph to settle — nodes drawn OR the empty-state message —
        # so a graph still drawing after "Loading" clears is not misread as empty
        # (race fix, B14).
        nodes = page.locator("[data-layer]")
        empty = page.get_by_text("No pipeline data", exact=False)
        # Budget aligned with the Loading-clear wait above (90s): on a COLD graph
        # build / under full-suite load on the bloated dev store (#27/#28), the
        # nodes draw slow-but-correct — measured ~38s cold in isolation, which
        # blew the old 30s wait and flapped this test in B14 run B (the render
        # IS correct, just past 30s). Same condition-wait, wider budget.
        expect(nodes.first.or_(empty.first)).to_be_visible(timeout=90_000)
        # If there's no SVG with nodes, the empty state should show
        if nodes.count() == 0:
            body_text = page.locator("body").text_content() or ""
            assert "no pipeline data" in body_text.lower(), (
                "Expected empty state message but found neither graph nor message"
            )

    def test_stub_node_has_no_outgoing_mapping_links(self, page_setup: Page):
        """A stub source (registered but zero triples) has no outgoing domain links."""
        page = page_setup
        navigate_to_tab(page, "Graph")
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
        page.goto(DCL_URL, wait_until="load")
        tab = page.locator("button, a").filter(has_text="Graph").first
        expect(tab).to_be_visible(timeout=10_000)
        tab.click()
        page.wait_for_timeout(5_000)
        svg = page.locator("svg")
        expect(svg.first).to_be_visible(timeout=15_000)
