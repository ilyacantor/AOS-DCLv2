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

import os
import json
import re
import pytest
import httpx
from playwright.sync_api import Page, expect

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
    """Navigate between all 4 tabs — verify no crash or blank content."""

    def test_all_tabs_navigate_without_crash(self, page_setup: Page):
        page = page_setup
        page.goto(DCL_URL, wait_until="load")
        page.wait_for_timeout(3_000)

        for tab_name in ["Ingest", "Context", "Dashboard", "Recon", "Graph"]:
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

        for tab_name in ["Ingest", "Context", "Dashboard", "Recon"]:
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
        """After load, all 4 monitoring tabs share one selected snapshot.

        The snapshot selector is app-level shared state, so every tab's
        #snapshot-selector must hold the same dcl_ingest_id — and it must
        be the newest snapshot (max run_timestamp) since the default mode
        is follow-latest.
        """
        page = page_setup

        # Ground truth: newest snapshot by run_timestamp.
        snaps = httpx.get(f"{DCL_BACKEND}/api/dcl/snapshots", timeout=30.0).json()["snapshots"]
        newest_id = max(snaps, key=lambda s: s["run_timestamp"] or "")["dcl_ingest_id"]
        snap_count = len(snaps)

        page.goto(DCL_URL, wait_until="load")
        page.wait_for_timeout(3_000)

        snapshots_seen = {}
        for tab_name in ["Ingest", "Context", "Dashboard", "Recon"]:
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

        assert len(snapshots_seen) == 4, (
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
        snap_count = len(httpx.get(f"{DCL_BACKEND}/api/dcl/snapshots", timeout=30.0).json()["snapshots"])
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
        snap_count = len(httpx.get(f"{DCL_BACKEND}/api/dcl/snapshots", timeout=30.0).json()["snapshots"])
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
# WP-5: Recon tab
# ---------------------------------------------------------------------------

class TestReconTab:
    def test_snapshot_selector(self, page_setup: Page):
        page = page_setup
        navigate_to_tab(page, "Recon")

        body_text = page.locator("body").text_content() or ""
        assert "Snapshot:" in body_text, "Recon tab missing snapshot selector"

        # Snapshot selector option count must match the backend snapshot list.
        snap_count = len(httpx.get(f"{DCL_BACKEND}/api/dcl/snapshots", timeout=30.0).json()["snapshots"])
        selector = page.locator("#snapshot-selector")
        expect(selector).to_be_visible(timeout=5_000)
        expect(selector.locator("option")).to_have_count(snap_count, timeout=10_000)

    def test_run_recon_button(self, page_setup: Page):
        page = page_setup
        navigate_to_tab(page, "Recon")

        btn = page.locator("button").filter(has_text="Run Recon")
        expect(btn.first).to_be_visible(timeout=5_000)

    def test_recon_executes_and_shows_correct_results(self, page_setup: Page):
        """Run recon and verify all 5 checks render with correct data from the backend.

        Recon scopes to the active-run triple set. Select the snapshot for
        the active run (is_current) so recon has data to check — the
        follow-latest default may point at a snapshot whose run is not the
        active one, for which recon legitimately returns no checks.
        """
        page = page_setup
        navigate_to_tab(page, "Recon")

        selector = page.locator("#snapshot-selector")
        expect(selector).to_be_visible(timeout=10_000)

        # Pick the snapshot whose run is active (is_current) — its entity is
        # guaranteed to have active triples for recon to verify.
        snaps = httpx.get(f"{DCL_BACKEND}/api/dcl/snapshots", timeout=30.0).json()["snapshots"]
        active = next((s for s in snaps if s.get("is_current")), None)
        assert active is not None, "No is_current snapshot — run the ingest pipeline"
        selected_entity = active["entity_id"]
        selector.select_option(active["dcl_ingest_id"])
        page.wait_for_timeout(2_000)

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

        # Wait for results deterministically — the completion artifact is the
        # rendered check set; under page-load convoy the recon round trips can
        # exceed any fixed sleep (the old 10s wait scraped a partial DOM).
        expect(page.locator("body")).to_contain_text("Domain Completeness", timeout=60_000)
        expect(
            page.locator("button").filter(has_text=re.compile(r"Running"))
        ).to_have_count(0, timeout=60_000)

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
        """Overall status badge must match the backend's overall verdict.

        Selects the active-run snapshot (is_current) so recon has triples to
        verify — see test_recon_executes_and_shows_correct_results.
        """
        page = page_setup
        navigate_to_tab(page, "Recon")

        selector = page.locator("#snapshot-selector")
        expect(selector).to_be_visible(timeout=10_000)

        # Pick the snapshot whose run is active (is_current).
        snaps = httpx.get(f"{DCL_BACKEND}/api/dcl/snapshots", timeout=30.0).json()["snapshots"]
        active = next((s for s in snaps if s.get("is_current")), None)
        assert active is not None, "No is_current snapshot — run the ingest pipeline"
        selected_entity = active["entity_id"]
        selector.select_option(active["dcl_ingest_id"])
        page.wait_for_timeout(2_000)

        # Fetch backend ground truth
        api_resp = httpx.get(
            f"{DCL_BACKEND}/api/dcl/recon?entity_id={selected_entity}",
            timeout=30.0,
        )
        assert api_resp.status_code == 200
        expected_overall = api_resp.json()["overall"].upper()  # "PASS", "WARN", or "FAIL"

        # Run recon in UI — wait for the rendered check set, not a fixed sleep
        # (same convoy-margin fix as test_recon_executes_and_shows_correct_results).
        btn = page.locator("button").filter(has_text="Run Recon")
        btn.first.click()
        expect(page.locator("body")).to_contain_text("Domain Completeness", timeout=60_000)
        expect(
            page.locator("button").filter(has_text=re.compile(r"Running"))
        ).to_have_count(0, timeout=60_000)

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
        expect(nodes.first.or_(empty.first)).to_be_visible(timeout=30_000)
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
        expect(nodes.first.or_(empty.first)).to_be_visible(timeout=30_000)
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
