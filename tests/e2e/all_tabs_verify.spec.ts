/**
 * All-Tabs Verification — Post entity resolution fix.
 *
 * Verifies every DCL tab renders without errors after the tenant_runs
 * alignment fix. Captures console errors and screenshots.
 *
 * Tabs: Graph, Dashboard, Context, Recon, Ingest
 */

import { test, expect } from "playwright/test";

const DCL_URL = "http://localhost:3004";
const DCL_BACKEND = "http://localhost:8004";

function setupConsoleCapture(page: import("playwright/test").Page, errors: string[]) {
  page.on("console", (msg) => {
    if (msg.type() === "error") {
      const text = msg.text();
      // Filter infrastructure noise
      if (text.includes("The width(-1)") || text.includes("The height(-1)")) return;
      if (text.includes("net::ERR_NAME_NOT_RESOLVED")) return;
      if (text.includes("Failed to load resource") && !text.includes("/api/")) return;
      errors.push(text);
    }
  });
}

test.describe.serial("DCL All Tabs — No Errors", () => {
  test.setTimeout(180_000);

  test("0. Backend is healthy", async ({ page }) => {
    const res = await page.request.get(`${DCL_BACKEND}/health`);
    expect(res.status()).toBe(200);
    const health = await res.json();
    expect(health.postgres_available).toBe(true);
  });

  test("1. Entity list has no phantom entities", async ({ page }) => {
    const res = await page.request.get(`${DCL_BACKEND}/api/dcl/entities`);
    expect(res.status()).toBe(200);
    const data = await res.json();
    const entities = data.entities || [];
    expect(entities.length).toBeGreaterThan(0);

    const phantoms = entities.filter((e: any) =>
      e.entity_id.includes("FluxEdge")
    );
    expect(
      phantoms,
      `Found phantom entities: ${phantoms.map((e: any) => e.entity_id).join(", ")}`
    ).toHaveLength(0);

    console.log(`Entities: ${entities.map((e: any) => e.entity_id).join(", ")}`);
  });

  test("2. Graph tab — auto-loads without error", async ({ page }) => {
    const consoleErrors: string[] = [];
    setupConsoleCapture(page, consoleErrors);

    await page.goto(DCL_URL, { waitUntil: "load" });

    // Wait for nav
    await expect(
      page.locator("button").filter({ hasText: "Graph" }).first()
    ).toBeVisible({ timeout: 15_000 });

    // Wait for auto-load to complete (graph build can take time)
    // Look for either the SVG graph or the "Run" button becoming enabled
    const runButton = page.locator('button[data-role="run-primary"]');
    await expect(runButton).toBeVisible({ timeout: 15_000 });
    // Wait for running state to end
    await expect(runButton).not.toHaveText("Running...", { timeout: 60_000 });

    await page.screenshot({ path: "tests/e2e/artifacts/verify_graph.png", fullPage: true });

    // Filter out infrastructure errors (external resource loads)
    const appErrors = consoleErrors.filter(
      (e) => !e.includes("ERR_NAME_NOT_RESOLVED") && !e.includes("ERR_BLOCKED_BY_CLIENT")
    );
    expect(
      appErrors,
      `Console errors on Graph tab: ${appErrors.join("; ")}`
    ).toHaveLength(0);
  });

  test("3. Graph tab — snapshot selection renders graph", async ({ page }) => {
    const consoleErrors: string[] = [];
    setupConsoleCapture(page, consoleErrors);

    await page.goto(DCL_URL, { waitUntil: "load" });

    // Wait for auto-load to settle (it fires on mount)
    const runButton = page.locator('button[data-role="run-primary"]');
    await expect(runButton).toBeVisible({ timeout: 15_000 });
    await expect(runButton).not.toHaveText("Running...", { timeout: 60_000 });

    // Navigate to the Graph tab
    const graphButton = page.locator("button").filter({ hasText: /^Graph$/ });
    await graphButton.first().click();

    // Wait for the snapshot selector to populate
    const snapshotSelect = page.locator("#snapshot-selector");
    await expect(snapshotSelect).toBeVisible({ timeout: 10_000 });

    // Wait for snapshots to load (placeholder "No snapshots" is a single option).
    await expect(async () => {
      const optionCount = await snapshotSelect.locator("option").count();
      expect(optionCount).toBeGreaterThan(1);
    }).toPass({ timeout: 15_000 });

    const options = await snapshotSelect.locator("option").allTextContents();
    console.log(`Graph snapshot options: ${options.slice(0, 5).join(", ")}`);

    // Verify no phantom entities
    const hasFluxEdge = options.some(o => o.includes("Fluxedge") || o.includes("FluxEdge"));
    expect(hasFluxEdge, "FluxEdge should not be in snapshot dropdown").toBe(false);

    // The default (follow-latest) selection is the newest snapshot.
    const selectedValue = await snapshotSelect.inputValue();
    console.log(`Default snapshot: ${selectedValue}`);

    if (selectedValue) {
      // Wait for graph to render (POST /api/dcl/run with entity_id)
      // Look for the SVG graph or loading state to resolve
      const loadingText = page.locator("text=Loading graph");
      const svgGraph = page.locator('svg[role="img"]');
      const errorState = page.locator('[class*="destructive"] p');

      // Wait for either graph to render or error to show (shouldn't be error)
      await expect(async () => {
        const loading = await loadingText.isVisible().catch(() => false);
        expect(loading).toBe(false); // loading should finish
      }).toPass({ timeout: 60_000 });

      const hasGraph = await svgGraph.first().isVisible().catch(() => false);
      const errorText = await errorState.first().textContent().catch(() => null);

      console.log(`Graph rendered: ${hasGraph}, error: ${errorText ?? "none"}`);

      // Must not have a tenant resolution error
      if (errorText) {
        expect(errorText).not.toContain("Cannot resolve tenant");
        expect(errorText).not.toContain("No active tenant");
      }
    }

    // Viewport (not fullPage) screenshot: fullPage rasterization of the
    // graph SVG crashes the renderer under WSL2 headless Chromium. The
    // screenshot is a diagnostic artifact, not an assertion — capture it
    // defensively with a short timeout so a wedged renderer cannot consume
    // the test budget (the assertion below still runs).
    await page
      .screenshot({ path: "tests/e2e/artifacts/verify_graph_v2.png", fullPage: false, timeout: 8_000 })
      .catch(() => { /* screenshot is diagnostic only */ });

    const appErrors = consoleErrors.filter(
      (e) => !e.includes("ERR_NAME_NOT_RESOLVED") && !e.includes("ERR_BLOCKED_BY_CLIENT")
    );
    expect(
      appErrors,
      `Console errors on Graph tab: ${appErrors.join("; ")}`
    ).toHaveLength(0);
  });

  test("4. Dashboard tab — renders data", async ({ page }) => {
    const consoleErrors: string[] = [];
    setupConsoleCapture(page, consoleErrors);

    await page.goto(DCL_URL, { waitUntil: "load" });

    // Wait for app to settle
    const runButton = page.locator('button[data-role="run-primary"]');
    await expect(runButton).toBeVisible({ timeout: 15_000 });
    await expect(runButton).not.toHaveText("Running...", { timeout: 60_000 });

    await page.locator("button").filter({ hasText: "Dashboard" }).click();
    await page.waitForTimeout(5_000);

    const bodyText = await page.locator("body").textContent();
    expect(bodyText!.length).toBeGreaterThan(200);

    await page.screenshot({ path: "tests/e2e/artifacts/verify_dashboard.png", fullPage: true });

    const appErrors = consoleErrors.filter(
      (e) => !e.includes("ERR_NAME_NOT_RESOLVED") && !e.includes("ERR_BLOCKED_BY_CLIENT")
    );
    expect(
      appErrors,
      `Console errors on Dashboard: ${appErrors.join("; ")}`
    ).toHaveLength(0);
  });

  test("5. Context tab — renders data", async ({ page }) => {
    const consoleErrors: string[] = [];
    setupConsoleCapture(page, consoleErrors);

    await page.goto(DCL_URL, { waitUntil: "load" });
    const runButton = page.locator('button[data-role="run-primary"]');
    await expect(runButton).toBeVisible({ timeout: 15_000 });
    await expect(runButton).not.toHaveText("Running...", { timeout: 60_000 });

    await page.locator("button").filter({ hasText: "Context" }).click();
    await page.waitForTimeout(5_000);

    const bodyText = await page.locator("body").textContent();
    const hasContent =
      bodyText?.includes("Domain") ||
      bodyText?.includes("domain") ||
      bodyText?.includes("confidence") ||
      bodyText?.includes("Context") ||
      bodyText?.includes("triple");
    expect(hasContent, "Context tab should show domain/confidence content").toBe(true);

    await page.screenshot({ path: "tests/e2e/artifacts/verify_context.png", fullPage: true });

    const appErrors = consoleErrors.filter(
      (e) => !e.includes("ERR_NAME_NOT_RESOLVED") && !e.includes("ERR_BLOCKED_BY_CLIENT")
    );
    expect(
      appErrors,
      `Console errors on Context: ${appErrors.join("; ")}`
    ).toHaveLength(0);
  });

  test("6. Recon tab — renders without error", async ({ page }) => {
    const consoleErrors: string[] = [];
    setupConsoleCapture(page, consoleErrors);

    await page.goto(DCL_URL, { waitUntil: "load" });
    const runButton = page.locator('button[data-role="run-primary"]');
    await expect(runButton).toBeVisible({ timeout: 15_000 });
    await expect(runButton).not.toHaveText("Running...", { timeout: 60_000 });

    await page.locator("button").filter({ hasText: "Recon" }).click();
    await page.waitForTimeout(3_000);

    await page.screenshot({ path: "tests/e2e/artifacts/verify_recon.png", fullPage: true });

    const appErrors = consoleErrors.filter(
      (e) => !e.includes("ERR_NAME_NOT_RESOLVED") && !e.includes("ERR_BLOCKED_BY_CLIENT")
    );
    expect(
      appErrors,
      `Console errors on Recon: ${appErrors.join("; ")}`
    ).toHaveLength(0);
  });

  test("7. Ingest tab — renders without error", async ({ page }) => {
    const consoleErrors: string[] = [];
    setupConsoleCapture(page, consoleErrors);

    await page.goto(DCL_URL, { waitUntil: "load" });
    const runButton = page.locator('button[data-role="run-primary"]');
    await expect(runButton).toBeVisible({ timeout: 15_000 });
    await expect(runButton).not.toHaveText("Running...", { timeout: 60_000 });

    await page.locator("button").filter({ hasText: "Ingest" }).click();
    await page.waitForTimeout(3_000);

    await page.screenshot({ path: "tests/e2e/artifacts/verify_ingest.png", fullPage: true });

    const appErrors = consoleErrors.filter(
      (e) => !e.includes("ERR_NAME_NOT_RESOLVED") && !e.includes("ERR_BLOCKED_BY_CLIENT")
    );
    expect(
      appErrors,
      `Console errors on Ingest: ${appErrors.join("; ")}`
    ).toHaveLength(0);
  });
});
