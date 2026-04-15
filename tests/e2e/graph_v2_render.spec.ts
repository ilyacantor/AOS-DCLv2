/**
 * Graph v2 Render Verification — NetCorp-G19H
 *
 * Verifies the Graph v2 tab renders correct topology for the selected entity.
 * Checks under test:
 *   1. L1 fabric plane nodes match API (expected: 4 — ipaas, api_gateway, data_warehouse, event_bus)
 *   2. L2 source nodes match API (expected: 5 key sources including kafka for event_bus)
 *   3. Provenance footer matches selected entity (NetCorp-G19H, not stale cache)
 *
 * Prerequisites:
 *   - DCL backend on :8004, frontend on :3004
 *   - NetCorp-G19H triples loaded
 */

import { test, expect } from "playwright/test";

const DCL_URL = "http://localhost:3004";
const DCL_BACKEND = "http://localhost:8004";

const EXPECTED_L1_LABELS = ["ipaas", "api_gateway", "data_warehouse", "event_bus"];
const EXPECTED_L2_LABELS = ["netsuite", "datadog", "jira", "aws_cost_explorer", "kafka"];
const EXPECTED_PROVENANCE = "NetCorp-G19H";
const STALE_PROVENANCE = "HelixHub-AEEU";

test.describe.serial("Graph v2 — NetCorp-G19H Render", () => {
  test.setTimeout(90_000);

  test("Graph v2 renders correct topology for NetCorp-G19H", async ({ page }) => {
    const consoleErrors: string[] = [];
    page.on("console", (msg) => {
      if (msg.type() === "error") {
        const text = msg.text();
        // Filter infrastructure noise
        if (text.includes("The width(-1)") || text.includes("The height(-1)")) return;
        if (text.includes("net::ERR_NAME_NOT_RESOLVED")) return;
        if (text.includes("Failed to load resource")) return;
        consoleErrors.push(text);
      }
    });

    // Block external requests
    await page.route("**/*", (route, request) => {
      if (request.url().includes("localhost")) {
        route.continue();
      } else {
        route.abort();
      }
    });

    // Intercept /api/dcl/run calls to capture API response (Step 2)
    const apiResponses: { url: string; body: any; requestBody: any }[] = [];
    await page.route("**/api/dcl/run", async (route, request) => {
      const url = request.url();
      let requestBody: any = null;
      try { requestBody = request.postDataJSON(); } catch { requestBody = request.postData(); }
      try {
        const response = await route.fetch();
        const body = await response.json();
        apiResponses.push({ url, body, requestBody });
        await route.fulfill({ response });
      } catch {
        // Page closed before response — ignore
        try { await route.continue(); } catch { /* already closed */ }
      }
    });

    // Clear localStorage to remove stale cache
    await page.goto(DCL_URL, { waitUntil: "load" });
    await page.evaluate(() => localStorage.clear());

    // Reload after clearing cache — forces fresh API call
    await page.goto(DCL_URL, { waitUntil: "load" });

    // Wait for nav to render
    const graphV2Button = page.locator("button").filter({ hasText: /^Graph$/ });
    await expect(graphV2Button).toBeVisible({ timeout: 15_000 });

    // Navigate to Graph v2 tab
    await graphV2Button.click();

    // Wait for entity selector to populate
    const entitySelect = page.locator("select");
    await expect(entitySelect).toBeVisible({ timeout: 10_000 });

    // Select NetCorp-G19H entity by value
    await entitySelect.selectOption("NetCorp-G19H");

    // Wait for SVG with Sankey graph to render (not just network idle)
    const svgGraph = page.locator('svg[role="img"][aria-label="Data-driven graph of DCL triple flow"]');
    await expect(svgGraph).toBeVisible({ timeout: 30_000 });

    // Wait for nodes to appear in the DOM
    const dataEntitiesGroup = page.locator('g[aria-label="Data entities"]');
    await expect(dataEntitiesGroup).toBeVisible({ timeout: 10_000 });

    // Wait for at least one node to render
    const nodeGroups = dataEntitiesGroup.locator("g[data-node-id]");
    await expect(nodeGroups.first()).toBeVisible({ timeout: 10_000 });

    // ===== Step 2: Network intercept — print divergence table =====
    for (const resp of apiResponses) {
      const apiGraph = resp.body?.graph || {};
      const apiNodes = apiGraph.nodes || [];
      const apiMeta = apiGraph.meta || {};
      console.log("=== API Response ===");
      console.log(`Request body: ${JSON.stringify(resp.requestBody)}`);
      console.log(`API snapshotName: ${apiMeta.snapshotName}`);
      console.log(`API node count: ${apiNodes.length}`);
      console.log(`API L1 nodes: ${apiNodes.filter((n: any) => n.level === "L1").map((n: any) => n.label)}`);
      console.log(`API L2 nodes: ${apiNodes.filter((n: any) => n.level === "L2").map((n: any) => n.label)}`);
      if (resp.body?.detail) console.log(`API ERROR: ${resp.body.detail}`);
    }
    if (apiResponses.length === 0) {
      console.log("=== NO /api/dcl/run calls intercepted ===");
    }

    // ===== Assertions =====

    // Collect all node labels from the DOM — normalize underscores to spaces for matching
    const allNodeTexts = await dataEntitiesGroup.locator("text").allTextContents();
    const normalizedNodeTexts = allNodeTexts.map(t => t.toLowerCase().trim().replace(/_/g, " "));

    // 1. Assert all 4 L1 fabric plane nodes present
    const matchedL1 = EXPECTED_L1_LABELS.filter(expected => {
      const normalized = expected.replace(/_/g, " ");
      return normalizedNodeTexts.some(t => t.includes(normalized));
    });
    expect(
      matchedL1,
      `Expected ${EXPECTED_L1_LABELS.length} L1 fabric nodes (${EXPECTED_L1_LABELS.join(", ")}) but found: ${matchedL1.join(", ")}. ` +
      `All DOM node texts: [${normalizedNodeTexts.join(", ")}]`
    ).toHaveLength(EXPECTED_L1_LABELS.length);

    // 2. Assert all expected L2 source nodes present
    const matchedL2 = EXPECTED_L2_LABELS.filter(expected => {
      const normalized = expected.replace(/_/g, " ");
      return normalizedNodeTexts.some(t => t.includes(normalized));
    });
    expect(
      matchedL2,
      `Expected ${EXPECTED_L2_LABELS.length} L2 source nodes (${EXPECTED_L2_LABELS.join(", ")}) but found: ${matchedL2.join(", ")}. ` +
      `All DOM node texts: [${normalizedNodeTexts.join(", ")}]`
    ).toHaveLength(EXPECTED_L2_LABELS.length);

    // 3. Assert provenance footer matches NetCorp-G19H
    const provenanceFooter = page.locator("span.font-mono").filter({ hasText: /NetCorp|HelixHub|CoreHub/i });
    const footerTexts = await provenanceFooter.allTextContents();
    const allFooterText = footerTexts.join(" ");

    expect(
      allFooterText,
      `Provenance footer should contain "${EXPECTED_PROVENANCE}" but got: "${allFooterText}"`
    ).toContain(EXPECTED_PROVENANCE);

    expect(
      allFooterText,
      `Provenance footer should NOT contain stale "${STALE_PROVENANCE}" but got: "${allFooterText}"`
    ).not.toContain(STALE_PROVENANCE);

    // Capture screenshot
    await page.screenshot({
      path: "tests/e2e/artifacts/graph_v2_netcorp.png",
      fullPage: true,
    });

    // Cleanup routes
    await page.unrouteAll({ behavior: "ignoreErrors" });
  });
});
