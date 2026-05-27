// Operator-visible outcome: selecting the latest snapshot in the Graph tab
// snapshot dropdown renders the data-driven Sankey with L1 fabric-plane nodes
// and L2 source nodes that match the /api/dcl/run response for that snapshot,
// and the provenance footer shows that snapshot's entity_id (no stale cache).
/**
 * Graph Render Verification — latest snapshot.
 *
 * Entity-agnostic: resolves the latest snapshot at runtime from
 * /api/dcl/snapshots, so the test does not depend on any one entity's
 * fixture data being present. The graph's node set is compared against the
 * /api/dcl/run API response captured via network intercept.
 *
 * Prerequisites: DCL backend on :8004, frontend on :3004, snapshots ingested.
 */

import { test, expect } from "playwright/test";

const DCL_URL = "http://localhost:3004";
const DCL_BACKEND = "http://localhost:8004";

interface Snapshot {
  dcl_ingest_id: string;
  snapshot_name: string | null;
  entity_id: string | null;
  run_timestamp: string;
}

test.describe.serial("Graph — latest snapshot render", () => {
  test.setTimeout(120_000);

  test("Graph tab renders topology matching the API for the latest snapshot", async ({ page }) => {
    const consoleErrors: string[] = [];
    page.on("console", (msg) => {
      if (msg.type() !== "error") return;
      const text = msg.text();
      if (text.includes("The width(-1)") || text.includes("The height(-1)")) return;
      if (text.includes("net::ERR_NAME_NOT_RESOLVED")) return;
      if (text.includes("Failed to load resource")) return;
      consoleErrors.push(text);
    });

    await page.route("**/*", (route, request) => {
      if (request.url().includes("localhost")) route.continue();
      else route.abort();
    });

    // Intercept /api/dcl/run to capture the API graph for divergence checks.
    const apiResponses: { body: any; requestBody: any }[] = [];
    await page.route("**/api/dcl/run", async (route, request) => {
      let requestBody: any = null;
      try { requestBody = request.postDataJSON(); } catch { requestBody = request.postData(); }
      try {
        const response = await route.fetch();
        const body = await response.json();
        apiResponses.push({ body, requestBody });
        await route.fulfill({ response });
      } catch {
        try { await route.continue(); } catch { /* page closed */ }
      }
    });

    // Ground truth: the latest snapshot (max run_timestamp).
    const snaps: Snapshot[] = (
      await (await page.request.get(`${DCL_BACKEND}/api/dcl/snapshots`)).json()
    ).snapshots;
    expect(snaps.length).toBeGreaterThan(0);
    const latest = snaps.reduce((a, b) =>
      (b.run_timestamp || "") > (a.run_timestamp || "") ? b : a,
    );
    expect(latest.entity_id, "latest snapshot must carry an entity_id").toBeTruthy();
    console.log(`[gt] latest snapshot = ${latest.snapshot_name} / entity ${latest.entity_id}`);

    // Clear stale cache, reload.
    await page.goto(DCL_URL, { waitUntil: "load" });
    await page.evaluate(() => localStorage.clear());
    await page.goto(DCL_URL, { waitUntil: "load" });

    const graphButton = page.locator("button").filter({ hasText: /^Graph$/ });
    await expect(graphButton.first()).toBeVisible({ timeout: 15_000 });
    await graphButton.first().click();

    // Select the latest snapshot by its dcl_ingest_id.
    const snapshotSelect = page.locator("#snapshot-selector");
    await expect(snapshotSelect).toBeVisible({ timeout: 10_000 });
    await expect(snapshotSelect.locator("option")).toHaveCount(snaps.length, { timeout: 15_000 });
    await snapshotSelect.selectOption(latest.dcl_ingest_id);

    // Wait for the Sankey SVG and its nodes.
    const svgGraph = page.locator('svg[role="img"][aria-label="Data-driven graph of DCL triple flow"]');
    await expect(svgGraph).toBeVisible({ timeout: 30_000 });
    const dataEntitiesGroup = page.locator('g[aria-label="Data entities"]');
    await expect(dataEntitiesGroup).toBeVisible({ timeout: 10_000 });
    await expect(dataEntitiesGroup.locator("g[data-node-id]").first()).toBeVisible({ timeout: 10_000 });

    // Capture the API graph for the latest snapshot.
    const latestApi = apiResponses
      .reverse()
      .find((r) => (r.body?.graph?.nodes?.length ?? 0) > 0);
    expect(latestApi, "an /api/dcl/run response with nodes must have been captured").toBeTruthy();
    const apiNodes = latestApi!.body.graph.nodes as Array<{ level: string; label: string }>;
    const apiL1 = [...new Set(apiNodes.filter((n) => n.level === "L1").map((n) => n.label.toLowerCase()))];
    const apiL2 = [...new Set(apiNodes.filter((n) => n.level === "L2").map((n) => n.label.toLowerCase()))];
    console.log(`[api] L1=${apiL1.join(",")} L2=${apiL2.join(",")}`);
    expect(apiL1.length, "API must return at least one L1 fabric plane").toBeGreaterThan(0);

    // The rendered DOM nodes must contain every L1 and L2 label the API returned.
    const domTexts = (await dataEntitiesGroup.locator("text").allTextContents()).map((t) =>
      t.toLowerCase().trim().replace(/_/g, " "),
    );
    for (const label of [...apiL1, ...apiL2]) {
      const normalized = label.replace(/_/g, " ");
      expect(
        domTexts.some((t) => t.includes(normalized)),
        `API node "${label}" must render in the graph DOM. DOM nodes: [${domTexts.join(", ")}]`,
      ).toBe(true);
    }

    // Provenance footer shows the latest snapshot's entity_id.
    const provenanceFooter = page.locator("span.font-mono");
    await expect(provenanceFooter.first()).toContainText(latest.entity_id!, { timeout: 15_000 });

    await page.screenshot({ path: "tests/e2e/artifacts/graph_v2_latest.png", fullPage: true });
    await page.unrouteAll({ behavior: "ignoreErrors" });

    expect(consoleErrors, `console errors: ${consoleErrors.join("; ")}`).toHaveLength(0);
  });
});
