/**
 * Graph v2 — L3 no-orphans visual gate (B17).
 *
 * Hits the live DCL frontend, selects the currently-active entity, and
 * asserts two things the user actually sees:
 *   1. Sankey SVG renders.
 *   2. Every L3 node in the intercepted /api/dcl/run payload has at
 *      least one outgoing consumption link into an L4 persona node.
 *
 * The pytest regression at backend/tests/test_graph_v2_no_orphans.py is
 * the fast gate; this spec is the B17 accountability gate — proves the
 * correct payload actually lands in the browser.
 *
 * Entity is resolved at runtime from /api/dcl/entities so the test
 * stays valid across pipeline reruns.
 */
import { test, expect } from "playwright/test";

const FRONTEND = "http://localhost:3004";
const BACKEND = "http://localhost:8004";

test.describe.serial("Graph v2 — L3 orphan gate", () => {
  test.setTimeout(120_000);

  test("no L3 orphans render for the live entity", async ({ page, request }) => {
    // The Graph tab is driven by the snapshot selector. Resolve the latest
    // snapshot from the backend — its dcl_ingest_id is what the dropdown
    // holds, its entity_id is what the graph renders.
    const snapResp = await request.get(`${BACKEND}/api/dcl/snapshots`);
    expect(snapResp.ok()).toBeTruthy();
    const { snapshots } = await snapResp.json();
    expect(snapshots?.length, "DCL has zero snapshots — run a pipeline").toBeGreaterThan(0);
    const latest = snapshots.reduce((a: any, b: any) =>
      (b.run_timestamp || "") > (a.run_timestamp || "") ? b : a,
    );
    const dclIngestId = latest.dcl_ingest_id as string;
    const entityId = latest.entity_id as string;

    // Capture the payload the frontend actually receives.
    const runPayloads: any[] = [];
    await page.route("**/api/dcl/run", async (route) => {
      const resp = await route.fetch();
      try {
        const body = await resp.json();
        runPayloads.push(body);
      } catch {}
      await route.fulfill({ response: resp });
    });

    await page.goto(FRONTEND, { waitUntil: "load" });
    await page.evaluate(() => localStorage.clear());
    await page.goto(FRONTEND, { waitUntil: "load" });

    const graphButton = page.locator("button").filter({ hasText: /^Graph$/ });
    await expect(graphButton.first()).toBeVisible({ timeout: 15_000 });
    await graphButton.first().click();

    const snapshotSelect = page.locator("#snapshot-selector");
    await expect(snapshotSelect).toBeVisible({ timeout: 10_000 });
    await expect(snapshotSelect.locator("option")).toHaveCount(snapshots.length, { timeout: 15_000 });
    await snapshotSelect.selectOption(dclIngestId);

    const svg = page.locator('svg[role="img"][aria-label="Data-driven graph of DCL triple flow"]');
    await expect(svg).toBeVisible({ timeout: 30_000 });

    const dataGroup = page.locator('g[aria-label="Data entities"]');
    await expect(dataGroup).toBeVisible({ timeout: 10_000 });
    await expect(dataGroup.locator("g[data-node-id]").first()).toBeVisible({ timeout: 10_000 });

    // Payload check (guard B17): compute orphans from the JSON the UI received.
    const payload = runPayloads[runPayloads.length - 1];
    expect(payload, "No /api/dcl/run payload intercepted").toBeTruthy();
    const graph = payload.graph ?? {};
    const nodes: any[] = graph.nodes ?? [];
    const links: any[] = graph.links ?? [];
    const l3 = new Set(nodes.filter(n => n.level === "L3").map(n => n.id));
    const l4 = new Set(nodes.filter(n => n.level === "L4").map(n => n.id));
    const consumers = new Set(
      links
        .filter(l => l.flowType === "consumption" && l4.has(l.target))
        .map(l => l.source)
    );
    const orphans = [...l3].filter(id => !consumers.has(id)).sort();
    expect(
      orphans,
      `L3 orphans for ${entityId}: ${JSON.stringify(orphans)} — fix config/persona_domains.yaml`
    ).toHaveLength(0);

    await page.screenshot({ path: "tests/e2e/artifacts/graph_v2_no_orphans.png", fullPage: true });
    await page.unrouteAll({ behavior: "ignoreErrors" });
  });
});
