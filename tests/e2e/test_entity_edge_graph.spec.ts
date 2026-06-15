// Operator-visible outcome: Graph tab Relationships mode for ContextOSDemo renders the engineering BELOW_MARKET software_engineering edge with gap 13.16% (internal 165000 workday_hr vs market 190000 radford_comp) — the cross-source comp-gap the agent traverses, which no single fact row holds.
/**
 * B17 acceptance — ContextOS Stage 4 entity-edge graph (the Gate 1B hero).
 *
 * Drives the dev frontend (http://localhost:3004 → vite proxy → :8104) with
 * REAL UI events only: click the Graph tab, selectOption the ContextOSDemo
 * snapshot, click the "Relationships" toggle. Asserts the rendered
 * engineering BELOW_MARKET edge carries the synthesized gap, then clicks the
 * edge and asserts the inspector states the gap and both cross-source origins.
 *
 * Ground truth (gap_pct, sources) is fetched at test time from the read-only
 * subgraph endpoint (the allowed page.request.get exception) — never hardcoded
 * and never agent-authored. The trigger is the operator's click, never a POST
 * from the test runner.
 *
 * Prerequisites: dev DCL backend on :8104, dev frontend on :3004, ContextOSDemo
 * ingested + edges derived (Stage 3).
 */

import { test, expect } from 'playwright/test';

const FRONTEND = process.env.DCL_FRONTEND_URL || 'http://localhost:3004';
const BACKEND = process.env.DCL_BACKEND_URL || 'http://localhost:8104';
const ENTITY = 'ContextOSDemo';

interface SubEdge {
  src_type: string; src_key: string; edge_type: string;
  dst_type: string; dst_key: string; properties: Record<string, unknown>;
}

test.describe.serial('Graph — Relationships (entity-edge hero)', () => {
  test.setTimeout(120_000);

  test('engineering BELOW_MARKET edge renders the synthesized 13.16% comp-gap', async ({ page }) => {
    // ── Ground truth from the read-only subgraph endpoint (Rule 1). ──
    const sub = (await (
      await page.request.get(`${BACKEND}/api/dcl/graph/subgraph?entity_id=${ENTITY}`)
    ).json()) as { edges: SubEdge[]; counts: { by_type: Record<string, number> } };

    const hero = sub.edges.find(
      (e) => e.edge_type === 'BELOW_MARKET' && e.src_key === 'engineering'
        && e.dst_key === 'software_engineering',
    );
    expect(hero, 'ground truth must contain the engineering→software_engineering BELOW_MARKET edge').toBeTruthy();
    const gapPct = String(hero!.properties.gap_pct);          // "13.16"
    const internalMedian = String(hero!.properties.internal_median); // "165000"
    const marketMedian = String(hero!.properties.market_median);     // "190000"
    const internalSource = String(hero!.properties.internal_source); // "workday_hr"
    const marketSource = String(hero!.properties.market_source);     // "radford_comp"
    expect(gapPct).toBe('13.16');
    expect(internalSource).toBe('workday_hr');
    expect(marketSource).toBe('radford_comp');

    // ── Operator path: open the app, Graph tab. ──
    await page.goto(FRONTEND, { waitUntil: 'domcontentloaded' });
    const graphButton = page.locator('button').filter({ hasText: /^Graph$/ });
    await expect(graphButton.first()).toBeVisible({ timeout: 15_000 });
    await graphButton.first().click();

    // Select the ContextOSDemo snapshot from the entity/snapshot selector.
    const selector = page.locator('#snapshot-selector');
    await expect(selector).toBeVisible({ timeout: 10_000 });
    // The option label carries the snapshot_name (e.g. "ContextOSDemo-e4ec").
    const ctxOption = selector.locator('option', { hasText: ENTITY }).first();
    await expect(ctxOption).toHaveCount(1, { timeout: 15_000 });
    const ctxValue = await ctxOption.getAttribute('value');
    expect(ctxValue, 'ContextOSDemo snapshot option must carry a dcl_ingest_id value').toBeTruthy();
    await selector.selectOption(ctxValue!);

    // Click the Relationships toggle (REAL click — this is what triggers the fetch).
    await page.getByTestId('graph-mode-relationships').click();

    // The entity-edge graph container renders for ContextOSDemo.
    const graph = page.getByTestId('entity-edge-graph');
    await expect(graph).toHaveAttribute('data-entity-id', ENTITY, { timeout: 20_000 });
    await expect(graph).toHaveAttribute('data-edge-count', String(sub.edges.length), { timeout: 20_000 });

    // The engineering department node is visible.
    const engNode = page.locator('[data-testid="ee-node"][data-node-key="engineering"]');
    await expect(engNode).toBeVisible({ timeout: 15_000 });

    // The hero edge: a BELOW_MARKET ee-edge from engineering carrying the gap.
    const heroEdge = page.locator(
      '[data-testid="ee-edge"][data-edge-type="BELOW_MARKET"][data-src="engineering"]',
    );
    await expect(heroEdge).toHaveCount(1, { timeout: 15_000 });
    await expect(heroEdge).toHaveAttribute('data-gap-pct', gapPct); // rendered 13.16, from real DCL data
    await expect(heroEdge).toHaveAttribute('data-dst', 'software_engineering');

    await page.screenshot({
      path: 'tests/e2e/screenshots/entity_edge_graph_below_market.png',
      fullPage: true,
    });

    // ── Click the hero edge → inspector states the gap and both sources. ──
    await heroEdge.click();
    const inspector = page.getByTestId('ee-inspector');
    await expect(inspector).toBeVisible({ timeout: 10_000 });
    await expect(inspector).toHaveAttribute('data-edge-type', 'BELOW_MARKET');
    const inspectorText = page.getByTestId('ee-inspector-text');
    await expect(inspectorText).toContainText(gapPct);          // 13.16
    await expect(inspectorText).toContainText(internalMedian);  // 165000
    await expect(inspectorText).toContainText(marketMedian);    // 190000
    await expect(inspectorText).toContainText(internalSource);  // workday_hr
    await expect(inspectorText).toContainText(marketSource);    // radford_comp

    await page.screenshot({
      path: 'tests/e2e/screenshots/entity_edge_graph_inspector.png',
      fullPage: true,
    });
  });

  test('Reveal source records drills the 13.16% gap to its two source records', async ({ page }) => {
    // ── Ground truth: the two source records, from the read-only provenance
    //    endpoint (Rule 1 — the allowed page.request.get exception). ──
    const prov = (await (
      await page.request.get(
        `${BACKEND}/api/dcl/graph/edge-provenance?entity_id=${ENTITY}` +
          `&src_type=department&src_key=engineering&edge_type=BELOW_MARKET` +
          `&dst_type=job_family&dst_key=software_engineering`,
      )
    ).json()) as {
      sources: { concept: string; value: number; source_system: string; triple_id: string }[];
      synthesized: { gap_pct: number };
    };
    const internal = prov.sources.find((s) => s.source_system === 'workday_hr')!;
    const market = prov.sources.find((s) => s.source_system === 'radford_comp')!;
    expect(internal.value).toBe(165000);
    expect(market.value).toBe(190000);
    expect(internal.triple_id).not.toBe(market.triple_id); // two different real rows
    expect(prov.synthesized.gap_pct).toBe(13.16);          // the fact they were synthesized into

    // ── Operator path: Graph tab → ContextOSDemo → Relationships. ──
    await page.goto(FRONTEND, { waitUntil: 'domcontentloaded' });
    const graphButton = page.locator('button').filter({ hasText: /^Graph$/ });
    await expect(graphButton.first()).toBeVisible({ timeout: 15_000 });
    await graphButton.first().click();

    const selector = page.locator('#snapshot-selector');
    await expect(selector).toBeVisible({ timeout: 10_000 });
    const ctxOption = selector.locator('option', { hasText: ENTITY }).first();
    await expect(ctxOption).toHaveCount(1, { timeout: 15_000 });
    const ctxValue = await ctxOption.getAttribute('value');
    await selector.selectOption(ctxValue!);

    await page.getByTestId('graph-mode-relationships').click();

    const graph = page.getByTestId('entity-edge-graph');
    await expect(graph).toHaveAttribute('data-entity-id', ENTITY, { timeout: 20_000 });

    // Click the hero edge → inspector opens.
    const heroEdge = page.locator(
      '[data-testid="ee-edge"][data-edge-type="BELOW_MARKET"][data-src="engineering"]',
    );
    await expect(heroEdge).toHaveCount(1, { timeout: 15_000 });
    await heroEdge.click();
    await expect(page.getByTestId('ee-inspector')).toBeVisible({ timeout: 10_000 });

    // ── The reveal: REAL click on "Reveal source records". ──
    const reveal = page.getByTestId('ee-provenance-reveal');
    await expect(reveal).toBeVisible({ timeout: 10_000 });
    await reveal.click();

    // The source-record table renders the two records behind the gap.
    const provTable = page.getByTestId('ee-provenance');
    await expect(provTable).toBeVisible({ timeout: 15_000 });

    // Row 1: the internal comp_band — workday_hr / 165000.
    const workdayRow = page.locator('[data-testid="ee-prov-row"][data-source="workday_hr"]');
    await expect(workdayRow).toHaveCount(1, { timeout: 10_000 });
    await expect(workdayRow.getByTestId('ee-prov-value')).toHaveText(String(internal.value)); // 165000
    await expect(workdayRow.getByTestId('ee-prov-source')).toHaveText('workday_hr');

    // Row 2: the market benchmark — radford_comp / 190000.
    const radfordRow = page.locator('[data-testid="ee-prov-row"][data-source="radford_comp"]');
    await expect(radfordRow).toHaveCount(1, { timeout: 10_000 });
    await expect(radfordRow.getByTestId('ee-prov-value')).toHaveText(String(market.value)); // 190000
    await expect(radfordRow.getByTestId('ee-prov-source')).toHaveText('radford_comp');

    // Exactly the two source records the gap was synthesized from — no more.
    await expect(page.getByTestId('ee-prov-row')).toHaveCount(prov.sources.length); // 2

    await page.screenshot({
      path: 'tests/e2e/screenshots/entity_edge_graph_provenance_reveal.png',
      fullPage: true,
    });
  });

  test('a fetch failure surfaces the real error, never a silent empty graph', async ({ page }) => {
    // Negative (Rule 7): force the subgraph fetch to fail; the readable error
    // must render — not an empty canvas.
    await page.goto(FRONTEND, { waitUntil: 'domcontentloaded' });
    const graphButton = page.locator('button').filter({ hasText: /^Graph$/ });
    await expect(graphButton.first()).toBeVisible({ timeout: 15_000 });
    await graphButton.first().click();

    const selector = page.locator('#snapshot-selector');
    await expect(selector).toBeVisible({ timeout: 10_000 });
    const ctxOption = selector.locator('option', { hasText: ENTITY }).first();
    await expect(ctxOption).toHaveCount(1, { timeout: 15_000 });
    const ctxValue = await ctxOption.getAttribute('value');
    await selector.selectOption(ctxValue!);

    // Make the subgraph endpoint 500 for this page only.
    await page.route('**/api/dcl/graph/subgraph**', (route) =>
      route.fulfill({ status: 500, contentType: 'application/json', body: '{"detail":"forced failure"}' }),
    );

    await page.getByTestId('graph-mode-relationships').click();

    const graph = page.getByTestId('entity-edge-graph');
    await expect(graph).toHaveAttribute('data-state', 'error', { timeout: 20_000 });
    const errText = page.getByTestId('ee-error');
    await expect(errText).toContainText('forced failure');
    await expect(errText).toContainText(ENTITY);
    await expect(errText).toContainText('/api/dcl/graph/subgraph');

    await page.screenshot({
      path: 'tests/e2e/screenshots/entity_edge_graph_error.png',
      fullPage: true,
    });
    await page.unrouteAll({ behavior: 'ignoreErrors' });
  });
});
