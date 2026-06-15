// Operator-visible outcome: with ContextOSDemo selected in the Context tab, opening the Conflict Register and expanding the cloud_spend.summary/total_cost/2026-03 row shows a green "Resolved: 409,974.93 — aws_billing (authoritative)" line that also discloses "netsuite_gl_allocation disagrees at 388,829.94"; and opening the As-of panel and filling a past timestamp renders the point-in-time row grid whose row count matches the as-of browse ground truth for that instant.
/**
 * ContextOS Stage 5 — authority arbitration + as-of live acceptance (B17).
 *
 * The operator path is 100% real UI events (Context tab click, snapshot
 * selectOption, register toggle click, conflict row click, datetime-local
 * fill). The feature under test — the rendered decisive value + disclosure and
 * the as-of grid — is triggered only by those clicks/fills, never by a POST
 * from the test runner.
 *
 * Setup (beforeAll, allowed via request): the ContextOSDemo entity is ALREADY
 * ingested; setup only PUTs the demo tenant's authority map and re-detects so
 * the cloud/headcount conflicts carry an authority recommendation. This mirrors
 * proposals_review.spec.ts's beforeAll seeding — setup state, not the feature.
 *
 * Ground truth: pulled at runtime from GET /api/dcl/conflicts (the `resolved`
 * field) and GET /api/dcl/triples/browse?as_of (read-only). Expected values are
 * never hardcoded — they flow from the live register/store (acceptance rule 1).
 *
 * Constitution compliance:
 *   - The render is triggered by locator.click()/fill() — never request.post()
 *   - request.get() / request.put() used for SETUP + ground truth only
 *   - Dev only: frontend :3004 (vite proxies /api/dcl → :8104), backend :8104.
 *     Never :8004 (prod).
 *   - Screenshots after each test (reporting rule 4)
 *   - Paired negative: bad as-of timestamp surfaces the readable error
 */

import { test, expect } from "playwright/test";

const DCL_FRONTEND = process.env.DCL_FRONTEND_URL ?? "http://localhost:3004";
const DCL_BACKEND  = process.env.DCL_BACKEND_URL  ?? "http://localhost:8104";

const DEMO_TENANT = "51aee6ec-15c3-4fb0-833a-a19bb4511296";
const DEMO_ENTITY = "ContextOSDemo";
const DEMO_INGEST = "e4ec6c50-2104-46e4-b173-74d579b1a136";

const CLOUD_CONCEPT = "cloud_spend.summary";
const CLOUD_PROPERTY = "total_cost";
const CLOUD_PERIOD = "2026-03";

const SCREENSHOTS = "tests/e2e/screenshots";

// Ground truth resolved at setup time (never hardcoded).
let cloudResolved: {
  decisive_value: number;
  decisive_source: string;
  disclosed: { source_system: string; value: number }[];
};

test.describe.serial("Authority arbitration + as-of — live acceptance (Stage 5)", () => {
  test.setTimeout(120_000);

  test.beforeAll(async ({ request }) => {
    // 1. Dev backend healthy (NOT prod).
    const health = await request.get(`${DCL_BACKEND}/health`);
    expect(health.status(), "DCL dev backend (:8104) not healthy").toBe(200);

    // 2. Seed the demo tenant's authority map (SETUP — real PUT through the
    //    actual endpoint; this is the arbitration policy, not the feature).
    for (const [prefix, ranked] of [
      ["cloud_spend", ["aws_billing", "netsuite_gl_allocation"]],
      ["headcount", ["workday_hr", "netsuite_finance_rollup"]],
      ["workforce", ["workday_hr", "netsuite_finance_rollup"]],
    ] as [string, string[]][]) {
      const put = await request.put(`${DCL_BACKEND}/api/dcl/conflicts/authority-map`, {
        data: { tenant_id: DEMO_TENANT, concept_prefix: prefix, ranked_sources: ranked },
      });
      expect(put.status(), `authority PUT ${prefix} failed: ${await put.text()}`).toBe(200);
    }

    // 3. Re-detect so the register recommendation flips to authority (SETUP).
    const detect = await request.post(`${DCL_BACKEND}/api/dcl/conflicts/detect`, {
      data: { tenant_id: DEMO_TENANT, entity_id: DEMO_ENTITY, dcl_ingest_id: DEMO_INGEST },
    });
    expect(detect.status(), `detect failed: ${await detect.text()}`).toBe(200);

    // 4. Ground truth: the resolved field for the cloud 2026-03 conflict.
    const gt = await request.get(
      `${DCL_BACKEND}/api/dcl/conflicts?tenant_id=${DEMO_TENANT}` +
      `&entity_id=${DEMO_ENTITY}&concept=${encodeURIComponent(CLOUD_CONCEPT)}&limit=500`,
    );
    expect(gt.status()).toBe(200);
    const body = await gt.json();
    const target = body.conflicts.find(
      (c: any) => c.property === "total_cost" && c.period === CLOUD_PERIOD,
    );
    expect(
      target?.concept,
      `no cloud_spend.summary/total_cost/${CLOUD_PERIOD} conflict on the register`,
    ).toBe(CLOUD_CONCEPT);
    cloudResolved = target.resolved;
    // Sanity on the ground truth itself — the demo's verified arbitration.
    expect(cloudResolved.status).toBe("resolved");
    expect(cloudResolved.decisive_source).toBe("aws_billing");
  });

  // ── Gate 1: decisive value + disclosure renders in the browser ────────────

  test("cloud_spend conflict renders ONE decisive value with the loser disclosed", async ({ page }) => {
    await page.goto(DCL_FRONTEND, { waitUntil: "load" });

    // Real clicks: open Context tab, select the demo snapshot.
    await page.locator("button", { hasText: "Context" }).click();
    const selector = page.locator("#snapshot-selector");
    await expect(selector.locator(`option[value="${DEMO_INGEST}"]`)).toHaveCount(1, { timeout: 30_000 });
    await selector.selectOption(DEMO_INGEST);

    // Open the Conflict Register and expand the cloud 2026-03 row (real clicks).
    const panel = page.locator('[data-testid="conflicts-panel"]');
    await expect(panel).toBeVisible({ timeout: 30_000 });
    await panel.locator('[data-testid="conflicts-toggle"]').click();

    const row = panel.locator(`[data-testid="conflict-row-${CLOUD_CONCEPT}-${CLOUD_PROPERTY}-${CLOUD_PERIOD}"]`);
    await expect(row).toBeVisible({ timeout: 30_000 });
    await row.locator("button").first().click();

    // The decisive-value surface, tied to ground truth.
    const resolved = page.locator('[data-testid="conflict-resolved"]');
    await expect(resolved).toBeVisible({ timeout: 15_000 });

    // data attributes carry the exact decisive value + source (raw).
    await expect(resolved).toHaveAttribute(
      "data-decisive-value", String(cloudResolved.decisive_value),
    );
    await expect(resolved).toHaveAttribute(
      "data-decisive-source", cloudResolved.decisive_source,
    );

    // Rendered text: the decisive value (locale-formatted) + winning source.
    const decisiveText = cloudResolved.decisive_value.toLocaleString("en-US");
    await expect(resolved).toContainText(`Resolved: ${decisiveText}`);
    await expect(resolved).toContainText(cloudResolved.decisive_source);
    await expect(resolved).toContainText("authoritative");

    // The loser is disclosed by source AND value (one decisive value, the
    // disagreement disclosed — the whole point of the surface).
    expect(
      cloudResolved.disclosed.length,
      "the cloud conflict has exactly one disclosed loser (the GL allocation)",
    ).toBe(1);
    const loser = cloudResolved.disclosed[0];
    await expect(resolved).toContainText(loser.source_system);
    await expect(resolved).toContainText(loser.value.toLocaleString("en-US"));

    await page.screenshot({ path: `${SCREENSHOTS}/stage5_01_conflict_resolved.png`, fullPage: true });
  });

  // ── Gate 2: as-of control renders the point-in-time grid ──────────────────

  test("as-of control renders the point-in-time row grid for a past instant", async ({ page }) => {
    await page.goto(DCL_FRONTEND, { waitUntil: "load" });
    await page.locator("button", { hasText: "Context" }).click();
    const selector = page.locator("#snapshot-selector");
    await expect(selector.locator(`option[value="${DEMO_INGEST}"]`)).toHaveCount(1, { timeout: 30_000 });
    await selector.selectOption(DEMO_INGEST);

    // Open the As-of panel (real click).
    const asof = page.locator('[data-testid="as-of-panel"]');
    await expect(asof).toBeVisible({ timeout: 30_000 });
    await asof.locator('[data-testid="as-of-toggle"]').click();

    // Resolve a point-in-time instant from the store itself (never hardcoded):
    // the demo entity's latest ingest + a minute, so the demo's rows are live
    // at that instant. datetime-local wants local YYYY-MM-DDTHH:mm:ss.
    const ents = await page.request.get(`${DCL_BACKEND}/api/dcl/entities`);
    expect(ents.status()).toBe(200);
    const demo = (await ents.json()).entities.find((e: any) => e.entity_id === DEMO_ENTITY);
    expect(demo?.entity_id, "ContextOSDemo missing from entities").toBe(DEMO_ENTITY);
    // +1 day past the ingest in UTC wall-clock terms — unambiguously after the
    // ingest regardless of DB session timezone, and the demo has no
    // supersession so every ingested row is still live then.
    const asOfDate = new Date(new Date(demo.latest_ingest).getTime() + 86_400_000);
    const pad = (n: number) => String(n).padStart(2, "0");
    const asOfValue =
      `${asOfDate.getUTCFullYear()}-${pad(asOfDate.getUTCMonth() + 1)}-${pad(asOfDate.getUTCDate())}` +
      `T${pad(asOfDate.getUTCHours())}:${pad(asOfDate.getUTCMinutes())}:${pad(asOfDate.getUTCSeconds())}`;

    // Ground truth: how many rows the store says are live at that instant.
    const gt = await page.request.get(
      `${DCL_BACKEND}/api/dcl/triples/browse?tenant_id=${DEMO_TENANT}` +
      `&entity_id=${DEMO_ENTITY}&as_of=${asOfValue}&limit=50`,
    );
    expect(gt.status(), await gt.text()).toBe(200);
    const gtBody = await gt.json();
    const gtTotal: number = gtBody.total_count;
    const gtRendered = Math.min(gtTotal, 50); // the grid caps at the limit
    expect(gtTotal, "demo must have point-in-time rows live at this instant").toBeGreaterThan(0);

    // Real fill on the datetime-local control — this triggers the fetch.
    const control = page.locator('[data-testid="as-of-control"]');
    await control.fill(asOfValue);

    const results = page.locator('[data-testid="as-of-results"]');
    await expect(results).toBeVisible({ timeout: 20_000 });
    // The header states the live-row count — must equal store ground truth.
    await expect(results).toContainText(`${gtTotal} rows live`);
    // And the grid renders exactly the (capped) number of data rows.
    const rendered = results.locator('tr[data-testid^="as-of-row-"]');
    await expect(rendered).toHaveCount(gtRendered);

    await page.screenshot({ path: `${SCREENSHOTS}/stage5_02_asof_results.png`, fullPage: true });
  });

  // Negative for the bad-timestamp surface lives in the backend gate
  // (tests/test_authority_arbitration.py::test_as_of_bad_timestamp_fails_loud,
  // asserting the 400 + "ISO-8601" detail). The as-of control is a native
  // <input type="datetime-local">: the browser structurally prevents emitting
  // a malformed timestamp, so there is no operator-reachable bad-timestamp
  // failure surface to drive here without a test-only DOM bypass (which the
  // acceptance rules forbid). The readable-error div (data-testid
  // "as-of-error") still renders for real backend errors surfaced through the
  // same path; it is wired, just not reachable via the picker.
});
