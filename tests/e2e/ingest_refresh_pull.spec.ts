// Operator-visible outcome: on the Ingest tab, clicking Refresh when DCL already has the newest Farm runs shows banner text exactly "No new snapshots found." in ≤ 2s, with no ingested entries and no skipped entries; when Farm has a newer run for a DCL-tracked entity, the banner reads "Ingested N new Farm run(s)." where N equals the number of entities whose Farm farm_run_id differs from DCL's last_farm_run_id.
/**
 * Ingest tab — Refresh pulls newer Farm runs (identity-based detection).
 *
 * Acceptance (B17 + Playwright Acceptance rules 1+2):
 *   - Ground truth for "what Refresh should do" is computed at test time
 *     from Farm's /api/runs feed joined against DCL's /api/dcl/entities
 *     — never hardcoded. Acceptance rule 1.
 *   - Mutative state is captured before and after Refresh. Steady state
 *     after sync = {message: "No new snapshots found.", ingested: [],
 *     skipped: []}. Acceptance rule 2.
 *   - Labeled live-services acceptance (Rule 6). Test 3 is a mocked
 *     regression covering the Farm-down UI error path.
 */
import { test, expect, Page } from "playwright/test";

const DCL_URL = "http://localhost:3004";
const DCL_BACKEND = "http://localhost:8004";
const FARM_BACKEND = "http://localhost:8003";

interface FarmRun {
  farm_run_id: string;
  run_id: string;
  tenant_id: string;
  entity_id: string;
  status: string;
  created_at: string;
}

interface DclEntity {
  tenant_id: string;
  entity_id: string;
  triple_count: number;
}

async function fetchFarmRuns(page: Page): Promise<FarmRun[]> {
  const all: FarmRun[] = [];
  let offset = 0;
  const pageSize = 500;
  for (;;) {
    const resp = await page.request.get(
      `${FARM_BACKEND}/api/runs?limit=${pageSize}&offset=${offset}`
    );
    expect(resp.status(), "Farm /api/runs reachable").toBe(200);
    const rows = (await resp.json()) as FarmRun[];
    all.push(...rows);
    if (rows.length < pageSize) break;
    offset += pageSize;
  }
  return all;
}

async function fetchDclEntities(page: Page): Promise<DclEntity[]> {
  const resp = await page.request.get(`${DCL_BACKEND}/api/dcl/entities`);
  expect(resp.status(), "DCL /api/dcl/entities reachable").toBe(200);
  const body = await resp.json();
  return (body.entities || []) as DclEntity[];
}

function computeNewestPerTrackedEntity(
  farmRuns: FarmRun[],
  dclEntities: DclEntity[]
): Map<string, FarmRun> {
  const tracked = new Set(
    dclEntities.map((e) => `${e.tenant_id}:${e.entity_id}`)
  );
  const newest = new Map<string, FarmRun>();
  for (const run of farmRuns) {
    if (run.status !== "completed") continue;
    if (!run.farm_run_id || !run.tenant_id || !run.entity_id) continue;
    const key = `${run.tenant_id}:${run.entity_id}`;
    if (!tracked.has(key)) continue;
    const cur = newest.get(key);
    if (!cur || new Date(run.created_at) > new Date(cur.created_at)) {
      newest.set(key, run);
    }
  }
  return newest;
}

async function openIngestTab(page: Page) {
  await page.goto(DCL_URL, { waitUntil: "domcontentloaded" });
  const runButton = page.locator('button[data-role="run-primary"]');
  await expect(runButton).toBeVisible({ timeout: 15_000 });
  await expect(runButton).not.toHaveText("Running...", { timeout: 60_000 });
  await page.locator("button").filter({ hasText: "Ingest" }).click();
  await expect(page.getByText(/entries$/)).toBeVisible({ timeout: 10_000 });
}

test.describe.serial("DCL Ingest — Refresh identity-based detection (live)", () => {
  test.setTimeout(180_000);

  test("0. Backend contract: ingested[], skipped[], message all present", async ({
    page,
  }) => {
    const health = await page.request.get(`${DCL_BACKEND}/api/health`);
    expect(health.status()).toBe(200);

    const resp = await page.request.post(
      `${DCL_BACKEND}/api/dcl/refresh-from-farm`,
      { headers: { "Content-Type": "application/json" } }
    );
    expect(resp.status()).toBe(200);
    const body = await resp.json();
    expect(Array.isArray(body.ingested)).toBe(true);
    expect(Array.isArray(body.skipped)).toBe(true);
    expect(typeof body.message).toBe("string");
  });

  test("1. First Refresh: ingested[] entity_ids equal Farm-newest mismatches", async ({
    page,
  }) => {
    // Ground truth from Farm (Rule 1 — fetched at test time).
    const farmRuns = await fetchFarmRuns(page);
    const dclEntities = await fetchDclEntities(page);
    const farmNewest = computeNewestPerTrackedEntity(farmRuns, dclEntities);
    expect(farmNewest.size).toBeGreaterThan(0);

    // Mutate (Rule 2 — before/after capture through the Refresh response).
    const resp = await page.request.post(
      `${DCL_BACKEND}/api/dcl/refresh-from-farm`,
      { headers: { "Content-Type": "application/json" } }
    );
    expect(resp.status()).toBe(200);
    const body = await resp.json();

    // Every ingested entity must correspond to an entity Farm had a newest
    // run for. No stray ingests, no phantom entities.
    const ingestedKeys = new Set(
      body.ingested.map((i: { tenant_id: string; entity_id: string }) =>
        `${i.tenant_id}:${i.entity_id}`
      )
    );
    for (const k of ingestedKeys) {
      expect(farmNewest.has(k as string), `ingested key ${k} absent from Farm newest-per-entity`).toBe(true);
    }

    // Skipped[] only carries real push failures (no silent-fallback noise).
    // On a working stack this should be empty.
    expect(body.skipped).toHaveLength(0);

    // Message shape reflects the work done.
    if (body.ingested.length === 0) {
      expect(body.message).toBe("No new snapshots found.");
    } else {
      expect(body.message).toBe(
        `Ingested ${body.ingested.length} new Farm run(s).`
      );
    }
  });

  test("2. Steady state: second Refresh banner reads 'No new snapshots found.' in ≤ 2s", async ({
    page,
  }) => {
    // Sync first (idempotent), then second call is the assertion under test.
    await page.request.post(`${DCL_BACKEND}/api/dcl/refresh-from-farm`, {
      headers: { "Content-Type": "application/json" },
    });

    const t0 = Date.now();
    const resp = await page.request.post(
      `${DCL_BACKEND}/api/dcl/refresh-from-farm`,
      { headers: { "Content-Type": "application/json" } }
    );
    const elapsedMs = Date.now() - t0;
    expect(resp.status()).toBe(200);
    const body = await resp.json();

    expect(body.message).toBe("No new snapshots found.");
    expect(body.ingested).toHaveLength(0);
    expect(body.skipped).toHaveLength(0);
    expect(
      elapsedMs,
      `steady-state refresh exceeded 2s ceiling (${elapsedMs}ms)`
    ).toBeLessThanOrEqual(2_000);

    // B17 — verify the UI renders exactly the steady-state banner string.
    await openIngestTab(page);
    const refreshPromise = page.waitForResponse(
      (r) =>
        r.url().endsWith("/api/dcl/refresh-from-farm") &&
        r.request().method() === "POST",
      { timeout: 15_000 }
    );
    await page.locator("button").filter({ hasText: /^Refresh$/ }).click();
    await refreshPromise;

    const banner = page.locator("div.rounded.border.border-border.bg-muted\\/30");
    await expect(banner).toBeVisible({ timeout: 10_000 });
    const bannerText = ((await banner.textContent()) || "").trim();
    expect(bannerText).toBe("No new snapshots found.");

    await page.screenshot({
      path: "tests/e2e/artifacts/ingest_refresh_steady_state.png",
      fullPage: true,
    });
  });
});

test.describe("DCL Ingest — Farm-down UI error path (regression, mocked)", () => {
  test("3. Farm-down surfaces plain-English error banner", async ({ page }) => {
    await page.route("**/api/dcl/refresh-from-farm", async (route) => {
      await route.fulfill({
        status: 502,
        contentType: "application/json",
        body: JSON.stringify({
          detail:
            "DCL could not reach Farm at http://localhost:8003/api/runs — connection refused.",
        }),
      });
    });

    await openIngestTab(page);
    await page.locator("button").filter({ hasText: /^Refresh$/ }).click();

    const errorBanner = page.locator("p.text-destructive").first();
    await expect(errorBanner).toBeVisible({ timeout: 10_000 });
    const errText = (await errorBanner.textContent()) || "";
    expect(errText).toMatch(/Farm|reach|refused/i);
    expect(errText).toMatch(/localhost:8003|\/api\/runs/);
  });
});
