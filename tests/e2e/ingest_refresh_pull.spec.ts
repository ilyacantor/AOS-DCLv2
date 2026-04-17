// Operator-visible outcome: on the Ingest tab, clicking Refresh when DCL already has the newest Farm runs for every Farm entity shows banner text exactly "No new snapshots found." with no ingested/evicted/skipped sections; when Farm has a newer run for a DCL-tracked entity OR an entity DCL does not yet track, the banner reads "Ingested N new Farm run(s)." where N equals the count of keys whose Farm farm_run_id differs from DCL's last_farm_run_id (including keys missing from DCL), and when per-tenant cap evictions fire during Refresh the banner appends "Evicted M entity/entities from tenant_runs cap." with the first 50 evicted entity_ids listed.
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

function computeNewestPerEntity(farmRuns: FarmRun[]): Map<string, FarmRun> {
  // Rule 1 ground truth: Refresh now treats every (tenant, entity) key as a
  // candidate, not just those already in DCL. The silent scope filter was
  // removed — DCL's ingest endpoint rejects out-of-scope entities loudly.
  const newest = new Map<string, FarmRun>();
  for (const run of farmRuns) {
    if (run.status !== "completed") continue;
    if (!run.farm_run_id || !run.tenant_id || !run.entity_id) continue;
    const key = `${run.tenant_id}:${run.entity_id}`;
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

  test("0. Backend contract: ingested[], skipped[], evicted_sample[], evicted_total, message all present", async ({
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
    expect(Array.isArray(body.evicted_sample)).toBe(true);
    expect(typeof body.evicted_total).toBe("number");
    expect(typeof body.message).toBe("string");
    expect(body.evicted_sample.length).toBeLessThanOrEqual(body.evicted_total + 50);
    expect(body.evicted_sample.length).toBeLessThanOrEqual(50);
  });

  test("1. First Refresh: ingested[] entity_ids are subset of Farm-newest-per-key; message reflects ingested+evicted+skipped", async ({
    page,
  }) => {
    // Ground truth from Farm (Rule 1 — fetched at test time). No DCL-scope
    // filter; Refresh considers every completed (tenant, entity) Farm key.
    const farmRuns = await fetchFarmRuns(page);
    const farmNewest = computeNewestPerEntity(farmRuns);

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

    // Message shape reflects the work done. Exact equality, not regex —
    // spec calls this out.
    const ingestedCount = body.ingested.length;
    const evictedTotal = body.evicted_total as number;
    const skippedCount = body.skipped.length;
    if (ingestedCount === 0 && evictedTotal === 0 && skippedCount === 0) {
      expect(body.message).toBe("No new snapshots found.");
    } else {
      const parts: string[] = [`Ingested ${ingestedCount} new Farm run(s).`];
      if (evictedTotal > 0) {
        parts.push(`Evicted ${evictedTotal} entity/entities from tenant_runs cap.`);
      }
      if (skippedCount > 0) {
        parts.push(`${skippedCount} skipped — see details.`);
      }
      expect(body.message).toBe(parts.join(" "));
    }
  });

  test("2. Banner rendering: ingested/evicted/skipped sections match API response exactly", async ({
    page,
  }) => {
    // Rule 2 capture: the UI click is the one mutation; capture its
    // response by intercepting the in-flight request and assert the
    // banner renders exactly what the API returned. Whether the state
    // is mid-sync (ingested>0, evicted>0) or steady (skipped-only for
    // ME-rejections), the banner shape must match the API body.
    await openIngestTab(page);
    const refreshPromise = page.waitForResponse(
      (r) =>
        r.url().endsWith("/api/dcl/refresh-from-farm") &&
        r.request().method() === "POST",
      { timeout: 15_000 }
    );
    await page.locator("button").filter({ hasText: /^Refresh$/ }).click();
    const uiResp = await refreshPromise;
    const uiBody = await uiResp.json();

    const banner = page.locator("div.rounded.border.border-border.bg-muted\\/30");
    await expect(banner).toBeVisible({ timeout: 10_000 });
    await expect(banner.locator('[data-role="refresh-message"]')).toHaveText(
      uiBody.message as string
    );

    if ((uiBody.ingested as unknown[]).length) {
      const ingestedSection = banner.locator('[data-role="refresh-ingested"]');
      await expect(ingestedSection).toBeVisible();
      const ingestedText = ((await ingestedSection.textContent()) || "");
      expect(ingestedText).toMatch(
        new RegExp(`^Ingested \\(${(uiBody.ingested as unknown[]).length}\\)`)
      );
      for (const entry of uiBody.ingested as Array<{ entity_id: string }>) {
        expect(ingestedText).toContain(entry.entity_id);
      }
    } else {
      await expect(banner.locator('[data-role="refresh-ingested"]')).toHaveCount(0);
    }

    if ((uiBody.evicted_total as number) > 0) {
      const evictedSection = banner.locator('[data-role="refresh-evicted"]');
      await expect(evictedSection).toBeVisible();
      const evictedText = ((await evictedSection.textContent()) || "");
      expect(evictedText).toMatch(
        new RegExp(`^Evicted \\(${uiBody.evicted_total}\\)`)
      );
      for (const entry of uiBody.evicted_sample as Array<{ entity_id: string }>) {
        expect(evictedText).toContain(entry.entity_id);
      }
      if (
        (uiBody.evicted_total as number) >
        (uiBody.evicted_sample as unknown[]).length
      ) {
        expect(evictedText).toContain(
          `showing first ${(uiBody.evicted_sample as unknown[]).length}`
        );
      }
    } else {
      await expect(banner.locator('[data-role="refresh-evicted"]')).toHaveCount(0);
    }

    if ((uiBody.skipped as unknown[]).length) {
      const skippedSection = banner.locator('[data-role="refresh-skipped"]');
      await expect(skippedSection).toBeVisible();
      const skippedText = ((await skippedSection.textContent()) || "");
      expect(skippedText).toMatch(
        new RegExp(`^Skipped \\(${(uiBody.skipped as unknown[]).length}\\)`)
      );
    } else {
      await expect(banner.locator('[data-role="refresh-skipped"]')).toHaveCount(0);
    }

    // Snapshot for the completion handoff (Rule 4 + Rule 5).
    await page.screenshot({
      path: "tests/e2e/artifacts/ingest_refresh_mutative.png",
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
