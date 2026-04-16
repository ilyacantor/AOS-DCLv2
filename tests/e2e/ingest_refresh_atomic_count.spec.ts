// Operator-visible outcome: after clicking Refresh on an Ingest tab with 5 stale tracked entities, the banner reads "Ingested 5 new Farm run(s). N skipped" (banner <3000 chars, no out-of-scope reasons), and selecting any refreshed entity in the dropdown shows a "Current triples for <entity>" MetricCard whose numeric value equals COUNT(*) FROM current_triples for that (tenant, entity).
/**
 * Ingest tab — Refresh atomic count + banner scope regression.
 *
 * Guards against two bugs the last session missed:
 *   A. Refresh's skipped[] listed every Farm entity DCL does not track
 *      ("Entity is present in Farm but not in DCL tenant_runs"). The
 *      endpoint is update-only — unknown-to-DCL entities must not appear
 *      in any response field.
 *   B. append_rows_for_entity read/compute/write run_row_count pattern
 *      was non-atomic. Concurrent 5000-row append batches under the same
 *      (tenant, entity) clobbered each other, losing one batch's
 *      contribution. tenant_runs.run_row_count (feeding the Ingest tab's
 *      `Current triples for <entity>` MetricCard) drifted below the true
 *      current_triples COUNT(*).
 *
 * Shape of the test:
 *   1. Seed 5 tracked entities' tenant_runs.updated_at = 2025-01-01Z via
 *      the Python helper (direct DB). Refresh will see them as stale.
 *   2. Click Refresh in the UI. Farm replays each entity's latest
 *      manifest_runs row via push-to-dcl, which fans into ~4 concurrent
 *      append batches per entity (batch_size=5000, max_concurrency=2).
 *   3. Assert API shape (Bug A negative):
 *      - no skipped[] reason contains "not in DCL tenant_runs"
 *      - banner text length is bounded (post-fix there are 21 tracked
 *        entities max, not 173 out-of-scope strings).
 *   4. Assert count correctness (Bug B regression):
 *      For every seeded entity, tenant_runs.run_row_count ==
 *      COUNT(*) FROM current_triples WHERE entity_id = X.
 *   5. Assert B17 rendering:
 *      The "Current triples for <selected_entity>" MetricCard value
 *      equals the DB COUNT(*) for that entity's current_triples slice.
 *
 * Non-goals:
 *   - Concurrency count: one Refresh fans out per Farm pusher's internal
 *     asyncio semaphore; observing concurrency from the outside would
 *     require instrumentation. The atomic-increment guarantee is proved
 *     by the post-refresh arithmetic equality under the observed
 *     many-batches-per-entity pusher path.
 */
import { test, expect } from "playwright/test";
import { execFileSync } from "child_process";
import * as path from "path";

const DCL_URL = "http://localhost:3004";
const DCL_BACKEND = "http://localhost:8004";
const FARM_BACKEND = "http://localhost:8003";

const REPO = path.resolve(__dirname, "..", "..");
const PY = path.join(REPO, ".venv", "bin", "python");
const HELPER = path.join(
  REPO,
  "tests",
  "e2e",
  "helpers",
  "seed_stale_tenant_runs.py",
);

type EntityCounts = Record<
  string,
  { tenant_id: string; run_row_count: number; current_count: number }
>;

function runHelper(args: string[]): string {
  return execFileSync(PY, [HELPER, ...args], { encoding: "utf8" });
}

function listTracked(): Array<{
  tenant_id: string;
  entity_id: string;
  updated_at: string | null;
  run_row_count: number;
  current_run_id: string | null;
}> {
  return JSON.parse(runHelper(["list-tracked"]));
}

function allEntityCounts(): EntityCounts {
  return JSON.parse(runHelper(["all-entity-counts"]));
}

function seedStale(entityIds: string[]): void {
  runHelper(["seed-stale", ...entityIds]);
}

async function openIngestTab(page: import("playwright/test").Page) {
  await page.goto(DCL_URL, { waitUntil: "domcontentloaded" });
  const runButton = page.locator('button[data-role="run-primary"]');
  await expect(runButton).toBeVisible({ timeout: 15_000 });
  await expect(runButton).not.toHaveText("Running...", { timeout: 60_000 });
  await page.locator("button").filter({ hasText: "Ingest" }).click();
  await expect(page.getByText(/entries$/)).toBeVisible({ timeout: 10_000 });
}

test.describe.serial(
  "DCL Ingest — Refresh atomic count + scope regression",
  () => {
    test.setTimeout(300_000);

    test("0. Helpers present and tracked entities visible", async () => {
      const tracked = listTracked();
      expect(tracked.length).toBeGreaterThanOrEqual(5);
    });

    test("1. Refresh produces scoped banner, no out-of-scope entries", async ({
      page,
    }) => {
      // Cohort = intersection of DCL's tracked entities and Farm's
      // completed-run entities. Only entities in the intersection can
      // actually be refreshed — seeding stale on a DCL-only entity is a
      // no-op because Farm has no replay candidate.
      const tracked = listTracked();
      const trackedIds = new Set(tracked.map((e) => e.entity_id));
      const farmRunsResp = await page.request.get(
        `${FARM_BACKEND}/api/runs?limit=500`,
      );
      expect(farmRunsResp.status()).toBe(200);
      const farmRuns = (await farmRunsResp.json()) as Array<{
        entity_id: string;
        status: string;
      }>;
      const farmCompletedIds = new Set(
        farmRuns.filter((r) => r.status === "completed").map((r) => r.entity_id),
      );
      const intersection = [...trackedIds].filter((e) =>
        farmCompletedIds.has(e),
      );
      expect(
        intersection.length,
        `need at least 5 entities in DCL ∩ Farm; got ${intersection.length}`,
      ).toBeGreaterThanOrEqual(5);
      const cohort = intersection.slice(0, 5);

      // Seed stale via helper (direct DB).
      seedStale(cohort);

      // Baseline counts before Refresh.
      const before = allEntityCounts();
      for (const e of cohort) {
        expect(before[e]).toBeDefined();
      }

      // Open UI, click Refresh, wait for the POST to complete.
      await openIngestTab(page);

      const refreshPromise = page.waitForResponse(
        (resp) =>
          resp.url().endsWith("/api/dcl/refresh-from-farm") &&
          resp.request().method() === "POST",
        { timeout: 180_000 },
      );
      await page
        .locator("button")
        .filter({ hasText: /^Refresh$/ })
        .click();
      const resp = await refreshPromise;
      expect(resp.status()).toBe(200);

      const body = await resp.json();
      expect(body).toHaveProperty("ingested");
      expect(body).toHaveProperty("skipped");
      expect(body).toHaveProperty("message");

      // Bug A regression: no out-of-scope entries in skipped[].
      const outOfScope = (body.skipped as Array<{ reason: string }>).filter(
        (s) => s.reason.includes("not in DCL tenant_runs"),
      );
      expect(
        outOfScope.length,
        `skipped[] still contains ${outOfScope.length} out-of-scope entries`,
      ).toBe(0);

      // All seeded entities should have been ingested (they were stale).
      const ingestedIds = new Set(
        (body.ingested as Array<{ entity_id: string }>).map((i) => i.entity_id),
      );
      for (const e of cohort) {
        expect(
          ingestedIds.has(e),
          `expected ${e} in ingested[] but got ${JSON.stringify([...ingestedIds])}`,
        ).toBe(true);
      }

      // Wait for the refetch + re-render to settle before reading DOM.
      await expect(
        page.locator("div.rounded.border.border-border.bg-muted\\/30"),
      ).toBeVisible({ timeout: 15_000 });

      // Bug A banner text assertion: bounded. Post-fix there are <=21
      // tracked entities × ~200 chars each; we cap at 3000 to give
      // headroom while still catching the 173-entity wall (would be
      // ~45000 chars).
      const banner = page.locator(
        "div.rounded.border.border-border.bg-muted\\/30",
      );
      const bannerText = (await banner.textContent()) || "";
      expect(bannerText.length).toBeLessThan(3000);
      expect(bannerText).not.toContain("not in DCL tenant_runs");
      expect(bannerText).toMatch(/Ingested \d+ new Farm run/);

      // Bug B regression: arithmetic equality per seeded entity.
      const after = allEntityCounts();
      for (const eid of cohort) {
        const row = after[eid];
        expect(
          row.run_row_count,
          `drift on ${eid}: run_row_count=${row.run_row_count} vs current_count=${row.current_count}`,
        ).toBe(row.current_count);
      }

      // Select the first cohort entity in the dropdown and assert the
      // MetricCard reflects the DB count — B17 end-to-end gate.
      const selectedEntity = cohort[0];
      const dropdown = page.locator("select").first();
      await dropdown.selectOption(selectedEntity);

      // Wait for the MetricCard to re-render with the entity label.
      // MetricCard renders "Current triples for <entity>" as a label div
      // followed by a numeric value div inside a single rounded-border card.
      const labelLocator = page.getByText(
        new RegExp(`^Current triples for ${selectedEntity}$`),
      );
      await expect(labelLocator).toBeVisible({ timeout: 10_000 });

      // The value div is the label's next sibling inside the same card.
      const displayedText = await labelLocator
        .locator("xpath=following-sibling::div[1]")
        .textContent();
      const displayed = parseInt(
        (displayedText || "").replace(/[^\d]/g, ""),
        10,
      );
      expect(
        Number.isNaN(displayed),
        `could not parse MetricCard value from: ${displayedText}`,
      ).toBe(false);
      expect(
        displayed,
        `MetricCard shows ${displayed} but DB says ${after[selectedEntity].current_count} for ${selectedEntity}`,
      ).toBe(after[selectedEntity].current_count);

      await page.screenshot({
        path: "tests/e2e/artifacts/ingest_refresh_atomic_count_after.png",
        fullPage: true,
      });
    });

    test("2. Second Refresh is idempotent — no double-ingest and banner scope holds", async ({
      page,
    }) => {
      const first = await page.request.post(
        `${DCL_BACKEND}/api/dcl/refresh-from-farm`,
        { headers: { "Content-Type": "application/json" } },
      );
      expect(first.status()).toBe(200);
      const firstBody = await first.json();

      // First call on already-current state: zero ingested, skipped[] only
      // contains tracked entities whose latest Farm run is not newer.
      expect(firstBody.ingested.length).toBe(0);
      const badA = (firstBody.skipped as Array<{ reason: string }>).filter(
        (s) => s.reason.includes("not in DCL tenant_runs"),
      );
      expect(badA.length).toBe(0);

      const second = await page.request.post(
        `${DCL_BACKEND}/api/dcl/refresh-from-farm`,
        { headers: { "Content-Type": "application/json" } },
      );
      expect(second.status()).toBe(200);
      const secondBody = await second.json();
      expect(secondBody.ingested.length).toBe(0);

      // After two no-op refreshes drift must still be zero across the
      // cohort — the fix is durable, not a one-shot.
      const counts = allEntityCounts();
      const driftEntities = Object.entries(counts).filter(
        ([, v]) => v.run_row_count !== v.current_count,
      );
      expect(
        driftEntities.length,
        `drift after idempotent refresh: ${JSON.stringify(driftEntities.slice(0, 5))}`,
      ).toBe(0);
    });
  },
);
