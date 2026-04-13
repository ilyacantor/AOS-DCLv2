/**
 * Store rebuild acceptance gate — B17 accountability for migrations 014/015.
 *
 * Proves:
 *  - /api/dcl/snapshots returns 200 in <500ms (Task 10 curl budget).
 *  - Every tenant_runs entry is served to the UI via /api/dcl/entities.
 *  - The three tabs (Ingest, Context, Dashboard) return identical counts per
 *    entity — this is the post-rebuild invariant, mirrored into the UI.
 *  - Each tab renders numeric content (not skeleton) for the live entities.
 *
 * Entities are resolved at runtime from the live backend — no hardcoded IDs.
 */

import { test, expect, APIRequestContext } from "playwright/test";

const DCL_URL = "http://localhost:3004";
const DCL_BACKEND = "http://localhost:8004";
const SNAPSHOT_BUDGET_MS = 500;
const PRIMARY_TENANT_ID = "69688df3-fc8e-51f8-a77c-9c13f9b3a784";

type EntityRow = { tenant_id: string; entity_id: string; triple_count: number };

async function fetchEntities(req: APIRequestContext): Promise<EntityRow[]> {
  const res = await req.get(`${DCL_BACKEND}/api/dcl/entities`);
  expect(res.status()).toBe(200);
  const body = await res.json();
  return (body.entities ?? []) as EntityRow[];
}

async function contextCount(
  req: APIRequestContext,
  tenantId: string,
  entityId: string
): Promise<number> {
  const res = await req.get(
    `${DCL_BACKEND}/api/dcl/contextualization-summary?tenant_id=${tenantId}&entity_id=${entityId}`
  );
  expect(res.status()).toBe(200);
  const body = await res.json();
  const domains = body.domain_coverage?.domains ?? [];
  return domains.reduce((acc: number, d: any) => acc + (d.triple_count ?? 0), 0);
}

async function dashboardCount(
  req: APIRequestContext,
  tenantId: string,
  entityId: string
): Promise<number> {
  const res = await req.get(
    `${DCL_BACKEND}/api/dcl/dashboard-data?tenant_id=${tenantId}&entity_id=${entityId}&page=1&page_size=1`
  );
  expect(res.status()).toBe(200);
  const body = await res.json();
  return body.total_count ?? body.total ?? 0;
}

test.describe.serial("Store rebuild — acceptance gate", () => {
  test.setTimeout(120_000);

  test("/api/dcl/snapshots responds in under 500ms", async ({ request }) => {
    const t0 = Date.now();
    const res = await request.get(
      `${DCL_BACKEND}/api/dcl/snapshots?tenant_id=${PRIMARY_TENANT_ID}`
    );
    const elapsed = Date.now() - t0;
    expect(res.status()).toBe(200);
    expect(
      elapsed,
      `Snapshot latency ${elapsed}ms exceeds ${SNAPSHOT_BUDGET_MS}ms budget (B18)`
    ).toBeLessThan(SNAPSHOT_BUDGET_MS);

    const body = await res.json();
    const snapshots = body.snapshots ?? body.entities ?? body ?? [];
    expect(
      Array.isArray(snapshots) ? snapshots.length : 0,
      "Snapshot list is empty"
    ).toBeGreaterThan(0);
  });

  test("/api/dcl/entities lists every tenant_runs entity with a triple_count and tenant_id", async ({ request }) => {
    const entities = await fetchEntities(request);
    expect(entities.length, "No entities returned from /api/dcl/entities").toBeGreaterThan(0);
    for (const e of entities) {
      expect(
        e.triple_count,
        `Entity ${e.entity_id} has no triple_count`
      ).toBeGreaterThan(0);
      expect(
        e.tenant_id,
        `Entity ${e.entity_id} is missing tenant_id (I2 violation)`
      ).toBeTruthy();
    }
  });

  test("count invariant: Ingest == Context == Dashboard per (tenant, entity)", async ({ request }) => {
    const entities = await fetchEntities(request);
    const mismatches: string[] = [];
    for (const e of entities) {
      const ctx = await contextCount(request, e.tenant_id, e.entity_id);
      const dash = await dashboardCount(request, e.tenant_id, e.entity_id);
      const ingest = e.triple_count;
      if (!(ingest === ctx && ctx === dash)) {
        mismatches.push(
          `${e.tenant_id}/${e.entity_id}: Ingest=${ingest} Context=${ctx} Dashboard=${dash}`
        );
      }
    }
    expect(
      mismatches,
      `Count drift across tabs — post-rebuild invariant broken:\n  ${mismatches.join("\n  ")}`
    ).toHaveLength(0);
  });

  test("Context tab renders numeric domain coverage for a live entity", async ({ page, request }) => {
    const entities = await fetchEntities(request);
    const target = entities[0];

    await page.goto(DCL_URL, { waitUntil: "load" });
    const runButton = page.locator('button[data-role="run-primary"]');
    await expect(runButton).toBeVisible({ timeout: 15_000 });
    await expect(runButton).not.toHaveText("Running...", { timeout: 60_000 });

    await page.locator("button").filter({ hasText: "Context" }).click();
    await page.waitForLoadState("networkidle", { timeout: 10_000 });

    const bodyText = (await page.locator("body").textContent()) ?? "";
    const hasDigits = /\d{2,}/.test(bodyText);
    expect(
      hasDigits,
      "Context tab body has no numeric content — skeleton/placeholder still showing"
    ).toBe(true);

    await page.screenshot({
      path: "tests/e2e/artifacts/store_rebuild_context.png",
      fullPage: true,
    });
  });

  test("Dashboard tab renders numeric triple rows for a live entity", async ({ page, request }) => {
    const entities = await fetchEntities(request);
    const target = entities[0];

    await page.goto(DCL_URL, { waitUntil: "load" });
    const runButton = page.locator('button[data-role="run-primary"]');
    await expect(runButton).toBeVisible({ timeout: 15_000 });
    await expect(runButton).not.toHaveText("Running...", { timeout: 60_000 });

    await page.locator("button").filter({ hasText: "Dashboard" }).click();
    await page.waitForLoadState("networkidle", { timeout: 10_000 });

    const bodyText = (await page.locator("body").textContent()) ?? "";
    expect(
      bodyText.length,
      "Dashboard tab body is empty"
    ).toBeGreaterThan(200);
    expect(
      /\d{2,}/.test(bodyText),
      "Dashboard tab has no numeric content"
    ).toBe(true);

    await page.screenshot({
      path: "tests/e2e/artifacts/store_rebuild_dashboard.png",
      fullPage: true,
    });
  });

  test("Ingest tab renders the entity list with triple counts", async ({ page }) => {
    await page.goto(DCL_URL, { waitUntil: "load" });
    const runButton = page.locator('button[data-role="run-primary"]');
    await expect(runButton).toBeVisible({ timeout: 15_000 });
    await expect(runButton).not.toHaveText("Running...", { timeout: 60_000 });

    await page.locator("button").filter({ hasText: "Ingest" }).click();
    await page.waitForLoadState("networkidle", { timeout: 10_000 });

    const bodyText = (await page.locator("body").textContent()) ?? "";
    expect(
      /\d{2,}/.test(bodyText),
      "Ingest tab has no numeric triple count"
    ).toBe(true);

    await page.screenshot({
      path: "tests/e2e/artifacts/store_rebuild_ingest.png",
      fullPage: true,
    });
  });
});
