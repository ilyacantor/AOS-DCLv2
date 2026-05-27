// Operator-visible outcome: selecting snapshot A in the Graph tab snapshot
// dropdown shows snapshot A's entity_id in the provenance label; switching to a
// snapshot for a different entity shows that entity_id instead. No cross-entity bleed.

import { test, expect } from "playwright/test";

const DCL_BACKEND = "http://localhost:8004";

interface Snapshot {
  dcl_ingest_id: string;
  snapshot_name: string | null;
  entity_id: string | null;
  run_timestamp: string;
  total_rows: number;
  is_current: boolean;
}

test.describe.serial("Snapshot isolation — no cross-entity bleed", () => {
  test.setTimeout(90_000);

  let snapA: Snapshot;
  let snapB: Snapshot;

  test("0. Ground truth — pick two snapshots for different entities", async ({ page }) => {
    const res = await page.request.get(`${DCL_BACKEND}/api/dcl/snapshots`);
    expect(res.status()).toBe(200);
    const snaps: Snapshot[] = (await res.json()).snapshots || [];
    // Snapshots with enough triples to render a real graph and a real
    // entity_id. Cap total_rows to skip oversized test-fixture entities
    // whose graph build is slow without adding isolation coverage.
    const viable = snaps.filter(
      (s) => s.total_rows > 1000 && s.total_rows < 60_000 && s.entity_id,
    );
    expect(
      viable.length,
      "Need at least 2 normal-sized snapshots for isolation test",
    ).toBeGreaterThanOrEqual(2);

    snapA = viable[0];
    // First snapshot whose entity differs from snapA's.
    const other = viable.find((s) => s.entity_id !== snapA.entity_id);
    expect(other, "Need two snapshots with distinct entity_ids").toBeTruthy();
    snapB = other!;

    expect(snapA.entity_id).not.toEqual(snapB.entity_id);
    console.log(`[gt] A = ${snapA.snapshot_name} / entity ${snapA.entity_id}`);
    console.log(`[gt] B = ${snapB.snapshot_name} / entity ${snapB.entity_id}`);
  });

  test("1. Select snapshot A — provenance label shows entity A", async ({ page }) => {
    await page.goto("http://localhost:3004", { waitUntil: "domcontentloaded" });

    const graphTab = page.locator("button", { hasText: "Graph" });
    await graphTab.click();

    const dropdown = page.locator("#snapshot-selector");
    await dropdown.waitFor({ state: "visible", timeout: 15_000 });
    await expect(dropdown.locator("option").nth(1)).toBeAttached({ timeout: 15_000 });
    await dropdown.selectOption(snapA.dcl_ingest_id);

    const provenanceLabel = page.locator(
      "span.text-muted-foreground.font-mono:not([class*='min-w'])",
    );
    await expect(provenanceLabel).toContainText(snapA.entity_id!, { timeout: 30_000 });
    await expect(provenanceLabel).not.toContainText(snapB.entity_id!);

    await page.screenshot({
      path: "tests/e2e/screenshots/snapshot_isolation_entityA.png",
    });
  });

  test("2. Select snapshot B — provenance label shows entity B, not A", async ({ page }) => {
    await page.goto("http://localhost:3004", { waitUntil: "domcontentloaded" });

    const graphTab = page.locator("button", { hasText: "Graph" });
    await graphTab.click();

    const dropdown = page.locator("#snapshot-selector");
    await dropdown.waitFor({ state: "visible", timeout: 15_000 });
    await expect(dropdown.locator("option").nth(1)).toBeAttached({ timeout: 15_000 });
    await dropdown.selectOption(snapB.dcl_ingest_id);

    const provenanceLabel = page.locator(
      "span.text-muted-foreground.font-mono:not([class*='min-w'])",
    );
    await expect(provenanceLabel).toContainText(snapB.entity_id!, { timeout: 30_000 });
    await expect(provenanceLabel).not.toContainText(snapA.entity_id!);

    await page.screenshot({
      path: "tests/e2e/screenshots/snapshot_isolation_entityB.png",
    });
  });
});
