// Operator-visible outcome: selecting entity A in the Graph tab entity dropdown
// shows entity A's snapshot name in the provenance label; switching to entity B
// shows entity B's snapshot name. No cross-entity bleed.

import { test, expect } from "playwright/test";

const DCL_BACKEND = "http://localhost:8004";

test.describe.serial("Snapshot isolation — no cross-entity bleed", () => {
  test.setTimeout(90_000);

  let entityA: { entity_id: string; display_name: string };
  let entityB: { entity_id: string; display_name: string };

  test("0. Ground truth — pick two entities with data", async ({ page }) => {
    const res = await page.request.get(`${DCL_BACKEND}/api/dcl/entities`);
    expect(res.status()).toBe(200);
    const data = await res.json();
    const entities: any[] = data.entities || [];
    const viable = entities.filter((e: any) => e.triple_count > 1000);
    expect(
      viable.length,
      "Need at least 2 entities with triples for isolation test"
    ).toBeGreaterThanOrEqual(2);

    entityA = {
      entity_id: viable[0].entity_id,
      display_name: viable[0].display_name,
    };
    entityB = {
      entity_id: viable[1].entity_id,
      display_name: viable[1].display_name,
    };

    expect(entityA.entity_id).not.toEqual(entityB.entity_id);
  });

  test("1. Select entity A — provenance label shows entity A", async ({
    page,
  }) => {
    await page.goto("http://localhost:3004", { waitUntil: "domcontentloaded" });

    const graphTab = page.locator("button", { hasText: "Graph" });
    await graphTab.click();

    const dropdown = page.locator("select").first();
    await dropdown.waitFor({ state: "visible", timeout: 15_000 });
    await dropdown.selectOption(entityA.entity_id);

    const provenanceLabel = page.locator(
      "span.text-muted-foreground.font-mono:not([class*='min-w'])"
    );
    await expect(provenanceLabel).toContainText(entityA.entity_id, {
      timeout: 30_000,
    });
    await expect(provenanceLabel).not.toContainText(entityB.entity_id);

    await page.screenshot({
      path: "tests/e2e/screenshots/snapshot_isolation_entityA.png",
    });
  });

  test("2. Select entity B — provenance label shows entity B, not A", async ({
    page,
  }) => {
    await page.goto("http://localhost:3004", { waitUntil: "domcontentloaded" });

    const graphTab = page.locator("button", { hasText: "Graph" });
    await graphTab.click();

    const dropdown = page.locator("select").first();
    await dropdown.waitFor({ state: "visible", timeout: 15_000 });
    await dropdown.selectOption(entityB.entity_id);

    const provenanceLabel = page.locator(
      "span.text-muted-foreground.font-mono:not([class*='min-w'])"
    );
    await expect(provenanceLabel).toContainText(entityB.entity_id, {
      timeout: 30_000,
    });
    await expect(provenanceLabel).not.toContainText(entityA.entity_id);

    await page.screenshot({
      path: "tests/e2e/screenshots/snapshot_isolation_entityB.png",
    });
  });
});
