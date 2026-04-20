// Operator-visible outcome: Console SE batch pipeline produces a new entity.
// That entity_id appears in every DCL tab (Graph, Dashboard, Context, Ingest)
// and in NLQ (Dashboard, Ask) with matching provenance. >20,000 triples in
// Context tab. NLQ "what is margin" returns numeric values, not an error.

import { test, expect } from "playwright/test";

const CONSOLE = "http://localhost:3009";
const DCL = "http://localhost:3004";
const NLQ = "http://localhost:3005";

test.describe.serial(
  "SE pipeline → DCL → NLQ — entity provenance across all surfaces",
  () => {
    let entityId: string;

    // ── Step 1 — Console SE Pipeline ──────────────────────────

    test("Step 1 — Console SE Pipeline completes green", async ({ page }) => {
      test.setTimeout(300_000);

      await page.goto(`${CONSOLE}/pipeline`, {
        waitUntil: "domcontentloaded",
      });
      const m = page.getByRole("main");

      await m.getByRole("button", { name: "SE", exact: true }).click();
      await m.getByRole("button", { name: "Batch", exact: true }).click();
      await m.getByRole("button", { name: /Run SE/i }).click();

      await expect(m.getByText("SE Mode")).toBeVisible({ timeout: 15_000 });

      await expect(m.getByText(/Pipeline completed/i).first()).toBeVisible({
        timeout: 270_000,
      });

      const body = (await m.textContent()) ?? "";
      expect(body).not.toContain("Pipeline stopped");

      await page.screenshot({
        path: "tests/e2e/screenshots/step1_console_pipeline.png",
        fullPage: true,
      });
    });

    // ── Helper: wait for DCL auto-run to settle ─────────────

    async function waitForDclReady(page: import("playwright/test").Page) {
      await page.goto(DCL, { waitUntil: "load" });
      const runButton = page.locator('button[data-role="run-primary"]');
      await expect(runButton).toBeVisible({ timeout: 15_000 });
      await expect(runButton).not.toHaveText("Running...", {
        timeout: 60_000,
      });
      const entitySelect = page.locator("select").first();
      await expect(entitySelect).toBeVisible({ timeout: 15_000 });
      await expect(async () => {
        const optCount = await entitySelect.locator("option").count();
        expect(optCount).toBeGreaterThan(1);
      }).toPass({ timeout: 15_000 });
      return entitySelect;
    }

    // ── Step 2 — DCL / Graph ──────────────────────────────────

    test("Step 2 — DCL Graph shows entity with matching provenance", async ({
      page,
    }) => {
      test.setTimeout(120_000);

      const entitySelect = await waitForDclReady(page);

      const options = entitySelect.locator("option");
      const count = await options.count();
      for (let i = 0; i < count; i++) {
        const text = (await options.nth(i).textContent()) ?? "";
        if (text.includes("*")) {
          entityId = (await options.nth(i).getAttribute("value")) ?? "";
          break;
        }
      }
      expect(
        entityId,
        "Most-recent entity (marked *) must exist in dropdown",
      ).toBeTruthy();

      await entitySelect.selectOption(entityId);

      await expect(
        page.locator('button[data-role="run-primary"]'),
      ).not.toHaveText("Running...", { timeout: 60_000 });

      const graph = page.locator(
        'svg[role="img"][aria-label="Data-driven graph of DCL triple flow"]',
      );
      await expect(graph).toBeVisible({ timeout: 30_000 });

      const provenance = page.locator("span.font-mono");
      await expect(provenance.first()).toContainText(entityId, {
        timeout: 15_000,
      });

      await page.screenshot({
        path: "tests/e2e/screenshots/step2_dcl_graph.png",
        fullPage: true,
      });
    });

    // ── Step 3 — DCL / Dashboard ──────────────────────────────

    test("Step 3 — DCL Dashboard renders data for entity", async ({
      page,
    }) => {
      test.setTimeout(90_000);

      const entitySelect = await waitForDclReady(page);
      await entitySelect.selectOption(entityId);

      await page.locator("button").filter({ hasText: "Dashboard" }).click();

      await expect(page.getByText(/\d+.*triples/i).first()).toBeVisible({
        timeout: 30_000,
      });

      const rows = page.locator("table tbody tr");
      await expect(rows.first()).toBeVisible({ timeout: 15_000 });

      const selectedVal = await entitySelect.inputValue();
      expect(selectedVal).toBe(entityId);

      await page.screenshot({
        path: "tests/e2e/screenshots/step3_dcl_dashboard.png",
        fullPage: true,
      });
    });

    // ── Step 4 — DCL / Context ────────────────────────────────

    test("Step 4 — DCL Context shows >20,000 triples", async ({ page }) => {
      test.setTimeout(90_000);

      const entitySelect = await waitForDclReady(page);
      await entitySelect.selectOption(entityId);

      await page.locator("button").filter({ hasText: "Context" }).click();

      // Wait for actual data to load — source system names prove the API responded
      await expect(
        page.getByText("Source Systems").first(),
      ).toBeVisible({ timeout: 30_000 });
      await expect(
        page.locator("table tbody tr").first(),
      ).toBeVisible({ timeout: 15_000 });

      // Read the Triples MetricCard value.
      // Structure: <div><div class="text-xs...">Triples</div><div class="text-lg font-semibold...">20,201</div></div>
      // Use retry loop since the metric may initially render as 0 before the API responds.
      let tripleCount = 0;
      await expect(async () => {
        const triplesLabel = page
          .getByText("Triples", { exact: true })
          .first();
        const card = triplesLabel.locator("xpath=..");
        const valueText =
          (await card.locator(".font-semibold").first().textContent()) ?? "0";
        tripleCount = parseInt(valueText.replace(/,/g, ""), 10);
        expect(tripleCount).toBeGreaterThan(0);
      }).toPass({ timeout: 30_000 });

      expect(
        tripleCount,
        `Context tab Triples metric must be >20,000; got ${tripleCount}`,
      ).toBeGreaterThan(20_000);

      await page.screenshot({
        path: "tests/e2e/screenshots/step4_dcl_context.png",
        fullPage: true,
      });
    });

    // ── Step 5 — DCL / Ingest ─────────────────────────────────

    test("Step 5 — DCL Ingest shows entity as latest entry", async ({
      page,
    }) => {
      test.setTimeout(90_000);

      const entitySelect = await waitForDclReady(page);
      await entitySelect.selectOption(entityId);

      await page.locator("button").filter({ hasText: "Ingest" }).click();

      await expect(
        page.getByText("Last Ingest").first(),
      ).toBeVisible({ timeout: 15_000 });

      const firstRow = page.locator("table tbody tr").first();
      await expect(firstRow).toBeVisible({ timeout: 15_000 });
      await expect(firstRow).toContainText(entityId);

      await page.screenshot({
        path: "tests/e2e/screenshots/step5_dcl_ingest.png",
        fullPage: true,
      });
    });

    // ── Step 6 — NLQ Dashboard ────────────────────────────────

    test("Step 6 — NLQ Dashboard loads with zero errors, entity in provenance", async ({
      page,
    }) => {
      test.setTimeout(60_000);

      await page.goto(NLQ, { waitUntil: "load" });

      await page.locator("#nav-tab-dashboard").click();

      const entitySelector = page.locator("#dashboard-entity-selector");
      await expect(entitySelector).toBeVisible({ timeout: 15_000 });

      const entityOption = entitySelector.locator(
        `option[value="${entityId}"]`,
      );
      await expect(
        entityOption,
        `Entity ${entityId} must appear in NLQ dashboard dropdown`,
      ).toBeAttached({ timeout: 15_000 });

      await entitySelector.selectOption(entityId);

      await expect(
        page.locator(".react-grid-layout").first(),
      ).toBeVisible({ timeout: 30_000 });

      const errorBanner = page.locator(
        '[class*="bg-red-900"], [role="alert"]',
      );
      const errorCount = await errorBanner.count();
      expect(errorCount, "NLQ Dashboard must have zero error cards").toBe(0);

      await page.screenshot({
        path: "tests/e2e/screenshots/step6_nlq_dashboard.png",
        fullPage: true,
      });
    });

    // ── Step 7 — NLQ Ask ──────────────────────────────────────

    test('Step 7 — NLQ Ask "what is margin" returns data, entity on surface', async ({
      page,
    }) => {
      test.setTimeout(120_000);

      await page.goto(NLQ, { waitUntil: "load" });

      await page.locator("#nav-tab-galaxy").click();

      const snapshotSelector = page.locator("#snapshot-selector");
      if (await snapshotSelector.isVisible({ timeout: 10_000 }).catch(() => false)) {
        const selectorText = (await snapshotSelector.textContent()) ?? "";
        expect(
          selectorText,
          `Snapshot selector must contain entity ${entityId}`,
        ).toContain(entityId);
      }

      const input = page.locator("#nlq-search-input");
      await expect(input).toBeVisible({ timeout: 10_000 });
      await input.fill("what is margin");
      await input.press("Enter");

      const responseArea = page
        .locator(
          "#financial-statement-visual, #galaxy-visual, #sales-funnel-visual",
        )
        .first();
      await expect(responseArea).toBeVisible({ timeout: 90_000 });

      const responseText = (await responseArea.textContent()) ?? "";
      const hasNumbers = /\d+\.?\d*/.test(responseText);
      expect(
        hasNumbers,
        `NLQ response must contain numeric values; got: "${responseText.slice(0, 200)}"`,
      ).toBe(true);
      expect(responseText.toLowerCase()).not.toContain("cannot reach");
      expect(responseText.toLowerCase()).not.toContain("error");

      const pageText = (await page.locator("body").textContent()) ?? "";
      expect(
        pageText,
        `Entity ${entityId} must be visible on NLQ surface`,
      ).toContain(entityId);

      await page.screenshot({
        path: "tests/e2e/screenshots/step7_nlq_ask.png",
        fullPage: true,
      });
    });
  },
);
