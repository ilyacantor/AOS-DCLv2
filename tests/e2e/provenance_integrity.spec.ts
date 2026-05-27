// Operator-visible outcome: after a Console SE pipeline run, the entity that
// just ran appears in DCL's entity dropdown (Ingest, Graph, Dashboard, Context,
// Recon tabs) and in NLQ's snapshot dropdown / dashboard — with correct data,
// no cross-entity bleed, and no blank/error states.

import { test, expect, type Page } from "playwright/test";

const CONSOLE_URL = "http://localhost:3009";
const DCL_URL = "http://localhost:3004";
const NLQ_URL = "http://localhost:3005";
const SCREENSHOTS = "tests/e2e/screenshots";

test.describe.serial(
  "Provenance integrity — Console → DCL → NLQ",
  () => {
    test.setTimeout(300_000);

    let expectedEntity: string;

    // ── Phase 1: Run SE pipeline via Console, capture entity name ──

    test("01. Console — run SE pipeline and capture entity", async ({
      page,
    }) => {
      await page.goto(`${CONSOLE_URL}/pipeline`, {
        waitUntil: "domcontentloaded",
      });

      const runBtn = page.locator("button").filter({ hasText: /Run SE/i });
      await expect(runBtn.first()).toBeVisible({ timeout: 15_000 });
      await runBtn.first().click();

      // Wait for pipeline completion — look for green checkmarks or completion text
      await expect(
        page.locator("text=/completed|success|✓/i").first()
      ).toBeVisible({ timeout: 120_000 });

      // Capture entity name from the first pipeline step card
      const stepCard = page.locator("[class*='card'], [class*='step'], [class*='pipeline']").first();
      const cardText = (await stepCard.textContent()) || "";
      // Entity names follow the pattern: Word-XXXX or Word-XXXX-hash
      const entityMatch = cardText.match(
        /([A-Z][a-zA-Z]+-[A-Z0-9]{3,6}(?:-[a-f0-9]{4})?)/
      );
      expect(
        entityMatch?.[1] ?? "",
        `Could not extract entity name from pipeline card: "${cardText.slice(0, 200)}"`
      ).toMatch(/^[A-Z][a-zA-Z]+-[A-Z0-9]{3,6}/);
      expectedEntity = entityMatch![1];

      await page.screenshot({ path: `${SCREENSHOTS}/01_console_pipeline_complete.png` });
    });

    // ── Phase 2: DCL Ingest tab ──

    test("02. DCL Ingest — entity visible in dropdown", async ({ page }) => {
      await page.goto(DCL_URL, { waitUntil: "domcontentloaded" });

      const ingestTab = page.locator("button").filter({ hasText: "Ingest" });
      await expect(ingestTab.first()).toBeVisible({ timeout: 15_000 });
      await ingestTab.first().click();

      // Snapshot dropdown should load and contain a snapshot for expectedEntity.
      // Snapshot option text is the snapshot_name (e.g. VeloCorp-KDDN-c712),
      // which embeds the entity_id.
      const dropdown = page.locator("#snapshot-selector");
      await expect(dropdown).toBeVisible({ timeout: 15_000 });

      // Wait for options to populate (placeholder "No snapshots" is a single option).
      await expect(
        dropdown.locator("option")
      ).not.toHaveCount(1, { timeout: 15_000 });

      const options = await dropdown.locator("option").allTextContents();
      const hasEntity = options.some((opt) =>
        opt.includes(expectedEntity.split("-").slice(0, 2).join("-"))
      );
      expect(
        hasEntity,
        `Expected a snapshot for entity ${expectedEntity} in Ingest dropdown, found: ${options.join(", ")}`
      ).toBe(true);

      await page.screenshot({ path: `${SCREENSHOTS}/02_dcl_ingest.png` });
    });

    // ── Phase 3: DCL Graph tab ──

    test("03. DCL Graph — entity in dropdown, provenance label, Sankey renders", async ({
      page,
    }) => {
      await page.goto(DCL_URL, { waitUntil: "domcontentloaded" });

      const graphTab = page.locator("button").filter({ hasText: "Graph" });
      await expect(graphTab.first()).toBeVisible({ timeout: 15_000 });
      await graphTab.first().click();

      const dropdown = page.locator("#snapshot-selector");
      await expect(dropdown).toBeVisible({ timeout: 15_000 });

      // Find the snapshot option whose name embeds the entity prefix, then
      // select it by its dcl_ingest_id value.
      const entityPrefix = expectedEntity.split("-").slice(0, 2).join("-");
      const allOptions = await dropdown.locator("option").all();
      let targetValue = "";
      let targetText = "";
      for (const opt of allOptions) {
        const text = (await opt.textContent()) || "";
        if (text.includes(entityPrefix)) {
          targetValue = (await opt.getAttribute("value")) || "";
          targetText = text;
          break;
        }
      }
      expect(targetText, `No snapshot for entity ${entityPrefix} in Graph dropdown`).toContain(entityPrefix);
      expect(targetValue, "matched snapshot option must have a dcl_ingest_id value").toBeTruthy();
      await dropdown.selectOption(targetValue);

      // Provenance label must reference the entity
      const provLabel = page.locator(
        "span.text-muted-foreground.font-mono:not([class*='min-w'])"
      );
      await expect(provLabel).toContainText(entityPrefix, { timeout: 30_000 });

      // Sankey must render — SVG with nodes
      const svgOrCanvas = page.locator("svg, canvas").first();
      await expect(svgOrCanvas).toBeVisible({ timeout: 30_000 });

      await page.screenshot({ path: `${SCREENSHOTS}/03_dcl_graph.png` });
    });

    // ── Phase 4: DCL Dashboard tab ──

    test("04. DCL Dashboard — data visible, no empty state", async ({
      page,
    }) => {
      await page.goto(DCL_URL, { waitUntil: "domcontentloaded" });

      const dashTab = page.locator("button").filter({ hasText: "Dashboard" });
      await expect(dashTab.first()).toBeVisible({ timeout: 15_000 });
      await dashTab.first().click();

      // Select the snapshot for the entity via the snapshot dropdown.
      const dropdown = page.locator("#snapshot-selector");
      if (await dropdown.isVisible()) {
        const entityPrefix = expectedEntity.split("-").slice(0, 2).join("-");
        const allOptions = await dropdown.locator("option").all();
        for (const opt of allOptions) {
          const text = (await opt.textContent()) || "";
          if (text.includes(entityPrefix)) {
            const val = await opt.getAttribute("value") || "";
            if (val) await dropdown.selectOption(val);
            break;
          }
        }
      }

      // Must show data — no "No triples match filters"
      const body = page.locator("body");
      const bodyText = (await body.textContent()) || "";
      expect(
        bodyText,
        "Dashboard shows 'No triples match filters' empty state"
      ).not.toContain("No triples match filters");

      await page.screenshot({ path: `${SCREENSHOTS}/04_dcl_dashboard.png` });
    });

    // ── Phase 5: DCL Context tab ──

    test("05. DCL Context — page loads without errors", async ({ page }) => {
      const consoleErrors: string[] = [];
      page.on("console", (msg) => {
        if (msg.type() === "error" && msg.text().includes("/api/")) {
          consoleErrors.push(msg.text());
        }
      });

      await page.goto(DCL_URL, { waitUntil: "domcontentloaded" });

      const ctxTab = page.locator("button").filter({ hasText: "Context" });
      await expect(ctxTab.first()).toBeVisible({ timeout: 15_000 });
      await ctxTab.first().click();

      // Wait for content to load
      await page.waitForTimeout(3_000);

      expect(
        consoleErrors.filter((e) => e.includes("500")),
        `Context tab had HTTP 500 errors: ${consoleErrors.join("; ")}`
      ).toHaveLength(0);

      await page.screenshot({ path: `${SCREENSHOTS}/05_dcl_context.png` });
    });

    // ── Phase 6: DCL Recon tab ──

    test("06. DCL Recon — page loads without errors", async ({ page }) => {
      const consoleErrors: string[] = [];
      page.on("console", (msg) => {
        if (msg.type() === "error" && msg.text().includes("/api/")) {
          consoleErrors.push(msg.text());
        }
      });

      await page.goto(DCL_URL, { waitUntil: "domcontentloaded" });

      const reconTab = page.locator("button").filter({ hasText: "Recon" });
      await expect(reconTab.first()).toBeVisible({ timeout: 15_000 });
      await reconTab.first().click();

      await page.waitForTimeout(3_000);

      expect(
        consoleErrors.filter((e) => e.includes("500")),
        `Recon tab had HTTP 500 errors: ${consoleErrors.join("; ")}`
      ).toHaveLength(0);

      await page.screenshot({ path: `${SCREENSHOTS}/06_dcl_recon.png` });
    });

    // ── Phase 7: NLQ Ask ──

    test("07. NLQ Ask — snapshot references entity, query returns substantive answer", async ({
      page,
    }) => {
      await page.goto(NLQ_URL, { waitUntil: "domcontentloaded" });

      // Click Ask tab if present
      const askTab = page.locator("button").filter({ hasText: /^Ask$/i });
      if (await askTab.first().isVisible({ timeout: 5_000 }).catch(() => false)) {
        await askTab.first().click();
      }

      // Snapshot dropdown must reference expectedEntity
      const entityPrefix = expectedEntity.split("-").slice(0, 2).join("-");
      const snapshotDropdown = page.locator("select").first();
      if (await snapshotDropdown.isVisible({ timeout: 10_000 }).catch(() => false)) {
        const selectedText = await snapshotDropdown.locator("option:checked").textContent();
        expect(
          selectedText || "",
          `NLQ snapshot dropdown does not reference ${entityPrefix}`
        ).toContain(entityPrefix);
      }

      // Type and submit a query
      const input = page.locator("input[type='text'], textarea").first();
      await expect(input).toBeVisible({ timeout: 10_000 });
      await input.fill("why did rev incr");
      await input.press("Enter");

      // Wait for response
      const responseArea = page.locator("[class*='answer'], [class*='response'], [class*='result'], main p, main div").first();
      await expect(responseArea).toBeVisible({ timeout: 30_000 });

      const responseText = (await page.locator("body").textContent()) || "";
      expect(responseText, "NLQ returned 'head-scratcher'").not.toContain(
        "head-scratcher"
      );
      expect(responseText, "NLQ returned '0 nodes'").not.toContain("0 nodes");

      await page.screenshot({ path: `${SCREENSHOTS}/07_nlq_ask.png` });
    });

    // ── Phase 8: NLQ Dashboard ──

    test("08. NLQ Dashboard — entity in dropdown, revenue/margin/pipeline data present", async ({
      page,
    }) => {
      await page.goto(NLQ_URL, { waitUntil: "domcontentloaded" });

      const dashTab = page.locator("button").filter({ hasText: /Dashboard/i });
      if (await dashTab.first().isVisible({ timeout: 5_000 }).catch(() => false)) {
        await dashTab.first().click();
      }

      const entityPrefix = expectedEntity.split("-").slice(0, 2).join("-");

      // Entity/snapshot dropdown should reference expectedEntity
      const dropdown = page.locator("select").first();
      if (await dropdown.isVisible({ timeout: 10_000 }).catch(() => false)) {
        const options = await dropdown.locator("option").allTextContents();
        const hasEntity = options.some((opt) => opt.includes(entityPrefix));
        expect(
          hasEntity,
          `NLQ dashboard dropdown does not contain ${entityPrefix}. Options: ${options.join(", ")}`
        ).toBe(true);
      }

      // Wait for dashboard cards to render
      await page.waitForTimeout(5_000);
      const bodyText = (await page.locator("body").textContent()) || "";

      // Revenue card must show a dollar value, not "No data"
      expect(bodyText, "NLQ dashboard shows 'No data for revenue'").not.toMatch(
        /No data for ['"]?revenue/i
      );

      // Gross margin card must show a percentage, not "No data"
      expect(bodyText, "NLQ dashboard shows 'No data for gross_margin'").not.toMatch(
        /No data for ['"]?gross.margin/i
      );

      // Sales pipeline must show stage data
      expect(
        bodyText,
        "NLQ dashboard shows 'No pipeline stages found'"
      ).not.toContain("No pipeline stages found");

      await page.screenshot({ path: `${SCREENSHOTS}/08_nlq_dashboard.png` });
    });
  }
);
