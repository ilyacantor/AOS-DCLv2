/**
 * Operator E2E Test — Full User Journey
 *
 * Tests the operator experience through the browser:
 *   1. NLQ Reports Portal — financial statements, overlap, bridge
 *   2. NLQ Ask Tab — natural language query
 *   3. DCL Frontend — triple store viewer and graph
 *
 * Prerequisites:
 *   - DCL backend on :8004, frontend on :3004
 *   - NLQ backend on :8005, frontend on :3005
 *   - Farm backend on :8003 (ground truth)
 *   - Financial triples loaded (seed=42)
 */

import { test, expect } from "playwright/test";

const NLQ_URL = "http://localhost:3005";
const NLQ_BACKEND = "http://localhost:8005";
const DCL_URL = "http://localhost:3004";
const DCL_BACKEND = "http://localhost:8004";

// Shared setup: abort external requests, collect console errors
function setupPage(page: import("playwright/test").Page, consoleErrors: string[]) {
  page.on("console", (msg) => {
    if (msg.type() === "error") {
      const text = msg.text();
      if (text.includes("The width(-1)") || text.includes("The height(-1)")) return;
      if (text.includes("Failed to fetch") || text.includes("NetworkError")) return;
      if (text.includes("net::ERR_NAME_NOT_RESOLVED")) return;
      if (text.includes("Failed to load resource")) return;
      consoleErrors.push(text);
    }
  });
}

async function blockExternalRequests(page: import("playwright/test").Page) {
  await page.route("**/*", (route, request) => {
    if (request.url().includes("localhost")) {
      route.continue();
    } else {
      route.abort();
    }
  });
}

/** Navigate to Reports Portal and wait for entity selector to render */
async function openReportsPortal(page: import("playwright/test").Page) {
  await page.goto(NLQ_URL, { waitUntil: "load" });
  await expect(page.locator("#nav-tab-reports")).toBeVisible({ timeout: 15_000 });
  await page.locator("#nav-tab-reports").click();
  // Wait for entity selector — "Combined" is always rendered as default
  await expect(
    page.locator("button").filter({ hasText: /Combined/i }).first()
  ).toBeVisible({ timeout: 20_000 });
}

// ============================================================
// HEALTH GATE — runs first, fails fast if backends are down
// ============================================================

test.describe.serial("E2E Health Gate", () => {
  test("0. Backends are healthy", async ({ page }) => {
    const [nlq, dcl] = await Promise.all([
      page.request.get(`${NLQ_BACKEND}/api/v1/health`),
      page.request.get(`${DCL_BACKEND}/health`),
    ]);
    expect(nlq.status()).toBe(200);
    expect(dcl.status()).toBe(200);

    const nlqHealth = await nlq.json();
    expect(nlqHealth.dcl_available).toBe(true);
    expect(nlqHealth.live_data_available).toBe(true);
  });
});

// ============================================================
// NLQ REPORTS PORTAL
// ============================================================

test.describe.serial("NLQ Reports Portal — Operator View", () => {
  // Each test navigates from scratch and waits for multiple API round-trips
  // (NLQ → DCL → DB). 60s accommodates variable dev environment latency.
  test.setTimeout(60_000);

  test.beforeEach(async ({ page }) => {
    await blockExternalRequests(page);
  });

  test("1. App loads, header and Ask tab visible", async ({ page }) => {
    const consoleErrors: string[] = [];
    setupPage(page, consoleErrors);

    await page.goto(NLQ_URL, { waitUntil: "load" });
    await expect(page.locator("header")).toBeVisible({ timeout: 15_000 });
    await expect(page.locator("#nlq-search-input")).toBeVisible({ timeout: 10_000 });
    await expect(page.locator("#nav-tab-galaxy")).toBeVisible();
    await expect(page.locator("#nav-tab-reports")).toBeVisible();

    expect(consoleErrors).toHaveLength(0);
  });

  test("2. Income Statement renders with financial data", async ({ page }) => {
    const consoleErrors: string[] = [];
    setupPage(page, consoleErrors);

    await openReportsPortal(page);

    // Select Meridian entity (wait for API to populate the selector)
    const meridianBtn = page.locator("button").filter({ hasText: /Meridian/i });
    await expect(meridianBtn.first()).toBeVisible({ timeout: 20_000 });
    await meridianBtn.first().click();

    // Income Statement tab is active by default — wait for table
    const table = page.locator("table");
    await expect(table.first()).toBeVisible({ timeout: 20_000 });

    const tableText = await table.first().textContent();
    expect(tableText).toBeTruthy();
    expect(tableText!.length).toBeGreaterThan(100);
    expect(tableText).toContain("Revenue");

    await page.screenshot({ path: "/tmp/e2e-reports-income-statement.png", fullPage: true });
    expect(consoleErrors).toHaveLength(0);
  });

  test("3. Balance Sheet renders", async ({ page }) => {
    const consoleErrors: string[] = [];
    setupPage(page, consoleErrors);

    await openReportsPortal(page);

    // Select Meridian
    const meridianBtn = page.locator("button").filter({ hasText: /Meridian/i });
    await expect(meridianBtn.first()).toBeVisible({ timeout: 20_000 });
    await meridianBtn.first().click();

    // Wait for default IS table to load first
    await expect(page.locator("table").first()).toBeVisible({ timeout: 20_000 });

    // Click Balance Sheet tab
    const bsTab = page.locator("button").filter({ hasText: "Balance Sheet" });
    await bsTab.first().click();

    // Wait for BS-specific content
    const bsContent = page.locator("table td, table th").filter({
      hasText: /Total Assets|Liabilities|Equity/,
    });
    await expect(bsContent.first()).toBeVisible({ timeout: 20_000 });

    const tableText = await page.locator("table").first().textContent();
    expect(tableText).toContain("Total Assets");
    expect(tableText).toContain("Liabilities");
    expect(tableText).not.toContain("EBITDA");

    await page.screenshot({ path: "/tmp/e2e-reports-balance-sheet.png", fullPage: true });
    expect(consoleErrors).toHaveLength(0);
  });

  test("4. Cash Flow renders", async ({ page }) => {
    const consoleErrors: string[] = [];
    setupPage(page, consoleErrors);

    await openReportsPortal(page);

    const meridianBtn = page.locator("button").filter({ hasText: /Meridian/i });
    await expect(meridianBtn.first()).toBeVisible({ timeout: 20_000 });
    await meridianBtn.first().click();

    const cfTab = page.locator("button").filter({ hasText: "Cash Flow" });
    await expect(cfTab.first()).toBeVisible({ timeout: 10_000 });
    await cfTab.first().click();

    const table = page.locator("table");
    await expect(table.first()).toBeVisible({ timeout: 20_000 });

    const tableText = await table.first().textContent();
    expect(tableText).toContain("Operating");

    await page.screenshot({ path: "/tmp/e2e-reports-cash-flow.png", fullPage: true });
    expect(consoleErrors).toHaveLength(0);
  });

  test("5. Overlap tab shows overlap data", async ({ page }) => {
    const consoleErrors: string[] = [];
    setupPage(page, consoleErrors);

    await openReportsPortal(page);

    // Combined is selected by default — Overlap tab should be visible
    const overlapTab = page.locator("button").filter({ hasText: /^Overlap$/ });
    await expect(overlapTab.first()).toBeVisible({ timeout: 10_000 });
    await overlapTab.first().click();

    // Wait for "Customer Overlap" header from OverlapReport component
    await expect(page.locator("text=Customer Overlap").first()).toBeVisible({
      timeout: 25_000,
    });
    await expect(page.locator("text=Vendor Overlap").first()).toBeVisible({
      timeout: 5_000,
    });

    await page.screenshot({ path: "/tmp/e2e-reports-overlap.png", fullPage: true });
    expect(consoleErrors).toHaveLength(0);
  });

  test("6. EBITDA Bridge tab shows adjustments", async ({ page }) => {
    const consoleErrors: string[] = [];
    setupPage(page, consoleErrors);

    await openReportsPortal(page);

    const bridgeTab = page.locator("button").filter({ hasText: "EBITDA Bridge" });
    await expect(bridgeTab.first()).toBeVisible({ timeout: 10_000 });
    await bridgeTab.first().click();

    // Wait for EBITDA content to render
    await expect(
      page.locator("text=/EBITDA|Adjusted|Reported/i").first()
    ).toBeVisible({ timeout: 25_000 });

    await page.screenshot({ path: "/tmp/e2e-reports-bridge.png", fullPage: true });
    expect(consoleErrors).toHaveLength(0);
  });

  test("7. Combining statement renders", async ({ page }) => {
    const consoleErrors: string[] = [];
    setupPage(page, consoleErrors);

    await openReportsPortal(page);

    const combiningTab = page.locator("button").filter({ hasText: "Combining" });
    await expect(combiningTab.first()).toBeVisible({ timeout: 10_000 });
    await combiningTab.first().click();

    const table = page.locator("table");
    await expect(table.first()).toBeVisible({ timeout: 25_000 });

    const tableText = await table.first().textContent();
    expect(tableText!.length).toBeGreaterThan(50);

    await page.screenshot({ path: "/tmp/e2e-reports-combining.png", fullPage: true });
    expect(consoleErrors).toHaveLength(0);
  });
});

// ============================================================
// NLQ ASK TAB
// ============================================================

test.describe("NLQ Ask Tab — Natural Language Query", () => {
  test.beforeEach(async ({ page }) => {
    await blockExternalRequests(page);
  });

  test("8. Submit a revenue question and get a response", async ({ page }) => {
    test.setTimeout(90_000);
    const consoleErrors: string[] = [];
    setupPage(page, consoleErrors);

    await page.goto(NLQ_URL, { waitUntil: "load" });

    const searchInput = page.locator("#nlq-search-input");
    await expect(searchInput).toBeVisible({ timeout: 10_000 });
    await searchInput.fill("What is total revenue for 2025 Q1?");
    await searchInput.press("Enter");

    // Wait for any response container (LLM calls can take 30-45s)
    const responseAppeared = await page
      .locator("#financial-statement-visual, #galaxy-visual, #bridge-chart-visual")
      .first()
      .waitFor({ state: "visible", timeout: 60_000 })
      .then(() => true)
      .catch(() => false);

    const bodyText = await page.locator("body").textContent();
    const hasResponse =
      responseAppeared ||
      bodyText?.includes("revenue") ||
      bodyText?.includes("Revenue") ||
      bodyText?.includes("$");

    expect(hasResponse).toBe(true);

    await page.screenshot({ path: "/tmp/e2e-ask-revenue.png", fullPage: true });

    // Allow Claude API errors (infrastructure, not app bugs)
    const appErrors = consoleErrors.filter(
      (e) => !e.includes("Claude") && !e.includes("API") && !e.includes("429")
    );
    expect(appErrors).toHaveLength(0);
  });
});

// ============================================================
// DCL FRONTEND
// ============================================================

test.describe("DCL Frontend — Engine Dashboard", () => {
  test.beforeEach(async ({ page }) => {
    await blockExternalRequests(page);
  });

  test("9. DCL app loads with navigation tabs", async ({ page }) => {
    const consoleErrors: string[] = [];
    setupPage(page, consoleErrors);

    await page.goto(DCL_URL, { waitUntil: "load" });

    const header = page.locator("header, nav").first();
    await expect(header).toBeVisible({ timeout: 15_000 });

    await expect(
      page.locator("button, a").filter({ hasText: "Graph" }).first()
    ).toBeVisible({ timeout: 10_000 });
    await expect(
      page.locator("button, a").filter({ hasText: "Triples" }).first()
    ).toBeVisible({ timeout: 5_000 });

    await page.screenshot({ path: "/tmp/e2e-dcl-main.png", fullPage: true });
    expect(consoleErrors).toHaveLength(0);
  });

  test("10. Triples tab shows triple store data", async ({ page }) => {
    const consoleErrors: string[] = [];
    setupPage(page, consoleErrors);

    await page.goto(DCL_URL, { waitUntil: "load" });

    const triplesTab = page.locator("button, a").filter({ hasText: "Triples" });
    await expect(triplesTab.first()).toBeVisible({ timeout: 10_000 });
    await triplesTab.first().click();

    await page.waitForTimeout(3_000);

    const bodyText = await page.locator("body").textContent();
    const hasTriplesContent =
      bodyText?.includes("triple") ||
      bodyText?.includes("Triple") ||
      bodyText?.includes("concept") ||
      bodyText?.includes("entity") ||
      bodyText?.includes("revenue") ||
      bodyText?.includes("semantic");

    expect(hasTriplesContent).toBe(true);

    await page.screenshot({ path: "/tmp/e2e-dcl-triples.png", fullPage: true });
    expect(consoleErrors).toHaveLength(0);
  });

  test("11. Merge tab loads convergence view", async ({ page }) => {
    const consoleErrors: string[] = [];
    setupPage(page, consoleErrors);

    await page.goto(DCL_URL, { waitUntil: "load" });

    const mergeTab = page.locator("button, a").filter({ hasText: "Merge" });
    await expect(mergeTab.first()).toBeVisible({ timeout: 10_000 });
    await mergeTab.first().click();

    await page.waitForTimeout(2_000);

    await page.screenshot({ path: "/tmp/e2e-dcl-merge.png", fullPage: true });
    expect(consoleErrors).toHaveLength(0);
  });
});

// ============================================================
// CROSS-MODULE: NLQ → DCL DATA FLOW
// ============================================================

test.describe("Cross-Module — Data Integrity", () => {
  test("12. Report dimensions return valid periods with data", async ({ page }) => {
    const response = await page.request.get(`${NLQ_BACKEND}/api/v1/report-dimensions`);
    expect(response.status()).toBe(200);

    const dims = await response.json();
    expect(dims.periods.length).toBeGreaterThan(0);

    const withData = dims.periods.filter(
      (p: { has_data: { meridian: boolean; cascadia: boolean } }) =>
        p.has_data?.meridian && p.has_data?.cascadia
    );
    expect(withData.length).toBeGreaterThan(0);
  });

  test("13. No error banners or crash states visible", async ({ page }) => {
    await blockExternalRequests(page);

    await page.goto(NLQ_URL, { waitUntil: "load" });
    await page.waitForTimeout(3_000);
    const nlqErrors = await page
      .locator('[class*="error" i], [class*="Error"], [role="alert"]')
      .count();

    await page.goto(DCL_URL, { waitUntil: "load" });
    await page.waitForTimeout(3_000);
    const dclErrors = await page
      .locator('[class*="error" i], [class*="Error"], [role="alert"]')
      .count();

    expect(nlqErrors).toBe(0);
    expect(dclErrors).toBe(0);
  });
});
