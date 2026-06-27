// Operator-visible outcome: all five DCL operator tabs load real aos-dev data with no "Failed to fetch" — Graph shows >1 snapshot option, Dashboard shows the top-domain "(count)" from /api/dcl/dashboard-data, Context renders the entity-scoped Conflict Register, Ingest shows a real dcl_ingest_id prefix from /api/dcl/ingest-log, Monitor shows the live schedule jobs (structural_drift/value_drift).
//
// Single-browser-session spec (one page, sequential tab clicks). This is the
// WSL2-robust restructure prescribed by dcl_deferred_work.md #33: the prior
// describe.serial-with-fresh-pages form wedged headless Chromium on the Graph
// SVG fullPage screenshot and one wedge blocked every sibling. It also drove a
// "Recon" tab that d7c3659 deleted and read ground truth from prod :8004.
//
// The UI (:3004) proxies /api -> :8104 (dcl-dev, aos-dev). Ground truth is read
// from :8104 via page.request.get (read-only GETs, allowed by the B17 carve-out)
// and compared against what renders — never hardcoded.

import { test, expect } from "playwright/test";

const UI = "http://localhost:3004";
const DEV = "http://localhost:8104"; // dcl-dev backend the UI proxies to

test.describe.serial("DCL operator tabs — real dev data, no Failed to fetch", () => {
  test.setTimeout(180_000);

  test("all five tabs render real aos-dev data", async ({ page }) => {
    const errors: string[] = [];
    page.on("console", (m) => {
      if (m.type() !== "error") return;
      const t = m.text();
      if (t.includes("ERR_NAME_NOT_RESOLVED") || t.includes("ERR_BLOCKED_BY_CLIENT")) return;
      if (t.includes("width(-1)") || t.includes("height(-1)")) return;
      errors.push(`console: ${t}`);
    });
    page.on("response", (r) => {
      if (r.url().includes("/api/dcl/") && r.status() >= 500) errors.push(`${r.status()} ${r.url()}`);
    });

    // --- Ground truth from the dev backend (read-only) ---
    const dash = await (await page.request.get(`${DEV}/api/dcl/dashboard-data`)).json();
    const topDomain = dash.aggregations.by_domain[0] as { domain: string; count: number };

    const schedRaw = await (await page.request.get(`${DEV}/api/dcl/monitor/schedule`)).json();
    const jobs: string[] = (Array.isArray(schedRaw) ? schedRaw : schedRaw.jobs ?? schedRaw.schedule ?? [])
      .map((j: any) => j.job_name)
      .filter(Boolean);

    const logRaw = await (await page.request.get(`${DEV}/api/dcl/ingest-log`)).json();
    const logs: any[] = Array.isArray(logRaw) ? logRaw : logRaw.logs ?? logRaw.entries ?? [];
    const firstIngestPrefix: string = (logs[0]?.dcl_ingest_id ?? "").slice(0, 8);

    expect(topDomain.count, "ground truth: top domain has a positive triple count").toBeGreaterThan(1);
    expect(jobs, "ground truth: monitor schedule returned at least one job").not.toEqual([]);
    expect(firstIngestPrefix.length, "ground truth: ingest-log has a dcl_ingest_id").toBe(8);

    // --- Load the app; wait for the on-mount auto-run to settle ---
    await page.goto(UI, { waitUntil: "load" });
    const runBtn = page.locator('button[data-role="run-primary"]');
    await expect(runBtn).toBeVisible({ timeout: 20_000 });
    await expect(runBtn).not.toHaveText("Running...", { timeout: 90_000 });

    const shot = (name: string) =>
      page.screenshot({ path: `tests/e2e/artifacts/verify_${name}.png`, timeout: 8_000 }).catch(() => {});
    const noFetchFail = (tab: string) =>
      expect(page.locator("body"), `"Failed to fetch" rendered on ${tab}`).not.toContainText("Failed to fetch");
    const tab = (label: string) => page.locator("button").filter({ hasText: new RegExp(`^${label}$`) }).first();

    // 1) GRAPH — snapshot dropdown populated from real runs
    await tab("Graph").click();
    const snap = page.locator("#snapshot-selector");
    await expect(snap).toBeVisible({ timeout: 15_000 });
    await expect
      .poll(async () => snap.locator("option").count(), { timeout: 20_000 })
      .toBeGreaterThan(1);
    await noFetchFail("Graph");
    await shot("graph");

    // 2) DASHBOARD — the entity-scoped total from the tab's OWN fetch renders
    // (DashboardTab fetches dashboard-data?entity_id=<selected>&page_size=50;
    // assert the displayed total equals what the backend served — render fidelity).
    const dashRespP = page.waitForResponse(
      (r) => r.url().includes("/api/dcl/dashboard-data") && r.url().includes("page_size="),
      { timeout: 25_000 }
    );
    await tab("Dashboard").click();
    const dashResp = await dashRespP;
    expect(dashResp.status(), "Dashboard's dashboard-data fetch status").toBe(200);
    const dashBody = await dashResp.json();
    const expectedTriples = `${(dashBody.total_count as number).toLocaleString("en-US")} triples`;
    await expect(
      page.getByText(expectedTriples).first(),
      `Dashboard should show "${expectedTriples}" from its own dashboard-data fetch`
    ).toBeVisible({ timeout: 20_000 });
    await noFetchFail("Dashboard");
    await shot("dashboard");

    // 3) CONTEXT — entity-scoped Conflict Register surface loads
    await tab("Context").click();
    await expect(page.locator('[data-testid="conflicts-panel"]')).toBeVisible({ timeout: 20_000 });
    await expect(page.getByText("Conflict Register").first()).toBeVisible({ timeout: 10_000 });
    await noFetchFail("Context");
    await shot("context");

    // 4) INGEST — a real dcl_ingest_id prefix from ground truth renders in the log table
    await tab("Ingest").click();
    await expect(
      page.getByText(firstIngestPrefix, { exact: false }).first(),
      `Ingest log should show dcl_ingest_id prefix ${firstIngestPrefix}`
    ).toBeVisible({ timeout: 20_000 });
    await noFetchFail("Ingest");
    await shot("ingest");

    // 5) MONITOR — the live schedule jobs render
    await tab("Monitor").click();
    for (const job of jobs) {
      await expect(page.getByText(job).first(), `Monitor should show job ${job}`).toBeVisible({ timeout: 15_000 });
    }
    await noFetchFail("Monitor");
    await shot("monitor");

    // Demo tab removed — the demo now lives in the platform guided tour (step 3)
    // and DCL's standalone /glassbox; it is no longer a console tab.
    await expect(page.getByRole("button", { name: "Demo", exact: true })).toHaveCount(0);

    expect(errors, `API 5xx / console errors across tabs: ${errors.join(" | ")}`).toEqual([]);
  });
});
