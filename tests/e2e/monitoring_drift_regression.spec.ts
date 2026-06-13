// Operator-visible outcome: with route-stubbed scheduler API returning two drift jobs (structural_drift paused, value_drift enabled), and route-stubbed proposals returning one structural_drift and one value_drift proposal, the Monitor tab renders both job rows with correct enabled badges and intervals; the ProposalsPanel shows structural_drift payload with +/-field text and value_drift payload with source:value claims and trend arrow.
/**
 * Gate 3B D3 — Monitoring surface mocked regression (taxonomy rule 6).
 *
 * Regression only — route-stubbed (not live-services). Labeled per taxonomy rule 6:
 * live acceptance + mocked regression, both required.
 * This file verifies rendering correctness of MonitoringTab + ProposalsPanel drift
 * types against fixed stubs. No backend calls, no DB state mutations.
 *
 * Stubs: page.route() intercepts:
 *   - GET /api/dcl/monitor/schedule
 *   - GET /api/dcl/proposals*
 *   - GET /api/dcl/snapshots
 *   - GET /api/dcl/conflicts/authority-map*
 */

import { test, expect } from "playwright/test";

const DCL_FRONTEND = process.env.DCL_FRONTEND_URL ?? "http://localhost:3004";

const MOCK_TENANT  = "aaaabbbb-cccc-4ddd-8eee-ffffffffffff";
const MOCK_ENTITY  = "MockDriftEntity";
const MOCK_INGEST  = "11112222-3333-4444-8555-666677778888";

const MOCK_STRUCT_PROPOSAL_ID = "aaaabbbb-0001-4001-8001-111100000001";
const MOCK_VALUE_PROPOSAL_ID  = "aaaabbbb-0002-4002-8002-222200000002";

const MOCK_SCHEDULE = {
  jobs: [
    {
      job_name: "structural_drift",
      interval_seconds: 300,
      enabled: false,
      last_run_at: "2026-06-13T10:00:00Z",
      last_status: "ok",
      last_detail: "scanned=5 findings=2 filed=1 deduped=1",
      updated_at: "2026-06-13T10:00:00Z",
    },
    {
      job_name: "value_drift",
      interval_seconds: 300,
      enabled: true,
      last_run_at: "2026-06-13T10:01:00Z",
      last_status: "ok",
      last_detail: "scanned=5 findings=1 filed=1 deduped=0",
      updated_at: "2026-06-13T10:01:00Z",
    },
  ],
  count: 2,
};

const MOCK_SNAPSHOTS = {
  snapshots: [{
    dcl_ingest_id: MOCK_INGEST,
    snapshot_name: `${MOCK_ENTITY}-${MOCK_INGEST.slice(0, 4)}`,
    entity_id: MOCK_ENTITY,
    run_timestamp: "2026-06-13T09:00:00Z",
    total_rows: 100,
    is_current: true,
  }],
};

const MOCK_PROPOSALS_PENDING = {
  proposals: [
    {
      proposal_id: MOCK_STRUCT_PROPOSAL_ID,
      tenant_id: MOCK_TENANT,
      entity_id: MOCK_ENTITY,
      proposal_type: "structural_drift",
      natural_key: `${MOCK_ENTITY}|aaa|bbb`,
      payload: {
        entity_id: MOCK_ENTITY,
        dcl_ingest_id_base: "aaa",
        dcl_ingest_id_compare: "bbb",
        added: [{ concept: "revenue.total", property: "new_field", period: "2025-Q1" }],
        removed: [{ concept: "revenue.total", property: "old_field", period: "2025-Q1" }],
      },
      confidence: 1.0,
      provenance: { basis: "inferred", source: "structural_drift_monitor" },
      status: "pending",
      created_at: "2026-06-13T10:00:00Z",
      decided_at: null,
      decided_by: null,
      decision_note: null,
      canonical_artifact_id: null,
    },
    {
      proposal_id: MOCK_VALUE_PROPOSAL_ID,
      tenant_id: MOCK_TENANT,
      entity_id: MOCK_ENTITY,
      proposal_type: "value_drift",
      natural_key: `${MOCK_ENTITY}|revenue.total|amount|2025-Q1`,
      payload: {
        entity_id: MOCK_ENTITY,
        concept: "revenue.total",
        property: "amount",
        period: "2025-Q1",
        claims: [
          { source_system: "sap",        value: 1000000, confidence_score: 0.95 },
          { source_system: "salesforce", value: 1100000, confidence_score: 0.95 },
        ],
        conflict_id: "cccc-dddd-eeee-ffff",
        conflict_class: "value_discrepancy",
        trend: { prior_count: 0, current_count: 1 },
      },
      confidence: 1.0,
      provenance: { basis: "inferred", source: "value_drift_monitor" },
      status: "pending",
      created_at: "2026-06-13T10:01:00Z",
      decided_at: null,
      decided_by: null,
      decision_note: null,
      canonical_artifact_id: null,
    },
  ],
  total_count: 2,
  tenant_id: MOCK_TENANT,
};

test.describe.serial("Monitoring drift — mocked regression (Gate 3B D3)", () => {
  test.setTimeout(120_000);

  test("Monitor tab renders both job rows with correct badges and intervals", async ({ page }) => {
    // Stub schedule endpoint.
    await page.route("**/api/dcl/monitor/schedule", (route) => {
      route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(MOCK_SCHEDULE) });
    });
    // Stub snapshots (needed by SnapshotSelector in MonitoringTab).
    await page.route("**/api/dcl/snapshots**", (route) => {
      route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(MOCK_SNAPSHOTS) });
    });
    // Stub proposals (pending count badge).
    await page.route("**/api/dcl/proposals**", (route) => {
      route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(MOCK_PROPOSALS_PENDING) });
    });
    // Stub authority map.
    await page.route("**/api/dcl/conflicts/authority-map**", (route) => {
      route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ entries: [] }) });
    });

    await page.goto(DCL_FRONTEND, { waitUntil: "load", timeout: 30_000 });
    await page.locator("button", { hasText: "Monitor" }).click();
    await page.waitForLoadState("load");

    // Both job rows visible.
    await expect(page.locator('[data-testid="job-row-structural_drift"]'))
      .toBeVisible({ timeout: 15_000 });
    await expect(page.locator('[data-testid="job-row-value_drift"]'))
      .toBeVisible({ timeout: 5_000 });

    // structural_drift: paused badge, interval 300s = "every 5m".
    await expect(page.locator('[data-testid="job-enabled-structural_drift"]'))
      .toHaveText("paused");
    await expect(page.locator('[data-testid="job-interval-structural_drift"]'))
      .toContainText("5m");

    // value_drift: enabled badge, interval "every 5m".
    await expect(page.locator('[data-testid="job-enabled-value_drift"]'))
      .toHaveText("enabled");
    await expect(page.locator('[data-testid="job-interval-value_drift"]'))
      .toContainText("5m");

    // structural_drift is paused → interval input shown; value_drift is enabled → Pause button shown.
    await expect(page.locator('[data-testid="interval-input-structural_drift"]')).toBeVisible();
    await expect(page.locator('[data-testid="pause-btn-value_drift"]')).toBeVisible();

    await page.screenshot({ path: "tests/e2e/screenshots/regression-drift-01-jobs.png" });
  });

  test("ProposalsPanel renders structural_drift payload with +/- field notation", async ({ page }) => {
    await page.route("**/api/dcl/monitor/schedule", (route) => {
      route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(MOCK_SCHEDULE) });
    });
    await page.route("**/api/dcl/snapshots**", (route) => {
      route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(MOCK_SNAPSHOTS) });
    });
    await page.route("**/api/dcl/proposals**", (route) => {
      route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(MOCK_PROPOSALS_PENDING) });
    });
    await page.route("**/api/dcl/conflicts/authority-map**", (route) => {
      route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ entries: [] }) });
    });

    await page.goto(DCL_FRONTEND, { waitUntil: "load", timeout: 30_000 });
    await page.locator("button", { hasText: "Monitor" }).click();

    // Select entity via snapshot selector.
    const selector = page.locator("#snapshot-selector");
    await selector.waitFor({ state: "visible", timeout: 15_000 });
    await selector.selectOption(MOCK_INGEST);

    // Open proposals toggle.
    await page.locator('[data-testid="proposals-toggle"]').click();
    await expect(page.locator('[data-testid="proposals-panel"]')).toBeVisible({ timeout: 10_000 });

    // structural_drift row must be visible.
    const sdRow = page.locator('[data-testid^="proposals-proposal-row-structural_drift"]');
    await expect(sdRow).toBeVisible({ timeout: 10_000 });

    // Payload summary must contain + and - field notation (from payloadSummary).
    const summaryText = await sdRow.locator('[data-testid^="proposals-payload-summary-"]').textContent() ?? "";
    expect(summaryText, "Summary must show added field (+new_field)").toContain("+revenue.total.new_field");
    expect(summaryText, "Summary must show removed field (-old_field)").toContain("-revenue.total.old_field");

    await page.screenshot({ path: "tests/e2e/screenshots/regression-drift-02-struct-payload.png" });
  });

  test("ProposalsPanel renders value_drift payload with source:value claims and trend arrow", async ({ page }) => {
    await page.route("**/api/dcl/monitor/schedule", (route) => {
      route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(MOCK_SCHEDULE) });
    });
    await page.route("**/api/dcl/snapshots**", (route) => {
      route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(MOCK_SNAPSHOTS) });
    });
    await page.route("**/api/dcl/proposals**", (route) => {
      route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(MOCK_PROPOSALS_PENDING) });
    });
    await page.route("**/api/dcl/conflicts/authority-map**", (route) => {
      route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ entries: [] }) });
    });

    await page.goto(DCL_FRONTEND, { waitUntil: "load", timeout: 30_000 });
    await page.locator("button", { hasText: "Monitor" }).click();

    const selector = page.locator("#snapshot-selector");
    await selector.waitFor({ state: "visible", timeout: 15_000 });
    await selector.selectOption(MOCK_INGEST);

    await page.locator('[data-testid="proposals-toggle"]').click();

    const vdRow = page.locator('[data-testid^="proposals-proposal-row-value_drift"]');
    await expect(vdRow).toBeVisible({ timeout: 10_000 });

    const summaryText = await vdRow.locator('[data-testid^="proposals-payload-summary-"]').textContent() ?? "";
    expect(summaryText, "Summary must mention sap source").toContain("sap");
    expect(summaryText, "Summary must mention salesforce source").toContain("salesforce");
    expect(summaryText, "Summary must show SAP value 1000000").toContain("1000000");
    expect(summaryText, "Summary must show SF value 1100000").toContain("1100000");
    expect(summaryText, "Summary must show trend arrow").toContain("→");
    expect(summaryText, "Summary must show 0→1 trend").toContain("0→1");

    await page.screenshot({ path: "tests/e2e/screenshots/regression-drift-03-value-payload.png" });
  });
});
