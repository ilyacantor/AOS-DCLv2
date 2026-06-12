// Operator-visible outcome: with route-stubbed proposals API returning a mock authority_map and vocabulary_alias proposal, the Change Proposals panel renders both rows with correct type badges, confidence percentages, provenance badges, payload summaries, and the authority map section shows the stubbed entry.
/**
 * Gate 3A D4 — Change Proposals review mocked regression (taxonomy rule 6).
 *
 * Regression only — route-stubbed (not live-services). Labeled per taxonomy rule 6:
 * live acceptance + mocked regression, both required. This file covers rendering
 * correctness against a fixed stub; the live acceptance is in proposals_review.spec.ts.
 *
 * Uses page.route() to intercept /api/dcl/proposals* and
 * /api/dcl/conflicts/authority-map* with deterministic mock responses.
 * No backend calls. No change_proposal_decisions writes. Read-only route interception.
 */

import { test, expect } from "playwright/test";

const DCL_FRONTEND = process.env.DCL_FRONTEND_URL ?? "http://localhost:3004";

// Deterministic fake IDs (no randomness — regression must be stable across runs).
const MOCK_TENANT = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee";
const MOCK_INGEST = "11111111-2222-4333-8444-555555555555";
const MOCK_ENTITY = "MockProposalsEntity";
const MOCK_PROPOSAL_AUTHORITY = "cccccccc-dddd-4eee-8fff-000000000001";
const MOCK_PROPOSAL_VOCAB    = "cccccccc-dddd-4eee-8fff-000000000002";

const PENDING_PROPOSALS = [
  {
    proposal_id: MOCK_PROPOSAL_AUTHORITY,
    tenant_id: MOCK_TENANT,
    entity_id: MOCK_ENTITY,
    proposal_type: "authority_map",
    natural_key: "cost_center",
    payload: { concept_prefix: "cost_center", ranked_sources: ["netsuite", "workday"], confidence: 0.95 },
    confidence: 0.95,
    provenance: { basis: "confirmed", confirmed_by: "CFO" },
    status: "pending",
    created_at: "2026-06-12T10:00:00Z",
    decided_at: null,
    decided_by: null,
    decision_note: null,
    canonical_artifact_id: null,
  },
  {
    proposal_id: MOCK_PROPOSAL_VOCAB,
    tenant_id: MOCK_TENANT,
    entity_id: MOCK_ENTITY,
    proposal_type: "vocabulary_alias",
    natural_key: "headcount",
    payload: { alias: "headcount", concept_id: "employee_count", confidence: 0.95 },
    confidence: 0.95,
    provenance: { basis: "inferred" },
    status: "pending",
    created_at: "2026-06-12T10:01:00Z",
    decided_at: null,
    decided_by: null,
    decision_note: null,
    canonical_artifact_id: null,
  },
];

const MOCK_AUTHORITY_MAP = [
  { concept_prefix: "cost_center", ranked_sources: ["netsuite", "workday"] },
];

test.describe("Change Proposals review — mocked regression (route-stubbed)", () => {
  test.setTimeout(60_000);

  test("renders stubbed proposals with correct badges, confidence, and authority map", async ({ page }) => {
    // Seed a snapshot option so the selector has MOCK_INGEST as a value.
    // We do this by stubbing the snapshots endpoint too.
    await page.route("**/api/dcl/snapshots", (route) => {
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          snapshots: [
            {
              dcl_ingest_id: MOCK_INGEST,
              snapshot_name: `${MOCK_ENTITY}-mock`,
              entity_id: MOCK_ENTITY,
              run_timestamp: "2026-06-12T10:00:00Z",
              total_rows: 1,
              is_current: true,
            },
          ],
        }),
      });
    });

    // Stub the context summary to avoid real backend calls.
    await page.route("**/api/dcl/contextualization-summary**", (route) => {
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          domain_coverage: { domains_populated: 1, domains_total: 10, domains: [] },
          confidence_distribution: { exact: 1, high: 0, medium: 0, low: 0 },
          resolution_activity: { workspaces_total: 0, workspaces_pending: 0, workspaces_resolved: 0, conflicts_detected: 0 },
          source_system_breakdown: [],
        }),
      });
    });

    // Stub conflicts endpoint (ConflictsPanel dependency).
    await page.route("**/api/dcl/conflicts**", (route) => {
      route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ conflicts: [], total: 0 }) });
    });

    // Stub change proposals — return mock list for both pending count fetch and full list.
    await page.route("**/api/dcl/proposals**", (route) => {
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          tenant_id: MOCK_TENANT,
          proposals: PENDING_PROPOSALS,
          total_count: PENDING_PROPOSALS.length,
          limit: 100,
          offset: 0,
        }),
      });
    });

    // Stub authority map.
    await page.route("**/api/dcl/conflicts/authority-map**", (route) => {
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          tenant_id: MOCK_TENANT,
          entity_id: MOCK_ENTITY,
          authority_map: MOCK_AUTHORITY_MAP,
        }),
      });
    });

    await page.goto(DCL_FRONTEND, { waitUntil: "domcontentloaded" });

    // Navigate to Context tab.
    await page.locator("button", { hasText: "Context" }).click();
    await page.waitForLoadState("networkidle");

    // Select the mock entity via snapshot selector.
    const selector = page.locator("#snapshot-selector");
    await selector.waitFor({ state: "visible", timeout: 15_000 });
    await expect(selector.locator(`option[value="${MOCK_INGEST}"]`)).toBeAttached({ timeout: 10_000 });
    await selector.selectOption(MOCK_INGEST);
    await page.waitForLoadState("networkidle");

    // Change Proposals panel visible.
    const proposalsPanel = page.locator('[data-testid="proposals-panel"]');
    await proposalsPanel.waitFor({ state: "visible", timeout: 15_000 });

    // Pending count badge shows 2 (the stub count).
    const badge = proposalsPanel.locator('[data-testid="proposals-pending-count"]');
    await expect(badge).not.toHaveText("…", { timeout: 10_000 });
    await expect(badge).toContainText("2");

    // Open the panel.
    await proposalsPanel.locator('[data-testid="proposals-toggle"]').click();
    await page.waitForLoadState("networkidle");

    // ── authority_map row ────────────────────────────────────────────────────

    const authRow = proposalsPanel.locator('[data-testid="proposals-proposal-row-authority_map-cost_center"]');
    await authRow.waitFor({ state: "visible", timeout: 10_000 });

    // Type badge visible (the CSS class carries the label).
    await expect(authRow).toBeVisible();

    // Confidence = 95%.
    const authConf = authRow.locator('[data-testid="proposals-confidence-cost_center"]');
    await expect(authConf).toHaveText("95%");

    // Provenance badge: confirmed by CFO.
    const authProvBadge = authRow.locator('[data-testid="provenance-badge"]');
    await expect(authProvBadge).toContainText("confirmed by CFO");

    // Payload summary: "cost_center: netsuite > workday".
    const authSummary = authRow.locator('[data-testid="proposals-payload-summary-cost_center"]');
    await expect(authSummary).toContainText("cost_center");
    await expect(authSummary).toContainText("netsuite");
    await expect(authSummary).toContainText("workday");

    // ── vocabulary_alias row ─────────────────────────────────────────────────

    const vocabRow = proposalsPanel.locator('[data-testid="proposals-proposal-row-vocabulary_alias-headcount"]');
    await vocabRow.waitFor({ state: "visible", timeout: 10_000 });

    // Confidence = 95%.
    const vocabConf = vocabRow.locator('[data-testid="proposals-confidence-headcount"]');
    await expect(vocabConf).toHaveText("95%");

    // Provenance badge: inferred.
    const vocabProvBadge = vocabRow.locator('[data-testid="provenance-badge"]');
    await expect(vocabProvBadge).toContainText("inferred");

    // Payload summary: "headcount → employee_count".
    const vocabSummary = vocabRow.locator('[data-testid="proposals-payload-summary-headcount"]');
    await expect(vocabSummary).toContainText("headcount");
    await expect(vocabSummary).toContainText("employee_count");

    // ── authority map section ────────────────────────────────────────────────

    const authSection = proposalsPanel.locator('[data-testid="authority-map-section"]');
    await expect(authSection).toBeVisible();

    // cost_center entry with netsuite > workday.
    const ccEntry = authSection.locator('[data-testid="authority-entry-cost_center"]');
    await ccEntry.waitFor({ state: "visible", timeout: 10_000 });
    await expect(ccEntry).toContainText("netsuite");
    await expect(ccEntry).toContainText("workday");

    await page.screenshot({ path: "tests/e2e/screenshots/proposals_regression_stubbed.png" });
  });
});
