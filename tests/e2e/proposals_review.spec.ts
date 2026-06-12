// Operator-visible outcome: with ProposalsTest entity selected in the Context tab, the Change Proposals panel shows 14 pending proposals; the authority_map/cost_center row shows confidence 95% and badge "confirmed by CFO"; clicking Approve writes cost_center → [netsuite, workday] to the authority map view; clicking Reject on vocabulary_alias/headcount removes it from pending and it appears under rejected filter; two pages load the same pending arr vocabulary proposal, page A approves it successfully, and page B's stale Approve click renders DCL's "can be decided only once" detail text on screen.

/**
 * Gate 3A D4 — Change Proposals review surface live acceptance (B17).
 *
 * Setup: beforeAll seeds a fresh DCL ingest snapshot (creates entity in selector)
 * then runs the onboarding scripted CFO session for the same tenant. Both are real
 * pipeline calls (B5/B15). The operator path is 100% UI clicks and fills.
 *
 * Ground truth: fetched at runtime from GET /api/dcl/proposals (read-only).
 * Expected values are never hardcoded; they flow from the real pipeline output.
 *
 * Constitution compliance:
 *   - Approve/Reject triggered by locator.click() — never page.request.post()
 *   - page.request.get() used for ground truth only (allowed exception)
 *   - execSync for onboarding script subprocess (B15 — real pipeline)
 *   - Screenshots after every test (reporting rule 4)
 *   - before/after state capture on Approve (acceptance rule 2)
 *   - Assertions tied to ground truth, never hardcoded (acceptance rule 1)
 */

import { test, expect } from "playwright/test";
import { execSync } from "child_process";
import { randomUUID } from "crypto";

const DCL_FRONTEND = process.env.DCL_FRONTEND_URL ?? "http://localhost:3004";
const DCL_BACKEND  = process.env.DCL_BACKEND_URL  ?? "http://localhost:8104";

// Per-run-unique tenant so reruns don't collide on the durable dev store (Gate 1B lesson).
const TENANT_ID = randomUUID();
const ENTITY_ID = `ProposalsTest-${TENANT_ID.slice(0, 8)}`;
const INGEST_ID = randomUUID();

// Shared ground truth fetched in beforeAll.
let allProposals: Record<string, unknown>[] = [];
let pendingProposals: Record<string, unknown>[] = [];
let costCenterProposal: Record<string, unknown>;
let headcountProposal: Record<string, unknown>;

const SCREENSHOTS = "tests/e2e/screenshots";

test.describe.serial("Change Proposals review — live acceptance (Gate 3A D4)", () => {
  test.setTimeout(180_000);

  // ── SETUP ─────────────────────────────────────────────────────────────────

  test.beforeAll(async ({ request }) => {
    // 1. Verify backends are healthy.
    const dclHealth = await request.get(`${DCL_BACKEND}/api/health`);
    expect(dclHealth.status(), "DCL backend not healthy").toBe(200);

    // 2. Seed a DCL snapshot so the entity appears in the snapshot selector (B5/B15).
    //    One dummy triple is enough to create the snapshot row in tenant_runs.
    const seedResp = await request.post(`${DCL_BACKEND}/api/dcl/ingest-triples`, {
      data: {
        tenant_id: TENANT_ID,
        entity_id: ENTITY_ID,
        dcl_ingest_id: INGEST_ID,
        triples: [
          {
            entity_id: ENTITY_ID,
            concept: "revenue.total",
            property: "amount",
            value: 1000000,
            period: "2026-Q1",
            source_system: "proposals_test",
            source_table: "proposals_test_seed",
            source_field: "amount",
            pipe_id: "00000000-0000-4000-8000-000000000001",
            confidence_score: 0.8,
            confidence_tier: "high",
            fabric_plane: "ipaas",
          },
        ],
      },
    });
    expect(
      seedResp.status(),
      `DCL ingest seed failed: ${await seedResp.text()}`,
    ).toBe(201);

    // 3. Run the onboarding scripted CFO session (real pipeline — B15).
    //    This submits ~14 proposals to DCL for TENANT_ID.
    execSync(
      `cd /home/ilyac/code/dcl-onboarding-agent && .venv/bin/python scripts/run_scripted_session.py` +
      ` --tenant-id ${TENANT_ID}` +
      ` --script tests/fixtures/scripted_cfo_session.yaml`,
      { timeout: 120_000, stdio: "pipe" },
    );

    // 4. Fetch ground truth proposals (read-only GET — allowed).
    const gtResp = await request.get(
      `${DCL_BACKEND}/api/dcl/proposals?entity_id=${encodeURIComponent(ENTITY_ID)}&limit=100`,
    );
    expect(gtResp.status(), "Ground truth proposals fetch failed").toBe(200);
    const gtBody = await gtResp.json();
    allProposals = gtBody.proposals ?? [];
    pendingProposals = allProposals.filter((p: Record<string, unknown>) => p.status === "pending");

    // Identify specific proposals used across tests.
    costCenterProposal = pendingProposals.find(
      (p: Record<string, unknown>) =>
        p.proposal_type === "authority_map" && p.natural_key === "cost_center",
    ) as Record<string, unknown>;
    expect(costCenterProposal, "cost_center authority_map proposal not found").toBeTruthy();

    headcountProposal = pendingProposals.find(
      (p: Record<string, unknown>) =>
        p.proposal_type === "vocabulary_alias" && p.natural_key === "headcount",
    ) as Record<string, unknown>;
    expect(headcountProposal, "headcount vocabulary_alias proposal not found").toBeTruthy();
  });

  // ── TEST 1: Navigate and assert pending count ──────────────────────────────

  test("1. Pending proposals count matches API ground truth", async ({ page }) => {
    await page.goto(DCL_FRONTEND, { waitUntil: "domcontentloaded" });

    // Navigate to Context tab.
    await page.locator("button", { hasText: "Context" }).click();
    await page.waitForLoadState("networkidle");

    // Select the ProposalsTest entity in the snapshot selector.
    const selector = page.locator("#snapshot-selector");
    await selector.waitFor({ state: "visible", timeout: 20_000 });
    // Wait for the option to appear (snapshot polling may need a moment).
    await expect(
      selector.locator(`option[value="${INGEST_ID}"]`),
    ).toBeAttached({ timeout: 20_000 });
    await selector.selectOption(INGEST_ID);

    // Wait for Change Proposals panel header to appear.
    const proposalsPanel = page.locator('[data-testid="proposals-panel"]');
    await proposalsPanel.waitFor({ state: "visible", timeout: 15_000 });

    // Open the panel.
    await proposalsPanel.locator('[data-testid="proposals-toggle"]').click();

    // Wait for list to load — status filter 'pending' is default.
    await page.waitForLoadState("networkidle");

    // Assert rendered pending count badge matches API count.
    const pendingBadge = proposalsPanel.locator('[data-testid="proposals-pending-count"]');
    await expect(pendingBadge).not.toHaveText("…", { timeout: 10_000 });
    const badgeText = await pendingBadge.textContent();
    const renderedPending = parseInt(badgeText?.match(/(\d+)/)?.[1] ?? "0", 10);
    expect(renderedPending, "Rendered pending count must match API").toBe(
      pendingProposals.length,
    );

    await page.screenshot({ path: `${SCREENSHOTS}/proposals_01_pending_count.png` });
  });

  // ── TEST 2: cost_center proposal confidence + provenance badge ─────────────

  test("2. cost_center authority proposal shows confidence + provenance badge", async ({ page }) => {
    await page.goto(DCL_FRONTEND, { waitUntil: "domcontentloaded" });
    await page.locator("button", { hasText: "Context" }).click();

    const selector = page.locator("#snapshot-selector");
    await selector.waitFor({ state: "visible", timeout: 20_000 });
    await expect(selector.locator(`option[value="${INGEST_ID}"]`)).toBeAttached({ timeout: 15_000 });
    await selector.selectOption(INGEST_ID);

    const proposalsPanel = page.locator('[data-testid="proposals-panel"]');
    await proposalsPanel.locator('[data-testid="proposals-toggle"]').click();
    await page.waitForLoadState("networkidle");

    // The cost_center authority_map row.
    const rowTestId = `proposals-proposal-row-authority_map-cost_center`;
    const ccRow = proposalsPanel.locator(`[data-testid="${rowTestId}"]`);
    await ccRow.waitFor({ state: "visible", timeout: 15_000 });

    // Confidence: ground truth says 0.95 → "95%".
    const expectedConf = `${Math.round((costCenterProposal.confidence as number) * 100)}%`;
    const confCell = ccRow.locator(`[data-testid="proposals-confidence-cost_center"]`);
    await expect(confCell).toHaveText(expectedConf);

    // Provenance badge: basis=confirmed, confirmed_by=CFO.
    const prov = costCenterProposal.provenance as Record<string, unknown>;
    const provBadge = ccRow.locator('[data-testid="provenance-badge"]');
    if (String(prov.basis) === "confirmed") {
      await expect(provBadge).toContainText(`confirmed by`);
      if (prov.confirmed_by) {
        await expect(provBadge).toContainText(String(prov.confirmed_by));
      }
    } else {
      await expect(provBadge).toContainText("inferred");
    }

    await page.screenshot({ path: `${SCREENSHOTS}/proposals_02_cost_center_proposal.png` });
  });

  // ── TEST 3: Approve cost_center — authority map updates ───────────────────

  test("3. Approve cost_center authority proposal → authority map gains entry", async ({ page }) => {
    await page.goto(DCL_FRONTEND, { waitUntil: "domcontentloaded" });
    await page.locator("button", { hasText: "Context" }).click();

    const selector = page.locator("#snapshot-selector");
    await selector.waitFor({ state: "visible", timeout: 20_000 });
    await expect(selector.locator(`option[value="${INGEST_ID}"]`)).toBeAttached({ timeout: 15_000 });
    await selector.selectOption(INGEST_ID);

    const proposalsPanel = page.locator('[data-testid="proposals-panel"]');
    await proposalsPanel.locator('[data-testid="proposals-toggle"]').click();
    await page.waitForLoadState("networkidle");

    // Fetch ground-truth authority map BEFORE approve (read-only GET).
    const authBefore = await page.request.get(
      `${DCL_BACKEND}/api/dcl/conflicts/authority-map?entity_id=${encodeURIComponent(ENTITY_ID)}`,
    );
    const beforeMap: { concept_prefix: string; ranked_sources: string[] }[] =
      (await authBefore.json()).authority_map ?? [];
    const hasCCBefore = beforeMap.some((e) => e.concept_prefix === "cost_center");
    expect(hasCCBefore, "cost_center should NOT be in authority map before approve").toBe(false);

    // Expand the cost_center proposal row.
    const ccRow = proposalsPanel.locator('[data-testid="proposals-proposal-row-authority_map-cost_center"]');
    await ccRow.waitFor({ state: "visible", timeout: 15_000 });
    await ccRow.locator("button").first().click();

    // Fill decided_by (it defaults to 'operator' but we fill explicitly).
    const decidedByInput = ccRow.locator('[data-testid="proposals-decided-by"]');
    await decidedByInput.fill("operator");

    // Click Approve.
    const approveBtn = ccRow.locator('[data-testid="proposals-approve-btn-cost_center"]');
    await approveBtn.click();

    // Wait for decision to process and authority map to refresh.
    await page.waitForLoadState("networkidle");

    // Authority map view now shows cost_center with ranked_sources from ground truth.
    const ccPayload = costCenterProposal.payload as Record<string, unknown>;
    const expectedSources = (ccPayload.ranked_sources as string[]) ?? [];

    const authSection = proposalsPanel.locator('[data-testid="authority-map-section"]');
    const ccEntry = authSection.locator('[data-testid="authority-entry-cost_center"]');
    await ccEntry.waitFor({ state: "visible", timeout: 15_000 });

    // Assert all expected ranked sources appear in the rendered entry.
    for (const src of expectedSources) {
      await expect(ccEntry).toContainText(src);
    }

    // Fetch ground-truth AFTER approve and compare.
    const authAfter = await page.request.get(
      `${DCL_BACKEND}/api/dcl/conflicts/authority-map?entity_id=${encodeURIComponent(ENTITY_ID)}`,
    );
    const afterMap: { concept_prefix: string; ranked_sources: string[] }[] =
      (await authAfter.json()).authority_map ?? [];
    const ccAfter = afterMap.find((e) => e.concept_prefix === "cost_center");
    expect(ccAfter, "cost_center must appear in authority map after approve").toBeTruthy();
    expect(ccAfter!.ranked_sources, "ranked_sources must match payload").toEqual(expectedSources);

    await page.screenshot({ path: `${SCREENSHOTS}/proposals_03_approve_cost_center.png` });
  });

  // ── TEST 4: Reject headcount vocabulary proposal ───────────────────────────

  test("4. Reject headcount vocabulary alias — disappears from pending, visible under rejected", async ({ page }) => {
    await page.goto(DCL_FRONTEND, { waitUntil: "domcontentloaded" });
    await page.locator("button", { hasText: "Context" }).click();

    const selector = page.locator("#snapshot-selector");
    await selector.waitFor({ state: "visible", timeout: 20_000 });
    await expect(selector.locator(`option[value="${INGEST_ID}"]`)).toBeAttached({ timeout: 15_000 });
    await selector.selectOption(INGEST_ID);

    const proposalsPanel = page.locator('[data-testid="proposals-panel"]');
    await proposalsPanel.locator('[data-testid="proposals-toggle"]').click();
    await page.waitForLoadState("networkidle");

    // Assert headcount row IS in pending before reject.
    const headcountRow = proposalsPanel.locator(
      '[data-testid="proposals-proposal-row-vocabulary_alias-headcount"]',
    );
    await headcountRow.waitFor({ state: "visible", timeout: 15_000 });

    // Expand + fill decided_by + click Reject.
    await headcountRow.locator("button").first().click();
    await headcountRow.locator('[data-testid="proposals-decided-by"]').fill("operator");
    await headcountRow.locator('[data-testid="proposals-reject-btn-headcount"]').click();
    await page.waitForLoadState("networkidle");

    // headcount row must no longer appear in the pending list.
    await expect(headcountRow).not.toBeVisible({ timeout: 10_000 });

    // Switch to rejected filter — headcount must appear there.
    await proposalsPanel.locator('[data-testid="proposals-status-filter-rejected"]').click();
    await page.waitForLoadState("networkidle");

    const headcountRejectedRow = proposalsPanel.locator(
      '[data-testid="proposals-proposal-row-vocabulary_alias-headcount"]',
    );
    await headcountRejectedRow.waitFor({ state: "visible", timeout: 15_000 });

    // Authority map must NOT contain headcount (rejection leaves zero residue).
    const authResp = await page.request.get(
      `${DCL_BACKEND}/api/dcl/conflicts/authority-map?entity_id=${encodeURIComponent(ENTITY_ID)}`,
    );
    const authMap: { concept_prefix: string }[] = (await authResp.json()).authority_map ?? [];
    const hasHeadcount = authMap.some((e) => e.concept_prefix === "headcount");
    expect(hasHeadcount, "Rejected vocabulary alias must leave zero authority residue").toBe(false);

    // Concept lookup must return resolved=false (alias not applied after rejection).
    const lookupResp = await page.request.get(
      `${DCL_BACKEND}/api/dcl/concept-lookup?tenant_id=${TENANT_ID}&alias=headcount`,
    );
    const lookup = await lookupResp.json();
    expect(lookup.resolved, "Rejected alias must not be resolvable").toBe(false);

    await page.screenshot({ path: `${SCREENSHOTS}/proposals_04_reject_headcount.png` });
  });

  // ── TEST 5: Negative — stale-Approve race renders 409 detail text ───────────
  // Real-world trigger: two operators open the same pending proposal; operator A
  // approves first; operator B's stale Approve click hits DCL's 409 path.
  // Both pages are loaded BEFORE page A approves so page B genuinely holds a
  // stale pending view — no mutative call from the test runner.

  test("5. Negative: stale-Approve race renders 409 already-decided detail text", async ({ page, context }) => {
    const arrGT = pendingProposals.find(
      (p: Record<string, unknown>) =>
        p.proposal_type === "vocabulary_alias" && p.natural_key === "arr",
    ) as Record<string, unknown>;
    expect(arrGT, "arr vocabulary proposal must be in ground truth").toBeTruthy();

    // ── Page A (operator 1) ───────────────────────────────────────────────────
    await page.goto(DCL_FRONTEND, { waitUntil: "domcontentloaded" });
    await page.locator("button", { hasText: "Context" }).click();
    const selectorA = page.locator("#snapshot-selector");
    await selectorA.waitFor({ state: "visible", timeout: 20_000 });
    await expect(selectorA.locator(`option[value="${INGEST_ID}"]`)).toBeAttached({ timeout: 15_000 });
    await selectorA.selectOption(INGEST_ID);
    const proposalsPanelA = page.locator('[data-testid="proposals-panel"]');
    await proposalsPanelA.locator('[data-testid="proposals-toggle"]').click();
    await page.waitForLoadState("networkidle");
    const arrRowA = proposalsPanelA.locator('[data-testid="proposals-proposal-row-vocabulary_alias-arr"]');
    await arrRowA.waitFor({ state: "visible", timeout: 15_000 });

    // ── Page B (operator 2) — loaded BEFORE page A approves ──────────────────
    const pageB = await context.newPage();
    await pageB.goto(DCL_FRONTEND, { waitUntil: "domcontentloaded" });
    await pageB.locator("button", { hasText: "Context" }).click();
    const selectorB = pageB.locator("#snapshot-selector");
    await selectorB.waitFor({ state: "visible", timeout: 20_000 });
    await expect(selectorB.locator(`option[value="${INGEST_ID}"]`)).toBeAttached({ timeout: 15_000 });
    await selectorB.selectOption(INGEST_ID);
    const proposalsPanelB = pageB.locator('[data-testid="proposals-panel"]');
    await proposalsPanelB.locator('[data-testid="proposals-toggle"]').click();
    await pageB.waitForLoadState("networkidle");
    // arr is pending in page B's stale view.
    const arrRowB = proposalsPanelB.locator('[data-testid="proposals-proposal-row-vocabulary_alias-arr"]');
    await arrRowB.waitFor({ state: "visible", timeout: 15_000 });

    // ── Page A: Approve arr (succeeds — arr leaves pending) ──────────────────
    await arrRowA.locator("button").first().click();
    await arrRowA.locator('[data-testid="proposals-decided-by"]').fill("operator-page-a");
    await arrRowA.locator('[data-testid="proposals-approve-btn-arr"]').click();
    await page.waitForLoadState("networkidle");
    await expect(arrRowA).not.toBeVisible({ timeout: 10_000 });

    // ── Page B: stale Approve click — backend returns 409 ────────────────────
    // page B has NOT reloaded; its DOM still shows arr as pending with Approve button.
    await arrRowB.locator("button").first().click();
    await arrRowB.locator('[data-testid="proposals-decided-by"]').fill("operator-page-b");
    await arrRowB.locator('[data-testid="proposals-approve-btn-arr"]').click();
    await pageB.waitForLoadState("networkidle");

    // DCL returns 409; the panel must render the already-decided detail text.
    const errorDiv = proposalsPanelB.locator('[data-testid="proposals-decide-error"]');
    await errorDiv.waitFor({ state: "visible", timeout: 10_000 });
    const errorText = await errorDiv.textContent();
    // DCL 409 detail: "Proposal <id> is already 'approved' — a proposal can be decided only once."
    expect(
      errorText,
      "Error must contain DCL's already-decided detail text",
    ).toMatch(/already\s+(approved|rejected)|can be decided only once/i);
    expect(errorText, "Error must not be a bare status code").not.toMatch(/^409$/);

    await page.screenshot({ path: `${SCREENSHOTS}/proposals_05_negative_409_page_a.png` });
    await pageB.screenshot({ path: `${SCREENSHOTS}/proposals_05_negative_409_page_b.png` });
  });
});
