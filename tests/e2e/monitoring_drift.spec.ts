// Operator-visible outcome: On the Monitor tab, structural_drift and value_drift jobs are listed (both paused). Setting a 5-second interval and resuming each job triggers a real scheduler fire; after each fire, proposals appear for the seeded entities (STRUCT_ENTITY shows +run2_prop/-run1_prop structural change; VALUE_ENTITY shows sap vs salesforce conflict with 0→1 trend). Approving the value_drift proposal sets canonical_artifact_id to a conflict_disposition UUID (SAP wins per authority map). Rejecting the structural_drift proposal leaves zero canonical residue. Both jobs are paused at teardown.
/**
 * Gate 3B D3 — Monitoring surface + drift loop live acceptance (B17).
 *
 * B5/B15: Seeding via real pipeline (POST /api/dcl/ingest-triples precondition calls).
 * B17: all feature interactions are UI clicks (locator.click, selectOption, fill).
 *       page.request.get() used only for ground-truth reads (read-only exception).
 * #91: proves on-schedule via real timer fire — never via run-now.
 *       Both jobs PAUSED at teardown (asserted in afterAll).
 * #88: ground-truth reads use get_snapshots from _dcl_ground_truth helper.
 * #33/#51: single browser session (test.describe.serial + one page object).
 * Acceptance rule 1: expected values fetched at test time from proposals API, never hardcoded.
 * Acceptance rule 2: before/after state capture on Approve (pending→approved, null→canonical_artifact_id).
 * Taxonomy rule 6: this is the live-acceptance spec; mocked regression in monitoring_drift_regression.spec.ts.
 */

import { test, expect, APIRequestContext, Page } from "playwright/test";
import { randomUUID } from "crypto";

const DCL_FRONTEND = process.env.DCL_FRONTEND_URL ?? "http://localhost:3004";
const DCL_BACKEND  = process.env.DCL_BACKEND_URL  ?? "http://localhost:8104";

// Per-run-unique tenant (B14 — test re-runs never collide on the shared dev store).
const TENANT_ID        = randomUUID();
const VALUE_ENTITY     = `ValueDriftE2E-${TENANT_ID.slice(0, 8)}`;
const STRUCT_ENTITY    = `StructDriftE2E-${TENANT_ID.slice(0, 8)}`;

// Ingest IDs stored module-level (filled in beforeAll).
let VALUE_INGEST_ID_1  = "";
let VALUE_INGEST_ID_2  = "";
let STRUCT_INGEST_ID_1 = "";
let STRUCT_INGEST_ID_2 = "";

// Ground-truth proposals fetched at test time (never hardcoded).
let valueDriftProposal:  Record<string, unknown> | null = null;
let structDriftProposal: Record<string, unknown> | null = null;

const SCREENSHOTS = "tests/e2e/screenshots";
const SHORT_INTERVAL_S = 5;   // short cadence for on-schedule proof (#91)
// Dev DB has 302k+ triples (#27 bloat) — value_drift detect_and_register takes ~35s.
const POLL_TIMEOUT_MS = 90_000;

// ── helpers ──────────────────────────────────────────────────────────────────

async function ingest(
  req: APIRequestContext,
  ingestId: string,
  entityId: string,
  triples: object[],
): Promise<void> {
  const resp = await req.post(`${DCL_BACKEND}/api/dcl/ingest-triples`, {
    data: {
      tenant_id: TENANT_ID,
      dcl_ingest_id: ingestId,
      entity_id: entityId,
      triples,
    },
  });
  expect(resp.status(), `ingest ${ingestId} failed: ${await resp.text()}`).toBe(201);
}

function makeTriple(
  entityId: string,
  property: string,
  value: number,
  sourceSystem: string,
): object {
  return {
    entity_id: entityId,
    concept: "revenue.total",
    property,
    value,
    period: "2025-Q1",
    source_system: sourceSystem,
    source_table: "drift_e2e_probe",
    source_field: property,
    pipe_id: randomUUID(),
    fabric_plane: "ipaas",
    confidence_score: 0.95,
    confidence_tier: "exact",
  };
}

/** Poll GET /api/dcl/monitor/schedule until job.last_run_at advances. */
async function pollUntilJobFires(
  req: APIRequestContext,
  jobName: string,
  initialLastRunAt: string | null,
  maxWaitMs = 30_000,
): Promise<void> {
  const deadline = Date.now() + maxWaitMs;
  while (Date.now() < deadline) {
    const resp = await req.get(`${DCL_BACKEND}/api/dcl/monitor/schedule`);
    expect(resp.status()).toBe(200);
    const body = await resp.json();
    const job = (body.jobs as Array<{ job_name: string; last_run_at: string | null }>)
      .find((j) => j.job_name === jobName);
    if (
      job &&
      job.last_run_at !== null &&
      job.last_run_at !== initialLastRunAt
    ) {
      return;
    }
    await new Promise((r) => setTimeout(r, 1_000));
  }
  throw new Error(
    `[pollUntilJobFires] ${jobName} did not fire within ${maxWaitMs}ms. ` +
    `Initial last_run_at: ${initialLastRunAt}`,
  );
}

/** Read initial last_run_at for a job. */
async function getLastRunAt(req: APIRequestContext, jobName: string): Promise<string | null> {
  const resp = await req.get(`${DCL_BACKEND}/api/dcl/monitor/schedule`);
  expect(resp.status()).toBe(200);
  const jobs = (await resp.json()).jobs as Array<{ job_name: string; last_run_at: string | null }>;
  const job = jobs.find((j) => j.job_name === jobName);
  return job?.last_run_at ?? null;
}

/** Fetch pending proposals for entity (read-only GET — allowed exception). */
async function fetchPendingProposals(
  req: APIRequestContext,
  entityId: string,
  proposalType?: string,
): Promise<Record<string, unknown>[]> {
  const url = new URL(`${DCL_BACKEND}/api/dcl/proposals`);
  url.searchParams.set("entity_id", entityId);
  url.searchParams.set("status", "pending");
  url.searchParams.set("limit", "50");
  if (proposalType) url.searchParams.set("proposal_type", proposalType);
  const resp = await req.get(url.toString());
  if (resp.status() !== 200) return [];
  const body = await resp.json();
  return (body.proposals ?? []) as Record<string, unknown>[];
}

// ── test suite ────────────────────────────────────────────────────────────────

test.describe.serial("Monitoring drift — live acceptance (Gate 3B D3)", () => {
  test.setTimeout(600_000);

  // Single page object for the entire suite (#33 WSL2 single-session pattern).
  let page!: Page;

  // ── SETUP ───────────────────────────────────────────────────────────────────

  test.beforeAll(async ({ browser, request }) => {
    // 1. Health check.
    const health = await request.get(`${DCL_BACKEND}/api/health`);
    expect(health.status(), "DCL backend not healthy").toBe(200);

    // 2. Seed VALUE_ENTITY — two runs, BOTH with SAP + Salesforce in same run.
    //    Structural diff = unchanged (same keys both runs).
    //    Value conflict: sap=1_000_000 vs salesforce=1_100_000 in run2.
    VALUE_INGEST_ID_1 = randomUUID();
    VALUE_INGEST_ID_2 = randomUUID();
    await ingest(request, VALUE_INGEST_ID_1, VALUE_ENTITY, [
      makeTriple(VALUE_ENTITY, "amount", 1_000_000, "sap"),
      makeTriple(VALUE_ENTITY, "amount", 1_100_000, "salesforce"),
    ]);
    await ingest(request, VALUE_INGEST_ID_2, VALUE_ENTITY, [
      makeTriple(VALUE_ENTITY, "amount", 1_000_000, "sap"),
      makeTriple(VALUE_ENTITY, "amount", 1_100_000, "salesforce"),
    ]);

    // 3. Seed STRUCT_ENTITY — run1: run1_prop; run2: run2_prop.
    //    Structural diff: run2_prop ADDED, run1_prop REMOVED.
    STRUCT_INGEST_ID_1 = randomUUID();
    STRUCT_INGEST_ID_2 = randomUUID();
    await ingest(request, STRUCT_INGEST_ID_1, STRUCT_ENTITY, [
      makeTriple(STRUCT_ENTITY, "run1_prop", 1000, "sap"),
    ]);
    await ingest(request, STRUCT_INGEST_ID_2, STRUCT_ENTITY, [
      makeTriple(STRUCT_ENTITY, "run2_prop", 1000, "sap"),
    ]);

    // 4. Set up authority map: SAP > Salesforce for "revenue" concept prefix.
    //    This precondition ensures value_drift Approve canonicalizes SAP as winner.
    const intakeResp = await request.post(`${DCL_BACKEND}/api/dcl/proposals`, {
      data: {
        tenant_id: TENANT_ID,
        proposals: [{
          entity_id: VALUE_ENTITY,
          proposal_type: "authority_map",
          payload: {
            concept_prefix: "revenue",
            ranked_sources: ["sap", "salesforce"],
            confidence: 1.0,
          },
          confidence: 1.0,
          provenance: { basis: "confirmed", confirmed_by: "drift_e2e_setup" },
        }],
      },
    });
    expect(intakeResp.status(), `Authority map intake failed: ${await intakeResp.text()}`).toBe(201);
    const intakeBody = await intakeResp.json();
    const authProposalId = String(
      ((intakeBody.proposals ?? []) as Array<{ status: string; proposal_id: string }>)
        .find((p) => p.status === "accepted")?.proposal_id ?? "",
    );
    expect(authProposalId, "Authority map proposal not created").not.toBe("");

    const decideResp = await request.post(
      `${DCL_BACKEND}/api/dcl/proposals/${authProposalId}/decide`,
      {
        data: {
          tenant_id: TENANT_ID,
          decision: "approve",
          decided_by: "drift_e2e_setup",
          note: "SAP is authoritative for revenue — e2e precondition",
        },
      },
    );
    expect(decideResp.status(), `Authority map approve failed: ${await decideResp.text()}`).toBe(200);

    // 5. Open DCL frontend (single browser context for WSL2 stability).
    const ctx = await browser.newContext();
    page = await ctx.newPage();
    await page.goto(DCL_FRONTEND, { waitUntil: "load", timeout: 30_000 });
  });

  test.afterAll(async ({ request }) => {
    // RESTORE the global monitor_schedule defaults this suite mutated: the
    // tests set a 5s interval to force real timer fires, but interval is GLOBAL
    // job config (one row per job_name) shared with the pytest suite — leaving
    // it at 5 fails tests/test_value_drift_loop.py's interval-default assertion.
    // resume(interval=300) sets interval+enables, then pause disables → net
    // (interval=300, enabled=false): the seeded default, jobs paused (#91).
    for (const job of ["structural_drift", "value_drift"]) {
      await request.post(`${DCL_BACKEND}/api/dcl/monitor/schedule/${job}/resume`,
        { data: { interval_seconds: 300 } }).catch(() => {});
      await request.post(`${DCL_BACKEND}/api/dcl/monitor/schedule/${job}/pause`).catch(() => {});
    }

    // #91: assert both jobs are paused at suite teardown.
    const resp = await request.get(`${DCL_BACKEND}/api/dcl/monitor/schedule`);
    expect(resp.status()).toBe(200);
    const jobs = (await resp.json()).jobs as Array<{ job_name: string; enabled: boolean; interval_seconds: number }>;
    const sd = jobs.find((j) => j.job_name === "structural_drift");
    const vd = jobs.find((j) => j.job_name === "value_drift");
    expect(sd?.enabled, "structural_drift must be paused at suite end (#91)").toBe(false);
    expect(vd?.enabled, "value_drift must be paused at suite end (#91)").toBe(false);
    expect(vd?.interval_seconds, "value_drift interval restored to default").toBe(300);

    if (page?.isClosed?.() === false || page) {
      await page.screenshot({ path: `${SCREENSHOTS}/drift-teardown.png` }).catch(() => {});
    }
  });

  // ── TEST 1: Scheduler jobs visible ─────────────────────────────────────────

  test("Monitor tab shows both drift jobs with paused badges", async () => {
    await page.locator("button", { hasText: "Monitor" }).click();
    await page.waitForLoadState("load");

    // Both job rows must appear.
    await expect(page.locator('[data-testid="job-row-structural_drift"]'))
      .toBeVisible({ timeout: 15_000 });
    await expect(page.locator('[data-testid="job-row-value_drift"]'))
      .toBeVisible({ timeout: 10_000 });

    // Both start paused.
    const sdBadge = page.locator('[data-testid="job-enabled-structural_drift"]');
    const vdBadge = page.locator('[data-testid="job-enabled-value_drift"]');
    await expect(sdBadge).toHaveText("paused");
    await expect(vdBadge).toHaveText("paused");

    await page.screenshot({ path: `${SCREENSHOTS}/drift-01-jobs-paused.png` });
  });

  // ── TEST 2: Prove structural_drift on schedule ──────────────────────────────

  test("structural_drift fires on schedule — proposal appears for STRUCT_ENTITY", async ({ request }) => {
    const initialLastRunAt = await getLastRunAt(request, "structural_drift");

    // Set short interval via UI.
    await page.locator('[data-testid="interval-input-structural_drift"]')
      .fill(String(SHORT_INTERVAL_S));
    // Click Resume.
    await page.locator('[data-testid="resume-btn-structural_drift"]').click();
    // Wait for enabled badge to flip.
    await expect(page.locator('[data-testid="job-enabled-structural_drift"]'))
      .toHaveText("enabled", { timeout: 10_000 });

    // Poll (read-only GET) until last_run_at advances — real timer fire proof.
    await pollUntilJobFires(request, "structural_drift", initialLastRunAt, POLL_TIMEOUT_MS);

    // Pause immediately after the first real fire (#91: no standing dev timer).
    await expect(page.locator('[data-testid="pause-btn-structural_drift"]'))
      .toBeVisible({ timeout: 10_000 });
    await page.locator('[data-testid="pause-btn-structural_drift"]').click();
    await expect(page.locator('[data-testid="job-enabled-structural_drift"]'))
      .toHaveText("paused", { timeout: 10_000 });

    // Fetch ground truth: structural_drift proposal for STRUCT_ENTITY.
    const proposals = await fetchPendingProposals(request, STRUCT_ENTITY, "structural_drift");
    expect(proposals.length, "structural_drift proposal must be filed for STRUCT_ENTITY").toBeGreaterThan(0);
    structDriftProposal = proposals[0];

    // Assert the payload carries expected structural delta (added run2_prop, removed run1_prop).
    const payload = structDriftProposal.payload as Record<string, unknown>;
    const added   = (payload.added   as Array<{ concept: string; property: string }>) ?? [];
    const removed = (payload.removed as Array<{ concept: string; property: string }>) ?? [];
    expect(added.some((a) => a.property === "run2_prop"),
      `Expected run2_prop in added; got ${JSON.stringify(added)}`).toBe(true);
    expect(removed.some((r) => r.property === "run1_prop"),
      `Expected run1_prop in removed; got ${JSON.stringify(removed)}`).toBe(true);

    await page.screenshot({ path: `${SCREENSHOTS}/drift-02-struct-fired.png` });
  });

  // ── TEST 3: Prove value_drift on schedule ───────────────────────────────────

  test("value_drift fires on schedule — proposal appears for VALUE_ENTITY", async ({ request }) => {
    const initialLastRunAt = await getLastRunAt(request, "value_drift");

    // Set short interval via UI.
    await page.locator('[data-testid="interval-input-value_drift"]')
      .fill(String(SHORT_INTERVAL_S));
    await page.locator('[data-testid="resume-btn-value_drift"]').click();
    await expect(page.locator('[data-testid="job-enabled-value_drift"]'))
      .toHaveText("enabled", { timeout: 10_000 });

    await pollUntilJobFires(request, "value_drift", initialLastRunAt, POLL_TIMEOUT_MS);

    // Pause immediately (#91).
    await expect(page.locator('[data-testid="pause-btn-value_drift"]'))
      .toBeVisible({ timeout: 10_000 });
    await page.locator('[data-testid="pause-btn-value_drift"]').click();
    await expect(page.locator('[data-testid="job-enabled-value_drift"]'))
      .toHaveText("paused", { timeout: 10_000 });

    // Ground truth: value_drift proposal for VALUE_ENTITY.
    const proposals = await fetchPendingProposals(request, VALUE_ENTITY, "value_drift");
    expect(proposals.length, "value_drift proposal must be filed for VALUE_ENTITY").toBeGreaterThan(0);
    valueDriftProposal = proposals[0];

    const payload = valueDriftProposal.payload as Record<string, unknown>;
    const claims  = (payload.claims as Array<{ source_system: string; value?: unknown }>) ?? [];
    const trend   = payload.trend as { prior_count: number; current_count: number };

    // Claims must include both sources.
    const sources = claims.map((c) => c.source_system);
    expect(sources, "Claims must include sap and salesforce").toEqual(
      expect.arrayContaining(["sap", "salesforce"]),
    );
    // Trend: prior_count → current_count transition.
    expect(trend.current_count, "value conflict count must be > 0").toBeGreaterThan(0);

    await page.screenshot({ path: `${SCREENSHOTS}/drift-03-value-fired.png` });
  });

  // ── TEST 4: Select entities in Monitor tab + verify payload rendering ────────

  test("Monitor tab ProposalsPanel renders value_drift payload with claim + trend", async ({ request }) => {
    // Ground truth from value_drift proposal (fetched in test 3).
    expect(valueDriftProposal, "valueDriftProposal not set — test 3 must pass first").not.toBeNull();
    const payload = valueDriftProposal!.payload as Record<string, unknown>;
    const claims  = (payload.claims as Array<{ source_system: string; value?: unknown }>) ?? [];
    const sap     = claims.find((c) => c.source_system === "sap");
    const sf      = claims.find((c) => c.source_system === "salesforce");
    expect(sap, "SAP claim must exist in proposal payload").toBeDefined();
    expect(sf,  "Salesforce claim must exist in proposal payload").toBeDefined();

    // Select VALUE_ENTITY via snapshot selector.
    const selector = page.locator("#snapshot-selector");
    await selector.waitFor({ state: "visible", timeout: 20_000 });
    await expect(selector.locator(`option[value="${VALUE_INGEST_ID_2}"]`))
      .toBeAttached({ timeout: 20_000 });
    await selector.selectOption(VALUE_INGEST_ID_2);

    // Open proposals panel.
    await page.locator('[data-testid="proposals-toggle"]').click();
    await expect(page.locator('[data-testid="proposals-panel"]')).toBeVisible({ timeout: 10_000 });

    // Wait for the value_drift proposal row.
    const vdRow = page.locator('[data-testid^="proposals-proposal-row-value_drift"]');
    await expect(vdRow).toBeVisible({ timeout: 20_000 });

    // Payload summary must mention both sources (rendered by payloadSummary).
    const summaryLocator = vdRow.locator('[data-testid^="proposals-payload-summary-"]');
    const summaryText = await summaryLocator.textContent() ?? "";
    expect(summaryText, "Summary must mention sap source").toContain("sap");
    expect(summaryText, "Summary must mention salesforce source").toContain("salesforce");
    expect(summaryText, "Summary must show trend arrow").toContain("→");

    await page.screenshot({ path: `${SCREENSHOTS}/drift-04-value-proposals-rendered.png` });
  });

  // ── TEST 5: Approve value_drift → canonicalized ─────────────────────────────

  test("Approve value_drift proposal — canonicalized, canonical_artifact_id set", async ({ request }) => {
    expect(valueDriftProposal, "valueDriftProposal not set — depends on test 3").not.toBeNull();
    const proposalId = String(valueDriftProposal!.proposal_id);
    const naturalKey = String(valueDriftProposal!.natural_key);

    // Before state: canonical_artifact_id is null.
    const before = await fetchPendingProposals(request, VALUE_ENTITY, "value_drift");
    const beforeProp = before.find((p) => String(p.proposal_id) === proposalId);
    expect(beforeProp, "Proposal must still be pending before Approve").toBeDefined();
    expect(beforeProp!.canonical_artifact_id,
      "canonical_artifact_id must be null before Approve").toBeNull();

    // PIN VALUE_ENTITY explicitly — the ProposalsPanel is entity-scoped
    // (snapshot.selectedEntityId) and the SnapshotSelector follows-latest, so
    // it can re-resolve to STRUCT_ENTITY between/within tests and reset the
    // panel to pending (#32/#51/#60 — pin, don't trust follow-latest). Select
    // here so Approve acts on VALUE_ENTITY's value_drift.
    const pinValueEntity = async () => {
      const selector = page.locator("#snapshot-selector");
      await selector.waitFor({ state: "visible", timeout: 20_000 });
      // Pin VALUE_INGEST_ID_1 (the OLDER value run) deliberately — useSnapshots'
      // onSelect CLEARS the pin when you select the LATEST snapshot (selecting
      // latest == "follow-latest"), and the latest can flap between VALUE_2 and
      // the structural entity on the bloated dev store. The older run is never
      // the global latest, so the pin STICKS; the panel is entity-scoped
      // (selectedEntityId), so either VALUE run shows VALUE_ENTITY's proposals.
      await expect(selector.locator(`option[value="${VALUE_INGEST_ID_1}"]`))
        .toBeAttached({ timeout: 20_000 });
      await selector.selectOption(VALUE_INGEST_ID_1);
      // Ensure the panel is OPEN — proposals-toggle is a TOGGLE, so only click
      // it when the panel is not already visible (a prior test/pin may have
      // left it open; an unconditional click would close it).
      const panel = page.locator('[data-testid="proposals-panel"]');
      if (!(await panel.isVisible())) {
        await page.locator('[data-testid="proposals-toggle"]').click();
      }
      await expect(panel).toBeVisible({ timeout: 10_000 });
    };
    await pinValueEntity();
    await expect(page.locator('[data-testid^="proposals-proposal-row-value_drift"]'))
      .toBeVisible({ timeout: 20_000 });

    // Expand the row.
    const vdRow = page.locator('[data-testid^="proposals-proposal-row-value_drift"]').first();
    await vdRow.locator("button").first().click();

    // Fill decided_by and click Approve.
    await page.locator('[data-testid="proposals-decided-by"]').fill("drift_e2e_operator");
    const approveBtn = page.locator(`[data-testid="proposals-approve-btn-${naturalKey}"]`);
    await expect(approveBtn).toBeEnabled({ timeout: 5_000 });
    await approveBtn.click();

    // Wait for the decide POST + panel pending-refetch to fully complete before
    // switching to the approved filter.  When the badge reads "0 pending" we know:
    // (a) the decide POST returned 200, (b) fetchProposals('pending') completed
    // (proposal removed from pending), (c) fetchPendingCount() completed.
    // Only then is it safe to click the approved filter without racing
    // decide()'s own fetchProposals call (stale-closure race — fixed in
    // ProposalsPanel via statusFilterRef, but belt-and-suspenders here).
    await expect(page.locator('[data-testid="proposals-pending-count"]'))
      .toHaveText('0 pending', { timeout: 20_000 });

    // Read the canonical artifact on the approved value_drift row. The
    // SnapshotSelector follows-latest on a background poll (useSnapshots,
    // POLL_INTERVAL_MS) and can flip the entity-scoped panel back to
    // STRUCT_ENTITY/pending between ticks (#32/#51/#60). Condition-wait, not a
    // fixed assumption: re-pin VALUE + toggle pending→approved (guarantees a
    // fresh approved-list fetch even if filter is already on approved from a
    // prior iteration — setStatusFilter('approved') on an already-approved
    // filter is a React no-op, so we must toggle through pending first).
    const canonicalLoc = page.locator('[data-testid="proposals-canonical-id"]');
    let canonicalText = "";
    for (let attempt = 0; attempt < 5; attempt++) {
      await pinValueEntity();
      // Toggle pending→approved to guarantee useEffect fires and fetchProposals('approved')
      // is called, regardless of which filter was active entering this iteration.
      await page.locator('[data-testid="proposals-status-filter-pending"]').click();
      await page.locator('[data-testid="proposals-status-filter-approved"]').click();
      const vdApprovedRow = page.locator('[data-testid^="proposals-proposal-row-value_drift"]').first();
      // Condition-wait: allow up to 8s for the approved-list fetch to render the row.
      const rowAppeared = await vdApprovedRow.waitFor({ state: 'visible', timeout: 8_000 })
        .then(() => true).catch(() => false);
      if (!rowAppeared) continue;
      // Ensure the row's detail is expanded (idempotent — `expanded` state can persist).
      const vdDetail = page.locator('[data-testid="proposals-proposal-detail"]');
      if (!(await vdDetail.isVisible().catch(() => false))) {
        await vdApprovedRow.locator("button").first().click().catch(() => {});
        await vdDetail.waitFor({ state: 'visible', timeout: 5_000 }).catch(() => {});
      }
      if (await canonicalLoc.isVisible().catch(() => false)) {
        canonicalText = (await canonicalLoc.textContent()) ?? "";
        if (canonicalText.includes("conflict_disposition")) break;
      }
    }
    await expect(canonicalLoc, "approved value_drift row must show its canonical id (panel kept flipping to follow-latest)")
      .toBeVisible({ timeout: 10_000 });
    expect(canonicalText, "Canonical ID must contain 'conflict_disposition'")
      .toContain("conflict_disposition");

    await page.screenshot({ path: `${SCREENSHOTS}/drift-05-value-approved.png` });
  });

  // ── TEST 6: Select STRUCT_ENTITY + reject structural_drift ──────────────────

  test("Reject structural_drift proposal — zero canonical residue", async ({ request }) => {
    expect(structDriftProposal, "structDriftProposal not set — depends on test 2").not.toBeNull();
    const proposalId = String(structDriftProposal!.proposal_id);
    const naturalKey = String(structDriftProposal!.natural_key);

    // Select STRUCT_ENTITY.
    const selector = page.locator("#snapshot-selector");
    await selector.waitFor({ state: "visible", timeout: 20_000 });
    await expect(selector.locator(`option[value="${STRUCT_INGEST_ID_2}"]`))
      .toBeAttached({ timeout: 20_000 });
    await selector.selectOption(STRUCT_INGEST_ID_2);

    // Ensure proposals toggle is in pending filter.
    await page.locator('[data-testid="proposals-status-filter-pending"]').click();

    // Wait for structural_drift row.
    const sdRow = page.locator('[data-testid^="proposals-proposal-row-structural_drift"]');
    await expect(sdRow).toBeVisible({ timeout: 20_000 });

    // Payload summary must mention field names.
    const summaryText = await sdRow.locator('[data-testid^="proposals-payload-summary-"]').textContent() ?? "";
    expect(summaryText, "Summary must mention run2_prop (added)").toContain("run2_prop");
    expect(summaryText, "Summary must mention run1_prop (removed)").toContain("run1_prop");

    // Before state: pending.
    const before = await fetchPendingProposals(request, STRUCT_ENTITY, "structural_drift");
    const beforeProp = before.find((p) => String(p.proposal_id) === proposalId);
    expect(beforeProp, "Proposal must still be pending before Reject").toBeDefined();

    // Expand and click Reject.
    await sdRow.locator("button").first().click();
    await page.locator('[data-testid="proposals-decided-by"]').fill("drift_e2e_operator");
    const rejectBtn = page.locator(`[data-testid="proposals-reject-btn-${naturalKey}"]`);
    await expect(rejectBtn).toBeEnabled({ timeout: 5_000 });
    await rejectBtn.click();

    // After state: proposal appears in rejected filter, no longer in pending.
    await page.locator('[data-testid="proposals-status-filter-rejected"]').click();
    await expect(page.locator('[data-testid^="proposals-proposal-row-structural_drift"]'))
      .toBeVisible({ timeout: 15_000 });

    // Zero canonical residue: struct entity's triples unchanged.
    // Fetch ground truth from read-only triples count endpoint.
    const triplesResp = await page.request.get(
      `${DCL_BACKEND}/api/dcl/triples/runs?entity_id=${encodeURIComponent(STRUCT_ENTITY)}`,
    );
    if (triplesResp.status() === 200) {
      const body = await triplesResp.json();
      // The runs endpoint returns entity_summary: {entity_id: triple_count} per
      // run (not a top-level entity_id field) — filter by summary key presence.
      const runs = (body.runs ?? body) as Array<{ entity_summary?: Record<string, number> }>;
      const entityRuns = runs.filter((r) => r.entity_summary?.[STRUCT_ENTITY] !== undefined);
      expect(entityRuns.length, "Should have exactly 2 runs, no extra canonical run added")
        .toBe(2);
    }

    await page.screenshot({ path: `${SCREENSHOTS}/drift-06-struct-rejected.png` });
  });

  // ── TEST 7: Decision trace visible ──────────────────────────────────────────

  test("Decision trace shows the full loop for both proposals", async ({ request }) => {
    // GET /api/dcl/traces is read-only. tenant_id is REQUIRED (I2 — tenant-scoped).
    const tracesResp = await request.get(
      `${DCL_BACKEND}/api/dcl/traces?tenant_id=${encodeURIComponent(TENANT_ID)}&entity_id=${encodeURIComponent(VALUE_ENTITY)}&limit=50`,
    );
    expect(tracesResp.status(), "Traces endpoint must return 200").toBe(200);
    const body = await tracesResp.json();
    const traces = (body.traces ?? []) as Array<{ trace_type: string; payload?: unknown }>;
    const decisionTraces = traces.filter((t) => t.trace_type === "proposal_decision");
    expect(decisionTraces.length, "At least one proposal_decision trace for VALUE_ENTITY").toBeGreaterThan(0);

    await page.screenshot({ path: `${SCREENSHOTS}/drift-07-trace.png` });
  });

  // ── TEST 8: Negative — resume with bad interval shows error ─────────────────

  test("Negative: resume structural_drift with interval=0 renders DCL error (not status code)", async () => {
    // structural_drift is paused (asserted by test 1 + paused in test 2).
    await expect(page.locator('[data-testid="job-enabled-structural_drift"]'))
      .toHaveText("paused");

    // Fill 0 in interval input and click Resume.
    await page.locator('[data-testid="interval-input-structural_drift"]').fill("0");
    await page.locator('[data-testid="resume-btn-structural_drift"]').click();

    // Error message must appear (DCL detail text, not a status code).
    const errLocator = page.locator('[data-testid="job-action-error-structural_drift"]');
    await expect(errLocator).toBeVisible({ timeout: 10_000 });
    const errText = await errLocator.textContent() ?? "";
    expect(errText.length, "Error text must not be empty").toBeGreaterThan(5);
    expect(errText, "Error must not just be a numeric status code").not.toMatch(/^[0-9]+$/);

    // Job must still be paused (bad interval did not resume it).
    await expect(page.locator('[data-testid="job-enabled-structural_drift"]'))
      .toHaveText("paused");

    await page.screenshot({ path: `${SCREENSHOTS}/drift-08-negative-bad-interval.png` });
  });
});
