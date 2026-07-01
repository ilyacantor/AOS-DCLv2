// Operator-visible outcome: clicking the "Agent Arc" tab renders the captured agent-context arc — the headline "data_sci_apac · $1145.18 / deploy", an overall PASS badge, all 6 beats showing PASS, and a governance boundary record that joins the scoped identity "finops-rightsizing" to the two action correlation_ids. Every expected value is pulled from the latest public/demo-captures/finops_arc__*.json at test time (never hardcoded, never agent-authored).
//
// TAXONOMY: live-services acceptance for the positive path (real click drives the
// real /api/demo/finops-arc read against the running DCL :8104, rendering the real
// capture). RENDER-ONLY replay: the headless arc ran the real ops and wrote the
// capture; this UI renders it. The paired negative test stubs the read to prove the
// no-silent-fallback error surface (regression).

import { test, expect } from 'playwright/test';
import * as fs from 'fs';
import * as path from 'path';

const APP = 'http://localhost:3004/';
const CAPTURES_DIR = path.resolve(__dirname, '../../public/demo-captures');

// Resolve the LATEST capture exactly as the backend does (newest by mtime).
function latestCapture(): any {
  const files = fs
    .readdirSync(CAPTURES_DIR)
    .filter((f) => /^finops_arc__.*\.json$/.test(f))
    .map((f) => ({ f, m: fs.statSync(path.join(CAPTURES_DIR, f)).mtimeMs }))
    .sort((a, b) => a.m - b.m);
  if (files.length === 0) throw new Error(`no finops_arc capture in ${CAPTURES_DIR}`);
  const newest = files[files.length - 1].f;
  return JSON.parse(fs.readFileSync(path.join(CAPTURES_DIR, newest), 'utf-8'));
}

async function openAgentArc(page: any) {
  await page.goto(APP);
  await page.getByRole('button', { name: 'Agent Arc' }).click();
}

test('Agent Arc renders the captured arc: headline, PASS, 6 beats, governance join', async ({ page }) => {
  const cap = latestCapture();
  const expectedTeam = cap.headline.worst_efficiency_team; // data_sci_apac
  const expectedPerDeploy = cap.headline.usd_per_deploy.toFixed(2); // 1145.18
  const br = cap.headline.boundary_record;

  await openAgentArc(page);

  // Header: overall PASS badge + the headline (worst team + the $/deploy answer).
  await expect(page.getByTestId('arc-overall')).toHaveText(cap.overall, { timeout: 20_000 });
  const headline = page.getByTestId('arc-headline');
  await expect(headline).toContainText(expectedTeam);
  await expect(headline).toContainText(expectedPerDeploy);

  // The 6-beat timeline — each beat shows its captured status (all PASS).
  expect(cap.beats.length).toBe(6);
  for (const b of cap.beats) {
    await expect(page.getByTestId(`beat-${b.beat}`)).toContainText(b.status);
  }
  await expect(page.locator('[data-testid^="beat-"]')).toHaveCount(cap.beats.length);

  // Beat 3 traversal graph renders via the Glass Box React-Flow canvas: the
  // billing-only path is excised, the cross-source answer node is the worst team.
  await expect(page.locator('.react-flow__node', { hasText: expectedTeam })).toBeVisible();
  await expect(page.locator('.react-flow__node', { hasText: cap.beats[2].values.provenance.cost_source })).toBeVisible();
  await expect(page.locator('.react-flow__node', { hasText: cap.beats[2].values.provenance.output_source })).toBeVisible();

  // Beat 4 action rows — both modes present with their correlation_ids.
  await expect(page.getByTestId('action-hitl')).toContainText(cap.headline.action_correlation_ids.hitl);
  await expect(page.getByTestId('action-autonomous')).toContainText(cap.headline.action_correlation_ids.autonomous);

  // Beat 5 governance boundary record — the read↔action join, tied by identity +
  // correlation_id. who-asked, what-resolved, and BOTH action correlation_ids.
  const gov = page.getByTestId('governance-record');
  await expect(gov).toContainText(br.who_asked); // finops-rightsizing
  await expect(gov).toContainText(br.what_resolved.worst_efficiency_team);
  for (const cid of br.what_action_correlation_ids) {
    await expect(gov).toContainText(cid);
  }

  // Beat 6 revocation strip — before(allowed) → denied → restored, real counts.
  const strip = page.getByTestId('revocation-strip');
  await expect(strip).toContainText(String(cap.headline.revoke.before_rows));
  await expect(strip).toContainText(cap.headline.revoke.after_narrow); // denied
  await expect(strip).toContainText(String(cap.headline.revoke.confirm_rows));

  await page.screenshot({ path: 'tests/e2e/screenshots/agent_arc.png', fullPage: true });
});

test('Agent Arc surfaces a readable error when no capture exists (no silent blank)', async ({ page }) => {
  // Simulate the backend's no-silent-fallback 404 (the read the UI itself fires).
  await page.route('**/api/demo/finops-arc', (r) =>
    r.fulfill({
      status: 404,
      contentType: 'application/json',
      body: JSON.stringify({
        detail: 'No finops_arc capture found. Run `python -m demo.finops_arc` first to produce one.',
      }),
    }),
  );

  await openAgentArc(page);

  // The actionable message reaches the user; nothing is rendered as resolved.
  await expect(page.getByTestId('arc-error')).toContainText('python -m demo.finops_arc', { timeout: 15_000 });
  await expect(page.getByTestId('arc-headline')).toHaveCount(0);
});
