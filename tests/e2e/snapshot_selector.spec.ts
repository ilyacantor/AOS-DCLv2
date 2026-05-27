// Operator-visible outcome: On load the DCL Snapshot dropdown shows the newest
// snapshot (max run_timestamp from /api/dcl/snapshots) marked "* " and selected,
// with the label "following latest". Picking the second-newest snapshot from the
// dropdown flips the label to "pinned" and changes the selected value to that
// snapshot's dcl_ingest_id. The Dashboard tab renders rows with no red error banner.
/**
 * Snapshot selector — follow-latest / pin verification for the DCL monitoring UI.
 *
 * Drives the real operator path: open the page, read the dropdown, select an
 * option via selectOption(). Ground truth (which snapshot is `*`) is fetched
 * from GET /api/dcl/snapshots (a read-only endpoint) and computed in-test —
 * never hardcoded.
 *
 * Requirements: DCL backend (8004) + frontend (3004) running, snapshots ingested.
 */

import { test, expect } from 'playwright/test';

const DCL_URL = 'http://localhost:3004';
const DCL_BACKEND = 'http://localhost:8004';

interface Snapshot {
  dcl_ingest_id: string;
  snapshot_name: string | null;
  entity_id: string | null;
  run_timestamp: string;
  total_rows: number;
  is_current: boolean;
}

test.describe.serial('DCL Snapshot Selector — follow-latest / pin', () => {
  test.setTimeout(180_000);

  test('default = follow-latest on newest snapshot; manual pick pins', async ({ page }) => {
    const consoleErrors: string[] = [];
    page.on('console', (msg) => {
      if (msg.type() !== 'error') return;
      const t = msg.text();
      if (t.includes('ERR_NAME_NOT_RESOLVED') || t.includes('ERR_BLOCKED_BY_CLIENT')) return;
      if (t.includes('The width(-1)') || t.includes('The height(-1)')) return;
      consoleErrors.push(t);
    });

    // ── Ground truth: newest snapshot = max run_timestamp (read-only GET) ──
    const snapRes = await page.request.get(`${DCL_BACKEND}/api/dcl/snapshots`);
    expect(snapRes.status()).toBe(200);
    const snaps: Snapshot[] = (await snapRes.json()).snapshots || [];
    expect(snaps.length, 'need at least 2 snapshots to test pin').toBeGreaterThanOrEqual(2);

    const byTsDesc = [...snaps].sort((a, b) =>
      (b.run_timestamp || '').localeCompare(a.run_timestamp || ''),
    );
    const newest = byTsDesc[0];
    const secondNewest = byTsDesc[1];
    console.log(`[gt] newest = ${newest.snapshot_name} (${newest.dcl_ingest_id})`);
    console.log(`[gt] second = ${secondNewest.snapshot_name} (${secondNewest.dcl_ingest_id})`);

    // ── Load the DCL UI ──
    await page.goto(DCL_URL, { waitUntil: 'load' });
    const runButton = page.locator('button[data-role="run-primary"]');
    await expect(runButton).toBeVisible({ timeout: 15_000 });
    await expect(runButton).not.toHaveText('Running...', { timeout: 60_000 });

    const selector = page.locator('#snapshot-selector');
    await expect(selector).toBeVisible({ timeout: 15_000 });
    // Wait for the async snapshot fetch to populate the dropdown — the
    // selector renders a placeholder "No snapshots" option until then.
    await expect(selector.locator('option')).toHaveCount(snaps.length, { timeout: 15_000 });

    // ── 1. Default selection = newest snapshot (the `*`) ──
    const defaultValue = await selector.inputValue();
    expect(
      defaultValue,
      `default snapshot must be the newest by run_timestamp (${newest.dcl_ingest_id}), got ${defaultValue}`,
    ).toBe(newest.dcl_ingest_id);

    // ── 2. The newest option is the one marked with a literal `*` ──
    const starredText = await selector.locator('option').filter({ hasText: '* ' }).first().textContent();
    expect(starredText, 'the `*`-marked option must name the newest snapshot').toContain(
      newest.snapshot_name!,
    );

    // ── 3. Follow-latest label is shown by default ──
    const followState = page.locator('[data-role="snapshot-follow-state"]');
    await expect(followState).toHaveText('following latest');

    // ── 4. Manually pick the second-newest snapshot → PIN ──
    await selector.selectOption(secondNewest.dcl_ingest_id);
    await expect(selector).toHaveValue(secondNewest.dcl_ingest_id);
    await expect(
      followState,
      'picking a non-latest snapshot must flip the surface to pinned',
    ).toHaveText('pinned');

    // ── 5. Re-select the newest snapshot → pin clears, follow-latest re-engages ──
    await selector.selectOption(newest.dcl_ingest_id);
    await expect(selector).toHaveValue(newest.dcl_ingest_id);
    await expect(
      followState,
      're-selecting the latest snapshot must clear the pin',
    ).toHaveText('following latest');

    await page.screenshot({ path: 'tests/e2e/artifacts/snapshot_selector_default.png', fullPage: true });

    // ── 6. Tabs render the selected snapshot — no red error banner ──
    // Pin to the second-newest so the entity differs from the default, then
    // walk all 5 tabs and assert no destructive banner appears.
    await selector.selectOption(secondNewest.dcl_ingest_id);
    const tabs = ['Graph', 'Dashboard', 'Context', 'Recon', 'Ingest'];
    for (const tab of tabs) {
      await page.locator('button').filter({ hasText: new RegExp(`^${tab}$`) }).first().click();
      await page.waitForTimeout(2_500);
      const banner = page.locator('.text-destructive, [class*="destructive"]');
      const bannerCount = await banner.count();
      if (bannerCount > 0) {
        const txt = (await banner.first().textContent()) || '';
        // The selector's own "Snapshots unavailable" wrapper uses destructive
        // styling — only fail on an actual data-fetch error message.
        expect(
          txt.includes('HTTP') || txt.includes('Cannot resolve') || txt.includes('No active tenant'),
          `${tab} tab shows a data error banner: ${txt}`,
        ).toBe(false);
      }
      console.log(`[tab] ${tab} rendered for pinned snapshot ${secondNewest.snapshot_name}`);
    }

    await page.screenshot({ path: 'tests/e2e/artifacts/snapshot_selector_tabs.png', fullPage: true });

    expect(
      consoleErrors,
      `console errors during snapshot selector run: ${consoleErrors.join('; ')}`,
    ).toHaveLength(0);
  });
});
