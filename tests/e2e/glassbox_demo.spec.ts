// Operator-visible outcome: picking a question reveals a plain-English walk-through one click at a time. The "Grow Revenue" whitespace story flags "Cedar banks somewhere else…" as the link and ends in "$6.2M / yr"; the run-rate story ends in "$2.64M" after noting the pruned $8.64M; clicking a step's "view record" opens its raw row (source_system + bitemporal_id).
//
// TAXONOMY: regression (mocked engine). RAILS MODE — the trace is replayed from
// demo/glassbox_gallery.json, so this is NOT live-services acceptance. It drives
// the real operator path through real clicks and pulls every expected value from
// the fixture at runtime (no hardcoded/agent-authored expectations).

import { test, expect, type Page } from 'playwright/test'
import * as fs from 'fs'
import * as path from 'path'

const APP = 'http://localhost:3004/glassbox'
const gallery = JSON.parse(fs.readFileSync(path.resolve(__dirname, '../../demo/glassbox_gallery.json'), 'utf-8'))
const q = (id: string) => gallery.questions.find((x: any) => x.id === id)

async function revealAll(page: Page, beatCount: number) {
  const btn = page.getByTestId('reveal-next')
  for (let i = 0; i < beatCount + 1; i++) {
    if (await btn.isVisible().catch(() => false)) await btn.click()
  }
}

test('Grow-Revenue story: flags the hidden link and lands the plain-English answer', async ({ page }) => {
  const ws = q('supply_chain_whitespace')
  const beats = ws.story.beats
  const link = beats.find((b: any) => b.link)
  const answer = ws.story.answer

  await page.goto(APP)
  await page.waitForLoadState('networkidle')
  await page.getByTestId('q-supply_chain_whitespace').click()
  await revealAll(page, beats.length)

  await expect(page.getByTestId(`beat-${link.id}`)).toContainText(link.text)
  await expect(page.getByTestId('answer-headline')).toHaveText(answer.headline)
  await page.screenshot({ path: 'tests/e2e/screenshots/glassbox_story.png', fullPage: true })

  // The raw record behind the link step is one click away.
  await page.getByTestId(`record-${link.id}`).click()
  await expect(page.getByTestId('drawer-source')).toHaveText(link.record.source_system)
  await expect(page.getByTestId('drawer-bitemporal')).toHaveText(link.record.bitemporal_id)
})

test('Conflict story: ends in $2.64M after noting the pruned $8.64M', async ({ page }) => {
  const rr = q('eng_runrate')
  const beats = rr.story.beats
  const answer = rr.story.answer
  const pruneBeat = beats.find((b: any) => b.text.includes('$8.64M'))

  await page.goto(APP)
  await page.waitForLoadState('networkidle')
  await page.getByTestId('q-eng_runrate').click()
  await revealAll(page, beats.length)

  await expect(page.getByTestId('answer-headline')).toHaveText(answer.headline)
  await expect(page.getByTestId(`beat-${pruneBeat.id}`)).toContainText('$8.64M')
})

test('Fetch failure surfaces a readable error (no silent blank)', async ({ page }) => {
  await page.route('**/api/demo/trace*', (r) => r.abort())
  await page.goto(APP)
  await page.waitForLoadState('networkidle')
  await page.getByTestId('q-eng_runrate').click()

  await expect(page.getByTestId('trace-error')).toContainText("Couldn't load", { timeout: 15000 })
  await expect(page.getByTestId('answer-headline')).toHaveCount(0)
})

test('Gallery presents all 10 preselected questions across the four outcome groups', async ({ page }) => {
  await page.goto(APP)
  await page.waitForLoadState('networkidle')
  for (const item of gallery.questions) {
    await expect(page.getByTestId(`q-${item.id}`)).toBeVisible()
  }
  for (const c of ['Grow Revenue', 'See the Real Risk', 'Stop the Leakage', 'Operate with Confidence']) {
    await expect(page.getByText(c, { exact: true })).toBeVisible()
  }
  await page.screenshot({ path: 'tests/e2e/screenshots/glassbox_gallery.png', fullPage: true })
})
