// Operator-visible outcome: picking a question auto-runs a React-Flow trace. For "eng_runrate" the "Source: Shadow CRM" node ends excised (line-through, detail "Cancelled · unauthorized source. Excised."), its incoming edge turns red+dashed, and the chat answers "$2.64M". For "salesforce_blast_radius" the link node "Hidden Dependency" lights up and the chat answers "9 systems · 2 teams". The gallery shows exactly the 5 preselected questions with no category buckets.
//
// TAXONOMY: regression (mocked engine). RAILS MODE — the trace is replayed from
// demo/glassbox_gallery.json, so this is NOT live-services acceptance. It drives
// the real operator path through real clicks and pulls every expected value from
// the fixture at runtime (no hardcoded/agent-authored expectations).

import { test, expect } from 'playwright/test'
import * as fs from 'fs'
import * as path from 'path'

const APP = 'http://localhost:3004/glassbox'
const gallery = JSON.parse(fs.readFileSync(path.resolve(__dirname, '../../demo/glassbox_gallery.json'), 'utf-8'))
const q = (id: string) => gallery.questions.find((x: any) => x.id === id)
const node = (page: any, label: string) => page.locator('.react-flow__node', { hasText: label })

test('Conflict story: excises the shadow source and answers from the system of record', async ({ page }) => {
  const g = q('eng_runrate').graph
  const decoy = g.candidates.find((c: any) => c.verdict === 'excised')

  await page.goto(APP)
  await page.waitForLoadState('networkidle')
  await page.getByTestId('q-eng_runrate').click()

  // The decoy source ends excised, showing its fixture-authored excision reason.
  const decoyNode = node(page, decoy.label)
  await expect(decoyNode.getByText(decoy.excisedDetail)).toBeVisible({ timeout: 12_000 })

  // Its incoming edge is severed: red, dashed, animation halted.
  const severed = page.locator(`.react-flow__edge[data-id="e-intake-${decoy.id}"] .react-flow__edge-path`)
  await expect(severed).toHaveCSS('stroke', 'rgb(239, 68, 68)')
  await expect(severed).toHaveCSS('stroke-dasharray', '6px, 5px')
  await expect(page.locator(`.react-flow__edge[data-id="e-intake-${decoy.id}"]`)).not.toHaveClass(/animated/)

  // The trusted answer (from the fixture) lands in the chat. The node graph is
  // intake + 2 candidates + verify + reducer = 5 nodes.
  await expect(page.locator('.whitespace-pre-line', { hasText: g.answer.headline })).toBeVisible({ timeout: 12_000 })
  await expect(page.locator('.react-flow__node')).toHaveCount(5)
  await page.screenshot({ path: 'tests/e2e/screenshots/glassbox_excision.png', fullPage: true })
})

test('Traversal story: lights the hidden-link node and lands the plain-English answer', async ({ page }) => {
  const item = q('salesforce_blast_radius')
  const g = item.graph

  await page.goto(APP)
  await page.waitForLoadState('networkidle')
  await page.getByTestId('q-salesforce_blast_radius').click()

  // The link/verify node carries the fixture's "aha", and the answer lands.
  await expect(node(page, g.link.label)).toBeVisible({ timeout: 12_000 })
  await expect(page.locator('.whitespace-pre-line', { hasText: g.answer.headline })).toBeVisible({ timeout: 12_000 })
  // Traversal has no decoy — no edge is severed.
  await expect(page.locator('.react-flow__edge .react-flow__edge-path[stroke="#ef4444"]')).toHaveCount(0)
})

test('Fetch failure surfaces a readable error (no silent blank)', async ({ page }) => {
  await page.route('**/api/demo/trace*', (r) => r.abort())
  await page.goto(APP)
  await page.waitForLoadState('networkidle')
  await page.getByTestId('q-eng_runrate').click()

  await expect(page.getByTestId('trace-error')).toContainText("Couldn't load", { timeout: 15_000 })
  // Nothing is shown as resolved: no reducer node reaches the canvas.
  await expect(page.locator('.react-flow__node', { hasText: 'Compute Reducer' })).toHaveCount(0)
})

test('Gallery presents exactly the 5 preselected questions, with no category buckets', async ({ page }) => {
  await page.goto(APP)
  await page.waitForLoadState('networkidle')

  for (const item of gallery.questions) {
    await expect(page.getByTestId(`q-${item.id}`)).toBeVisible()
  }
  await expect(page.locator('[data-testid^="q-"]')).toHaveCount(gallery.questions.length)
  expect(gallery.questions.length).toBe(5)

  // The retired categorization must not appear anywhere.
  for (const c of ['Grow Revenue', 'See the Real Risk', 'Stop the Leakage', 'Operate with Confidence']) {
    await expect(page.getByText(c, { exact: true })).toHaveCount(0)
  }
  await page.screenshot({ path: 'tests/e2e/screenshots/glassbox_gallery.png', fullPage: true })
})
