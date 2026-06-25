// Operator-visible outcome: the Glass Box shows a verticalized question gallery (General/BFSI/Healthcare). Picking the BFSI "supply-chain whitespace" question streams a traversal that hops the payment graph, lights a "relationship found" node (Cedar Logistics — not our customer), and assembles "7 prospects · $6.2M / yr"; picking the General run-rate question prunes the $8.64M shadow_crm decoy and computes $2.64M. Clicking any node opens its raw row.
//
// TAXONOMY: regression (mocked engine). RAILS MODE — the SSE replays
// demo/glassbox_gallery.json, so this is NOT live-services acceptance. It drives
// the real operator path through real clicks and pulls every expected value from
// the fixture at runtime (no hardcoded/agent-authored expectations). Live-services
// acceptance is deferred (dcl_deferred_work.md).

import { test, expect } from 'playwright/test'
import * as fs from 'fs'
import * as path from 'path'

const APP = 'http://localhost:3004/glassbox'

const gallery = JSON.parse(
  fs.readFileSync(path.resolve(__dirname, '../../demo/glassbox_gallery.json'), 'utf-8'),
)
const q = (id: string) => gallery.questions.find((x: any) => x.id === id)
const ev = (question: any, stage: string) => question.events.find((e: any) => e.stage === stage)

test('BFSI traversal: finds the hidden whitespace relationship and assembles the revenue answer', async ({ page }) => {
  const ws = q('supply_chain_whitespace')
  const discovered = ws.events.find((e: any) => e.stage === 'TRAVERSE' && e.discovered)
  const compute = ev(ws, 'COMPUTE')

  await page.goto(APP)
  await page.waitForLoadState('networkidle')

  await page.getByTestId('q-supply_chain_whitespace').click()

  // The non-obvious relationship is surfaced and flagged.
  const found = page.locator(`[data-node-id="${discovered.node.id}"]`)
  await expect(found).toHaveAttribute('data-discovered', 'true', { timeout: 15000 })
  await expect(found).toContainText(discovered.node.label)
  await expect(page.getByTestId('discovered-ribbon').first()).toBeVisible()

  // The answer is assembled deterministically from the traversed path.
  await expect(page.getByTestId('operator-result')).toHaveText(compute.result_label, { timeout: 15000 })
  await expect(page.getByTestId('result-value')).toHaveText(compute.result_label)
  await expect(page.getByTestId('trace-answer')).toContainText('Cedar Logistics')

  // Audit drawer on the discovered hop shows its raw row.
  await found.click()
  await expect(page.getByTestId('drawer-source')).toHaveText(discovered.node.raw_row.source_system)
  await expect(page.getByTestId('drawer-bitemporal')).toHaveText(discovered.node.raw_row.bitemporal_id)

  await page.screenshot({ path: 'tests/e2e/screenshots/glassbox_traversal.png', fullPage: true })
})

test('General conflict: prunes the unauthorized $8.64M decoy and computes $2.64M', async ({ page }) => {
  const rr = q('eng_runrate')
  const retrieve = ev(rr, 'RETRIEVE')
  const prune = ev(rr, 'PRUNE')
  const compute = ev(rr, 'COMPUTE')
  const survivor = retrieve.nodes.find((n: any) => n.id === prune.survived[0])
  const dropped = retrieve.nodes.find((n: any) => n.id === prune.dropped[0].id)

  await page.goto(APP)
  await page.waitForLoadState('networkidle')
  await page.getByTestId('q-eng_runrate').click()

  const nodeA = page.locator(`[data-node-id="${survivor.id}"]`)
  const nodeB = page.locator(`[data-node-id="${dropped.id}"]`)
  await expect(nodeA).toContainText(survivor.value_label, { timeout: 15000 })
  await expect(nodeB).toContainText(dropped.value_label)
  await expect(nodeB).toHaveAttribute('data-dropped', 'true', { timeout: 15000 })
  await expect(page.getByTestId(`drop-${dropped.id}`)).toContainText(prune.dropped[0].reason)

  await expect(page.getByTestId('operator-result')).toHaveText(compute.result_label, { timeout: 15000 })
  await expect(page.getByTestId('result-value')).toHaveText(compute.result_label)
  await expect(page.getByTestId('trace-answer')).toContainText(dropped.value_label)
})

test('Stream failure surfaces a readable error (no silent blank)', async ({ page }) => {
  await page.route('**/api/demo/stream-trace*', (r) => r.abort())
  await page.goto(APP)
  await page.waitForLoadState('networkidle')
  await page.getByTestId('q-eng_runrate').click()

  await expect(page.getByTestId('trace-error')).toContainText('Trace stream failed', { timeout: 15000 })
  await expect(page.getByTestId('operator-result')).toHaveCount(0)
  await expect(page.getByTestId('result-value')).toHaveCount(0)
})

test('Glass Box is reachable as a tab inside the DCL console', async ({ page }) => {
  const compute = ev(q('eng_runrate'), 'COMPUTE')
  await page.goto('http://localhost:3004/')
  await page.getByRole('button', { name: 'Glass Box', exact: true }).click()
  await expect(page.getByTestId('replay-tag')).toHaveText(/captured lab trace · replay/)
  await page.getByTestId('q-eng_runrate').click()
  await expect(page.getByTestId('operator-result')).toHaveText(compute.result_label, { timeout: 15000 })
  await page.screenshot({ path: 'tests/e2e/screenshots/glassbox_tab.png', fullPage: true })
})
