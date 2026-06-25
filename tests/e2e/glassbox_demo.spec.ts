// Operator-visible outcome: clicking "Ask" streams a 3-hop trace; the canvas shows workday_main $2.64M (active) and shadow_crm $8.64M (cancelled), then prunes the $8.64M shadow_crm node (reason negative_status_unauthorized_source) and routes the surviving $2.64M into a SUM operator block; the chat answers "$2.64M"; clicking the survivor opens a drawer showing source_system=workday_main + its bitemporal_id.
//
// TAXONOMY: regression (mocked engine). The engine is in RAILS MODE — the SSE
// stream replays demo/glassbox_trace.json, so this is NOT live-services
// acceptance. It drives the real operator path through real UI clicks and pulls
// every expected value from the fixture at runtime (no hardcoded/agent-authored
// expectations). Live-services acceptance is deferred until contextOS is
// extracted (see dcl_deferred_work.md).

import { test, expect } from 'playwright/test'
import * as fs from 'fs'
import * as path from 'path'

const APP = 'http://localhost:3004/glassbox'

// Ground truth: the captured trace fixture IS the source of truth for a replay.
const trace = JSON.parse(
  fs.readFileSync(path.resolve(__dirname, '../../demo/glassbox_trace.json'), 'utf-8'),
)
const retrieve = trace.events.find((e: any) => e.stage === 'RETRIEVE')
const prune = trace.events.find((e: any) => e.stage === 'PRUNE')
const compute = trace.events.find((e: any) => e.stage === 'COMPUTE')
const droppedId: string = prune.dropped[0].id
const droppedReason: string = prune.dropped[0].reason
const survivorId: string = prune.survived[0]
const survivor = retrieve.nodes.find((n: any) => n.id === survivorId)
const dropped = retrieve.nodes.find((n: any) => n.id === droppedId)

test('Glass Box resolves the lateral conflict and computes the surviving run-rate', async ({ page }) => {
  await page.goto(APP)
  await page.waitForLoadState('networkidle') // let any Vite dep re-optimize settle

  // The buyer-facing honesty affordance is present (replay, not live).
  await expect(page.getByTestId('replay-tag')).toHaveText(/captured lab trace · replay/)

  await page.getByTestId('run-trace').click()

  // RETRIEVE — both conflicting nodes spawn with their source + value.
  const nodeA = page.locator(`[data-node-id="${survivorId}"]`)
  const nodeB = page.locator(`[data-node-id="${droppedId}"]`)
  await expect(nodeA).toContainText(survivor.source, { timeout: 15000 })
  await expect(nodeA).toContainText(survivor.value_label)
  await expect(nodeB).toContainText(dropped.source)
  await expect(nodeB).toContainText(dropped.value_label)

  // PRUNE — the shadow/unauthorized node is killed and severed; survivor stays.
  await expect(nodeB).toHaveAttribute('data-dropped', 'true', { timeout: 15000 })
  await expect(page.getByTestId(`drop-${droppedId}`)).toContainText(droppedReason)
  await expect(nodeA).toHaveAttribute('data-dropped', 'false')

  // COMPUTE — surviving value routed into the deterministic SUM operator block.
  await expect(page.getByTestId('operator-result')).toHaveText(compute.result_label, { timeout: 15000 })

  // The chat answers the surviving value and explains the pruned decoy.
  await expect(page.getByTestId('result-value')).toHaveText(compute.result_label, { timeout: 15000 })
  await expect(page.getByTestId('trace-answer')).toContainText(dropped.value_label)
  await expect(page.getByTestId('trace-answer')).toContainText('pruned')

  // Audit drawer — clicking the survivor reveals its raw Postgres row.
  await nodeA.click()
  await expect(page.getByTestId('drawer-source')).toHaveText(survivor.raw_row.source_system)
  await expect(page.getByTestId('drawer-bitemporal')).toHaveText(survivor.raw_row.bitemporal_id)
  await expect(page.getByTestId('drawer-updated')).toHaveText(survivor.raw_row.updated_at)

  await page.screenshot({ path: 'tests/e2e/screenshots/glassbox_resolved.png', fullPage: true })
})

test('Glass Box surfaces a readable error when the trace stream fails (no silent blank)', async ({ page }) => {
  // Inject a network failure on the SSE endpoint — the app must say so, not
  // sit blank pretending it resolved.
  await page.route('**/api/demo/stream-trace', (r) => r.abort())
  await page.goto(APP)
  await page.waitForLoadState('networkidle')
  await page.getByTestId('run-trace').click()

  await expect(page.getByTestId('trace-error')).toContainText('Trace stream failed', { timeout: 15000 })
  // The failure path must NOT render a computed result.
  await expect(page.getByTestId('operator-result')).toHaveCount(0)
  await expect(page.getByTestId('result-value')).toHaveCount(0)
})
