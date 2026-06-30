// Operator-visible outcome: picking a question auto-runs a React-Flow trace. The LEAD question "agent_rightsize" is agentic — contextOS relates cost x output (the "Source: Billing" node ends excised: a single system can't rank efficiency), the chat reports "data_sci_apac — worst $/deploy", and the agent's terminal node "Pinpoints the idle instances" is tagged FinOps agent while the source/relate nodes are tagged contextOS. The second question "agent_execute" is the core function — the FinOps agent right-sizes a CONCRETE EC2 instance (proposal "c5.2xlarge → c5.large", then HITL "Head of Cloud Platform approves → executed"), every node tagged by system (contextOS / FinOps agent / HITL). For "eng_runrate" the "Source: Shadow CRM" node ends excised (line-through, detail "Cancelled · unauthorized source. Excised."), its incoming edge turns red+dashed, and the chat answers "$2.64M". For "salesforce_blast_radius" the link node "Hidden Dependency" lights up and the chat answers "9 systems · 2 teams". The gallery shows exactly the 6 preselected questions with no category buckets.
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
  // The request source is labeled in the picker — here a human persona.
  await expect(page.getByTestId('q-eng_runrate')).toContainText(q('eng_runrate').asker)
  await page.screenshot({ path: 'tests/e2e/screenshots/glassbox_excision.png', fullPage: true })
})

test('Agentic lead: contextOS surfaces the worst $/deploy team; the agent pinpoints the idle instances', async ({ page }) => {
  const item = q('agent_rightsize')
  const g = item.graph
  const billing = g.candidates.find((c: any) => c.verdict === 'excised')

  await page.goto(APP)
  await page.waitForLoadState('networkidle')

  // "This should lead": the agentic question is first in the gallery, and its
  // request source is labeled — the lead is an AGENT (FinOps Agent), not a human.
  await expect(page.locator('[data-testid^="q-"]').first()).toHaveAttribute('data-testid', 'q-agent_rightsize')
  await expect(page.getByTestId('q-agent_rightsize')).toContainText(item.asker)
  await page.getByTestId('q-agent_rightsize').click()

  // A single billing system has no output dimension — it's excised. That excision
  // is the whole point: only the cross-source join can rank efficiency.
  const billingNode = node(page, billing.label)
  await expect(billingNode.getByText(billing.excisedDetail)).toBeVisible({ timeout: 12_000 })
  await expect(
    page.locator(`.react-flow__edge[data-id="e-intake-${billing.id}"] .react-flow__edge-path`),
  ).toHaveCSS('stroke', 'rgb(239, 68, 68)')

  // The agent's terminal step is "pinpoints the idle instances" — it does NOT
  // right-size a team — and the chat reports the contextOS insight.
  await expect(node(page, g.action.label)).toBeVisible({ timeout: 12_000 })
  await expect(page.locator('.whitespace-pre-line', { hasText: g.answer.headline })).toBeVisible({ timeout: 12_000 })
  // intake + 3 candidate sources + cross-source verify + agent step = 6 nodes.
  await expect(page.locator('.react-flow__node')).toHaveCount(6)
  // The boundary is denoted: the cross-source context is contextOS, the agent's
  // pinpoint step is the FinOps agent.
  await expect(page.getByTestId('system-contextOS').first()).toBeVisible()
  await expect(page.getByTestId('system-agent').first()).toBeVisible()
  // The source label also renders above the chat message — the actual interaction.
  await expect(page.getByTestId('chat-messages').getByText(item.asker, { exact: true })).toHaveCount(1)
  await page.screenshot({ path: 'tests/e2e/screenshots/glassbox_agentic.png', fullPage: true })
})

test('Agentic execution: the agent right-sizes a concrete EC2 instance, gated by HITL — agent/contextOS denoted', async ({ page }) => {
  const item = q('agent_execute')
  const g = item.graph

  await page.goto(APP)
  await page.waitForLoadState('networkidle')
  await expect(page.getByTestId('q-agent_execute')).toContainText(item.asker)
  await page.getByTestId('q-agent_execute').click()

  // The action is a CONCRETE EC2 rightsize, not a "team" right-size: the agent's
  // proposal (c5.2xlarge → c5.large) and the HITL execution both land.
  await expect(node(page, g.link.label)).toBeVisible({ timeout: 12_000 })
  await expect(node(page, g.action.label)).toBeVisible({ timeout: 12_000 })
  await expect(page.locator('.whitespace-pre-line', { hasText: g.answer.headline })).toBeVisible({ timeout: 12_000 })

  // Every step is denoted by the system it runs in: contextOS supplies the signal,
  // the FinOps agent proposes, and the HITL gate approves before execution.
  await expect(page.getByTestId('system-contextOS').first()).toBeVisible()
  await expect(page.getByTestId('system-agent').first()).toBeVisible()
  await expect(page.getByTestId('system-hitl')).toBeVisible()

  // intake + contextOS signal + agent telemetry + proposal + HITL execution = 5 nodes.
  await expect(page.locator('.react-flow__node')).toHaveCount(5)
  await expect(page.getByTestId('chat-messages').getByText(item.asker, { exact: true })).toHaveCount(1)
  await page.screenshot({ path: 'tests/e2e/screenshots/glassbox_agent_execute.png', fullPage: true })
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

test('Gallery presents exactly the 7 preselected questions, with no category buckets', async ({ page }) => {
  await page.goto(APP)
  await page.waitForLoadState('networkidle')

  for (const item of gallery.questions) {
    await expect(page.getByTestId(`q-${item.id}`)).toBeVisible()
  }
  await expect(page.locator('[data-testid^="q-"]')).toHaveCount(gallery.questions.length)
  expect(gallery.questions.length).toBe(7)

  // The retired categorization must not appear anywhere.
  for (const c of ['Grow Revenue', 'See the Real Risk', 'Stop the Leakage', 'Operate with Confidence']) {
    await expect(page.getByText(c, { exact: true })).toHaveCount(0)
  }
  await page.screenshot({ path: 'tests/e2e/screenshots/glassbox_gallery.png', fullPage: true })
})
