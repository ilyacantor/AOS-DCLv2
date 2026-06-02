// Operator-visible outcome: After the Confluent orders feed lands on the dev
// DCL, an operator who opens the DCL monitoring UI, picks that snapshot, and
// opens the Context tab sees a "Revenue" row with Triples = 2 in the Domain
// Coverage table and NO "Cloud Spend" row — the order amounts (amount_usd)
// surfaced as revenue, not cloud spend, with source "confluent". (#49)
/**
 * Orders -> revenue classification — B17 acceptance for the context-aware mapper
 * fix (#49). amount_usd EXACT-matches cloud_spend's example_fields, so the raw
 * name default is cloud_spend; DCL's pipe co-occurrence routes it to revenue
 * (currency anchors finance for the pipe). This test proves the operator sees
 * that result rendered, and never sees the order amount under cloud spend.
 *
 * Targets the dev stack: UI on :3004 (vite proxies /api -> :8104), ground truth
 * read read-only from :8104. The order batch is delivered through DCL's real
 * records-path contract (/api/dcl/ingest-records) — the same contract AAM's
 * event-bus transport hands off to. There is no operator UI button that fires
 * the event-bus fabric feed (data arrives via the automated transport), so the
 * batch is the pipeline-delivery precondition (B15); the acceptance assertion is
 * the operator's UI path: select the snapshot, open Context, read the table.
 *
 * Requires: dcl-dev backend (:8104) + dcl-frontend (:3004) running.
 */

import { test, expect } from "playwright/test";
import { randomUUID } from "crypto";

const DCL_UI = "http://localhost:3004";
const DCL_DEV = "http://localhost:8104";

// Real Confluent orders.v1 event shape (Farm sim: farm/src/fabric_sims/confluent/sim.py).
const ORDERS = [
  { order_id: "ord-10000", customer_name: "Acme Robotics", amount_usd: 1234.5, currency: "USD", status: "placed", period: "2026-Q2" },
  { order_id: "ord-10001", customer_name: "Globex Logistics", amount_usd: 880.0, currency: "USD", status: "fulfilled", period: "2026-Q2" },
];

test.describe.serial("Orders surface as revenue, not cloud spend (#49)", () => {
  test.setTimeout(120_000);

  test("Context tab shows Revenue=2 and no Cloud Spend row for the orders feed", async ({ page }) => {
    const consoleErrors: string[] = [];
    page.on("console", (msg) => {
      if (msg.type() !== "error") return;
      const t = msg.text();
      if (t.includes("ERR_NAME_NOT_RESOLVED") || t.includes("Failed to load resource")) return;
      if (t.includes("The width(-1)") || t.includes("The height(-1)")) return;
      consoleErrors.push(t);
    });

    const tenant = randomUUID();
    const dclIngestId = randomUUID();
    const entityId = `OrdersRevenueB17-${dclIngestId.slice(0, 4)}`;

    // ── Pipeline delivery (records-path) — the event-bus feed has no operator
    //    UI trigger; this is the precondition, not the acceptance step. ──
    const ingestResp = await page.request.post(`${DCL_DEV}/api/dcl/ingest-records`, {
      data: {
        tenant_id: tenant, run_id: dclIngestId, entity_id: entityId, run_mode: "Dev",
        pipes: [{
          pipe_id: randomUUID(), source_system: "confluent", fabric_plane: "event_bus",
          fabric_product: "confluent", domain: null, identity_key: null,
          record_key_field: "order_id", records: ORDERS,
        }],
      },
    });
    expect(ingestResp.status(), `ingest-records HTTP ${ingestResp.status()}`).toBe(201);

    // ── Ground truth (read-only): the domains that actually landed for this
    //    entity. Orders are a revenue feed — revenue must be present with 2
    //    triples and cloud_spend must be absent. Read from the source of record,
    //    not hardcoded. ──
    const gtResp = await page.request.get(`${DCL_DEV}/api/dcl/contextualization-summary?entity_id=${entityId}`);
    expect(gtResp.status()).toBe(200);
    const gt = await gtResp.json();
    const gtDomains: Record<string, number> = Object.fromEntries(
      (gt.domain_coverage?.domains ?? []).map((d: any) => [d.domain, d.triple_count]),
    );
    expect(gtDomains["revenue"], `ground truth: revenue triples for ${entityId}`).toBe(2);
    expect(gtDomains["cloud_spend"], "ground truth: no cloud_spend for an orders feed").toBeUndefined();

    // ── Operator path: open the DCL UI, pick this snapshot, open Context. ──
    await page.goto(DCL_UI, { waitUntil: "load" });
    const runButton = page.locator('button[data-role="run-primary"]');
    await expect(runButton).toBeVisible({ timeout: 15_000 });
    await expect(runButton).not.toHaveText("Running...", { timeout: 60_000 });

    const snapshotSelect = page.locator("#snapshot-selector");
    await expect(snapshotSelect).toBeVisible({ timeout: 15_000 });
    // The just-delivered snapshot must be selectable by its dcl_ingest_id.
    await expect(async () => {
      const values = await snapshotSelect.locator("option").evaluateAll(
        (opts) => opts.map((o) => (o as HTMLOptionElement).value),
      );
      expect(values).toContain(dclIngestId);
    }).toPass({ timeout: 30_000 });
    await snapshotSelect.selectOption(dclIngestId);

    const contextTab = page.locator("button").filter({ hasText: /^Context$/ });
    await expect(contextTab.first()).toBeVisible({ timeout: 10_000 });
    await contextTab.first().click();

    // Domain Coverage table renders. Find the row whose first cell is the domain.
    const domainTable = page.locator("table").filter({ has: page.getByText("Domain", { exact: true }) }).first();
    await expect(domainTable).toBeVisible({ timeout: 15_000 });

    // Positive: a "Revenue" row with Triples = 2 (formatDomain('revenue') = 'Revenue').
    const revenueRow = page.locator("tr").filter({ has: page.getByRole("cell", { name: "Revenue", exact: true }) });
    await expect(revenueRow.first()).toBeVisible({ timeout: 15_000 });
    await expect(revenueRow.first().getByRole("cell", { name: "2", exact: true }).first()).toBeVisible();

    // Paired negative: NO "Cloud Spend" row — the order amount never classified
    // as cloud spend (the #49 bug cannot return).
    const cloudSpendRow = page.locator("tr").filter({ has: page.getByRole("cell", { name: "Cloud Spend", exact: true }) });
    await expect(cloudSpendRow).toHaveCount(0);

    // Provenance: the orders source surfaces as confluent.
    await expect(page.getByText("confluent", { exact: false }).first()).toBeVisible({ timeout: 10_000 });

    await page.screenshot({ path: "tests/e2e/artifacts/orders_revenue_context.png", fullPage: true });

    expect(consoleErrors, `console errors: ${consoleErrors.join("; ")}`).toHaveLength(0);
  });
});
