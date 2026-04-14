/**
 * Store rebuild acceptance gate — B17 accountability for migrations 014/015/016
 * and the Phase 5 read-path migration (plan drifting-nibbling-graham).
 *
 * Proves:
 *  - /api/dcl/snapshots returns 200 in <500ms (Task 10 curl budget).
 *  - Every tenant_runs entry is served to the UI via /api/dcl/entities.
 *  - The three tabs (Ingest, Context, Dashboard) return identical counts per
 *    entity — this is the post-rebuild invariant, mirrored into the UI.
 *  - Each tab renders numeric content (not skeleton) for the live entities.
 *  - Read-path migration: every Farm-pushed entity is visible in the
 *    SnapshotPanel dropdown, /api/dcl/query serves it with source="ingest",
 *    and /api/dcl/semantic-export.ingest_summary is populated from
 *    current_triples (not the legacy IngestStore). Regression gate for the
 *    "ApexEdge invisible" bug: any entity that lands in tenant_runs MUST
 *    surface through all three read surfaces.
 *
 * Entities are resolved at runtime from the live backend — no hardcoded IDs.
 */

import { test, expect, APIRequestContext } from "playwright/test";

const DCL_URL = "http://localhost:3004";
const DCL_BACKEND = "http://localhost:8004";
const SNAPSHOT_BUDGET_MS = 500;
const PRIMARY_TENANT_ID = "69688df3-fc8e-51f8-a77c-9c13f9b3a784";
const TENANT_RUNS_CAP = 10;

type EntityRow = { tenant_id: string; entity_id: string; triple_count: number };

async function fetchEntities(req: APIRequestContext): Promise<EntityRow[]> {
  const res = await req.get(`${DCL_BACKEND}/api/dcl/entities`);
  expect(res.status()).toBe(200);
  const body = await res.json();
  return (body.entities ?? []) as EntityRow[];
}

async function contextCount(
  req: APIRequestContext,
  tenantId: string,
  entityId: string
): Promise<number> {
  const res = await req.get(
    `${DCL_BACKEND}/api/dcl/contextualization-summary?tenant_id=${tenantId}&entity_id=${entityId}`
  );
  expect(res.status()).toBe(200);
  const body = await res.json();
  const domains = body.domain_coverage?.domains ?? [];
  return domains.reduce((acc: number, d: any) => acc + (d.triple_count ?? 0), 0);
}

async function dashboardCount(
  req: APIRequestContext,
  tenantId: string,
  entityId: string
): Promise<number> {
  const res = await req.get(
    `${DCL_BACKEND}/api/dcl/dashboard-data?tenant_id=${tenantId}&entity_id=${entityId}&page=1&page_size=1`
  );
  expect(res.status()).toBe(200);
  const body = await res.json();
  return body.total_count ?? body.total ?? 0;
}

test.describe.serial("Store rebuild — acceptance gate", () => {
  test.setTimeout(120_000);

  test("/api/dcl/snapshots responds in under 500ms", async ({ request }) => {
    const t0 = Date.now();
    const res = await request.get(
      `${DCL_BACKEND}/api/dcl/snapshots?tenant_id=${PRIMARY_TENANT_ID}`
    );
    const elapsed = Date.now() - t0;
    expect(res.status()).toBe(200);
    expect(
      elapsed,
      `Snapshot latency ${elapsed}ms exceeds ${SNAPSHOT_BUDGET_MS}ms budget (B18)`
    ).toBeLessThan(SNAPSHOT_BUDGET_MS);

    const body = await res.json();
    const snapshots = body.snapshots ?? body.entities ?? body ?? [];
    expect(
      Array.isArray(snapshots) ? snapshots.length : 0,
      "Snapshot list is empty"
    ).toBeGreaterThan(0);
  });

  test("/api/dcl/entities lists every tenant_runs entity with a triple_count and tenant_id", async ({ request }) => {
    const entities = await fetchEntities(request);
    expect(entities.length, "No entities returned from /api/dcl/entities").toBeGreaterThan(0);
    for (const e of entities) {
      expect(
        e.triple_count,
        `Entity ${e.entity_id} has no triple_count`
      ).toBeGreaterThan(0);
      expect(
        e.tenant_id,
        `Entity ${e.entity_id} is missing tenant_id (I2 violation)`
      ).toBeTruthy();
    }
  });

  test("tenant_runs cap: each tenant has at most 10 entities (LIFO by updated_at)", async ({ request }) => {
    const entities = await fetchEntities(request);
    const byTenant = new Map<string, number>();
    for (const e of entities) {
      byTenant.set(e.tenant_id, (byTenant.get(e.tenant_id) ?? 0) + 1);
    }
    const overCap: string[] = [];
    for (const [tenant, count] of byTenant) {
      if (count > TENANT_RUNS_CAP) {
        overCap.push(`${tenant}: ${count} > ${TENANT_RUNS_CAP}`);
      }
    }
    expect(
      overCap,
      `Tenants exceeding per-tenant cap of ${TENANT_RUNS_CAP}:\n  ${overCap.join("\n  ")}`
    ).toHaveLength(0);
  });

  test("count invariant: Ingest == Context == Dashboard per (tenant, entity)", async ({ request }) => {
    const entities = await fetchEntities(request);
    const mismatches: string[] = [];
    for (const e of entities) {
      const ctx = await contextCount(request, e.tenant_id, e.entity_id);
      const dash = await dashboardCount(request, e.tenant_id, e.entity_id);
      const ingest = e.triple_count;
      if (!(ingest === ctx && ctx === dash)) {
        mismatches.push(
          `${e.tenant_id}/${e.entity_id}: Ingest=${ingest} Context=${ctx} Dashboard=${dash}`
        );
      }
    }
    expect(
      mismatches,
      `Count drift across tabs — post-rebuild invariant broken:\n  ${mismatches.join("\n  ")}`
    ).toHaveLength(0);
  });

  test("Context tab renders numeric domain coverage for a live entity", async ({ page, request }) => {
    const entities = await fetchEntities(request);
    const target = entities[0];

    await page.goto(DCL_URL, { waitUntil: "load" });
    const runButton = page.locator('button[data-role="run-primary"]');
    await expect(runButton).toBeVisible({ timeout: 15_000 });
    await expect(runButton).not.toHaveText("Running...", { timeout: 60_000 });

    await page.locator("button").filter({ hasText: "Context" }).click();
    await page.waitForLoadState("networkidle", { timeout: 10_000 });

    const bodyText = (await page.locator("body").textContent()) ?? "";
    const hasDigits = /\d{2,}/.test(bodyText);
    expect(
      hasDigits,
      "Context tab body has no numeric content — skeleton/placeholder still showing"
    ).toBe(true);

    await page.screenshot({
      path: "tests/e2e/artifacts/store_rebuild_context.png",
      fullPage: true,
    });
  });

  test("Dashboard tab renders numeric triple rows for a live entity", async ({ page, request }) => {
    const entities = await fetchEntities(request);
    const target = entities[0];

    await page.goto(DCL_URL, { waitUntil: "load" });
    const runButton = page.locator('button[data-role="run-primary"]');
    await expect(runButton).toBeVisible({ timeout: 15_000 });
    await expect(runButton).not.toHaveText("Running...", { timeout: 60_000 });

    await page.locator("button").filter({ hasText: "Dashboard" }).click();
    await page.waitForLoadState("networkidle", { timeout: 10_000 });

    const bodyText = (await page.locator("body").textContent()) ?? "";
    expect(
      bodyText.length,
      "Dashboard tab body is empty"
    ).toBeGreaterThan(200);
    expect(
      /\d{2,}/.test(bodyText),
      "Dashboard tab has no numeric content"
    ).toBe(true);

    await page.screenshot({
      path: "tests/e2e/artifacts/store_rebuild_dashboard.png",
      fullPage: true,
    });
  });

  test("Ingest tab renders the entity list with triple counts", async ({ page }) => {
    await page.goto(DCL_URL, { waitUntil: "load" });
    const runButton = page.locator('button[data-role="run-primary"]');
    await expect(runButton).toBeVisible({ timeout: 15_000 });
    await expect(runButton).not.toHaveText("Running...", { timeout: 60_000 });

    await page.locator("button").filter({ hasText: "Ingest" }).click();
    await page.waitForLoadState("networkidle", { timeout: 10_000 });

    const bodyText = (await page.locator("body").textContent()) ?? "";
    expect(
      /\d{2,}/.test(bodyText),
      "Ingest tab has no numeric triple count"
    ).toBe(true);

    await page.screenshot({
      path: "tests/e2e/artifacts/store_rebuild_ingest.png",
      fullPage: true,
    });
  });

  // ───────────────────────────────────────────────────────────────────────
  // Phase 5 read-path migration gate (B11/B17).
  // If any of these regress, the "ApexEdge invisible" bug is back.
  // ───────────────────────────────────────────────────────────────────────

  test("/api/dcl/snapshots exposes enrichment fields from current_triples (not legacy IngestStore)", async ({ request }) => {
    const res = await request.get(
      `${DCL_BACKEND}/api/dcl/snapshots?tenant_id=${PRIMARY_TENANT_ID}`
    );
    expect(res.status()).toBe(200);
    const body = await res.json();
    const snapshots = (body.snapshots ?? []) as Array<Record<string, unknown>>;
    expect(snapshots.length, "No snapshots returned").toBeGreaterThan(0);

    for (const s of snapshots) {
      expect(s.entity_id, "snapshot row missing entity_id").toBeTruthy();
      expect(s.dcl_ingest_id, `${s.entity_id}: missing dcl_ingest_id (I1)`).toBeTruthy();
      expect(
        Array.isArray(s.source_systems),
        `${s.entity_id}: source_systems is not an array — handler regressed to legacy IngestStore`
      ).toBe(true);
      expect(
        Array.isArray(s.fabric_plane_vendors),
        `${s.entity_id}: fabric_plane_vendors missing`
      ).toBe(true);
      expect(
        Array.isArray(s.pipe_source_names),
        `${s.entity_id}: pipe_source_names missing`
      ).toBe(true);
      expect(
        (s.source_systems as string[]).length,
        `${s.entity_id}: source_systems is empty — current_triples aggregation failed`
      ).toBeGreaterThan(0);
      expect(
        s.total_rows,
        `${s.entity_id}: total_rows is zero`
      ).toBeTruthy();
    }
  });

  test("/api/dcl/query serves every live entity with source=ingest from current_triples", async ({ request }) => {
    const entities = await fetchEntities(request);
    const misses: string[] = [];
    for (const e of entities) {
      const res = await request.post(`${DCL_BACKEND}/api/dcl/query`, {
        data: {
          metric: "revenue",
          entity_id: e.entity_id,
          tenant_id: e.tenant_id,
        },
      });
      if (res.status() !== 200) {
        misses.push(`${e.entity_id}: HTTP ${res.status()}`);
        continue;
      }
      const body = await res.json();
      const source = body?.metadata?.source;
      const recordCount = body?.metadata?.record_count ?? 0;
      if (source !== "ingest") {
        misses.push(`${e.entity_id}: source=${source} (expected 'ingest')`);
      } else if (recordCount <= 0) {
        misses.push(`${e.entity_id}: record_count=${recordCount}`);
      }
    }
    expect(
      misses,
      `Entities not served by /api/dcl/query from current_triples:\n  ${misses.join("\n  ")}`
    ).toHaveLength(0);
  });

  test("/api/dcl/semantic-export.ingest_summary is populated from current_triples", async ({ request }) => {
    const res = await request.get(
      `${DCL_BACKEND}/api/dcl/semantic-export?tenant_id=${PRIMARY_TENANT_ID}`
    );
    expect(res.status()).toBe(200);
    const body = await res.json();
    expect(
      body.ingest_summary,
      "semantic-export.ingest_summary is null — handler regressed to silent-fallback (A1 violation)"
    ).toBeTruthy();
    expect(
      typeof body.ingest_summary,
      "ingest_summary must be an object"
    ).toBe("object");
  });

  test("Entity selector dropdown lists every live entity from tenant_runs", async ({ page, request }) => {
    // The RunSelector <select> in the top bar is the live entity picker the
    // user sees — it drives every tab (Ingest/Context/Dashboard). If an
    // entity landed in tenant_runs but is missing from this dropdown, the
    // "ApexEdge invisible" bug is back.
    const entities = await fetchEntities(request);
    expect(
      entities.length,
      "No live entities in /api/dcl/entities"
    ).toBeGreaterThan(0);

    await page.goto(DCL_URL, { waitUntil: "load" });
    const runButton = page.locator('button[data-role="run-primary"]');
    await expect(runButton).toBeVisible({ timeout: 15_000 });
    await expect(runButton).not.toHaveText("Running...", { timeout: 60_000 });

    // The entity select lives next to the "Entity:" label in RunSelector.
    const entitySelect = page
      .locator("label, span")
      .filter({ hasText: /^Entity:$/ })
      .locator("xpath=following-sibling::select[1]")
      .first();
    await expect(entitySelect).toBeVisible({ timeout: 15_000 });

    // Read every <option> value. Skip the "All Entities" sentinel.
    const optionValues = await entitySelect.locator("option").evaluateAll(
      (opts) =>
        (opts as HTMLOptionElement[])
          .map((o) => o.value)
          .filter((v) => v.length > 0)
    );
    const optionSet = new Set(optionValues);
    const missing = entities
      .map((e) => e.entity_id)
      .filter((id) => !optionSet.has(id));
    expect(
      missing,
      `Entity selector is missing live entities: ${missing.join(", ")}`
    ).toHaveLength(0);

    await page.screenshot({
      path: "tests/e2e/artifacts/store_rebuild_entity_dropdown.png",
      fullPage: true,
    });
  });
});
