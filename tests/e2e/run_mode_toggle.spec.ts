/**
 * Operator-visible outcome: on the DCL graph view (http://localhost:3004), the
 * operator clicks the "Prod" toggle in the top-right Mode group, clicks the
 * "Run" button (data-role="run-primary"), and within 60 seconds sees the
 * post-run metrics span show a non-zero "X LLM" and "X RAG" badge plus a
 * processing time of at least 1.5 seconds — the latency signature of an
 * LLM-touched run. The Sankey graph re-renders. No console errors.
 *
 * If the backend is missing OPENAI_API_KEY or PINECONE_API_KEY, the run
 * returns 503 and a readable Pipeline Error toast names the missing var.
 *
 * Drives the Dev/Prod toggle on POST /api/dcl/run (mode=Farm) end-to-end via
 * real UI clicks — no fetch/page.request calls from the test runner.
 *
 * Pre-reqs:
 *   - DCL backend on :8004 with seed data and OPENAI_API_KEY/PINECONE_API_KEY
 *     loaded from .env.
 *   - DCL frontend on :3004.
 */

import { test, expect } from "playwright/test";

const DCL_URL = "http://localhost:3004";

test.describe.serial("Run-mode toggle drives Prod path", () => {
  test.setTimeout(120_000);

  test("operator selects Prod, clicks Run, and graph or readable error renders", async ({ page }) => {
    const consoleErrors: string[] = [];
    page.on("console", (msg) => {
      if (msg.type() !== "error") return;
      const t = msg.text();
      if (t.includes("Failed to fetch") || t.includes("net::ERR_") || t.includes("The width(-1)") || t.includes("The height(-1)")) return;
      if (t.includes("Failed to load resource")) return;
      consoleErrors.push(t);
    });

    await page.goto(DCL_URL, { waitUntil: "networkidle", timeout: 30_000 });

    // Toggle to Prod via real click — no programmatic state mutation.
    const prodToggle = page.locator('button:has-text("Prod")').first();
    await expect(prodToggle).toBeVisible({ timeout: 15_000 });
    await prodToggle.click();
    // Active toggle picks up the bg-primary class — proves state actually flipped.
    await expect(prodToggle).toHaveClass(/bg-primary/, { timeout: 5_000 });

    // Capture the /api/dcl/run response in flight — we assert on its status,
    // then on the durable post-run UI state. Toasts auto-dismiss before any
    // post-run assertion can read them, so they are not the gate.
    const runResponsePromise = page.waitForResponse(
      (resp) =>
        resp.url().includes("/api/dcl/run") && resp.request().method() === "POST",
      { timeout: 90_000 },
    );

    const runBtn = page.locator('[data-role="run-primary"]');
    await expect(runBtn).toBeVisible();
    await expect(runBtn).toHaveText(/^Run$/, { timeout: 5_000 });
    await runBtn.click();
    await expect(runBtn).toHaveText(/Running/, { timeout: 5_000 });

    const runResponse = await runResponsePromise;
    const status = runResponse.status();

    // Wait for the button to settle back to "Run" — proves the in-flight
    // state machine completed cleanly.
    await expect(runBtn).toHaveText(/^Run$/, { timeout: 30_000 });

    if (status === 503) {
      // Prod with missing keys: error toast renders. Destructive variant is
      // sticky long enough to assert. Body must name the missing env var so
      // the operator can fix the config without reading server logs.
      const errorTitle = page.locator('text=/Pipeline Error/i');
      await expect(errorTitle).toBeVisible({ timeout: 5_000 });
      const errorBody = await errorTitle.locator("..").innerText();
      expect(
        errorBody,
        "503 toast must name OPENAI_API_KEY or PINECONE_API_KEY for the operator",
      ).toMatch(/OPENAI_API_KEY|PINECONE_API_KEY/);
    } else {
      expect(status, `Unexpected run status ${status}; expected 200 or 503`).toBe(200);

      // Pipeline Complete in Prod: post-run metrics span MUST surface AI
      // activity. These badges only render when llmCalls/ragWrites > 0;
      // their presence is the gate that AI actually fired in Farm-mode read.
      const metricsSpan = page.locator('[data-role="run-metrics"]');
      await expect(metricsSpan).toBeVisible({ timeout: 5_000 });

      const llmBadge = page.locator('[data-role="llm-calls"]');
      await expect(
        llmBadge,
        "LLM badge must render in Prod — proves _apply_prod_mode_ai fired in Farm read path",
      ).toBeVisible({ timeout: 5_000 });
      const llmText = await llmBadge.innerText();
      const llmMatch = llmText.match(/(\d+)\s+LLM/);
      expect(llmMatch, `LLM badge text malformed: ${llmText}`).not.toBeNull();
      expect(
        Number(llmMatch![1]),
        "llmCalls must be > 0 — Prod read fires the validator + RAG embedding calls",
      ).toBeGreaterThan(0);

      const ragBadge = page.locator('[data-role="rag-writes"]');
      await expect(
        ragBadge,
        "RAG badge must render in Prod — proves Pinecone lessons were written",
      ).toBeVisible({ timeout: 5_000 });
      const ragText = await ragBadge.innerText();
      const ragMatch = ragText.match(/(\d+)\s+RAG/);
      expect(ragMatch, `RAG badge text malformed: ${ragText}`).not.toBeNull();
      expect(
        Number(ragMatch![1]),
        "ragWrites must be > 0 — Prod read stores Pinecone lessons",
      ).toBeGreaterThan(0);

      // Latency signature: LLM-touched runs are seconds, not milliseconds.
      const elapsedText = await metricsSpan.innerText();
      const elapsedMatch = elapsedText.match(/^(\d+\.\d+)s/);
      expect(elapsedMatch, `Elapsed text malformed: ${elapsedText}`).not.toBeNull();
      expect(
        Number(elapsedMatch![1]),
        "Prod read must take >= 1.5s (LLM signature). A sub-second run means AI did not fire.",
      ).toBeGreaterThanOrEqual(1.5);
    }

    await page.screenshot({
      path: "tests/e2e/artifacts/run_mode_toggle_after.png",
      fullPage: true,
    });

    expect(consoleErrors, `Unexpected console errors: ${consoleErrors.join("\n")}`).toHaveLength(0);
  });
});
