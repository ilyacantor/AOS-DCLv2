import { defineConfig } from "playwright/test";

export default defineConfig({
  testDir: "./tests/e2e",
  testMatch: "**/*.spec.ts",
  timeout: 30_000,
  retries: 1,
  // Integration specs share one live DCL database; parallel state-mutating
  // refreshes race (see ingest_refresh_pull + ingest_refresh_atomic_count).
  // Serialize the whole suite rather than invent file-level locks.
  workers: 1,
  use: {
    baseURL: "http://localhost:3004",
    trace: "on-first-retry",
    screenshot: "only-on-failure",
    video: "retain-on-failure",
  },
  reporter: [["list"]],
});
