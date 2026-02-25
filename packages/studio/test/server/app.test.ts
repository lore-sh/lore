import { describe, expect, test } from "bun:test";
import { initDb, openDb } from "@lore/core";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createStudioApp, isAssetRequestPath } from "../../src/server/app";

describe("studio app asset routing", () => {
  test("isAssetRequestPath matches vite asset URLs", () => {
    expect(isAssetRequestPath("/assets/index-abc123.js")).toBe(true);
    expect(isAssetRequestPath("/assets/styles-abc123.css")).toBe(true);
    expect(isAssetRequestPath("/tables/expenses")).toBe(false);
    expect(isAssetRequestPath("/schema")).toBe(false);
  });

  test("missing asset path returns 404 instead of index fallback", async () => {
    const dir = mkdtempSync(join(tmpdir(), "studio-app-test-"));
    const dbPath = join(dir, "lore.db");
    await initDb({ dbPath });
    const db = openDb(dbPath);
    const app = createStudioApp(db);
    const response = await app.request("/assets/__missing_studio_asset__.js");
    db.$client.close(false);
    rmSync(dir, { recursive: true, force: true });

    expect(response.status).toBe(404);
    expect(await response.text()).toContain("Asset not found");
  });
});
