import { afterEach, describe, expect, test } from "bun:test";
import { initDb, openDb } from "@lore/core";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createStudioApp, isAssetRequestPath, setStudioAssetPathsForTest } from "../../src/server/app";

describe("studio app asset routing", () => {
  afterEach(() => {
    setStudioAssetPathsForTest(undefined);
  });

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

  test("root path returns 500 when assets are not embedded", async () => {
    const dir = mkdtempSync(join(tmpdir(), "studio-app-test-"));
    const dbPath = join(dir, "lore.db");
    await initDb({ dbPath });
    const db = openDb(dbPath);
    const app = createStudioApp(db);
    const response = await app.request("/");
    db.$client.close(false);
    rmSync(dir, { recursive: true, force: true });

    expect(response.status).toBe(500);
    expect(await response.text()).toContain("Studio assets are not available");
  });

  test("serves embedded file assets and returns 304 for matching etag", async () => {
    const dir = mkdtempSync(join(tmpdir(), "studio-app-test-"));
    const dbPath = join(dir, "lore.db");
    const indexPath = join(dir, "index.html");
    writeFileSync(indexPath, "<!doctype html><title>lore studio</title>", "utf8");
    setStudioAssetPathsForTest(new Map([["/", indexPath]]));
    await initDb({ dbPath });
    const db = openDb(dbPath);
    const app = createStudioApp(db);
    const first = await app.request("/");
    const etag = first.headers.get("etag");
    if (!etag) {
      throw new Error("expected etag header");
    }
    const second = await app.request("/", {
      headers: {
        "if-none-match": etag,
      },
    });
    const firstBody = await first.text();
    db.$client.close(false);
    rmSync(dir, { recursive: true, force: true });

    expect(first.status).toBe(200);
    expect(firstBody).toContain("<title>lore studio</title>");
    expect(second.status).toBe(304);
  });
});
