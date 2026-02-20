import { afterEach, describe, expect, test } from "bun:test";
import { initDatabase } from "@toss/core";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createStudioApp } from "../../src/server/app";

const tempDirs: string[] = [];

afterEach(() => {
  for (const dir of tempDirs.splice(0)) {
    rmSync(dir, { recursive: true, force: true });
  }
});

function createTempPath(prefix: string): string {
  const dir = mkdtempSync(join(tmpdir(), prefix));
  tempDirs.push(dir);
  return join(dir, "toss.db");
}

describe("studio api error mapping", () => {
  test("returns NOT_INITIALIZED as 400", async () => {
    const dbPath = createTempPath("studio-api-not-initialized-");
    const app = createStudioApp({ dbPath });
    const response = await app.request("/api/status");
    const body = await response.text();

    expect(response.status).toBe(400);
    expect(body).toContain('"error":"NOT_INITIALIZED"');
  });

  test("returns NOT_FOUND as 404 for missing table", async () => {
    const dbPath = createTempPath("studio-api-not-found-");
    await initDatabase({ dbPath, generateSkills: false });

    const app = createStudioApp({ dbPath });
    const response = await app.request("/api/tables/missing");
    const body = await response.text();

    expect(response.status).toBe(404);
    expect(body).toContain('"error":"NOT_FOUND"');
  });

  test("returns INVALID_OPERATION as 400 for malformed filter", async () => {
    const app = createStudioApp();
    const response = await app.request("/api/tables/anything?filter=%7Binvalid");
    const body = await response.text();

    expect(response.status).toBe(400);
    expect(body).toContain('"error":"INVALID_OPERATION"');
  });
});
