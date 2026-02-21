import { afterEach, describe, expect, test } from "bun:test";
import { configureDatabase, initDatabase } from "@toss/core";
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

async function withDbPath<T>(dbPath: string, run: () => Promise<T>): Promise<T> {
  configureDatabase(dbPath);
  return await run();
}

describe("studio api error mapping", () => {
  test("returns NOT_INITIALIZED as 400", async () => {
    const dbPath = createTempPath("studio-api-not-initialized-");
    const { status, body } = await withDbPath(dbPath, async () => {
      const app = createStudioApp();
      const response = await app.request("/api/status");
      return { status: response.status, body: await response.text() };
    });

    expect(status).toBe(400);
    expect(body).toContain('"error":"NOT_INITIALIZED"');
  });

  test("returns NOT_FOUND as 404 for missing table", async () => {
    const dbPath = createTempPath("studio-api-not-found-");
    const { status, body } = await withDbPath(dbPath, async () => {
      await initDatabase({ dbPath, generateSkills: false });
      const app = createStudioApp();
      const response = await app.request("/api/tables/missing");
      return { status: response.status, body: await response.text() };
    });

    expect(status).toBe(404);
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
