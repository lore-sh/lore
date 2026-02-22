import { afterEach, describe, expect, test } from "bun:test";
import { applyPlan, configureDatabase, initDatabase, revertCommit } from "@toss/core";
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

async function writeJson(path: string, value: unknown): Promise<void> {
  await Bun.write(path, JSON.stringify(value, null, 2));
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
    expect(body).toContain('"code":"NOT_INITIALIZED"');
  });

  test("returns NOT_FOUND as 404 for missing table", async () => {
    const dbPath = createTempPath("studio-api-not-found-");
    const { status, body } = await withDbPath(dbPath, async () => {
      await initDatabase({ dbPath });
      const app = createStudioApp();
      const response = await app.request("/api/tables/missing");
      return { status: response.status, body: await response.text() };
    });

    expect(status).toBe(404);
    expect(body).toContain('"code":"NOT_FOUND"');
  });

  test("removes legacy /api/schema endpoint", async () => {
    const app = createStudioApp();
    const response = await app.request("/api/schema");

    expect(response.status).toBe(404);
    expect(await response.text()).toContain('"code":"NOT_FOUND"');
  });

  test("returns ALREADY_REVERTED as 409", async () => {
    const dbPath = createTempPath("studio-api-already-reverted-");
    const { status, body } = await withDbPath(dbPath, async () => {
      await initDatabase({ dbPath });
      const planPath = join(dbPath, "..", "plan.json");
      await writeJson(planPath, {
        message: "create expenses",
        operations: [
          {
            type: "create_table",
            table: "expenses",
            columns: [{ name: "id", type: "INTEGER", primaryKey: true }],
          },
        ],
      });
      const commit = await applyPlan(planPath);
      const first = revertCommit(commit.commitId);
      expect(first.ok).toBe(true);

      const app = createStudioApp();
      const response = await app.request(`/api/commits/${commit.commitId}/revert`, {
        method: "POST",
      });
      return { status: response.status, body: await response.text() };
    });

    expect(status).toBe(409);
    expect(body).toContain('"code":"ALREADY_REVERTED"');
  });
});
