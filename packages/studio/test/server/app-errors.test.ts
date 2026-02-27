import { afterEach, describe, expect, test } from "bun:test";
import { apply, initDb, openDb, parsePlan, revert, type Database } from "@lore/core";
import { schemaHash } from "@lore/core/src/inspect";
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
  return join(dir, "lore.db");
}

async function writeJson(path: string, value: unknown): Promise<void> {
  await Bun.write(path, JSON.stringify(value, null, 2));
}

async function withInitializedDbPath<T>(dbPath: string, run: (db: Database) => Promise<T>): Promise<T> {
  await initDb({ dbPath });
  const db = openDb(dbPath);
  try {
    return await run(db);
  } finally {
    try {
      db.$client.close(false);
    } catch {
      // ignore duplicate close in tests
    }
  }
}

async function applyPlan(db: Database, planRef: string) {
  const payload = await Bun.file(planRef).text();
  const parsed = JSON.parse(payload) as Record<string, unknown>;
  if (!Object.hasOwn(parsed, "baseSchemaHash")) {
    parsed.baseSchemaHash = schemaHash(db);
  }
  return apply(db, parsePlan(JSON.stringify(parsed)));
}

describe("studio api error mapping", () => {
  test("returns INTERNAL when database is unexpectedly closed", async () => {
    const dbPath = createTempPath("studio-api-not-initialized-");
    const { status, body } = await withInitializedDbPath(dbPath, async (db) => {
      const app = createStudioApp(db);
      db.$client.close(false);
      const response = await app.request("/api/status");
      return { status: response.status, body: await response.text() };
    });

    expect(status).toBe(500);
    expect(body).toContain('"code":"INTERNAL"');
  });

  test("returns NOT_FOUND as 404 for coded table lookup errors", async () => {
    const dbPath = createTempPath("studio-api-not-found-");
    const { status, body, contentType } = await withInitializedDbPath(dbPath, async (db) => {
      const app = createStudioApp(db);
      const response = await app.request("/api/tables/missing/history?limit=10&page=1");
      return {
        status: response.status,
        body: await response.text(),
        contentType: response.headers.get("content-type"),
      };
    });

    expect(status).toBe(404);
    expect(contentType).toContain("application/problem+json");
    expect(body).toContain('"code":"NOT_FOUND"');
    expect(body).toContain('"status":404');
    expect(body).toContain('"title":"NOT_FOUND"');
    expect(body).toContain('"instance":"/api/tables/missing/history"');
  });

  test("removes legacy /api/schema endpoint", async () => {
    const dbPath = createTempPath("studio-api-no-schema-");
    const response = await withInitializedDbPath(dbPath, async (db) => {
      const app = createStudioApp(db);
      return await app.request("/api/schema");
    });

    expect(response.status).toBe(404);
    expect(await response.text()).toContain('"code":"NOT_FOUND"');
  });

  test("returns ALREADY_REVERTED as 409", async () => {
    const dbPath = createTempPath("studio-api-already-reverted-");
    const { status, body } = await withInitializedDbPath(dbPath, async (db) => {
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
      const commit = await applyPlan(db, planPath);
      const first = revert(db, commit.commitId);
      expect(first.ok).toBe(true);

      const app = createStudioApp(db);
      const response = await app.request(`/api/commits/${commit.commitId}/revert`, {
        method: "POST",
      });
      return { status: response.status, body: await response.text() };
    });

    expect(status).toBe(409);
    expect(body).toContain('"code":"ALREADY_REVERTED"');
  });
});
