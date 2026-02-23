import { afterEach, describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { apply, initDb, parsePlan } from "@toss/core";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { createStudioApp } from "../../../src/server/app";

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

async function writePlan(dir: string, name: string, payload: unknown): Promise<string> {
  const planPath = join(dir, name);
  await Bun.write(planPath, JSON.stringify(payload, null, 2));
  return planPath;
}

async function withDbPath<T>(dbPath: string, run: (db: Database) => Promise<T>): Promise<T> {
  const db = new Database(dbPath, { strict: true });
  try {
    return await run(db);
  } finally {
    db.close(false);
  }
}

async function applyPlan(db: Database, planRef: string) {
  const payload = await Bun.file(planRef).text();
  return apply(db, parsePlan(payload));
}

describe("studio history and revert routes", () => {
  test("GET /api/commits supports kind/table/page", async () => {
    const dbPath = createTempPath("studio-history-route-");
    await withDbPath(dbPath, async (db) => {
      await initDb({ dbPath });
      const dir = dirname(dbPath);

      await applyPlan(
        db,
        await writePlan(dir, "create-expenses.json", {
          message: "create expenses",
          operations: [
            {
              type: "create_table",
              table: "expenses",
              columns: [{ name: "id", type: "INTEGER", primaryKey: true }],
            },
          ],
        }),
      );
      await applyPlan(
        db,
        await writePlan(dir, "create-calendar.json", {
          message: "create calendar",
          operations: [
            {
              type: "create_table",
              table: "calendar",
              columns: [{ name: "id", type: "INTEGER", primaryKey: true }],
            },
          ],
        }),
      );
      await applyPlan(
        db,
        await writePlan(dir, "insert-expenses.json", {
          message: "insert expenses",
          operations: [{ type: "insert", table: "expenses", values: { id: 1 } }],
        }),
      );
      await applyPlan(
        db,
        await writePlan(dir, "insert-calendar.json", {
          message: "insert calendar",
          operations: [{ type: "insert", table: "calendar", values: { id: 1 } }],
        }),
      );

      const app = createStudioApp(db);
      const response = await app.request("/api/commits?kind=apply&table=expenses&limit=1&page=2");
      expect(response.status).toBe(200);
      const payload = await response.json();
      expect(Array.isArray(payload)).toBe(true);
      expect(payload).toHaveLength(1);
      expect(payload[0]?.message).toBe("create expenses");
      expect(payload[0]?.affectedTables).toEqual(["expenses"]);
    });
  });

  test("GET /api/tables/:name/history applies limit", async () => {
    const dbPath = createTempPath("studio-table-history-route-");
    await withDbPath(dbPath, async (db) => {
      await initDb({ dbPath });
      const dir = dirname(dbPath);

      await applyPlan(
        db,
        await writePlan(dir, "create-expenses.json", {
          message: "create expenses",
          operations: [
            {
              type: "create_table",
              table: "expenses",
              columns: [{ name: "id", type: "INTEGER", primaryKey: true }],
            },
          ],
        }),
      );
      await applyPlan(
        db,
        await writePlan(dir, "insert-expenses.json", {
          message: "insert expenses",
          operations: [{ type: "insert", table: "expenses", values: { id: 1 } }],
        }),
      );
      await applyPlan(
        db,
        await writePlan(dir, "insert-expenses-2.json", {
          message: "insert expenses 2",
          operations: [{ type: "insert", table: "expenses", values: { id: 2 } }],
        }),
      );

      const app = createStudioApp(db);
      const response = await app.request("/api/tables/expenses/history?limit=2");
      expect(response.status).toBe(200);
      const payload = await response.json();
      expect(payload).toHaveLength(2);
      expect(payload.map((entry: { message: string }) => entry.message)).toEqual(["insert expenses 2", "insert expenses"]);
    });
  });

  test("GET /api/tables/:name/history returns NOT_FOUND for unknown table", async () => {
    const dbPath = createTempPath("studio-table-history-missing-route-");
    await withDbPath(dbPath, async (db) => {
      await initDb({ dbPath });
      const app = createStudioApp(db);
      const response = await app.request("/api/tables/missing/history?limit=10&page=1");

      expect(response.status).toBe(404);
      const payload = await response.json();
      expect(payload.code).toBe("NOT_FOUND");
    });
  });

  test("POST /api/commits/:id/revert returns success and conflict", async () => {
    const dbPath = createTempPath("studio-revert-route-");
    await withDbPath(dbPath, async (db) => {
      await initDb({ dbPath });
      const dir = dirname(dbPath);

      await applyPlan(
        db,
        await writePlan(dir, "create-expenses.json", {
          message: "create expenses",
          operations: [
            {
              type: "create_table",
              table: "expenses",
              columns: [
                { name: "id", type: "INTEGER", primaryKey: true },
                { name: "amount", type: "INTEGER", notNull: true },
              ],
            },
          ],
        }),
      );
      const insert = await applyPlan(
        db,
        await writePlan(dir, "insert-expenses.json", {
          message: "insert expenses",
          operations: [{ type: "insert", table: "expenses", values: { id: 1, amount: 100 } }],
        }),
      );
      const update = await applyPlan(
        db,
        await writePlan(dir, "update-expenses.json", {
          message: "update expenses",
          operations: [{ type: "update", table: "expenses", where: { id: 1 }, values: { amount: 200 } }],
        }),
      );

      const app = createStudioApp(db);

      const conflictResponse = await app.request(`/api/commits/${insert.commitId}/revert`, { method: "POST" });
      expect(conflictResponse.status).toBe(409);
      const conflictBody = await conflictResponse.json();
      expect(conflictBody.ok).toBe(false);
      expect(conflictBody.conflicts.length).toBeGreaterThan(0);

      const okResponse = await app.request(`/api/commits/${update.commitId}/revert`, { method: "POST" });
      expect(okResponse.status).toBe(200);
      const okBody = await okResponse.json();
      expect(okBody.ok).toBe(true);
    });
  });
});
