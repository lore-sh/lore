import { afterEach, describe, expect, test } from "bun:test";
import { applyPlan, configureDatabase, initDatabase } from "@toss/core";
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

async function withDbPath<T>(dbPath: string, run: () => Promise<T>): Promise<T> {
  configureDatabase(dbPath);
  return await run();
}

describe("studio history and revert routes", () => {
  test("GET /api/history supports kind/table/page", async () => {
    const dbPath = createTempPath("studio-history-route-");
    await withDbPath(dbPath, async () => {
      await initDatabase({ dbPath });
      const dir = dirname(dbPath);

      await applyPlan(
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
        await writePlan(dir, "insert-expenses.json", {
          message: "insert expenses",
          operations: [{ type: "insert", table: "expenses", values: { id: 1 } }],
        }),
      );
      await applyPlan(
        await writePlan(dir, "insert-calendar.json", {
          message: "insert calendar",
          operations: [{ type: "insert", table: "calendar", values: { id: 1 } }],
        }),
      );

      const app = createStudioApp();
      const response = await app.request("/api/history?kind=apply&table=expenses&limit=1&page=2");
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
    await withDbPath(dbPath, async () => {
      await initDatabase({ dbPath });
      const dir = dirname(dbPath);

      await applyPlan(
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
        await writePlan(dir, "insert-expenses.json", {
          message: "insert expenses",
          operations: [{ type: "insert", table: "expenses", values: { id: 1 } }],
        }),
      );
      await applyPlan(
        await writePlan(dir, "insert-expenses-2.json", {
          message: "insert expenses 2",
          operations: [{ type: "insert", table: "expenses", values: { id: 2 } }],
        }),
      );

      const app = createStudioApp();
      const response = await app.request("/api/tables/expenses/history?limit=2");
      expect(response.status).toBe(200);
      const payload = await response.json();
      expect(payload).toHaveLength(2);
      expect(payload.map((entry: { message: string }) => entry.message)).toEqual(["insert expenses 2", "insert expenses"]);
    });
  });

  test("POST /api/revert/:id returns success and conflict", async () => {
    const dbPath = createTempPath("studio-revert-route-");
    await withDbPath(dbPath, async () => {
      await initDatabase({ dbPath });
      const dir = dirname(dbPath);

      await applyPlan(
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
        await writePlan(dir, "insert-expenses.json", {
          message: "insert expenses",
          operations: [{ type: "insert", table: "expenses", values: { id: 1, amount: 100 } }],
        }),
      );
      const update = await applyPlan(
        await writePlan(dir, "update-expenses.json", {
          message: "update expenses",
          operations: [{ type: "update", table: "expenses", where: { id: 1 }, values: { amount: 200 } }],
        }),
      );

      const app = createStudioApp();

      const conflictResponse = await app.request(`/api/revert/${insert.commitId}`, { method: "POST" });
      expect(conflictResponse.status).toBe(200);
      const conflictBody = await conflictResponse.json();
      expect(conflictBody.ok).toBe(false);
      expect(conflictBody.conflicts.length).toBeGreaterThan(0);

      const okResponse = await app.request(`/api/revert/${update.commitId}`, { method: "POST" });
      expect(okResponse.status).toBe(200);
      const okBody = await okResponse.json();
      expect(okBody.ok).toBe(true);
    });
  });
});
