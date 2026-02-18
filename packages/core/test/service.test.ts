import { afterEach, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  applyPlan,
  getHistory,
  getStatus,
  initDatabase,
  isTossError,
  readQuery,
  revertCommit,
} from "../src";

const tmpDirs: string[] = [];

function createTestContext(): { dir: string; dbPath: string } {
  const dir = mkdtempSync(join(tmpdir(), "toss-core-test-"));
  tmpDirs.push(dir);
  const dbPath = join(dir, "toss.db");
  return { dir, dbPath };
}

async function writePlanFile(dir: string, name: string, payload: unknown): Promise<string> {
  const path = join(dir, name);
  writeFileSync(path, JSON.stringify(payload), "utf8");
  return path;
}

afterEach(() => {
  while (tmpDirs.length > 0) {
    const dir = tmpDirs.pop();
    if (dir) {
      rmSync(dir, { recursive: true, force: true });
    }
  }
});

describe("toss core service", () => {
  test("init -> apply -> read -> status -> history works", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const createPlanPath = await writePlanFile(dir, "create.json", {
      message: "create expenses table",
      operations: [
        {
          type: "create_table",
          table: "expenses",
          columns: [
            { name: "date", type: "TEXT", notNull: true },
            { name: "item", type: "TEXT", notNull: true },
            { name: "amount", type: "INTEGER", notNull: true },
          ],
        },
      ],
    });

    const insertPlanPath = await writePlanFile(dir, "insert.json", {
      message: "insert dinner",
      operations: [
        {
          type: "insert",
          table: "expenses",
          values: {
            date: "2026-02-18",
            item: "dinner",
            amount: 1200,
          },
        },
      ],
    });

    await applyPlan(createPlanPath, { dbPath });
    await applyPlan(insertPlanPath, { dbPath });

    const rows = readQuery("SELECT item, amount FROM expenses", { dbPath });
    expect(rows).toEqual([{ item: "dinner", amount: 1200 }]);

    const status = getStatus({ dbPath });
    expect(status.tableCount).toBe(1);
    expect(status.tables).toEqual([{ name: "expenses", count: 1 }]);

    const history = getHistory({ dbPath });
    expect(history).toHaveLength(2);
    expect(history[0]?.kind).toBe("apply");
  });

  test("apply rejects non-additive operation types", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const badPlanPath = await writePlanFile(dir, "bad.json", {
      message: "attempt delete",
      operations: [{ type: "delete", table: "expenses", where: { id: 1 } }],
    });

    try {
      await applyPlan(badPlanPath, { dbPath });
      throw new Error("applyPlan should have failed");
    } catch (error) {
      expect(isTossError(error)).toBe(true);
      if (isTossError(error)) {
        expect(error.code).toBe("INVALID_PLAN");
      }
    }
  });

  test("read rejects write SQL", async () => {
    const { dbPath } = createTestContext();
    await initDatabase({ dbPath });

    expect(() => readQuery("DELETE FROM expenses", { dbPath })).toThrow();
  });

  test("revert insert commit safely rebuilds head", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const createPlanPath = await writePlanFile(dir, "create.json", {
      message: "create expenses table",
      operations: [
        {
          type: "create_table",
          table: "expenses",
          columns: [
            { name: "item", type: "TEXT", notNull: true },
            { name: "amount", type: "INTEGER", notNull: true },
          ],
        },
      ],
    });

    const insertAPath = await writePlanFile(dir, "insert-a.json", {
      message: "insert lunch",
      operations: [{ type: "insert", table: "expenses", values: { item: "lunch", amount: 850 } }],
    });

    const insertBPath = await writePlanFile(dir, "insert-b.json", {
      message: "insert dinner",
      operations: [{ type: "insert", table: "expenses", values: { item: "dinner", amount: 1200 } }],
    });

    await applyPlan(createPlanPath, { dbPath });
    const firstInsertCommit = await applyPlan(insertAPath, { dbPath });
    await applyPlan(insertBPath, { dbPath });

    revertCommit(firstInsertCommit.id, { dbPath });

    const rows = readQuery("SELECT item, amount FROM expenses ORDER BY item", { dbPath });
    expect(rows).toEqual([{ item: "dinner", amount: 1200 }]);
  });

  test("revert create_table is blocked when later commits depend on that table", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const createPlanPath = await writePlanFile(dir, "create.json", {
      message: "create expenses table",
      operations: [
        {
          type: "create_table",
          table: "expenses",
          columns: [
            { name: "item", type: "TEXT", notNull: true },
            { name: "amount", type: "INTEGER", notNull: true },
          ],
        },
      ],
    });

    const insertPath = await writePlanFile(dir, "insert.json", {
      message: "insert dinner",
      operations: [{ type: "insert", table: "expenses", values: { item: "dinner", amount: 1200 } }],
    });

    const createCommit = await applyPlan(createPlanPath, { dbPath });
    await applyPlan(insertPath, { dbPath });

    expect(() => revertCommit(createCommit.id, { dbPath })).toThrow();
  });
});
