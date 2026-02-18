import { afterEach, describe, expect, test } from "bun:test";
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
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

  test("apply rejects unknown operation types", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const badPlanPath = await writePlanFile(dir, "bad.json", {
      message: "attempt unknown operation",
      operations: [{ type: "rename_table", table: "expenses", to: "costs" }],
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

  test("apply supports schema evolution and data migration operations", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const createPath = await writePlanFile(dir, "create.json", {
      message: "create ledger and legacy tables",
      operations: [
        {
          type: "create_table",
          table: "ledger",
          columns: [
            { name: "item", type: "TEXT", notNull: true },
            { name: "amount", type: "TEXT", notNull: true },
            { name: "note", type: "TEXT" },
            { name: "category", type: "TEXT" },
          ],
        },
        {
          type: "create_table",
          table: "legacy",
          columns: [{ name: "dummy", type: "TEXT" }],
        },
      ],
    });

    const insertPath = await writePlanFile(dir, "insert.json", {
      message: "seed rows",
      operations: [
        { type: "insert", table: "ledger", values: { item: "dinner", amount: "1200", note: "meal", category: null } },
        { type: "insert", table: "ledger", values: { item: "lunch", amount: "850", note: "meal", category: null } },
      ],
    });

    const migrationPath = await writePlanFile(dir, "migration.json", {
      message: "migrate ledger schema and clean old data",
      operations: [
        {
          type: "update",
          table: "ledger",
          values: { category: "food" },
          where: { item: "dinner" },
        },
        {
          type: "delete",
          table: "ledger",
          where: { item: "lunch" },
        },
        {
          type: "alter_column_type",
          table: "ledger",
          column: "amount",
          newType: "INTEGER",
        },
        {
          type: "drop_column",
          table: "ledger",
          column: "note",
        },
        {
          type: "drop_table",
          table: "legacy",
        },
      ],
    });

    await applyPlan(createPath, { dbPath });
    await applyPlan(insertPath, { dbPath });
    await applyPlan(migrationPath, { dbPath });

    const rows = readQuery("SELECT item, amount, category, typeof(amount) AS amount_type FROM ledger", { dbPath });
    expect(rows).toEqual([{ item: "dinner", amount: 1200, category: "food", amount_type: "integer" }]);

    const noteColumn = readQuery("SELECT COUNT(*) AS c FROM pragma_table_info('ledger') WHERE name='note'", { dbPath });
    expect(noteColumn).toEqual([{ c: 0 }]);

    const legacyTable = readQuery("SELECT COUNT(*) AS c FROM sqlite_master WHERE type='table' AND name='legacy'", { dbPath });
    expect(legacyTable).toEqual([{ c: 0 }]);
  });

  test("apply rejects update/delete without where predicates", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const createPath = await writePlanFile(dir, "create.json", {
      message: "create ledger table",
      operations: [
        {
          type: "create_table",
          table: "ledger",
          columns: [
            { name: "item", type: "TEXT", notNull: true },
            { name: "amount", type: "INTEGER", notNull: true },
          ],
        },
      ],
    });
    await applyPlan(createPath, { dbPath });

    const badUpdatePath = await writePlanFile(dir, "bad-update.json", {
      message: "unsafe update",
      operations: [{ type: "update", table: "ledger", values: { amount: 1 }, where: {} }],
    });
    const badDeletePath = await writePlanFile(dir, "bad-delete.json", {
      message: "unsafe delete",
      operations: [{ type: "delete", table: "ledger", where: {} }],
    });

    await expect(applyPlan(badUpdatePath, { dbPath })).rejects.toThrow();
    await expect(applyPlan(badDeletePath, { dbPath })).rejects.toThrow();
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

  test("init can generate skills and AGENTS.md for Claude Code", async () => {
    const { dir, dbPath } = createTestContext();
    const result = await initDatabase({ dbPath, generateSkills: true, workspacePath: dir });

    expect(result.generatedSkills).not.toBeNull();
    if (!result.generatedSkills) {
      throw new Error("generatedSkills should be present");
    }

    expect(existsSync(result.generatedSkills.skillPath)).toBe(true);
    expect(existsSync(join(result.generatedSkills.referencesDir, "context.md"))).toBe(true);
    expect(existsSync(join(result.generatedSkills.referencesDir, "contracts.md"))).toBe(true);
    expect(existsSync(result.generatedSkills.agentsPath)).toBe(true);

    const agents = readFileSync(result.generatedSkills.agentsPath, "utf8");
    const skill = readFileSync(result.generatedSkills.skillPath, "utf8");
    expect(agents.includes("Unified toss workflow")).toBe(true);
    expect(skill.includes("name: toss")).toBe(true);
    expect(skill.includes("Remember Flow (read-before-apply)")).toBe(true);
    expect(skill.includes("Schema cleanup -> include `drop_column` / `drop_table`")).toBe(true);
    expect(skill.includes("Type migration -> include `alter_column_type`")).toBe(true);
    expect(agents.includes("toss:init:skills:start")).toBe(true);
    expect(skill.includes("pragma_table_info")).toBe(true);
    expect(skill.includes("retry once")).toBe(true);
  });
});
