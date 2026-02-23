import { describe, expect, test } from "bun:test";
import { history, initDb, query } from "../src";
import { applyPlan, createTestContext, planCheck, writePlanFile, withTmpDirCleanup, currentDb } from "./helpers";

const testWithTmp = (name: string, fn: () => void | Promise<void>) => test(name, withTmpDirCleanup(fn));

describe("check", () => {
  testWithTmp("returns ok for valid plan and does not persist changes", async () => {
    const { dir, dbPath } = createTestContext();
    await initDb({ dbPath });

    const createPlan = await writePlanFile(dir, "create-table-plan.json", {
      message: "create todos table",
      operations: [
        {
          type: "create_table",
          table: "todos",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "body", type: "TEXT", notNull: true },
          ],
        },
      ],
    });

    const result = await planCheck(currentDb(), createPlan);
    expect(result.ok).toBe(true);
    expect(result.errors).toEqual([]);
    expect(result.summary.operations).toBe(1);
    expect(result.summary.predicted.schemaEffects).toBeGreaterThan(0);

    const tableRows = query(currentDb(), "SELECT name FROM sqlite_master WHERE type='table' AND name='todos'");
    expect(tableRows).toEqual([]);
    const commits = history(currentDb());
    expect(commits).toHaveLength(0);
  });

  testWithTmp("throws for invalid plan payload", async () => {
    const { dir, dbPath } = createTestContext();
    await initDb({ dbPath });

    const invalidPlan = await writePlanFile(dir, "invalid-plan.json", {
      message: "",
      operations: [],
    });
    await expect(planCheck(currentDb(), invalidPlan)).rejects.toMatchObject({
      code: "INVALID_PLAN",
    });
  });

  testWithTmp("flags destructive operations with high risk and keeps rows unchanged", async () => {
    const { dir, dbPath } = createTestContext();
    await initDb({ dbPath });

    const setup = await writePlanFile(dir, "setup-plan-check.json", {
      message: "setup expenses",
      operations: [
        {
          type: "create_table",
          table: "expenses",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "item", type: "TEXT", notNull: true },
          ],
        },
        {
          type: "insert",
          table: "expenses",
          values: { id: 1, item: "lunch" },
        },
      ],
    });
    await applyPlan(currentDb(), setup);

    const destructive = await writePlanFile(dir, "destructive-plan.json", {
      message: "delete expense",
      operations: [{ type: "delete", table: "expenses", where: { id: 1 } }],
    });
    const result = await planCheck(currentDb(), destructive);
    expect(result.ok).toBe(true);
    expect(result.risk).toBe("high");
    expect(result.warnings.some((warning) => warning.code === "DESTRUCTIVE_OPERATION")).toBe(true);

    const rows = query(currentDb(), "SELECT id, item FROM expenses ORDER BY id");
    expect(rows).toEqual([{ id: 1, item: "lunch" }]);
  });

  testWithTmp("returns runtime error when plan references missing table", async () => {
    const { dir, dbPath } = createTestContext();
    await initDb({ dbPath });

    const invalidRuntime = await writePlanFile(dir, "missing-table-plan.json", {
      message: "insert missing table",
      operations: [{ type: "insert", table: "missing_table", values: { id: 1 } }],
    });
    const result = await planCheck(currentDb(), invalidRuntime);
    expect(result.ok).toBe(false);
    expect(result.errors.length).toBeGreaterThan(0);
  });

  testWithTmp("throws on legacy scalar column default payload", async () => {
    const { dir, dbPath } = createTestContext();
    await initDb({ dbPath });

    const legacyDefault = await writePlanFile(dir, "legacy-default-shape.json", {
      message: "legacy default shape",
      operations: [
        {
          type: "create_table",
          table: "legacy_defaults",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "count", type: "INTEGER", default: 0 },
          ],
        },
      ],
    });
    await expect(planCheck(currentDb(), legacyDefault)).rejects.toMatchObject({
      code: "INVALID_PLAN",
    });
  });

  testWithTmp("accepts SQL default object payload", async () => {
    const { dir, dbPath } = createTestContext();
    await initDb({ dbPath });

    const sqlDefault = await writePlanFile(dir, "sql-default-shape.json", {
      message: "sql default shape",
      operations: [
        {
          type: "create_table",
          table: "events",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            {
              name: "recorded_at",
              type: "TEXT",
              notNull: true,
              default: { kind: "sql", expr: "CURRENT_TIMESTAMP" },
            },
          ],
        },
      ],
    });
    const result = await planCheck(currentDb(), sqlDefault);
    expect(result.ok).toBe(true);
    expect(result.errors).toEqual([]);
  });

  testWithTmp("throws when plan file is missing", async () => {
    const { dir, dbPath } = createTestContext();
    await initDb({ dbPath });

    await expect(planCheck(currentDb(), `${dir}/missing-plan.json`)).rejects.toThrow();
  });
});
