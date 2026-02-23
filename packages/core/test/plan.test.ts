import { describe, expect, test } from "bun:test";
import { applyPlan, getHistory, initDatabase, planCheck, readQuery } from "../src";
import { createTestContext, writePlanFile, withTmpDirCleanup, currentDb } from "./helpers";

const testWithTmp = (name: string, fn: () => void | Promise<void>) => test(name, withTmpDirCleanup(fn));

describe("planCheck", () => {
  testWithTmp("returns ok for valid plan and does not persist changes", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

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

    const tableRows = readQuery(currentDb(), "SELECT name FROM sqlite_master WHERE type='table' AND name='todos'");
    expect(tableRows).toEqual([]);
    const history = getHistory(currentDb(), );
    expect(history).toHaveLength(0);
  });

  testWithTmp("returns errors for invalid plan payload", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const invalidPlan = await writePlanFile(dir, "invalid-plan.json", {
      message: "",
      operations: [],
    });
    const result = await planCheck(currentDb(), invalidPlan);
    expect(result.ok).toBe(false);
    expect(result.errors.length).toBeGreaterThan(0);
    expect(result.risk).toBe("high");
  });

  testWithTmp("flags destructive operations with high risk and keeps rows unchanged", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

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

    const rows = readQuery(currentDb(), "SELECT id, item FROM expenses ORDER BY id");
    expect(rows).toEqual([{ id: 1, item: "lunch" }]);
  });

  testWithTmp("returns runtime error when plan references missing table", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const invalidRuntime = await writePlanFile(dir, "missing-table-plan.json", {
      message: "insert missing table",
      operations: [{ type: "insert", table: "missing_table", values: { id: 1 } }],
    });
    const result = await planCheck(currentDb(), invalidRuntime);
    expect(result.ok).toBe(false);
    expect(result.errors.length).toBeGreaterThan(0);
  });

  testWithTmp("rejects legacy scalar column default payload", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

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
    const result = await planCheck(currentDb(), legacyDefault);
    expect(result.ok).toBe(false);
    expect(result.errors.some((error) => error.code === "INVALID_PLAN")).toBe(true);
  });

  testWithTmp("accepts SQL default object payload", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

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

  testWithTmp("returns error result when plan file is missing", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const result = await planCheck(currentDb(), `${dir}/missing-plan.json`);
    expect(result.ok).toBe(false);
    expect(result.errors.length).toBeGreaterThan(0);
  });

});
