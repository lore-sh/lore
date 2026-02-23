import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import {
  commitById,
  commitEffects,
  commitHistory,
  initDb,
  queryTable,
  schema,
  tableOverview,
} from "../src";
import { applyPlan, createTestContext, writePlanFile, withTmpDirCleanup, currentDb } from "./helpers";

const testWithTmp = (name: string, fn: () => void | Promise<void>) => test(name, withTmpDirCleanup(fn));

describe("domain query API", () => {
  testWithTmp("tableOverview and queryTable return table metadata and page data", async () => {
    const { dir, dbPath } = createTestContext();
    await initDb({ dbPath });

    const createPlanPath = await writePlanFile(dir, "create-expenses.json", {
      message: "create expenses",
      operations: [
        {
          type: "create_table",
          table: "expenses",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "title", type: "TEXT", notNull: true, unique: true },
            { name: "amount", type: "INTEGER", notNull: true },
          ],
        },
      ],
    });
    const insertOnePath = await writePlanFile(dir, "insert-one.json", {
      message: "insert dinner",
      operations: [{ type: "insert", table: "expenses", values: { id: 1, title: "dinner", amount: 1200 } }],
    });
    const insertTwoPath = await writePlanFile(dir, "insert-two.json", {
      message: "insert hotel",
      operations: [{ type: "insert", table: "expenses", values: { id: 2, title: "hotel", amount: 2400 } }],
    });

    await applyPlan(currentDb(), createPlanPath);
    await applyPlan(currentDb(), insertOnePath);
    await applyPlan(currentDb(), insertTwoPath);

    const tables = tableOverview(currentDb());
    expect(tables).toHaveLength(1);
    expect(tables[0]?.name).toBe("expenses");
    expect(tables[0]?.rowCount).toBe(2);
    expect(tables[0]?.columnCount).toBe(3);

    const firstPage = queryTable(currentDb(), {
      table: "expenses",
      page: 1,
      pageSize: 1,
      sortBy: "amount",
      sortDir: "desc",
    });
    expect(firstPage.totalRows).toBe(2);
    expect(firstPage.totalPages).toBe(2);
    expect(firstPage.rows[0]).toMatchObject({ title: "hotel", amount: 2400 });

    const filtered = queryTable(currentDb(), {
      table: "expenses",
      filters: { title: "din" },
    });
    expect(filtered.totalRows).toBe(1);
    expect(filtered.rows[0]).toMatchObject({ title: "dinner", amount: 1200 });
  });

  testWithTmp("schema includes per-column unique/notNull/hidden metadata", async () => {
    const { dbPath } = createTestContext();
    await initDb({ dbPath });

    const direct = new Database(dbPath);
    direct.run("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL UNIQUE, bio TEXT)");
    direct.close(false);

    const dbSchema = schema(currentDb(), { table: "users" });
    expect(dbSchema.tables).toHaveLength(1);
    const table = dbSchema.tables[0];
    expect(table?.columns.find((column) => column.name === "id")).toMatchObject({
      primaryKey: true,
      unique: true,
      hidden: false,
    });
    expect(table?.columns.find((column) => column.name === "email")).toMatchObject({
      notNull: true,
      unique: true,
      hidden: false,
    });
  });

  testWithTmp("commitHistory, commitById, and commitEffects expose commit domain objects", async () => {
    const { dir, dbPath } = createTestContext();
    await initDb({ dbPath });

    const createPlanPath = await writePlanFile(dir, "history-create.json", {
      message: "create tasks",
      operations: [
        {
          type: "create_table",
          table: "tasks",
          columns: [{ name: "id", type: "INTEGER", primaryKey: true }],
        },
      ],
    });
    const insertPlanPath = await writePlanFile(dir, "history-insert.json", {
      message: "insert task",
      operations: [{ type: "insert", table: "tasks", values: { id: 1 } }],
    });

    await applyPlan(currentDb(), createPlanPath);
    await applyPlan(currentDb(), insertPlanPath);

    const summaries = commitHistory(currentDb());
    expect(summaries).toHaveLength(2);
    expect(summaries[0]?.message).toBe("insert task");

    const latestId = summaries[0]?.commitId;
    expect(typeof latestId).toBe("string");
    if (!latestId) {
      throw new Error("expected latest commit id");
    }

    const commit = commitById(currentDb(), latestId);
    expect(commit).not.toBeNull();
    expect(commit?.operations).toHaveLength(1);

    const effects = commitEffects(currentDb(), latestId);
    expect(effects.rows).toHaveLength(1);
    expect(effects.rows[0]?.tableName).toBe("tasks");
    expect(effects.schemas).toHaveLength(0);
  });

  testWithTmp("commitHistory filters by historical table names even after drop", async () => {
    const { dir, dbPath } = createTestContext();
    await initDb({ dbPath });

    const createPlanPath = await writePlanFile(dir, "history-filter-create.json", {
      message: "create invoices",
      operations: [
        {
          type: "create_table",
          table: "invoices",
          columns: [{ name: "id", type: "INTEGER", primaryKey: true }],
        },
      ],
    });
    const dropPlanPath = await writePlanFile(dir, "history-filter-drop.json", {
      message: "drop invoices",
      operations: [{ type: "drop_table", table: "invoices" }],
    });
    await applyPlan(currentDb(), createPlanPath);
    await applyPlan(currentDb(), dropPlanPath);

    const filtered = commitHistory(currentDb(), { table: "INVOICES" });
    expect(filtered.length).toBeGreaterThan(0);
    expect(commitHistory(currentDb(), { table: "missing_table" })).toEqual([]);
  });

  testWithTmp("queryTable validates sort/filter columns", async () => {
    const { dbPath } = createTestContext();
    await initDb({ dbPath });

    const direct = new Database(dbPath);
    direct.run("CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)");
    direct.run("INSERT INTO notes(id, body) VALUES (1, 'a')");
    direct.close(false);

    expect(() => queryTable(currentDb(), { table: "notes", sortBy: "missing_col" })).toThrow(/Sort column not found/i);
    expect(() => queryTable(currentDb(), { table: "notes", filters: { missing_col: "x" } })).toThrow(/Filter column not found/i);
  });
});
