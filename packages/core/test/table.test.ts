import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { initDb, queryTable, tableOverview } from "../src";
import { applyPlan, createTestContext, writePlanFile, withTmpDirCleanup, currentDb } from "./helpers";

const testWithTmp = (name: string, fn: () => void | Promise<void>) => test(name, withTmpDirCleanup(fn));

async function setupExpenses(dir: string): Promise<void> {
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
}

describe("table domain", () => {
  testWithTmp("tableOverview and queryTable return table metadata and page data", async () => {
    const { dir, dbPath } = createTestContext();
    await initDb({ dbPath });
    await setupExpenses(dir);

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
