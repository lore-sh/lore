import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { OP_TABLE } from "../src/engine/db";
import {
  applyPlan,
  getStudioCommitDetail,
  getStudioSchema,
  initDatabase,
  isTossError,
  listStudioHistory,
  listStudioTableHistory,
  listStudioTables,
  readStudioTable,
} from "../src";
import { createTestContext, writePlanFile, withTmpDirCleanup } from "./helpers";

const testWithTmp = (name: string, fn: () => void | Promise<void>) => test(name, withTmpDirCleanup(fn));

describe("studio api", () => {
  testWithTmp("list tables and read table data with sort/filter/pagination", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const createPlanPath = await writePlanFile(dir, "studio-create.json", {
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
    const insertOnePath = await writePlanFile(dir, "studio-insert-1.json", {
      message: "insert dinner",
      operations: [{ type: "insert", table: "expenses", values: { id: 1, title: "dinner", amount: 1200 } }],
    });
    const insertTwoPath = await writePlanFile(dir, "studio-insert-2.json", {
      message: "insert hotel",
      operations: [{ type: "insert", table: "expenses", values: { id: 2, title: "hotel", amount: 2400 } }],
    });

    await applyPlan(createPlanPath);
    await applyPlan(insertOnePath);
    await applyPlan(insertTwoPath);

    const tables = listStudioTables();
    expect(tables.tables).toHaveLength(1);
    expect(tables.tables[0]?.name).toBe("expenses");
    expect(tables.tables[0]?.rowCount).toBe(2);
    expect(tables.tables[0]?.columnCount).toBe(3);
    expect(typeof tables.tables[0]?.lastUpdatedAt).toBe("number");

    const firstPage = readStudioTable({
      table: "expenses",
      page: 1,
      pageSize: 1,
      sortBy: "amount",
      sortDir: "desc",
    });
    expect(firstPage.totalRows).toBe(2);
    expect(firstPage.totalPages).toBe(2);
    expect(firstPage.rows[0]).toMatchObject({ title: "hotel", amount: 2400 });

    const filtered = readStudioTable({
      table: "expenses",
      filters: { title: "din" },
    });
    expect(filtered.totalRows).toBe(1);
    expect(filtered.rows[0]).toMatchObject({ title: "dinner", amount: 1200 });
  });

  testWithTmp("schema view includes column constraints", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });
    const createPlanPath = await writePlanFile(dir, "studio-schema-create.json", {
      message: "create expenses",
      operations: [
        {
          type: "create_table",
          table: "expenses",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "title", type: "TEXT", notNull: true, unique: true },
            { name: "memo", type: "TEXT" },
          ],
        },
      ],
    });
    await applyPlan(createPlanPath);

    const schema = getStudioSchema();
    const table = schema.tables.find((entry) => entry.name === "expenses");
    expect(table).toBeDefined();
    expect(table?.columns.find((column) => column.name === "id")).toMatchObject({
      primaryKey: true,
      unique: true,
    });
    expect(table?.columns.find((column) => column.name === "title")).toMatchObject({
      notNull: true,
      unique: true,
    });
  });

  testWithTmp("schema view does not mark composite primary key columns as individually unique", async () => {
    const { dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    direct.run(`
      CREATE TABLE ledger (
        account TEXT NOT NULL,
        at TEXT NOT NULL,
        amount INTEGER NOT NULL,
        PRIMARY KEY (account, at)
      )
    `);
    direct.close(false);

    const schema = getStudioSchema();
    const table = schema.tables.find((entry) => entry.name === "ledger");
    expect(table).toBeDefined();
    expect(table?.columns.find((column) => column.name === "account")).toMatchObject({
      primaryKey: true,
      unique: false,
    });
    expect(table?.columns.find((column) => column.name === "at")).toMatchObject({
      primaryKey: true,
      unique: false,
    });
  });

  testWithTmp("schema view does not treat partial unique index as globally unique", async () => {
    const { dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    direct.run(`
      CREATE TABLE sessions (
        id INTEGER PRIMARY KEY,
        token TEXT NOT NULL,
        revoked INTEGER NOT NULL DEFAULT 0
      )
    `);
    direct.run(`
      CREATE UNIQUE INDEX sessions_token_active_unique
      ON sessions(token)
      WHERE revoked = 0
    `);
    direct.close(false);

    const schema = getStudioSchema();
    const table = schema.tables.find((entry) => entry.name === "sessions");
    expect(table).toBeDefined();
    expect(table?.columns.find((column) => column.name === "id")).toMatchObject({
      primaryKey: true,
      unique: true,
    });
    expect(table?.columns.find((column) => column.name === "token")).toMatchObject({
      unique: false,
    });
  });

  testWithTmp("schema view does not treat unique indexes with expression terms as single-column unique", async () => {
    const { dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    direct.run(`
      CREATE TABLE expr_unique (
        id INTEGER PRIMARY KEY,
        a TEXT NOT NULL,
        b TEXT NOT NULL
      )
    `);
    direct.run(`
      CREATE UNIQUE INDEX expr_unique_idx
      ON expr_unique(a, lower(b))
    `);
    direct.close(false);

    const schema = getStudioSchema();
    const table = schema.tables.find((entry) => entry.name === "expr_unique");
    expect(table).toBeDefined();
    expect(table?.columns.find((column) => column.name === "id")).toMatchObject({
      primaryKey: true,
      unique: true,
    });
    expect(table?.columns.find((column) => column.name === "a")).toMatchObject({
      unique: false,
    });
  });

  testWithTmp("history detail returns operations and effects", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });
    const createPlanPath = await writePlanFile(dir, "studio-history-create.json", {
      message: "create expenses",
      operations: [
        {
          type: "create_table",
          table: "expenses",
          columns: [{ name: "id", type: "INTEGER", primaryKey: true }],
        },
      ],
    });
    const insertPlanPath = await writePlanFile(dir, "studio-history-insert.json", {
      message: "insert 1",
      operations: [{ type: "insert", table: "expenses", values: { id: 1 } }],
    });

    await applyPlan(createPlanPath);
    await applyPlan(insertPlanPath);

    const history = listStudioHistory();
    expect(history).toHaveLength(2);
    expect(history[0]?.shortId.length).toBe(12);
    expect(history[0]?.message).toBe("insert 1");

    const commitId = history[0]?.commitId;
    expect(typeof commitId).toBe("string");
    const detail = getStudioCommitDetail(commitId ?? "");
    expect(detail.commit.operations).toHaveLength(1);
    expect(detail.rowEffects).toHaveLength(1);
    expect(detail.rowEffects[0]?.tableName).toBe("expenses");
    expect(detail.schemaEffects).toHaveLength(0);
  });

  testWithTmp("history list applies limit without decoding skipped commit operations", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const createPlanPath = await writePlanFile(dir, "studio-history-limit-create.json", {
      message: "create ledger",
      operations: [
        {
          type: "create_table",
          table: "ledger",
          columns: [{ name: "id", type: "INTEGER", primaryKey: true }],
        },
      ],
    });
    const insertOnePath = await writePlanFile(dir, "studio-history-limit-insert-1.json", {
      message: "insert 1",
      operations: [{ type: "insert", table: "ledger", values: { id: 1 } }],
    });
    const insertTwoPath = await writePlanFile(dir, "studio-history-limit-insert-2.json", {
      message: "insert 2",
      operations: [{ type: "insert", table: "ledger", values: { id: 2 } }],
    });

    await applyPlan(createPlanPath);
    await applyPlan(insertOnePath);
    await applyPlan(insertTwoPath);

    const direct = new Database(dbPath);
    direct.run("PRAGMA ignore_check_constraints=ON");
    const oldest = direct
      .query<{ commit_id: string }, []>("SELECT commit_id FROM _toss_commit ORDER BY seq ASC LIMIT 1")
      .get();
    expect(typeof oldest?.commit_id).toBe("string");
    direct.query(`UPDATE ${OP_TABLE} SET op_json = ? WHERE commit_id = ?`).run("{", oldest?.commit_id ?? "");
    direct.run("PRAGMA ignore_check_constraints=OFF");
    direct.close(false);

    const history = listStudioHistory({ limit: 1 });
    expect(history).toHaveLength(1);
    expect(history[0]?.message).toBe("insert 2");
  });

  testWithTmp("history supports kind/table/page filters and includes summary metadata", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const createExpensesPath = await writePlanFile(dir, "studio-history-filter-create-expenses.json", {
      message: "create expenses",
      operations: [
        {
          type: "create_table",
          table: "expenses",
          columns: [{ name: "id", type: "INTEGER", primaryKey: true }],
        },
      ],
    });
    const createCalendarPath = await writePlanFile(dir, "studio-history-filter-create-calendar.json", {
      message: "create calendar",
      operations: [
        {
          type: "create_table",
          table: "calendar",
          columns: [{ name: "id", type: "INTEGER", primaryKey: true }],
        },
      ],
    });
    const insertExpensesPath = await writePlanFile(dir, "studio-history-filter-insert-expenses.json", {
      message: "insert expenses",
      operations: [{ type: "insert", table: "expenses", values: { id: 1 } }],
    });
    const insertCalendarPath = await writePlanFile(dir, "studio-history-filter-insert-calendar.json", {
      message: "insert calendar",
      operations: [{ type: "insert", table: "calendar", values: { id: 1 } }],
    });

    await applyPlan(createExpensesPath);
    await applyPlan(createCalendarPath);
    await applyPlan(insertExpensesPath);
    await applyPlan(insertCalendarPath);

    const filtered = listStudioHistory({
      kind: "apply",
      table: "expenses",
      limit: 10,
      page: 1,
    });
    expect(filtered).toHaveLength(2);
    expect(filtered[0]?.message).toBe("insert expenses");
    expect(filtered[0]?.operationCount).toBe(1);
    expect(filtered[0]?.rowEffectCount).toBe(1);
    expect(filtered[0]?.schemaEffectCount).toBe(0);
    expect(filtered[0]?.affectedTables).toEqual(["expenses"]);
    expect(filtered[1]?.message).toBe("create expenses");

    const paged = listStudioHistory({ limit: 1, page: 2 });
    expect(paged).toHaveLength(1);
    expect(paged[0]?.message).toBe("insert expenses");
  });

  testWithTmp("history table filter resolves table names case-insensitively", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const createExpensesPath = await writePlanFile(dir, "studio-history-case-create-expenses.json", {
      message: "create Expenses",
      operations: [
        {
          type: "create_table",
          table: "Expenses",
          columns: [{ name: "id", type: "INTEGER", primaryKey: true }],
        },
      ],
    });
    const insertExpensesPath = await writePlanFile(dir, "studio-history-case-insert-expenses.json", {
      message: "insert Expenses",
      operations: [{ type: "insert", table: "Expenses", values: { id: 1 } }],
    });

    await applyPlan(createExpensesPath);
    await applyPlan(insertExpensesPath);

    const history = listStudioHistory({
      table: "expenses",
      limit: 10,
      page: 1,
    });
    expect(history).toHaveLength(2);
    expect(history.map((entry) => entry.message)).toEqual(["insert Expenses", "create Expenses"]);
    expect(history.every((entry) => entry.affectedTables.includes("Expenses"))).toBe(true);
  });

  testWithTmp("history table filter still works for dropped tables", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const createPath = await writePlanFile(dir, "studio-history-dropped-create.json", {
      message: "create archive",
      operations: [
        {
          type: "create_table",
          table: "archive",
          columns: [{ name: "id", type: "INTEGER", primaryKey: true }],
        },
      ],
    });
    const insertPath = await writePlanFile(dir, "studio-history-dropped-insert.json", {
      message: "insert archive",
      operations: [{ type: "insert", table: "archive", values: { id: 1 } }],
    });
    const dropPath = await writePlanFile(dir, "studio-history-dropped-drop.json", {
      message: "drop archive",
      operations: [{ type: "drop_table", table: "archive" }],
    });

    await applyPlan(createPath);
    await applyPlan(insertPath);
    await applyPlan(dropPath);

    const history = listStudioHistory({
      table: "archive",
      limit: 10,
      page: 1,
    });
    expect(history).toHaveLength(3);
    expect(history.map((entry) => entry.message)).toEqual(["drop archive", "insert archive", "create archive"]);
  });

  testWithTmp("table history includes row and schema affecting commits only", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const createExpensesPath = await writePlanFile(dir, "studio-table-history-create-expenses.json", {
      message: "create expenses",
      operations: [
        {
          type: "create_table",
          table: "expenses",
          columns: [{ name: "id", type: "INTEGER", primaryKey: true }],
        },
      ],
    });
    const createCalendarPath = await writePlanFile(dir, "studio-table-history-create-calendar.json", {
      message: "create calendar",
      operations: [
        {
          type: "create_table",
          table: "calendar",
          columns: [{ name: "id", type: "INTEGER", primaryKey: true }],
        },
      ],
    });
    const insertExpensesPath = await writePlanFile(dir, "studio-table-history-insert-expenses.json", {
      message: "insert expenses",
      operations: [{ type: "insert", table: "expenses", values: { id: 1 } }],
    });

    await applyPlan(createExpensesPath);
    await applyPlan(createCalendarPath);
    await applyPlan(insertExpensesPath);

    const history = listStudioTableHistory("expenses", { limit: 10, page: 1 });
    expect(history).toHaveLength(2);
    expect(history.map((entry) => entry.message)).toEqual(["insert expenses", "create expenses"]);
    expect(history.every((entry) => entry.affectedTables.includes("expenses"))).toBe(true);
  });

  testWithTmp("table history supports page and validates table existence", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    await applyPlan(
      await writePlanFile(dir, "studio-table-history-page-create.json", {
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
      await writePlanFile(dir, "studio-table-history-page-insert-1.json", {
        message: "insert expenses 1",
        operations: [{ type: "insert", table: "expenses", values: { id: 1 } }],
      }),
    );
    await applyPlan(
      await writePlanFile(dir, "studio-table-history-page-insert-2.json", {
        message: "insert expenses 2",
        operations: [{ type: "insert", table: "expenses", values: { id: 2 } }],
      }),
    );

    const secondPage = listStudioTableHistory("expenses", { limit: 1, page: 2 });
    expect(secondPage).toHaveLength(1);
    expect(secondPage[0]?.message).toBe("insert expenses 1");

    try {
      listStudioTableHistory("missing_table", { limit: 10, page: 1 });
      throw new Error("listStudioTableHistory should fail for unknown tables");
    } catch (error) {
      expect(isTossError(error)).toBe(true);
      if (isTossError(error)) {
        expect(error.code).toBe("NOT_FOUND");
      }
    }
  });

  testWithTmp("generated columns are visible in metadata and usable for sort/filter", async () => {
    const { dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    direct.run(`
      CREATE TABLE calc (
        id INTEGER PRIMARY KEY,
        amount INTEGER NOT NULL,
        amount_x2 INTEGER GENERATED ALWAYS AS (amount * 2) VIRTUAL,
        amount_x3 INTEGER GENERATED ALWAYS AS (amount * 3) STORED
      )
    `);
    direct.run(`INSERT INTO calc(id, amount) VALUES (1, 1)`);
    direct.run(`INSERT INTO calc(id, amount) VALUES (2, 2)`);
    direct.close(false);

    const tables = listStudioTables();
    const calcSummary = tables.tables.find((table) => table.name === "calc");
    expect(calcSummary).toBeDefined();
    expect(calcSummary?.columnCount).toBe(4);

    const sorted = readStudioTable({
      table: "calc",
      sortBy: "amount_x3",
      sortDir: "desc",
    });
    expect(sorted.columns.some((column) => column.name === "amount_x2")).toBe(true);
    expect(sorted.columns.some((column) => column.name === "amount_x3")).toBe(true);
    expect(sorted.rows[0]).toMatchObject({ id: 2, amount_x2: 4, amount_x3: 6 });

    const filtered = readStudioTable({
      table: "calc",
      filters: { amount_x2: "4" },
    });
    expect(filtered.totalRows).toBe(1);
    expect(filtered.rows[0]).toMatchObject({ id: 2, amount_x2: 4, amount_x3: 6 });

    const schema = getStudioSchema();
    const calcSchema = schema.tables.find((table) => table.name === "calc");
    expect(calcSchema).toBeDefined();
    expect(calcSchema?.columns.find((column) => column.name === "amount_x2")?.hidden).toBe(false);
    expect(calcSchema?.columns.find((column) => column.name === "amount_x3")?.hidden).toBe(false);
  });

  testWithTmp("invalid filter column fails clearly", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });
    const createPlanPath = await writePlanFile(dir, "studio-filter-create.json", {
      message: "create expenses",
      operations: [
        {
          type: "create_table",
          table: "expenses",
          columns: [{ name: "id", type: "INTEGER", primaryKey: true }],
        },
      ],
    });
    await applyPlan(createPlanPath);

    try {
      readStudioTable({ table: "expenses", filters: { unknown: "1" } });
      throw new Error("readStudioTable should fail for unknown filter columns");
    } catch (error) {
      expect(isTossError(error)).toBe(true);
      if (isTossError(error)) {
        expect(error.code).toBe("INVALID_OPERATION");
      }
    }
  });

  testWithTmp("WITHOUT ROWID tables are readable with default and custom sort", async () => {
    const { dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    direct.run(`
      CREATE TABLE ledger (
        account TEXT NOT NULL,
        at TEXT NOT NULL,
        amount INTEGER NOT NULL,
        PRIMARY KEY (account, at)
      ) WITHOUT ROWID
    `);
    direct.run(`INSERT INTO ledger(account, at, amount) VALUES ('a', '2026-01-01', 100)`);
    direct.run(`INSERT INTO ledger(account, at, amount) VALUES ('b', '2026-01-01', 100)`);
    direct.run(`INSERT INTO ledger(account, at, amount) VALUES ('c', '2026-01-01', 200)`);
    direct.close(false);

    const defaultSorted = readStudioTable({
      table: "ledger",
      page: 1,
      pageSize: 10,
    });
    expect(defaultSorted.totalRows).toBe(3);
    expect(defaultSorted.rows.map((row) => row.account)).toEqual(["a", "b", "c"]);

    const customSorted = readStudioTable({
      table: "ledger",
      sortBy: "amount",
      sortDir: "desc",
    });
    expect(customSorted.rows.map((row) => row.account)).toEqual(["c", "a", "b"]);
  });

  testWithTmp("rowid-shadowed tables still use pseudo rowid for deterministic pagination tie-breaks", async () => {
    const { dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    direct.run(`
      CREATE TABLE shadowed (
        rowid TEXT NOT NULL,
        amount INTEGER NOT NULL,
        name TEXT NOT NULL
      )
    `);
    direct.run(`INSERT INTO shadowed(rowid, amount, name) VALUES ('z', 10, 'first')`);
    direct.run(`INSERT INTO shadowed(rowid, amount, name) VALUES ('a', 10, 'second')`);
    direct.run(`INSERT INTO shadowed(rowid, amount, name) VALUES ('m', 10, 'third')`);
    direct.close(false);

    const defaultSorted = readStudioTable({
      table: "shadowed",
      page: 1,
      pageSize: 10,
    });
    expect(defaultSorted.rows.map((row) => row.name)).toEqual(["first", "second", "third"]);

    const sortedByAmount = readStudioTable({
      table: "shadowed",
      page: 1,
      pageSize: 10,
      sortBy: "amount",
      sortDir: "asc",
    });
    expect(sortedByAmount.rows.map((row) => row.name)).toEqual(["first", "second", "third"]);
  });
});
