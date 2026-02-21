import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import {
  applyPlan,
  getHistory,
  getStatus,
  initDatabase,
  isTossError,
  readQuery,
} from "../src";
import { executeOperation } from "../src/engine/execute";
import type { RestoreTableOperation } from "../src/types";
import { createTestContext, writePlanFile, withTmpDirCleanup } from "./helpers";

const testWithTmp = (name: string, fn: () => void | Promise<void>) => test(name, withTmpDirCleanup(fn));

describe("applyPlan", () => {
  testWithTmp("init -> apply -> status -> history works", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const createPlanPath = await writePlanFile(dir, "create.json", {
      message: "create expenses table",
      operations: [
        {
          type: "create_table",
          table: "expenses",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "item", type: "TEXT", notNull: true },
            { name: "amount", type: "INTEGER", notNull: true },
          ],
        },
      ],
    });
    const insertPlanPath = await writePlanFile(dir, "insert.json", {
      message: "insert dinner",
      operations: [{ type: "insert", table: "expenses", values: { id: 1, item: "dinner", amount: 1200 } }],
    });

    await applyPlan(createPlanPath);
    const insertCommit = await applyPlan(insertPlanPath);

    const status = getStatus();
    expect(status.tableCount).toBe(1);
    expect(status.headCommit?.commitId).toBe(insertCommit.commitId);
    expect(status.snapshotCount).toBe(0);
    expect(status.lastVerifiedAt).toBeNull();
    expect(status.lastVerifiedOk).toBeNull();

    const history = getHistory();
    expect(history).toHaveLength(2);
    expect(history[0]?.commitId).toBe(insertCommit.commitId);
    expect(history[0]?.parentIds).toHaveLength(1);
    expect(history[0]?.stateHashAfter.length).toBeGreaterThan(10);
  });

  testWithTmp("operations fail for table without primary key", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    direct.run("CREATE TABLE no_pk (name TEXT)");
    direct.close(false);

    const insertPlan = await writePlanFile(dir, "insert-no-pk.json", {
      message: "insert no pk",
      operations: [{ type: "insert", table: "no_pk", values: { name: "a" } }],
    });

    try {
      await applyPlan(insertPlan);
      throw new Error("applyPlan should fail for table without PK");
    } catch (error) {
      expect(isTossError(error)).toBe(true);
      if (isTossError(error)) {
        expect(error.code).toBe("TABLE_WITHOUT_PRIMARY_KEY");
      }
    }
  });

  testWithTmp("operations fail when observed table contains NULL primary-key values", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    direct.run("CREATE TABLE weak_pk (k TEXT PRIMARY KEY, v TEXT)");
    direct.run("INSERT INTO weak_pk(k, v) VALUES (NULL, 'a')");
    direct.run("INSERT INTO weak_pk(k, v) VALUES (NULL, 'b')");
    direct.close(false);

    const plan = await writePlanFile(dir, "plan-with-nullable-pk-values.json", {
      message: "noop create",
      operations: [
        {
          type: "create_table",
          table: "safe_table",
          columns: [{ name: "id", type: "INTEGER", primaryKey: true }],
        },
      ],
    });

    try {
      await applyPlan(plan);
      throw new Error("applyPlan should fail when NULL PK values exist in tracked table");
    } catch (error) {
      expect(isTossError(error)).toBe(true);
      if (isTossError(error)) {
        expect(error.code).toBe("NULL_PRIMARY_KEY_VALUE");
      }
    }
  });

  testWithTmp("create_table supports SQL timestamp defaults without planner-provided now values", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const createPlanPath = await writePlanFile(dir, "create-events.sql-default.json", {
      message: "create events with sql default timestamp",
      operations: [
        {
          type: "create_table",
          table: "events",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "title", type: "TEXT", notNull: true },
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
    await applyPlan(createPlanPath);

    const insertPlanPath = await writePlanFile(dir, "insert-events.sql-default.json", {
      message: "insert event without recorded_at",
      operations: [{ type: "insert", table: "events", values: { id: 1, title: "release" } }],
    });
    await applyPlan(insertPlanPath);

    const rows = readQuery("SELECT id, title, recorded_at FROM events");
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({ id: 1, title: "release" });
    expect(typeof rows[0]?.recorded_at).toBe("string");
    expect((rows[0]?.recorded_at as string).length).toBeGreaterThan(0);
  });

  testWithTmp("add_column with SQL default is rejected for non-empty tables", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const setup = await writePlanFile(dir, "add-column-sql-default-setup.json", {
      message: "setup tasks with one row",
      operations: [
        {
          type: "create_table",
          table: "tasks",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "title", type: "TEXT", notNull: true },
          ],
        },
        {
          type: "insert",
          table: "tasks",
          values: { id: 1, title: "first" },
        },
      ],
    });
    await applyPlan(setup);

    const addColumnPlan = await writePlanFile(dir, "add-column-sql-default-fail.json", {
      message: "try to add created_at with sql default",
      operations: [
        {
          type: "add_column",
          table: "tasks",
          column: {
            name: "created_at",
            type: "TEXT",
            default: { kind: "sql", expr: "CURRENT_TIMESTAMP" },
          },
        },
      ],
    });

    await expect(applyPlan(addColumnPlan)).rejects.toThrow(
      /add_column with SQL default is only allowed on empty tables/i,
    );
  });

  testWithTmp("restore_table malformed row value fails with INVALID_OPERATION instead of TypeError", () => {
    const db = new Database(":memory:");
    const operation: RestoreTableOperation = {
      type: "restore_table",
      table: "users",
      ddlSql: "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
      rows: [
        {
          id: { storageClass: "integer", sqlLiteral: "1" },
          name: { storageClass: "text", sqlLiteral: "'alice'" },
        },
        {
          id: { storageClass: "integer", sqlLiteral: "2" },
          name: undefined as unknown as { storageClass: "text"; sqlLiteral: string },
        },
      ],
      secondaryObjects: [],
    };

    try {
      executeOperation(db, operation);
      throw new Error("executeOperation should fail for malformed restore row");
    } catch (error) {
      expect(error).not.toBeInstanceOf(TypeError);
      expect(isTossError(error)).toBe(true);
      if (isTossError(error)) {
        expect(error.code).toBe("INVALID_OPERATION");
        expect(error.message).toContain("restore_table row contains unsupported encoded value");
      }
      const restored = db
        .query("SELECT name FROM sqlite_master WHERE type='table' AND name='users' LIMIT 1")
        .get() as { name: string } | null;
      expect(restored).toBeNull();
    } finally {
      db.close(false);
    }
  });
});
