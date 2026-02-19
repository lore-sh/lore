import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import {
  applyPlan,
  initDatabase,
  isTossError,
  readQuery,
  revertCommit,
} from "../src";
import { EFFECT_ROW_TABLE } from "../src/db";
import { createTestContext, writePlanFile, withTmpDirCleanup } from "./helpers";

const testWithTmp = (name: string, fn: () => void | Promise<void>) => test(name, withTmpDirCleanup(fn));


describe("revertCommit", () => {
  testWithTmp("drop_table revert restores table definition and rows", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const setup = writePlanFile(dir, "setup.json", {
      message: "setup",
      operations: [
        {
          type: "create_table",
          table: "expenses",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "item", type: "TEXT", notNull: true },
          ],
        },
        { type: "insert", table: "expenses", values: { id: 1, item: "dinner" } },
      ],
    });
    const drop = writePlanFile(dir, "drop.json", {
      message: "drop table",
      operations: [{ type: "drop_table", table: "expenses" }],
    });

    await applyPlan(setup, { dbPath });
    const dropCommit = await applyPlan(drop, { dbPath });

    const revert = revertCommit(dropCommit.commitId, { dbPath });
    expect(revert.ok).toBe(true);

    const rows = readQuery("SELECT id, item FROM expenses", { dbPath });
    expect(rows).toEqual([{ id: 1, item: "dinner" }]);
  });

  testWithTmp("revert works when original DDL used unquoted table name", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    direct.run("CREATE TABLE my_table (id INTEGER PRIMARY KEY, v TEXT)");
    direct.run("INSERT INTO my_table(id, v) VALUES (1, 'x')");
    direct.close(false);

    const drop = writePlanFile(dir, "drop-unquoted.json", {
      message: "drop table",
      operations: [{ type: "drop_table", table: "my_table" }],
    });

    const dropCommit = await applyPlan(drop, { dbPath });
    const reverted = revertCommit(dropCommit.commitId, { dbPath });
    expect(reverted.ok).toBe(true);
    const rows = readQuery("SELECT id, v FROM my_table", { dbPath });
    expect(rows).toEqual([{ id: 1, v: "x" }]);
  });

  testWithTmp("drop_column and alter_column_type revert restore schema and values", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const setup = writePlanFile(dir, "setup.json", {
      message: "setup ledger",
      operations: [
        {
          type: "create_table",
          table: "ledger",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "amount", type: "TEXT", notNull: true },
            { name: "note", type: "TEXT" },
          ],
        },
        { type: "insert", table: "ledger", values: { id: 1, amount: "1200", note: "meal" } },
      ],
    });
    const mutate = writePlanFile(dir, "mutate.json", {
      message: "schema mutate",
      operations: [
        { type: "alter_column_type", table: "ledger", column: "amount", newType: "INTEGER" },
        { type: "drop_column", table: "ledger", column: "note" },
      ],
    });

    await applyPlan(setup, { dbPath });
    const mutateCommit = await applyPlan(mutate, { dbPath });

    const revert = revertCommit(mutateCommit.commitId, { dbPath });
    expect(revert.ok).toBe(true);

    const typeRow = readQuery("SELECT typeof(amount) AS t, note FROM ledger WHERE id=1", { dbPath });
    expect(typeRow).toEqual([{ t: "text", note: "meal" }]);
  });

  testWithTmp("restore_table path handles self-referential FOREIGN KEY tables", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    direct.run("PRAGMA foreign_keys=ON");
    direct.run(`
      CREATE TABLE graph_nodes (
        id INTEGER PRIMARY KEY,
        parent_id INTEGER,
        name TEXT,
        tag TEXT,
        FOREIGN KEY(parent_id) REFERENCES graph_nodes(id)
      )
    `);
    direct.run("INSERT INTO graph_nodes(id, parent_id, name, tag) VALUES (1, NULL, 'root', 'a')");
    direct.run("INSERT INTO graph_nodes(id, parent_id, name, tag) VALUES (2, 1, 'child', 'b')");
    direct.close(false);

    const dropColumn = writePlanFile(dir, "drop-tag-self-fk.json", {
      message: "drop tag",
      operations: [{ type: "drop_column", table: "graph_nodes", column: "tag" }],
    });
    const dropped = await applyPlan(dropColumn, { dbPath });
    const reverted = revertCommit(dropped.commitId, { dbPath });
    expect(reverted.ok).toBe(true);

    const verifyDb = new Database(dbPath);
    verifyDb.run("PRAGMA foreign_keys=ON");
    const rows = verifyDb
      .query("SELECT id, parent_id, tag FROM graph_nodes ORDER BY id")
      .all() as Array<{ id: number; parent_id: number | null; tag: string | null }>;
    expect(rows).toEqual([
      { id: 1, parent_id: null, tag: "a" },
      { id: 2, parent_id: 1, tag: "b" },
    ]);
    const fkRows = verifyDb.query("PRAGMA foreign_key_list('graph_nodes')").all() as Array<{ table: string }>;
    expect(fkRows).toHaveLength(1);
    expect(fkRows[0]?.table).toBe("graph_nodes");
    verifyDb.close(false);
  });

  testWithTmp("restore_table path supports legacy single-quoted table identifiers", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    direct.run("CREATE TABLE 'sq_restore' ('id' INTEGER PRIMARY KEY, 'name' TEXT, 'tag' TEXT)");
    direct.run("INSERT INTO 'sq_restore'('id', 'name', 'tag') VALUES (1, 'a', 'x')");
    direct.close(false);

    const dropColumn = writePlanFile(dir, "drop-tag-single-quoted-table.json", {
      message: "drop single-quoted tag",
      operations: [{ type: "drop_column", table: "sq_restore", column: "tag" }],
    });
    const dropped = await applyPlan(dropColumn, { dbPath });
    const reverted = revertCommit(dropped.commitId, { dbPath });
    expect(reverted.ok).toBe(true);

    const rows = readQuery('SELECT "id", "name", "tag" FROM "sq_restore"', { dbPath });
    expect(rows).toEqual([{ id: 1, name: "a", tag: "x" }]);
  });

  testWithTmp("update/delete revert detects row conflicts", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const setup = writePlanFile(dir, "setup.json", {
      message: "setup",
      operations: [
        {
          type: "create_table",
          table: "items",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "name", type: "TEXT", notNull: true },
            { name: "qty", type: "INTEGER", notNull: true },
          ],
        },
        { type: "insert", table: "items", values: { id: 1, name: "apple", qty: 3 } },
      ],
    });
    const updateA = writePlanFile(dir, "update-a.json", {
      message: "set qty 5",
      operations: [{ type: "update", table: "items", values: { qty: 5 }, where: { id: 1 } }],
    });
    const updateB = writePlanFile(dir, "update-b.json", {
      message: "set qty 7",
      operations: [{ type: "update", table: "items", values: { qty: 7 }, where: { id: 1 } }],
    });

    await applyPlan(setup, { dbPath });
    const target = await applyPlan(updateA, { dbPath });
    await applyPlan(updateB, { dbPath });

    const result = revertCommit(target.commitId, { dbPath });
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.conflicts.length).toBeGreaterThan(0);
      expect(result.conflicts[0]?.kind).toBe("row");
    }
  });

  testWithTmp("revert delete returns structured conflict when later commit reinserted same PK", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const setup = writePlanFile(dir, "setup-delete-conflict.json", {
      message: "setup",
      operations: [
        {
          type: "create_table",
          table: "users",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "name", type: "TEXT", notNull: true },
          ],
        },
        { type: "insert", table: "users", values: { id: 1, name: "alice" } },
      ],
    });
    const deletePlan = writePlanFile(dir, "delete-user.json", {
      message: "delete user",
      operations: [{ type: "delete", table: "users", where: { id: 1 } }],
    });
    const reinsertPlan = writePlanFile(dir, "reinsert-user.json", {
      message: "reinsert user",
      operations: [{ type: "insert", table: "users", values: { id: 1, name: "alice" } }],
    });

    await applyPlan(setup, { dbPath });
    const deleted = await applyPlan(deletePlan, { dbPath });
    await applyPlan(reinsertPlan, { dbPath });

    const result = revertCommit(deleted.commitId, { dbPath });
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.conflicts.length).toBeGreaterThan(0);
      expect(result.conflicts[0]?.kind).toBe("row");
      expect(result.conflicts[0]?.reason.includes("PRIMARY KEY")).toBe(true);
    }
  });

  testWithTmp("revert of revert is supported", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const setup = writePlanFile(dir, "setup.json", {
      message: "setup",
      operations: [
        {
          type: "create_table",
          table: "events",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "title", type: "TEXT", notNull: true },
          ],
        },
        { type: "insert", table: "events", values: { id: 1, title: "dentist" } },
      ],
    });
    const drop = writePlanFile(dir, "drop.json", {
      message: "drop events",
      operations: [{ type: "drop_table", table: "events" }],
    });

    await applyPlan(setup, { dbPath });
    const dropped = await applyPlan(drop, { dbPath });
    const first = revertCommit(dropped.commitId, { dbPath });
    expect(first.ok).toBe(true);
    if (!first.ok) {
      throw new Error("expected first revert success");
    }

    const second = revertCommit(first.revertCommit.commitId, { dbPath });
    expect(second.ok).toBe(true);
    if (!second.ok) {
      throw new Error("expected second revert success");
    }

    const tableCount = readQuery("SELECT COUNT(*) AS c FROM sqlite_master WHERE type='table' AND name='events'", { dbPath });
    expect(tableCount).toEqual([{ c: 0 }]);
  });

  testWithTmp("revert throws when update effect is missing beforeRow", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const setup = writePlanFile(dir, "setup-missing-before.json", {
      message: "setup",
      operations: [
        {
          type: "create_table",
          table: "audit_items",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "name", type: "TEXT", notNull: true },
          ],
        },
        { type: "insert", table: "audit_items", values: { id: 1, name: "old" } },
      ],
    });
    const update = writePlanFile(dir, "update-missing-before.json", {
      message: "update",
      operations: [{ type: "update", table: "audit_items", values: { name: "new" }, where: { id: 1 } }],
    });
    await applyPlan(setup, { dbPath });
    const updated = await applyPlan(update, { dbPath });

    const tamper = new Database(dbPath);
    tamper
      .query(`UPDATE ${EFFECT_ROW_TABLE} SET before_row_json=NULL WHERE commit_id=? AND op_kind='update'`)
      .run(updated.commitId);
    tamper.close(false);

    try {
      revertCommit(updated.commitId, { dbPath });
      throw new Error("revertCommit should fail when beforeRow is missing");
    } catch (error) {
      expect(isTossError(error)).toBe(true);
      if (isTossError(error)) {
        expect(error.code).toBe("REVERT_FAILED");
      }
    }
  });

  testWithTmp("revert throws when update beforeRow contains non-primitive values", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const setup = writePlanFile(dir, "setup-non-primitive-before.json", {
      message: "setup",
      operations: [
        {
          type: "create_table",
          table: "complex_items",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "payload", type: "TEXT", notNull: true },
          ],
        },
        { type: "insert", table: "complex_items", values: { id: 1, payload: "a" } },
      ],
    });
    const update = writePlanFile(dir, "update-non-primitive-before.json", {
      message: "update payload",
      operations: [{ type: "update", table: "complex_items", values: { payload: "b" }, where: { id: 1 } }],
    });
    await applyPlan(setup, { dbPath });
    const updated = await applyPlan(update, { dbPath });

    const tamper = new Database(dbPath);
    tamper
      .query(`UPDATE ${EFFECT_ROW_TABLE} SET before_row_json=? WHERE commit_id=? AND op_kind='update'`)
      .run('{"id":1,"payload":{"nested":true}}', updated.commitId);
    tamper.close(false);

    try {
      revertCommit(updated.commitId, { dbPath });
      throw new Error("revertCommit should fail when beforeRow contains non-primitive values");
    } catch (error) {
      expect(isTossError(error)).toBe(true);
      if (isTossError(error)) {
        expect(error.code).toBe("REVERT_FAILED");
      }
    }
  });
});
