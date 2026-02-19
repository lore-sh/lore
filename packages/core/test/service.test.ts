import { afterEach, describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { existsSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { COMMIT_TABLE, EFFECT_ROW_TABLE, ENGINE_META_TABLE } from "../src/db";
import { schemaHash } from "../src/rows";
import {
  applyPlan,
  getHistory,
  getStatus,
  initDatabase,
  isTossError,
  readQuery,
  recoverFromSnapshot,
  revertCommit,
  verifyDatabase,
} from "../src";

const tmpDirs: string[] = [];

function createTestContext(): { dir: string; dbPath: string } {
  const dir = mkdtempSync(join(tmpdir(), "toss-core-test-"));
  tmpDirs.push(dir);
  const dbPath = join(dir, "toss.db");
  return { dir, dbPath };
}

function writePlanFile(dir: string, name: string, payload: unknown): string {
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

describe("toss canonical engine", () => {
  test("init -> apply -> status -> history works", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const createPlanPath = writePlanFile(dir, "create.json", {
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
    const insertPlanPath = writePlanFile(dir, "insert.json", {
      message: "insert dinner",
      operations: [{ type: "insert", table: "expenses", values: { id: 1, item: "dinner", amount: 1200 } }],
    });

    await applyPlan(createPlanPath, { dbPath });
    const insertCommit = await applyPlan(insertPlanPath, { dbPath });

    const status = getStatus({ dbPath });
    expect(status.tableCount).toBe(1);
    expect(status.headCommit?.commitId).toBe(insertCommit.commitId);
    expect(status.snapshotCount).toBe(0);
    expect(status.lastVerifiedAt).toBeNull();
    expect(status.lastVerifiedOk).toBeNull();
    expect(status.lastVerifiedOkAt).toBeNull();

    const history = getHistory({ dbPath, verbose: true });
    expect(history).toHaveLength(2);
    expect(history[0]?.commitId).toBe(insertCommit.commitId);
    expect(history[0]?.parentIds).toHaveLength(1);
    expect(history[0]?.stateHashAfter.length).toBeGreaterThan(10);
  });

  test("force-new reinitializes database", async () => {
    const { dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    direct.run('CREATE TABLE foo (id INTEGER PRIMARY KEY, v TEXT)');
    direct.run("INSERT INTO foo(id, v) VALUES(1, 'x')");
    direct.close(false);

    const reinit = await initDatabase({ dbPath, forceNew: true });
    expect(reinit.dbPath).toBe(dbPath);
    const status = getStatus({ dbPath });
    expect(status.tableCount).toBe(0);
    expect(status.headCommit).toBeNull();
  });

  test("operations fail for table without primary key", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    direct.run("CREATE TABLE no_pk (name TEXT)");
    direct.close(false);

    const insertPlan = writePlanFile(dir, "insert-no-pk.json", {
      message: "insert no pk",
      operations: [{ type: "insert", table: "no_pk", values: { name: "a" } }],
    });

    try {
      await applyPlan(insertPlan, { dbPath });
      throw new Error("applyPlan should fail for table without PK");
    } catch (error) {
      expect(isTossError(error)).toBe(true);
      if (isTossError(error)) {
        expect(error.code).toBe("TABLE_WITHOUT_PRIMARY_KEY");
      }
    }
  });

  test("schemaHash includes UNIQUE/CHECK/FOREIGN KEY constraints", async () => {
    const ctxA = createTestContext();
    await initDatabase({ dbPath: ctxA.dbPath });
    const dbA = new Database(ctxA.dbPath);
    dbA.run(`
      CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        email TEXT UNIQUE,
        org_id INTEGER,
        CHECK (length(email) > 0),
        FOREIGN KEY(org_id) REFERENCES orgs(id)
      )
    `);
    const hashA = schemaHash(dbA);
    dbA.close(false);

    const ctxB = createTestContext();
    await initDatabase({ dbPath: ctxB.dbPath });
    const dbB = new Database(ctxB.dbPath);
    dbB.run(`
      CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        email TEXT,
        org_id INTEGER
      )
    `);
    const hashB = schemaHash(dbB);
    dbB.close(false);

    expect(hashA).not.toBe(hashB);
  });

  test("schemaHash preserves whitespace inside SQL string literals", async () => {
    const ctxA = createTestContext();
    await initDatabase({ dbPath: ctxA.dbPath });
    const dbA = new Database(ctxA.dbPath);
    dbA.run(`
      CREATE TABLE checks (
        id INTEGER PRIMARY KEY,
        value TEXT CHECK (value <> 'a  b')
      )
    `);
    const hashA = schemaHash(dbA);
    dbA.close(false);

    const ctxB = createTestContext();
    await initDatabase({ dbPath: ctxB.dbPath });
    const dbB = new Database(ctxB.dbPath);
    dbB.run(`
      CREATE TABLE checks (
        id INTEGER PRIMARY KEY,
        value TEXT CHECK (value <> 'a b')
      )
    `);
    const hashB = schemaHash(dbB);
    dbB.close(false);

    expect(hashA).not.toBe(hashB);
  });

  test("schemaHash includes COLLATE and generated-column expressions", async () => {
    const ctxA = createTestContext();
    await initDatabase({ dbPath: ctxA.dbPath });
    const dbA = new Database(ctxA.dbPath);
    dbA.run(`
      CREATE TABLE docs (
        id INTEGER PRIMARY KEY,
        title TEXT COLLATE NOCASE,
        normalized TEXT GENERATED ALWAYS AS (trim(lower(title))) STORED
      )
    `);
    const hashA = schemaHash(dbA);
    dbA.close(false);

    const ctxB = createTestContext();
    await initDatabase({ dbPath: ctxB.dbPath });
    const dbB = new Database(ctxB.dbPath);
    dbB.run(`
      CREATE TABLE docs (
        id INTEGER PRIMARY KEY,
        title TEXT COLLATE BINARY,
        normalized TEXT GENERATED ALWAYS AS (title) STORED
      )
    `);
    const hashB = schemaHash(dbB);
    dbB.close(false);

    expect(hashA).not.toBe(hashB);
  });

  test("drop_table revert restores table definition and rows", async () => {
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

  test("revert works when original DDL used unquoted table name", async () => {
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

  test("drop_column and alter_column_type revert restore schema and values", async () => {
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

  test("update/delete revert detects row conflicts", async () => {
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

  test("revert delete returns structured conflict when later commit reinserted same PK", async () => {
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

  test("revert of revert is supported", async () => {
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

  test("revert throws when update effect is missing beforeRow", async () => {
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

  test("revert throws when update beforeRow contains non-primitive values", async () => {
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

  test("verify stores last_verified_ok and preserves last_verified_ok_at on failure", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const setup = writePlanFile(dir, "setup.json", {
      message: "setup",
      operations: [
        {
          type: "create_table",
          table: "notes",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "body", type: "TEXT", notNull: true },
          ],
        },
      ],
    });
    await applyPlan(setup, { dbPath });

    const quick = verifyDatabase({ dbPath });
    expect(quick.ok).toBe(true);
    expect(quick.mode).toBe("quick");

    const firstStatus = getStatus({ dbPath });
    expect(firstStatus.lastVerifiedAt).not.toBeNull();
    expect(firstStatus.lastVerifiedOk).toBe(true);
    expect(firstStatus.lastVerifiedOkAt).not.toBeNull();

    const tamper = new Database(dbPath);
    tamper.query(`UPDATE ${COMMIT_TABLE} SET message='tampered' WHERE seq=1`).run();
    tamper.close(false);

    const broken = verifyDatabase({ dbPath, full: true });
    expect(broken.ok).toBe(false);
    expect(broken.mode).toBe("full");

    const secondStatus = getStatus({ dbPath });
    expect(secondStatus.lastVerifiedOk).toBe(false);
    expect(secondStatus.lastVerifiedOkAt).toBe(firstStatus.lastVerifiedOkAt);
  });

  test("snapshot recover restores and replays exact commit ids", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const tweak = new Database(dbPath);
    tweak
      .query(`UPDATE ${ENGINE_META_TABLE} SET value='1' WHERE key='snapshot_interval'`)
      .run();
    tweak
      .query(`UPDATE ${ENGINE_META_TABLE} SET value='10' WHERE key='snapshot_retain'`)
      .run();
    tweak.close(false);

    const create = writePlanFile(dir, "create.json", {
      message: "create logs",
      operations: [
        {
          type: "create_table",
          table: "logs",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "msg", type: "TEXT", notNull: true },
          ],
        },
      ],
    });
    const insert = writePlanFile(dir, "insert.json", {
      message: "insert log",
      operations: [{ type: "insert", table: "logs", values: { id: 1, msg: "hello" } }],
    });

    const firstCommit = await applyPlan(create, { dbPath });
    const secondCommit = await applyPlan(insert, { dbPath });

    const result = await recoverFromSnapshot(firstCommit.commitId, { dbPath });
    expect(result.replayedCommits).toBeGreaterThanOrEqual(1);

    const rows = readQuery("SELECT id, msg FROM logs", { dbPath });
    expect(rows).toEqual([{ id: 1, msg: "hello" }]);

    const history = getHistory({ dbPath, verbose: true });
    expect(history[0]?.commitId).toBe(secondCommit.commitId);
    expect(history[0]?.kind).toBe(secondCommit.kind);
    expect(history[0]?.createdAt).toBe(secondCommit.createdAt);
  });

  test("recover failure during replay does not overwrite original database", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const tweak = new Database(dbPath);
    tweak
      .query(`UPDATE ${ENGINE_META_TABLE} SET value='1' WHERE key='snapshot_interval'`)
      .run();
    tweak
      .query(`UPDATE ${ENGINE_META_TABLE} SET value='10' WHERE key='snapshot_retain'`)
      .run();
    tweak.close(false);

    const create = writePlanFile(dir, "create-safe-recover.json", {
      message: "create table",
      operations: [
        {
          type: "create_table",
          table: "recover_guard",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "value", type: "TEXT", notNull: true },
          ],
        },
      ],
    });
    const insertA = writePlanFile(dir, "insert-a-safe-recover.json", {
      message: "insert a",
      operations: [{ type: "insert", table: "recover_guard", values: { id: 1, value: "a" } }],
    });
    const insertB = writePlanFile(dir, "insert-b-safe-recover.json", {
      message: "insert b",
      operations: [{ type: "insert", table: "recover_guard", values: { id: 2, value: "b" } }],
    });

    const base = await applyPlan(create, { dbPath });
    await applyPlan(insertA, { dbPath });
    const latest = await applyPlan(insertB, { dbPath });

    const tamper = new Database(dbPath);
    tamper
      .query(`UPDATE ${COMMIT_TABLE} SET state_hash_after='broken-state-hash' WHERE commit_id=?`)
      .run(latest.commitId);
    tamper.close(false);

    try {
      await recoverFromSnapshot(base.commitId, { dbPath });
      throw new Error("recoverFromSnapshot should fail due to tampered replay metadata");
    } catch (error) {
      expect(isTossError(error)).toBe(true);
      if (isTossError(error)) {
        expect(error.code).toBe("RECOVER_FAILED");
      }
    }

    const rowsAfterFailure = readQuery("SELECT id, value FROM recover_guard ORDER BY id", { dbPath });
    expect(rowsAfterFailure).toEqual([
      { id: 1, value: "a" },
      { id: 2, value: "b" },
    ]);
  });

  test("snapshot creation does not leak tmp wal/shm sidecars", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const tweak = new Database(dbPath);
    tweak
      .query(`UPDATE ${ENGINE_META_TABLE} SET value='1' WHERE key='snapshot_interval'`)
      .run();
    tweak
      .query(`UPDATE ${ENGINE_META_TABLE} SET value='10' WHERE key='snapshot_retain'`)
      .run();
    tweak.close(false);

    const create = writePlanFile(dir, "create-snap-clean.json", {
      message: "create snapshots table",
      operations: [
        {
          type: "create_table",
          table: "snap_clean",
          columns: [{ name: "id", type: "INTEGER", primaryKey: true }],
        },
      ],
    });
    await applyPlan(create, { dbPath });

    const snapshotDir = join(dir, ".toss", "snapshots");
    const names = readdirSync(snapshotDir).sort();
    expect(names.length).toBeGreaterThan(0);
    expect(names.some((name) => name.startsWith("tmp-"))).toBe(false);
    expect(names.some((name) => name.endsWith(".db-wal"))).toBe(false);
    expect(names.some((name) => name.endsWith(".db-shm"))).toBe(false);
  });

  test("snapshot recover succeeds when untouched pre-existing tables exist", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    direct.run("CREATE TABLE external_data (id INTEGER PRIMARY KEY, body TEXT)");
    direct.run("INSERT INTO external_data(id, body) VALUES(1, 'stable')");
    direct.close(false);

    const tweak = new Database(dbPath);
    tweak
      .query(`UPDATE ${ENGINE_META_TABLE} SET value='1' WHERE key='snapshot_interval'`)
      .run();
    tweak
      .query(`UPDATE ${ENGINE_META_TABLE} SET value='10' WHERE key='snapshot_retain'`)
      .run();
    tweak.close(false);

    const create = writePlanFile(dir, "create-orders.json", {
      message: "create orders",
      operations: [
        {
          type: "create_table",
          table: "orders",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "item", type: "TEXT", notNull: true },
          ],
        },
      ],
    });
    const insert = writePlanFile(dir, "insert-orders.json", {
      message: "insert order",
      operations: [{ type: "insert", table: "orders", values: { id: 1, item: "book" } }],
    });

    const snapshotBase = await applyPlan(create, { dbPath });
    const replayed = await applyPlan(insert, { dbPath });

    const recovered = await recoverFromSnapshot(snapshotBase.commitId, { dbPath });
    expect(recovered.replayedCommits).toBe(1);

    const extRows = readQuery("SELECT id, body FROM external_data", { dbPath });
    expect(extRows).toEqual([{ id: 1, body: "stable" }]);
    const orderRows = readQuery("SELECT id, item FROM orders", { dbPath });
    expect(orderRows).toEqual([{ id: 1, item: "book" }]);

    const history = getHistory({ dbPath, verbose: true });
    expect(history[0]?.commitId).toBe(replayed.commitId);
  });

  test("init generates toss skill with migration guidance", async () => {
    const { dir, dbPath } = createTestContext();
    const result = await initDatabase({ dbPath, generateSkills: true, workspacePath: dir });
    expect(result.generatedSkills).not.toBeNull();
    if (!result.generatedSkills) {
      throw new Error("generatedSkills should exist");
    }

    expect(existsSync(result.generatedSkills.skillPath)).toBe(true);
    const skill = readFileSync(result.generatedSkills.skillPath, "utf8");
    expect(skill.includes("toss history --verbose")).toBe(true);
    expect(skill.includes("toss verify --quick")).toBe(true);
    expect(skill.includes("staged migrations")).toBe(true);
  });
});
