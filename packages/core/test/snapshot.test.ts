import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { readdirSync } from "node:fs";
import { join } from "node:path";
import {
  applyPlan,
  getHistory,
  initDatabase,
  isTossError,
  readQuery,
  recoverFromSnapshot,
  revertCommit,
  verifyDatabase,
} from "../src";
import { COMMIT_TABLE } from "../src/db";
import { createTestContext, enableSnapshotEveryCommit, writePlanFile, withTmpDirCleanup } from "./helpers";

const testWithTmp = (name: string, fn: () => void | Promise<void>) => test(name, withTmpDirCleanup(fn));


describe("snapshot / recover", () => {
  testWithTmp("snapshot recover restores and replays exact commit ids", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    enableSnapshotEveryCommit(dbPath);

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

    const verify = verifyDatabase({ dbPath });
    expect(verify.ok).toBe(true);
    expect(verify.chainValid).toBe(true);
  });

  testWithTmp("recover failure during replay does not overwrite original database", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    enableSnapshotEveryCommit(dbPath);

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

  testWithTmp("snapshot creation does not leak tmp wal/shm sidecars", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    enableSnapshotEveryCommit(dbPath);

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

  testWithTmp("snapshot recover succeeds when untouched pre-existing tables exist", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });

    const direct = new Database(dbPath);
    direct.run("CREATE TABLE external_data (id INTEGER PRIMARY KEY, body TEXT)");
    direct.run("INSERT INTO external_data(id, body) VALUES(1, 'stable')");
    direct.close(false);

    enableSnapshotEveryCommit(dbPath);

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

  testWithTmp("snapshot replay does not re-fire triggers already captured in observed effects", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });
    enableSnapshotEveryCommit(dbPath);

    const create = writePlanFile(dir, "create-ledger-tables.json", {
      message: "create ledger tables",
      operations: [
        {
          type: "create_table",
          table: "account",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "balance", type: "INTEGER", notNull: true },
          ],
        },
        {
          type: "create_table",
          table: "ledger",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "account_id", type: "INTEGER", notNull: true },
            { name: "amount", type: "INTEGER", notNull: true },
          ],
        },
      ],
    });
    await applyPlan(create, { dbPath });

    const direct = new Database(dbPath);
    direct.run(`
      CREATE TRIGGER "trg-ledger-ai"
      AFTER INSERT ON ledger
      BEGIN
        UPDATE account SET balance = balance + NEW.amount WHERE id = NEW.account_id;
      END
    `);
    direct.run("INSERT INTO account(id, balance) VALUES (1, 0)");
    direct.close(false);

    const marker = writePlanFile(dir, "marker-ledger.json", {
      message: "marker",
      operations: [
        {
          type: "create_table",
          table: "marker",
          columns: [{ name: "id", type: "INTEGER", primaryKey: true }],
        },
      ],
    });
    const markerCommit = await applyPlan(marker, { dbPath });

    const insertLedger = writePlanFile(dir, "insert-ledger-with-trigger.json", {
      message: "insert ledger with trigger",
      operations: [{ type: "insert", table: "ledger", values: { id: 1, account_id: 1, amount: 7 } }],
    });
    await applyPlan(insertLedger, { dbPath });

    await recoverFromSnapshot(markerCommit.commitId, { dbPath });

    const rows = readQuery("SELECT id, balance FROM account WHERE id=1", { dbPath });
    expect(rows).toEqual([{ id: 1, balance: 7 }]);
  });

  testWithTmp("snapshot replay interleaves schema before blocked row effects in revert commit", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });
    enableSnapshotEveryCommit(dbPath);

    const direct = new Database(dbPath);
    direct.run("PRAGMA foreign_keys=ON");
    direct.run("CREATE TABLE parent_created_later (id INTEGER PRIMARY KEY, body TEXT NOT NULL)");
    direct.run(`
      CREATE TABLE child_requires_parent (
        id INTEGER PRIMARY KEY,
        parent_id INTEGER NOT NULL,
        body TEXT NOT NULL,
        FOREIGN KEY(parent_id) REFERENCES parent_created_later(id)
      )
    `);
    direct.run("INSERT INTO parent_created_later(id, body) VALUES (1, 'p1')");
    direct.run("INSERT INTO child_requires_parent(id, parent_id, body) VALUES (1, 1, 'c1')");
    direct.close(false);

    const destructive = writePlanFile(dir, "recover-delete-child-drop-parent.json", {
      message: "delete child then drop parent",
      operations: [
        { type: "delete", table: "child_requires_parent", where: { id: 1 } },
        { type: "drop_table", table: "parent_created_later" },
      ],
    });
    const dropped = await applyPlan(destructive, { dbPath });

    const reverted = revertCommit(dropped.commitId, { dbPath });
    expect(reverted.ok).toBe(true);
    if (!reverted.ok) {
      throw new Error("expected revert success before replay test");
    }

    const recovered = await recoverFromSnapshot(dropped.commitId, { dbPath });
    expect(recovered.replayedCommits).toBeGreaterThanOrEqual(1);

    const verify = new Database(dbPath);
    verify.run("PRAGMA foreign_keys=ON");
    const parentRows = verify
      .query("SELECT id, body FROM parent_created_later ORDER BY id")
      .all() as Array<{ id: number; body: string }>;
    const childRows = verify
      .query("SELECT id, parent_id, body FROM child_requires_parent ORDER BY id")
      .all() as Array<{ id: number; parent_id: number; body: string }>;
    expect(parentRows).toEqual([{ id: 1, body: "p1" }]);
    expect(childRows).toEqual([{ id: 1, parent_id: 1, body: "c1" }]);
    const fkCheck = verify.query("PRAGMA foreign_key_check").all() as unknown[];
    expect(fkCheck).toEqual([]);
    verify.close(false);

    const history = getHistory({ dbPath, verbose: true });
    expect(history[0]?.commitId).toBe(reverted.revertCommit.commitId);
  });

  testWithTmp("snapshot replay succeeds for first AUTOINCREMENT insert commit", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });
    enableSnapshotEveryCommit(dbPath);

    const direct = new Database(dbPath);
    direct.run("CREATE TABLE auto_replay (id INTEGER PRIMARY KEY AUTOINCREMENT, body TEXT NOT NULL)");
    direct.close(false);

    const basePlan = writePlanFile(dir, "autoinc-base-marker.json", {
      message: "base marker",
      operations: [
        {
          type: "create_table",
          table: "autoinc_base_marker",
          columns: [{ name: "id", type: "INTEGER", primaryKey: true }],
        },
      ],
    });
    const baseCommit = await applyPlan(basePlan, { dbPath });

    const firstInsertPlan = writePlanFile(dir, "autoinc-first-insert.json", {
      message: "first autoincrement insert",
      operations: [{ type: "insert", table: "auto_replay", values: { body: "x" } }],
    });
    const firstInsert = await applyPlan(firstInsertPlan, { dbPath });

    const recovered = await recoverFromSnapshot(baseCommit.commitId, { dbPath });
    expect(recovered.replayedCommits).toBeGreaterThanOrEqual(1);

    const rows = readQuery("SELECT id, body FROM auto_replay ORDER BY id", { dbPath });
    expect(rows).toEqual([{ id: 1, body: "x" }]);

    const history = getHistory({ dbPath, verbose: true });
    expect(history[0]?.commitId).toBe(firstInsert.commitId);
  });

  testWithTmp("snapshot replay succeeds for drop_table on AUTOINCREMENT table", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });
    enableSnapshotEveryCommit(dbPath);

    const direct = new Database(dbPath);
    direct.run("CREATE TABLE auto_schema (id INTEGER PRIMARY KEY AUTOINCREMENT, body TEXT NOT NULL)");
    direct.run("INSERT INTO auto_schema(body) VALUES ('a')");
    direct.close(false);

    const markerPlan = writePlanFile(dir, "autoinc-schema-marker.json", {
      message: "marker",
      operations: [
        {
          type: "create_table",
          table: "autoinc_schema_marker",
          columns: [{ name: "id", type: "INTEGER", primaryKey: true }],
        },
      ],
    });
    const markerCommit = await applyPlan(markerPlan, { dbPath });

    const dropPlan = writePlanFile(dir, "drop-autoinc-schema-replay.json", {
      message: "drop autoincrement table",
      operations: [{ type: "drop_table", table: "auto_schema" }],
    });
    const dropped = await applyPlan(dropPlan, { dbPath });

    const recovered = await recoverFromSnapshot(markerCommit.commitId, { dbPath });
    expect(recovered.replayedCommits).toBeGreaterThanOrEqual(1);

    const tableRows = readQuery("SELECT COUNT(*) AS c FROM sqlite_master WHERE type='table' AND name='auto_schema'", { dbPath });
    expect(tableRows).toEqual([{ c: 0 }]);
    const history = getHistory({ dbPath, verbose: true });
    expect(history[0]?.commitId).toBe(dropped.commitId);
  });

  testWithTmp("snapshot replay restores FK-related schema effects in dependency-safe order", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });
    enableSnapshotEveryCommit(dbPath);

    const direct = new Database(dbPath);
    direct.run("PRAGMA foreign_keys=ON");
    direct.run("CREATE TABLE z_parent (id INTEGER PRIMARY KEY, body TEXT NOT NULL)");
    direct.run(`
      CREATE TABLE a_child (
        id INTEGER PRIMARY KEY,
        parent_id INTEGER NOT NULL,
        body TEXT NOT NULL,
        FOREIGN KEY(parent_id) REFERENCES z_parent(id)
      )
    `);
    direct.run("INSERT INTO z_parent(id, body) VALUES (1, 'p1')");
    direct.run("INSERT INTO a_child(id, parent_id, body) VALUES (1, 1, 'c1')");
    direct.close(false);

    const dropBoth = writePlanFile(dir, "recover-drop-fk-both.json", {
      message: "drop child then parent",
      operations: [
        { type: "drop_table", table: "a_child" },
        { type: "drop_table", table: "z_parent" },
      ],
    });
    const dropped = await applyPlan(dropBoth, { dbPath });

    const reverted = revertCommit(dropped.commitId, { dbPath });
    expect(reverted.ok).toBe(true);
    if (!reverted.ok) {
      throw new Error("expected revert success before recover replay");
    }

    const recovered = await recoverFromSnapshot(dropped.commitId, { dbPath });
    expect(recovered.replayedCommits).toBeGreaterThanOrEqual(1);

    const verify = new Database(dbPath);
    verify.run("PRAGMA foreign_keys=ON");
    const parentRows = verify.query("SELECT id, body FROM z_parent ORDER BY id").all() as Array<{ id: number; body: string }>;
    const childRows = verify
      .query("SELECT id, parent_id, body FROM a_child ORDER BY id")
      .all() as Array<{ id: number; parent_id: number; body: string }>;
    expect(parentRows).toEqual([{ id: 1, body: "p1" }]);
    expect(childRows).toEqual([{ id: 1, parent_id: 1, body: "c1" }]);
    const fkCheck = verify.query("PRAGMA foreign_key_check").all() as unknown[];
    expect(fkCheck).toEqual([]);
    verify.close(false);

    const history = getHistory({ dbPath, verbose: true });
    expect(history[0]?.commitId).toBe(reverted.revertCommit.commitId);
  });

  testWithTmp("snapshot replay of self-FK schema rebuild revert preserves FK targets", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });
    enableSnapshotEveryCommit(dbPath);

    const direct = new Database(dbPath);
    direct.run("PRAGMA foreign_keys=ON");
    direct.run(`
      CREATE TABLE self_fk_replay (
        id INTEGER PRIMARY KEY,
        parent_id INTEGER REFERENCES self_fk_replay(id),
        note TEXT
      )
    `);
    direct.run("INSERT INTO self_fk_replay(id, parent_id, note) VALUES (1, NULL, 'root')");
    direct.run("INSERT INTO self_fk_replay(id, parent_id, note) VALUES (2, 1, 'child')");
    direct.close(false);

    const dropPlan = writePlanFile(dir, "self-fk-replay-drop-note.json", {
      message: "drop self fk note",
      operations: [{ type: "drop_column", table: "self_fk_replay", column: "note" }],
    });
    const dropped = await applyPlan(dropPlan, { dbPath });

    const reverted = revertCommit(dropped.commitId, { dbPath });
    expect(reverted.ok).toBe(true);
    if (!reverted.ok) {
      throw new Error("expected revert success for self-FK schema rebuild");
    }

    const recovered = await recoverFromSnapshot(dropped.commitId, { dbPath });
    expect(recovered.replayedCommits).toBeGreaterThanOrEqual(1);

    const verify = new Database(dbPath);
    verify.run("PRAGMA foreign_keys=ON");

    const columns = verify.query("PRAGMA table_info('self_fk_replay')").all() as Array<{ name: string }>;
    expect(columns.some((column) => column.name === "note")).toBe(true);

    const fkRows = verify.query("PRAGMA foreign_key_list('self_fk_replay')").all() as Array<{ table: string }>;
    expect(fkRows).toHaveLength(1);
    expect(fkRows[0]?.table).toBe("self_fk_replay");

    const fkCheck = verify.query("PRAGMA foreign_key_check").all() as unknown[];
    expect(fkCheck).toEqual([]);

    verify.run("INSERT INTO self_fk_replay(id, parent_id, note) VALUES (3, 1, 'grandchild')");
    expect(() => verify.run("INSERT INTO self_fk_replay(id, parent_id, note) VALUES (4, 999, 'bad')")).toThrow();
    verify.close(false);

    const history = getHistory({ dbPath, verbose: true });
    expect(history[0]?.commitId).toBe(reverted.revertCommit.commitId);
  });

  testWithTmp("snapshot replay preserves TEXT bytes with embedded NUL", async () => {
    const { dir, dbPath } = createTestContext();
    await initDatabase({ dbPath });
    enableSnapshotEveryCommit(dbPath);

    const create = writePlanFile(dir, "create-text-nul-recover.json", {
      message: "create text nul replay table",
      operations: [
        {
          type: "create_table",
          table: "text_nul_recover",
          columns: [
            { name: "id", type: "INTEGER", primaryKey: true },
            { name: "payload", type: "TEXT", notNull: true },
            { name: "tag", type: "TEXT", notNull: true },
          ],
        },
      ],
    });
    await applyPlan(create, { dbPath });

    const direct = new Database(dbPath);
    direct.run("INSERT INTO text_nul_recover(id, payload, tag) VALUES (1, CAST(X'410042' AS TEXT), 'a')");
    direct.close(false);

    const marker = writePlanFile(dir, "text-nul-recover-marker.json", {
      message: "marker",
      operations: [
        {
          type: "create_table",
          table: "text_nul_recover_marker",
          columns: [{ name: "id", type: "INTEGER", primaryKey: true }],
        },
      ],
    });
    const markerCommit = await applyPlan(marker, { dbPath });

    const update = writePlanFile(dir, "text-nul-recover-update.json", {
      message: "update tag",
      operations: [{ type: "update", table: "text_nul_recover", values: { tag: "b" }, where: { id: 1 } }],
    });
    const updated = await applyPlan(update, { dbPath });

    await recoverFromSnapshot(markerCommit.commitId, { dbPath });
    const rows = readQuery(
      "SELECT id, hex(CAST(payload AS BLOB)) AS payload_hex, length(CAST(payload AS BLOB)) AS payload_len, tag FROM text_nul_recover",
      { dbPath },
    );
    expect(rows).toEqual([{ id: 1, payload_hex: "410042", payload_len: 3, tag: "b" }]);

    const history = getHistory({ dbPath, verbose: true });
    expect(history[0]?.commitId).toBe(updated.commitId);
  });
});
