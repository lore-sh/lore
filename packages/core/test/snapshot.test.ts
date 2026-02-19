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
});
